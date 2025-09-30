package client

import (
	"context"
	"encoding/json"
	"fmt"
	"forgejo-ci-bridge/internal/database"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	pingv1 "code.forgejo.org/forgejo/actions-proto/ping/v1"
	"code.forgejo.org/forgejo/actions-proto/ping/v1/pingv1connect"
	runnerv1 "code.forgejo.org/forgejo/actions-proto/runner/v1"
	"code.forgejo.org/forgejo/actions-proto/runner/v1/runnerv1connect"
	"connectrpc.com/connect"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type forgejoClient struct {
	pingv1connect.PingServiceClient
	runnerv1connect.RunnerServiceClient
	token  string
	runner *runnerv1.Runner
}

func (j *forgejoClient) clientAuth(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		if j.runner != nil {
			req.Header().Set("x-runner-uuid", j.runner.Uuid)
			req.Header().Set("x-runner-token", j.runner.Token)
		}
		return next(ctx, req)
	}
}

type Client struct {
	logger *slog.Logger

	db database.Service

	jm map[string]*forgejoClient
	jo []*forgejoClient
	gh map[string]*GitHubClient
}

var (
	labels = loadLabels()
	client *Client
)

const (
	CLIENT_NAME = "forgejo-ci-bridge"
	CLIENT_VER  = "0.1.0"
)

func loadLabels() []string {
	labels := strings.Split(os.Getenv("LABELS"), ",")
	for i := range labels {
		labels[i] = strings.TrimSpace(labels[i])
	}
	return labels
}

func New(ctx context.Context, logger *slog.Logger, db database.Service) (*Client, error) {
	if client != nil {
		return client, nil
	}

	var forgejoUsers [][]string
	err := json.Unmarshal([]byte(os.Getenv("FORGEJO_USERS")), &forgejoUsers)
	if err != nil {
		return nil, err
	}
	var githubUsers map[string][]string
	err = json.Unmarshal([]byte(os.Getenv("GITHUB_USERS")), &githubUsers)
	if err != nil {
		return nil, err
	}

	clients := make([]*forgejoClient, len(forgejoUsers))
	mapping := make(map[string]*forgejoClient, len(forgejoUsers))
	for i, user := range forgejoUsers {
		url := user[0] + "/api/actions"
		client := &forgejoClient{token: user[1]}
		clients[i] = client
		mapping[client.token] = client
		opts := []connect.ClientOption{
			connect.WithInterceptors(connect.UnaryInterceptorFunc(clients[i].clientAuth)),
		}
		client.PingServiceClient = pingv1connect.NewPingServiceClient(
			http.DefaultClient,
			url,
			opts...,
		)
		client.RunnerServiceClient = runnerv1connect.NewRunnerServiceClient(
			http.DefaultClient,
			url,
			opts...,
		)
	}

	gh := make(map[string]*GitHubClient, len(githubUsers))
	for joUser, ghInfo := range githubUsers {
		ghClient, err := initGithubClient(ctx, ghInfo[0], ghInfo[1])
		if err != nil {
			return nil, err
		}
		gh[joUser] = ghClient
	}

	client = &Client{
		logger: logger.WithGroup("proto"),

		db: db,
		jo: clients,
		jm: mapping,
		gh: gh,
	}
	client.EnsurePing(ctx)

	return client, nil
}

func (c *Client) EnsurePing(ctx context.Context) {
	wg := sync.WaitGroup{}
	for i, client := range c.jo {
		wg.Go(func() {
			ping := CLIENT_NAME
			pong, err := client.Ping(ctx, connect.NewRequest(&pingv1.PingRequest{
				Data: ping,
			}))
			if err != nil {
				c.logger.Error("ping error", "ping", ping, "err", err)
				os.Exit(1)
			}
			if !strings.Contains(pong.Msg.Data, ping) {
				c.logger.Error("unexpected pong", "ping", ping, "pong", pong.Msg.Data)
				os.Exit(1)
			}
			c.logger.Info("ping success", "pong", pong.Msg.Data, "i", i)
		})
	}
	wg.Wait()
}

func needsUpdate(runner *runnerv1.Runner) bool {
	if runner.Version != CLIENT_VER {
		return true
	}
	set := make(map[string]struct{}, len(runner.Labels))
	for i := range runner.Labels {
		set[runner.Labels[i]] = struct{}{}
	}
	for i := range labels {
		if _, ok := set[labels[i]]; !ok {
			return true
		}
		delete(set, labels[i])
	}
	return len(set) != 0
}

func (c *Client) RegisterBridge(ctx context.Context, db database.Service) error {
	wg := errgroup.Group{}
	for i, client := range c.jo {
		wg.Go(func() error {
			runner := db.LoadRunner(client.token)
			if runner == nil {
				c.logger.Info("registering", "name", CLIENT_NAME, "i", i)
				res, err := client.Register(ctx, connect.NewRequest(&runnerv1.RegisterRequest{
					Name:      CLIENT_NAME,
					Token:     client.token,
					Version:   CLIENT_VER,
					Labels:    labels,
					Ephemeral: false,
				}))
				if err != nil {
					return err
				}
				runner = res.Msg.Runner
			} else if needsUpdate(runner) {
				c.logger.Info("updating info", "labels", labels, "i", i)
				res, err := client.Declare(ctx, connect.NewRequest(&runnerv1.DeclareRequest{
					Version: CLIENT_VER,
					Labels:  labels,
				}))
				if err != nil {
					return err
				}
				runner.Labels = res.Msg.Runner.Labels
				runner.Version = res.Msg.Runner.Version
			} else {
				c.logger.Info("already registered", "name", runner.Name, "i", i)
			}
			if err := db.SaveRunner(client.token, runner); err != nil {
				return err
			}
			client.runner = runner
			c.logger.Info("using runner", "runner", runner, "i", i)
			return nil
		})
	}
	return wg.Wait()
}

func (c *Client) PollTasks(ctx context.Context, version int64) chan *database.ForgejoTask {
	c.logger.Info("starting pollers")
	channel := make(chan *database.ForgejoTask)
	taskVer := c.db.GetTaskVersion()
	wg := sync.WaitGroup{}
	for i, client := range c.jo {
		wg.Go(func() {
			c.logger.Info("poller starting", "i", i)
			limiter := rate.NewLimiter(rate.Every(5*time.Second), 1)
			for {
				select {
				case <-ctx.Done():
					c.logger.Info("poller ending", "i", i)
					return
				default:
					if err := limiter.Wait(ctx); err != nil {
						continue
					}
					task, err := client.FetchTask(ctx, connect.NewRequest(&runnerv1.FetchTaskRequest{
						TasksVersion: taskVer,
					}))
					if err != nil {
						c.logger.Error("error fetching tasks", "err", err, "i", i)
						continue
					}
					if err := c.db.SetTaskVersion(taskVer); err != nil {
						c.logger.Error("error persisting task version", "err", err, "i", i)
					}
					if task.Msg.Task != nil {
						c.logger.Info("next task", "taskver", task.Msg.Task.Id, "i", i)
						channel <- &database.ForgejoTask{
							Task:  task.Msg.Task,
							Token: client.token,
						}
					}
				}
			}
		})
	}
	go func() {
		wg.Wait()
		close(channel)
		c.logger.Info("all poller exiting")
	}()
	return channel
}

func (c *Client) findClient(task *database.ForgejoTask) *forgejoClient {
	client := c.jm[task.Token]
	if client == nil {
		panic(fmt.Sprintf(`client not found: "%s" (%v)`, task.Token, c.jm))
	}
	return client
}

func (c *Client) PushLog(
	ctx context.Context, task *database.ForgejoTask, rows []*runnerv1.LogRow, nomore bool,
) error {
	jc := c.findClient(task)
	res, err := jc.UpdateLog(ctx, connect.NewRequest(&runnerv1.UpdateLogRequest{
		TaskId: task.Id,
		Index:  task.LogIdx,
		Rows:   rows,
		NoMore: nomore,
	}))
	if err != nil {
		return err
	}
	if res.Msg.AckIndex > task.LogIdx {
		task.LogIdx = res.Msg.AckIndex
	}
	return nil
}

func (c *Client) MarkTaskRunning(ctx context.Context, task *database.ForgejoTask) error {
	jc := c.findClient(task)
	_, err := jc.UpdateTask(ctx, connect.NewRequest(&runnerv1.UpdateTaskRequest{
		State: &runnerv1.TaskState{
			Id:        task.Id,
			StartedAt: timestamppb.Now(),
		},
	}))
	return err
}

func (c *Client) MarkTaskDone(ctx context.Context, task *database.ForgejoTask, steps []*runnerv1.StepState) error {
	jc := c.findClient(task)
	_, err := jc.UpdateTask(ctx, connect.NewRequest(&runnerv1.UpdateTaskRequest{
		State: &runnerv1.TaskState{
			Id:        task.Id,
			Result:    runnerv1.Result_RESULT_SUCCESS,
			StoppedAt: timestamppb.Now(),
			Steps:     steps,
		},
	}))
	return err
}
