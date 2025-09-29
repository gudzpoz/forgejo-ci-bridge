package client

import (
	"context"
	"forgejo-ci-bridge/internal/database"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	pingv1 "code.forgejo.org/forgejo/actions-proto/ping/v1"
	"code.forgejo.org/forgejo/actions-proto/ping/v1/pingv1connect"
	runnerv1 "code.forgejo.org/forgejo/actions-proto/runner/v1"
	"code.forgejo.org/forgejo/actions-proto/runner/v1/runnerv1connect"
	"connectrpc.com/connect"
	"golang.org/x/time/rate"
)

type Client struct {
	logger *slog.Logger

	db database.Service

	pingv1connect.PingServiceClient
	runnerv1connect.RunnerServiceClient
}

var (
	scheme = os.Getenv("FORGEJO_SCHEME")
	host   = os.Getenv("FORGEJO")
	labels = loadLabels()
	client *Client
	runner *runnerv1.Runner
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

func clientAuth(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		if runner != nil {
			req.Header().Set("x-runner-uuid", runner.Uuid)
			req.Header().Set("x-runner-token", runner.Token)
		}
		return next(ctx, req)
	}
}

func New(logger *slog.Logger, db database.Service) *Client {
	if client != nil {
		return client
	}

	opts := []connect.ClientOption{
		connect.WithInterceptors(connect.UnaryInterceptorFunc(clientAuth)),
	}

	if scheme != "http" {
		scheme = "https"
	}
	url := scheme + "://" + host + "/api/actions"
	client = &Client{
		logger: logger.WithGroup("proto"),
		db:     db,
		PingServiceClient: pingv1connect.NewPingServiceClient(
			http.DefaultClient,
			url,
			opts...,
		),
		RunnerServiceClient: runnerv1connect.NewRunnerServiceClient(
			http.DefaultClient,
			url,
			opts...,
		),
	}

	return client
}

func (c *Client) EnsurePing(ctx context.Context) {
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
	c.logger.Info("ping success", "pong", pong.Msg.Data)
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
	if runner = db.LoadRunner(); runner == nil {
		c.logger.Info("registering", "name", CLIENT_NAME)
		res, err := c.Register(ctx, connect.NewRequest(&runnerv1.RegisterRequest{
			Name:      CLIENT_NAME,
			Token:     os.Getenv("TOKEN"),
			Version:   CLIENT_VER,
			Labels:    labels,
			Ephemeral: false,
		}))
		if err != nil {
			return err
		}
		runner = res.Msg.Runner
	} else if needsUpdate(runner) {
		c.logger.Info("updating info", "labels", labels)
		res, err := c.Declare(ctx, connect.NewRequest(&runnerv1.DeclareRequest{
			Version: CLIENT_VER,
			Labels:  labels,
		}))
		if err != nil {
			return err
		}
		runner.Labels = res.Msg.Runner.Labels
		runner.Version = res.Msg.Runner.Version
	} else {
		c.logger.Info("already registered", "name", runner.Name)
	}
	if err := db.SaveRunner(runner); err != nil {
		return err
	}
	c.logger.Info("using runner", "runner", runner)
	return nil
}

func (c *Client) PollTasks(ctx context.Context, version int64) chan *runnerv1.Task {
	channel := make(chan *runnerv1.Task)
	taskVer := c.db.GetTaskVersion()
	go func() {
		limiter := rate.NewLimiter(rate.Every(5*time.Second), 1)
		for {
			select {
			case <-ctx.Done():
				close(channel)
			default:
				if err := limiter.Wait(ctx); err != nil {
					continue
				}
				task, err := c.FetchTask(ctx, connect.NewRequest(&runnerv1.FetchTaskRequest{
					TasksVersion: taskVer,
				}))
				if err != nil {
					c.logger.Error("error fetching tasks", "err", err)
					continue
				}
				if err := c.db.SetTaskVersion(taskVer); err != nil {
					c.logger.Error("error persisting task version", "err", err)
				}
				c.logger.Info("next task", "taskver", task.Msg.Task.Id)
				channel <- task.Msg.Task
			}
		}
	}()
	return channel
}
