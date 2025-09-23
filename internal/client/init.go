package client

import (
	"context"
	"forgejo-ci-bridge/internal/database"
	"log/slog"
	"net/http"
	"os"
	"strings"

	pingv1 "code.forgejo.org/forgejo/actions-proto/ping/v1"
	"code.forgejo.org/forgejo/actions-proto/ping/v1/pingv1connect"
	runnerv1 "code.forgejo.org/forgejo/actions-proto/runner/v1"
	"code.forgejo.org/forgejo/actions-proto/runner/v1/runnerv1connect"
	"connectrpc.com/connect"
)

type Client struct {
	logger *slog.Logger

	pingv1connect.PingServiceClient
	runnerv1connect.RunnerServiceClient
}

var (
	host   = os.Getenv("FORGEJO")
	client *Client
	runner *runnerv1.Runner
)

const (
	CLIENT_NAME = "forgejo-ci-bridge"
	CLIENT_VER  = "0.1.0"
)

func New(logger *slog.Logger) *Client {
	if client != nil {
		return client
	}

	url := "https://" + host + "/api/actions"
	client = &Client{
		logger: logger.WithGroup("proto"),
		PingServiceClient: pingv1connect.NewPingServiceClient(
			http.DefaultClient,
			url,
		),
		RunnerServiceClient: runnerv1connect.NewRunnerServiceClient(
			http.DefaultClient,
			url,
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

func (c *Client) RegisterBridge(ctx context.Context, db database.Service) error {
	if runner = db.LoadRunner(); runner == nil {
		labels := strings.Split(os.Getenv("LABELS"), ",")
		for i := range labels {
			labels[i] = strings.TrimSpace(labels[i])
		}
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
		if err := db.SaveRunner(runner); err != nil {
			return err
		}
	}
	c.logger.Info("using runner", "runner", runner)
	return nil
}
