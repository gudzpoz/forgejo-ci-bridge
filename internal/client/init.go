package client

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"strings"

	pingv1 "code.forgejo.org/forgejo/actions-proto/ping/v1"
	"code.forgejo.org/forgejo/actions-proto/ping/v1/pingv1connect"
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
	ping := "forgejo-ci-bridge"
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
