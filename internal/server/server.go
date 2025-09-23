package server

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"

	_ "github.com/joho/godotenv/autoload"

	"forgejo-ci-bridge/internal/client"
	"forgejo-ci-bridge/internal/database"
)

type Server struct {
	port int

	db database.Service
}

func NewServer(ctx context.Context) *http.Server {
	logger := slog.Default().WithGroup("ci")

	port, _ := strconv.Atoi(os.Getenv("PORT"))
	NewServer := &Server{
		port: port,

		db: database.New(logger),
	}

	client := client.New(logger)
	client.EnsurePing(ctx)
	if err := client.RegisterBridge(ctx, NewServer.db); err != nil {
		logger.Error("unable to register runner", "err", err)
		os.Exit(1)
	}

	// Declare Server config
	server := &http.Server{
		Addr:         fmt.Sprintf(":%d", NewServer.port),
		Handler:      NewServer.RegisterRoutes(),
		IdleTimeout:  time.Minute,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	return server
}
