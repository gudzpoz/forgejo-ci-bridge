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
	logger *slog.Logger

	port int
	serv *http.Server

	db     database.Service
	client *client.Client

	pollCtx    context.Context
	pollCancel context.CancelFunc
	pollEnd    chan bool
}

func NewServer(ctx context.Context) *Server {
	logger := slog.Default().WithGroup("ci")
	db := database.New(logger)

	client := client.New(logger, db)
	client.EnsurePing(ctx)
	if err := client.RegisterBridge(ctx, db); err != nil {
		logger.Error("unable to register runner", "err", err)
		os.Exit(1)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	pollEnd := make(chan bool)
	port, _ := strconv.Atoi(os.Getenv("PORT"))
	NewServer := &Server{
		logger:     logger,
		port:       port,
		db:         db,
		client:     client,
		pollCtx:    runCtx,
		pollCancel: cancel,
		pollEnd:    pollEnd,
	}

	// Declare Server config
	NewServer.serv = &http.Server{
		Addr:         fmt.Sprintf(":%d", NewServer.port),
		Handler:      NewServer.RegisterRoutes(),
		IdleTimeout:  time.Minute,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
	}
	NewServer.serv.RegisterOnShutdown(func() {
		NewServer.pollCancel()
	})

	return NewServer
}

func (s *Server) ListenAndServe() error {
	go func() {
		tasks := s.client.PollTasks(s.pollCtx, 0)
		for task := range tasks {
			s.logger.Info("execute task", "task", task)
		}
	}()
	return s.serv.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	err := s.serv.Shutdown(ctx)
	<-s.pollEnd
	return err
}
