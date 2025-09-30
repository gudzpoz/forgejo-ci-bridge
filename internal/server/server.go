package server

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"sync"
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
}

func NewServer(logger *slog.Logger, ctx context.Context) *Server {
	db := database.New(logger)

	client, err := client.New(ctx, logger, db)
	if err != nil {
		logger.Error("unable to create client", "err", err)
		os.Exit(1)
	}
	if err := client.RegisterBridge(ctx, db); err != nil {
		logger.Error("unable to register runner", "err", err)
		os.Exit(1)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	port, _ := strconv.Atoi(os.Getenv("PORT"))
	NewServer := &Server{
		logger:     logger,
		port:       port,
		db:         db,
		client:     client,
		pollCtx:    runCtx,
		pollCancel: cancel,
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
	tasks, err := s.db.QueryAllOpenTasks()
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	for i := range tasks {
		tracker := client.NewTracker(*s.logger, tasks[i], s.client)
		wg.Go(func() { tracker.Track(s.pollCtx) })
	}
	wg.Go(func() {
		tasks := s.client.PollTasks(s.pollCtx, 0)
		for task := range tasks {
			s.logger.Info("execute task", "task", task)
			info, err := s.db.PersistTask(task.Task, task.Token)
			if err != nil {
				s.logger.Error("unable to persist task", "err", err, "task", task)
			} else {
				tracker := client.NewTracker(*s.logger, info, s.client)
				wg.Go(func() { tracker.Track(s.pollCtx) })
			}
		}
		s.logger.Info("dispatcher exiting")
	})
	err = s.serv.ListenAndServe()
	wg.Wait()
	return err
}

func (s *Server) Shutdown(ctx context.Context) error {
	err := s.serv.Shutdown(ctx)
	return err
}
