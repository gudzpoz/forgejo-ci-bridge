package client

import (
	"context"
	"forgejo-ci-bridge/internal/database"
	"log/slog"
	"math/bits"
	"strconv"
	"time"

	runnerv1 "code.forgejo.org/forgejo/actions-proto/runner/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Tracker struct {
	logger slog.Logger

	task   *database.ForgejoTask
	client *Client
}

func NewTracker(logger slog.Logger, task *database.ForgejoTask, client *Client) *Tracker {
	return &Tracker{
		logger: *logger.WithGroup("track"),
		task:   task,
		client: client,
	}
}

const (
	StatusInit = iota
	StatusPushed
	StatusTracked
	StatusRunStarted
	StatusRunEnded

	StatusDone = database.StatusDone
)

func LightSleep(ctx context.Context, long time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(long):
	}
}

func (t *Tracker) Track(ctx context.Context) {
	sync := func(status int64, msg string) {
		t.logger.Info(msg, "sha", t.task.Sha)
		t.task.Status = status
		if err := t.client.db.UpdateTaskStatus(t.task); err != nil {
			t.logger.Error("unable to sync task status", "sha", t.task.Sha, "status", status)
		}

		line := runnerv1.LogRow{
			Time:    timestamppb.Now(),
			Content: msg,
		}
		logs := []*runnerv1.LogRow{&line}
		err := t.client.PushLog(ctx, t.task, logs)
		if err != nil {
			t.logger.Error("unable to sync task status", "sha", t.task.Sha, "status", status)
		}

		if err := t.client.db.UpdateTaskStatus(t.task); err != nil {
			t.logger.Error("unable to sync task status", "sha", t.task.Sha, "status", status)
		}
	}

	switch t.task.Status {
	case StatusInit:
		for ctx.Err() == nil {
			if err := t.client.CheckExistence(ctx, t.task.Sha); err != nil {
				t.logger.Info("commit not found", "sha", t.task.Sha, "err", err)
				LightSleep(ctx, 10*time.Second) // go slower
			} else {
				break
			}
		}
		if ctx.Err() != nil {
			break
		}
		sync(StatusPushed, "GitHub mirror synchronized: "+t.task.Sha)
		fallthrough

	case StatusPushed:
		for ctx.Err() == nil {
			run, err := t.client.CheckAssociatedWorkflowRuns(ctx, t.task)
			if err != nil {
				t.logger.Info("workflow run not started", "sha", t.task.Sha, "err", err)
				LightSleep(ctx, 10*time.Second) // go slower
			} else {
				t.task.RunId = *run.ID
				break
			}
		}
		if ctx.Err() != nil {
			break
		}
		sync(StatusRunStarted, "GitHub Actions run started: "+
			"https://github.com/"+github_repo[0]+"/"+github_repo[1]+
			"/actions/runs/"+strconv.FormatInt(t.task.RunId, 10))
		fallthrough

	case StatusRunStarted:
		count := uint64(0)
		for ctx.Err() == nil {
			run, err := t.client.GetWorkflowRun(ctx, t.task)
			if err != nil {
				t.logger.Info("workflow run not found", "sha", t.task.Sha, "err", err)
				LightSleep(ctx, 30*time.Second) // go slower
			} else if *run.Status == "completed" {
				break
			} else {
				count++
				if bits.OnesCount64(count) == 1 {
					t.logger.Info("workflow running...", "sha", t.task.Sha, "duration", count*30)
				}
			}
		}
		if ctx.Err() != nil {
			break
		}
		sync(StatusRunEnded, "GitHub Actions run completed")
		fallthrough

	case StatusRunEnded:
		logs, err := t.client.DownloadWorkflowLog(ctx, t.task)
		if err != nil {
			t.logger.Info("workflow log download failed", "sha", t.task.Sha, "err", err)
		}
		if ctx.Err() != nil {
			break
		}
		if len(logs) != 0 {
			rows := make([]*runnerv1.LogRow, 0, len(logs))
			for _, line := range logs {
				rows = append(rows, &runnerv1.LogRow{
					Time:    timestamppb.Now(),
					Content: line,
				})
			}
			if err := t.client.PushLog(ctx, t.task, rows); err != nil {
				t.logger.Info("workflow log sync failed", "sha", t.task.Sha, "err", err)
			}
		}
		sync(StatusDone, "GitHub Actions run completed")
		fallthrough

	case StatusDone:
		err := t.client.MarkTaskDone(ctx, t.task.Task)
		if err != nil {
			t.logger.Info("failed to mark task done", "sha", t.task.Sha, "err", err)
		}
	}
	t.logger.Info("tracker exiting", "sha", t.task.Sha)
}
