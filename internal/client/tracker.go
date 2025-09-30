package client

import (
	"context"
	"forgejo-ci-bridge/internal/database"
	"log/slog"
	"math/bits"
	"strconv"
	"strings"
	"time"

	runnerv1 "code.forgejo.org/forgejo/actions-proto/runner/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gopkg.in/yaml.v3"
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
	StatusLogging

	StatusDone = database.StatusDone
)

type stepInfo struct {
	Run  *string `yaml:"run,omitempty"`
	Uses *string `yaml:"uses,omitempty"`
}
type jobInfo struct {
	Steps []*stepInfo `yaml:"steps,omitempty"`
}
type workflowFile struct {
	Jobs map[string]*jobInfo `yaml:"jobs,omitempty"`
}

func (w *workflowFile) getSteps(job string) []string {
	if w.Jobs == nil {
		return nil
	}
	j, ok := w.Jobs[job]
	if !ok || j.Steps == nil {
		return nil
	}
	names := make([]string, 0, len(j.Steps))
	for _, step := range j.Steps {
		name := step.Uses
		if name == nil && step.Run != nil {
			name = step.Run
		}
		var s string
		if name != nil {
			end := strings.Index(*name, "\n")
			if end == -1 {
				end = len(*name)
			}
			s = (*name)[0:end]
		}
		names = append(names, s)
	}
	return names
}

func LightSleep(ctx context.Context, long time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(long):
	}
}

func (t *Tracker) Track(ctx context.Context) {
	repo := t.task.Repo
	sync := func(status int64, msg string) {
		t.logger.Info(msg, "sha", t.task.Sha)
		t.task.Status = status
		if err := t.client.db.UpdateTaskStatus(t.task); err != nil {
			t.logger.Error("unable to sync task status", "sha", t.task.Sha, "status", status, "err", err)
		}

		if msg == "" {
			return
		}
		line := runnerv1.LogRow{
			Time:    timestamppb.Now(),
			Content: msg,
		}
		logs := []*runnerv1.LogRow{&line}
		err := t.client.PushLog(ctx, t.task, logs, false)
		if err != nil {
			t.logger.Error("unable to push logs", "sha", t.task.Sha, "status", status, "err", err)
		}

		if err := t.client.db.UpdateTaskStatus(t.task); err != nil {
			t.logger.Error("unable to sync task status", "sha", t.task.Sha, "status", status, "err", err)
		}
	}

	switch t.task.Status {
	case StatusInit:
		for ctx.Err() == nil {
			if err := t.client.CheckExistence(ctx, repo, t.task.Sha); err != nil {
				t.logger.Info("commit not found", "sha", t.task.Sha, "err", err)
			} else {
				break
			}
			LightSleep(ctx, 10*time.Second) // go slower
		}
		if ctx.Err() != nil {
			break
		}
		sync(StatusPushed, "GitHub mirror synchronized: "+t.task.Sha)
		fallthrough

	case StatusPushed:
		for ctx.Err() == nil {
			run, job, err := t.client.CheckAssociatedWorkflowRuns(ctx, repo, t.task)
			if err != nil {
				t.logger.Info("workflow run not started", "sha", t.task.Sha, "err", err)
			} else {
				t.task.RunId = *run.ID
				t.task.JobId = *job.ID
				break
			}
			LightSleep(ctx, 10*time.Second) // go slower
		}
		if ctx.Err() != nil {
			break
		}
		repo := strings.Split(t.task.Repo, "/")
		sync(StatusRunStarted, "GitHub Actions run started: "+
			"https://github.com/"+t.client.gh[repo[0]].user+"/"+repo[1]+
			"/actions/runs/"+strconv.FormatInt(t.task.RunId, 10))
		fallthrough

	case StatusRunStarted:
		for {
			if err := t.client.MarkTaskRunning(ctx, t.task); err != nil {
				t.logger.Error("unable to mark task running", "sha", t.task.Sha, "err", err)
			} else {
				break
			}
			LightSleep(ctx, 5*time.Second) // go slower
		}
		if ctx.Err() != nil {
			break
		}
		count := uint64(0)
		for ctx.Err() == nil {
			run, err := t.client.GetWorkflowRun(ctx, repo, t.task)
			if err != nil {
				t.logger.Info("workflow run not found", "sha", t.task.Sha, "err", err)
			} else if *run.Status == "completed" {
				break
			} else {
				count++
				sync(StatusRunStarted, "GitHub Actions still running: "+*run.Status)
				if bits.OnesCount64(count) == 1 {
					t.logger.Info("workflow running...", "sha", t.task.Sha, "duration", count*30)
				}
			}
			LightSleep(ctx, 30*time.Second) // go slower
		}
		if ctx.Err() != nil {
			break
		}
		sync(StatusRunEnded, "GitHub Actions run completed")
		fallthrough

	case StatusRunEnded:
		sync(StatusLogging, "Sending GitHub Actions logs")
		fallthrough

	case StatusLogging:
		var file workflowFile
		if err := yaml.Unmarshal(t.task.WorkflowPayload, &file); err != nil {
			t.logger.Info("workflow yaml parse failed", "sha", t.task.Sha, "err", err)
		}
		steps := file.getSteps(t.task.Job)

		logs, err := t.client.DownloadWorkflowLog(ctx, repo, t.task)
		if err != nil {
			t.logger.Info("workflow log download failed", "sha", t.task.Sha, "err", err)
		}
		if ctx.Err() != nil {
			break
		}
		if err := t.client.PushLog(ctx, t.task, logs, true); err != nil {
			t.logger.Info("workflow log sync failed", "sha", t.task.Sha, "err", err)
		}
		if len(logs) == 0 {
			logs = []*runnerv1.LogRow{
				{
					Time:    timestamppb.Now(),
					Content: "===no log returned===",
				},
			}
		}
		err = t.client.MarkTaskDone(
			ctx, t.task,
			mergeStepLogs(steps, logs, t.task.LogIdx-int64(len(logs))),
		)
		if err != nil {
			t.logger.Info("failed to mark task done", "sha", t.task.Sha, "err", err)
		}
		sync(StatusDone, "")
		fallthrough

	case StatusDone:
	}
	t.logger.Info("tracker exiting", "sha", t.task.Sha)
}

func mergeStepLogs(steps []string, logs []*runnerv1.LogRow, offset int64) []*runnerv1.StepState {
	if len(steps) == 0 {
		return []*runnerv1.StepState{
			{
				Id:        0,
				Result:    runnerv1.Result_RESULT_SUCCESS,
				StartedAt: logs[0].Time,
				StoppedAt: logs[len(logs)-1].Time,
				LogIndex:  offset,
				LogLength: int64(len(logs)),
			},
		}
	}

	states := make([]*runnerv1.StepState, 0, len(steps)+1)
	var from, to int
	commit := func() {
		states = append(states, &runnerv1.StepState{
			Id:        int64(len(states)),
			Result:    runnerv1.Result_RESULT_SUCCESS,
			StartedAt: logs[max(min(from, len(logs)), 0)].Time,
			StoppedAt: logs[max(min(to-1, len(logs)), 0)].Time,
			LogIndex:  int64(from) + offset,
			LogLength: int64(to - from),
		})
		from = min(to, len(logs))
	}
	for _, step := range steps {
		if step == "" {
			commit()
			continue
		}
		for {
			if to >= len(logs) {
				break
			}
			var content = ""
			at := to
			content = logs[at].Content
			if strings.HasPrefix(content, "##[") {
				if strings.Contains(content[0:min(len(content), len(step)+16)], step) {
					break
				}
			}
			to++
		}
		commit()
		to++
	}
	to = len(logs)
	commit()
	if len(states) > 1 {
		states[1].LogIndex = 0
	}
	return states[1:]
}
