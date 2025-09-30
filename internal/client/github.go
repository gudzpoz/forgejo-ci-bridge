package client

import (
	"bufio"
	"context"
	"fmt"
	"forgejo-ci-bridge/internal/database"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	runnerv1 "code.forgejo.org/forgejo/actions-proto/runner/v1"
	"github.com/google/go-github/v75/github"
	"golang.org/x/time/rate"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	github_token = os.Getenv("GITHUB_TOKEN")
	github_repo  = strings.Split(os.Getenv("GITHUB_REPO"), "/")
	date_regxp   = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T`)
	date_len     = len(time.DateOnly + "T")
)

type GitHubClient struct {
	*github.Client

	limit *rate.Limiter
}

func initGithubClient(ctx context.Context) (*GitHubClient, error) {
	client := github.NewClient(nil).WithAuthToken(github_token)
	client.RateLimitRedirectionalEndpoints = true
	limit, _, err := client.RateLimit.Get(ctx)
	if err != nil {
		return nil, err
	}
	return &GitHubClient{
		Client: client,
		limit:  rate.NewLimiter(rate.Limit(float64(limit.Core.Limit)/3600), 1),
	}, nil
}

func (c *Client) CheckExistence(ctx context.Context, sha string) error {
	if err := c.gh.limit.Wait(ctx); err != nil {
		return err
	}
	commits, _, err := c.gh.Repositories.ListCommits(ctx, github_repo[0], github_repo[1], &github.CommitsListOptions{
		SHA: sha,
		ListOptions: github.ListOptions{
			PerPage: 1,
		},
	})
	if err != nil {
		return err
	}
	if len(commits) == 0 || *commits[0].SHA != sha {
		return fmt.Errorf("unknown commit: %s", sha)
	}
	return nil
}

func (c *Client) CheckAssociatedWorkflowRuns(
	ctx context.Context, task *database.ForgejoTask,
) (*github.WorkflowRun, *github.WorkflowJob, error) {
	if err := c.gh.limit.Wait(ctx); err != nil {
		return nil, nil, err
	}
	runs, _, err := c.gh.Actions.ListWorkflowRunsByFileName(
		ctx, github_repo[0], github_repo[1], task.Yml,
		&github.ListWorkflowRunsOptions{
			HeadSHA: task.Sha,
			ListOptions: github.ListOptions{
				PerPage: 1,
			},
		},
	)
	if err != nil {
		return nil, nil, err
	}
	if *runs.TotalCount == 0 {
		return nil, nil, fmt.Errorf("workflow run not found for: %s", task.Sha)
	}
	run := runs.WorkflowRuns[0]
	jobs, _, err := c.gh.Actions.ListWorkflowJobs(
		ctx, github_repo[0], github_repo[1], *run.ID,
		&github.ListWorkflowJobsOptions{},
	)
	if err != nil {
		return nil, nil, err
	}
	var job *github.WorkflowJob
	job = nil
	for _, j := range jobs.Jobs {
		if *j.Name == task.Job {
			job = j
		}
	}
	if job == nil {
		return nil, nil, fmt.Errorf("no matching job found for %s", task.Job)
	}
	return run, job, nil
}

func (c *Client) GetWorkflowRun(ctx context.Context, task *database.ForgejoTask) (*github.WorkflowRun, error) {
	if err := c.gh.limit.Wait(ctx); err != nil {
		return nil, err
	}
	run, _, err := c.gh.Actions.GetWorkflowRunByID(ctx, github_repo[0], github_repo[1], task.RunId)
	return run, err
}

type GitHubWorkflowLog struct {
	Steps  []string
	States []*runnerv1.StepState
}

func (c *Client) DownloadWorkflowLog(ctx context.Context, task *database.ForgejoTask) ([]*runnerv1.LogRow, error) {
	url, _, err := c.gh.Actions.GetWorkflowJobLogs(ctx, github_repo[0], github_repo[1], task.JobId, 4)
	if err != nil {
		return nil, err
	}
	res, err := http.DefaultClient.Get(url.String())
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := res.Body.Close(); err != nil {
			c.logger.Error("error closing conn", "url", url.String())
		}
	}()

	logs := make([]*runnerv1.LogRow, 0, 100)
	var at time.Time
	sb := make([]string, 0, 10)
	commit := func() {
		if len(sb) != 0 {
			content := strings.Join(sb, "\n")
			logs = append(logs, &runnerv1.LogRow{
				Time:    timestamppb.New(at),
				Content: content,
			})
		}
		sb = sb[0:0]
	}
	proc := func(line string) string {
		if len(line) == 0 ||
			line[0] < '0' || '9' < line[0] ||
			!date_regxp.MatchString(line[0:min(len(line), date_len)]) {
			return line
		}
		end := strings.Index(line[0:min(64, len(line))], " ")
		if end == -1 {
			return line
		}
		when, err := time.Parse(time.RFC3339Nano, line[0:end])
		if err != nil {
			return line
		}
		commit()
		at = when
		return line[end+1:]
	}

	sc := bufio.NewScanner(res.Body)
	for sc.Scan() {
		line := sc.Text()
		sb = append(sb, proc(line))
	}
	commit()
	if err := sc.Err(); err != nil {
		return logs, err
	}
	return logs, nil
}
