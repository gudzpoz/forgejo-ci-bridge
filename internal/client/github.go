package client

import (
	"bufio"
	"context"
	"fmt"
	"forgejo-ci-bridge/internal/database"
	"net/http"
	"regexp"
	"strings"
	"time"

	runnerv1 "code.forgejo.org/forgejo/actions-proto/runner/v1"
	"github.com/google/go-github/v75/github"
	"golang.org/x/time/rate"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	date_regxp = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T`)
	date_len   = len(time.DateOnly + "T")
)

type GitHubClient struct {
	*github.Client

	user  string
	limit *rate.Limiter
}

func initGithubClient(ctx context.Context, user string, token string) (*GitHubClient, error) {
	client := github.NewClient(nil).WithAuthToken(token)
	client.RateLimitRedirectionalEndpoints = true
	limit, _, err := client.RateLimit.Get(ctx)
	if err != nil {
		return nil, err
	}
	return &GitHubClient{
		Client: client,
		user:   user,
		limit:  rate.NewLimiter(rate.Limit(float64(limit.Core.Limit)/3600), 1),
	}, nil
}

func (c *Client) getGitHubRepo(userSlashRepo string) (gh *GitHubClient, user string, repo string) {
	info := strings.Split(userSlashRepo, "/")
	joUser := info[0]
	gh = c.gh[joUser]
	user = gh.user
	repo = info[1]
	return
}

func (c *Client) CheckExistence(ctx context.Context, repo string, sha string) error {
	gh, user, repo := c.getGitHubRepo(repo)
	if err := gh.limit.Wait(ctx); err != nil {
		return err
	}
	commits, _, err := gh.Repositories.ListCommits(ctx, user, repo, &github.CommitsListOptions{
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
	ctx context.Context, repo string, task *database.ForgejoTask,
) (*github.WorkflowRun, *github.WorkflowJob, error) {
	gh, user, repo := c.getGitHubRepo(repo)
	if err := gh.limit.Wait(ctx); err != nil {
		return nil, nil, err
	}
	runs, _, err := gh.Actions.ListWorkflowRunsByFileName(
		ctx, user, repo, task.Yml,
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
	jobs, _, err := gh.Actions.ListWorkflowJobs(
		ctx, user, repo, *run.ID,
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

func (c *Client) GetWorkflowRun(ctx context.Context, repo string, task *database.ForgejoTask) (*github.WorkflowRun, error) {
	gh, user, repo := c.getGitHubRepo(repo)
	if err := gh.limit.Wait(ctx); err != nil {
		return nil, err
	}
	run, _, err := gh.Actions.GetWorkflowRunByID(ctx, user, repo, task.RunId)
	return run, err
}

type GitHubWorkflowLog struct {
	Steps  []string
	States []*runnerv1.StepState
}

func (c *Client) DownloadWorkflowLog(ctx context.Context, repo string, task *database.ForgejoTask) ([]*runnerv1.LogRow, error) {
	gh, user, repo := c.getGitHubRepo(repo)
	url, _, err := gh.Actions.GetWorkflowJobLogs(ctx, user, repo, task.JobId, 4)
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
