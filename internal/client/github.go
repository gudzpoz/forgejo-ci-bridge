package client

import (
	"archive/zip"
	"bufio"
	"bytes"
	"context"
	"fmt"
	"forgejo-ci-bridge/internal/database"
	"io"
	"os"
	"strings"

	"github.com/google/go-github/v75/github"
	"golang.org/x/time/rate"
)

var (
	github_token = os.Getenv("GITHUB_TOKEN")
	github_repo  = strings.Split(os.Getenv("GITHUB_REPO"), "/")
)

type GitHubClient struct {
	*github.Client

	limit *rate.Limiter
}

func initGithubClient(ctx context.Context) (*GitHubClient, error) {
	client := github.NewClient(nil)
	client.WithAuthToken(github_token)
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
) (*github.WorkflowRun, error) {
	if err := c.gh.limit.Wait(ctx); err != nil {
		return nil, err
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
		return nil, err
	}
	if *runs.TotalCount == 0 {
		return nil, fmt.Errorf("workflow run not found for: %s", task.Sha)
	}
	return runs.WorkflowRuns[0], nil
}

func (c *Client) GetWorkflowRun(ctx context.Context, task *database.ForgejoTask) (*github.WorkflowRun, error) {
	if err := c.gh.limit.Wait(ctx); err != nil {
		return nil, err
	}
	run, _, err := c.gh.Actions.GetWorkflowRunByID(ctx, github_repo[0], github_repo[1], task.RunId)
	return run, err
}

func (c *Client) DownloadWorkflowLog(ctx context.Context, task *database.ForgejoTask) ([]string, error) {
	url, _, err := c.gh.Actions.GetWorkflowRunLogs(ctx, github_repo[0], github_repo[1], task.RunId, 8)
	if err != nil {
		return nil, err
	}
	res, err := c.gh.Client.Client().Get(url.String())
	if err != nil {
		return nil, err
	}
	buff := bytes.NewBuffer([]byte{})
	_, err = io.Copy(buff, res.Body)
	res.Body.Close()
	if err != nil {
		return nil, err
	}
	unzip, err := zip.NewReader(bytes.NewReader(buff.Bytes()), int64(len(buff.Bytes())))
	if err != nil {
		return nil, err
	}
	lines := make([]string, 0, 100)
	for _, f := range unzip.File {
		rc, err := f.Open()
		if err != nil {
			return lines, err
		}
		defer rc.Close()
		sc := bufio.NewScanner(rc)
		for sc.Scan() {
			lines = append(lines, sc.Text())
		}
		if err := sc.Err(); err != nil {
			return lines, err
		}
	}
	return lines, nil
}
