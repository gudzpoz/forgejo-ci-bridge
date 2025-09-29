package database

import (
	"encoding/json"

	runnerv1 "code.forgejo.org/forgejo/actions-proto/runner/v1"
)

const StatusDone = 1000

type ForgejoTask struct {
	*runnerv1.Task

	Repo string
	Ref  string
	Sha  string
	Yml  string
	Job  string

	LogIdx int64
	Status int64
	RunId  int64
}

func ExtractTask(task *runnerv1.Task) *ForgejoTask {
	info := task.Context.Fields
	repo := info["repository"].GetStringValue()
	ref := info["ref_type"].GetStringValue() + ":" + info["ref"].GetStringValue()
	yml := info["workflow"].GetStringValue()
	job := info["job"].GetStringValue()
	sha := info["sha"].GetStringValue()

	return &ForgejoTask{
		Task: task,
		Repo: repo,
		Ref:  ref,
		Sha:  sha,
		Yml:  yml,
		Job:  job,

		LogIdx: 0,
		Status: 0,
		RunId:  0,
	}
}

func (s *service) PersistTask(task *runnerv1.Task) (*ForgejoTask, error) {
	info := ExtractTask(task)
	bytes, err := json.Marshal(task)
	if err != nil {
		return nil, err
	}
	_, err = s.wdb.Exec(
		`INSERT INTO tasks (id, repo, ref, sha, status, loglines, tracking, workflow)
		 VALUES (?,?,?,?,?,?,?,?)`,
		info.Id, info.Repo, info.Ref, info.Sha, 0, 0, 0, string(bytes),
	)
	if err != nil {
		return nil, err
	}
	return info, nil
}

func (s *service) QueryAllOpenTasks() ([]*ForgejoTask, error) {
	rows, err := s.rdb.Query(
		"SELECT status, tracking, workflow FROM tasks WHERE status < ?",
		StatusDone,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	tasks := make([]*ForgejoTask, 0, 10)
	for rows.Next() {
		var status, run int64
		var workflow string
		if err := rows.Scan(&status, &run, &workflow); err != nil {
			return nil, err
		}
		var task runnerv1.Task
		if err := json.Unmarshal([]byte(workflow), &task); err != nil {
			return nil, err
		}
		info := ExtractTask(&task)
		info.Status = status
		info.RunId = run
		tasks = append(tasks, info)
	}
	return tasks, nil
}

func (s *service) UpdateTaskStatus(task *ForgejoTask) error {
	_, err := s.wdb.Exec(
		"UPDATE tasks SET status = ?, loglines = ?, tracking = ? WHERE id = ?",
		task.Status, task.LogIdx, task.RunId, task.Id,
	)
	return err
}
