package database

import (
	"log/slog"
	"math/rand"
	"path"
	"strconv"
	"strings"
	"testing"

	runnerv1 "code.forgejo.org/forgejo/actions-proto/runner/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

func getInstance(t *testing.T) *service {
	dir := t.TempDir()
	if dbInstance != nil {
		dbInstance.Close()
		dbInstance = nil
	}
	dbPath = path.Join(dir, "test.db")
	t.Cleanup(func() {
		if dbInstance != nil {
			dbInstance.Close()
			dbInstance = nil
		}
	})
	return New(slog.Default()).(*service)
}

func TestStartDatabase(t *testing.T) {
	db := getInstance(t)
	if status, ok := db.Health()["status"]; !ok || status != "up" {
		t.Errorf("database fail: %s", status)
	}

	kv := make(map[string]string, 10000)
	for range 10000 {
		k := strconv.FormatInt(rand.Int63(), 10)
		v := strconv.FormatInt(rand.Int63(), 10)
		kv[k] = v
		if value, err := db.GetConfig(k, "<not found>"); err != nil || value != "<not found>" {
			t.Error("assert key not found")
		}
		if err := db.SetConfig(k, v); err != nil {
			t.Error("assert insert key ok")
		}
	}
	for k, v := range kv {
		if value, err := db.GetConfig(k, "<not found>"); err != nil || value != v {
			t.Error("assert the same value")
		}
		v = strconv.FormatInt(rand.Int63(), 10)
		kv[k] = v
		if err := db.SetConfig(k, v); err != nil {
			t.Error("assert update value ok")
		}
	}
	for k, v := range kv {
		if value, err := db.GetConfig(k, "<not found>"); err != nil || value != v {
			t.Error("assert the same value")
		}
	}
}

func TestTasks(t *testing.T) {
	db := getInstance(t)
	if tasks, err := db.QueryAllOpenTasks(); err != nil || len(tasks) != 0 {
		t.Error("assert no tasks")
	}
	for i := range 100 {
		fields := make(map[string]*structpb.Value, 1000)
		for _, v := range []string{"repository", "ref_type", "ref", "workflow", "job", "sha"} {
			fields[v] = structpb.NewStringValue(strings.ToUpper(v))
		}
		task, err := db.PersistTask(&runnerv1.Task{
			Id: int64(i),
			Context: &structpb.Struct{
				Fields: fields,
			},
		}, strconv.FormatInt(int64(i), 10))
		if err != nil || task.Repo != "REPOSITORY" || task.Sha != "SHA" {
			t.Error("assert persist ok")
		}
	}
	tasks, err := db.QueryAllOpenTasks()
	if err != nil || len(tasks) != 100 {
		t.Error("query error")
	}
	for i, task := range tasks {
		if err != nil || task.Repo != "REPOSITORY" || task.Sha != "SHA" ||
			task.Token != strconv.FormatInt(int64(i), 10) {
			t.Error("assert persist ok")
		}
	}
	for i, task := range tasks {
		task.Status = int64(i) * 1
		task.RunId = int64(i) * 2
		task.JobId = int64(i) * 3
		task.LogIdx = int64(i) * 4
		if i%2 == 0 {
			task.Status = StatusDone
		}
		if err := db.UpdateTaskStatus(task); err != nil {
			t.Error("assert update ok")
		}
	}
	tasks, err = db.QueryAllOpenTasks()
	if err != nil || len(tasks) != 50 {
		t.Error("query error")
	}
	for _, task := range tasks {
		i := task.Id
		if i%2 != 0 || task.Status != i || task.RunId != i*2 || task.JobId != i*3 || task.LogIdx != i*4 {
			task.Status = StatusDone
		}
	}
}
