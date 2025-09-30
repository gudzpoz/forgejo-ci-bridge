package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	runnerv1 "code.forgejo.org/forgejo/actions-proto/runner/v1"
	_ "github.com/joho/godotenv/autoload"
	_ "github.com/mattn/go-sqlite3"
)

// Service represents a service that interacts with a database.
type Service interface {
	// Health returns a map of health status information.
	// The keys and values in the map are service-specific.
	Health() map[string]string

	LoadRunner(token string) *runnerv1.Runner
	SaveRunner(token string, runner *runnerv1.Runner) error

	GetTaskVersion() int64
	SetTaskVersion(version int64) error

	QueryAllOpenTasks() ([]*ForgejoTask, error)
	PersistTask(task *runnerv1.Task, token string) (*ForgejoTask, error)
	UpdateTaskStatus(task *ForgejoTask) error

	// Close terminates the database connection.
	// It returns an error if the connection cannot be closed.
	Close() error
}

type service struct {
	logger *slog.Logger

	rdb *sql.DB
	wdb *sql.DB
}

var (
	dbPath     = os.Getenv("DB_PATH")
	dbInstance *service
)

func New(logger *slog.Logger) Service {
	logger = logger.WithGroup("db")

	// Reuse Connection
	if dbInstance != nil {
		return dbInstance
	}

	isNew := false
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		isNew = true
	}
	url := "file:" + dbPath + "?_journal=WAL&_timeout=5000&_fk=true"
	read_url := url + "&mode=ro"
	write_url := url + "&mode=rwc&_txlock=immediate&_vacuum=incremental"

	wdb, err := sql.Open("sqlite3", write_url)
	if err != nil {
		logger.Error("db open error", "err", err)
		os.Exit(1)
	}
	wdb.SetMaxOpenConns(1)
	wdb.SetMaxIdleConns(1)
	// Hopefully a finite lifetime lets SQLite set checkpoints and free up WAL space
	wdb.SetConnMaxLifetime(time.Hour * 12)
	wdb.SetConnMaxIdleTime(0)

	var rdb *sql.DB
	rdb, err = sql.Open("sqlite3", read_url)
	if err != nil {
		logger.Error("db open error", "err", err)
		os.Exit(1)
	}

	dbInstance = &service{
		logger: logger,

		rdb: rdb,
		wdb: wdb,
	}

	if isNew {
		err = dbInstance.init()
		if err != nil {
			logger.Error("db init error", "err", err)
			os.Exit(1)
		}
	}
	if err = dbInstance.upgrade(); err != nil {
		logger.Error("db upgrade error", "err", err)
		os.Exit(1)
	}

	return dbInstance
}

func (s *service) init() error {
	if _, err := s.wdb.Exec("CREATE TABLE config (key text PRIMARY KEY, value text)"); err != nil {
		return err
	}
	if _, err := s.wdb.Exec("INSERT INTO config (key, value) VALUES ('dbversion', '0')"); err != nil {
		return err
	}
	// Change to incremental vacuum
	_, err := s.wdb.Exec("VACUUM")
	return err
}

func (s *service) upgrade() error {
	verStr, err := s.GetConfig("dbversion", "0")
	if err != nil {
		return err
	}
	ver, err := strconv.Atoi(verStr)
	if err != nil {
		return err
	}

	try := func(nextVer int, sqls ...string) error {
		tx, err := s.wdb.Begin()
		if err != nil {
			return err
		}
		for _, sql := range sqls {
			_, err = tx.Exec(sql)
			if err != nil {
				tx.Rollback()
				return err
			}
		}
		_, err = tx.Exec("UPDATE config SET value = ? WHERE key = ?", strconv.Itoa(nextVer), "dbversion")
		if err != nil {
			tx.Rollback()
			return err
		}
		return tx.Commit()
	}

	switch ver {
	case 0:
		if err := try(1); err != nil {
			return err
		}
		fallthrough
	case 1:
		if err := try(2,
			`CREATE TABLE tasks (
			  id integer PRIMARY KEY,
				repo text NOT NULL,
				ref text NOT NULL,
				sha text NOT NULL,
				status integer NOT NULL,
				runid integer NOT NULL,
				jobid integer NOT NULL,
				loglines integer NOT NULL,
				workflow text NOT NULL
			)`,
		); err != nil {
			return err
		}
		fallthrough
	case 2:
		break
	default:
		return fmt.Errorf("unknown database version: %d", ver)
	}
	return nil
}

func (s *service) GetConfig(key string, defaultValue string) (string, error) {
	var value string
	err := s.rdb.QueryRow("SELECT value FROM config WHERE key = ?", key).Scan(&value)
	if err == sql.ErrNoRows {
		return defaultValue, nil
	}
	return value, err
}

func (s *service) SetConfig(key string, value string) error {
	_, err := s.wdb.Exec(
		"INSERT INTO config (key, value) VALUES (?, ?)"+
			" ON CONFLICT (key) DO UPDATE SET value = ?",
		key, value, value,
	)
	return err
}

func (s *service) LoadRunner(token string) *runnerv1.Runner {
	encoded, err := s.GetConfig("runner_"+token, "")
	if err != nil {
		return nil
	}
	var runner runnerv1.Runner
	if err := json.Unmarshal([]byte(encoded), &runner); err != nil {
		return nil
	}
	return &runner
}
func (s *service) SaveRunner(token string, runner *runnerv1.Runner) error {
	bytes, err := json.Marshal(runner)
	if err != nil {
		return err
	}
	return s.SetConfig("runner_"+token, string(bytes))
}

func (s *service) GetTaskVersion() int64 {
	ver, _ := s.GetConfig("taskver", "0")
	i, _ := strconv.ParseInt(ver, 16, 64)
	return i
}
func (s *service) SetTaskVersion(version int64) error {
	return s.SetConfig("taskver", strconv.FormatInt(version, 16))
}

// Health checks the health of the database connection by pinging the database.
// It returns a map with keys indicating various health statistics.
func (s *service) Health() map[string]string {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	stats := make(map[string]string)

	// Ping the database
	err := s.wdb.PingContext(ctx)
	if err != nil {
		stats["status"] = "down"
		stats["error"] = fmt.Sprintf("db down: %v", err)
		s.logger.Error("health: db down", "err", err) // Log the error and terminate the program
		os.Exit(1)
		return stats
	}

	// Database is up, add more statistics
	stats["status"] = "up"
	stats["message"] = "It's healthy"

	// Get database stats (like open connections, in use, idle, etc.)
	dbStats := s.wdb.Stats()
	stats["open_connections"] = strconv.Itoa(dbStats.OpenConnections)
	stats["in_use"] = strconv.Itoa(dbStats.InUse)
	stats["idle"] = strconv.Itoa(dbStats.Idle)
	stats["wait_count"] = strconv.FormatInt(dbStats.WaitCount, 10)
	stats["wait_duration"] = dbStats.WaitDuration.String()
	stats["max_idle_closed"] = strconv.FormatInt(dbStats.MaxIdleClosed, 10)
	stats["max_lifetime_closed"] = strconv.FormatInt(dbStats.MaxLifetimeClosed, 10)

	// Evaluate stats to provide a health message
	if dbStats.OpenConnections > 40 { // Assuming 50 is the max for this example
		stats["message"] = "The database is experiencing heavy load."
	}

	if dbStats.WaitCount > 1000 {
		stats["message"] = "The database has a high number of wait events, indicating potential bottlenecks."
	}

	if dbStats.MaxIdleClosed > int64(dbStats.OpenConnections)/2 {
		stats["message"] = "Many idle connections are being closed, consider revising the connection pool settings."
	}

	if dbStats.MaxLifetimeClosed > int64(dbStats.OpenConnections)/2 {
		stats["message"] = "Many connections are being closed due to max lifetime, consider increasing max lifetime or revising the connection usage pattern."
	}

	return stats
}

// Close closes the database connection.
// It logs a message indicating the disconnection from the specific database.
// If the connection is successfully closed, it returns nil.
// If an error occurs while closing the connection, it returns the error.
func (s *service) Close() error {
	s.logger.Info("disconnected", "db", dbPath)
	err := s.wdb.Close()
	if err := s.rdb.Close(); err != nil {
		return err
	}
	return err
}
