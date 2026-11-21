package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	_ "modernc.org/sqlite"

	"github.com/Code-oclock/hawk-synthetic-monitor/internal/entity"
	"github.com/Code-oclock/hawk-synthetic-monitor/internal/usecase"
)

var _ usecase.ResultRepo = (*Repo)(nil)

// Repo stores monitor run results in an SQLite database.
type Repo struct {
	db *sql.DB
}

// Open creates the file and schema; path ":memory:" works as an in-memory database.
func Open(ctx context.Context, path string) (*Repo, error) {
	dsn := path
	if path != ":memory:" {
		if strings.Contains(path, "?") {
			dsn = path + "&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)"
		} else {
			dsn = path + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)"
		}
	}

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite db: %w", err)
	}
	db.SetMaxOpenConns(1)

	const createTableQuery = `
CREATE TABLE IF NOT EXISTS runs (
	id TEXT PRIMARY KEY,
	started_at INTEGER NOT NULL,
	status TEXT NOT NULL,
	data TEXT NOT NULL
);`
	if _, err := db.ExecContext(ctx, createTableQuery); err != nil {
		_ = db.Close()

		return nil, fmt.Errorf("create runs table: %w", err)
	}

	const createIndexQuery = `CREATE INDEX IF NOT EXISTS idx_runs_started_at ON runs(started_at);`
	if _, err := db.ExecContext(ctx, createIndexQuery); err != nil {
		_ = db.Close()

		return nil, fmt.Errorf("create runs index: %w", err)
	}

	return &Repo{db: db}, nil
}

// Close closes the database connection.
func (r *Repo) Close() error {
	if r.db == nil {
		return nil
	}
	if err := r.db.Close(); err != nil {
		return fmt.Errorf("close sqlite db: %w", err)
	}

	return nil
}

// Save upserts a result by run ID.
func (r *Repo) Save(ctx context.Context, result entity.RunResult) error {
	data, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("marshal run result: %w", err)
	}

	var startedAt int64
	if !result.StartedAt.IsZero() {
		startedAt = result.StartedAt.UnixNano()
	}

	const query = `
INSERT INTO runs (id, started_at, status, data)
VALUES (?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
	started_at = excluded.started_at,
	status = excluded.status,
	data = excluded.data;`

	if _, err := r.db.ExecContext(ctx, query, result.ID, startedAt, string(result.Status), string(data)); err != nil {
		return fmt.Errorf("save run: %w", err)
	}

	return nil
}

// Latest returns the last passed or failed result by started_at, or nil if none.
func (r *Repo) Latest(ctx context.Context) (*entity.RunResult, error) {
	const query = `SELECT data FROM runs WHERE status IN ('passed', 'failed') ORDER BY started_at DESC LIMIT 1;`

	var data []byte
	err := r.db.QueryRowContext(ctx, query).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("latest run: %w", err)
	}

	var result entity.RunResult
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, fmt.Errorf("unmarshal run result: %w", err)
	}

	return &result, nil
}

// Recent returns newest results first, up to limit.
func (r *Repo) Recent(ctx context.Context, limit int) ([]entity.RunResult, error) {
	if limit <= 0 {
		return []entity.RunResult{}, nil
	}

	const query = `SELECT data FROM runs ORDER BY started_at DESC LIMIT ?;`

	rows, err := r.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("recent runs: %w", err)
	}
	defer rows.Close()

	var results []entity.RunResult
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return nil, fmt.Errorf("scan run: %w", err)
		}

		var res entity.RunResult
		if err := json.Unmarshal(data, &res); err != nil {
			return nil, fmt.Errorf("unmarshal run result: %w", err)
		}
		results = append(results, res)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate runs: %w", err)
	}
	if results == nil {
		return []entity.RunResult{}, nil
	}

	return results, nil
}

// Stats returns counts and success rate for runs with started_at >= since.
func (r *Repo) Stats(ctx context.Context, since time.Time) (entity.Stats, error) {
	var sinceNano int64 = math.MinInt64
	if !since.IsZero() {
		sinceNano = since.UnixNano()
	}

	const query = `
SELECT
	COUNT(*),
	COUNT(CASE WHEN status = 'passed' THEN 1 END),
	COUNT(CASE WHEN status = 'failed' THEN 1 END),
	COUNT(CASE WHEN status = 'canceled' THEN 1 END)
FROM runs
WHERE started_at >= ?;`

	var stats entity.Stats
	err := r.db.QueryRowContext(ctx, query, sinceNano).Scan(
		&stats.Total,
		&stats.Passed,
		&stats.Failed,
		&stats.Canceled,
	)
	if err != nil {
		return entity.Stats{}, fmt.Errorf("stats: %w", err)
	}

	if denom := stats.Passed + stats.Failed; denom > 0 {
		stats.SuccessRate = float64(stats.Passed) / float64(denom)
	}

	return stats, nil
}
