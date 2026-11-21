// Package heartbeat delivers monitor run heartbeats to an external watcher.
package heartbeat

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/Code-oclock/hawk-synthetic-monitor/internal/entity"
	"github.com/Code-oclock/hawk-synthetic-monitor/internal/usecase"
)

const defaultTimeout = 10 * time.Second

var _ usecase.Heartbeat = (*Pinger)(nil)

type Pinger struct {
	url    string
	client *http.Client
}

type payload struct {
	Status     string `json:"status"`
	RunID      string `json:"run_id"`
	FinishedAt string `json:"finished_at"`
}

// New creates a heartbeat pinger targeting the external watcher URL.
func New(rawURL string, client *http.Client) *Pinger {
	if client == nil {
		client = &http.Client{
			Timeout: defaultTimeout,
		}
	}

	return &Pinger{
		url:    rawURL,
		client: client,
	}
}

// Ping reports run outcome to the watcher URL.
func (p *Pinger) Ping(ctx context.Context, result entity.RunResult) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	body, err := json.Marshal(payload{
		Status:     string(result.Status),
		RunID:      result.ID,
		FinishedAt: result.FinishedAt.Format(time.RFC3339),
	})
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.url, bytes.NewReader(body))
	if err != nil {
		var urlErr *url.Error
		if errors.As(err, &urlErr) {
			return fmt.Errorf("create request: %w", urlErr.Err)
		}

		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := p.client
	if client == nil {
		client = &http.Client{Timeout: defaultTimeout}
	}

	resp, err := client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(err, context.Canceled) {
			return context.Canceled
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return context.DeadlineExceeded
		}

		var urlErr *url.Error
		if errors.As(err, &urlErr) {
			return fmt.Errorf("send ping: %w", urlErr.Err)
		}

		return fmt.Errorf("send ping: %w", err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}

	return fmt.Errorf("status %d", resp.StatusCode)
}
