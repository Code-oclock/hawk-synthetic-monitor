// Package hawk talks to a Hawk collector and its GraphQL API: it sends test events and finds them.
package hawk

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Code-oclock/hawk-synthetic-monitor/internal/usecase"
)

const (
	defaultTimeout = 10 * time.Second
	maxBodyBytes   = 1 << 20
)

var _ usecase.HawkGateway = (*Client)(nil)

type Config struct {
	CollectorURL     string
	APIURL           string
	HealthURL        string
	IntegrationToken string
	ProjectID        string
	Email            string
	Password         string
	HTTPClient       *http.Client
}

type Client struct {
	cfg  Config
	http *http.Client

	mu    sync.Mutex
	token string
}

func New(cfg Config) *Client {
	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: defaultTimeout}
	}

	return &Client{cfg: cfg, http: httpClient}
}

// Health checks the collector health endpoint.
func (c *Client) Health(ctx context.Context) error {
	healthURL := c.cfg.HealthURL
	if healthURL == "" {
		healthURL = strings.TrimRight(c.cfg.CollectorURL, "/") + "/health"
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, healthURL, http.NoBody)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		return fmt.Errorf("%w: %w", usecase.ErrTransient, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		return fmt.Errorf("%w: %w", usecase.ErrTransient, err)
	}

	if resp.StatusCode == http.StatusTooManyRequests || (resp.StatusCode >= 500 && resp.StatusCode <= 599) {
		return fmt.Errorf("%w: health check status %d", usecase.ErrTransient, resp.StatusCode)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health check status %d", resp.StatusCode)
	}

	var status struct {
		RedisStatus   *bool `json:"redis_status"`
		MongoDBStatus *bool `json:"mongo_db_status"`
	}
	if err := json.Unmarshal(body, &status); err != nil {
		return fmt.Errorf("health check response not json: %w", err)
	}

	if status.RedisStatus == nil || !*status.RedisStatus || status.MongoDBStatus == nil || !*status.MongoDBStatus {
		return errors.New("health check failed: unhealthy status")
	}

	return nil
}

// postJSON sends body as JSON and returns the HTTP status and body bytes.
func (c *Client) postJSON(ctx context.Context, url string, body any, bearer string) (status int, respBody []byte, err error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return 0, nil, fmt.Errorf("encode request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return 0, nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return 0, nil, ctx.Err()
		}

		return 0, nil, fmt.Errorf("%w: %w", usecase.ErrTransient, err)
	}
	defer resp.Body.Close()

	respBody, err = io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
	if err != nil {
		if ctx.Err() != nil {
			return 0, nil, ctx.Err()
		}

		return 0, nil, fmt.Errorf("%w: %w", usecase.ErrTransient, err)
	}

	return resp.StatusCode, respBody, nil
}
