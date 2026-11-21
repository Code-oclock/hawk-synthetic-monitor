// Package rabbitmq inspects RabbitMQ queues via the management HTTP API.
package rabbitmq

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"

	"github.com/Code-oclock/hawk-synthetic-monitor/internal/entity"
	"github.com/Code-oclock/hawk-synthetic-monitor/internal/usecase"
)

const (
	maxErrorBytes    = 1024
	maxResponseBytes = 4 * 1024 * 1024
)

var _ usecase.QueueInspector = (*Client)(nil)

// Config holds configuration for the RabbitMQ client.
type Config struct {
	BaseURL  string
	User     string
	Password string
	Vhost    string
	Queues   []string
}

// Client inspects RabbitMQ queues via the management HTTP API.
type Client struct {
	cfg    Config
	client *http.Client
}

type queueResponse struct {
	Name      string `json:"name"`
	Messages  int    `json:"messages"`
	Consumers int    `json:"consumers"`
}

// New creates a RabbitMQ queue inspector client.
func New(cfg Config, client *http.Client) *Client {
	if cfg.Vhost == "" {
		cfg.Vhost = "/"
	}
	if client == nil {
		client = http.DefaultClient
	}

	cfgCopy := cfg
	if cfg.Queues != nil {
		cfgCopy.Queues = slices.Clone(cfg.Queues)
	}

	return &Client{
		cfg:    cfgCopy,
		client: client,
	}
}

// Queues returns queue statistics for configured queues.
func (c *Client) Queues(ctx context.Context) ([]entity.QueueStat, error) {
	u, err := url.Parse(c.cfg.BaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse base url: %w", c.sanitizeErr(err))
	}
	u.User = nil

	vhost := c.cfg.Vhost
	if vhost == "" {
		vhost = "/"
	}

	escapedVhost := url.PathEscape(vhost)
	basePath := strings.TrimRight(u.Path, "/")
	baseRawPath := strings.TrimRight(u.EscapedPath(), "/")

	u.Path = basePath + "/api/queues/" + vhost
	u.RawPath = baseRawPath + "/api/queues/" + escapedVhost
	u.RawQuery = "columns=name,messages,consumers"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", c.sanitizeErr(err))
	}
	req.URL = u
	req.SetBasicAuth(c.cfg.User, c.cfg.Password)

	client := c.client
	if client == nil {
		client = http.DefaultClient
	}

	resp, err := client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		return nil, fmt.Errorf("send request: %w", c.sanitizeErr(err))
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBytes))
		msg := strings.TrimSpace(string(body))
		if c.cfg.Password != "" && strings.Contains(msg, c.cfg.Password) {
			msg = strings.ReplaceAll(msg, c.cfg.Password, "[REDACTED]")
		}
		if msg != "" {
			return nil, fmt.Errorf("rabbitmq status %d: %s", resp.StatusCode, msg)
		}

		return nil, fmt.Errorf("rabbitmq status %d", resp.StatusCode)
	}

	var items []queueResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes)).Decode(&items); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	if len(c.cfg.Queues) > 0 {
		byName := make(map[string]queueResponse, len(items))
		for _, item := range items {
			byName[item.Name] = item
		}

		stats := make([]entity.QueueStat, len(c.cfg.Queues))
		for i, name := range c.cfg.Queues {
			item, ok := byName[name]
			if !ok {
				stats[i] = entity.QueueStat{
					Name:    name,
					Missing: true,
				}

				continue
			}

			stats[i] = entity.QueueStat{
				Name:      name,
				Messages:  item.Messages,
				Consumers: item.Consumers,
				Missing:   false,
			}
		}

		return stats, nil
	}

	stats := make([]entity.QueueStat, len(items))
	for i, item := range items {
		stats[i] = entity.QueueStat{
			Name:      item.Name,
			Messages:  item.Messages,
			Consumers: item.Consumers,
			Missing:   false,
		}
	}

	slices.SortFunc(stats, func(a, b entity.QueueStat) int {
		return cmp.Compare(a.Name, b.Name)
	})

	return stats, nil
}

// sanitizeErr strips the configured password from an error message.
func (c *Client) sanitizeErr(err error) error {
	if err == nil || c.cfg.Password == "" {
		return err
	}
	if strings.Contains(err.Error(), c.cfg.Password) {
		return errors.New(strings.ReplaceAll(err.Error(), c.cfg.Password, "[REDACTED]"))
	}

	return err
}
