package hawk

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type eventNode struct {
	ID         string `json:"id"`
	TotalCount int    `json:"totalCount"`
	Payload    struct {
		Title string `json:"title"`
	} `json:"payload"`
}

// FindEvent looks for an event with exactly this title among the latest events of the project.
func (c *Client) FindEvent(ctx context.Context, title string) (*Event, error) {
	nodes, err := c.latestEvents(ctx)
	if err != nil {
		return nil, err
	}

	for _, node := range nodes {
		if node.Payload.Title == title {
			return &Event{ID: node.ID, Title: node.Payload.Title, TotalCount: node.TotalCount}, nil
		}
	}

	return nil, ErrNotFound
}

// WaitForEvent polls FindEvent every interval until the event appears or ctx ends.
// Any error other than ErrNotFound is returned immediately.
func (c *Client) WaitForEvent(ctx context.Context, title string, interval time.Duration) (*Event, error) {
	if interval <= 0 {
		return nil, errors.New("hawk: poll interval must be positive")
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		event, err := c.FindEvent(ctx, title)
		if !errors.Is(err, ErrNotFound) {
			return event, err
		}

		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("wait for event %q: %w", title, ctx.Err())
		case <-ticker.C:
		}
	}
}

// latestEvents retries once with a fresh login: access tokens live only 15 minutes.
func (c *Client) latestEvents(ctx context.Context) ([]eventNode, error) {
	for attempt := 0; ; attempt++ {
		token, err := c.accessToken(ctx)
		if err != nil {
			return nil, err
		}

		nodes, err := c.fetchEvents(ctx, token)
		if attempt == 0 && isUnauthenticated(err) {
			c.dropToken(token)

			continue
		}

		return nodes, err
	}
}

func (c *Client) fetchEvents(ctx context.Context, token string) ([]eventNode, error) {
	var out struct {
		Project struct {
			Events []eventNode `json:"events"`
		} `json:"project"`
	}
	const q = `query($projectId: ID!) {
		project(projectId: $projectId) { events(limit: 20) { id totalCount payload { title } } }
	}`
	if err := c.query(ctx, q, map[string]any{"projectId": c.cfg.ProjectID}, token, &out); err != nil {
		return nil, fmt.Errorf("fetch events: %w", err)
	}

	return out.Project.Events, nil
}
