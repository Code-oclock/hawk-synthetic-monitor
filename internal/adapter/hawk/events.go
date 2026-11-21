package hawk

import (
	"context"
	"fmt"

	"github.com/Code-oclock/hawk-synthetic-monitor/internal/entity"
	"github.com/Code-oclock/hawk-synthetic-monitor/internal/usecase"
)

type eventNode struct {
	ID          string `json:"id"`
	TotalCount  int    `json:"totalCount"`
	CatcherType string `json:"catcherType"`
	Payload     struct {
		Title string `json:"title"`
		Type  string `json:"type"`
	} `json:"payload"`
}

// FindEvent looks for an event with exactly this title among the latest events of the project.
func (c *Client) FindEvent(ctx context.Context, title string) (entity.Event, error) {
	nodes, err := c.latestEvents(ctx)
	if err != nil {
		return entity.Event{}, err
	}

	for _, node := range nodes {
		if node.Payload.Title == title {
			if node.ID == "" {
				return entity.Event{}, fmt.Errorf("%w: event has empty id", ErrProtocol)
			}

			return entity.Event{
				ProjectID:   c.cfg.ProjectID,
				ID:          node.ID,
				Title:       node.Payload.Title,
				CatcherType: node.CatcherType,
				Type:        node.Payload.Type,
				TotalCount:  node.TotalCount,
			}, nil
		}
	}

	return entity.Event{}, usecase.ErrNotFound
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
		Project *struct {
			Events []eventNode `json:"events"`
		} `json:"project"`
	}
	const q = `query($projectId: ID!) {
		project(projectId: $projectId) { events(limit: 20) { id totalCount catcherType payload { title type } } }
	}`
	if err := c.query(ctx, q, map[string]any{"projectId": c.cfg.ProjectID}, token, &out); err != nil {
		return nil, fmt.Errorf("fetch events: %w", err)
	}

	if out.Project == nil {
		return nil, fmt.Errorf("%w: project not found", ErrProtocol)
	}

	return out.Project.Events, nil
}
