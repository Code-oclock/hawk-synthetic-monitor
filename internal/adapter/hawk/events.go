package hawk

import (
	"context"
	"errors"
	"fmt"

	"github.com/Code-oclock/hawk-synthetic-monitor/internal/entity"
	"github.com/Code-oclock/hawk-synthetic-monitor/internal/usecase"
)

var _ usecase.Cleaner = (*Client)(nil)

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

			if node.CatcherType != catcherType || node.Payload.Type != eventType {
				return entity.Event{}, fmt.Errorf("%w: event has catcherType %q and type %q", ErrProtocol, node.CatcherType, node.Payload.Type)
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

// RemoveEvent deletes the event group through the GraphQL API. It needs a project admin.
func (c *Client) RemoveEvent(ctx context.Context, event entity.Event) error {
	if event.ID == "" {
		return errors.New("empty event id")
	}

	projectID := event.ProjectID
	if projectID == "" {
		projectID = c.cfg.ProjectID
	}

	return c.withToken(ctx, func(token string) error {
		return c.removeEvent(ctx, projectID, event.ID, token)
	})
}

func (c *Client) removeEvent(ctx context.Context, projectID, eventID, token string) error {
	var out struct {
		RemoveEvent *bool `json:"removeEvent"`
	}
	const mutation = `mutation($projectId: ID!, $eventId: ID!) { removeEvent(projectId: $projectId, eventId: $eventId) }`
	vars := map[string]any{
		"projectId": projectID,
		"eventId":   eventID,
	}
	if err := c.query(ctx, mutation, vars, token, &out); err != nil {
		return fmt.Errorf("remove event: %w", err)
	}

	if out.RemoveEvent == nil {
		return fmt.Errorf("%w: missing removeEvent in response", ErrProtocol)
	}
	if !*out.RemoveEvent {
		return fmt.Errorf("%w: remove event returned false", ErrProtocol)
	}

	return nil
}

// latestEvents retries once with a fresh login: access tokens live only 15 minutes.
func (c *Client) latestEvents(ctx context.Context) ([]eventNode, error) {
	var nodes []eventNode
	err := c.withToken(ctx, func(token string) error {
		var err error
		nodes, err = c.fetchEvents(ctx, token)

		return err
	})
	if err != nil {
		return nil, err
	}

	return nodes, nil
}

func (c *Client) withToken(ctx context.Context, fn func(token string) error) error {
	for attempt := 0; ; attempt++ {
		token, err := c.accessToken(ctx)
		if err != nil {
			return err
		}

		err = fn(token)
		if attempt == 0 && isUnauthenticated(err) {
			c.dropToken(token)

			continue
		}

		return err
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
