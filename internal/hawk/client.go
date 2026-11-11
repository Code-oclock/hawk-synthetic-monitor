package hawk

import (
	"context"
	"net/http"
	"time"
)

type Config struct {
	CollectorURL     string
	APIURL           string
	IntegrationToken string
	ProjectID        string
	Email            string
	Password         string
	HTTPClient       *http.Client
}

type Client struct{}

func New(cfg Config) *Client {
	panic("not implemented")
}

// SendEvent posts a test event to the collector. A rejection is returned as *CollectorError.
func (c *Client) SendEvent(ctx context.Context, title string) error {
	panic("not implemented")
}

// FindEvent looks for an event with exactly this title among the latest events of the project.
func (c *Client) FindEvent(ctx context.Context, title string) (*Event, error) {
	panic("not implemented")
}

// WaitForEvent polls FindEvent every interval until the event appears or ctx ends.
// Any error other than ErrNotFound is returned immediately.
func (c *Client) WaitForEvent(ctx context.Context, title string, interval time.Duration) (*Event, error) {
	panic("not implemented")
}
