package hawk

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

const defaultTimeout = 10 * time.Second

type Config struct {
	CollectorURL     string
	APIURL           string
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

// postJSON sends body as JSON and decodes the response into out, whatever the status code is.
func (c *Client) postJSON(ctx context.Context, url string, body any, bearer string, out any) (int, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return 0, fmt.Errorf("encode request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return 0, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	// A non-JSON body (a proxy error page, for example) leaves out empty; the caller checks the status.
	_ = json.NewDecoder(resp.Body).Decode(out)

	return resp.StatusCode, nil
}
