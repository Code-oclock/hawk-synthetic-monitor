package hawk

import (
	"context"
	"fmt"
	"net/http"
)

type collectorRequest struct {
	Token       string       `json:"token"`
	CatcherType string       `json:"catcherType"`
	Payload     eventPayload `json:"payload"`
}

type eventPayload struct {
	Title     string           `json:"title"`
	Type      string           `json:"type"`
	Backtrace []backtraceFrame `json:"backtrace"`
}

type backtraceFrame struct {
	File     string `json:"file"`
	Line     int    `json:"line"`
	Function string `json:"function"`
}

type collectorResponse struct {
	Code    int    `json:"code"`
	Error   bool   `json:"error"`
	Message string `json:"message"`
}

// SendEvent posts a test event to the collector. A rejection is returned as *CollectorError.
func (c *Client) SendEvent(ctx context.Context, title string) error {
	req := collectorRequest{
		Token:       c.cfg.IntegrationToken,
		CatcherType: "errors/golang",
		Payload: eventPayload{
			Title:     title,
			Type:      "error",
			Backtrace: []backtraceFrame{{File: "main.go", Line: 1, Function: "main"}},
		},
	}

	var resp collectorResponse
	status, err := c.postJSON(ctx, c.cfg.CollectorURL, req, "", &resp)
	if err != nil {
		return fmt.Errorf("send event: %w", err)
	}

	if status == http.StatusOK && !resp.Error {
		return nil
	}

	rejection := &CollectorError{Code: resp.Code, Message: resp.Message}
	if rejection.Code == 0 {
		rejection.Code = status
	}
	if rejection.Message == "" {
		rejection.Message = http.StatusText(status)
	}

	return rejection
}
