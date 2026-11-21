package hawk

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/Code-oclock/hawk-synthetic-monitor/internal/usecase"
)

const (
	catcherType = "errors/golang"
	eventType   = "error"
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
	Error   *bool  `json:"error"`
	Message string `json:"message"`
}

// SendEvent posts a test event to the collector.
func (c *Client) SendEvent(ctx context.Context, title string) error {
	req := collectorRequest{
		Token:       c.cfg.IntegrationToken,
		CatcherType: catcherType,
		Payload: eventPayload{
			Title:     title,
			Type:      eventType,
			Backtrace: []backtraceFrame{{File: "main.go", Line: 1, Function: "main"}},
		},
	}

	status, body, err := c.postJSON(ctx, c.cfg.CollectorURL, req, "")
	if err != nil {
		return err
	}

	if status == http.StatusOK {
		if bytes.Equal(bytes.TrimSpace(body), []byte("null")) {
			return fmt.Errorf("%w: collector response is null", ErrProtocol)
		}

		var resp collectorResponse
		if err := json.Unmarshal(body, &resp); err != nil {
			return fmt.Errorf("%w: collector response not json: %w", ErrProtocol, err)
		}

		if resp.Error == nil {
			return fmt.Errorf("%w: collector response missing error field", ErrProtocol)
		}

		if !*resp.Error {
			return nil
		}

		rejection := &CollectorError{Code: resp.Code, Message: resp.Message}
		if rejection.Code == 0 {
			rejection.Code = http.StatusOK
		}
		if rejection.Message == "" {
			rejection.Message = http.StatusText(http.StatusOK)
		}

		return rejection
	}

	var resp collectorResponse
	var rejection *CollectorError
	if err := json.Unmarshal(body, &resp); err == nil && (resp.Code != 0 || resp.Message != "") {
		rejection = &CollectorError{Code: resp.Code, Message: resp.Message}
		if rejection.Code == 0 {
			rejection.Code = status
		}
		if rejection.Message == "" {
			rejection.Message = http.StatusText(status)
		}
	} else {
		rejection = &CollectorError{Code: status, Message: http.StatusText(status)}
	}

	if status == http.StatusTooManyRequests || (status >= 500 && status <= 599) {
		return fmt.Errorf("%w: %w", usecase.ErrTransient, rejection)
	}

	return rejection
}
