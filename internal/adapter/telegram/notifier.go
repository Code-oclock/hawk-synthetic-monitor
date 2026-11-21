// Package telegram sends check alerts to the CodeX bot webhook.
package telegram

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Code-oclock/hawk-synthetic-monitor/internal/entity"
	"github.com/Code-oclock/hawk-synthetic-monitor/internal/usecase"
)

const (
	defaultTimeout = 10 * time.Second
	maxRunes       = 3500
)

var _ usecase.Notifier = (*Notifier)(nil)

type Notifier struct {
	url         string
	client      *http.Client
	retryDelays []time.Duration
}

// New creates a Telegram notifier targeting the given CodeX bot webhook URL.
func New(rawURL string, client *http.Client) *Notifier {
	if client == nil {
		client = &http.Client{
			Timeout: defaultTimeout,
		}
	}

	return &Notifier{
		url:         rawURL,
		client:      client,
		retryDelays: []time.Duration{time.Second, 2 * time.Second},
	}
}

// WithRetryDelays sets the waits between attempts; one more attempt is made per delay.
func (n *Notifier) WithRetryDelays(delays []time.Duration) *Notifier {
	n.retryDelays = delays

	return n
}

// SendText delivers a plain-text message to the CodeX bot webhook.
func (n *Notifier) SendText(ctx context.Context, text string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	runes := []rune(text)
	if len(runes) > maxRunes {
		text = string(runes[:maxRunes]) + "…"
	}

	form := url.Values{}
	form.Set("message", text)
	encodedForm := form.Encode()

	delays := n.retryDelays
	maxAttempts := 1 + len(delays)
	client := n.client
	if client == nil {
		client = &http.Client{Timeout: defaultTimeout}
	}

	var attemptErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, n.url, strings.NewReader(encodedForm))
		if err != nil {
			var urlErr *url.Error
			if errors.As(err, &urlErr) {
				return fmt.Errorf("create request: %w", urlErr.Err)
			}

			return fmt.Errorf("create request: %w", err)
		}
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		resp, err := client.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if errors.Is(err, context.Canceled) {
				return context.Canceled
			}

			var urlErr *url.Error
			if errors.As(err, &urlErr) {
				attemptErr = fmt.Errorf("send request: %w", urlErr.Err)
			} else {
				attemptErr = fmt.Errorf("send request: %w", err)
			}
		} else {
			if resp.Body != nil {
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
			}

			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return nil
			}

			attemptErr = fmt.Errorf("status %d", resp.StatusCode)
			if resp.StatusCode != http.StatusTooManyRequests && resp.StatusCode < 500 {
				return fmt.Errorf("telegram notification failed after %s: %w", formatAttempts(attempt), attemptErr)
			}
		}

		if attempt == maxAttempts {
			return fmt.Errorf("telegram notification failed after %s: %w", formatAttempts(attempt), attemptErr)
		}

		timer := time.NewTimer(delays[attempt-1])
		select {
		case <-ctx.Done():
			timer.Stop()

			return ctx.Err()
		case <-timer.C:
		}
	}

	return fmt.Errorf("telegram notification failed after %s: %w", formatAttempts(maxAttempts), attemptErr)
}

// Notify delivers the alert to the CodeX bot webhook.
func (n *Notifier) Notify(ctx context.Context, alert entity.Alert) error {
	return n.SendText(ctx, FormatMessage(alert))
}

func formatAttempts(n int) string {
	if n == 1 {
		return "1 attempt"
	}

	return fmt.Sprintf("%d attempts", n)
}

// FormatMessage formats an alert into a plain-text Telegram message.
func FormatMessage(alert entity.Alert) string {
	var lines []string

	if alert.Kind == entity.AlertRecovery {
		lines = formatRecovery(alert.Result)
	} else {
		lines = formatFailureAlert(alert.Result)
	}

	msg := strings.Join(lines, "\n")

	runes := []rune(msg)
	if len(runes) > maxRunes {
		return string(runes[:maxRunes]) + "…"
	}

	return msg
}

func formatRun(res entity.RunResult) string {
	if res.Trigger != "" {
		return fmt.Sprintf("run: %s  (%s)", res.ID, res.Trigger)
	}

	return "run: " + res.ID
}

func formatFailureAlert(res entity.RunResult) []string {
	lines := []string{
		"🔴 Hawk check FAILED",
		formatRun(res),
	}

	for _, s := range res.Stages {
		lines = append(lines, formatStage(s))
	}

	lines = append(lines, "total: "+formatDuration(&res.Duration))

	for i, repErr := range res.ReportingErrors {
		if i >= 3 {
			break
		}
		if text := formatFailure(&repErr); text != "" {
			lines = append(lines, text)
		}
	}

	return lines
}

func formatRecovery(res entity.RunResult) []string {
	lines := []string{
		"🟢 Hawk check recovered",
		formatRun(res),
	}

	for _, s := range res.Stages {
		lines = append(lines, formatStage(s))
	}

	if res.VisibilityLatency != nil {
		lines = append(lines, "visible latency: "+formatDuration(res.VisibilityLatency))
	}
	if res.WebhookLatency != nil {
		lines = append(lines, "webhook latency: "+formatDuration(res.WebhookLatency))
	}

	return lines
}

func formatStage(s entity.StageResult) string {
	d := formatDuration(s.Duration)

	switch s.Status {
	case entity.StatusPassed:
		return fmt.Sprintf("%s: passed %s", s.Stage, d)
	case entity.StatusFailed:
		fail := formatFailure(s.Failure)
		if fail != "" {
			return fmt.Sprintf("%s: failed %s — %s", s.Stage, d, fail)
		}

		return fmt.Sprintf("%s: failed %s", s.Stage, d)
	case entity.StatusSkipped:
		if s.SkipReason != "" {
			return fmt.Sprintf("%s: skipped — %s", s.Stage, s.SkipReason)
		}

		return fmt.Sprintf("%s: skipped", s.Stage)
	default:
		return fmt.Sprintf("%s: %s %s", s.Stage, s.Status, d)
	}
}

func formatFailure(f *entity.Failure) string {
	if f == nil {
		return ""
	}
	if f.Code != "" && f.Message != "" {
		return fmt.Sprintf("%s: %s", f.Code, f.Message)
	}
	if f.Code != "" {
		return f.Code
	}

	return f.Message
}

func formatDuration(d *time.Duration) string {
	if d == nil {
		return "-"
	}

	return d.Round(time.Millisecond).String()
}
