package telegram_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/Code-oclock/hawk-synthetic-monitor/internal/adapter/telegram"
	"github.com/Code-oclock/hawk-synthetic-monitor/internal/entity"
)

func newNotifier(url string, client *http.Client) *telegram.Notifier {
	return telegram.New(url, client).WithRetryDelays([]time.Duration{time.Millisecond, 2 * time.Millisecond})
}

func TestNotifier_Notify_CanceledWhileWaiting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	firstReqDone := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		select {
		case <-firstReqDone:
		default:
			close(firstReqDone)
		}
	}))
	defer srv.Close()

	go func() {
		<-firstReqDone
		time.Sleep(15 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	n := telegram.New(srv.URL, srv.Client()).WithRetryDelays([]time.Duration{time.Second, time.Second})
	err := n.Notify(ctx, entity.Alert{
		Kind: entity.AlertFailure,
		Result: entity.RunResult{
			ID: "run-cancel-wait",
		},
	})
	elapsed := time.Since(start)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got err = %v, want context.Canceled", err)
	}
	if elapsed >= 500*time.Millisecond {
		t.Fatalf("expected fast cancellation, took %v", elapsed)
	}
}

func TestNotifier_Notify_Retry500Then200(t *testing.T) {
	t.Parallel()

	var reqCount int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := atomic.AddInt32(&reqCount, 1)
		if count == 1 {
			w.WriteHeader(http.StatusInternalServerError)

			return
		}

		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	n := newNotifier(srv.URL, srv.Client())
	err := n.Notify(context.Background(), entity.Alert{
		Kind: entity.AlertFailure,
		Result: entity.RunResult{
			ID: "run-retry-500",
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := atomic.LoadInt32(&reqCount); got != 2 {
		t.Errorf("request count = %d, want 2", got)
	}
}

func TestNotifier_Notify_NoRetryOn404(t *testing.T) {
	t.Parallel()

	var reqCount int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&reqCount, 1)
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	n := newNotifier(srv.URL, srv.Client())
	err := n.Notify(context.Background(), entity.Alert{
		Kind: entity.AlertFailure,
		Result: entity.RunResult{
			ID: "run-404",
		},
	})
	if err == nil {
		t.Fatal("expected error for 404, got nil")
	}
	if got := atomic.LoadInt32(&reqCount); got != 1 {
		t.Errorf("request count = %d, want 1", got)
	}
}

func TestNotifier_Notify_Persistent502(t *testing.T) {
	t.Parallel()

	var reqCount int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&reqCount, 1)
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()

	n := newNotifier(srv.URL, srv.Client())
	err := n.Notify(context.Background(), entity.Alert{
		Kind: entity.AlertFailure,
		Result: entity.RunResult{
			ID: "run-persistent-502",
		},
	})
	if err == nil {
		t.Fatal("expected error for persistent 502, got nil")
	}
	if got := atomic.LoadInt32(&reqCount); got != 3 {
		t.Errorf("request count = %d, want 3", got)
	}
	if !strings.Contains(err.Error(), "3 attempts") {
		t.Errorf("error %q should mention '3 attempts'", err.Error())
	}
	if strings.Contains(err.Error(), srv.URL) {
		t.Errorf("error %q must not leak server URL", err.Error())
	}
}

func TestNotifier_Notify_NetworkErrorThenSuccess(t *testing.T) {
	t.Parallel()

	var reqCount int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := atomic.AddInt32(&reqCount, 1)
		if count == 1 {
			hj, ok := w.(http.Hijacker)
			if !ok {
				t.Errorf("server does not support hijacking")

				return
			}

			conn, _, err := hj.Hijack()
			if err != nil {
				t.Errorf("hijack failed: %v", err)

				return
			}

			_ = conn.Close()

			return
		}

		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	n := newNotifier(srv.URL, srv.Client())
	err := n.Notify(context.Background(), entity.Alert{
		Kind: entity.AlertFailure,
		Result: entity.RunResult{
			ID: "run-hijack-retry",
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := atomic.LoadInt32(&reqCount); got != 2 {
		t.Errorf("request count = %d, want 2", got)
	}
}

func TestNotifier_Notify_FormBody(t *testing.T) {
	t.Parallel()

	var (
		gotMethod      string
		gotContentType string
		gotMessage     string
		hasParseMode   bool
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotContentType = r.Header.Get("Content-Type")

		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
		}

		// Re-assign body so ParseForm can read it if needed.
		r.Body = io.NopCloser(strings.NewReader(string(bodyBytes)))
		if err := r.ParseForm(); err != nil {
			t.Errorf("parse form: %v", err)
		}

		gotMessage = r.PostForm.Get("message")
		hasParseMode = r.PostForm.Has("parse_mode")

		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	n := newNotifier(srv.URL, srv.Client())

	alert := entity.Alert{
		Kind: entity.AlertRecovery,
		Result: entity.RunResult{
			ID:      "run-ok",
			Trigger: entity.TriggerScheduled,
		},
	}

	if err := n.Notify(context.Background(), alert); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if gotMethod != http.MethodPost {
		t.Errorf("method = %q, want %q", gotMethod, http.MethodPost)
	}
	if !strings.HasPrefix(gotContentType, "application/x-www-form-urlencoded") {
		t.Errorf("content-type = %q, want application/x-www-form-urlencoded", gotContentType)
	}
	if !strings.Contains(gotMessage, "🟢 Hawk check recovered") {
		t.Errorf("message = %q, want recovery header", gotMessage)
	}
	if hasParseMode {
		t.Errorf("request unexpectedly contained parse_mode")
	}
}

func TestNotifier_Notify_HTTPStatusErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		statusCode int
		wantSubstr string
	}{
		{
			name:       "500 Internal Server Error",
			statusCode: http.StatusInternalServerError,
			wantSubstr: "500",
		},
		{
			name:       "404 Not Found",
			statusCode: http.StatusNotFound,
			wantSubstr: "404",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.statusCode)
			}))
			defer srv.Close()

			n := newNotifier(srv.URL, srv.Client())
			err := n.Notify(context.Background(), entity.Alert{
				Kind: entity.AlertFailure,
				Result: entity.RunResult{
					ID: "run-err",
				},
			})

			if err == nil {
				t.Fatalf("expected error for status %d, got nil", tt.statusCode)
			}
			if !strings.Contains(err.Error(), tt.wantSubstr) {
				t.Errorf("error %q should mention status code %q", err.Error(), tt.wantSubstr)
			}
			if strings.Contains(err.Error(), srv.URL) {
				t.Errorf("error %q must not leak server URL", err.Error())
			}
		})
	}
}

func TestNotifier_Notify_UnreachableServer(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	targetURL := srv.URL
	srv.Close()

	n := newNotifier(targetURL, &http.Client{Timeout: 500 * time.Millisecond})
	err := n.Notify(context.Background(), entity.Alert{
		Kind: entity.AlertFailure,
		Result: entity.RunResult{
			ID: "run-unreachable",
		},
	})

	if err == nil {
		t.Fatal("expected error for unreachable server, got nil")
	}
	if strings.Contains(err.Error(), targetURL) {
		t.Errorf("network error %q must not leak server URL", err.Error())
	}
}

func TestNotifier_Notify_CanceledContext(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	n := newNotifier(srv.URL, srv.Client())
	err := n.Notify(ctx, entity.Alert{
		Kind: entity.AlertFailure,
		Result: entity.RunResult{
			ID: "run-canceled",
		},
	})

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got err = %v, want context.Canceled", err)
	}
}

func TestNotifier_New_NilClient(t *testing.T) {
	t.Parallel()

	n := newNotifier("https://example.com/secret", nil)
	if n == nil {
		t.Fatal("expected non-nil Notifier")
	}
}

func TestFormatMessage_Failure(t *testing.T) {
	t.Parallel()

	healthDur := 5 * time.Millisecond
	sendDur := 2 * time.Second
	totalDur := 2005 * time.Millisecond

	alert := entity.Alert{
		Kind: entity.AlertFailure,
		Result: entity.RunResult{
			ID:       "run-42",
			Trigger:  entity.TriggerScheduled,
			Duration: totalDur,
			Stages: []entity.StageResult{
				{
					Stage:    entity.StageHealth,
					Status:   entity.StatusPassed,
					Duration: &healthDur,
				},
				{
					Stage:    entity.StageSend,
					Status:   entity.StatusFailed,
					Duration: &sendDur,
					Failure: &entity.Failure{
						Code:    "timeout",
						Message: "deadline exceeded",
					},
				},
				{
					Stage:      entity.StageVisible,
					Status:     entity.StatusSkipped,
					SkipReason: "send failed",
				},
			},
			ReportingErrors: []entity.Failure{
				{Code: "error", Message: "save: disk full"},
				{Code: "unavailable", Message: "metrics: connection refused"},
			},
		},
	}

	got := telegram.FormatMessage(alert)
	want := strings.Join([]string{
		"🔴 Hawk check FAILED",
		"run: run-42  (scheduled)",
		"health: passed 5ms",
		"send: failed 2s — timeout: deadline exceeded",
		"visible: skipped — send failed",
		"total: 2.005s",
		"error: save: disk full",
		"unavailable: metrics: connection refused",
	}, "\n")

	if got != want {
		t.Errorf("FormatMessage() mismatch:\ngot:\n%s\nwant:\n%s", got, want)
	}
}

func TestFormatMessage_Failure_NilDuration(t *testing.T) {
	t.Parallel()

	alert := entity.Alert{
		Kind: entity.AlertFailure,
		Result: entity.RunResult{
			ID: "run-nil-dur",
			Stages: []entity.StageResult{
				{
					Stage:    entity.StageHealth,
					Status:   entity.StatusPassed,
					Duration: nil,
				},
				{
					Stage:    entity.StageSend,
					Status:   entity.StatusFailed,
					Duration: nil,
					Failure: &entity.Failure{
						Code:    "fail_code",
						Message: "fail_msg",
					},
				},
				{
					Stage:      entity.StageVisible,
					Status:     entity.StatusSkipped,
					SkipReason: "skipped_reason",
				},
			},
		},
	}

	got := telegram.FormatMessage(alert)
	want := strings.Join([]string{
		"🔴 Hawk check FAILED",
		"run: run-nil-dur",
		"health: passed -",
		"send: failed - — fail_code: fail_msg",
		"visible: skipped — skipped_reason",
		"total: 0s",
	}, "\n")

	if got != want {
		t.Errorf("FormatMessage() mismatch:\ngot:\n%s\nwant:\n%s", got, want)
	}
}

func TestFormatMessage_Failure_ReportingErrorsLimit(t *testing.T) {
	t.Parallel()

	alert := entity.Alert{
		Kind: entity.AlertFailure,
		Result: entity.RunResult{
			ID: "run-rep-errs",
			ReportingErrors: []entity.Failure{
				{Code: "err1", Message: "msg1"},
				{Code: "err2", Message: "msg2"},
				{Code: "err3", Message: "msg3"},
				{Code: "err4", Message: "msg4"},
				{Code: "err5", Message: "msg5"},
			},
		},
	}

	got := telegram.FormatMessage(alert)

	if !strings.Contains(got, "err1: msg1") ||
		!strings.Contains(got, "err2: msg2") ||
		!strings.Contains(got, "err3: msg3") {
		t.Errorf("expected first 3 reporting errors, got:\n%s", got)
	}
	if strings.Contains(got, "err4: msg4") || strings.Contains(got, "err5: msg5") {
		t.Errorf("reporting errors beyond first 3 must be omitted, got:\n%s", got)
	}
}

func TestFormatMessage_Recovery(t *testing.T) {
	t.Parallel()

	healthDur := 5 * time.Millisecond
	sendDur := 20 * time.Millisecond
	visDur := 150 * time.Millisecond
	visLat := 125 * time.Millisecond
	webDur := 300 * time.Millisecond
	webLat := 280 * time.Millisecond

	alert := entity.Alert{
		Kind: entity.AlertRecovery,
		Result: entity.RunResult{
			ID:                "run-rec-1",
			Trigger:           entity.TriggerManual,
			VisibilityLatency: &visLat,
			WebhookLatency:    &webLat,
			Stages: []entity.StageResult{
				{
					Stage:    entity.StageHealth,
					Status:   entity.StatusPassed,
					Duration: &healthDur,
				},
				{
					Stage:    entity.StageSend,
					Status:   entity.StatusPassed,
					Duration: &sendDur,
				},
				{
					Stage:    entity.StageVisible,
					Status:   entity.StatusPassed,
					Duration: &visDur,
				},
				{
					Stage:    entity.StageWebhook,
					Status:   entity.StatusPassed,
					Duration: &webDur,
				},
			},
		},
	}

	got := telegram.FormatMessage(alert)
	want := strings.Join([]string{
		"🟢 Hawk check recovered",
		"run: run-rec-1  (manual)",
		"health: passed 5ms",
		"send: passed 20ms",
		"visible: passed 150ms",
		"webhook: passed 300ms",
		"visible latency: 125ms",
		"webhook latency: 280ms",
	}, "\n")

	if got != want {
		t.Errorf("FormatMessage() mismatch:\ngot:\n%s\nwant:\n%s", got, want)
	}
}

func TestFormatMessage_Recovery_NilLatencies(t *testing.T) {
	t.Parallel()

	healthDur := 10 * time.Millisecond

	alert := entity.Alert{
		Kind: entity.AlertRecovery,
		Result: entity.RunResult{
			ID: "run-rec-nolat",
			Stages: []entity.StageResult{
				{
					Stage:    entity.StageHealth,
					Status:   entity.StatusPassed,
					Duration: &healthDur,
				},
			},
		},
	}

	got := telegram.FormatMessage(alert)
	want := strings.Join([]string{
		"🟢 Hawk check recovered",
		"run: run-rec-nolat",
		"health: passed 10ms",
	}, "\n")

	if got != want {
		t.Errorf("FormatMessage() mismatch:\ngot:\n%s\nwant:\n%s", got, want)
	}
}

func TestFormatMessage_Truncation(t *testing.T) {
	t.Parallel()

	longReason := strings.Repeat("абвг", 1000) // 4000 Cyrillic runes (8000 bytes)

	alert := entity.Alert{
		Kind: entity.AlertFailure,
		Result: entity.RunResult{
			ID: "run-long",
			Stages: []entity.StageResult{
				{
					Stage:      entity.StageVisible,
					Status:     entity.StatusSkipped,
					SkipReason: longReason,
				},
			},
		},
	}

	got := telegram.FormatMessage(alert)

	if !utf8.ValidString(got) {
		t.Fatal("truncated message is not valid UTF-8")
	}
	if !strings.HasSuffix(got, "…") {
		t.Errorf("expected suffix '…', got %q", got[len(got)-10:])
	}
	if runeCount := utf8.RuneCountInString(got); runeCount != 3501 {
		t.Errorf("expected 3501 runes (3500 runes + ellipsis), got %d", runeCount)
	}
}
