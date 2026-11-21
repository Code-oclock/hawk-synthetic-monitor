package heartbeat_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Code-oclock/hawk-synthetic-monitor/internal/adapter/heartbeat"
	"github.com/Code-oclock/hawk-synthetic-monitor/internal/entity"
)

func TestPinger_Ping_BodyShape(t *testing.T) {
	t.Parallel()

	var (
		gotMethod      string
		gotContentType string
		gotBody        map[string]string
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotContentType = r.Header.Get("Content-Type")

		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
		}
		if err := json.Unmarshal(bodyBytes, &gotBody); err != nil {
			t.Errorf("unmarshal body: %v", err)
		}

		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	finished := time.Date(2026, 11, 22, 1, 30, 0, 0, time.UTC)
	p := heartbeat.New(srv.URL, srv.Client())
	err := p.Ping(context.Background(), entity.RunResult{
		ID:         "run-123",
		Status:     entity.StatusPassed,
		FinishedAt: finished,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if gotMethod != http.MethodPost {
		t.Errorf("method = %q, want %q", gotMethod, http.MethodPost)
	}
	if gotContentType != "application/json" {
		t.Errorf("content-type = %q, want application/json", gotContentType)
	}
	if gotBody["status"] != "passed" {
		t.Errorf("status = %q, want passed", gotBody["status"])
	}
	if gotBody["run_id"] != "run-123" {
		t.Errorf("run_id = %q, want run-123", gotBody["run_id"])
	}
	if gotBody["finished_at"] != "2026-11-22T01:30:00Z" {
		t.Errorf("finished_at = %q, want 2026-11-22T01:30:00Z", gotBody["finished_at"])
	}
}

func TestPinger_Ping_Success200And204(t *testing.T) {
	t.Parallel()

	for _, code := range []int{http.StatusOK, http.StatusNoContent} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(code)
		}))

		p := heartbeat.New(srv.URL, srv.Client())
		err := p.Ping(context.Background(), entity.RunResult{
			ID:     "run-ok",
			Status: entity.StatusPassed,
		})
		srv.Close()

		if err != nil {
			t.Errorf("code %d: unexpected error %v", code, err)
		}
	}
}

func TestPinger_Ping_HTTPError500(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	p := heartbeat.New(srv.URL, srv.Client())
	err := p.Ping(context.Background(), entity.RunResult{
		ID:     "run-err",
		Status: entity.StatusFailed,
	})
	if err == nil {
		t.Fatal("expected error for 500, got nil")
	}
	if !strings.Contains(err.Error(), "500") {
		t.Errorf("error %q should mention 500", err.Error())
	}
	if strings.Contains(err.Error(), srv.URL) {
		t.Errorf("error %q must not leak server URL", err.Error())
	}
}

func TestPinger_Ping_CanceledContext(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	p := heartbeat.New(srv.URL, srv.Client())
	err := p.Ping(ctx, entity.RunResult{
		ID:     "run-canceled",
		Status: entity.StatusPassed,
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got err = %v, want context.Canceled", err)
	}
}

func TestPinger_Ping_NetworkError(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	targetURL := srv.URL
	srv.Close()

	p := heartbeat.New(targetURL, &http.Client{Timeout: 500 * time.Millisecond})
	err := p.Ping(context.Background(), entity.RunResult{
		ID:     "run-unreachable",
		Status: entity.StatusPassed,
	})
	if err == nil {
		t.Fatal("expected error for unreachable server, got nil")
	}
	if strings.Contains(err.Error(), targetURL) {
		t.Errorf("error %q must not leak server URL", err.Error())
	}
}

func TestPinger_New_NilClient(t *testing.T) {
	t.Parallel()

	p := heartbeat.New("http://example.com/ping", nil)
	if p == nil {
		t.Fatal("expected non-nil Pinger")
	}
}
