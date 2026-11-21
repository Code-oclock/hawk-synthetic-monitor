package watchdog_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Code-oclock/hawk-synthetic-monitor/internal/watchdog"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(start time.Time) *fakeClock {
	return &fakeClock{now: start}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.now = c.now.Add(d)
}

type recordingNotify struct {
	mu       sync.Mutex
	messages []string
	err      error
}

func (r *recordingNotify) Notify(_ context.Context, text string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.err != nil {
		return r.err
	}
	r.messages = append(r.messages, text)

	return nil
}

func (r *recordingNotify) Messages() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make([]string, len(r.messages))
	copy(out, r.messages)

	return out
}

func (r *recordingNotify) SetError(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.err = err
}

func TestWatchdog_NoAlertBeforeTimeout(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)
	clock := newFakeClock(start)
	rec := &recordingNotify{}

	cfg := watchdog.Config{
		Timeout:    10 * time.Minute,
		CheckEvery: 30 * time.Second,
	}
	w := watchdog.New(cfg, rec.Notify, clock.Now)

	clock.Advance(9 * time.Minute)
	w.Check(context.Background())

	if len(rec.Messages()) != 0 {
		t.Fatalf("expected 0 messages before timeout, got: %v", rec.Messages())
	}

	handler := w.Handler()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/ping", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("ping status = %d, want 200", rr.Code)
	}

	clock.Advance(9 * time.Minute)
	w.Check(context.Background())

	if len(rec.Messages()) != 0 {
		t.Fatalf("expected 0 messages after fresh ping, got: %v", rec.Messages())
	}
}

func TestWatchdog_SilentAfterTimeout_NeverPinged(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)
	clock := newFakeClock(start)
	rec := &recordingNotify{}

	cfg := watchdog.Config{
		Timeout:    10 * time.Minute,
		CheckEvery: 30 * time.Second,
	}
	w := watchdog.New(cfg, rec.Notify, clock.Now)

	clock.Advance(11 * time.Minute)
	w.Check(context.Background())

	msgs := rec.Messages()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got: %v", msgs)
	}

	msg := msgs[0]
	if !strings.Contains(msg, "⚠️ Hawk monitor is silent: no heartbeat for 11m0s") {
		t.Errorf("message %q does not match expected prefix", msg)
	}
	if !strings.Contains(msg, "(last ping never)") {
		t.Errorf("message %q should contain '(last ping never)'", msg)
	}
}

func TestWatchdog_SilentAfterTimeout_AfterPing(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)
	clock := newFakeClock(start)
	rec := &recordingNotify{}

	cfg := watchdog.Config{
		Timeout:    10 * time.Minute,
		CheckEvery: 30 * time.Second,
	}
	w := watchdog.New(cfg, rec.Notify, clock.Now)

	clock.Advance(5 * time.Minute)
	handler := w.Handler()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/ping", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	clock.Advance(11 * time.Minute)
	w.Check(context.Background())

	msgs := rec.Messages()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got: %v", msgs)
	}

	msg := msgs[0]
	wantTime := start.Add(5 * time.Minute).Format(time.RFC3339)
	if !strings.Contains(msg, "⚠️ Hawk monitor is silent: no heartbeat for 11m0s") {
		t.Errorf("message %q missing duration", msg)
	}
	if !strings.Contains(msg, "(last ping "+wantTime+")") {
		t.Errorf("message %q missing RFC3339 last ping %s", msg, wantTime)
	}
}

func TestWatchdog_OneAlertPerSilence(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)
	clock := newFakeClock(start)
	rec := &recordingNotify{}

	cfg := watchdog.Config{
		Timeout:    10 * time.Minute,
		CheckEvery: 30 * time.Second,
	}
	w := watchdog.New(cfg, rec.Notify, clock.Now)

	clock.Advance(11 * time.Minute)
	w.Check(context.Background())
	w.Check(context.Background())
	clock.Advance(2 * time.Minute)
	w.Check(context.Background())

	msgs := rec.Messages()
	if len(msgs) != 1 {
		t.Fatalf("expected exactly 1 alert per silence, got %d: %v", len(msgs), msgs)
	}
}

func TestWatchdog_RecoveryMessageOnPing(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)
	clock := newFakeClock(start)
	rec := &recordingNotify{}

	cfg := watchdog.Config{
		Timeout:    10 * time.Minute,
		CheckEvery: 30 * time.Second,
	}
	w := watchdog.New(cfg, rec.Notify, clock.Now)
	handler := w.Handler()

	clock.Advance(11 * time.Minute)
	w.Check(context.Background())

	if len(rec.Messages()) != 1 {
		t.Fatalf("expected 1 silence alert, got: %v", rec.Messages())
	}

	clock.Advance(1 * time.Minute)
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/ping", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	msgs := rec.Messages()
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages, got: %v", msgs)
	}
	if msgs[1] != "✅ Hawk monitor is back: heartbeat received" {
		t.Errorf("got recovery %q, want '✅ Hawk monitor is back: heartbeat received'", msgs[1])
	}

	// Ping while healthy: no alert.
	req2 := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/ping", nil)
	rr2 := httptest.NewRecorder()
	handler.ServeHTTP(rr2, req2)

	if len(rec.Messages()) != 2 {
		t.Fatalf("subsequent healthy ping should not trigger alert, got: %v", rec.Messages())
	}

	// Check while healthy: no alert.
	w.Check(context.Background())
	if len(rec.Messages()) != 2 {
		t.Fatalf("check while healthy should not trigger alert, got: %v", rec.Messages())
	}
}

func TestWatchdog_NotifyFailureRetried(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)
	clock := newFakeClock(start)
	rec := &recordingNotify{}

	cfg := watchdog.Config{
		Timeout:    10 * time.Minute,
		CheckEvery: 30 * time.Second,
	}
	w := watchdog.New(cfg, rec.Notify, clock.Now)
	handler := w.Handler()

	clock.Advance(11 * time.Minute)

	// Silence notify fails on first attempt.
	rec.SetError(errors.New("telegram down"))
	w.Check(context.Background())

	if len(rec.Messages()) != 0 {
		t.Fatalf("no messages should be recorded on failure, got: %v", rec.Messages())
	}

	// Retry on next check succeeds.
	rec.SetError(nil)
	w.Check(context.Background())

	msgs := rec.Messages()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message after retry, got: %v", msgs)
	}

	// Recovery notify fails on first ping.
	rec.SetError(errors.New("telegram down again"))
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/ping", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if len(rec.Messages()) != 1 {
		t.Fatalf("recovery message should not be saved on error, got: %v", rec.Messages())
	}

	// Retry recovery on next ping succeeds.
	rec.SetError(nil)
	req2 := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/ping", nil)
	rr2 := httptest.NewRecorder()
	handler.ServeHTTP(rr2, req2)

	msgs = rec.Messages()
	if len(msgs) != 2 {
		t.Fatalf("expected recovery after retry, got: %v", msgs)
	}
	if msgs[1] != "✅ Hawk monitor is back: heartbeat received" {
		t.Errorf("got recovery %q, want '✅ Hawk monitor is back: heartbeat received'", msgs[1])
	}
}

func TestWatchdog_ConcurrentPingsUnderRace(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)
	clock := newFakeClock(start)
	rec := &recordingNotify{}

	cfg := watchdog.Config{
		Timeout:    10 * time.Minute,
		CheckEvery: 30 * time.Second,
	}
	w := watchdog.New(cfg, rec.Notify, clock.Now)
	srv := httptest.NewServer(w.Handler())
	defer srv.Close()

	clock.Advance(11 * time.Minute)
	w.Check(context.Background())

	var wg sync.WaitGroup
	const workers = 10
	const pingsPerWorker = 20

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			client := srv.Client()
			for j := 0; j < pingsPerWorker; j++ {
				req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL+"/ping", http.NoBody)
				if err != nil {
					t.Errorf("create req: %v", err)

					return
				}
				resp, err := client.Do(req)
				if err != nil {
					t.Errorf("ping: %v", err)

					return
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()

				if resp.StatusCode != http.StatusOK {
					t.Errorf("status = %d, want 200", resp.StatusCode)
				}
			}
		}()
	}

	wg.Wait()

	msgs := rec.Messages()
	// Initial silence + exactly 1 recovery message.
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages (1 silence + 1 recovery), got %d: %v", len(msgs), msgs)
	}
}

func TestWatchdog_HandlerRouting(t *testing.T) {
	t.Parallel()

	w := watchdog.New(watchdog.Config{}, func(context.Context, string) error { return nil }, nil)
	srv := httptest.NewServer(w.Handler())
	defer srv.Close()

	client := srv.Client()

	tests := []struct {
		name       string
		method     string
		path       string
		wantCode   int
		wantSubstr string
	}{
		{
			name:     "POST /ping ok",
			method:   http.MethodPost,
			path:     "/ping",
			wantCode: http.StatusOK,
		},
		{
			name:     "GET /ping 405",
			method:   http.MethodGet,
			path:     "/ping",
			wantCode: http.StatusMethodNotAllowed,
		},
		{
			name:       "GET /healthz 200",
			method:     http.MethodGet,
			path:       "/healthz",
			wantCode:   http.StatusOK,
			wantSubstr: `{"status":"ok"}`,
		},
		{
			name:     "POST /healthz 405",
			method:   http.MethodPost,
			path:     "/healthz",
			wantCode: http.StatusMethodNotAllowed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequestWithContext(context.Background(), tt.method, srv.URL+tt.path, http.NoBody)
			if err != nil {
				t.Fatalf("create req: %v", err)
			}
			resp, err := client.Do(req)
			if err != nil {
				t.Fatalf("do req: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != tt.wantCode {
				t.Errorf("status = %d, want %d", resp.StatusCode, tt.wantCode)
			}

			if tt.wantSubstr != "" {
				bodyBytes, _ := io.ReadAll(resp.Body)
				if !strings.Contains(string(bodyBytes), tt.wantSubstr) {
					t.Errorf("body %q does not contain %q", string(bodyBytes), tt.wantSubstr)
				}
			}
		})
	}
}

func TestWatchdog_Run_StopsOnCancel(t *testing.T) {
	t.Parallel()

	w := watchdog.New(watchdog.Config{
		Timeout:    1 * time.Hour,
		CheckEvery: 10 * time.Millisecond,
	}, func(context.Context, string) error { return nil }, nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		defer close(done)

		w.Run(ctx)
	}()

	time.Sleep(25 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Run did not terminate upon ctx cancellation")
	}
}
