// Package watchdog monitors periodic heartbeats and alerts when pings stop.
package watchdog

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

const (
	backMessage = "✅ Hawk monitor is back: heartbeat received"
	maxPingBody = 64 << 10
)

type Config struct {
	Timeout    time.Duration
	CheckEvery time.Duration
}

type Watchdog struct {
	cfg         Config
	notify      func(ctx context.Context, text string) error
	now         func() time.Time
	startTime   time.Time
	mu          sync.Mutex
	lastPing    time.Time
	alertActive bool
}

// New creates a watchdog with the given config and notification callback.
func New(cfg Config, notify func(ctx context.Context, text string) error, now func() time.Time) *Watchdog {
	if now == nil {
		now = time.Now
	}

	return &Watchdog{
		cfg:       cfg,
		notify:    notify,
		now:       now,
		startTime: now(),
	}
}

// Handler returns an HTTP handler serving ping and healthz endpoints.
func (w *Watchdog) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /ping", w.handlePing)
	mux.HandleFunc("GET /healthz", w.handleHealthz)

	return mux
}

func (w *Watchdog) handlePing(rw http.ResponseWriter, r *http.Request) {
	if r.Body != nil {
		_, _ = io.Copy(io.Discard, io.LimitReader(r.Body, maxPingBody))
		_ = r.Body.Close()
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	w.lastPing = w.now()

	if w.alertActive {
		notifyCtx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()

		if err := w.notify(notifyCtx, backMessage); err == nil {
			w.alertActive = false
		}
	}

	rw.WriteHeader(http.StatusOK)
}

func (w *Watchdog) handleHealthz(rw http.ResponseWriter, _ *http.Request) {
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(http.StatusOK)
	_, _ = rw.Write([]byte(`{"status":"ok"}` + "\n"))
}

// Run executes periodic checks until context is canceled.
func (w *Watchdog) Run(ctx context.Context) {
	if w.cfg.CheckEvery <= 0 {
		<-ctx.Done()

		return
	}

	ticker := time.NewTicker(w.cfg.CheckEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.Check(ctx)
		}
	}
}

// Check verifies whether silence duration exceeded timeout.
func (w *Watchdog) Check(ctx context.Context) {
	w.mu.Lock()
	defer w.mu.Unlock()

	now := w.now()
	ref := w.startTime
	lastPingStr := "never"
	if !w.lastPing.IsZero() {
		ref = w.lastPing
		lastPingStr = w.lastPing.Format(time.RFC3339)
	}

	elapsed := now.Sub(ref)
	if elapsed <= w.cfg.Timeout || w.alertActive {
		return
	}

	msg := fmt.Sprintf("⚠️ Hawk monitor is silent: no heartbeat for %s (last ping %s)", roundDuration(elapsed), lastPingStr)
	if err := w.notify(ctx, msg); err == nil {
		w.alertActive = true
	}
}

func roundDuration(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	if d < time.Second {
		return d.Round(time.Millisecond).String()
	}

	return d.Round(time.Second).String()
}
