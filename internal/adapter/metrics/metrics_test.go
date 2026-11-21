package metrics_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/Code-oclock/hawk-synthetic-monitor/internal/adapter/metrics"
	"github.com/Code-oclock/hawk-synthetic-monitor/internal/entity"
)

func ptr(d time.Duration) *time.Duration { return &d }

func newMetrics() (*metrics.Metrics, *prometheus.Registry) {
	reg := prometheus.NewRegistry()

	return metrics.New(reg), reg
}

func TestObservePassedAndFailed(t *testing.T) {
	m, reg := newMetrics()

	m.Observe(entity.RunResult{
		Status: entity.StatusPassed, FinishedAt: time.Unix(1000, 0),
		Stages: []entity.StageResult{
			{Stage: entity.StageHealth, Status: entity.StatusPassed, Duration: ptr(10 * time.Millisecond)},
		},
		VisibilityLatency: ptr(300 * time.Millisecond), WebhookLatency: ptr(400 * time.Millisecond),
	})
	m.Observe(entity.RunResult{
		Status: entity.StatusFailed, FinishedAt: time.Unix(2000, 0),
		Stages: []entity.StageResult{
			{Stage: entity.StageHealth, Status: entity.StatusFailed, Duration: ptr(time.Second)},
			{Stage: entity.StageSend, Status: entity.StatusSkipped},
		},
	})

	count, err := testutil.GatherAndCount(reg, "hawk_monitor_stage_duration_seconds")
	if err != nil {
		t.Fatal(err)
	}
	// one histogram series for the health stage; the skipped send stage observes nothing
	if count != 1 {
		t.Fatalf("stage duration series = %d, want 1", count)
	}

	body := dump(t, m, reg)
	for _, want := range []string{
		`hawk_monitor_runs_total{status="passed"} 1`,
		`hawk_monitor_runs_total{status="failed"} 1`,
		`hawk_monitor_stage_total{stage="send",status="skipped"} 1`,
		`hawk_monitor_last_run_passed 0`,
		"hawk_monitor_visibility_latency_seconds_count 1",
		"hawk_monitor_webhook_latency_seconds_count 1",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics output misses %q", want)
		}
	}
	if !strings.Contains(body, "hawk_monitor_last_success_timestamp_seconds 1000") ||
		!strings.Contains(body, "hawk_monitor_last_run_timestamp_seconds 2000") {
		t.Errorf("timestamps wrong")
	}
}

func TestCanceledRunLeavesGaugesAlone(t *testing.T) {
	m, reg := newMetrics()
	m.Observe(entity.RunResult{Status: entity.StatusPassed, FinishedAt: time.Unix(1000, 0)})
	m.Observe(entity.RunResult{Status: entity.StatusCanceled, FinishedAt: time.Unix(2000, 0)})

	body := dump(t, m, reg)
	if !strings.Contains(body, `hawk_monitor_runs_total{status="canceled"} 1`) {
		t.Error("canceled run not counted")
	}
	if !strings.Contains(body, "hawk_monitor_last_run_timestamp_seconds 1000") || !strings.Contains(body, "hawk_monitor_last_run_passed 1") {
		t.Error("canceled run must not change last-run gauges")
	}
}

func dump(t *testing.T, m *metrics.Metrics, g prometheus.Gatherer) string {
	t.Helper()

	rec := httptest.NewRecorder()
	m.Handler(g).ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/metrics", http.NoBody))

	return rec.Body.String()
}
