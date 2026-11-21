package http_test

import (
	"context"
	"encoding/json"
	"errors"
	nethttp "net/http"
	"net/http/httptest"
	"testing"
	"time"

	httpctl "github.com/Code-oclock/hawk-synthetic-monitor/internal/controller/http"
	"github.com/Code-oclock/hawk-synthetic-monitor/internal/entity"
	"github.com/Code-oclock/hawk-synthetic-monitor/internal/usecase"
)

type fakeRunner struct {
	result entity.RunResult
	err    error
}

func (f fakeRunner) Run(context.Context, entity.Trigger) (entity.RunResult, error) {
	return f.result, f.err
}

type fakeHistory struct {
	limit int
	since []time.Time
}

func (f *fakeHistory) Recent(_ context.Context, limit int) ([]entity.RunResult, error) {
	f.limit = limit

	return []entity.RunResult{{ID: "r1", Status: entity.StatusPassed}}, nil
}

func (f *fakeHistory) Stats(_ context.Context, since time.Time) (entity.Stats, error) {
	f.since = append(f.since, since)

	return entity.Stats{Total: 4, Passed: 3, Failed: 1, SuccessRate: 0.75}, nil
}

func do(t *testing.T, h nethttp.Handler, method, target string) *httptest.ResponseRecorder {
	t.Helper()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), method, target, nethttp.NoBody))

	return rec
}

func newHandler(runner fakeRunner, history *fakeHistory, now time.Time) nethttp.Handler {
	metrics := nethttp.HandlerFunc(func(w nethttp.ResponseWriter, _ *nethttp.Request) { _, _ = w.Write([]byte("m")) })

	return httpctl.NewHandler(runner, history, metrics, func() time.Time { return now })
}

func TestHealthzAndMetrics(t *testing.T) {
	h := newHandler(fakeRunner{}, &fakeHistory{}, time.Now())

	if rec := do(t, h, "GET", "/healthz"); rec.Code != 200 {
		t.Fatalf("healthz = %d", rec.Code)
	}
	if rec := do(t, h, "GET", "/metrics"); rec.Body.String() != "m" {
		t.Fatalf("metrics not delegated: %q", rec.Body.String())
	}
}

func TestRun(t *testing.T) {
	failed := entity.RunResult{ID: "x", Status: entity.StatusFailed, Stages: []entity.StageResult{
		{Stage: entity.StageHealth, Status: entity.StatusFailed, Failure: &entity.Failure{Code: "timeout", Message: "m"}},
		{Stage: entity.StageSend, Status: entity.StatusSkipped, SkipReason: "health failed"},
	}}
	cases := map[string]struct {
		runner fakeRunner
		code   int
	}{
		"passed":       {fakeRunner{result: entity.RunResult{ID: "x", Status: entity.StatusPassed}}, 200},
		"failed check": {fakeRunner{result: failed}, 200},
		"busy":         {fakeRunner{err: usecase.ErrBusy}, 409},
		"other error":  {fakeRunner{err: errors.New("boom")}, 500},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			rec := do(t, newHandler(tc.runner, &fakeHistory{}, time.Now()), "POST", "/run")
			if rec.Code != tc.code {
				t.Fatalf("code = %d, want %d: %s", rec.Code, tc.code, rec.Body)
			}
		})
	}
}

func TestRunBodyShape(t *testing.T) {
	d := 120 * time.Millisecond
	runner := fakeRunner{result: entity.RunResult{ID: "x", Status: entity.StatusFailed, VisibilityLatency: &d, Stages: []entity.StageResult{
		{Stage: entity.StageSend, Status: entity.StatusSkipped, SkipReason: "why"},
	}}}

	rec := do(t, newHandler(runner, &fakeHistory{}, time.Now()), "POST", "/run")

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"id", "status", "duration_ms", "stages", "visibility_latency_ms", "started_at"} {
		if _, ok := body[key]; !ok {
			t.Errorf("missing key %q", key)
		}
	}
	if _, ok := body["webhook_latency_ms"]; ok {
		t.Error("nil latency must be omitted")
	}
	stage := body["stages"].([]any)[0].(map[string]any)
	if _, ok := stage["duration_ms"]; ok || stage["skip_reason"] != "why" {
		t.Errorf("unexpected stage shape: %v", stage)
	}
}

func TestRuns(t *testing.T) {
	history := &fakeHistory{}
	h := newHandler(fakeRunner{}, history, time.Now())

	if rec := do(t, h, "GET", "/runs"); rec.Code != 200 || history.limit != 20 {
		t.Fatalf("default: code %d limit %d", rec.Code, history.limit)
	}
	if rec := do(t, h, "GET", "/runs?limit=5"); rec.Code != 200 || history.limit != 5 {
		t.Fatalf("limit=5: code %d limit %d", rec.Code, history.limit)
	}
	for _, bad := range []string{"0", "101", "abc"} {
		if rec := do(t, h, "GET", "/runs?limit="+bad); rec.Code != 400 {
			t.Errorf("limit=%s code = %d, want 400", bad, rec.Code)
		}
	}
}

func TestStatsWindows(t *testing.T) {
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	history := &fakeHistory{}

	rec := do(t, newHandler(fakeRunner{}, history, now), "GET", "/stats")

	if rec.Code != 200 || len(history.since) != 2 {
		t.Fatalf("code %d, calls %d", rec.Code, len(history.since))
	}
	if !history.since[0].Equal(now.Add(-24*time.Hour)) || !history.since[1].Equal(now.Add(-7*24*time.Hour)) {
		t.Errorf("windows wrong: %v", history.since)
	}
	var body map[string]map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["last_24h"]["success_rate"] != 0.75 || body["last_7d"]["total"] != float64(4) {
		t.Errorf("unexpected body: %v", body)
	}
}

func TestWrongMethod(t *testing.T) {
	h := newHandler(fakeRunner{}, &fakeHistory{}, time.Now())

	if rec := do(t, h, "GET", "/run"); rec.Code != 405 {
		t.Fatalf("GET /run = %d, want 405", rec.Code)
	}
}

func TestRunSerialization_DiagnosticsAndCleanup(t *testing.T) {
	now := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)
	failedRes := entity.RunResult{
		ID:     "run-failed-diag",
		Status: entity.StatusFailed,
		Diagnostics: &entity.Diagnostics{
			CollectedAt: now,
			CollectorHealth: &entity.Failure{
				Code:    "timeout",
				Message: "health timeout",
			},
			Queues: []entity.QueueStat{
				{Name: "events", Messages: 10, Consumers: 1, Missing: false},
				{Name: "dead-letter", Messages: 0, Consumers: 0, Missing: true},
			},
			Errors: []entity.Failure{
				{Code: "error", Message: "queue check err"},
			},
		},
	}

	hFailed := newHandler(fakeRunner{result: failedRes}, &fakeHistory{}, now)
	recFailed := do(t, hFailed, "POST", "/run")
	if recFailed.Code != 200 {
		t.Fatalf("code = %d, want 200", recFailed.Code)
	}

	var failedBody map[string]any
	if err := json.Unmarshal(recFailed.Body.Bytes(), &failedBody); err != nil {
		t.Fatal(err)
	}

	if _, ok := failedBody["cleanup"]; ok {
		t.Error("cleanup must be omitted when empty")
	}

	diagVal, ok := failedBody["diagnostics"].(map[string]any)
	if !ok {
		t.Fatalf("diagnostics missing or not map: %v", failedBody["diagnostics"])
	}
	if diagVal["collected_at"] != now.Format(time.RFC3339Nano) {
		t.Errorf("collected_at = %v, want %v", diagVal["collected_at"], now.Format(time.RFC3339Nano))
	}
	healthVal, ok := diagVal["collector_health"].(map[string]any)
	if !ok || healthVal["code"] != "timeout" {
		t.Errorf("collector_health = %v, want timeout failure", diagVal["collector_health"])
	}
	queuesVal, ok := diagVal["queues"].([]any)
	if !ok || len(queuesVal) != 2 {
		t.Fatalf("queues = %v, want 2 items", diagVal["queues"])
	}
	q0 := queuesVal[0].(map[string]any)
	if q0["name"] != "events" || q0["messages"] != float64(10) || q0["consumers"] != float64(1) || q0["missing"] != false {
		t.Errorf("q0 = %v", q0)
	}
	errorsVal, ok := diagVal["errors"].([]any)
	if !ok || len(errorsVal) != 1 {
		t.Fatalf("errors = %v, want 1 item", diagVal["errors"])
	}

	passedRes := entity.RunResult{
		ID:     "run-passed-clean",
		Status: entity.StatusPassed,
		Cleanup: entity.ActionResult{
			Status: entity.StatusPassed,
		},
	}
	hPassed := newHandler(fakeRunner{result: passedRes}, &fakeHistory{}, now)
	recPassed := do(t, hPassed, "POST", "/run")
	if recPassed.Code != 200 {
		t.Fatalf("code = %d, want 200", recPassed.Code)
	}

	var passedBody map[string]any
	if err := json.Unmarshal(recPassed.Body.Bytes(), &passedBody); err != nil {
		t.Fatal(err)
	}

	if _, ok := passedBody["diagnostics"]; ok {
		t.Error("diagnostics must be omitted when nil")
	}

	cleanVal, ok := passedBody["cleanup"].(map[string]any)
	if !ok {
		t.Fatalf("cleanup missing or not map: %v", passedBody["cleanup"])
	}
	if cleanVal["status"] != "passed" {
		t.Errorf("cleanup status = %v, want passed", cleanVal["status"])
	}
	if _, ok := cleanVal["failure"]; ok {
		t.Errorf("cleanup failure must be omitted when nil: %v", cleanVal["failure"])
	}
}
