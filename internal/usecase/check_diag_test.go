package usecase_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/Code-oclock/hawk-synthetic-monitor/internal/entity"
	"github.com/Code-oclock/hawk-synthetic-monitor/internal/usecase"
)

type fakeInspector struct {
	mu     sync.Mutex
	queues []entity.QueueStat
	err    error
	calls  int
}

func (f *fakeInspector) Queues(_ context.Context) ([]entity.QueueStat, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return nil, f.err
	}

	return f.queues, nil
}

func (e *testEnv) setInspector(i usecase.QueueInspector) {
	e.deps.Inspector = i
	e.check = usecase.NewCheck(e.deps, e.opts)
}

func TestCheck_Diagnostics_FailedRunInspectorOK(t *testing.T) {
	env := newTestEnv(testOptions())
	inspector := &fakeInspector{
		queues: []entity.QueueStat{
			{Name: "events", Messages: 10, Consumers: 2, Missing: false},
		},
	}
	env.setInspector(inspector)
	env.hawk.sendFn = func(context.Context, string) error {
		return errors.New("send failed")
	}

	res, err := env.check.Run(context.Background(), entity.TriggerScheduled)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Status != entity.StatusFailed {
		t.Fatalf("status = %v, want failed", res.Status)
	}
	if inspector.calls != 1 {
		t.Fatalf("inspector calls = %d, want 1", inspector.calls)
	}
	if res.Diagnostics == nil {
		t.Fatal("diagnostics is nil, want non-nil")
	}
	if res.Diagnostics.CollectorHealth != nil {
		t.Errorf("collector health = %v, want nil", res.Diagnostics.CollectorHealth)
	}
	if len(res.Diagnostics.Queues) != 1 || res.Diagnostics.Queues[0].Name != "events" {
		t.Errorf("queues = %v, want [{Name: events ...}]", res.Diagnostics.Queues)
	}
	if len(res.Diagnostics.Errors) != 0 {
		t.Errorf("errors = %v, want empty", res.Diagnostics.Errors)
	}
	if res.Diagnostics.CollectedAt.IsZero() {
		t.Error("collected at is zero")
	}
}

func TestCheck_Diagnostics_HealthFailingAtDiagnosis(t *testing.T) {
	env := newTestEnv(testOptions())
	inspector := &fakeInspector{
		queues: []entity.QueueStat{{Name: "q1", Messages: 0, Consumers: 1}},
	}
	env.setInspector(inspector)

	var healthCalls int
	env.hawk.healthFn = func(context.Context) error {
		healthCalls++
		if healthCalls > 1 {
			return errors.New("collector unhealthy")
		}

		return nil
	}
	env.hawk.sendFn = func(context.Context, string) error {
		return errors.New("send failed")
	}

	res, err := env.check.Run(context.Background(), entity.TriggerScheduled)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Status != entity.StatusFailed {
		t.Fatalf("status = %v, want failed", res.Status)
	}
	if res.Diagnostics == nil {
		t.Fatal("diagnostics is nil, want non-nil")
	}
	if res.Diagnostics.CollectorHealth == nil {
		t.Fatal("collector health is nil, want failure")
	}
	if res.Diagnostics.CollectorHealth.Code != "error" || res.Diagnostics.CollectorHealth.Message != "collector unhealthy" {
		t.Errorf("collector health = %+v, want error/collector unhealthy", res.Diagnostics.CollectorHealth)
	}
}

func TestCheck_Diagnostics_InspectorError(t *testing.T) {
	env := newTestEnv(testOptions())
	inspector := &fakeInspector{
		err: errors.New("rabbit unreachable"),
	}
	env.setInspector(inspector)
	env.hawk.sendFn = func(context.Context, string) error {
		return errors.New("send failed")
	}

	res, err := env.check.Run(context.Background(), entity.TriggerScheduled)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Status != entity.StatusFailed {
		t.Fatalf("status = %v, want failed", res.Status)
	}
	if res.Diagnostics == nil {
		t.Fatal("diagnostics is nil, want non-nil")
	}
	if res.Diagnostics.Queues != nil {
		t.Errorf("queues = %v, want nil", res.Diagnostics.Queues)
	}
	if len(res.Diagnostics.Errors) != 1 {
		t.Fatalf("errors len = %d, want 1", len(res.Diagnostics.Errors))
	}
	if res.Diagnostics.Errors[0].Code != "error" {
		t.Errorf("error code = %q, want error", res.Diagnostics.Errors[0].Code)
	}
	if res.Diagnostics.Errors[0].Message != "inspect queues: rabbit unreachable" {
		t.Errorf("error message = %q, want 'inspect queues: rabbit unreachable'", res.Diagnostics.Errors[0].Message)
	}
}

func TestCheck_Diagnostics_PassedRunNotDiagnosed(t *testing.T) {
	env := newTestEnv(testOptions())
	inspector := &fakeInspector{
		queues: []entity.QueueStat{{Name: "q1", Messages: 1}},
	}
	env.setInspector(inspector)

	runPassing(t, env)

	if inspector.calls != 0 {
		t.Fatalf("inspector calls = %d, want 0", inspector.calls)
	}
}

func TestCheck_Diagnostics_NilInspector(t *testing.T) {
	env := newTestEnv(testOptions())
	env.hawk.sendFn = func(context.Context, string) error {
		return errors.New("send failed")
	}

	res, err := env.check.Run(context.Background(), entity.TriggerScheduled)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Status != entity.StatusFailed {
		t.Fatalf("status = %v, want failed", res.Status)
	}
	if res.Diagnostics != nil {
		t.Errorf("diagnostics = %v, want nil", res.Diagnostics)
	}
}

func TestCheck_Diagnostics_NotifierReceivesAlertWithDiagnostics(t *testing.T) {
	env := newTestEnv(testOptions())
	inspector := &fakeInspector{
		queues: []entity.QueueStat{{Name: "events", Messages: 5, Consumers: 1}},
	}
	env.setInspector(inspector)
	env.hawk.sendFn = func(context.Context, string) error {
		return errors.New("send failed")
	}

	res, err := env.check.Run(context.Background(), entity.TriggerScheduled)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Status != entity.StatusFailed {
		t.Fatalf("status = %v, want failed", res.Status)
	}
	if len(env.notifier.alerts) != 1 {
		t.Fatalf("alerts count = %d, want 1", len(env.notifier.alerts))
	}
	alert := env.notifier.alerts[0]
	if alert.Result.Diagnostics == nil {
		t.Fatal("alert result diagnostics is nil, want non-nil")
	}
	if len(alert.Result.Diagnostics.Queues) != 1 || alert.Result.Diagnostics.Queues[0].Name != "events" {
		t.Errorf("alert diagnostics queues = %v, want [{Name: events ...}]", alert.Result.Diagnostics.Queues)
	}
}

func TestCheck_Diagnostics_CanceledRunNotDiagnosed(t *testing.T) {
	env := newTestEnv(testOptions())
	inspector := &fakeInspector{
		queues: []entity.QueueStat{{Name: "q1", Messages: 1}},
	}
	env.setInspector(inspector)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	res, err := env.check.Run(ctx, entity.TriggerScheduled)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Status != entity.StatusCanceled {
		t.Fatalf("status = %v, want canceled", res.Status)
	}
	if inspector.calls != 0 {
		t.Fatalf("inspector calls = %d, want 0", inspector.calls)
	}
	if res.Diagnostics != nil {
		t.Errorf("diagnostics = %v, want nil", res.Diagnostics)
	}
}
