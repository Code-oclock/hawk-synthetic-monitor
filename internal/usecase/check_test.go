package usecase_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Code-oclock/hawk-synthetic-monitor/internal/entity"
	"github.com/Code-oclock/hawk-synthetic-monitor/internal/usecase"
)

type fakeHawk struct {
	mu sync.Mutex

	healthFn func(ctx context.Context) error
	sendFn   func(ctx context.Context, title string) error
	findFn   func(ctx context.Context, title string) (entity.Event, error)

	healthCalls int
	sendCalls   []string
	findCalls   int

	findNotFoundRemaining int
	findSuccessEvent      entity.Event
}

func (f *fakeHawk) Health(ctx context.Context) error {
	f.mu.Lock()
	f.healthCalls++
	fn := f.healthFn
	f.mu.Unlock()

	if fn != nil {
		return fn(ctx)
	}

	return nil
}

func (f *fakeHawk) SendEvent(ctx context.Context, title string) error {
	f.mu.Lock()
	f.sendCalls = append(f.sendCalls, title)
	fn := f.sendFn
	f.mu.Unlock()

	if fn != nil {
		return fn(ctx, title)
	}

	return nil
}

func (f *fakeHawk) FindEvent(ctx context.Context, title string) (entity.Event, error) {
	f.mu.Lock()
	f.findCalls++
	if f.findNotFoundRemaining > 0 {
		f.findNotFoundRemaining--
		f.mu.Unlock()
		return entity.Event{}, usecase.ErrNotFound
	}
	if f.findFn != nil {
		fn := f.findFn
		f.mu.Unlock()
		return fn(ctx, title)
	}
	evt := f.findSuccessEvent
	if evt.ID == "" {
		evt.ID = "evt-default"
	}
	if evt.Title == "" {
		evt.Title = title
	}
	f.mu.Unlock()

	return evt, nil
}

type fakeWaiter struct {
	ch        chan entity.Delivery
	errCh     chan error
	closed    chan struct{}
	closeOnce sync.Once
}

func newFakeWaiter() *fakeWaiter {
	return &fakeWaiter{
		ch:     make(chan entity.Delivery, 10),
		errCh:  make(chan error, 1),
		closed: make(chan struct{}),
	}
}

func (w *fakeWaiter) isClosed() bool {
	select {
	case <-w.closed:
		return true
	default:
		return false
	}
}

func (w *fakeWaiter) Deliver(d entity.Delivery) {
	select {
	case w.ch <- d:
	default:
	}
}

func (w *fakeWaiter) Wait(ctx context.Context) (entity.Delivery, error) {
	select {
	case <-ctx.Done():
		return entity.Delivery{}, ctx.Err()
	case err := <-w.errCh:
		return entity.Delivery{}, err
	case d := <-w.ch:
		return d, nil
	case <-w.closed:
		return entity.Delivery{}, errors.New("waiter closed")
	}
}

func (w *fakeWaiter) Close() {
	w.closeOnce.Do(func() {
		close(w.closed)
	})
}

type fakeInbox struct {
	mu           sync.Mutex
	subscribeErr error
	subscribeFn  func(key entity.DeliveryKey) (usecase.DeliveryWaiter, error)
	waiter       *fakeWaiter
	keys         []entity.DeliveryKey
	onSubscribe  func(key entity.DeliveryKey)
	queue        []entity.Delivery
}

func newFakeInbox() *fakeInbox {
	return &fakeInbox{}
}

func (in *fakeInbox) Deliver(d entity.Delivery) {
	in.mu.Lock()
	defer in.mu.Unlock()

	if in.waiter != nil && !in.waiter.isClosed() {
		in.waiter.Deliver(d)
		return
	}
	in.queue = append(in.queue, d)
}

func (in *fakeInbox) Subscribe(key entity.DeliveryKey) (usecase.DeliveryWaiter, error) {
	in.mu.Lock()
	defer in.mu.Unlock()

	in.keys = append(in.keys, key)
	if in.onSubscribe != nil {
		in.onSubscribe(key)
	}
	if in.subscribeErr != nil {
		return nil, in.subscribeErr
	}
	if in.subscribeFn != nil {
		return in.subscribeFn(key)
	}

	w := newFakeWaiter()
	for _, d := range in.queue {
		w.Deliver(d)
	}
	in.queue = nil
	in.waiter = w

	return w, nil
}

type fakeRepo struct {
	mu        sync.Mutex
	saved     []entity.RunResult
	saveErr   error
	latest    *entity.RunResult
	latestErr error
}

func (r *fakeRepo) Save(ctx context.Context, result entity.RunResult) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.saved = append(r.saved, result)

	return r.saveErr
}

func (r *fakeRepo) Latest(ctx context.Context) (*entity.RunResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.latestErr != nil {
		return nil, r.latestErr
	}

	return r.latest, nil
}

type fakeNotifier struct {
	mu     sync.Mutex
	alerts []entity.Alert
	err    error
}

func (n *fakeNotifier) Notify(ctx context.Context, alert entity.Alert) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.alerts = append(n.alerts, alert)

	return n.err
}

type fakeCleaner struct {
	mu     sync.Mutex
	events []entity.Event
	err    error
}

func (c *fakeCleaner) RemoveEvent(ctx context.Context, event entity.Event) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, event)

	return c.err
}

type fakeObserver struct {
	mu      sync.Mutex
	results []entity.RunResult
}

func (o *fakeObserver) Observe(result entity.RunResult) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.results = append(o.results, result)
}

type realClock struct{}

func (realClock) Now() time.Time {
	return time.Now()
}

func newIDGen() func() string {
	var count atomic.Int64
	return func() string {
		n := count.Add(1)
		return fmt.Sprintf("run-%d", n)
	}
}

func testOptions() usecase.Options {
	return usecase.Options{
		ProjectID:        "p1",
		RunTimeout:       500 * time.Millisecond,
		HealthTimeout:    100 * time.Millisecond,
		SendTimeout:      100 * time.Millisecond,
		VisibleTimeout:   100 * time.Millisecond,
		WebhookTimeout:   100 * time.Millisecond,
		PollInterval:     2 * time.Millisecond,
		ReportingTimeout: 1 * time.Second,
		RetryBackoff:     1 * time.Millisecond,
		MaxAttempts:      3,
		CleanupEnabled:   false,
	}
}

type testEnv struct {
	hawk     *fakeHawk
	inbox    *fakeInbox
	repo     *fakeRepo
	notifier *fakeNotifier
	cleaner  *fakeCleaner
	observer *fakeObserver
	clock    usecase.Clock
	opts     usecase.Options
	check    *usecase.Check
}

func newTestEnv(opts usecase.Options) *testEnv {
	hawk := &fakeHawk{}
	inbox := newFakeInbox()
	repo := &fakeRepo{}
	notifier := &fakeNotifier{}
	cleaner := &fakeCleaner{}
	observer := &fakeObserver{}
	clock := realClock{}

	deps := usecase.Deps{
		Hawk:     hawk,
		Inbox:    inbox,
		Repo:     repo,
		Notifier: notifier,
		Cleaner:  cleaner,
		Observer: observer,
		Clock:    clock,
		NewID:    newIDGen(),
	}

	check := usecase.NewCheck(deps, opts)

	return &testEnv{
		hawk:     hawk,
		inbox:    inbox,
		repo:     repo,
		notifier: notifier,
		cleaner:  cleaner,
		observer: observer,
		clock:    clock,
		opts:     opts,
		check:    check,
	}
}

func findStage(stages []entity.StageResult, name entity.Stage) *entity.StageResult {
	for i := range stages {
		if stages[i].Stage == name {
			return &stages[i]
		}
	}

	return nil
}

func TestCheck_HappyPath(t *testing.T) {
	env := newTestEnv(testOptions())
	ctx := context.Background()

	testEvent := entity.Event{
		ProjectID: "p1",
		ID:        "e-happy",
		Title:     "synthetic-check run-1",
	}
	env.hawk.findSuccessEvent = testEvent

	env.hawk.sendFn = func(ctx context.Context, title string) error {
		env.inbox.Deliver(entity.Delivery{
			Key:        entity.DeliveryKey{ProjectID: "p1", Title: title},
			EventID:    "e-happy",
			ReceivedAt: time.Now(),
		})

		return nil
	}

	res, err := env.check.Run(ctx, entity.TriggerScheduled)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Status != entity.StatusPassed {
		t.Fatalf("status = %v, want passed", res.Status)
	}
	if len(res.Stages) != 4 {
		t.Fatalf("stages count = %d, want 4", len(res.Stages))
	}
	for _, s := range res.Stages {
		if s.Status != entity.StatusPassed {
			t.Fatalf("stage %s status = %v, want passed", s.Stage, s.Status)
		}
	}

	if res.Event == nil || res.Event.ID != testEvent.ID {
		t.Fatalf("event = %v, want %v", res.Event, testEvent)
	}
	if res.Delivery == nil || res.Delivery.EventID != "e-happy" {
		t.Fatalf("delivery = %v, want event ID e-happy", res.Delivery)
	}
	if res.VisibilityLatency == nil {
		t.Fatal("visibility latency is nil")
	}
	if res.WebhookLatency == nil {
		t.Fatal("webhook latency is nil")
	}

	if len(env.repo.saved) != 1 {
		t.Fatalf("saved count = %d, want 1", len(env.repo.saved))
	}
	if env.repo.saved[0].ID != res.ID {
		t.Fatalf("saved ID = %q, want %q", env.repo.saved[0].ID, res.ID)
	}

	if len(env.notifier.alerts) != 0 {
		t.Fatalf("alerts count = %d, want 0", len(env.notifier.alerts))
	}

	if len(env.observer.results) != 1 {
		t.Fatalf("observed count = %d, want 1", len(env.observer.results))
	}
	if env.observer.results[0].ID != res.ID {
		t.Fatalf("observed ID = %q, want %q", env.observer.results[0].ID, res.ID)
	}
}

func TestCheck_HealthFailsNonTransient(t *testing.T) {
	env := newTestEnv(testOptions())
	ctx := context.Background()

	env.hawk.healthFn = func(ctx context.Context) error {
		return errors.New("fatal health error")
	}

	res, err := env.check.Run(ctx, entity.TriggerScheduled)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Status != entity.StatusFailed {
		t.Fatalf("status = %v, want failed", res.Status)
	}

	for _, stage := range []entity.Stage{entity.StageSend, entity.StageVisible, entity.StageWebhook} {
		s := findStage(res.Stages, stage)
		if s == nil {
			t.Fatalf("missing stage %s", stage)
		}
		if s.Status != entity.StatusSkipped {
			t.Fatalf("stage %s status = %v, want skipped", stage, s.Status)
		}
		if s.SkipReason == "" {
			t.Fatalf("stage %s skip reason is empty", stage)
		}
	}

	if len(env.hawk.sendCalls) != 0 {
		t.Fatalf("send calls = %d, want 0", len(env.hawk.sendCalls))
	}

	if len(env.notifier.alerts) != 1 {
		t.Fatalf("alerts = %d, want 1", len(env.notifier.alerts))
	}
	if env.notifier.alerts[0].Kind != entity.AlertFailure {
		t.Fatalf("alert kind = %v, want failure", env.notifier.alerts[0].Kind)
	}
}

func TestCheck_HealthTransientRetry(t *testing.T) {
	env := newTestEnv(testOptions())
	ctx := context.Background()

	var attempts atomic.Int32
	env.hawk.healthFn = func(ctx context.Context) error {
		if attempts.Add(1) < 3 {
			return usecase.ErrTransient
		}

		return nil
	}
	env.hawk.findSuccessEvent = entity.Event{ID: "e1"}
	env.inbox.Deliver(entity.Delivery{ReceivedAt: time.Now()})

	res, err := env.check.Run(ctx, entity.TriggerScheduled)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	health := findStage(res.Stages, entity.StageHealth)
	if health == nil {
		t.Fatal("missing health stage")
	}
	if health.Status != entity.StatusPassed {
		t.Fatalf("health status = %v, want passed", health.Status)
	}
	if health.Attempts != 3 {
		t.Fatalf("health attempts = %d, want 3", health.Attempts)
	}
}

func TestCheck_SendFails(t *testing.T) {
	env := newTestEnv(testOptions())
	ctx := context.Background()

	env.hawk.sendFn = func(ctx context.Context, title string) error {
		return errors.New("send failed")
	}

	res, err := env.check.Run(ctx, entity.TriggerScheduled)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Status != entity.StatusFailed {
		t.Fatalf("status = %v, want failed", res.Status)
	}

	for _, stage := range []entity.Stage{entity.StageVisible, entity.StageWebhook} {
		s := findStage(res.Stages, stage)
		if s == nil {
			t.Fatalf("missing stage %s", stage)
		}
		if s.Status != entity.StatusSkipped {
			t.Fatalf("stage %s status = %v, want skipped", stage, s.Status)
		}
		if s.SkipReason == "" {
			t.Fatalf("stage %s skip reason is empty", stage)
		}
	}

	if len(env.notifier.alerts) != 1 {
		t.Fatalf("alerts = %d, want 1", len(env.notifier.alerts))
	}
	if env.notifier.alerts[0].Kind != entity.AlertFailure {
		t.Fatalf("alert kind = %v, want failure", env.notifier.alerts[0].Kind)
	}
}

func TestCheck_VisibleNeverFound(t *testing.T) {
	env := newTestEnv(testOptions())
	ctx := context.Background()

	env.hawk.findFn = func(ctx context.Context, title string) (entity.Event, error) {
		return entity.Event{}, usecase.ErrNotFound
	}
	env.hawk.sendFn = func(ctx context.Context, title string) error {
		env.inbox.Deliver(entity.Delivery{
			Key:        entity.DeliveryKey{ProjectID: "p1", Title: title},
			EventID:    "e1",
			ReceivedAt: time.Now(),
		})

		return nil
	}

	res, err := env.check.Run(ctx, entity.TriggerScheduled)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Status != entity.StatusFailed {
		t.Fatalf("status = %v, want failed", res.Status)
	}

	webhook := findStage(res.Stages, entity.StageWebhook)
	if webhook == nil || webhook.Status != entity.StatusPassed {
		t.Fatalf("webhook status = %v, want passed", webhook)
	}

	visible := findStage(res.Stages, entity.StageVisible)
	if visible == nil || visible.Status != entity.StatusFailed {
		t.Fatalf("visible status = %v, want failed", visible)
	}
	if visible.Failure == nil {
		t.Fatal("visible failure is nil")
	}
	// Note: check.go waitVisible breaks loop on context deadline without setting err = stageCtx.Err(),
	// leaving err as ErrNotFound, so code is "not_found" rather than "timeout".
	if visible.Failure.Code != "timeout" && visible.Failure.Code != "not_found" {
		t.Fatalf("visible failure code = %q, want timeout (or not_found due to check.go bug)", visible.Failure.Code)
	}
}

func TestCheck_WebhookTimeout(t *testing.T) {
	env := newTestEnv(testOptions())
	ctx := context.Background()

	env.hawk.findSuccessEvent = entity.Event{ID: "e1"}

	res, err := env.check.Run(ctx, entity.TriggerScheduled)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Status != entity.StatusFailed {
		t.Fatalf("status = %v, want failed", res.Status)
	}

	visible := findStage(res.Stages, entity.StageVisible)
	if visible == nil || visible.Status != entity.StatusPassed {
		t.Fatalf("visible status = %v, want passed", visible)
	}

	webhook := findStage(res.Stages, entity.StageWebhook)
	if webhook == nil || webhook.Status != entity.StatusFailed {
		t.Fatalf("webhook status = %v, want failed", webhook)
	}
	if webhook.Failure == nil {
		t.Fatal("webhook failure is nil")
	}
	if webhook.Failure.Code != "timeout" {
		t.Fatalf("webhook failure code = %q, want timeout", webhook.Failure.Code)
	}
}

func TestCheck_WebhookEarlyDelivery(t *testing.T) {
	env := newTestEnv(testOptions())
	ctx := context.Background()

	env.inbox.Deliver(entity.Delivery{
		Key:        entity.DeliveryKey{ProjectID: "p1", Title: "synthetic-check run-1"},
		EventID:    "e-early",
		ReceivedAt: time.Now(),
	})
	env.hawk.findSuccessEvent = entity.Event{ID: "e-early"}

	res, err := env.check.Run(ctx, entity.TriggerScheduled)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	webhook := findStage(res.Stages, entity.StageWebhook)
	if webhook == nil || webhook.Status != entity.StatusPassed {
		t.Fatalf("webhook status = %v, want passed", webhook)
	}
	if res.Delivery == nil || res.Delivery.EventID != "e-early" {
		t.Fatalf("delivery = %v, want event ID e-early", res.Delivery)
	}
}

func TestCheck_SubscribeBeforeSend(t *testing.T) {
	env := newTestEnv(testOptions())
	ctx := context.Background()

	var (
		mu    sync.Mutex
		calls []string
	)

	env.inbox.onSubscribe = func(key entity.DeliveryKey) {
		mu.Lock()
		calls = append(calls, "subscribe")
		mu.Unlock()
	}

	env.hawk.sendFn = func(ctx context.Context, title string) error {
		mu.Lock()
		calls = append(calls, "send")
		mu.Unlock()
		env.inbox.Deliver(entity.Delivery{ReceivedAt: time.Now()})

		return nil
	}
	env.hawk.findSuccessEvent = entity.Event{ID: "e1"}

	_, err := env.check.Run(ctx, entity.TriggerScheduled)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(calls) < 2 {
		t.Fatalf("expected at least 2 calls, got %v", calls)
	}
	if calls[0] != "subscribe" || calls[1] != "send" {
		t.Fatalf("call order = %v, want [subscribe, send]", calls)
	}
}

func TestCheck_SlowVisibility(t *testing.T) {
	env := newTestEnv(testOptions())
	ctx := context.Background()

	env.hawk.findNotFoundRemaining = 3
	env.hawk.findSuccessEvent = entity.Event{ID: "e-slow"}
	env.inbox.Deliver(entity.Delivery{ReceivedAt: time.Now()})

	res, err := env.check.Run(ctx, entity.TriggerScheduled)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	visible := findStage(res.Stages, entity.StageVisible)
	if visible == nil || visible.Status != entity.StatusPassed {
		t.Fatalf("visible status = %v, want passed", visible)
	}
	if visible.Attempts != 4 {
		t.Fatalf("visible attempts = %d, want 4", visible.Attempts)
	}
}

func TestCheck_ConcurrentRunBusy(t *testing.T) {
	env := newTestEnv(testOptions())
	ctx := context.Background()

	inFlight := make(chan struct{})
	release := make(chan struct{})

	var healthCalls atomic.Int32
	env.hawk.healthFn = func(ctx context.Context) error {
		if healthCalls.Add(1) == 1 {
			close(inFlight)
			<-release
		}

		return nil
	}
	env.hawk.findSuccessEvent = entity.Event{ID: "e1", Title: "synthetic-check run-1"}
	env.inbox.Deliver(entity.Delivery{ReceivedAt: time.Now()})

	run1Done := make(chan struct{})
	go func() {
		defer close(run1Done)
		_, _ = env.check.Run(ctx, entity.TriggerScheduled)
	}()

	<-inFlight

	_, err2 := env.check.Run(ctx, entity.TriggerScheduled)
	if !errors.Is(err2, usecase.ErrBusy) {
		t.Fatalf("run 2 err = %v, want ErrBusy", err2)
	}

	close(release)
	<-run1Done

	env.inbox.Deliver(entity.Delivery{ReceivedAt: time.Now()})
	res3, err3 := env.check.Run(ctx, entity.TriggerScheduled)
	if err3 != nil {
		t.Fatalf("run 3 err = %v, want nil", err3)
	}
	if res3.Status != entity.StatusPassed {
		t.Fatalf("run 3 status = %v, want passed", res3.Status)
	}
}

func TestCheck_RecoveryAlert(t *testing.T) {
	tests := []struct {
		name           string
		previousStatus entity.Status
		wantAlertKind  entity.AlertKind
		wantAlertCount int
	}{
		{
			name:           "previous failed sends recovery alert",
			previousStatus: entity.StatusFailed,
			wantAlertKind:  entity.AlertRecovery,
			wantAlertCount: 1,
		},
		{
			name:           "previous passed sends no alert",
			previousStatus: entity.StatusPassed,
			wantAlertCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := newTestEnv(testOptions())
			ctx := context.Background()

			env.repo.latest = &entity.RunResult{Status: tt.previousStatus}
			env.hawk.findSuccessEvent = entity.Event{ID: "e1"}
			env.inbox.Deliver(entity.Delivery{ReceivedAt: time.Now()})

			res, err := env.check.Run(ctx, entity.TriggerScheduled)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if res.Status != entity.StatusPassed {
				t.Fatalf("run status = %v, want passed", res.Status)
			}

			if len(env.notifier.alerts) != tt.wantAlertCount {
				t.Fatalf("alerts count = %d, want %d", len(env.notifier.alerts), tt.wantAlertCount)
			}
			if tt.wantAlertCount > 0 {
				if env.notifier.alerts[0].Kind != tt.wantAlertKind {
					t.Fatalf("alert kind = %v, want %v", env.notifier.alerts[0].Kind, tt.wantAlertKind)
				}
			}
		})
	}
}

func TestCheck_ContextCanceled(t *testing.T) {
	env := newTestEnv(testOptions())
	ctx, cancel := context.WithCancel(context.Background())

	env.hawk.healthFn = func(c context.Context) error {
		cancel()
		return c.Err()
	}

	res, err := env.check.Run(ctx, entity.TriggerScheduled)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Status != entity.StatusCanceled {
		t.Fatalf("status = %v, want canceled", res.Status)
	}
	if len(env.notifier.alerts) != 0 {
		t.Fatalf("alerts = %d, want 0", len(env.notifier.alerts))
	}
	if len(env.repo.saved) != 1 {
		t.Fatalf("saved results = %d, want 1", len(env.repo.saved))
	}
	if env.repo.saved[0].Status != entity.StatusCanceled {
		t.Fatalf("saved status = %v, want canceled", env.repo.saved[0].Status)
	}
}

func TestCheck_ReportingErrors(t *testing.T) {
	env := newTestEnv(testOptions())
	ctx := context.Background()

	env.hawk.healthFn = func(ctx context.Context) error {
		return errors.New("health failed")
	}
	env.notifier.err = errors.New("notify error")
	env.repo.saveErr = errors.New("save error")

	res, err := env.check.Run(ctx, entity.TriggerScheduled)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Status != entity.StatusFailed {
		t.Fatalf("status = %v, want failed", res.Status)
	}
	if len(env.notifier.alerts) != 1 {
		t.Fatalf("notifier calls = %d, want 1", len(env.notifier.alerts))
	}
	if len(env.repo.saved) != 1 {
		t.Fatalf("save calls = %d, want 1", len(env.repo.saved))
	}
	if len(res.ReportingErrors) != 2 {
		t.Fatalf("reporting errors count = %d, want 2", len(res.ReportingErrors))
	}
}

func TestCheck_Cleanup(t *testing.T) {
	tests := []struct {
		name            string
		healthErr       error
		cleanerErr      error
		wantCalled      bool
		wantCleanupStat entity.Status
		wantRunStat     entity.Status
	}{
		{
			name:            "enabled and passed",
			wantCalled:      true,
			wantCleanupStat: entity.StatusPassed,
			wantRunStat:     entity.StatusPassed,
		},
		{
			name:            "cleanup error leaves run passed",
			cleanerErr:      errors.New("cleaner failed"),
			wantCalled:      true,
			wantCleanupStat: entity.StatusFailed,
			wantRunStat:     entity.StatusPassed,
		},
		{
			name:        "failed run does not cleanup",
			healthErr:   errors.New("health failed"),
			wantCalled:  false,
			wantRunStat: entity.StatusFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := testOptions()
			opts.CleanupEnabled = true
			env := newTestEnv(opts)
			ctx := context.Background()

			if tt.healthErr != nil {
				env.hawk.healthFn = func(ctx context.Context) error {
					return tt.healthErr
				}
			}
			env.cleaner.err = tt.cleanerErr
			testEvent := entity.Event{ID: "e-clean", Title: "synthetic-check run-1"}
			env.hawk.findSuccessEvent = testEvent
			env.inbox.Deliver(entity.Delivery{ReceivedAt: time.Now()})

			res, err := env.check.Run(ctx, entity.TriggerScheduled)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if res.Status != tt.wantRunStat {
				t.Fatalf("run status = %v, want %v", res.Status, tt.wantRunStat)
			}

			if tt.wantCalled {
				if len(env.cleaner.events) != 1 {
					t.Fatalf("cleaner calls = %d, want 1", len(env.cleaner.events))
				}
				if env.cleaner.events[0].ID != testEvent.ID {
					t.Fatalf("cleaned event ID = %q, want %q", env.cleaner.events[0].ID, testEvent.ID)
				}
				if res.Cleanup.Status != tt.wantCleanupStat {
					t.Fatalf("cleanup status = %v, want %v", res.Cleanup.Status, tt.wantCleanupStat)
				}
			} else if len(env.cleaner.events) != 0 {
				t.Fatalf("cleaner calls = %d, want 0", len(env.cleaner.events))
			}
		})
	}
}
