package usecase

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Code-oclock/hawk-synthetic-monitor/internal/entity"
)

const titlePrefix = "synthetic-check "

// Deps are the adapters a Check needs. Cleaner, Observer and Notifier may be nil.
type Deps struct {
	Hawk     HawkGateway
	Inbox    DeliveryInbox
	Repo     ResultRepo
	Notifier Notifier
	Cleaner  Cleaner
	Observer Observer
	Clock    Clock
	NewID    func() string
}

// Check runs one end-to-end check of Hawk at a time.
type Check struct {
	deps Deps
	opts Options

	mu   sync.Mutex
	busy bool
}

func NewCheck(deps Deps, opts Options) *Check {
	if opts.MaxAttempts < 1 {
		opts.MaxAttempts = 1
	}

	return &Check{deps: deps, opts: opts}
}

// Run executes health, send, visible and webhook stages. It returns ErrBusy if another run is active.
// A failed check is a normal result, not an error.
func (c *Check) Run(ctx context.Context, trigger entity.Trigger) (entity.RunResult, error) {
	if !c.acquire() {
		return entity.RunResult{}, ErrBusy
	}
	defer c.release()

	runCtx, cancel := context.WithTimeout(ctx, c.opts.RunTimeout)
	defer cancel()

	previous := c.previousStatus(ctx)
	run := c.execute(runCtx, trigger)
	if ctx.Err() != nil {
		run.Status = entity.StatusCanceled
	}

	c.report(ctx, &run, previous)

	return run, nil
}

func (c *Check) acquire() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.busy {
		return false
	}
	c.busy = true

	return true
}

func (c *Check) release() {
	c.mu.Lock()
	c.busy = false
	c.mu.Unlock()
}

func (c *Check) previousStatus(ctx context.Context) entity.Status {
	last, err := c.deps.Repo.Latest(ctx)
	if err != nil || last == nil {
		return ""
	}

	return last.Status
}

func (c *Check) execute(ctx context.Context, trigger entity.Trigger) (run entity.RunResult) {
	id := c.deps.NewID()
	title := titlePrefix + id
	run = entity.RunResult{
		ID:        id,
		ProjectID: c.opts.ProjectID,
		Title:     title,
		Trigger:   trigger,
		StartedAt: c.deps.Clock.Now(),
	}
	defer func() {
		run.FinishedAt = c.deps.Clock.Now()
		run.Duration = run.FinishedAt.Sub(run.StartedAt)
	}()

	health := c.retry(ctx, entity.StageHealth, c.opts.HealthTimeout, func(ctx context.Context) error {
		return c.deps.Hawk.Health(ctx)
	})
	run.Stages = append(run.Stages, health)
	if health.Status != entity.StatusPassed {
		run.Status = entity.StatusFailed
		run.Stages = append(run.Stages, skipped(entity.StageSend, "health failed"),
			skipped(entity.StageVisible, "health failed"), skipped(entity.StageWebhook, "health failed"))

		return run
	}

	waiter, subscribeErr := c.deps.Inbox.Subscribe(entity.DeliveryKey{ProjectID: c.opts.ProjectID, Title: title})
	if waiter != nil {
		defer waiter.Close()
	}

	send := c.retry(ctx, entity.StageSend, c.opts.SendTimeout, func(ctx context.Context) error {
		return c.deps.Hawk.SendEvent(ctx, title)
	})
	run.Stages = append(run.Stages, send)
	if send.Status != entity.StatusPassed {
		run.Status = entity.StatusFailed
		run.Stages = append(run.Stages, skipped(entity.StageVisible, "send failed"), skipped(entity.StageWebhook, "send failed"))

		return run
	}

	sentAt := *send.FinishedAt
	var (
		visible, webhook entity.StageResult
		wg               sync.WaitGroup
	)
	wg.Add(2)
	go func() {
		defer wg.Done()

		visible = c.waitVisible(ctx, title, sentAt, &run)
	}()
	go func() {
		defer wg.Done()

		webhook = c.waitWebhook(ctx, waiter, subscribeErr, sentAt, &run)
	}()
	wg.Wait()

	run.Stages = append(run.Stages, visible, webhook)
	run.Status = entity.StatusPassed
	for _, stage := range run.Stages {
		if stage.Status != entity.StatusPassed {
			run.Status = entity.StatusFailed
		}
	}

	return run
}

// retry runs fn up to MaxAttempts times while it fails with ErrTransient, inside one stage timeout.
func (c *Check) retry(ctx context.Context, stage entity.Stage, timeout time.Duration, fn func(context.Context) error) entity.StageResult {
	stageCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	started := c.deps.Clock.Now()
	var (
		err      error
		attempts int
	)
	for attempts < c.opts.MaxAttempts {
		attempts++
		if err = fn(stageCtx); err == nil || !errors.Is(err, ErrTransient) {
			break
		}
		if !sleep(stageCtx, c.opts.RetryBackoff) {
			break
		}
	}

	return c.finish(stage, started, attempts, err)
}

func (c *Check) waitVisible(ctx context.Context, title string, sentAt time.Time, run *entity.RunResult) entity.StageResult {
	stageCtx, cancel := context.WithDeadline(ctx, sentAt.Add(c.opts.VisibleTimeout))
	defer cancel()

	started := c.deps.Clock.Now()
	var (
		err      error
		attempts int
		event    entity.Event
	)
	for {
		attempts++
		event, err = c.deps.Hawk.FindEvent(stageCtx, title)
		if err == nil || (!errors.Is(err, ErrNotFound) && !errors.Is(err, ErrTransient)) {
			break
		}
		if !sleep(stageCtx, c.opts.PollInterval) {
			break
		}
	}

	// Out of time while polling: report a timeout, keep the last poll error as detail.
	if err != nil && stageCtx.Err() != nil && (errors.Is(err, ErrNotFound) || errors.Is(err, ErrTransient)) {
		err = fmt.Errorf("%w: last poll: %w", stageCtx.Err(), err)
	}

	result := c.finish(entity.StageVisible, started, attempts, err)
	if result.Status == entity.StatusPassed {
		latency := result.FinishedAt.Sub(sentAt)
		run.Event = &event
		run.VisibilityLatency = &latency
	}

	return result
}

func (c *Check) waitWebhook(
	ctx context.Context, waiter DeliveryWaiter, subscribeErr error, sentAt time.Time, run *entity.RunResult,
) entity.StageResult {
	started := c.deps.Clock.Now()
	if subscribeErr != nil || waiter == nil {
		if subscribeErr == nil {
			subscribeErr = errNoWaiter
		}

		return c.finish(entity.StageWebhook, started, 0, fmt.Errorf("subscribe: %w", subscribeErr))
	}

	stageCtx, cancel := context.WithDeadline(ctx, sentAt.Add(c.opts.WebhookTimeout))
	defer cancel()

	delivery, err := waiter.Wait(stageCtx)
	result := c.finish(entity.StageWebhook, started, 1, err)
	if result.Status == entity.StatusPassed {
		latency := delivery.ReceivedAt.Sub(sentAt)
		run.Delivery = &delivery
		run.WebhookLatency = &latency
	}

	return result
}

var errNoWaiter = errors.New("no delivery waiter")

func (c *Check) finish(stage entity.Stage, started time.Time, attempts int, err error) entity.StageResult {
	finished := c.deps.Clock.Now()
	duration := finished.Sub(started)
	result := entity.StageResult{
		Stage:      stage,
		Status:     entity.StatusPassed,
		StartedAt:  &started,
		FinishedAt: &finished,
		Duration:   &duration,
		Attempts:   attempts,
	}
	if err != nil {
		result.Status = entity.StatusFailed
		result.Failure = classify(err)
	}

	return result
}

func skipped(stage entity.Stage, reason string) entity.StageResult {
	return entity.StageResult{Stage: stage, Status: entity.StatusSkipped, SkipReason: reason}
}

func classify(err error) *entity.Failure {
	code := "error"
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		code = "timeout"
	case errors.Is(err, context.Canceled):
		code = "canceled"
	case errors.Is(err, ErrNotFound):
		code = "not_found"
	case errors.Is(err, ErrTransient):
		code = "unavailable"
	}

	return &entity.Failure{Code: code, Message: err.Error()}
}

// sleep waits for d or until ctx ends and reports whether the wait completed.
func sleep(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// report stores the result and sends alerts with their own bounded contexts, so a canceled run still reports.
func (c *Check) report(ctx context.Context, run *entity.RunResult, previous entity.Status) {
	repCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.opts.ReportingTimeout)
	defer cancel()

	if run.Status == entity.StatusPassed && c.opts.CleanupEnabled && c.deps.Cleaner != nil && run.Event != nil {
		run.Cleanup = c.cleanup(repCtx, *run.Event)
	}

	if alert, ok := alertFor(run, previous); ok && c.deps.Notifier != nil {
		run.Notification = entity.ActionResult{Status: entity.StatusPassed}
		if err := c.deps.Notifier.Notify(repCtx, alert); err != nil {
			run.Notification = entity.ActionResult{Status: entity.StatusFailed, Failure: classify(err)}
			run.ReportingErrors = append(run.ReportingErrors, *classify(fmt.Errorf("notify: %w", err)))
		}
	}

	if err := c.deps.Repo.Save(repCtx, *run); err != nil {
		run.ReportingErrors = append(run.ReportingErrors, *classify(fmt.Errorf("save: %w", err)))
	}
	if c.deps.Observer != nil {
		c.deps.Observer.Observe(*run)
	}
}

func (c *Check) cleanup(ctx context.Context, event entity.Event) entity.ActionResult {
	if err := c.deps.Cleaner.RemoveEvent(ctx, event); err != nil {
		return entity.ActionResult{Status: entity.StatusFailed, Failure: classify(err)}
	}

	return entity.ActionResult{Status: entity.StatusPassed}
}

func alertFor(run *entity.RunResult, previous entity.Status) (entity.Alert, bool) {
	switch {
	case run.Status == entity.StatusFailed:
		return entity.Alert{Kind: entity.AlertFailure, Result: *run}, true
	case run.Status == entity.StatusPassed && previous == entity.StatusFailed:
		return entity.Alert{Kind: entity.AlertRecovery, Result: *run}, true
	default:
		return entity.Alert{}, false
	}
}
