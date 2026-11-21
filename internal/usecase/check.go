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

// Deps are the adapters a Check needs. Cleaner, Observer, Heartbeat, Inspector and Notifier may be nil.
type Deps struct {
	Hawk      HawkGateway
	Inbox     DeliveryInbox
	Repo      ResultRepo
	Notifier  Notifier
	Cleaner   Cleaner
	Observer  Observer
	Heartbeat Heartbeat
	Inspector QueueInspector
	Clock     Clock
	NewID     func() string
}

// Check runs one end-to-end check of Hawk at a time.
type Check struct {
	deps Deps
	opts Options

	mu   sync.Mutex
	busy bool
	// recoveryDue is true while a failure was seen and no recovery alert has been delivered yet.
	recoveryDue bool
	stateLoaded bool
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

	c.loadState(ctx)
	run := c.execute(runCtx, trigger)
	if ctx.Err() != nil {
		run.Status = entity.StatusCanceled
	}

	c.report(ctx, &run)

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

// loadState seeds the recovery flag from the stored history on the first run after a start.
func (c *Check) loadState(ctx context.Context) {
	c.mu.Lock()
	loaded := c.stateLoaded
	c.stateLoaded = true
	c.mu.Unlock()

	if loaded {
		return
	}

	readCtx, cancel := context.WithTimeout(ctx, c.opts.ReportingTimeout)
	defer cancel()

	last, err := c.deps.Repo.Latest(readCtx)
	if err != nil || last == nil {
		return
	}

	c.mu.Lock()
	c.recoveryDue = last.Status == entity.StatusFailed
	c.mu.Unlock()
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

	if subscribeErr != nil || waiter == nil {
		if subscribeErr == nil {
			subscribeErr = errNoWaiter
		}
		started := c.deps.Clock.Now()
		failed := c.finish(entity.StageSend, started, 0, fmt.Errorf("monitor internal error, subscribe: %w", subscribeErr))
		failed.Failure.Code = "internal"
		run.Status = entity.StatusFailed
		run.Stages = append(run.Stages, failed,
			skipped(entity.StageVisible, "send not attempted"), skipped(entity.StageWebhook, "send not attempted"))

		return run
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

	// Deadlines and latencies count from the start of sending: a webhook may arrive before the collector answers.
	sentAt := *send.StartedAt
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

		webhook = c.waitWebhook(ctx, waiter, sentAt, &run)
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
		if attempts >= c.opts.MaxAttempts {
			break
		}
		if !sleep(stageCtx, c.opts.RetryBackoff) {
			err = fmt.Errorf("%w: last attempt: %w", stageCtx.Err(), err)

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
	ctx context.Context, waiter DeliveryWaiter, sentAt time.Time, run *entity.RunResult,
) entity.StageResult {
	started := c.deps.Clock.Now()

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
func (c *Check) report(ctx context.Context, run *entity.RunResult) {
	base := context.WithoutCancel(ctx)

	if run.Status == entity.StatusPassed && c.opts.CleanupEnabled && c.deps.Cleaner != nil && run.Event != nil {
		cleanCtx, cancel := context.WithTimeout(base, c.opts.ReportingTimeout)
		run.Cleanup = c.cleanup(cleanCtx, *run.Event)
		cancel()
	}

	if run.Status == entity.StatusFailed && c.deps.Inspector != nil {
		diagCtx, cancel := context.WithTimeout(base, c.opts.ReportingTimeout)
		run.Diagnostics = c.diagnose(diagCtx)
		cancel()
	}

	if alert, ok := c.nextAlert(run); ok && c.deps.Notifier != nil {
		notifyCtx, cancel := context.WithTimeout(base, c.opts.ReportingTimeout)
		err := c.deps.Notifier.Notify(notifyCtx, alert)
		cancel()

		run.Notification = entity.ActionResult{Status: entity.StatusPassed}
		if err != nil {
			run.Notification = entity.ActionResult{Status: entity.StatusFailed, Failure: classify(err)}
			run.ReportingErrors = append(run.ReportingErrors, *classify(fmt.Errorf("notify: %w", err)))
		} else if alert.Kind == entity.AlertRecovery {
			c.setRecoveryDue(false)
		}
	}

	saveCtx, cancel := context.WithTimeout(base, c.opts.ReportingTimeout)
	defer cancel()

	if err := c.deps.Repo.Save(saveCtx, *run); err != nil {
		run.ReportingErrors = append(run.ReportingErrors, *classify(fmt.Errorf("save: %w", err)))
	}
	if c.deps.Heartbeat != nil && run.Status != entity.StatusCanceled {
		pingCtx, cancelPing := context.WithTimeout(base, c.opts.ReportingTimeout)
		defer cancelPing()

		if err := c.deps.Heartbeat.Ping(pingCtx, *run); err != nil {
			run.ReportingErrors = append(run.ReportingErrors, *classify(fmt.Errorf("heartbeat: %w", err)))
		}
	}
	if c.deps.Observer != nil {
		c.deps.Observer.Observe(*run)
	}
}

// nextAlert decides what to tell the chat. A recovery alert stays due until one is delivered.
func (c *Check) nextAlert(run *entity.RunResult) (entity.Alert, bool) {
	switch run.Status {
	case entity.StatusFailed:
		c.setRecoveryDue(true)

		return entity.Alert{Kind: entity.AlertFailure, Result: *run}, true
	case entity.StatusPassed:
		c.mu.Lock()
		due := c.recoveryDue
		c.mu.Unlock()

		if due {
			return entity.Alert{Kind: entity.AlertRecovery, Result: *run}, true
		}
	}

	return entity.Alert{}, false
}

func (c *Check) setRecoveryDue(due bool) {
	c.mu.Lock()
	c.recoveryDue = due
	c.mu.Unlock()
}

func (c *Check) cleanup(ctx context.Context, event entity.Event) entity.ActionResult {
	if err := c.deps.Cleaner.RemoveEvent(ctx, event); err != nil {
		return entity.ActionResult{Status: entity.StatusFailed, Failure: classify(err)}
	}

	return entity.ActionResult{Status: entity.StatusPassed}
}

func (c *Check) diagnose(ctx context.Context) *entity.Diagnostics {
	diag := &entity.Diagnostics{
		CollectedAt: c.deps.Clock.Now(),
	}

	if err := c.deps.Hawk.Health(ctx); err != nil {
		diag.CollectorHealth = classify(err)
	}

	queues, err := c.deps.Inspector.Queues(ctx)
	if err != nil {
		diag.Errors = append(diag.Errors, *classify(fmt.Errorf("inspect queues: %w", err)))
	} else {
		diag.Queues = queues
	}

	return diag
}
