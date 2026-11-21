package usecase_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Code-oclock/hawk-synthetic-monitor/internal/entity"
	"github.com/Code-oclock/hawk-synthetic-monitor/internal/usecase"
)

func transientErr() error { return fmt.Errorf("%w: boom", usecase.ErrTransient) }

func runPassing(t *testing.T, env *testEnv) {
	t.Helper()

	env.inbox.Deliver(entity.Delivery{ReceivedAt: time.Now()})

	res, err := env.check.Run(context.Background(), entity.TriggerScheduled)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Status != entity.StatusPassed {
		t.Fatalf("run status = %v, want passed", res.Status)
	}
}

func TestCheck_RecoveryRetriedUntilDelivered(t *testing.T) {
	env := newTestEnv(testOptions())
	env.hawk.healthFn = func(context.Context) error { return errors.New("down") }

	res, err := env.check.Run(context.Background(), entity.TriggerScheduled)
	if err != nil || res.Status != entity.StatusFailed {
		t.Fatalf("setup run: %v %v", res.Status, err)
	}

	env.hawk.healthFn = nil
	env.notifier.err = errors.New("telegram down")
	runPassing(t, env)
	runPassing(t, env)

	env.notifier.err = nil
	runPassing(t, env)
	runPassing(t, env)

	var recoveries int
	for _, a := range env.notifier.alerts {
		if a.Kind == entity.AlertRecovery {
			recoveries++
		}
	}
	// Two attempts that failed, one that was delivered, and nothing after that.
	if recoveries != 3 {
		t.Fatalf("recovery alert attempts = %d, want 3", recoveries)
	}
}

func TestCheck_SubscribeFailureAbortsBeforeSend(t *testing.T) {
	env := newTestEnv(testOptions())
	env.inbox.subscribeErr = errors.New("key taken")

	res, err := env.check.Run(context.Background(), entity.TriggerScheduled)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.Status != entity.StatusFailed {
		t.Fatalf("status = %v, want failed", res.Status)
	}
	if len(env.hawk.sendCalls) != 0 {
		t.Fatalf("event was sent despite the subscribe failure: %v", env.hawk.sendCalls)
	}

	send := findStage(res.Stages, entity.StageSend)
	if send == nil || send.Failure == nil || send.Failure.Code != "internal" {
		t.Fatalf("send stage = %+v, want an internal failure", send)
	}
}

func TestCheck_WebhookBeforeSendResponseHasNonNegativeLatency(t *testing.T) {
	env := newTestEnv(testOptions())
	env.hawk.sendFn = func(ctx context.Context, _ string) error {
		// The notification races ahead of the collector's answer.
		env.inbox.Deliver(entity.Delivery{ReceivedAt: time.Now()})
		time.Sleep(20 * time.Millisecond)

		return nil
	}

	res, err := env.check.Run(context.Background(), entity.TriggerScheduled)
	if err != nil || res.Status != entity.StatusPassed {
		t.Fatalf("run: %v %v", res.Status, err)
	}

	if res.WebhookLatency == nil || *res.WebhookLatency < 0 {
		t.Fatalf("webhook latency = %v, want non-negative", res.WebhookLatency)
	}
}

func TestCheck_RetryStopsAtLastAttemptWithoutSleeping(t *testing.T) {
	opts := testOptions()
	opts.MaxAttempts = 2
	opts.RetryBackoff = 100 * time.Millisecond
	opts.HealthTimeout = time.Second
	env := newTestEnv(opts)
	env.hawk.healthFn = func(context.Context) error { return transientErr() }

	started := time.Now()
	res, _ := env.check.Run(context.Background(), entity.TriggerScheduled)

	// One backoff between two attempts is expected, a second one after the last attempt is not.
	if elapsed := time.Since(started); elapsed > 180*time.Millisecond {
		t.Fatalf("run took %v: it slept after the last attempt", elapsed)
	}
	health := findStage(res.Stages, entity.StageHealth)
	if health == nil || health.Attempts != 2 {
		t.Fatalf("health stage = %+v, want 2 attempts", health)
	}
}
