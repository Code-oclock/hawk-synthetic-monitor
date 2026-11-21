// Package scheduler triggers checks at a fixed interval.
package scheduler

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/Code-oclock/hawk-synthetic-monitor/internal/entity"
	"github.com/Code-oclock/hawk-synthetic-monitor/internal/usecase"
)

type Runner interface {
	Run(ctx context.Context, trigger entity.Trigger) (entity.RunResult, error)
}

// Run starts a check right away and then every interval until ctx ends.
// A tick that finds a run still active is skipped.
func Run(ctx context.Context, runner Runner, interval time.Duration, log *slog.Logger) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		runOnce(ctx, runner, log)

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func runOnce(ctx context.Context, runner Runner, log *slog.Logger) {
	result, err := runner.Run(ctx, entity.TriggerScheduled)
	if errors.Is(err, usecase.ErrBusy) {
		log.Warn("scheduled run skipped: previous run is still active")

		return
	}
	if err != nil {
		log.Error("scheduled run failed to start", "error", err)

		return
	}

	log.Info("check finished", "run", result.ID, "status", result.Status, "duration", result.Duration.Round(time.Millisecond))
}
