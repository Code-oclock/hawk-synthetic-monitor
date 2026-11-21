// Package app wires config, adapters and the use case together and runs the service.
package app

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/Code-oclock/hawk-synthetic-monitor/internal/adapter/hawk"
	"github.com/Code-oclock/hawk-synthetic-monitor/internal/adapter/metrics"
	"github.com/Code-oclock/hawk-synthetic-monitor/internal/adapter/sqlite"
	"github.com/Code-oclock/hawk-synthetic-monitor/internal/adapter/telegram"
	"github.com/Code-oclock/hawk-synthetic-monitor/internal/adapter/webhook"
	"github.com/Code-oclock/hawk-synthetic-monitor/internal/config"
	httpctl "github.com/Code-oclock/hawk-synthetic-monitor/internal/controller/http"
	"github.com/Code-oclock/hawk-synthetic-monitor/internal/controller/scheduler"
	"github.com/Code-oclock/hawk-synthetic-monitor/internal/usecase"
)

const (
	reportingTimeout = 15 * time.Second
	shutdownTimeout  = 10 * time.Second
)

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

func newID() string {
	buf := make([]byte, 6)
	_, _ = rand.Read(buf)

	return hex.EncodeToString(buf)
}

// Run starts the scheduler and both HTTP servers and blocks until ctx ends.
func Run(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	repo, err := sqlite.Open(ctx, cfg.DBPath)
	if err != nil {
		return fmt.Errorf("open history: %w", err)
	}
	defer repo.Close()

	registry := prometheus.NewRegistry()
	observer := metrics.New(registry)
	inbox := webhook.NewInbox(nil)

	hawkClient := hawk.New(hawk.Config{
		CollectorURL:     cfg.CollectorURL,
		APIURL:           cfg.APIURL,
		HealthURL:        cfg.HealthURL,
		IntegrationToken: cfg.IntegrationToken,
		ProjectID:        cfg.ProjectID,
		Email:            cfg.Email,
		Password:         cfg.Password,
	})

	deps := usecase.Deps{
		Hawk:     hawkClient,
		Cleaner:  hawkClient,
		Inbox:    inbox,
		Repo:     repo,
		Observer: observer,
		Clock:    systemClock{},
		NewID:    newID,
	}
	if cfg.TelegramURL != "" {
		deps.Notifier = telegram.New(cfg.TelegramURL, nil)
	} else {
		log.Warn("TELEGRAM_NOTIFY_URL is not set: failures will not be reported to Telegram")
	}

	check := usecase.NewCheck(deps, usecase.Options{
		ProjectID:        cfg.ProjectID,
		RunTimeout:       cfg.RunTimeout,
		HealthTimeout:    cfg.HealthTimeout,
		SendTimeout:      cfg.SendTimeout,
		VisibleTimeout:   cfg.VisibleTimeout,
		WebhookTimeout:   cfg.WebhookTimeout,
		PollInterval:     cfg.PollInterval,
		ReportingTimeout: reportingTimeout,
		RetryBackoff:     cfg.RetryBackoff,
		MaxAttempts:      cfg.MaxAttempts,
		CleanupEnabled:   cfg.CleanupEnabled,
	})

	opsMux := httpctl.NewHandler(check, repo, observer.Handler(registry), time.Now)
	hookMux := http.NewServeMux()
	hookMux.Handle("POST /webhook", inbox.Handler())

	servers := []*http.Server{
		{Addr: cfg.HTTPAddr, Handler: opsMux, ReadHeaderTimeout: 5 * time.Second},
		{Addr: cfg.WebhookAddr, Handler: hookMux, ReadHeaderTimeout: 5 * time.Second},
	}

	errs := make(chan error, len(servers))
	for _, srv := range servers {
		listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", srv.Addr)
		if err != nil {
			return fmt.Errorf("listen on %s: %w", srv.Addr, err)
		}
		log.Info("listening", "addr", listener.Addr().String())

		go func() {
			if err := srv.Serve(listener); !errors.Is(err, http.ErrServerClosed) {
				errs <- err
			}
		}()
	}

	schedulerDone := make(chan struct{})
	runCtx, stop := context.WithCancel(ctx)
	defer stop()

	go func() {
		defer close(schedulerDone)

		scheduler.Run(runCtx, check, cfg.Interval, log)
	}()

	var runErr error
	select {
	case <-ctx.Done():
	case runErr = <-errs:
		log.Error("server stopped", "error", runErr)
	}

	stop()
	<-schedulerDone

	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancel()

	for _, srv := range servers {
		_ = srv.Shutdown(shutdownCtx)
	}

	return runErr
}
