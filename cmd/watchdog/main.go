// Command watchdog alerts Telegram when hawk synthetic monitor heartbeats stop.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Code-oclock/hawk-synthetic-monitor/internal/adapter/telegram"
	"github.com/Code-oclock/hawk-synthetic-monitor/internal/watchdog"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(log); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	telegramURL := os.Getenv("TELEGRAM_NOTIFY_URL")
	if telegramURL == "" {
		return errors.New("TELEGRAM_NOTIFY_URL is required")
	}
	addr := os.Getenv("WATCHDOG_ADDR")
	if addr == "" {
		addr = ":8090"
	}
	timeout, err := envDuration("WATCHDOG_TIMEOUT", 25*time.Minute)
	if err != nil {
		return err
	}
	checkEvery, err := envDuration("WATCHDOG_CHECK_EVERY", 30*time.Second)
	if err != nil {
		return err
	}

	tg := telegram.New(telegramURL, nil)
	send := func(ctx context.Context, text string) error {
		err := tg.SendText(ctx, text)
		if err != nil {
			log.Error("alert not delivered", "error", err)
		} else {
			log.Warn("alert delivered", "text", text)
		}

		return err
	}

	wd := watchdog.New(watchdog.Config{Timeout: timeout, CheckEvery: checkEvery}, send, nil)
	srv := &http.Server{Addr: addr, Handler: wd.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", srv.Addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", srv.Addr, err)
	}
	defer listener.Close()

	log.Info("watchdog started", "addr", listener.Addr().String(), "timeout", timeout, "check_every", checkEvery)
	go wd.Run(ctx)
	go func() {
		if err := srv.Serve(listener); !errors.Is(err, http.ErrServerClosed) {
			log.Error("server error", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Info("shutting down watchdog")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return srv.Shutdown(shutdownCtx)
}

func envDuration(key string, fallback time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration", key)
	}

	return d, nil
}
