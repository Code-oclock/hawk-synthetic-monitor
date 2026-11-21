// Command monitor runs the Hawk synthetic monitor.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/Code-oclock/hawk-synthetic-monitor/internal/adapter/hawk"
	"github.com/Code-oclock/hawk-synthetic-monitor/internal/usecase"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run() error {
	client := hawk.New(hawk.Config{
		CollectorURL:     os.Getenv("HAWK_COLLECTOR_URL"),
		APIURL:           os.Getenv("HAWK_API_URL"),
		IntegrationToken: os.Getenv("HAWK_INTEGRATION_TOKEN"),
		ProjectID:        os.Getenv("HAWK_PROJECT_ID"),
		Email:            os.Getenv("HAWK_EMAIL"),
		Password:         os.Getenv("HAWK_PASSWORD"),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	title := fmt.Sprintf("Synthetic check %d", time.Now().UnixNano())

	start := time.Now()
	if err := client.SendEvent(ctx, title); err != nil {
		return err
	}
	sent := time.Since(start)

	for {
		_, err := client.FindEvent(ctx, title)
		if err == nil {
			break
		}
		if !errors.Is(err, usecase.ErrNotFound) {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}

	fmt.Printf("sent in %v, visible after %v\n", sent, time.Since(start)-sent)

	return nil
}
