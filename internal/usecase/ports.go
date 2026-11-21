// Package usecase runs a check of Hawk. It declares the interfaces it needs; adapters implement them.
package usecase

import (
	"context"
	"errors"
	"time"

	"github.com/Code-oclock/hawk-synthetic-monitor/internal/entity"
)

var (
	// ErrNotFound means the event is not visible yet.
	ErrNotFound = errors.New("event not found")
	// ErrTransient marks a failure worth retrying: network error, 5xx, 429.
	ErrTransient = errors.New("transient gateway failure")
	// ErrBusy means another run is already active.
	ErrBusy = errors.New("run already active")
)

type HawkGateway interface {
	Health(ctx context.Context) error
	// SendEvent posts an event with exactly this title.
	SendEvent(ctx context.Context, title string) error
	FindEvent(ctx context.Context, title string) (entity.Event, error)
}

type DeliveryInbox interface {
	// Subscribe registers interest before the event is sent, so an early webhook is not lost.
	Subscribe(key entity.DeliveryKey) (DeliveryWaiter, error)
}

type DeliveryWaiter interface {
	Wait(ctx context.Context) (entity.Delivery, error)
	// Close unsubscribes; it is idempotent.
	Close()
}

type ResultRepo interface {
	// Save upserts a result by run ID.
	Save(ctx context.Context, result entity.RunResult) error
	// Latest returns the last passed or failed result, or nil if there is none.
	Latest(ctx context.Context) (*entity.RunResult, error)
}

type Notifier interface {
	Notify(ctx context.Context, alert entity.Alert) error
}

type Cleaner interface {
	RemoveEvent(ctx context.Context, event entity.Event) error
}

// Heartbeat tells an external watcher that the monitor is alive and its checks are running.
type Heartbeat interface {
	Ping(ctx context.Context, result entity.RunResult) error
}

type Observer interface {
	Observe(result entity.RunResult)
}

type Clock interface {
	Now() time.Time
}

type Options struct {
	ProjectID        string
	RunTimeout       time.Duration
	HealthTimeout    time.Duration
	SendTimeout      time.Duration
	VisibleTimeout   time.Duration
	WebhookTimeout   time.Duration
	PollInterval     time.Duration
	ReportingTimeout time.Duration
	RetryBackoff     time.Duration
	MaxAttempts      int
	CleanupEnabled   bool
}
