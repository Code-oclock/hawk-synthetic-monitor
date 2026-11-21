// Package config reads the service settings from environment variables.
package config

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	CollectorURL     string
	APIURL           string
	HealthURL        string
	ProjectID        string
	IntegrationToken string
	Email            string
	Password         string

	Interval       time.Duration
	RunTimeout     time.Duration
	HealthTimeout  time.Duration
	SendTimeout    time.Duration
	VisibleTimeout time.Duration
	WebhookTimeout time.Duration
	PollInterval   time.Duration
	RetryBackoff   time.Duration
	MaxAttempts    int
	CleanupEnabled bool

	HTTPAddr     string
	WebhookAddr  string
	DBPath       string
	TelegramURL  string
	HeartbeatURL string

	RabbitURL      string
	RabbitUser     string
	RabbitPassword string
	RabbitVhost    string
	RabbitQueues   []string
}

type reader struct {
	get  func(string) string
	errs []error
}

// Load reads the configuration through get (usually os.Getenv) and validates it.
func Load(get func(string) string) (Config, error) {
	r := &reader{get: get}
	cfg := Config{
		CollectorURL:     r.required("HAWK_COLLECTOR_URL"),
		APIURL:           r.required("HAWK_API_URL"),
		HealthURL:        get("HAWK_HEALTH_URL"),
		ProjectID:        r.required("HAWK_PROJECT_ID"),
		IntegrationToken: r.required("HAWK_INTEGRATION_TOKEN"),
		Email:            r.required("HAWK_EMAIL"),
		Password:         r.required("HAWK_PASSWORD"),

		Interval:       r.duration("MONITOR_INTERVAL", 10*time.Minute),
		RunTimeout:     r.duration("MONITOR_RUN_TIMEOUT", 90*time.Second),
		HealthTimeout:  r.duration("MONITOR_HEALTH_TIMEOUT", 10*time.Second),
		SendTimeout:    r.duration("MONITOR_SEND_TIMEOUT", 15*time.Second),
		VisibleTimeout: r.duration("MONITOR_VISIBLE_TIMEOUT", 30*time.Second),
		WebhookTimeout: r.duration("MONITOR_WEBHOOK_TIMEOUT", 30*time.Second),
		PollInterval:   r.duration("MONITOR_POLL_INTERVAL", 500*time.Millisecond),
		RetryBackoff:   r.duration("MONITOR_RETRY_BACKOFF", time.Second),
		MaxAttempts:    r.integer("MONITOR_MAX_ATTEMPTS", 3),
		CleanupEnabled: r.boolean("MONITOR_CLEANUP", false),

		HTTPAddr:     r.str("MONITOR_HTTP_ADDR", ":8080"),
		WebhookAddr:  r.str("MONITOR_WEBHOOK_ADDR", ":8085"),
		DBPath:       r.str("MONITOR_DB_PATH", "monitor.db"),
		TelegramURL:  get("TELEGRAM_NOTIFY_URL"),
		HeartbeatURL: get("HEARTBEAT_URL"),

		RabbitURL:      get("RABBITMQ_MGMT_URL"),
		RabbitUser:     r.str("RABBITMQ_USER", "guest"),
		RabbitPassword: get("RABBITMQ_PASSWORD"),
		RabbitVhost:    r.str("RABBITMQ_VHOST", "/"),
		RabbitQueues:   r.list("RABBITMQ_QUEUES", []string{"errors/default", "grouper", "notifier", "sender/webhook"}),
	}

	if cfg.Interval < time.Second {
		r.errs = append(r.errs, errors.New("MONITOR_INTERVAL must be at least 1s"))
	}
	if cfg.RunTimeout <= cfg.VisibleTimeout || cfg.RunTimeout <= cfg.WebhookTimeout {
		r.errs = append(r.errs, errors.New("MONITOR_RUN_TIMEOUT must exceed the visible and webhook timeouts"))
	}

	if err := errors.Join(r.errs...); err != nil {
		return Config{}, fmt.Errorf("invalid configuration: %w", err)
	}

	return cfg, nil
}

func (r *reader) required(key string) string {
	value := strings.TrimSpace(r.get(key))
	if value == "" {
		r.errs = append(r.errs, fmt.Errorf("%s is required", key))
	}

	return value
}

func (r *reader) str(key, fallback string) string {
	if value := strings.TrimSpace(r.get(key)); value != "" {
		return value
	}

	return fallback
}

func (r *reader) duration(key string, fallback time.Duration) time.Duration {
	raw := strings.TrimSpace(r.get(key))
	if raw == "" {
		return fallback
	}

	value, err := time.ParseDuration(raw)
	if err != nil || value <= 0 {
		r.errs = append(r.errs, fmt.Errorf("%s must be a positive duration like 30s", key))

		return fallback
	}

	return value
}

func (r *reader) integer(key string, fallback int) int {
	raw := strings.TrimSpace(r.get(key))
	if raw == "" {
		return fallback
	}

	value, err := strconv.Atoi(raw)
	if err != nil || value < 1 {
		r.errs = append(r.errs, fmt.Errorf("%s must be a positive integer", key))

		return fallback
	}

	return value
}

func (r *reader) boolean(key string, fallback bool) bool {
	raw := strings.TrimSpace(r.get(key))
	if raw == "" {
		return fallback
	}

	value, err := strconv.ParseBool(raw)
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("%s must be true or false", key))

		return fallback
	}

	return value
}

func (r *reader) list(key string, fallback []string) []string {
	var items []string
	for _, item := range strings.Split(r.get(key), ",") {
		if item = strings.TrimSpace(item); item != "" {
			items = append(items, item)
		}
	}
	if len(items) == 0 {
		return fallback
	}

	return items
}
