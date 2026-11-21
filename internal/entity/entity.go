// Package entity holds the domain types of a monitoring run. It imports nothing from the project.
package entity

import "time"

type Status string

const (
	StatusRunning  Status = "running"
	StatusPassed   Status = "passed"
	StatusFailed   Status = "failed"
	StatusSkipped  Status = "skipped"
	StatusCanceled Status = "canceled"
)

type Stage string

const (
	StageHealth  Stage = "health"
	StageSend    Stage = "send"
	StageVisible Stage = "visible"
	StageWebhook Stage = "webhook"
)

type Trigger string

const (
	TriggerScheduled Trigger = "scheduled"
	TriggerManual    Trigger = "manual"
)

// Failure is a short machine-readable code plus a human message.
type Failure struct {
	Code    string
	Message string
}

// Event is a Hawk event as seen through the API.
type Event struct {
	ProjectID   string
	ID          string
	Title       string
	CatcherType string
	Type        string
	TotalCount  int
}

// DeliveryKey identifies a webhook delivery we are waiting for.
type DeliveryKey struct {
	ProjectID string
	Title     string
}

type Delivery struct {
	Key        DeliveryKey
	EventID    string
	ReceivedAt time.Time
}

type StageResult struct {
	Stage      Stage
	Status     Status
	StartedAt  *time.Time
	FinishedAt *time.Time
	// Duration is nil when the stage was skipped.
	Duration   *time.Duration
	Attempts   int
	Failure    *Failure
	SkipReason string
}

// ActionResult is the outcome of a side action such as cleanup or notification.
type ActionResult struct {
	Status  Status
	Failure *Failure
}

// QueueStat is the state of a RabbitMQ queue on the path of an event through Hawk.
type QueueStat struct {
	Name      string
	Messages  int
	Consumers int
	Missing   bool
}

// Diagnostics is a snapshot of Hawk taken right after a failed run.
type Diagnostics struct {
	CollectedAt time.Time
	// CollectorHealth is nil when the collector answered its health check.
	CollectorHealth *Failure
	Queues          []QueueStat
	// Errors lists problems met while collecting the snapshot itself.
	Errors []Failure
}

// RunResult describes one check of Hawk. Status reflects Hawk only; reporting problems go to ReportingErrors.
type RunResult struct {
	ID         string
	ProjectID  string
	Title      string
	Trigger    Trigger
	Status     Status
	StartedAt  time.Time
	FinishedAt time.Time
	Duration   time.Duration
	Stages     []StageResult
	Event      *Event
	Delivery   *Delivery

	VisibilityLatency *time.Duration
	WebhookLatency    *time.Duration

	// Diagnostics is set only for failed runs and only when a queue inspector is configured.
	Diagnostics *Diagnostics

	Cleanup         ActionResult
	Notification    ActionResult
	ReportingErrors []Failure
}

type AlertKind string

const (
	AlertFailure  AlertKind = "failure"
	AlertRecovery AlertKind = "recovery"
)

type Alert struct {
	Kind   AlertKind
	Result RunResult
}

// Stats aggregates run counts over a time window.
type Stats struct {
	Total       int
	Passed      int
	Failed      int
	Canceled    int
	SuccessRate float64
}
