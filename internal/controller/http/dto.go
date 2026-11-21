package http

import (
	"time"

	"github.com/Code-oclock/hawk-synthetic-monitor/internal/entity"
)

type failureDTO struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type stageDTO struct {
	Stage      string      `json:"stage"`
	Status     string      `json:"status"`
	DurationMS *int64      `json:"duration_ms,omitempty"`
	Attempts   int         `json:"attempts"`
	Failure    *failureDTO `json:"failure,omitempty"`
	SkipReason string      `json:"skip_reason,omitempty"`
}

type runDTO struct {
	ID                  string       `json:"id"`
	ProjectID           string       `json:"project_id"`
	Title               string       `json:"title"`
	Trigger             string       `json:"trigger"`
	Status              string       `json:"status"`
	StartedAt           time.Time    `json:"started_at"`
	FinishedAt          time.Time    `json:"finished_at"`
	DurationMS          int64        `json:"duration_ms"`
	Stages              []stageDTO   `json:"stages"`
	VisibilityLatencyMS *int64       `json:"visibility_latency_ms,omitempty"`
	WebhookLatencyMS    *int64       `json:"webhook_latency_ms,omitempty"`
	ReportingErrors     []failureDTO `json:"reporting_errors"`
}

type statsDTO struct {
	Total       int     `json:"total"`
	Passed      int     `json:"passed"`
	Failed      int     `json:"failed"`
	Canceled    int     `json:"canceled"`
	SuccessRate float64 `json:"success_rate"`
}

func toRunDTO(r entity.RunResult) runDTO {
	out := runDTO{
		ID:                  r.ID,
		ProjectID:           r.ProjectID,
		Title:               r.Title,
		Trigger:             string(r.Trigger),
		Status:              string(r.Status),
		StartedAt:           r.StartedAt,
		FinishedAt:          r.FinishedAt,
		DurationMS:          r.Duration.Milliseconds(),
		Stages:              make([]stageDTO, 0, len(r.Stages)),
		VisibilityLatencyMS: millis(r.VisibilityLatency),
		WebhookLatencyMS:    millis(r.WebhookLatency),
		ReportingErrors:     make([]failureDTO, 0, len(r.ReportingErrors)),
	}
	for _, s := range r.Stages {
		out.Stages = append(out.Stages, stageDTO{
			Stage:      string(s.Stage),
			Status:     string(s.Status),
			DurationMS: millis(s.Duration),
			Attempts:   s.Attempts,
			Failure:    toFailureDTO(s.Failure),
			SkipReason: s.SkipReason,
		})
	}
	for _, f := range r.ReportingErrors {
		out.ReportingErrors = append(out.ReportingErrors, failureDTO(f))
	}

	return out
}

func toStatsDTO(s entity.Stats) statsDTO {
	return statsDTO(s)
}

func toFailureDTO(f *entity.Failure) *failureDTO {
	if f == nil {
		return nil
	}
	dto := failureDTO(*f)

	return &dto
}

func millis(d *time.Duration) *int64 {
	if d == nil {
		return nil
	}
	ms := d.Milliseconds()

	return &ms
}
