// Package metrics exposes check results as Prometheus metrics.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Code-oclock/hawk-synthetic-monitor/internal/entity"
	"github.com/Code-oclock/hawk-synthetic-monitor/internal/usecase"
)

var _ usecase.Observer = (*Metrics)(nil)

var buckets = []float64{.01, .05, .1, .25, .5, 1, 2.5, 5, 10, 30}

type Metrics struct {
	runs          *prometheus.CounterVec
	stages        *prometheus.CounterVec
	stageDuration *prometheus.HistogramVec
	visibility    prometheus.Histogram
	webhook       prometheus.Histogram
	lastRun       prometheus.Gauge
	lastSuccess   prometheus.Gauge
	lastPassed    prometheus.Gauge
}

// New registers all collectors on reg.
func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		runs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hawk_monitor_runs_total", Help: "Finished checks by status.",
		}, []string{"status"}),
		stages: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hawk_monitor_stage_total", Help: "Stage outcomes.",
		}, []string{"stage", "status"}),
		stageDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "hawk_monitor_stage_duration_seconds", Help: "Stage duration.", Buckets: buckets,
		}, []string{"stage"}),
		visibility: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "hawk_monitor_visibility_latency_seconds", Help: "Send to visible in the API.", Buckets: buckets,
		}),
		webhook: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "hawk_monitor_webhook_latency_seconds", Help: "Send to webhook delivery.", Buckets: buckets,
		}),
		lastRun: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "hawk_monitor_last_run_timestamp_seconds", Help: "Finish time of the last run.",
		}),
		lastSuccess: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "hawk_monitor_last_success_timestamp_seconds", Help: "Finish time of the last passed run.",
		}),
		lastPassed: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "hawk_monitor_last_run_passed", Help: "1 if the last finished run passed, 0 if it failed.",
		}),
	}
	reg.MustRegister(m.runs, m.stages, m.stageDuration, m.visibility, m.webhook, m.lastRun, m.lastSuccess, m.lastPassed)

	return m
}

// Observe records one finished run.
func (m *Metrics) Observe(result entity.RunResult) {
	m.runs.WithLabelValues(string(result.Status)).Inc()

	for _, stage := range result.Stages {
		m.stages.WithLabelValues(string(stage.Stage), string(stage.Status)).Inc()
		if stage.Duration != nil && (stage.Status == entity.StatusPassed || stage.Status == entity.StatusFailed) {
			m.stageDuration.WithLabelValues(string(stage.Stage)).Observe(stage.Duration.Seconds())
		}
	}

	if result.VisibilityLatency != nil {
		m.visibility.Observe(result.VisibilityLatency.Seconds())
	}
	if result.WebhookLatency != nil {
		m.webhook.Observe(result.WebhookLatency.Seconds())
	}

	if result.Status == entity.StatusCanceled {
		return
	}

	finished := float64(result.FinishedAt.Unix())
	m.lastRun.Set(finished)
	if result.Status == entity.StatusPassed {
		m.lastSuccess.Set(finished)
		m.lastPassed.Set(1)
	} else {
		m.lastPassed.Set(0)
	}
}

// Handler serves the metrics gathered from g.
func (m *Metrics) Handler(g prometheus.Gatherer) http.Handler {
	return promhttp.HandlerFor(g, promhttp.HandlerOpts{})
}
