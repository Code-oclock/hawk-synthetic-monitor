// Package http serves the monitor's operational endpoints: health, metrics, manual runs, history and stats.
package http

import (
	"context"
	"encoding/json"
	"errors"
	nethttp "net/http"
	"strconv"
	"time"

	"github.com/Code-oclock/hawk-synthetic-monitor/internal/entity"
	"github.com/Code-oclock/hawk-synthetic-monitor/internal/usecase"
)

const (
	defaultRuns = 20
	maxRuns     = 100
)

type Runner interface {
	Run(ctx context.Context, trigger entity.Trigger) (entity.RunResult, error)
}

type History interface {
	Recent(ctx context.Context, limit int) ([]entity.RunResult, error)
	Stats(ctx context.Context, since time.Time) (entity.Stats, error)
}

type server struct {
	runner  Runner
	history History
	now     func() time.Time
}

func NewHandler(runner Runner, history History, metrics nethttp.Handler, now func() time.Time) nethttp.Handler {
	if now == nil {
		now = time.Now
	}
	s := &server{runner: runner, history: history, now: now}

	mux := nethttp.NewServeMux()
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.Handle("GET /metrics", metrics)
	mux.HandleFunc("POST /run", s.run)
	mux.HandleFunc("GET /runs", s.runs)
	mux.HandleFunc("GET /stats", s.stats)

	return mux
}

func (s *server) healthz(w nethttp.ResponseWriter, _ *nethttp.Request) {
	writeJSON(w, nethttp.StatusOK, map[string]string{"status": "ok"})
}

func (s *server) run(w nethttp.ResponseWriter, r *nethttp.Request) {
	result, err := s.runner.Run(r.Context(), entity.TriggerManual)
	switch {
	case errors.Is(err, usecase.ErrBusy):
		writeError(w, nethttp.StatusConflict, "run already active")
	case err != nil:
		writeError(w, nethttp.StatusInternalServerError, err.Error())
	default:
		writeJSON(w, nethttp.StatusOK, toRunDTO(result))
	}
}

func (s *server) runs(w nethttp.ResponseWriter, r *nethttp.Request) {
	limit := defaultRuns
	if raw := r.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > maxRuns {
			writeError(w, nethttp.StatusBadRequest, "limit must be an integer from 1 to "+strconv.Itoa(maxRuns))

			return
		}
		limit = parsed
	}

	results, err := s.history.Recent(r.Context(), limit)
	if err != nil {
		writeError(w, nethttp.StatusInternalServerError, err.Error())

		return
	}

	out := make([]runDTO, 0, len(results))
	for _, result := range results {
		out = append(out, toRunDTO(result))
	}
	writeJSON(w, nethttp.StatusOK, out)
}

func (s *server) stats(w nethttp.ResponseWriter, r *nethttp.Request) {
	now := s.now()

	day, err := s.history.Stats(r.Context(), now.Add(-24*time.Hour))
	if err != nil {
		writeError(w, nethttp.StatusInternalServerError, err.Error())

		return
	}
	week, err := s.history.Stats(r.Context(), now.Add(-7*24*time.Hour))
	if err != nil {
		writeError(w, nethttp.StatusInternalServerError, err.Error())

		return
	}

	writeJSON(w, nethttp.StatusOK, map[string]statsDTO{"last_24h": toStatsDTO(day), "last_7d": toStatsDTO(week)})
}

func writeJSON(w nethttp.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w nethttp.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
