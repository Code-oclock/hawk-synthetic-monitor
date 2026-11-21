package sqlite_test

import (
	"context"
	"fmt"
	"math"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Code-oclock/hawk-synthetic-monitor/internal/adapter/sqlite"
	"github.com/Code-oclock/hawk-synthetic-monitor/internal/entity"
)

func TestSave_LatestRoundtrip(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "roundtrip.db")
	repo, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()
	dur1 := 125 * time.Millisecond
	dur2 := 350 * time.Millisecond
	now := time.Now().Truncate(time.Millisecond)

	original := entity.RunResult{
		ID:         "run-rt-1",
		ProjectID:  "proj-alpha",
		Title:      "Roundtrip Verification",
		Trigger:    entity.TriggerScheduled,
		Status:     entity.StatusPassed,
		StartedAt:  now.Add(-2 * time.Minute),
		FinishedAt: now.Add(-time.Minute),
		Duration:   time.Minute,
		Stages: []entity.StageResult{
			{
				Stage:    entity.StageHealth,
				Status:   entity.StatusPassed,
				Duration: &dur1,
			},
			{
				Stage:    entity.StageSend,
				Status:   entity.StatusPassed,
				Duration: &dur2,
			},
		},
	}

	if err := repo.Save(ctx, original); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	latest, err := repo.Latest(ctx)
	if err != nil {
		t.Fatalf("Latest failed: %v", err)
	}
	if latest == nil {
		t.Fatal("Latest returned nil, expected result")
	}

	if latest.ID != original.ID {
		t.Errorf("ID mismatch: got %q, want %q", latest.ID, original.ID)
	}
	if latest.Status != original.Status {
		t.Errorf("Status mismatch: got %q, want %q", latest.Status, original.Status)
	}
	if latest.Title != original.Title {
		t.Errorf("Title mismatch: got %q, want %q", latest.Title, original.Title)
	}
	if len(latest.Stages) != len(original.Stages) {
		t.Fatalf("Stages count mismatch: got %d, want %d", len(latest.Stages), len(original.Stages))
	}
	for i := range original.Stages {
		gotDur := latest.Stages[i].Duration
		wantDur := original.Stages[i].Duration
		if gotDur == nil || wantDur == nil {
			t.Fatalf("Stage %d duration nil: got %v, want %v", i, gotDur, wantDur)
		}
		if *gotDur != *wantDur {
			t.Errorf("Stage %d duration mismatch: got %v, want %v", i, *gotDur, *wantDur)
		}
	}
}

func TestSave_UpsertReplacesResult(t *testing.T) {
	t.Parallel()

	repo, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open :memory: failed: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()
	runID := "run-upsert-1"
	t0 := time.Now().Add(-10 * time.Minute)

	first := entity.RunResult{
		ID:        runID,
		Title:     "Initial Title",
		Status:    entity.StatusRunning,
		StartedAt: t0,
	}
	if err := repo.Save(ctx, first); err != nil {
		t.Fatalf("Save first failed: %v", err)
	}

	updated := entity.RunResult{
		ID:        runID,
		Title:     "Updated Title",
		Status:    entity.StatusPassed,
		StartedAt: t0,
		Duration:  5 * time.Second,
	}
	if err := repo.Save(ctx, updated); err != nil {
		t.Fatalf("Save updated failed: %v", err)
	}

	recent, err := repo.Recent(ctx, 10)
	if err != nil {
		t.Fatalf("Recent failed: %v", err)
	}
	if len(recent) != 1 {
		t.Fatalf("Expected 1 row after upsert, got %d", len(recent))
	}
	if recent[0].ID != runID {
		t.Errorf("ID mismatch: got %q, want %q", recent[0].ID, runID)
	}
	if recent[0].Status != entity.StatusPassed {
		t.Errorf("Status mismatch: got %q, want %q", recent[0].Status, entity.StatusPassed)
	}
	if recent[0].Title != "Updated Title" {
		t.Errorf("Title mismatch: got %q, want %q", recent[0].Title, "Updated Title")
	}
}

func TestLatest_IgnoresRunningAndCanceled_EmptyDB(t *testing.T) {
	t.Parallel()

	repo, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()

	// Empty DB returns nil, nil.
	latest, err := repo.Latest(ctx)
	if err != nil {
		t.Fatalf("Latest on empty DB returned error: %v", err)
	}
	if latest != nil {
		t.Fatalf("Latest on empty DB returned %v, want nil", latest)
	}

	t0 := time.Now().Add(-5 * time.Minute)

	// Save running result: Latest must ignore it.
	err = repo.Save(ctx, entity.RunResult{
		ID:        "run-running",
		Status:    entity.StatusRunning,
		StartedAt: t0,
	})
	if err != nil {
		t.Fatalf("Save running failed: %v", err)
	}
	latest, err = repo.Latest(ctx)
	if err != nil {
		t.Fatalf("Latest returned error: %v", err)
	}
	if latest != nil {
		t.Fatalf("Latest should ignore running status, got: %v", latest)
	}

	// Save canceled result: Latest must ignore it.
	err = repo.Save(ctx, entity.RunResult{
		ID:        "run-canceled",
		Status:    entity.StatusCanceled,
		StartedAt: t0.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("Save canceled failed: %v", err)
	}
	latest, err = repo.Latest(ctx)
	if err != nil {
		t.Fatalf("Latest returned error: %v", err)
	}
	if latest != nil {
		t.Fatalf("Latest should ignore canceled status, got: %v", latest)
	}

	// Save failed result: Latest should return it.
	err = repo.Save(ctx, entity.RunResult{
		ID:        "run-failed",
		Status:    entity.StatusFailed,
		StartedAt: t0.Add(2 * time.Minute),
	})
	if err != nil {
		t.Fatalf("Save failed status failed: %v", err)
	}
	latest, err = repo.Latest(ctx)
	if err != nil {
		t.Fatalf("Latest returned error: %v", err)
	}
	if latest == nil || latest.ID != "run-failed" {
		t.Fatalf("Latest expected run-failed, got: %v", latest)
	}

	// Save newer running result: Latest should still return run-failed.
	err = repo.Save(ctx, entity.RunResult{
		ID:        "run-running-newer",
		Status:    entity.StatusRunning,
		StartedAt: t0.Add(3 * time.Minute),
	})
	if err != nil {
		t.Fatalf("Save newer running failed: %v", err)
	}
	latest, err = repo.Latest(ctx)
	if err != nil {
		t.Fatalf("Latest returned error: %v", err)
	}
	if latest == nil || latest.ID != "run-failed" {
		t.Fatalf("Latest expected run-failed to remain latest valid, got: %v", latest)
	}

	// Save newer passed result: Latest should return run-passed.
	err = repo.Save(ctx, entity.RunResult{
		ID:        "run-passed",
		Status:    entity.StatusPassed,
		StartedAt: t0.Add(4 * time.Minute),
	})
	if err != nil {
		t.Fatalf("Save passed failed: %v", err)
	}
	latest, err = repo.Latest(ctx)
	if err != nil {
		t.Fatalf("Latest returned error: %v", err)
	}
	if latest == nil || latest.ID != "run-passed" {
		t.Fatalf("Latest expected run-passed, got: %v", latest)
	}
}

func TestRecent_OrderAndLimit(t *testing.T) {
	t.Parallel()

	repo, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()
	baseTime := time.Now().Add(-10 * time.Minute)

	for i := 0; i < 5; i++ {
		err := repo.Save(ctx, entity.RunResult{
			ID:        fmt.Sprintf("run-%d", i),
			Status:    entity.StatusPassed,
			StartedAt: baseTime.Add(time.Duration(i) * time.Minute),
		})
		if err != nil {
			t.Fatalf("Save run-%d failed: %v", i, err)
		}
	}

	// Limit 3: newest first (run-4, run-3, run-2).
	recent3, err := repo.Recent(ctx, 3)
	if err != nil {
		t.Fatalf("Recent(3) failed: %v", err)
	}
	if len(recent3) != 3 {
		t.Fatalf("Recent(3) length got %d, want 3", len(recent3))
	}
	expected3 := []string{"run-4", "run-3", "run-2"}
	for i, wantID := range expected3 {
		if recent3[i].ID != wantID {
			t.Errorf("Recent(3)[%d] got %q, want %q", i, recent3[i].ID, wantID)
		}
	}

	// Limit 10: all 5 runs newest first.
	recentAll, err := repo.Recent(ctx, 10)
	if err != nil {
		t.Fatalf("Recent(10) failed: %v", err)
	}
	if len(recentAll) != 5 {
		t.Fatalf("Recent(10) length got %d, want 5", len(recentAll))
	}
	expectedAll := []string{"run-4", "run-3", "run-2", "run-1", "run-0"}
	for i, wantID := range expectedAll {
		if recentAll[i].ID != wantID {
			t.Errorf("Recent(10)[%d] got %q, want %q", i, recentAll[i].ID, wantID)
		}
	}

	// Limit <= 0 returns empty slice.
	recent0, err := repo.Recent(ctx, 0)
	if err != nil {
		t.Fatalf("Recent(0) failed: %v", err)
	}
	if len(recent0) != 0 {
		t.Errorf("Recent(0) length got %d, want 0", len(recent0))
	}
}

func TestStats_CountsAndSuccessRateWithSince(t *testing.T) {
	t.Parallel()

	repo, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()
	now := time.Now().Truncate(time.Second)

	runs := []struct {
		id     string
		status entity.Status
		offset time.Duration
	}{
		{id: "run-old", status: entity.StatusPassed, offset: -2 * time.Hour},
		{id: "run-pass-1", status: entity.StatusPassed, offset: -45 * time.Minute},
		{id: "run-pass-2", status: entity.StatusPassed, offset: -30 * time.Minute},
		{id: "run-fail-1", status: entity.StatusFailed, offset: -15 * time.Minute},
		{id: "run-canc-1", status: entity.StatusCanceled, offset: -5 * time.Minute},
		{id: "run-run-1", status: entity.StatusRunning, offset: -1 * time.Minute},
	}

	for _, r := range runs {
		err := repo.Save(ctx, entity.RunResult{
			ID:        r.id,
			Status:    r.status,
			StartedAt: now.Add(r.offset),
		})
		if err != nil {
			t.Fatalf("Save %s failed: %v", r.id, err)
		}
	}

	// Filter since 1 hour ago (excludes run-old).
	since := now.Add(-1 * time.Hour)
	stats, err := repo.Stats(ctx, since)
	if err != nil {
		t.Fatalf("Stats failed: %v", err)
	}

	// Total within last hour: pass-1, pass-2, fail-1, canc-1, run-1 => 5 runs.
	if stats.Total != 5 {
		t.Errorf("Stats.Total got %d, want 5", stats.Total)
	}
	if stats.Passed != 2 {
		t.Errorf("Stats.Passed got %d, want 2", stats.Passed)
	}
	if stats.Failed != 1 {
		t.Errorf("Stats.Failed got %d, want 1", stats.Failed)
	}
	if stats.Canceled != 1 {
		t.Errorf("Stats.Canceled got %d, want 1", stats.Canceled)
	}

	expectedRate := 2.0 / 3.0
	if math.Abs(stats.SuccessRate-expectedRate) > 1e-9 {
		t.Errorf("Stats.SuccessRate got %f, want %f", stats.SuccessRate, expectedRate)
	}

	// Future filter: no runs match -> SuccessRate = 0.
	futureStats, err := repo.Stats(ctx, now.Add(time.Hour))
	if err != nil {
		t.Fatalf("Stats future failed: %v", err)
	}
	if futureStats.Total != 0 || futureStats.SuccessRate != 0 {
		t.Errorf("Future Stats unexpected: %+v", futureStats)
	}

	// Filter matching only the canceled run: Passed+Failed = 0 -> SuccessRate = 0.
	cancOnlyStats, err := repo.Stats(ctx, now.Add(-10*time.Minute))
	// Within -10 mins: run-canc-1 (canceled) and run-run-1 (running)
	if err != nil {
		t.Fatalf("Stats canceled-only failed: %v", err)
	}
	if cancOnlyStats.Passed != 0 || cancOnlyStats.Failed != 0 {
		t.Errorf("Unexpected pass/fail in cancOnlyStats: %+v", cancOnlyStats)
	}
	if cancOnlyStats.SuccessRate != 0 {
		t.Errorf("SuccessRate for 0 pass/fail should be 0, got %f", cancOnlyStats.SuccessRate)
	}
}

func TestPersistence(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "persist.db")

	repo1, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("Open 1 failed: %v", err)
	}

	ctx := context.Background()
	run := entity.RunResult{
		ID:        "run-persist-1",
		Title:     "Persistence Check",
		Status:    entity.StatusPassed,
		StartedAt: time.Now(),
	}

	if err := repo1.Save(ctx, run); err != nil {
		t.Fatalf("Save failed: %v", err)
	}
	if err := repo1.Close(); err != nil {
		t.Fatalf("Close 1 failed: %v", err)
	}

	repo2, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("Open 2 failed: %v", err)
	}
	defer repo2.Close()

	latest, err := repo2.Latest(ctx)
	if err != nil {
		t.Fatalf("Latest failed on reopened repo: %v", err)
	}
	if latest == nil {
		t.Fatal("Latest returned nil on reopened repo")
	}
	if latest.ID != run.ID {
		t.Errorf("Reopened ID mismatch: got %q, want %q", latest.ID, run.ID)
	}
	if latest.Title != run.Title {
		t.Errorf("Reopened Title mismatch: got %q, want %q", latest.Title, run.Title)
	}
}

func TestConcurrentSaves(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "concurrent.db")
	repo, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer repo.Close()

	const workers = 8
	const iterations = 10
	var wg sync.WaitGroup
	wg.Add(workers)

	for w := 0; w < workers; w++ {
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				run := entity.RunResult{
					ID:        fmt.Sprintf("worker-%d-run-%d", workerID, i),
					Status:    entity.StatusPassed,
					StartedAt: time.Now(),
				}
				if err := repo.Save(context.Background(), run); err != nil {
					t.Errorf("worker %d save %d failed: %v", workerID, i, err)
					return
				}
			}
		}(w)
	}

	wg.Wait()

	recent, err := repo.Recent(context.Background(), workers*iterations+10)
	if err != nil {
		t.Fatalf("Recent failed after concurrent saves: %v", err)
	}
	if len(recent) != workers*iterations {
		t.Errorf("Recent count mismatch: got %d, want %d", len(recent), workers*iterations)
	}
}

func TestCanceledContext(t *testing.T) {
	t.Parallel()

	repo, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer repo.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	run := entity.RunResult{
		ID:        "run-canceled-ctx",
		Status:    entity.StatusPassed,
		StartedAt: time.Now(),
	}

	if err := repo.Save(ctx, run); err == nil {
		t.Error("Save with canceled context expected error, got nil")
	}

	if _, err := repo.Latest(ctx); err == nil {
		t.Error("Latest with canceled context expected error, got nil")
	}

	if _, err := repo.Recent(ctx, 10); err == nil {
		t.Error("Recent with canceled context expected error, got nil")
	}

	if _, err := repo.Stats(ctx, time.Now()); err == nil {
		t.Error("Stats with canceled context expected error, got nil")
	}
}
