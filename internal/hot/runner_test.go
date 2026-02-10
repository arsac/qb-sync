package hot

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/autobrr/go-qbittorrent"

	"github.com/arsac/qb-sync/internal/config"
	"github.com/arsac/qb-sync/internal/qbclient"
)

// shutdownDrainFixture holds the components for testing shutdownDrain.
type shutdownDrainFixture struct {
	runner     *Runner
	task       *QBTask
	mockClient *mockQBClient
	coldDest   *mockColdDest
}

// drainWasCalled returns true if the drain handoff was attempted (stop is the
// first step in deleteGroupFromHot).
func (f *shutdownDrainFixture) drainWasCalled() bool {
	return f.mockClient.stopCalled
}

func setupShutdownDrain(
	t *testing.T,
	annotation string,
	checkFn func(ctx context.Context, key string) (bool, error),
) *shutdownDrainFixture {
	t.Helper()
	logger := testLogger(t)

	cfg := &config.HotConfig{
		DrainAnnotation: annotation,
	}

	mockClient := &mockQBClient{
		getTorrentsResult: []qbittorrent.Torrent{
			{Hash: "abc123", SeedingTime: 9999, Size: 1000},
		},
	}
	coldDest := &mockColdDest{}

	task := &QBTask{
		cfg:             cfg,
		logger:          logger,
		srcClient:       mockClient,
		grpcDest:        coldDest,
		source:          qbclient.NewSource(nil, ""),
		completedOnCold: map[string]bool{"abc123": true},
		trackedTorrents: make(map[string]trackedTorrent),
	}

	r := &Runner{
		cfg:             cfg,
		logger:          logger,
		checkAnnotation: checkFn,
	}

	return &shutdownDrainFixture{
		runner:     r,
		task:       task,
		mockClient: mockClient,
		coldDest:   coldDest,
	}
}

func TestShutdownDrain(t *testing.T) {
	t.Run("no annotation configured drains unconditionally", func(t *testing.T) {
		f := setupShutdownDrain(t, "", nil)

		f.runner.shutdownDrain(f.task)

		if !f.drainWasCalled() {
			t.Error("drain should have been called when no annotation is configured")
		}
	})

	t.Run("annotation true allows drain", func(t *testing.T) {
		f := setupShutdownDrain(t, "qbsync/drain", func(_ context.Context, key string) (bool, error) {
			if key != "qbsync/drain" {
				t.Errorf("expected annotation key 'qbsync/drain', got %q", key)
			}
			return true, nil
		})

		f.runner.shutdownDrain(f.task)

		if !f.drainWasCalled() {
			t.Error("drain should have been called when annotation is true")
		}
	})

	t.Run("annotation false skips drain", func(t *testing.T) {
		f := setupShutdownDrain(t, "qbsync/drain", func(_ context.Context, _ string) (bool, error) {
			return false, nil
		})

		f.runner.shutdownDrain(f.task)

		if f.drainWasCalled() {
			t.Error("drain should NOT have been called when annotation is false")
		}
	})

	t.Run("annotation check error skips drain", func(t *testing.T) {
		f := setupShutdownDrain(t, "qbsync/drain", func(_ context.Context, _ string) (bool, error) {
			return false, errors.New("k8s API unreachable")
		})

		f.runner.shutdownDrain(f.task)

		if f.drainWasCalled() {
			t.Error("drain should NOT have been called when annotation check fails")
		}
	})

	t.Run("uses configured drain timeout", func(t *testing.T) {
		var capturedDeadline time.Time
		var hasDeadline bool

		f := setupShutdownDrain(t, "qbsync/drain", func(ctx context.Context, _ string) (bool, error) {
			capturedDeadline, hasDeadline = ctx.Deadline()
			return true, nil
		})
		f.runner.cfg.DrainTimeout = 3 * time.Minute

		f.runner.shutdownDrain(f.task)

		if !hasDeadline {
			t.Fatal("context should have a deadline")
		}
		remaining := time.Until(capturedDeadline)
		if remaining < 2*time.Minute || remaining > 4*time.Minute {
			t.Errorf("expected deadline ~3m from now, got %v remaining", remaining)
		}
	})

	t.Run("defaults to 5 minute timeout when DrainTimeout is zero", func(t *testing.T) {
		var capturedDeadline time.Time
		var hasDeadline bool

		f := setupShutdownDrain(t, "qbsync/drain", func(ctx context.Context, _ string) (bool, error) {
			capturedDeadline, hasDeadline = ctx.Deadline()
			return true, nil
		})
		// DrainTimeout is zero (default) → should default to 5m

		f.runner.shutdownDrain(f.task)

		if !hasDeadline {
			t.Fatal("context should have a deadline")
		}
		remaining := time.Until(capturedDeadline)
		if remaining < 4*time.Minute || remaining > 6*time.Minute {
			t.Errorf("expected deadline ~5m from now, got %v remaining", remaining)
		}
	})
}
