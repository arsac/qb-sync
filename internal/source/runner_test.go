package source

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
	dest       *mockDest
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

	cfg := &config.SourceConfig{
		DrainAnnotation: annotation,
	}

	mockClient := &mockQBClient{
		getTorrentsResult: []qbittorrent.Torrent{
			{Hash: "abc123", SeedingTime: 9999, Size: 1000},
		},
	}
	dest := &mockDest{}

	completed := newTorrentStore("", 0, logger)
	completed.MarkComplete("abc123", "")
	task := &QBTask{
		cfg:       cfg,
		logger:    logger,
		srcClient: mockClient,
		grpcDest:  dest,
		source:    qbclient.NewSource(nil, ""),
		store:     completed,
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
		dest:       dest,
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

// TestWaitForDestination pins that a destination which is not up yet is a
// condition the source waits out, not one it exits on. Exiting turns a
// destination that is merely slow to start into a CrashLoopBackOff, and under a
// deploy timeout into a release that can never converge.
func TestWaitForDestination(t *testing.T) {
	t.Parallel()

	newRunner := func(t *testing.T) *Runner {
		t.Helper()
		return &Runner{
			cfg:              &config.SourceConfig{DestinationAddr: "dest:50051"},
			logger:           testLogger(t),
			destWaitInterval: time.Millisecond,
		}
	}

	t.Run("waits out early failures instead of returning them", func(t *testing.T) {
		t.Parallel()
		var calls int
		err := newRunner(t).waitForDestination(context.Background(), func(context.Context) error {
			calls++
			if calls < 3 {
				return errors.New("connection refused")
			}
			return nil
		})
		if err != nil {
			t.Fatalf("expected the wait to succeed once the destination arrived, got %v", err)
		}
		if calls != 3 {
			t.Errorf("attempts = %d, want 3", calls)
		}
	})

	t.Run("returns when the context ends", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(context.Background())
		var calls int
		done := make(chan error, 1)

		go func() {
			done <- newRunner(t).waitForDestination(ctx, func(context.Context) error {
				if calls++; calls == 2 {
					cancel()
				}
				return errors.New("connection refused")
			})
		}()

		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Errorf("err = %v, want context.Canceled", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("waitForDestination did not return after its context was cancelled")
		}
	})

	t.Run("a health check that fails instantly does not spin", func(t *testing.T) {
		t.Parallel()
		r := newRunner(t)
		r.destWaitInterval = 20 * time.Millisecond

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		var calls int
		_ = r.waitForDestination(ctx, func(context.Context) error {
			calls++
			return errors.New("NOT_SERVING") // returns immediately, unlike WaitForReady
		})

		// ~5 windows fit in the budget. A hot loop would run into the thousands.
		if calls > 20 {
			t.Errorf("attempts = %d, want the retry to be paced by the interval", calls)
		}
	})
}
