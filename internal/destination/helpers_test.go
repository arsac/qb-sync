package destination

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"golang.org/x/sync/semaphore"
)

func testLogger(t *testing.T) *slog.Logger {
	t.Helper()
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func slogDiscard() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

// newTestDestServer creates a minimal Server for destination-path unit tests
// (write, finalization, early-finalize). Returns the server and its temp directory.
func newTestDestServer(t *testing.T) (*Server, string) {
	t.Helper()
	tmpDir := t.TempDir()
	logger := testLogger(t)
	bgCtx, bgCancel := context.WithCancel(context.Background())
	s := &Server{
		config:           ServerConfig{BasePath: tmpDir},
		logger:           logger,
		store:            newTorrentStore(tmpDir, logger),
		memBudget:        semaphore.NewWeighted(512 * 1024 * 1024),
		finalizeSem:      semaphore.NewWeighted(1),
		qbStageSem:       semaphore.NewWeighted(1),
		earlyFinalizeSem: semaphore.NewWeighted(defaultStreamWorkers),
		bgCtx:            bgCtx,
		bgCancel:         bgCancel,
		processStart:     time.Now(),
	}
	t.Cleanup(func() {
		bgCancel()
		s.bgWg.Wait()
	})
	return s, tmpDir
}

// waitEarlyFinalize blocks until every background early finalization started
// for state has landed, so a test can assert on the file's post-finalize state.
func waitEarlyFinalize(t *testing.T, state *serverTorrentState) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		state.mu.Lock()
		inFlight := state.earlyFinalizing
		state.mu.Unlock()
		if inFlight == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("early finalization did not complete: %d still in flight", inFlight)
		}
		time.Sleep(time.Millisecond)
	}
}
