package destination

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"

	"golang.org/x/sync/semaphore"
)

func testLogger(t *testing.T) *slog.Logger {
	t.Helper()
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func slogDiscard() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// newTestDestServer creates a minimal Server for destination-path unit tests
// (write, finalization, early-finalize). Returns the server and its temp directory.
func newTestDestServer(t *testing.T) (*Server, string) {
	t.Helper()
	tmpDir := t.TempDir()
	logger := testLogger(t)
	bgCtx, bgCancel := context.WithCancel(context.Background())
	s := &Server{
		config:      ServerConfig{BasePath: tmpDir},
		logger:      logger,
		store:       newTorrentStore(tmpDir, logger),
		memBudget:   semaphore.NewWeighted(512 * 1024 * 1024),
		finalizeSem: semaphore.NewWeighted(1),
		bgCtx:       bgCtx,
		bgCancel:    bgCancel,
	}
	t.Cleanup(func() {
		bgCancel()
		s.bgWg.Wait()
	})
	return s, tmpDir
}
