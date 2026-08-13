//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/arsac/qb-sync/internal/destination"
)

// minResumePieces is how much must reach the destination before the fault is
// injected. It only has to be large enough that resuming is distinguishable
// from starting over.
const minResumePieces = 50

// backupTorrentContent copies the torrent's source files aside and returns a
// restore function.
//
// The stall is forced by destroying the source data, so releasing the torrent
// afterwards only means anything once the source can read again - which is what
// the operator who cleared the fault would have done before removing the tag.
func backupTorrentContent(t *testing.T, env *TestEnv, hash string) func() {
	t.Helper()

	backupDir := t.TempDir()
	saved := make(map[string]string)
	for i, path := range torrentContentPaths(t, env, hash) {
		data, err := os.ReadFile(path)
		require.NoError(t, err, "reading %s", path)

		backup := filepath.Join(backupDir, fmt.Sprintf("%d.bin", i))
		require.NoError(t, os.WriteFile(backup, data, 0o600))
		saved[path] = backup
	}

	return func() {
		t.Helper()
		for path, backup := range saved {
			data, err := os.ReadFile(backup)
			require.NoError(t, err)
			// The file still exists (truncated, not removed), so its original
			// mode is preserved and the perm argument does not apply.
			require.NoError(t, os.WriteFile(path, data, 0o600), "restoring %s", path)
		}
		t.Logf("Restored %d content files", len(saved))
	}
}

// TestE2E_QuarantineReleaseResumesRatherThanRecopies covers stories 17-19 of
// docs/specs/2026-08-01-quarantine-and-reclamation.md: quarantining releases
// the destination's hold, preserves the bytes already transferred, and a later
// release resumes from them instead of re-copying.
//
// This is the operator's recovery path. A four-hour destination outage is the
// case the guard exists to tolerate, and the whole point of holding the bytes
// through quarantine is that clearing the fault must not cost a full re-copy.
func TestE2E_QuarantineReleaseResumesRatherThanRecopies(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	t.Parallel()

	// A long orphan timeout keeps reclamation out of this test. What the
	// cleaner eventually does to an unreleased torrent is ADR-0002's subject
	// and is covered separately; here the torrent is released well inside the
	// reclamation window.
	env := SetupTestEnv(t, WithDestinationServerConfig(func(cfg *destination.ServerConfig) {
		cfg.OrphanTimeout = time.Hour
	}))
	ctx := context.Background()

	env.CleanupBothSides(ctx, wiredCDHash)

	t.Log("Downloading torrent on source...")
	env.DownloadTorrentOnSource(ctx, testTorrentURL, wiredCDHash, torrentDownloadTimeout)
	restoreContent := backupTorrentContent(t, env, wiredCDHash)

	cfg := env.CreateSourceConfig()
	cfg.MaxBytesPerSec = reproRateLimit // keep the transfer partial
	cfg.SyncFailedGuard = reproGuard

	task, dest, err := env.CreateSourceTask(cfg)
	require.NoError(t, err)
	defer dest.Close()

	orchestratorCtx, cancelOrchestrator := context.WithCancel(ctx)
	defer cancelOrchestrator()

	go func() { _ = task.Run(orchestratorCtx) }()

	t.Logf("Waiting for at least %d pieces to reach the destination...", minResumePieces)
	var streamedBeforeQuarantine int
	require.Eventually(t, func() bool {
		progress, progressErr := task.Progress(ctx, wiredCDHash)
		if progressErr != nil {
			return false
		}
		streamedBeforeQuarantine = progress.Streamed
		return progress.Streamed >= minResumePieces
	}, 2*time.Minute, 250*time.Millisecond, "a partial transfer should build up before the fault")

	t.Logf("Streamed %d pieces before quarantine", streamedBeforeQuarantine)
	require.NotEmpty(t, env.PartialFiles(), "destination should hold .partial files mid-transfer")

	// Break the source so the transfer stalls and the guard eventually fires.
	truncateTorrentContent(t, env, wiredCDHash)

	t.Logf("Waiting up to %s for the stall to outlast the %s guard...",
		reproObservationWindow, reproGuard)
	require.Eventually(t, func() bool {
		return env.SourceTorrentHasTag(ctx, wiredCDHash, defaultSyncFailedTag)
	}, reproObservationWindow, 2*time.Second,
		"a stalled torrent must be quarantined once the stall outlasts the guard")

	// Story 18: quarantining preserves the data already transferred.
	assert.NotEmpty(t, env.PartialFiles(),
		"quarantine must keep the bytes already transferred, or releasing means re-copying everything")

	// Story 17: the source releases its hold, so the destination is free to
	// reclaim the disk if nobody ever releases the torrent.
	//
	// Polled rather than read once: markSyncFailed applies the tag before it
	// untracks, so seeing the tag does not by itself mean the release has
	// happened yet.
	require.Eventually(t, func() bool {
		_, progressErr := task.Progress(ctx, wiredCDHash)
		return progressErr != nil
	}, 30*time.Second, 250*time.Millisecond, "a quarantined torrent must be untracked on the source")

	// The operator clears the fault and releases the torrent.
	restoreContent()
	require.NoError(t,
		env.SourceClient().RemoveTagsCtx(ctx, []string{wiredCDHash}, defaultSyncFailedTag),
		"removing the sync-failed tag releases the torrent")

	// Story 19: the release resumes from the retained bytes. The quarantine's
	// abort dropped the destination's in-memory entry, so a just-re-tracked
	// torrent reads 0 until the streaming queue's lazy InitTorrent surfaces the
	// persisted bitmap - skip those readings rather than judging them. The
	// first non-zero reading still discriminates resume from re-copy: a resume
	// jumps straight to the retained count in one in-memory pass, while a
	// re-copy would climb through small counts across many polls at the
	// throttled transfer rate.
	var streamedAfterRelease int
	require.Eventually(t, func() bool {
		progress, retrackErr := task.Progress(ctx, wiredCDHash)
		if retrackErr != nil || progress.Streamed == 0 {
			return false
		}
		streamedAfterRelease = progress.Streamed
		return true
	}, time.Minute, 250*time.Millisecond,
		"a released torrent must be tracked again and seeded from the destination")

	assert.GreaterOrEqual(t, streamedAfterRelease, streamedBeforeQuarantine,
		"a released torrent must resume from the destination's retained pieces, not re-copy from zero")
	t.Logf("Resumed at %d pieces (%d before quarantine)", streamedAfterRelease, streamedBeforeQuarantine)

	env.WaitForTorrentCompleteOnDestination(ctx, wiredCDHash, syncCompleteTimeout,
		"the released torrent should finish syncing")
}
