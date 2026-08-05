//go:build e2e

package e2e

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/autobrr/go-qbittorrent"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/arsac/qb-sync/internal/destination"
)

// Reproductions for the two failure modes described in
// docs/specs/2026-08-01-quarantine-and-reclamation.md. Both were identified by
// reading the code; these tests exist to confirm they are real before anything
// is changed. See docs/adr/0001 and docs/adr/0002 for the decisions they
// motivate.

const (
	// reproObservationWindow bounds how long we wait for the stall to be
	// noticed and quarantined. It must comfortably exceed reproGuard plus the
	// stall threshold, which is two orchestrator cycles.
	reproObservationWindow = 3 * time.Minute

	// reproGuard shrinks the quarantine guard from its 4h default. The retry
	// backoff is derived from it, so this one knob shrinks the whole mechanism.
	reproGuard = 10 * time.Second

	// reproRateLimit throttles streaming so a 53 MB torrent cannot complete
	// before the test has a chance to observe or intervene mid-transfer.
	reproRateLimit = 1024 * 1024 // 1 MiB/s

	// reproOrphanTimeout / reproOrphanInterval shrink the destination's
	// reclamation schedule. E2E builds ServerConfig directly and never calls
	// Validate, so these are not bound by minOrphanTimeout.
	reproOrphanTimeout  = 5 * time.Second
	reproOrphanInterval = 2 * time.Second
)

// torrentContentPaths resolves the on-disk source path of every file in the
// torrent, mirroring the layout qBittorrent reports.
func torrentContentPaths(t *testing.T, env *TestEnv, hash string) []string {
	t.Helper()

	ctx := context.Background()
	files, err := env.SourceClient().GetFilesInformationCtx(ctx, hash)
	require.NoError(t, err, "listing torrent files on source")
	require.NotNil(t, files)

	torrents, err := env.SourceClient().GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
		Hashes: []string{hash},
	})
	require.NoError(t, err)
	require.Len(t, torrents, 1)

	paths := make([]string, 0, len(*files))
	for _, f := range *files {
		rel := filepath.FromSlash(f.Name)
		// Try the save-path-relative location first, then the layout that
		// includes the torrent name as a root folder.
		candidates := []string{
			filepath.Join(env.SourcePath(), rel),
			filepath.Join(env.SourcePath(), torrents[0].Name, rel),
		}
		for _, path := range candidates {
			if _, statErr := os.Stat(path); statErr == nil {
				paths = append(paths, path)
				break
			}
		}
	}

	require.NotEmpty(t, paths, "expected to resolve at least one content file")
	return paths
}

// truncateTorrentContent truncates every file of the torrent on the source
// data path to zero bytes, leaving the directory entries in place.
//
// This makes every piece permanently unreadable without deleting anything:
// ReadPiece's ENOENT retry does not fire (the files still exist), so the read
// fails with a short read on every attempt. qBittorrent is not told, and does
// not recheck on its own, so the source continues to report every piece as
// downloaded. That is exactly the state the source orchestrator cannot
// currently escape.
func truncateTorrentContent(t *testing.T, env *TestEnv, hash string) int {
	t.Helper()

	paths := torrentContentPaths(t, env, hash)
	for _, path := range paths {
		require.NoError(t, os.Truncate(path, 0), "truncating %s", path)
	}

	t.Logf("Truncated %d content files to zero bytes", len(paths))
	return len(paths)
}

// TestE2E_Repro_UnreadableSourcePieceWedgesTorrent verifies that a torrent
// whose source data cannot be read is quarantined rather than retried forever.
//
// Before the stall clock existed, every send failed and the next poll simply
// re-queued the piece. Nothing counted those failures - the failed bitmap is
// never read and RetryFailed is never called - so the torrent never completed
// streaming, never reached finalization, and therefore never reached the
// failure streak that would quarantine it. It stayed tracked indefinitely,
// which the first version of this test pinned.
//
// The guard is shrunk to seconds here. That works through the single
// --sync-failed-guard knob because the retry backoff is derived from it rather
// than being an independent constant.
func TestE2E_Repro_UnreadableSourcePieceWedgesTorrent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	t.Parallel()

	env := SetupTestEnv(t)
	ctx := context.Background()

	env.CleanupBothSides(ctx, wiredCDHash)

	t.Log("Downloading torrent on source...")
	env.DownloadTorrentOnSource(ctx, testTorrentURL, wiredCDHash, torrentDownloadTimeout)

	// Break the source data before the orchestrator ever reads it. Doing this
	// up front rather than mid-stream removes the race that would otherwise
	// let a 53 MB torrent finish before we could intervene.
	truncateTorrentContent(t, env, wiredCDHash)

	cfg := env.CreateSourceConfig()
	cfg.MaxBytesPerSec = reproRateLimit
	cfg.SyncFailedGuard = reproGuard

	task, dest, err := env.CreateSourceTask(cfg)
	require.NoError(t, err)
	defer dest.Close()

	orchestratorCtx, cancel := context.WithTimeout(ctx, reproObservationWindow+time.Minute)
	defer cancel()

	go func() { _ = task.Run(orchestratorCtx) }()

	t.Log("Waiting for the orchestrator to start tracking the torrent...")
	require.Eventually(t, func() bool {
		_, progressErr := task.Progress(ctx, wiredCDHash)
		return progressErr == nil
	}, time.Minute, 250*time.Millisecond, "torrent should be tracked despite unreadable data")

	t.Logf("Waiting up to %s for the stall to outlast the %s guard...",
		reproObservationWindow, reproGuard)

	require.Eventually(t, func() bool {
		return env.SourceTorrentHasTag(ctx, wiredCDHash, defaultSyncFailedTag)
	}, reproObservationWindow, 2*time.Second,
		"an unreadable torrent must be quarantined once the stall outlasts the guard, not wedge forever")

	// Quarantine must also release it: a torrent left in tracked keeps the
	// active-torrents gauge elevated and keeps consuming sync capacity.
	_, progressErr := task.Progress(ctx, wiredCDHash)
	assert.Error(t, progressErr, "a quarantined torrent must be untracked")
}

// TestE2E_Repro_OrphanIsNotReclaimed is the regression test for ADR-0002: the
// destination must reclaim disk from a transfer whose source went away.
//
// Judging orphan-ness on store membership cannot work, which is what this
// guards. The cleaner would skip any torrent present in the store, and recovery
// repopulates the store from every unfinalized metadata directory at startup,
// so a transfer left without an explicit abort would be shielded permanently.
func TestE2E_Repro_OrphanIsNotReclaimed(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	t.Parallel()

	env := SetupTestEnv(t, WithDestinationServerConfig(func(cfg *destination.ServerConfig) {
		cfg.OrphanTimeout = reproOrphanTimeout
		cfg.OrphanCleanupInterval = reproOrphanInterval
	}))
	ctx := context.Background()

	env.CleanupBothSides(ctx, wiredCDHash)

	t.Log("Downloading torrent on source...")
	env.DownloadTorrentOnSource(ctx, testTorrentURL, wiredCDHash, torrentDownloadTimeout)

	cfg := env.CreateSourceConfig()
	cfg.MaxBytesPerSec = reproRateLimit // keep the transfer partial
	task, dest, err := env.CreateSourceTask(cfg)
	require.NoError(t, err)

	orchestratorCtx, cancelOrchestrator := context.WithCancel(ctx)

	go func() { _ = task.Run(orchestratorCtx) }()

	t.Log("Waiting for streaming to begin so partial data exists on the destination...")
	require.Eventually(t, func() bool {
		progress, progressErr := task.Progress(ctx, wiredCDHash)
		return progressErr == nil && progress.Streamed > 0 && !progress.Complete
	}, 2*time.Minute, 250*time.Millisecond, "streaming should start and still be incomplete")

	destMetaDir := filepath.Join(env.DestinationPath(), ".qbsync", wiredCDHash)
	require.DirExists(t, destMetaDir, "destination metadata directory should exist mid-transfer")
	require.NotEmpty(t, env.PartialFiles(), "destination should hold .partial files mid-transfer")

	// Abandon the transfer the way a crashed or decommissioned source does:
	// stop talking to the destination, without deleting the torrent from
	// source qBittorrent and without sending an abort.
	t.Log("Abandoning the transfer (simulating a source crash)...")
	cancelOrchestrator()
	dest.Close()

	// Give the cleaner several scan intervals past the timeout.
	wait := reproOrphanTimeout + 6*reproOrphanInterval
	t.Logf("Waiting %s for the destination to reclaim the orphan...", wait)

	require.Eventually(t, func() bool {
		_, statErr := os.Stat(destMetaDir)
		return os.IsNotExist(statErr)
	}, wait, time.Second,
		"BUG: orphan is never reclaimed, its metadata directory persists forever")

	assert.Empty(t, env.PartialFiles(),
		"reclamation should remove the .partial files it wrote")
}
