//go:build e2e

package e2e

import (
	"context"
	"os"
	"path/filepath"
	"strings"
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
	// reproObservationWindow is how long we watch a torrent that should be
	// making progress but is not. Long enough that a merely slow transfer
	// would have advanced, short enough to keep the suite usable.
	reproObservationWindow = 45 * time.Second

	// reproRateLimit throttles streaming so a 53 MB torrent cannot complete
	// before the test has a chance to observe or intervene mid-transfer.
	reproRateLimit = 1024 * 1024 // 1 MiB/s

	// reproOrphanTimeout / reproOrphanInterval shrink the destination's
	// reclamation schedule. E2E builds ServerConfig directly and never calls
	// Validate, so these are not bound by minOrphanTimeout.
	reproOrphanTimeout  = 5 * time.Second
	reproOrphanInterval = 2 * time.Second
)

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

	ctx := context.Background()
	files, err := env.SourceClient().GetFilesInformationCtx(ctx, hash)
	require.NoError(t, err, "listing torrent files on source")
	require.NotNil(t, files)

	torrents, err := env.SourceClient().GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
		Hashes: []string{hash},
	})
	require.NoError(t, err)
	require.Len(t, torrents, 1)

	// The torrent's content lives under the source data path, mirroring the
	// layout qBittorrent reports. Derive the root from the first file's path.
	var truncated int
	for _, f := range *files {
		rel := filepath.FromSlash(f.Name)
		// Try the save-path-relative location first, then the layout that
		// includes the torrent name as a root folder.
		candidates := []string{
			filepath.Join(env.SourcePath(), rel),
			filepath.Join(env.SourcePath(), torrents[0].Name, rel),
		}
		for _, path := range candidates {
			if _, statErr := os.Stat(path); statErr != nil {
				continue
			}
			require.NoError(t, os.Truncate(path, 0), "truncating %s", path)
			truncated++
			break
		}
	}

	require.NotZero(t, truncated, "expected to truncate at least one content file")
	t.Logf("Truncated %d of %d content files to zero bytes", truncated, len(*files))
	return truncated
}

// countPartialFiles returns the number of .partial files under the destination
// data path. These are the files qb-sync itself wrote; anything at a final
// path was pre-existing, hardlinked, or deselected.
func countPartialFiles(t *testing.T, env *TestEnv) int {
	t.Helper()

	var n int
	walkErr := filepath.Walk(env.DestinationPath(), func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // unreadable entries are not our concern here
		}
		if !info.IsDir() && strings.HasSuffix(path, ".partial") {
			n++
		}
		return nil
	})
	require.NoError(t, walkErr)
	return n
}

// TestE2E_Repro_UnreadableSourcePieceWedgesTorrent characterises the wedge.
//
// When source data cannot be read, every send fails and the piece is simply
// re-queued by the next poll. Nothing counts those failures: the failed bitmap
// is never read and RetryFailed is never called. The torrent therefore never
// reaches full streaming, never reaches finalization, and so never reaches the
// retry cap that would quarantine it. It stays tracked indefinitely.
//
// This test pins that behaviour so the fix has something to invert. It is
// expected to PASS against the current code, demonstrating the bug, and should
// be rewritten to assert quarantine once the guard exists.
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
	task, dest, err := env.CreateSourceTask(cfg)
	require.NoError(t, err)
	defer dest.Close()

	orchestratorCtx, cancel := context.WithTimeout(ctx, reproObservationWindow+30*time.Second)
	defer cancel()

	go func() { _ = task.Run(orchestratorCtx) }()

	t.Log("Waiting for the orchestrator to start tracking the torrent...")
	require.Eventually(t, func() bool {
		_, progressErr := task.Progress(ctx, wiredCDHash)
		return progressErr == nil
	}, time.Minute, 250*time.Millisecond, "torrent should be tracked despite unreadable data")

	t.Logf("Observing for %s...", reproObservationWindow)
	time.Sleep(reproObservationWindow)

	progress, err := task.Progress(ctx, wiredCDHash)
	require.NoError(t, err, "torrent should STILL be tracked: this is the wedge")
	t.Logf("After %s: streamed=%d/%d complete=%v failed=%d",
		reproObservationWindow, progress.Streamed, progress.TotalPieces, progress.Complete, progress.Failed)

	assert.False(t, progress.Complete,
		"torrent cannot complete: its source data is unreadable")
	assert.Less(t, progress.Streamed, progress.TotalPieces,
		"streaming must be stuck short of the total")

	// The heart of the bug: no terminal state is ever reached. The torrent is
	// still tracked, and the source torrent carries no sync-failed tag, so it
	// will be retried forever with no operator signal.
	torrents, err := env.SourceClient().GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
		Hashes: []string{wiredCDHash},
	})
	require.NoError(t, err)
	require.Len(t, torrents, 1)

	assert.NotContains(t, torrents[0].Tags, "sync-failed",
		"BUG: an unreadable torrent is never quarantined, it wedges in tracked forever")
}

// TestE2E_Repro_AbandonedTransferIsNotReclaimed demonstrates that the
// destination cannot reclaim disk from a transfer whose source went away.
//
// The orphan cleaner skips any torrent present in the store, and recovery
// repopulates the store from every unfinalized metadata directory at startup.
// So a transfer abandoned without an explicit abort is shielded permanently.
//
// This test asserts the DESIRED behaviour and is expected to FAIL against the
// current code. It becomes the regression test for ADR-0002.
func TestE2E_Repro_AbandonedTransferIsNotReclaimed(t *testing.T) {
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
	require.NotZero(t, countPartialFiles(t, env), "destination should hold .partial files mid-transfer")

	// Abandon the transfer the way a crashed or decommissioned source does:
	// stop talking to the destination, without deleting the torrent from
	// source qBittorrent and without sending an abort.
	t.Log("Abandoning the transfer (simulating a source crash)...")
	cancelOrchestrator()
	dest.Close()

	// Give the cleaner several scan intervals past the timeout.
	wait := reproOrphanTimeout + 6*reproOrphanInterval
	t.Logf("Waiting %s for the destination to reclaim the abandoned transfer...", wait)

	require.Eventually(t, func() bool {
		_, statErr := os.Stat(destMetaDir)
		return os.IsNotExist(statErr)
	}, wait, time.Second,
		"BUG: abandoned transfer is never reclaimed, its metadata directory persists forever")

	assert.Zero(t, countPartialFiles(t, env),
		"reclamation should remove the .partial files it wrote")
}
