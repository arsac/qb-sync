//go:build e2e

package e2e

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestE2E_PerFileEarlyFinalization verifies that individual files in a multi-file
// torrent are renamed from .partial to their final path as soon as all their pieces
// are written, without waiting for the entire torrent to complete.
//
// Strategy: Wired CD has 18 audio files. During streaming, small files complete
// before larger ones. Early finalization renames these to their final paths while
// the torrent is still syncing (before FinalizeTorrent adds it to destination qBittorrent).
// We poll the destination filesystem for non-.partial files and verify at least one appears
// before the torrent is complete on destination qBittorrent.
func TestE2E_PerFileEarlyFinalization(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := SetupTestEnv(t)
	ctx := context.Background()

	env.CleanupBothSides(ctx, wiredCDHash)

	t.Log("Adding Wired CD torrent to source (18 files)...")
	env.DownloadTorrentOnSource(ctx, testTorrentURL, wiredCDHash, torrentDownloadTimeout)

	// Get file list from source to know what paths to look for on destination.
	files, err := env.SourceClient().GetFilesInformationCtx(ctx, wiredCDHash)
	require.NoError(t, err)
	require.NotNil(t, files)
	require.GreaterOrEqual(t, len(*files), 10, "Wired CD should have multiple files")
	t.Logf("Torrent has %d files", len(*files))

	// Collect expected final paths on destination filesystem.
	// Files are stored at destinationPath/<torrentName>/<fileName> (no saveSubPath without category).
	finalPaths := make([]string, len(*files))
	for i, f := range *files {
		finalPaths[i] = filepath.Join(env.DestinationPath(), f.Name)
	}

	// Start orchestrator.
	cfg := env.CreateSourceConfig()
	task, dest, err := env.CreateSourceTask(cfg)
	require.NoError(t, err)
	defer dest.Close()

	orchestratorCtx, cancelOrchestrator := context.WithTimeout(ctx, orchestratorTimeout)
	defer cancelOrchestrator()

	orchestratorDone := make(chan error, 1)
	go func() {
		orchestratorDone <- task.Run(orchestratorCtx)
	}()

	// Poll for early-finalized files: look for files at their final path
	// (no .partial suffix) while the torrent is NOT yet complete on destination qBittorrent.
	var earlyFinalizedCount atomic.Int32
	var observedBeforeComplete atomic.Bool

	earlyFinalizeCtx, earlyFinalizeCancel := context.WithTimeout(ctx, syncCompleteTimeout)
	defer earlyFinalizeCancel()

	// Background poller: check filesystem every 200ms.
	pollerDone := make(chan struct{})
	go func() {
		defer close(pollerDone)
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-earlyFinalizeCtx.Done():
				return
			case <-ticker.C:
			}

			// Count files at their final path (not .partial).
			var count int32
			for _, fp := range finalPaths {
				if _, statErr := os.Stat(fp); statErr == nil {
					count++
				}
			}
			if count == 0 {
				continue
			}

			earlyFinalizedCount.Store(count)

			// Check if torrent is NOT yet complete on destination qBittorrent.
			// If we see finalized files before torrent completion, that's
			// proof of per-file early finalization.
			if !env.IsTorrentCompleteOnDestination(ctx, wiredCDHash) {
				if observedBeforeComplete.CompareAndSwap(false, true) {
					t.Logf("Early finalization observed: %d files at final path before torrent completion", count)
				}
			}
		}
	}()

	// Wait for full sync.
	t.Log("Waiting for torrent to sync to destination...")
	env.WaitForTorrentCompleteOnDestination(ctx, wiredCDHash, syncCompleteTimeout,
		"torrent should be complete on destination")

	// Stop poller.
	earlyFinalizeCancel()
	<-pollerDone

	// Assert that we observed early-finalized files before torrent completion.
	assert.True(t, observedBeforeComplete.Load(),
		"should observe at least one file at final path before torrent completion on destination qBittorrent")
	t.Logf("Final early-finalized file count observed during streaming: %d", earlyFinalizedCount.Load())

	// Stop orchestrator.
	cancelOrchestrator()
	<-orchestratorDone

	// Verify ALL files are at their final path after sync completes.
	for _, fp := range finalPaths {
		assert.FileExists(t, fp, "file should exist at final path after sync: %s", fp)

		// Verify no leftover .partial files.
		partialPath := fp + ".partial"
		_, statErr := os.Stat(partialPath)
		assert.True(t, os.IsNotExist(statErr),
			".partial file should not exist after sync: %s", partialPath)
	}

	// Verify no .partial files remain anywhere under the destination path for this torrent.
	var leftoverPartials []string
	_ = filepath.WalkDir(env.DestinationPath(), func(path string, _ os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if strings.HasSuffix(path, ".partial") {
			leftoverPartials = append(leftoverPartials, path)
		}
		return nil
	})
	assert.Empty(t, leftoverPartials, "no .partial files should remain after sync")

	env.AssertTorrentCompleteOnDestination(ctx, wiredCDHash)

	t.Log("Per-file early finalization E2E test completed successfully!")

	env.CleanupBothSides(ctx, wiredCDHash)
}
