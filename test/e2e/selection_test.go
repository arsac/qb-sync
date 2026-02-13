//go:build e2e

package e2e

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/autobrr/go-qbittorrent"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestE2E_PartialFileSelection verifies that only selected files are synced to
// destination when some files are deselected (priority 0) in qBittorrent.
//
// Strategy:
//  1. Add Wired CD torrent to source and deselect files 5, 6, 7 before download
//  2. Wait for selected files to download (progress reaches 1.0)
//  3. Start orchestrator — it sees the selection on the first cycle
//  4. Wait for sync to destination
//  5. Verify: selected files exist on destination, deselected files do NOT
func TestE2E_PartialFileSelection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := SetupTestEnv(t)
	ctx := context.Background()

	env.CleanupBothSides(ctx, wiredCDHash)

	// Deselected file indices (0-based).
	deselectedIndices := map[int]bool{5: true, 6: true, 7: true}

	// Step 1: Add torrent to source in stopped state so we can set priorities
	// before download begins.
	t.Log("Adding Wired CD torrent to source (stopped)...")
	err := env.AddTorrentToSource(ctx, testTorrentURL, map[string]string{
		"stopped": "true",
	})
	require.NoError(t, err)

	torrent := env.WaitForTorrent(env.SourceClient(), wiredCDHash, torrentAppearTimeout)
	require.NotNil(t, torrent)
	t.Logf("Torrent added: %s", torrent.Name)

	// Wait for metadata to be available (files list populated).
	var files *qbittorrent.TorrentFiles
	require.Eventually(t, func() bool {
		f, filesErr := env.SourceClient().GetFilesInformationCtx(ctx, wiredCDHash)
		if filesErr != nil || f == nil || len(*f) == 0 {
			return false
		}
		files = f
		return true
	}, 30*time.Second, time.Second, "torrent metadata should be available")
	require.GreaterOrEqual(t, len(*files), 10, "Wired CD should have multiple files")
	t.Logf("Torrent has %d files", len(*files))

	// Step 2: Set file priorities — deselect files 5, 6, 7 (priority 0).
	ids := make([]string, 0, len(deselectedIndices))
	for idx := range deselectedIndices {
		ids = append(ids, strconv.Itoa(idx))
	}
	t.Logf("Deselecting files: %v", ids)
	err = env.SourceClient().SetFilePriorityCtx(ctx, wiredCDHash, strings.Join(ids, "|"), 0)
	require.NoError(t, err)

	// Verify priorities were applied.
	files, err = env.SourceClient().GetFilesInformationCtx(ctx, wiredCDHash)
	require.NoError(t, err)
	for _, f := range *files {
		if deselectedIndices[f.Index] {
			require.Equal(t, 0, f.Priority,
				"file %d (%s) should have priority 0", f.Index, f.Name)
		} else {
			require.Positive(t, f.Priority,
				"file %d (%s) should have priority > 0", f.Index, f.Name)
		}
	}
	t.Log("File priorities confirmed")

	// Step 3: Resume torrent and wait for download to complete.
	t.Log("Resuming torrent...")
	err = env.SourceClient().ResumeCtx(ctx, []string{wiredCDHash})
	require.NoError(t, err)

	t.Log("Waiting for selected files to download...")
	env.WaitForTorrentComplete(env.SourceClient(), wiredCDHash, torrentDownloadTimeout)
	t.Log("Download complete (selected files only)")

	// Build expected paths on destination filesystem.
	type fileExpectation struct {
		name     string
		destPath string
		selected bool
	}
	expectations := make([]fileExpectation, len(*files))
	for i, f := range *files {
		expectations[i] = fileExpectation{
			name:     f.Name,
			destPath: filepath.Join(env.DestinationPath(), f.Name),
			selected: !deselectedIndices[f.Index],
		}
	}

	// Step 4: Start orchestrator — file selection is already set, so the
	// orchestrator picks it up on the first InitTorrent call.
	t.Log("Starting orchestrator...")
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

	// Step 5: Wait for sync to complete on destination.
	t.Log("Waiting for torrent to sync to destination...")
	env.WaitForTorrentCompleteOnDestination(ctx, wiredCDHash, syncCompleteTimeout,
		"torrent with partial file selection should sync to destination")

	// Stop orchestrator.
	cancelOrchestrator()
	<-orchestratorDone

	env.AssertTorrentCompleteOnDestination(ctx, wiredCDHash)

	// Step 6: Verify files on destination filesystem.
	t.Log("Verifying file selection on destination filesystem...")
	for _, exp := range expectations {
		_, statErr := os.Stat(exp.destPath)
		if exp.selected {
			require.NoError(t, statErr,
				"selected file should exist on destination: %s", exp.name)
		} else {
			assert.True(t, os.IsNotExist(statErr),
				"deselected file should NOT exist on destination: %s", exp.name)
		}
	}

	// Verify no .partial files remain for selected files.
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

	// Verify destination qBittorrent also reports the torrent as partial.
	destFiles, err := env.DestinationClient().GetFilesInformationCtx(ctx, wiredCDHash)
	require.NoError(t, err)
	require.NotNil(t, destFiles)
	t.Logf("Destination torrent has %d files", len(*destFiles))

	for _, cf := range *destFiles {
		if deselectedIndices[cf.Index] {
			assert.Equal(t, 0, cf.Priority,
				"deselected file %d should have priority 0 on destination", cf.Index)
		}
	}

	t.Log("Partial file selection E2E test completed successfully!")
	t.Logf("  - Total files: %d", len(*files))
	t.Logf("  - Selected: %d, Deselected: %d", len(*files)-len(deselectedIndices), len(deselectedIndices))

	env.CleanupBothSides(ctx, wiredCDHash)
}
