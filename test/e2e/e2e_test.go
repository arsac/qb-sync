//go:build e2e

package e2e

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/autobrr/go-qbittorrent"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/arsac/qb-sync/internal/utils"
	pb "github.com/arsac/qb-sync/proto"
)

const (
	// Big Buck Bunny - small, fast to download test torrent.
	bigBuckBunnyURL  = "https://webtorrent.io/torrents/big-buck-bunny.torrent"
	bigBuckBunnyHash = "dd8255ecdc7ca55fb0bbf81323d87062db1f6d1c"

	// Common test timeouts.
	torrentDownloadTimeout = 5 * time.Minute
	syncCompleteTimeout    = 3 * time.Minute
	orchestratorTimeout    = 5 * time.Minute
	pollInterval           = 2 * time.Second
	shortPollInterval      = time.Second
)

func TestE2E_QBitTorrentConnectivity(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	t.Parallel()

	env := SetupTestEnv(t)
	ctx := context.Background()

	// Verify source qBittorrent is accessible
	version, err := env.SourceClient().GetAppVersionCtx(ctx)
	require.NoError(t, err)
	t.Logf("Source qBittorrent version: %s", version)

	// Verify destination qBittorrent is accessible
	version, err = env.DestinationClient().GetAppVersionCtx(ctx)
	require.NoError(t, err)
	t.Logf("Destination qBittorrent version: %s", version)
}

func TestE2E_GRPCConnectivity(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	t.Parallel()

	env := SetupTestEnv(t)
	ctx := context.Background()

	// Create gRPC destination and verify connectivity
	dest, err := env.CreateGRPCDestination()
	require.NoError(t, err)
	defer dest.Close()

	// Validate connection
	err = dest.ValidateConnection(ctx)
	require.NoError(t, err, "gRPC connection should be valid")
}

func TestE2E_AddTorrentToSource(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	t.Parallel()

	env := SetupTestEnv(t)
	ctx := context.Background()

	// Cleanup any existing torrents
	env.CleanupTorrent(ctx, env.SourceClient(), wiredCDHash)

	// Add torrent to source
	err := env.AddTorrentToSource(ctx, testTorrentURL, nil)
	require.NoError(t, err)

	// Wait for torrent
	torrent := env.WaitForTorrent(env.SourceClient(), wiredCDHash, 30*time.Second)
	require.NotNil(t, torrent)
	t.Logf("Added torrent: %s (%s)", torrent.Name, torrent.Hash)

	// Cleanup
	env.CleanupTorrent(ctx, env.SourceClient(), wiredCDHash)
}

func TestE2E_SourceTaskCreation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	t.Parallel()

	env := SetupTestEnv(t)

	cfg := env.CreateSourceConfig(WithDryRun(true))
	task, dest, err := env.CreateSourceTask(cfg)
	require.NoError(t, err)
	require.NotNil(t, task)
	defer dest.Close()

	t.Log("Successfully created source task with gRPC destination")
}

func TestE2E_DryRunDoesNotDelete(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	t.Parallel()

	env := SetupTestEnv(t)
	ctx := context.Background()

	// Cleanup any existing torrents
	env.CleanupTorrent(ctx, env.SourceClient(), wiredCDHash)

	// Add torrent to source
	err := env.AddTorrentToSource(ctx, testTorrentURL, nil)
	require.NoError(t, err)

	// Wait for torrent
	torrent := env.WaitForTorrent(env.SourceClient(), wiredCDHash, 30*time.Second)
	require.NotNil(t, torrent)

	// Create task in dry run mode and drain to bypass space check
	cfg := env.CreateSourceConfig(WithDryRun(true), WithMinSeedingTime(0))
	task, dest, err := env.CreateSourceTask(cfg)
	require.NoError(t, err)
	defer dest.Close()

	// Login to source client
	err = task.Login(ctx)
	require.NoError(t, err)

	// Mark torrent as complete on destination (simulates sync without actually syncing)
	task.MarkCompletedOnDestination(wiredCDHash)

	// Drain in dry run mode
	err = task.Drain(ctx)
	require.NoError(t, err)

	// Torrent should still exist on source (dry run)
	torrents, err := env.SourceClient().GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
		Hashes: []string{wiredCDHash},
	})
	require.NoError(t, err)
	assert.Len(t, torrents, 1, "torrent should still exist on source in dry run mode")

	// Cleanup
	env.CleanupTorrent(ctx, env.SourceClient(), wiredCDHash)
}

func TestE2E_InitTorrentOnDestination(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	t.Parallel()

	env := SetupTestEnv(t)
	ctx := context.Background()

	// Create gRPC destination
	dest, err := env.CreateGRPCDestination()
	require.NoError(t, err)
	defer dest.Close()

	// Add torrent to source first to get metadata
	env.CleanupTorrent(ctx, env.SourceClient(), wiredCDHash)
	err = env.AddTorrentToSource(ctx, testTorrentURL, nil)
	require.NoError(t, err)

	// Wait for torrent
	torrent := env.WaitForTorrent(env.SourceClient(), wiredCDHash, 30*time.Second)
	require.NotNil(t, torrent)

	// Get torrent properties for piece info
	props, err := env.SourceClient().GetTorrentPropertiesCtx(ctx, wiredCDHash)
	require.NoError(t, err)

	// Export torrent file
	torrentData, err := env.SourceClient().ExportTorrentCtx(ctx, wiredCDHash)
	require.NoError(t, err)

	// Get file info
	files, err := env.SourceClient().GetFilesInformationCtx(ctx, wiredCDHash)
	require.NoError(t, err)
	require.NotNil(t, files)

	// Build file info for proto
	var pbFiles []*pb.FileInfo
	var offset int64
	for _, f := range *files {
		pbFiles = append(pbFiles, &pb.FileInfo{
			Path:   f.Name,
			Size:   f.Size,
			Offset: offset,
		})
		offset += f.Size
	}

	// Initialize on destination via gRPC
	_, err = dest.InitTorrent(ctx, &pb.InitTorrentRequest{
		TorrentHash: wiredCDHash,
		Name:        torrent.Name,
		NumPieces:   int32(props.PiecesNum),
		PieceSize:   int64(props.PieceSize),
		TotalSize:   torrent.Size,
		TorrentFile: torrentData,
		Files:       pbFiles,
	})
	require.NoError(t, err)

	t.Log("Successfully initialized torrent on destination server")

	// Cleanup
	env.CleanupTorrent(ctx, env.SourceClient(), wiredCDHash)
}

// TestE2E_FullSyncFlow tests the complete source->destination sync flow:
// 1. Download torrent on source
// 2. Run source orchestrator to stream pieces to destination
// 3. Verify torrent is finalized and added to destination qBittorrent
// 4. Verify synced tag is added on source.
func TestE2E_FullSyncFlow(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	t.Parallel()

	env := SetupTestEnv(t)
	ctx := context.Background()

	env.CleanupBothSides(ctx, bigBuckBunnyHash)

	t.Log("Adding Big Buck Bunny torrent to source...")
	env.DownloadTorrentOnSource(ctx, bigBuckBunnyURL, bigBuckBunnyHash, 5*time.Minute)

	cfg := env.CreateSourceConfig()
	task, dest, err := env.CreateSourceTask(cfg)
	require.NoError(t, err)
	defer dest.Close()

	orchestratorCtx, cancelOrchestrator := context.WithTimeout(ctx, 3*time.Minute)
	defer cancelOrchestrator()

	orchestratorDone := make(chan error, 1)
	go func() {
		orchestratorDone <- task.Run(orchestratorCtx)
	}()

	t.Log("Waiting for torrent to be synced to destination...")
	env.WaitForTorrentCompleteOnDestination(ctx, bigBuckBunnyHash, 3*time.Minute, "torrent should be complete on destination")

	// Wait for the orchestrator to finish post-finalization work (tag application)
	// before canceling. WaitForTorrentCompleteOnDestination returns as soon as the torrent
	// appears on destination qBittorrent, but the FinalizeTorrent RPC may still be in-flight
	// on the source side — canceling now would kill it before tags are applied.
	require.Eventually(t, func() bool {
		torrents, tagErr := env.SourceClient().GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
			Hashes: []string{bigBuckBunnyHash},
		})
		if tagErr != nil || len(torrents) == 0 {
			return false
		}
		return strings.Contains(torrents[0].Tags, "synced")
	}, 30*time.Second, time.Second, "source torrent should have 'synced' tag before shutdown")

	cancelOrchestrator()
	<-orchestratorDone

	env.AssertTorrentCompleteOnDestination(ctx, bigBuckBunnyHash)

	// Verify destination qBittorrent's savePath is the container mount point, not the source's path.
	destTorrents, err := env.DestinationClient().GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
		Hashes: []string{bigBuckBunnyHash},
	})
	require.NoError(t, err)
	require.Len(t, destTorrents, 1)
	assert.Equal(t, "/destination-data", destTorrents[0].SavePath,
		"destination torrent savePath should be the container mount point")

	// Verify synced tag is applied on destination
	assert.Contains(t, destTorrents[0].Tags, "synced",
		"destination torrent should have 'synced' tag")

	// Verify synced tag is applied on source
	sourceTorrents, err := env.SourceClient().GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
		Hashes: []string{bigBuckBunnyHash},
	})
	require.NoError(t, err)
	require.Len(t, sourceTorrents, 1)
	assert.Contains(t, sourceTorrents[0].Tags, "synced",
		"source torrent should have 'synced' tag")

	t.Log("Full sync flow completed successfully!")

	env.CleanupBothSides(ctx, bigBuckBunnyHash)
}

// TestE2E_TempPathSync tests the full sync flow when temp_path_enabled is set
// on the source qBittorrent instance. When temp_path is enabled, qBittorrent
// downloads files into the temp directory first, then moves them to save_path
// on completion. The orchestrator must resolve ContentPath (which points at
// the temp dir during download) to find pieces on disk.
// Starting the orchestrator before the download completes maximizes the chance
// of exercising the temp path reading code.
func TestE2E_TempPathSync(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	t.Parallel()

	env := SetupTestEnv(t, WithSourceTempPath())
	ctx := context.Background()

	// Verify temp path is enabled on source qBittorrent.
	prefs, err := env.SourceClient().GetAppPreferencesCtx(ctx)
	require.NoError(t, err)
	require.True(t, prefs.TempPathEnabled, "temp_path_enabled should be true")
	require.Equal(t, "/incomplete", prefs.TempPath, "temp_path should be /incomplete")
	t.Logf("Source qBittorrent prefs: temp_path_enabled=%v, temp_path=%s", prefs.TempPathEnabled, prefs.TempPath)

	env.CleanupBothSides(ctx, wiredCDHash)

	// Add torrent to source.
	t.Log("Adding Wired CD torrent to source (with temp_path enabled)...")
	err = env.AddTorrentToSource(ctx, testTorrentURL, nil)
	require.NoError(t, err)

	torrent := env.WaitForTorrent(env.SourceClient(), wiredCDHash, torrentAppearTimeout)
	require.NotNil(t, torrent)

	// Log torrent paths — DownloadPath and ContentPath should reference /incomplete
	// while the torrent is downloading.
	t.Logf("Torrent SavePath:     %s", torrent.SavePath)
	t.Logf("Torrent DownloadPath: %s", torrent.DownloadPath)
	t.Logf("Torrent ContentPath:  %s", torrent.ContentPath)

	// Start orchestrator immediately (before download finishes) so it may observe
	// files in the temp directory and exercise the temp path resolution.
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

	// Wait for sync to complete on destination.
	t.Log("Waiting for torrent to sync to destination...")
	env.WaitForTorrentCompleteOnDestination(ctx, wiredCDHash, syncCompleteTimeout,
		"torrent with temp_path should sync to destination")

	// Wait for synced tag on destination (reliable, applied via background context).
	env.WaitForSyncedTagOnDestination(ctx, wiredCDHash, 30*time.Second,
		"destination torrent should have 'synced' tag")

	// Source synced tag is for visibility only. With temp_path enabled, qBittorrent's
	// addTags API may silently drop the request. Check non-fatally: known quirk.
	hasSyncedTag := false
	tagCtx, tagCancel := context.WithTimeout(ctx, 30*time.Second)
	defer tagCancel()
	for tagCtx.Err() == nil {
		torrents, tagErr := env.SourceClient().GetTorrentsCtx(tagCtx, qbittorrent.TorrentFilterOptions{
			Hashes: []string{wiredCDHash},
		})
		if tagErr == nil && len(torrents) > 0 && strings.Contains(torrents[0].Tags, "synced") {
			hasSyncedTag = true
			break
		}
		time.Sleep(time.Second)
	}
	if !hasSyncedTag {
		t.Log("Source torrent missing 'synced' tag (known temp_path quirk, non-fatal)")
	}

	cancelOrchestrator()
	<-orchestratorDone

	env.AssertTorrentCompleteOnDestination(ctx, wiredCDHash)

	t.Log("Temp path sync flow completed successfully!")

	env.CleanupBothSides(ctx, wiredCDHash)
}

// TestE2E_TorrentSeedsOnSourceAfterSync verifies that after a regular sync the
// torrent continues seeding on source (no stop). The stop only happens during
// disk pressure cleanup in maybeMoveToDestination.
func TestE2E_TorrentSeedsOnSourceAfterSync(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	t.Parallel()

	env := SetupTestEnv(t)
	ctx := context.Background()

	env.CleanupBothSides(ctx, wiredCDHash)

	t.Log("Adding Wired CD torrent to source...")
	env.DownloadTorrentOnSource(ctx, testTorrentURL, wiredCDHash, torrentDownloadTimeout)

	// No Force, no space pressure → torrent should stay on source after sync
	cfg := env.CreateSourceConfig()
	task, dest, err := env.CreateSourceTask(cfg)
	require.NoError(t, err)
	defer dest.Close()

	orchestratorCtx, cancelOrchestrator := context.WithTimeout(ctx, syncCompleteTimeout)
	defer cancelOrchestrator()

	orchestratorDone := make(chan error, 1)
	go func() {
		orchestratorDone <- task.Run(orchestratorCtx)
	}()

	t.Log("Waiting for torrent to be synced to destination...")
	env.WaitForTorrentCompleteOnDestination(ctx, wiredCDHash, syncCompleteTimeout, "torrent should be complete on destination")

	cancelOrchestrator()
	<-orchestratorDone

	// Torrent should still exist on source and NOT be stopped
	torrents, err := env.SourceClient().GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
		Hashes: []string{wiredCDHash},
	})
	require.NoError(t, err)
	require.Len(t, torrents, 1, "torrent should still exist on source after sync")

	assert.False(t, env.IsTorrentStopped(ctx, env.SourceClient(), wiredCDHash),
		"torrent on source should still be seeding after sync (state: %s)", torrents[0].State)

	env.AssertTorrentCompleteOnDestination(ctx, wiredCDHash)

	// Destination torrent should be STOPPED (explicitly stopped during finalization, not yet started).
	// Use Eventually because the stop may still be propagating through qBittorrent.
	require.Eventually(t, func() bool {
		return env.IsTorrentStopped(ctx, env.DestinationClient(), wiredCDHash)
	}, 15*time.Second, time.Second,
		"torrent on destination should be stopped after sync (no dual seeding)")

	t.Log("Torrent correctly continues seeding on source after sync, destination is stopped!")

	env.CleanupBothSides(ctx, wiredCDHash)
}

// TestE2E_StopBeforeDeleteOnDiskPressure verifies that during disk pressure
// cleanup, torrents are stopped on source before being deleted. This ensures
// a clean handoff: destination is already seeding, source stops, then source deletes.
func TestE2E_StopBeforeDeleteOnDiskPressure(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	t.Parallel()

	env := SetupTestEnv(t)
	ctx := context.Background()

	env.CleanupBothSides(ctx, wiredCDHash)

	t.Log("Adding Wired CD torrent to source...")
	env.DownloadTorrentOnSource(ctx, testTorrentURL, wiredCDHash, torrentDownloadTimeout)

	// First: sync to destination without Force (torrent stays on source)
	cfg := env.CreateSourceConfig()
	task, dest, err := env.CreateSourceTask(cfg)
	require.NoError(t, err)
	defer dest.Close()

	orchestratorCtx, cancelOrchestrator := context.WithTimeout(ctx, syncCompleteTimeout)
	defer cancelOrchestrator()

	orchestratorDone := make(chan error, 1)
	go func() {
		orchestratorDone <- task.Run(orchestratorCtx)
	}()

	t.Log("Waiting for torrent to be synced to destination...")
	env.WaitForTorrentCompleteOnDestination(ctx, wiredCDHash, syncCompleteTimeout, "torrent should be complete on destination")

	// Wait for background finalization to fully complete (including the explicit stop
	// after addAndVerifyTorrent). The synced tag is applied after the stop, so this
	// ensures no race between the finalization stop and the drain's StartTorrent.
	env.WaitForSyncedTagOnDestination(ctx, wiredCDHash, 30*time.Second,
		"destination torrent should have 'synced' tag after finalization")

	cancelOrchestrator()
	<-orchestratorDone

	env.AssertTorrentCompleteOnDestination(ctx, wiredCDHash)

	// Verify torrent is still seeding on source (not stopped yet)
	torrents, err := env.SourceClient().GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
		Hashes: []string{wiredCDHash},
	})
	require.NoError(t, err)
	require.Len(t, torrents, 1, "torrent should still exist on source")
	assert.False(t, env.IsTorrentStopped(ctx, env.SourceClient(), wiredCDHash),
		"torrent should be seeding on source before disk pressure cleanup")

	// Verify destination torrent is STOPPED before handoff (explicitly stopped during finalization).
	require.True(t, env.IsTorrentStopped(ctx, env.DestinationClient(), wiredCDHash),
		"destination torrent should be stopped before disk pressure handoff")

	// Now: drain to evacuate all synced torrents
	t.Log("Running drain...")
	drainCfg := env.CreateSourceConfig(WithMinSeedingTime(0))
	drainTask, drainDest, err := env.CreateSourceTask(drainCfg)
	require.NoError(t, err)
	defer drainDest.Close()

	err = drainTask.Login(ctx)
	require.NoError(t, err)

	// Mark as completed on destination so drain picks it up
	drainTask.MarkCompletedOnDestination(wiredCDHash)

	err = drainTask.Drain(ctx)
	require.NoError(t, err)

	// Torrent should now be deleted from source (stop -> start destination -> delete)
	env.WaitForTorrentDeleted(ctx, env.SourceClient(), wiredCDHash, 10*time.Second,
		"torrent should be deleted from source after drain")

	// Destination torrent should now be SEEDING (started during handoff).
	// qBittorrent may take a moment to transition from stoppedUP to an active state.
	require.Eventually(t, func() bool {
		return !env.IsTorrentStopped(ctx, env.DestinationClient(), wiredCDHash)
	}, 30*time.Second, time.Second,
		"torrent on destination should be seeding after drain handoff")

	t.Log("Torrent handed off: source deleted, destination now seeding!")

	env.CleanupBothSides(ctx, wiredCDHash)
}

// TestE2E_SourceRemovedTagOnDiskPressure verifies that the source-removed tag is applied
// on the destination torrent when a torrent is removed from source during disk pressure cleanup.
// Flow: sync to destination -> force cleanup -> verify destination has source-removed AND synced tags.
func TestE2E_SourceRemovedTagOnDiskPressure(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	t.Parallel()

	env := SetupTestEnv(t)
	ctx := context.Background()

	env.CleanupBothSides(ctx, wiredCDHash)

	t.Log("Adding Wired CD torrent to source...")
	env.DownloadTorrentOnSource(ctx, testTorrentURL, wiredCDHash, torrentDownloadTimeout)

	// First: sync to destination without Force (torrent stays on source)
	cfg := env.CreateSourceConfig()
	task, dest, err := env.CreateSourceTask(cfg)
	require.NoError(t, err)
	defer dest.Close()

	orchestratorCtx, cancelOrchestrator := context.WithTimeout(ctx, syncCompleteTimeout)
	defer cancelOrchestrator()

	orchestratorDone := make(chan error, 1)
	go func() {
		orchestratorDone <- task.Run(orchestratorCtx)
	}()

	t.Log("Waiting for torrent to be synced to destination...")
	env.WaitForTorrentCompleteOnDestination(ctx, wiredCDHash, syncCompleteTimeout, "torrent should be complete on destination")

	// Wait for background finalization to apply the synced tag on destination before canceling the orchestrator.
	// The synced tag is applied after addAndVerifyTorrent completes.
	env.WaitForSyncedTagOnDestination(ctx, wiredCDHash, 30*time.Second,
		"destination torrent should have 'synced' tag after finalization")

	cancelOrchestrator()
	<-orchestratorDone

	env.AssertTorrentCompleteOnDestination(ctx, wiredCDHash)

	// Now: drain to evacuate all synced torrents
	t.Log("Running drain...")
	drainCfg := env.CreateSourceConfig(WithMinSeedingTime(0))
	drainTask, drainDest, err := env.CreateSourceTask(drainCfg)
	require.NoError(t, err)
	defer drainDest.Close()

	err = drainTask.Login(ctx)
	require.NoError(t, err)

	// Mark as completed on destination so drain picks it up
	drainTask.MarkCompletedOnDestination(wiredCDHash)

	err = drainTask.Drain(ctx)
	require.NoError(t, err)

	// Torrent should now be deleted from source
	env.WaitForTorrentDeleted(ctx, env.SourceClient(), wiredCDHash, 10*time.Second,
		"torrent should be deleted from source after drain")

	// Destination torrent should now be seeding
	require.Eventually(t, func() bool {
		return !env.IsTorrentStopped(ctx, env.DestinationClient(), wiredCDHash)
	}, 15*time.Second, time.Second,
		"torrent on destination should be seeding after drain handoff")

	// Verify destination torrent has the source-removed tag
	destTorrents, err := env.DestinationClient().GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
		Hashes: []string{wiredCDHash},
	})
	require.NoError(t, err)
	require.Len(t, destTorrents, 1)
	assert.Contains(t, destTorrents[0].Tags, "source-removed",
		"destination torrent should have 'source-removed' tag after drain handoff")
	assert.Contains(t, destTorrents[0].Tags, "synced",
		"destination torrent should retain 'synced' tag after handoff")

	t.Log("Source-removed tag correctly applied on destination after drain handoff!")

	env.CleanupBothSides(ctx, wiredCDHash)
}

// TestE2E_DestinationServerRestart tests that streaming can recover after the destination gRPC server restarts.
// Scenario: Start streaming, stop destination server mid-stream, restart destination server, verify sync completes.
func TestE2E_DestinationServerRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	t.Parallel()

	env := SetupTestEnv(t)
	ctx := context.Background()

	env.CleanupBothSides(ctx, wiredCDHash)

	t.Log("Adding Wired CD torrent to source...")
	env.DownloadTorrentOnSource(ctx, testTorrentURL, wiredCDHash, torrentDownloadTimeout)

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

	t.Log("Waiting for streaming to start and make progress...")
	require.Eventually(t, func() bool {
		progress, progressErr := task.Progress(ctx, wiredCDHash)
		if progressErr != nil {
			return false
		}
		t.Logf("Streaming progress: %d/%d pieces", progress.Streamed, progress.TotalPieces)
		return progress.Streamed > 0 && progress.Streamed >= progress.TotalPieces/10
	}, 2*time.Minute, time.Second, "streaming should start and make progress")

	t.Log("Stopping destination gRPC server mid-stream...")
	env.StopDestinationServer()
	time.Sleep(5 * time.Second)

	t.Log("Restarting destination gRPC server...")
	env.StartDestinationServer()

	t.Log("Waiting for sync to complete after destination server restart...")
	env.WaitForTorrentCompleteOnDestination(
		ctx,
		wiredCDHash,
		syncCompleteTimeout,
		"torrent should be complete on destination after restart",
	)

	cancelOrchestrator()
	<-orchestratorDone

	env.AssertTorrentCompleteOnDestination(ctx, wiredCDHash)

	env.CleanupBothSides(ctx, wiredCDHash)
}

// TestE2E_DestinationQBittorrentRestart tests that finalization can recover after destination qBittorrent restarts.
// Scenario: Stream completes, stop destination qBittorrent before finalization, restart, verify sync completes.
func TestE2E_DestinationQBittorrentRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	t.Parallel()

	env := SetupTestEnv(t)
	ctx := context.Background()

	env.CleanupBothSides(ctx, wiredCDHash)

	t.Log("Adding Wired CD torrent to source...")
	env.DownloadTorrentOnSource(ctx, testTorrentURL, wiredCDHash, torrentDownloadTimeout)

	t.Log("Stopping destination qBittorrent before sync...")
	err := env.StopDestinationQBittorrent(ctx)
	require.NoError(t, err)

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

	t.Log("Waiting for streaming to complete (destination qB is down)...")
	time.Sleep(15 * time.Second)

	t.Log("Restarting destination qBittorrent...")
	err = env.StartDestinationQBittorrent(ctx)
	require.NoError(t, err)

	t.Log("Waiting for sync to complete after destination qBittorrent restart...")
	env.WaitForTorrentCompleteOnDestination(
		ctx,
		wiredCDHash,
		syncCompleteTimeout,
		"torrent should be complete on destination after qBittorrent restart",
	)

	cancelOrchestrator()
	<-orchestratorDone

	env.AssertTorrentCompleteOnDestination(ctx, wiredCDHash)

	env.CleanupBothSides(ctx, wiredCDHash)
}

// TestE2E_SourceQBittorrentRestart tests that syncing recovers after source qBittorrent restarts.
// Scenario: Start streaming, stop source qBittorrent mid-stream, restart it, create a new
// orchestrator (port may change after restart), verify torrent survives and sync completes.
func TestE2E_SourceQBittorrentRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	t.Parallel()

	env := SetupTestEnv(t)
	ctx := context.Background()

	env.CleanupBothSides(ctx, wiredCDHash)

	t.Log("Adding Wired CD torrent to source...")
	env.DownloadTorrentOnSource(ctx, testTorrentURL, wiredCDHash, torrentDownloadTimeout)

	cfg := env.CreateSourceConfig()
	task, dest, err := env.CreateSourceTask(cfg)
	require.NoError(t, err)

	orchestratorCtx, cancelOrchestrator := context.WithTimeout(ctx, orchestratorTimeout)

	orchestratorDone := make(chan error, 1)
	go func() {
		orchestratorDone <- task.Run(orchestratorCtx)
	}()

	t.Log("Waiting for streaming to start...")
	time.Sleep(2 * time.Second)

	t.Log("Stopping source qBittorrent mid-stream...")
	err = env.StopSourceQBittorrent(ctx)
	require.NoError(t, err)

	// Stop the first orchestrator — it can't recover because
	// the container port mapping may change after restart.
	cancelOrchestrator()
	<-orchestratorDone
	dest.Close()

	time.Sleep(5 * time.Second)

	t.Log("Restarting source qBittorrent...")
	err = env.StartSourceQBittorrent(ctx)
	require.NoError(t, err)

	torrent := env.WaitForTorrent(env.SourceClient(), wiredCDHash, 30*time.Second)
	require.NotNil(t, torrent, "torrent should still exist after source qBittorrent restart")

	// Create a new orchestrator with the (potentially new) source URL.
	t.Log("Starting new orchestrator after source qBittorrent restart...")
	cfg2 := env.CreateSourceConfig()
	task2, dest2, err := env.CreateSourceTask(cfg2)
	require.NoError(t, err)
	defer dest2.Close()

	orchestratorCtx2, cancelOrchestrator2 := context.WithTimeout(ctx, orchestratorTimeout)
	defer cancelOrchestrator2()

	orchestratorDone2 := make(chan error, 1)
	go func() {
		orchestratorDone2 <- task2.Run(orchestratorCtx2)
	}()

	t.Log("Waiting for sync to complete after source qBittorrent restart...")
	env.WaitForTorrentCompleteOnDestination(
		ctx,
		wiredCDHash,
		syncCompleteTimeout,
		"torrent should be complete on destination after source qBittorrent restart",
	)

	cancelOrchestrator2()
	<-orchestratorDone2

	env.AssertTorrentCompleteOnDestination(ctx, wiredCDHash)

	env.CleanupBothSides(ctx, wiredCDHash)
}

// TestE2E_OrchestratorRestart tests that a new orchestrator can resume syncing.
// Scenario: Start orchestrator, stop it mid-sync, start new orchestrator, verify sync completes.
func TestE2E_OrchestratorRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	t.Parallel()

	env := SetupTestEnv(t)
	ctx := context.Background()

	env.CleanupBothSides(ctx, wiredCDHash)

	t.Log("Adding Wired CD torrent to source...")
	env.DownloadTorrentOnSource(ctx, testTorrentURL, wiredCDHash, torrentDownloadTimeout)

	cfg := env.CreateSourceConfig()
	task1, dest1, err := env.CreateSourceTask(cfg)
	require.NoError(t, err)

	orchestratorCtx1, cancelOrchestrator1 := context.WithTimeout(ctx, 30*time.Second)

	orchestratorDone1 := make(chan error, 1)
	go func() {
		orchestratorDone1 <- task1.Run(orchestratorCtx1)
	}()

	t.Log("Running first orchestrator for a few seconds...")
	time.Sleep(5 * time.Second)

	t.Log("Stopping first orchestrator...")
	cancelOrchestrator1()
	<-orchestratorDone1
	dest1.Close()

	t.Log("Starting second orchestrator...")
	task2, dest2, err := env.CreateSourceTask(cfg)
	require.NoError(t, err)
	defer dest2.Close()

	orchestratorCtx2, cancelOrchestrator2 := context.WithTimeout(ctx, syncCompleteTimeout)
	defer cancelOrchestrator2()

	orchestratorDone2 := make(chan error, 1)
	go func() {
		orchestratorDone2 <- task2.Run(orchestratorCtx2)
	}()

	t.Log("Waiting for sync to complete with second orchestrator...")
	env.WaitForTorrentCompleteOnDestination(
		ctx,
		wiredCDHash,
		syncCompleteTimeout,
		"torrent should be complete on destination after orchestrator restart",
	)

	cancelOrchestrator2()
	<-orchestratorDone2

	env.AssertTorrentCompleteOnDestination(ctx, wiredCDHash)

	env.CleanupBothSides(ctx, wiredCDHash)
}

// TestE2E_HardlinkDetection verifies that hardlink detection works in the Docker test environment.
// This is a prerequisite for the hardlink grouping logic used during space management.
func TestE2E_HardlinkDetection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	t.Parallel()

	env := SetupTestEnv(t)

	// Create original file
	originalPath := env.CreateTestFile("torrent-a/video.mkv", 1024*1024) // 1MB

	// Create hardlink in different directory
	hardlinkDir := filepath.Join(env.SourcePath(), "torrent-b")
	require.NoError(t, os.MkdirAll(hardlinkDir, 0755))
	hardlinkPath := filepath.Join(hardlinkDir, "video.mkv")
	require.NoError(t, os.Link(originalPath, hardlinkPath))

	// Verify hardlink detection works
	linked, err := utils.AreHardlinked(originalPath, hardlinkPath)
	require.NoError(t, err)
	assert.True(t, linked, "files should be detected as hardlinked")

	// Verify inodes match
	inode1, err := utils.GetInode(originalPath)
	require.NoError(t, err)
	inode2, err := utils.GetInode(hardlinkPath)
	require.NoError(t, err)
	assert.Equal(t, inode1, inode2, "inodes should match for hardlinked files")

	// Create a non-hardlinked file
	separatePath := env.CreateTestFile("torrent-c/video.mkv", 1024*1024)

	// Verify it's NOT detected as hardlinked
	linked, err = utils.AreHardlinked(originalPath, separatePath)
	require.NoError(t, err)
	assert.False(t, linked, "separate files should not be detected as hardlinked")

	t.Log("Hardlink detection works correctly in test environment")
}

// TestE2E_HardlinkGroupDeletion tests that hardlinked torrents are deleted together.
// Scenario: Two torrents share a hardlinked file, both complete on destination,
// when space cleanup runs, both should be deleted as a group.
func TestE2E_HardlinkGroupDeletion(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	t.Parallel()

	env := SetupTestEnv(t)
	ctx := context.Background()

	// We'll use Big Buck Bunny and Wired CD - two different torrents
	// After they download, we'll hardlink a file between their directories
	// to simulate cross-seeded content

	env.CleanupBothSides(ctx, bigBuckBunnyHash, wiredCDHash)

	t.Log("Adding Big Buck Bunny and Wired CD torrents to source...")
	err := env.AddTorrentToSource(ctx, bigBuckBunnyURL, nil)
	require.NoError(t, err)
	err = env.AddTorrentToSource(ctx, testTorrentURL, nil) // Wired CD
	require.NoError(t, err)

	// Wait for both to appear
	bbbTorrent := env.WaitForTorrent(env.SourceClient(), bigBuckBunnyHash, 30*time.Second)
	require.NotNil(t, bbbTorrent)
	wiredCDTorrent := env.WaitForTorrent(env.SourceClient(), wiredCDHash, 30*time.Second)
	require.NotNil(t, wiredCDTorrent)

	// Wait for both to complete
	t.Log("Waiting for torrents to complete downloading...")
	env.WaitForTorrentComplete(env.SourceClient(), bigBuckBunnyHash, torrentDownloadTimeout)
	env.WaitForTorrentComplete(env.SourceClient(), wiredCDHash, torrentDownloadTimeout)
	t.Log("Both torrents downloaded")

	// Sync both to destination via orchestrator (so they actually exist on destination qBittorrent)
	cfg := env.CreateSourceConfig()
	syncTask, syncDest, err := env.CreateSourceTask(cfg)
	require.NoError(t, err)

	orchestratorCtx, cancelOrchestrator := context.WithTimeout(ctx, orchestratorTimeout)
	defer cancelOrchestrator()

	orchestratorDone := make(chan error, 1)
	go func() {
		orchestratorDone <- syncTask.Run(orchestratorCtx)
	}()

	t.Log("Waiting for both torrents to sync to destination...")
	env.WaitForTorrentCompleteOnDestination(ctx, bigBuckBunnyHash, syncCompleteTimeout, "BBB should be complete on destination")
	env.WaitForTorrentCompleteOnDestination(ctx, wiredCDHash, syncCompleteTimeout, "Wired CD should be complete on destination")

	cancelOrchestrator()
	<-orchestratorDone
	syncDest.Close()
	t.Log("Both torrents synced to destination")

	// Get file info to find actual file paths
	bbbFiles, err := env.SourceClient().GetFilesInformationCtx(ctx, bigBuckBunnyHash)
	require.NoError(t, err)
	require.NotNil(t, bbbFiles)
	require.NotEmpty(t, *bbbFiles)

	wiredCDFiles, err := env.SourceClient().GetFilesInformationCtx(ctx, wiredCDHash)
	require.NoError(t, err)
	require.NotNil(t, wiredCDFiles)
	require.NotEmpty(t, *wiredCDFiles)

	// Create hardlink between a BBB file and a Wired CD file
	// We need to hardlink FROM a BBB file TO a path that Wired CD expects
	// Since they have different file structures, we'll add an extra hardlinked file
	// to BBB's directory that matches one of Wired CD's files

	// Get first file from each torrent - use env.SourcePath() for host filesystem access
	// (qBittorrent's SavePath is the container path, not accessible from host)
	wiredCDFirstFile := (*wiredCDFiles)[0]
	wiredCDFilePath := filepath.Join(env.SourcePath(), wiredCDFirstFile.Name)

	bbbFirstFile := (*bbbFiles)[0]
	bbbFilePath := filepath.Join(env.SourcePath(), bbbFirstFile.Name)

	t.Logf("BBB file: %s", bbbFilePath)
	t.Logf("Wired CD file: %s", wiredCDFilePath)

	// Verify the original files exist and have different inodes
	bbbInode, err := utils.GetInode(bbbFilePath)
	require.NoError(t, err, "BBB file should exist")
	wiredCDInode, err := utils.GetInode(wiredCDFilePath)
	require.NoError(t, err, "Wired CD file should exist")
	t.Logf("Original inodes - BBB: %d, Wired CD: %d", bbbInode, wiredCDInode)
	require.NotEqual(t, bbbInode, wiredCDInode, "files should have different inodes initially")

	// Now, to simulate hardlinking, we need to make one file a hardlink of the other.
	// In a real cross-seed scenario, you'd have the SAME content file shared between torrents.
	// For testing, we'll remove the Wired CD file and replace it with a hardlink to BBB's file.
	// (This would break the actual torrent verification, but we're just testing the grouping logic)
	require.NoError(t, os.Remove(wiredCDFilePath))
	require.NoError(t, os.Link(bbbFilePath, wiredCDFilePath))

	// Verify they're now hardlinked
	linked, err := utils.AreHardlinked(bbbFilePath, wiredCDFilePath)
	require.NoError(t, err)
	require.True(t, linked, "files should now be hardlinked")
	t.Log("Successfully created hardlink between torrent files")

	// Create source task and drain (bypasses space check and seeding time)
	drainCfg := env.CreateSourceConfig(WithMinSeedingTime(0))
	task, dest, err := env.CreateSourceTask(drainCfg)
	require.NoError(t, err)
	defer dest.Close()

	// Login
	err = task.Login(ctx)
	require.NoError(t, err)

	// Mark both as complete on destination (new task instance needs to know about destination state)
	task.MarkCompletedOnDestination(bigBuckBunnyHash)
	task.MarkCompletedOnDestination(wiredCDHash)
	t.Log("Both torrents marked as complete on destination")

	// Drain - this should detect the hardlink group and delete both
	t.Log("Running Drain...")
	err = task.Drain(ctx)
	require.NoError(t, err)

	// Verify BOTH torrents were deleted (because they're in the same hardlink group)
	env.WaitForTorrentDeleted(ctx, env.SourceClient(), bigBuckBunnyHash, 10*time.Second,
		"Big Buck Bunny should be deleted")
	env.WaitForTorrentDeleted(ctx, env.SourceClient(), wiredCDHash, 10*time.Second,
		"Wired CD should be deleted (hardlink group)")

	t.Log("Both hardlinked torrents were deleted together as expected!")

	env.CleanupBothSides(ctx, bigBuckBunnyHash, wiredCDHash)
}

// TestE2E_DestinationHardlinkDeduplication tests that the destination server creates hardlinks
// for files that share the same inode as previously synced files.
// Scenario:
//  1. Sync Torrent A to destination (registers file inodes in destination's inode tracking)
//  2. Create a fake Torrent B on source with files hardlinked to Torrent A's files
//  3. Initialize Torrent B on destination - destination should detect the inodes and create hardlinks
//  4. Verify InitTorrentResponse shows hardlinked=true and piecesCovered=true
//  5. Verify files on destination are actual hardlinks (same inode)
func TestE2E_DestinationHardlinkDeduplication(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	t.Parallel()

	env := SetupTestEnv(t)
	ctx := context.Background()

	env.CleanupBothSides(ctx, wiredCDHash)

	// Step 1: Sync Wired CD to destination (this registers the file inodes)
	t.Log("Adding Wired CD torrent to source...")
	torrent := env.DownloadTorrentOnSource(ctx, testTorrentURL, wiredCDHash, torrentDownloadTimeout)

	// Get file info and inodes from SOURCE filesystem BEFORE sync
	// These are the SOURCE inodes that will be registered on destination during sync
	sourceFiles, err := env.SourceClient().GetFilesInformationCtx(ctx, wiredCDHash)
	require.NoError(t, err)
	require.NotNil(t, sourceFiles)
	require.NotEmpty(t, *sourceFiles)

	t.Log("Getting inodes of files on SOURCE filesystem...")
	type fileWithInode struct {
		path  string
		size  int64
		inode uint64
	}
	var sourceFileInodes []fileWithInode

	for _, f := range *sourceFiles {
		filePath := filepath.Join(env.SourcePath(), f.Name)
		inode, inodeErr := utils.GetInode(filePath)
		require.NoError(t, inodeErr, "should be able to get inode for %s", filePath)
		sourceFileInodes = append(sourceFileInodes, fileWithInode{
			path:  f.Name,
			size:  f.Size,
			inode: inode,
		})
		t.Logf("  SOURCE File: %s, Size: %d, Inode: %d", f.Name, f.Size, inode)
	}

	// Create source task and run orchestrator to sync to destination
	cfg := env.CreateSourceConfig()
	task, dest, err := env.CreateSourceTask(cfg)
	require.NoError(t, err)
	defer dest.Close()

	orchestratorCtx, cancelOrchestrator := context.WithTimeout(ctx, syncCompleteTimeout)
	defer cancelOrchestrator()

	orchestratorDone := make(chan error, 1)
	go func() {
		orchestratorDone <- task.Run(orchestratorCtx)
	}()

	// Wait for sync to complete (torrent complete on destination qBittorrent)
	t.Log("Waiting for Wired CD to sync to destination...")
	env.WaitForTorrentCompleteOnDestination(ctx, wiredCDHash, syncCompleteTimeout, "torrent should be complete on destination")

	// Stop orchestrator
	cancelOrchestrator()
	<-orchestratorDone

	t.Log("Wired CD synced to destination - SOURCE inodes should now be registered")

	// Step 2: Create a second gRPC destination and initialize a "fake" torrent B
	// with the same SOURCE file inodes as torrent A
	dest2, err := env.CreateGRPCDestination()
	require.NoError(t, err)
	defer dest2.Close()

	// Build file info with the SAME SOURCE inodes as the synced files
	var pbFiles []*pb.FileInfo
	var offset int64
	for _, f := range sourceFileInodes {
		baseName := filepath.Base(f.path)
		pbFiles = append(pbFiles, &pb.FileInfo{
			Path:     "FakeTorrentB/" + baseName, // Different path, same HOT inode
			Size:     f.size,
			Offset:   offset,
			Inode:    f.inode, // Same SOURCE inode - this triggers hardlink detection!
			Selected: true,
		})
		offset += f.size
	}

	// Get properties for piece info
	props, err := env.SourceClient().GetTorrentPropertiesCtx(ctx, wiredCDHash)
	require.NoError(t, err)

	fakeTorrentHash := "fffffffffffffffffffffffffffffffffffffffb" // Fake hash for torrent B

	t.Log("Initializing fake Torrent B with same inodes on destination...")
	initResult, err := dest2.InitTorrent(ctx, &pb.InitTorrentRequest{
		TorrentHash: fakeTorrentHash,
		Name:        "FakeTorrentB",
		NumPieces:   int32(props.PiecesNum),
		PieceSize:   int64(props.PieceSize),
		TotalSize:   torrent.Size,
		Files:       pbFiles,
		// No torrent file needed for this test
	})
	require.NoError(t, err)

	// Step 4: Verify hardlink results
	t.Log("Verifying hardlink results...")
	require.NotNil(t, initResult.HardlinkResults, "should have hardlink results")

	hardlinkedCount := 0
	for i, result := range initResult.HardlinkResults {
		t.Logf("  File %d: hardlinked=%v, source=%s, error=%s",
			i, result.GetHardlinked(), result.GetSourcePath(), result.GetError())
		if result.GetHardlinked() {
			hardlinkedCount++
		}
	}

	// All files should be hardlinked since they have matching SOURCE inodes
	assert.Equal(t, len(pbFiles), hardlinkedCount,
		"all %d files should be hardlinked (got %d)", len(pbFiles), hardlinkedCount)

	// Verify pieces are covered (not needed = covered by hardlinks)
	coveredCount := 0
	for _, needed := range initResult.PiecesNeeded {
		if !needed {
			coveredCount++
		}
	}
	t.Logf("Pieces covered by hardlinks: %d/%d", coveredCount, len(initResult.PiecesNeeded))
	assert.Equal(t, len(initResult.PiecesNeeded), coveredCount,
		"all pieces should be covered by hardlinked files")

	// Step 5: Verify the files on destination filesystem are actual hardlinks
	t.Log("Verifying files on destination filesystem are hardlinks...")
	for i, f := range sourceFileInodes {
		originalPath := filepath.Join(env.DestinationPath(), f.path)
		baseName := filepath.Base(f.path)
		hardlinkPath := filepath.Join(env.DestinationPath(), "FakeTorrentB", baseName)

		// Verify hardlink exists
		_, statErr := os.Stat(hardlinkPath)
		require.NoError(t, statErr, "hardlinked file should exist: %s", hardlinkPath)

		// Verify they share the same destination inode (are actually hardlinked on destination)
		originalInode, err := utils.GetInode(originalPath)
		require.NoError(t, err)
		hardlinkInode, err := utils.GetInode(hardlinkPath)
		require.NoError(t, err)

		assert.Equal(t, originalInode, hardlinkInode,
			"file %d should have same destination inode (original=%d, hardlink=%d)",
			i, originalInode, hardlinkInode)

		// Verify using AreHardlinked utility
		linked, err := utils.AreHardlinked(originalPath, hardlinkPath)
		require.NoError(t, err)
		assert.True(t, linked, "files should be detected as hardlinked")
	}

	t.Log("Destination hardlink deduplication test completed successfully!")
	t.Logf("  - Files hardlinked: %d/%d", hardlinkedCount, len(pbFiles))
	t.Logf("  - Pieces covered: %d/%d", coveredCount, len(initResult.PiecesNeeded))
	t.Log("  - All hardlinks verified on filesystem")

	env.CleanupBothSides(ctx, wiredCDHash)
}

// TestE2E_NonHardlinkedDeletedIndependently verifies that non-hardlinked torrents
// are deleted independently based on their individual priority.
func TestE2E_NonHardlinkedDeletedIndependently(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	t.Parallel()

	env := SetupTestEnv(t)
	ctx := context.Background()

	// Cleanup
	env.CleanupTorrent(ctx, env.SourceClient(), bigBuckBunnyHash)
	env.CleanupTorrent(ctx, env.SourceClient(), wiredCDHash)

	// Add both torrents
	t.Log("Adding Big Buck Bunny and Wired CD torrents...")
	err := env.AddTorrentToSource(ctx, bigBuckBunnyURL, nil)
	require.NoError(t, err)
	err = env.AddTorrentToSource(ctx, testTorrentURL, nil)
	require.NoError(t, err)

	// Wait for both to appear
	env.WaitForTorrent(env.SourceClient(), bigBuckBunnyHash, 30*time.Second)
	env.WaitForTorrent(env.SourceClient(), wiredCDHash, 30*time.Second)

	// Wait for both to download
	t.Log("Waiting for torrents to download...")
	env.WaitForTorrentComplete(env.SourceClient(), bigBuckBunnyHash, torrentDownloadTimeout)
	env.WaitForTorrentComplete(env.SourceClient(), wiredCDHash, torrentDownloadTimeout)
	t.Log("Both torrents downloaded")

	// Sync both to destination via orchestrator (so they actually exist on destination qBittorrent)
	cfg := env.CreateSourceConfig()
	syncTask, syncDest, err := env.CreateSourceTask(cfg)
	require.NoError(t, err)

	orchestratorCtx, cancelOrchestrator := context.WithTimeout(ctx, orchestratorTimeout)
	defer cancelOrchestrator()

	orchestratorDone := make(chan error, 1)
	go func() {
		orchestratorDone <- syncTask.Run(orchestratorCtx)
	}()

	t.Log("Waiting for both torrents to sync to destination...")
	env.WaitForTorrentCompleteOnDestination(ctx, bigBuckBunnyHash, syncCompleteTimeout, "BBB should be complete on destination")
	env.WaitForTorrentCompleteOnDestination(ctx, wiredCDHash, syncCompleteTimeout, "Wired CD should be complete on destination")

	cancelOrchestrator()
	<-orchestratorDone
	syncDest.Close()
	t.Log("Both torrents synced to destination")

	// Verify they're NOT hardlinked (they shouldn't be - different files)
	torrents, err := env.SourceClient().GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
		Hashes: []string{bigBuckBunnyHash, wiredCDHash},
	})
	require.NoError(t, err)
	require.Len(t, torrents, 2)

	// Create task and drain
	drainCfg := env.CreateSourceConfig(WithMinSeedingTime(0))
	task, dest, err := env.CreateSourceTask(drainCfg)
	require.NoError(t, err)
	defer dest.Close()

	// Mark both as complete on destination (new task instance needs to know about destination state)
	task.MarkCompletedOnDestination(bigBuckBunnyHash)
	task.MarkCompletedOnDestination(wiredCDHash)

	err = task.Login(ctx)
	require.NoError(t, err)

	// Drain
	t.Log("Running Drain...")
	err = task.Drain(ctx)
	require.NoError(t, err)

	// Both should be deleted (independently, not as a group)
	env.WaitForTorrentDeleted(ctx, env.SourceClient(), bigBuckBunnyHash, 10*time.Second,
		"Big Buck Bunny should be deleted")
	env.WaitForTorrentDeleted(ctx, env.SourceClient(), wiredCDHash, 10*time.Second,
		"Wired CD should be deleted")

	t.Log("Non-hardlinked torrents were deleted independently as expected!")

	env.CleanupBothSides(ctx, bigBuckBunnyHash, wiredCDHash)
}

// TestE2E_FullSyncFlowWiredCD tests the full sync flow with Wired CD,
// which has 18 files (audio tracks). This validates multi-file torrent
// handling more thoroughly.
func TestE2E_FullSyncFlowWiredCD(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	t.Parallel()

	env := SetupTestEnv(t)
	ctx := context.Background()

	env.CleanupBothSides(ctx, wiredCDHash)

	t.Log("Adding Wired CD torrent to source (18 files)...")
	err := env.AddTorrentToSource(ctx, testTorrentURL, nil)
	require.NoError(t, err)

	torrent := env.WaitForTorrent(env.SourceClient(), wiredCDHash, 30*time.Second)
	require.NotNil(t, torrent)
	t.Logf("Torrent added: %s", torrent.Name)

	// Get file count to verify it's multi-file
	files, err := env.SourceClient().GetFilesInformationCtx(ctx, wiredCDHash)
	require.NoError(t, err)
	require.NotNil(t, files)
	t.Logf("Torrent has %d files", len(*files))
	assert.GreaterOrEqual(t, len(*files), 10, "Wired CD should have at least 10 files")

	// Wait for torrent to complete downloading
	t.Log("Waiting for torrent to complete downloading...")
	env.WaitForTorrentComplete(env.SourceClient(), wiredCDHash, torrentDownloadTimeout)
	t.Log("Torrent download complete")

	// Create source task and run the orchestrator
	cfg := env.CreateSourceConfig()
	task, dest, err := env.CreateSourceTask(cfg)
	require.NoError(t, err)
	defer dest.Close()

	// Run the orchestrator in background with timeout
	orchestratorCtx, cancelOrchestrator := context.WithTimeout(ctx, orchestratorTimeout)
	defer cancelOrchestrator()

	orchestratorDone := make(chan error, 1)
	go func() {
		orchestratorDone <- task.Run(orchestratorCtx)
	}()

	// Wait for torrent to be synced (complete on destination qBittorrent)
	t.Log("Waiting for torrent to be synced to destination...")
	env.WaitForTorrentCompleteOnDestination(ctx, wiredCDHash, syncCompleteTimeout, "torrent should be complete on destination")

	t.Log("Torrent is complete on destination qBittorrent")

	// Stop the orchestrator
	cancelOrchestrator()
	<-orchestratorDone

	env.AssertTorrentCompleteOnDestination(ctx, wiredCDHash)

	// Verify all files exist on destination
	destFiles, err := env.DestinationClient().GetFilesInformationCtx(ctx, wiredCDHash)
	require.NoError(t, err)
	require.NotNil(t, destFiles)
	assert.Len(t, *destFiles, len(*files), "destination should have same number of files as source")

	t.Logf("Full sync flow with Wired CD (%d files) completed successfully!", len(*destFiles))

	env.CleanupBothSides(ctx, wiredCDHash)
}

// TestE2E_OrphanCleanupOnTorrentRemoval tests that partial files are cleaned up
// when a torrent is removed from source qBittorrent mid-stream.
// This verifies the active cleanup path: source detects removal -> notifies destination -> destination aborts.
func TestE2E_OrphanCleanupOnTorrentRemoval(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	t.Parallel()

	env := SetupTestEnv(t)
	ctx := context.Background()

	env.CleanupBothSides(ctx, wiredCDHash)

	t.Log("Adding Wired CD torrent to source...")
	env.DownloadTorrentOnSource(ctx, testTorrentURL, wiredCDHash, torrentDownloadTimeout)

	// Create source task
	cfg := env.CreateSourceConfig()
	task, dest, err := env.CreateSourceTask(cfg)
	require.NoError(t, err)
	defer dest.Close()

	// Run orchestrator in background
	orchestratorCtx, cancelOrchestrator := context.WithTimeout(ctx, syncCompleteTimeout)
	defer cancelOrchestrator()

	orchestratorDone := make(chan error, 1)
	go func() {
		orchestratorDone <- task.Run(orchestratorCtx)
	}()

	// Wait for streaming to start and make some progress (but not complete)
	t.Log("Waiting for streaming to start...")
	var initialProgress int
	require.Eventually(t, func() bool {
		progress, progressErr := task.Progress(ctx, wiredCDHash)
		if progressErr != nil {
			return false
		}
		initialProgress = progress.Streamed
		t.Logf("Streaming progress: %d/%d pieces", progress.Streamed, progress.TotalPieces)
		// Wait for at least some pieces to be streamed
		return progress.Streamed > 0 && progress.Streamed < progress.TotalPieces
	}, 2*time.Minute, time.Second, "streaming should start")

	t.Logf("Streaming started with %d pieces transferred, now removing torrent from source...", initialProgress)

	// Check that partial files exist on destination before removal
	destMetaDir := filepath.Join(env.DestinationPath(), ".qbsync", wiredCDHash)
	_, metaErr := os.Stat(destMetaDir)
	require.NoError(t, metaErr, "destination meta directory should exist during streaming")
	t.Logf("Destination meta directory exists: %s", destMetaDir)

	// Delete the torrent from source qBittorrent mid-stream
	// This should trigger: PieceMonitor detects removal -> orchestrator handles -> calls AbortTorrent
	t.Log("Deleting torrent from source qBittorrent...")
	err = env.SourceClient().DeleteTorrentsCtx(ctx, []string{wiredCDHash}, true)
	require.NoError(t, err)
	t.Log("Torrent deleted from source")

	// Wait for cleanup to propagate to destination
	// The orchestrator should detect the removal and call AbortTorrent on destination
	t.Log("Waiting for destination server to clean up partial files...")
	require.Eventually(t, func() bool {
		// Check if meta directory has been removed
		_, statErr := os.Stat(destMetaDir)
		if os.IsNotExist(statErr) {
			return true
		}
		t.Log("Meta directory still exists, waiting...")
		return false
	}, 30*time.Second, time.Second, "destination meta directory should be cleaned up after torrent removal")

	t.Log("Destination meta directory cleaned up successfully")

	// Verify the orchestrator is still running (didn't crash)
	select {
	case runErr := <-orchestratorDone:
		// Orchestrator shouldn't have exited yet (unless context-related)
		if runErr != nil && !errors.Is(runErr, context.Canceled) && !errors.Is(runErr, context.DeadlineExceeded) {
			t.Fatalf("Orchestrator exited unexpectedly with error: %v", runErr)
		}
	default:
		t.Log("Orchestrator is still running (as expected)")
	}

	// Verify torrent is NOT on destination qBittorrent (was never finalized)
	destTorrents, err := env.DestinationClient().GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
		Hashes: []string{wiredCDHash},
	})
	require.NoError(t, err)
	assert.Empty(t, destTorrents, "torrent should NOT exist on destination qBittorrent (was aborted)")

	// Stop the orchestrator gracefully
	cancelOrchestrator()
	<-orchestratorDone

	t.Log("Orphan cleanup on torrent removal test completed successfully!")

	env.CleanupBothSides(ctx, wiredCDHash)
}

// TestE2E_CategoryWithATM tests that torrents with a category and Automatic Torrent
// Management (ATM) enabled sync correctly. When ATM is enabled, qBittorrent moves
// files into a category-specific subdirectory (e.g., /downloads/music/), which means
// torrent.SavePath differs from the base DataPath. The orchestrator must handle this
// path difference when reading files for streaming and when constructing paths for
// hardlink detection.
func TestE2E_CategoryWithATM(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	t.Parallel()

	env := SetupTestEnv(t)
	ctx := context.Background()

	env.CleanupBothSides(ctx, wiredCDHash)

	// Step 1: Create a category "music" with save path /downloads/music on source qBittorrent.
	// This causes ATM-managed torrents in this category to save under /downloads/music/.
	t.Log("Creating 'music' category on source qBittorrent...")
	err := env.SourceClient().CreateCategoryCtx(ctx, "music", "/downloads/music")
	require.NoError(t, err)

	// Step 2: Add torrent with category "music" and ATM enabled.
	t.Log("Adding Wired CD torrent with category 'music' and ATM...")
	err = env.SourceClient().AddTorrentFromUrlCtx(ctx, testTorrentURL, map[string]string{
		"category": "music",
		"autoTMM":  "true",
	})
	require.NoError(t, err)

	torrent := env.WaitForTorrent(env.SourceClient(), wiredCDHash, torrentAppearTimeout)
	require.NotNil(t, torrent)

	// Verify ATM placed the torrent in the category subdirectory.
	t.Logf("Torrent SavePath: %s", torrent.SavePath)
	t.Logf("Torrent ContentPath: %s", torrent.ContentPath)
	t.Logf("Torrent Category: %s", torrent.Category)
	assert.Equal(t, "music", torrent.Category, "torrent should have 'music' category")
	assert.Contains(t, torrent.SavePath, "music",
		"ATM should set save path to category subdirectory")

	// Wait for download to complete.
	t.Log("Waiting for torrent to complete downloading...")
	env.WaitForTorrentComplete(env.SourceClient(), wiredCDHash, torrentDownloadTimeout)
	t.Log("Torrent download complete")

	// Verify files exist at the category subdirectory on the host filesystem.
	files, err := env.SourceClient().GetFilesInformationCtx(ctx, wiredCDHash)
	require.NoError(t, err)
	require.NotNil(t, files)
	require.NotEmpty(t, *files)

	// With ATM + category "music", files are under sourcePath/music/ on the host.
	firstFile := (*files)[0]
	expectedPath := filepath.Join(env.SourcePath(), "music", firstFile.Name)
	_, statErr := os.Stat(expectedPath)
	require.NoError(t, statErr, "file should exist at category subdirectory: %s", expectedPath)
	t.Logf("Verified file exists at: %s", expectedPath)

	// Also verify the file does NOT exist at the base path (without category subdir).
	wrongPath := filepath.Join(env.SourcePath(), firstFile.Name)
	_, statErr = os.Stat(wrongPath)
	assert.True(t, os.IsNotExist(statErr),
		"file should NOT exist at base path (without category subdir): %s", wrongPath)

	// Step 3: Run the orchestrator and verify the sync completes.
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

	t.Log("Waiting for torrent to be synced to destination...")
	env.WaitForTorrentCompleteOnDestination(ctx, wiredCDHash, syncCompleteTimeout,
		"torrent with ATM + category should sync to destination")

	cancelOrchestrator()
	<-orchestratorDone

	env.AssertTorrentCompleteOnDestination(ctx, wiredCDHash)

	// Verify category was preserved on destination.
	destTorrents, err := env.DestinationClient().GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
		Hashes: []string{wiredCDHash},
	})
	require.NoError(t, err)
	require.Len(t, destTorrents, 1)
	assert.Equal(t, "music", destTorrents[0].Category,
		"category should be preserved on destination")

	t.Log("Category with ATM sync flow completed successfully!")

	env.CleanupBothSides(ctx, wiredCDHash)
}

// TestE2E_ExcludeSyncTag verifies that torrents tagged with the exclude-sync tag
// are skipped by the orchestrator, and that removing the tag allows them to sync.
//
// Strategy:
//  1. Download Big Buck Bunny on source
//  2. Apply exclude-sync tag before starting the orchestrator
//  3. Run the orchestrator for several cycles — torrent should NOT appear on destination
//  4. Remove the tag — torrent should sync to destination
func TestE2E_ExcludeSyncTag(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	t.Parallel()

	env := SetupTestEnv(t)
	ctx := context.Background()

	env.CleanupBothSides(ctx, bigBuckBunnyHash)

	// Step 1: Download torrent on source.
	t.Log("Downloading Big Buck Bunny on source...")
	env.DownloadTorrentOnSource(ctx, bigBuckBunnyURL, bigBuckBunnyHash, torrentDownloadTimeout)

	// Step 2: Apply exclude-sync tag to the torrent.
	t.Log("Applying 'no-sync' tag to torrent...")
	err := env.SourceClient().AddTagsCtx(ctx, []string{bigBuckBunnyHash}, "no-sync")
	require.NoError(t, err)

	// Verify the tag was applied.
	require.Eventually(t, func() bool {
		torrents, tagErr := env.SourceClient().GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
			Hashes: []string{bigBuckBunnyHash},
		})
		return tagErr == nil && len(torrents) > 0 && strings.Contains(torrents[0].Tags, "no-sync")
	}, 10*time.Second, time.Second, "no-sync tag should be applied")

	// Step 3: Start orchestrator with exclude-sync-tag configured.
	t.Log("Starting orchestrator with exclude-sync-tag='no-sync'...")
	cfg := env.CreateSourceConfig(WithExcludeSyncTag("no-sync"))
	task, dest, err := env.CreateSourceTask(cfg)
	require.NoError(t, err)
	defer dest.Close()

	orchestratorCtx, cancelOrchestrator := context.WithCancel(ctx)
	defer cancelOrchestrator()

	orchestratorDone := make(chan error, 1)
	go func() {
		orchestratorDone <- task.Run(orchestratorCtx)
	}()

	// Wait several orchestrator cycles (SleepInterval=1s, so 10s gives ~10 cycles).
	t.Log("Waiting 10 seconds to verify torrent is NOT synced...")
	time.Sleep(10 * time.Second)

	// Verify the torrent did NOT appear on destination.
	destTorrents, err := env.DestinationClient().GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
		Hashes: []string{bigBuckBunnyHash},
	})
	require.NoError(t, err)
	assert.Empty(t, destTorrents,
		"torrent with exclude-sync tag should NOT appear on destination")
	t.Log("Confirmed: torrent was excluded from sync")

	// Step 4: Remove the tag — torrent should now sync.
	t.Log("Removing 'no-sync' tag...")
	err = env.SourceClient().RemoveTagsCtx(ctx, []string{bigBuckBunnyHash}, "no-sync")
	require.NoError(t, err)

	// Wait for sync to complete on destination.
	t.Log("Waiting for torrent to sync after tag removal...")
	env.WaitForTorrentCompleteOnDestination(ctx, bigBuckBunnyHash, syncCompleteTimeout,
		"torrent should sync to destination after exclude-sync tag is removed")

	cancelOrchestrator()
	<-orchestratorDone

	env.AssertTorrentCompleteOnDestination(ctx, bigBuckBunnyHash)

	t.Log("Exclude-sync tag E2E test completed successfully!")

	env.CleanupBothSides(ctx, bigBuckBunnyHash)
}

// TestE2E_ExcludeSyncTagReactive verifies that adding the exclude-sync tag to
// a torrent already synced to destination causes the source to forget it from
// the completed cache, and that removing the tag re-discovers it as complete.
//
// Note: the in-progress abort path (abortExcludedTracked) is covered by unit tests.
// Big Buck Bunny syncs too fast (~15s) to reliably catch mid-sync in E2E, so this
// test exercises the completed-torrent path (forgetExcludedCompleted).
//
// Strategy:
//  1. Download Big Buck Bunny on source (no tag)
//  2. Start the orchestrator — torrent syncs to destination
//  3. Wait for torrent to be in the source's completedOnDest cache
//  4. Apply exclude-sync tag
//  5. Assert: torrent is forgotten from completedOnDest cache
//  6. Assert: torrent still exists on destination (files preserved for completed torrents)
//  7. Remove the tag
//  8. Assert: orchestrator re-discovers torrent as complete on destination
func TestE2E_ExcludeSyncTagReactive(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	t.Parallel()

	env := SetupTestEnv(t)
	ctx := context.Background()

	env.CleanupBothSides(ctx, bigBuckBunnyHash)

	// Step 1: Download torrent on source WITHOUT the exclude-sync tag.
	t.Log("Downloading Big Buck Bunny on source...")
	env.DownloadTorrentOnSource(ctx, bigBuckBunnyURL, bigBuckBunnyHash, torrentDownloadTimeout)

	// Step 2: Start orchestrator — torrent should sync to destination.
	t.Log("Starting orchestrator (no exclude tag on torrent)...")
	cfg := env.CreateSourceConfig(WithExcludeSyncTag("no-sync"))
	task, dest, err := env.CreateSourceTask(cfg)
	require.NoError(t, err)
	defer dest.Close()

	orchestratorCtx, cancelOrchestrator := context.WithCancel(ctx)
	defer cancelOrchestrator()

	orchestratorDone := make(chan error, 1)
	go func() {
		orchestratorDone <- task.Run(orchestratorCtx)
	}()

	// Step 3: Wait for the torrent to appear in the source's completedOnDest cache.
	t.Log("Waiting for torrent to complete sync...")
	require.Eventually(t, func() bool {
		for _, hash := range task.FetchCompletedOnDestination() {
			if hash == bigBuckBunnyHash {
				return true
			}
		}
		return false
	}, syncCompleteTimeout, pollInterval,
		"torrent should appear in completedOnDest cache after sync")
	t.Log("Confirmed: torrent is in completedOnDest cache")

	// Step 4: Apply exclude-sync tag.
	t.Log("Applying 'no-sync' tag to synced torrent...")
	err = env.SourceClient().AddTagsCtx(ctx, []string{bigBuckBunnyHash}, "no-sync")
	require.NoError(t, err)

	// Step 5: Wait for the torrent to be forgotten from completedOnDest cache.
	t.Log("Waiting for torrent to be forgotten from completedOnDest cache...")
	require.Eventually(t, func() bool {
		for _, hash := range task.FetchCompletedOnDestination() {
			if hash == bigBuckBunnyHash {
				return false
			}
		}
		return true
	}, 30*time.Second, pollInterval,
		"torrent should be forgotten from completedOnDest after exclude tag is applied")
	t.Log("Confirmed: torrent was forgotten from completedOnDest cache")

	// Step 6: Verify the torrent still exists on destination (files preserved for completed torrents).
	destTorrents, err := env.DestinationClient().GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
		Hashes: []string{bigBuckBunnyHash},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, destTorrents,
		"torrent should still exist on destination (completed torrents are preserved)")
	t.Log("Confirmed: torrent files preserved on destination")

	// Step 7: Remove the tag — orchestrator should re-discover it as complete.
	t.Log("Removing 'no-sync' tag...")
	err = env.SourceClient().RemoveTagsCtx(ctx, []string{bigBuckBunnyHash}, "no-sync")
	require.NoError(t, err)

	// Step 8: Wait for the torrent to re-appear in completedOnDest cache.
	t.Log("Waiting for orchestrator to re-discover torrent as complete...")
	require.Eventually(t, func() bool {
		for _, hash := range task.FetchCompletedOnDestination() {
			if hash == bigBuckBunnyHash {
				return true
			}
		}
		return false
	}, syncCompleteTimeout, pollInterval,
		"torrent should be re-discovered as complete after tag removal")

	cancelOrchestrator()
	<-orchestratorDone

	env.AssertTorrentCompleteOnDestination(ctx, bigBuckBunnyHash)

	t.Log("Reactive exclude-sync tag E2E test completed successfully!")

	env.CleanupBothSides(ctx, bigBuckBunnyHash)
}
