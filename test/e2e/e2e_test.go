//go:build e2e

package e2e

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/autobrr/go-qbittorrent"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/arsac/qb-sync/internal/utils"
	pb "github.com/arsac/qb-sync/proto"
)

const (
	// Big Buck Bunny - small, fast to download test torrent
	bigBuckBunnyURL  = "https://webtorrent.io/torrents/big-buck-bunny.torrent"
	bigBuckBunnyHash = "dd8255ecdc7ca55fb0bbf81323d87062db1f6d1c"

	// Common test timeouts
	torrentDownloadTimeout = 5 * time.Minute
	largeDownloadTimeout   = 10 * time.Minute
	syncCompleteTimeout    = 3 * time.Minute
	orchestratorTimeout    = 5 * time.Minute
	pollInterval           = 2 * time.Second
	shortPollInterval      = time.Second
)

func TestE2E_QBitTorrentConnectivity(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := SetupTestEnv(t)
	ctx := context.Background()

	// Verify hot qBittorrent is accessible
	version, err := env.HotClient().GetAppVersionCtx(ctx)
	require.NoError(t, err)
	t.Logf("Hot qBittorrent version: %s", version)

	// Verify cold qBittorrent is accessible
	version, err = env.ColdClient().GetAppVersionCtx(ctx)
	require.NoError(t, err)
	t.Logf("Cold qBittorrent version: %s", version)
}

func TestE2E_GRPCConnectivity(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

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

func TestE2E_AddTorrentToHot(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := SetupTestEnv(t)
	ctx := context.Background()

	// Cleanup any existing torrents
	env.CleanupTorrent(ctx, env.HotClient(), sintelHash)

	// Add torrent to hot
	err := env.AddTorrentToHot(ctx, testTorrentURL, nil)
	require.NoError(t, err)

	// Wait for torrent
	torrent := env.WaitForTorrent(env.HotClient(), sintelHash, 30*time.Second)
	require.NotNil(t, torrent)
	t.Logf("Added torrent: %s (%s)", torrent.Name, torrent.Hash)

	// Cleanup
	env.CleanupTorrent(ctx, env.HotClient(), sintelHash)
}

func TestE2E_HotTaskCreation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := SetupTestEnv(t)

	cfg := env.CreateHotConfig(WithDryRun(true))
	task, dest, err := env.CreateHotTask(cfg)
	require.NoError(t, err)
	require.NotNil(t, task)
	defer dest.Close()

	t.Log("Successfully created hot task with gRPC destination")
}

func TestE2E_DryRunDoesNotDelete(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := SetupTestEnv(t)
	ctx := context.Background()

	// Cleanup any existing torrents
	env.CleanupTorrent(ctx, env.HotClient(), sintelHash)

	// Add torrent to hot
	err := env.AddTorrentToHot(ctx, testTorrentURL, nil)
	require.NoError(t, err)

	// Wait for torrent
	torrent := env.WaitForTorrent(env.HotClient(), sintelHash, 30*time.Second)
	require.NotNil(t, torrent)

	// Create task in dry run mode with Force to bypass space check
	cfg := env.CreateHotConfig(WithDryRun(true), WithForce(true))
	task, dest, err := env.CreateHotTask(cfg)
	require.NoError(t, err)
	defer dest.Close()

	// Login to source client
	err = task.Login(ctx)
	require.NoError(t, err)

	// Mark torrent as complete on cold (simulates sync without actually syncing)
	task.MarkCompletedOnCold(sintelHash)

	// Run maybeMoveToCold in dry run mode
	err = task.MaybeMoveToCold(ctx)
	require.NoError(t, err)

	// Torrent should still exist on hot (dry run)
	torrents, err := env.HotClient().GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
		Hashes: []string{sintelHash},
	})
	require.NoError(t, err)
	assert.Len(t, torrents, 1, "torrent should still exist on hot in dry run mode")

	// Cleanup
	env.CleanupTorrent(ctx, env.HotClient(), sintelHash)
}

func TestE2E_InitTorrentOnCold(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := SetupTestEnv(t)
	ctx := context.Background()

	// Create gRPC destination
	dest, err := env.CreateGRPCDestination()
	require.NoError(t, err)
	defer dest.Close()

	// Add torrent to hot first to get metadata
	env.CleanupTorrent(ctx, env.HotClient(), sintelHash)
	err = env.AddTorrentToHot(ctx, testTorrentURL, nil)
	require.NoError(t, err)

	// Wait for torrent
	torrent := env.WaitForTorrent(env.HotClient(), sintelHash, 30*time.Second)
	require.NotNil(t, torrent)

	// Get torrent properties for piece info
	props, err := env.HotClient().GetTorrentPropertiesCtx(ctx, sintelHash)
	require.NoError(t, err)

	// Export torrent file
	torrentData, err := env.HotClient().ExportTorrentCtx(ctx, sintelHash)
	require.NoError(t, err)

	// Get file info
	files, err := env.HotClient().GetFilesInformationCtx(ctx, sintelHash)
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

	// Initialize on cold via gRPC
	_, err = dest.InitTorrent(ctx, &pb.InitTorrentRequest{
		TorrentHash: sintelHash,
		Name:        torrent.Name,
		NumPieces:   int32(props.PiecesNum),
		PieceSize:   int64(props.PieceSize),
		TotalSize:   torrent.Size,
		TorrentFile: torrentData,
		Files:       pbFiles,
	})
	require.NoError(t, err)

	t.Log("Successfully initialized torrent on cold server")

	// Cleanup
	env.CleanupTorrent(ctx, env.HotClient(), sintelHash)
}

// TestE2E_FullSyncFlow tests the complete hot->cold sync flow:
// 1. Download torrent on hot
// 2. Run hot orchestrator to stream pieces to cold
// 3. Verify torrent is finalized and added to cold qBittorrent
// 4. Verify synced tag is added on hot
func TestE2E_FullSyncFlow(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := SetupTestEnv(t)
	ctx := context.Background()

	env.CleanupBothSides(ctx, bigBuckBunnyHash)

	t.Log("Adding Big Buck Bunny torrent to hot...")
	env.DownloadTorrentOnHot(ctx, bigBuckBunnyURL, bigBuckBunnyHash, 5*time.Minute)

	cfg := env.CreateHotConfig()
	task, dest, err := env.CreateHotTask(cfg)
	require.NoError(t, err)
	defer dest.Close()

	orchestratorCtx, cancelOrchestrator := context.WithTimeout(ctx, 3*time.Minute)
	defer cancelOrchestrator()

	orchestratorDone := make(chan error, 1)
	go func() {
		orchestratorDone <- task.Run(orchestratorCtx)
	}()

	t.Log("Waiting for torrent to be synced to cold...")
	env.WaitForTorrentCompleteOnCold(ctx, bigBuckBunnyHash, 3*time.Minute, "torrent should be complete on cold")

	cancelOrchestrator()
	<-orchestratorDone

	env.AssertTorrentCompleteOnCold(ctx, bigBuckBunnyHash)

	// Verify cold qBittorrent's savePath is the container mount point, not the hot's path.
	coldTorrents, err := env.ColdClient().GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
		Hashes: []string{bigBuckBunnyHash},
	})
	require.NoError(t, err)
	require.Len(t, coldTorrents, 1)
	assert.Equal(t, "/cold-data", coldTorrents[0].SavePath,
		"cold torrent savePath should be the container mount point")

	t.Log("Full sync flow completed successfully!")

	env.CleanupBothSides(ctx, bigBuckBunnyHash)
}

// TestE2E_TorrentSeedsOnHotAfterSync verifies that after a regular sync the
// torrent continues seeding on hot (no stop). The stop only happens during
// disk pressure cleanup in maybeMoveToCold.
func TestE2E_TorrentSeedsOnHotAfterSync(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := SetupTestEnv(t)
	ctx := context.Background()

	env.CleanupBothSides(ctx, bigBuckBunnyHash)

	t.Log("Adding Big Buck Bunny torrent to hot...")
	env.DownloadTorrentOnHot(ctx, bigBuckBunnyURL, bigBuckBunnyHash, 5*time.Minute)

	// No Force, no space pressure → torrent should stay on hot after sync
	cfg := env.CreateHotConfig()
	task, dest, err := env.CreateHotTask(cfg)
	require.NoError(t, err)
	defer dest.Close()

	orchestratorCtx, cancelOrchestrator := context.WithTimeout(ctx, 3*time.Minute)
	defer cancelOrchestrator()

	orchestratorDone := make(chan error, 1)
	go func() {
		orchestratorDone <- task.Run(orchestratorCtx)
	}()

	t.Log("Waiting for torrent to be synced to cold...")
	env.WaitForTorrentCompleteOnCold(ctx, bigBuckBunnyHash, 3*time.Minute, "torrent should be complete on cold")

	cancelOrchestrator()
	<-orchestratorDone

	// Torrent should still exist on hot and NOT be stopped
	torrents, err := env.HotClient().GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
		Hashes: []string{bigBuckBunnyHash},
	})
	require.NoError(t, err)
	require.Len(t, torrents, 1, "torrent should still exist on hot after sync")

	assert.False(t, env.IsTorrentStopped(ctx, env.HotClient(), bigBuckBunnyHash),
		"torrent on hot should still be seeding after sync (state: %s)", torrents[0].State)

	env.AssertTorrentCompleteOnCold(ctx, bigBuckBunnyHash)

	// Cold torrent should be STOPPED (explicitly stopped during finalization, not yet started).
	// Use Eventually because the stop may still be propagating through qBittorrent.
	require.Eventually(t, func() bool {
		return env.IsTorrentStopped(ctx, env.ColdClient(), bigBuckBunnyHash)
	}, 15*time.Second, time.Second,
		"torrent on cold should be stopped after sync (no dual seeding)")

	t.Log("Torrent correctly continues seeding on hot after sync, cold is stopped!")

	env.CleanupBothSides(ctx, bigBuckBunnyHash)
}

// TestE2E_StopBeforeDeleteOnDiskPressure verifies that during disk pressure
// cleanup, torrents are stopped on hot before being deleted. This ensures
// a clean handoff: cold is already seeding, hot stops, then hot deletes.
func TestE2E_StopBeforeDeleteOnDiskPressure(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := SetupTestEnv(t)
	ctx := context.Background()

	env.CleanupBothSides(ctx, bigBuckBunnyHash)

	t.Log("Adding Big Buck Bunny torrent to hot...")
	env.DownloadTorrentOnHot(ctx, bigBuckBunnyURL, bigBuckBunnyHash, 5*time.Minute)

	// First: sync to cold without Force (torrent stays on hot)
	cfg := env.CreateHotConfig()
	task, dest, err := env.CreateHotTask(cfg)
	require.NoError(t, err)
	defer dest.Close()

	orchestratorCtx, cancelOrchestrator := context.WithTimeout(ctx, 3*time.Minute)
	defer cancelOrchestrator()

	orchestratorDone := make(chan error, 1)
	go func() {
		orchestratorDone <- task.Run(orchestratorCtx)
	}()

	t.Log("Waiting for torrent to be synced to cold...")
	env.WaitForTorrentCompleteOnCold(ctx, bigBuckBunnyHash, 3*time.Minute, "torrent should be complete on cold")

	cancelOrchestrator()
	<-orchestratorDone

	env.AssertTorrentCompleteOnCold(ctx, bigBuckBunnyHash)

	// Verify torrent is still seeding on hot (not stopped yet)
	torrents, err := env.HotClient().GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
		Hashes: []string{bigBuckBunnyHash},
	})
	require.NoError(t, err)
	require.Len(t, torrents, 1, "torrent should still exist on hot")
	assert.False(t, env.IsTorrentStopped(ctx, env.HotClient(), bigBuckBunnyHash),
		"torrent should be seeding on hot before disk pressure cleanup")

	// Verify cold torrent is STOPPED before handoff (explicitly stopped during finalization).
	// Use Eventually because the stop may still be propagating through qBittorrent.
	require.Eventually(t, func() bool {
		return env.IsTorrentStopped(ctx, env.ColdClient(), bigBuckBunnyHash)
	}, 15*time.Second, time.Second,
		"cold torrent should be stopped before disk pressure handoff")

	// Now: simulate disk pressure with Force + no min seeding time
	t.Log("Running disk pressure cleanup (Force mode)...")
	forceCfg := env.CreateHotConfig(WithForce(true), WithMinSeedingTime(0))
	forceTask, forceDest, err := env.CreateHotTask(forceCfg)
	require.NoError(t, err)
	defer forceDest.Close()

	err = forceTask.Login(ctx)
	require.NoError(t, err)

	// Mark as completed on cold so maybeMoveToCold picks it up
	forceTask.MarkCompletedOnCold(bigBuckBunnyHash)

	err = forceTask.MaybeMoveToCold(ctx)
	require.NoError(t, err)

	// Torrent should now be deleted from hot (stop → start cold → delete)
	env.WaitForTorrentDeleted(ctx, env.HotClient(), bigBuckBunnyHash, 10*time.Second,
		"torrent should be deleted from hot after disk pressure cleanup")

	// Cold torrent should now be SEEDING (started during handoff).
	// qBittorrent may take a moment to transition from stoppedUP to an active state.
	require.Eventually(t, func() bool {
		return !env.IsTorrentStopped(ctx, env.ColdClient(), bigBuckBunnyHash)
	}, 15*time.Second, time.Second,
		"torrent on cold should be seeding after disk pressure handoff")

	t.Log("Torrent handed off: hot deleted, cold now seeding!")

	env.CleanupBothSides(ctx, bigBuckBunnyHash)
}

// TestE2E_ColdServerRestart tests that streaming can recover after the cold gRPC server restarts.
// Scenario: Start streaming, stop cold server mid-stream, restart cold server, verify sync completes.
func TestE2E_ColdServerRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := SetupTestEnv(t)
	ctx := context.Background()

	env.CleanupBothSides(ctx, bigBuckBunnyHash)

	t.Log("Adding Big Buck Bunny torrent to hot...")
	env.DownloadTorrentOnHot(ctx, bigBuckBunnyURL, bigBuckBunnyHash, 5*time.Minute)

	cfg := env.CreateHotConfig()
	task, dest, err := env.CreateHotTask(cfg)
	require.NoError(t, err)
	defer dest.Close()

	orchestratorCtx, cancelOrchestrator := context.WithTimeout(ctx, 5*time.Minute)
	defer cancelOrchestrator()

	orchestratorDone := make(chan error, 1)
	go func() {
		orchestratorDone <- task.Run(orchestratorCtx)
	}()

	t.Log("Waiting for streaming to start and make progress...")
	require.Eventually(t, func() bool {
		progress, progressErr := task.Progress(ctx, bigBuckBunnyHash)
		if progressErr != nil {
			return false
		}
		t.Logf("Streaming progress: %d/%d pieces", progress.Streamed, progress.TotalPieces)
		return progress.Streamed > 0 && progress.Streamed >= progress.TotalPieces/10
	}, 2*time.Minute, time.Second, "streaming should start and make progress")

	t.Log("Stopping cold gRPC server mid-stream...")
	env.StopColdServer()
	time.Sleep(5 * time.Second)

	t.Log("Restarting cold gRPC server...")
	env.StartColdServer()

	t.Log("Waiting for sync to complete after cold server restart...")
	env.WaitForTorrentCompleteOnCold(ctx, bigBuckBunnyHash, 3*time.Minute, "torrent should be complete on cold after restart")

	cancelOrchestrator()
	<-orchestratorDone

	env.AssertTorrentCompleteOnCold(ctx, bigBuckBunnyHash)

	env.CleanupBothSides(ctx, bigBuckBunnyHash)
}

// TestE2E_ColdQBittorrentRestart tests that finalization can recover after cold qBittorrent restarts.
// Scenario: Stream completes, stop cold qBittorrent before finalization, restart, verify sync completes.
func TestE2E_ColdQBittorrentRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := SetupTestEnv(t)
	ctx := context.Background()

	env.CleanupBothSides(ctx, bigBuckBunnyHash)

	t.Log("Adding Big Buck Bunny torrent to hot...")
	env.DownloadTorrentOnHot(ctx, bigBuckBunnyURL, bigBuckBunnyHash, 5*time.Minute)

	t.Log("Stopping cold qBittorrent before sync...")
	err := env.StopColdQBittorrent(ctx)
	require.NoError(t, err)

	cfg := env.CreateHotConfig()
	task, dest, err := env.CreateHotTask(cfg)
	require.NoError(t, err)
	defer dest.Close()

	orchestratorCtx, cancelOrchestrator := context.WithTimeout(ctx, 5*time.Minute)
	defer cancelOrchestrator()

	orchestratorDone := make(chan error, 1)
	go func() {
		orchestratorDone <- task.Run(orchestratorCtx)
	}()

	t.Log("Waiting for streaming to complete (cold qB is down)...")
	time.Sleep(15 * time.Second)

	t.Log("Restarting cold qBittorrent...")
	err = env.StartColdQBittorrent(ctx)
	require.NoError(t, err)

	t.Log("Waiting for sync to complete after cold qBittorrent restart...")
	env.WaitForTorrentCompleteOnCold(ctx, bigBuckBunnyHash, 3*time.Minute, "torrent should be complete on cold after qBittorrent restart")

	cancelOrchestrator()
	<-orchestratorDone

	env.AssertTorrentCompleteOnCold(ctx, bigBuckBunnyHash)

	env.CleanupBothSides(ctx, bigBuckBunnyHash)
}

// TestE2E_HotQBittorrentRestart tests that the orchestrator can recover after hot qBittorrent restarts.
// Scenario: Start orchestrator, stop hot qBittorrent, restart, verify sync completes.
func TestE2E_HotQBittorrentRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := SetupTestEnv(t)
	ctx := context.Background()

	env.CleanupBothSides(ctx, bigBuckBunnyHash)

	t.Log("Adding Big Buck Bunny torrent to hot...")
	env.DownloadTorrentOnHot(ctx, bigBuckBunnyURL, bigBuckBunnyHash, 5*time.Minute)

	cfg := env.CreateHotConfig()
	task, dest, err := env.CreateHotTask(cfg)
	require.NoError(t, err)
	defer dest.Close()

	orchestratorCtx, cancelOrchestrator := context.WithTimeout(ctx, 5*time.Minute)
	defer cancelOrchestrator()

	orchestratorDone := make(chan error, 1)
	go func() {
		orchestratorDone <- task.Run(orchestratorCtx)
	}()

	t.Log("Waiting for streaming to start...")
	time.Sleep(2 * time.Second)

	t.Log("Stopping hot qBittorrent mid-stream...")
	err = env.StopHotQBittorrent(ctx)
	require.NoError(t, err)

	time.Sleep(5 * time.Second)

	t.Log("Restarting hot qBittorrent...")
	err = env.StartHotQBittorrent(ctx)
	require.NoError(t, err)

	torrent := env.WaitForTorrent(env.HotClient(), bigBuckBunnyHash, 30*time.Second)
	require.NotNil(t, torrent, "torrent should still exist after hot qBittorrent restart")

	t.Log("Waiting for sync to complete after hot qBittorrent restart...")
	env.WaitForTorrentCompleteOnCold(ctx, bigBuckBunnyHash, 3*time.Minute, "torrent should be complete on cold after hot qBittorrent restart")

	cancelOrchestrator()
	<-orchestratorDone

	env.AssertTorrentCompleteOnCold(ctx, bigBuckBunnyHash)

	env.CleanupBothSides(ctx, bigBuckBunnyHash)
}

// TestE2E_OrchestratorRestart tests that a new orchestrator can resume syncing.
// Scenario: Start orchestrator, stop it mid-sync, start new orchestrator, verify sync completes.
func TestE2E_OrchestratorRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := SetupTestEnv(t)
	ctx := context.Background()

	env.CleanupBothSides(ctx, bigBuckBunnyHash)

	t.Log("Adding Big Buck Bunny torrent to hot...")
	env.DownloadTorrentOnHot(ctx, bigBuckBunnyURL, bigBuckBunnyHash, 5*time.Minute)

	cfg := env.CreateHotConfig()
	task1, dest1, err := env.CreateHotTask(cfg)
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
	task2, dest2, err := env.CreateHotTask(cfg)
	require.NoError(t, err)
	defer dest2.Close()

	orchestratorCtx2, cancelOrchestrator2 := context.WithTimeout(ctx, 3*time.Minute)
	defer cancelOrchestrator2()

	orchestratorDone2 := make(chan error, 1)
	go func() {
		orchestratorDone2 <- task2.Run(orchestratorCtx2)
	}()

	t.Log("Waiting for sync to complete with second orchestrator...")
	env.WaitForTorrentCompleteOnCold(ctx, bigBuckBunnyHash, 3*time.Minute, "torrent should be complete on cold after orchestrator restart")

	cancelOrchestrator2()
	<-orchestratorDone2

	env.AssertTorrentCompleteOnCold(ctx, bigBuckBunnyHash)

	env.CleanupBothSides(ctx, bigBuckBunnyHash)
}

// TestE2E_HardlinkDetection verifies that hardlink detection works in the Docker test environment.
// This is a prerequisite for the hardlink grouping logic used during space management.
func TestE2E_HardlinkDetection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := SetupTestEnv(t)

	// Create original file
	originalPath := env.CreateTestFile("torrent-a/video.mkv", 1024*1024) // 1MB

	// Create hardlink in different directory
	hardlinkDir := filepath.Join(env.HotPath(), "torrent-b")
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
// Scenario: Two torrents share a hardlinked file, both complete on cold,
// when space cleanup runs, both should be deleted as a group.
func TestE2E_HardlinkGroupDeletion(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := SetupTestEnv(t)
	ctx := context.Background()

	// We'll use Big Buck Bunny and Sintel - two different torrents
	// After they download, we'll hardlink a file between their directories
	// to simulate cross-seeded content

	env.CleanupBothSides(ctx, bigBuckBunnyHash, sintelHash)

	t.Log("Adding Big Buck Bunny and Sintel torrents to hot...")
	err := env.AddTorrentToHot(ctx, bigBuckBunnyURL, nil)
	require.NoError(t, err)
	err = env.AddTorrentToHot(ctx, testTorrentURL, nil) // Sintel
	require.NoError(t, err)

	// Wait for both to appear
	bbbTorrent := env.WaitForTorrent(env.HotClient(), bigBuckBunnyHash, 30*time.Second)
	require.NotNil(t, bbbTorrent)
	sintelTorrent := env.WaitForTorrent(env.HotClient(), sintelHash, 30*time.Second)
	require.NotNil(t, sintelTorrent)

	// Wait for both to complete
	t.Log("Waiting for torrents to complete downloading...")
	env.WaitForTorrentComplete(env.HotClient(), bigBuckBunnyHash, 5*time.Minute)
	env.WaitForTorrentComplete(env.HotClient(), sintelHash, 10*time.Minute)
	t.Log("Both torrents downloaded")

	// Sync both to cold via orchestrator (so they actually exist on cold qBittorrent)
	cfg := env.CreateHotConfig()
	syncTask, syncDest, err := env.CreateHotTask(cfg)
	require.NoError(t, err)

	orchestratorCtx, cancelOrchestrator := context.WithTimeout(ctx, 5*time.Minute)
	defer cancelOrchestrator()

	orchestratorDone := make(chan error, 1)
	go func() {
		orchestratorDone <- syncTask.Run(orchestratorCtx)
	}()

	t.Log("Waiting for both torrents to sync to cold...")
	env.WaitForTorrentCompleteOnCold(ctx, bigBuckBunnyHash, 3*time.Minute, "BBB should be complete on cold")
	env.WaitForTorrentCompleteOnCold(ctx, sintelHash, 5*time.Minute, "Sintel should be complete on cold")

	cancelOrchestrator()
	<-orchestratorDone
	syncDest.Close()
	t.Log("Both torrents synced to cold")

	// Get file info to find actual file paths
	bbbFiles, err := env.HotClient().GetFilesInformationCtx(ctx, bigBuckBunnyHash)
	require.NoError(t, err)
	require.NotNil(t, bbbFiles)
	require.NotEmpty(t, *bbbFiles)

	sintelFiles, err := env.HotClient().GetFilesInformationCtx(ctx, sintelHash)
	require.NoError(t, err)
	require.NotNil(t, sintelFiles)
	require.NotEmpty(t, *sintelFiles)

	// Create hardlink between a BBB file and a Sintel file
	// We need to hardlink FROM a BBB file TO a path that Sintel expects
	// Since they have different file structures, we'll add an extra hardlinked file
	// to BBB's directory that matches one of Sintel's files

	// Get first file from each torrent - use env.HotPath() for host filesystem access
	// (qBittorrent's SavePath is the container path, not accessible from host)
	sintelFirstFile := (*sintelFiles)[0]
	sintelFilePath := filepath.Join(env.HotPath(), sintelFirstFile.Name)

	bbbFirstFile := (*bbbFiles)[0]
	bbbFilePath := filepath.Join(env.HotPath(), bbbFirstFile.Name)

	t.Logf("BBB file: %s", bbbFilePath)
	t.Logf("Sintel file: %s", sintelFilePath)

	// Verify the original files exist and have different inodes
	bbbInode, err := utils.GetInode(bbbFilePath)
	require.NoError(t, err, "BBB file should exist")
	sintelInode, err := utils.GetInode(sintelFilePath)
	require.NoError(t, err, "Sintel file should exist")
	t.Logf("Original inodes - BBB: %d, Sintel: %d", bbbInode, sintelInode)
	require.NotEqual(t, bbbInode, sintelInode, "files should have different inodes initially")

	// Now, to simulate hardlinking, we need to make one file a hardlink of the other.
	// In a real cross-seed scenario, you'd have the SAME content file shared between torrents.
	// For testing, we'll remove the Sintel file and replace it with a hardlink to BBB's file.
	// (This would break the actual torrent verification, but we're just testing the grouping logic)
	require.NoError(t, os.Remove(sintelFilePath))
	require.NoError(t, os.Link(bbbFilePath, sintelFilePath))

	// Verify they're now hardlinked
	linked, err := utils.AreHardlinked(bbbFilePath, sintelFilePath)
	require.NoError(t, err)
	require.True(t, linked, "files should now be hardlinked")
	t.Log("Successfully created hardlink between torrent files")

	// Create hot task with Force mode (to bypass space check) and no min seeding time
	forceCfg := env.CreateHotConfig(WithForce(true), WithMinSeedingTime(0))
	task, dest, err := env.CreateHotTask(forceCfg)
	require.NoError(t, err)
	defer dest.Close()

	// Login
	err = task.Login(ctx)
	require.NoError(t, err)

	// Mark both as complete on cold (new task instance needs to know about cold state)
	task.MarkCompletedOnCold(bigBuckBunnyHash)
	task.MarkCompletedOnCold(sintelHash)
	t.Log("Both torrents marked as complete on cold")

	// Run maybeMoveToCold - this should detect the hardlink group and delete both
	t.Log("Running maybeMoveToCold...")
	err = task.MaybeMoveToCold(ctx)
	require.NoError(t, err)

	// Verify BOTH torrents were deleted (because they're in the same hardlink group)
	env.WaitForTorrentDeleted(ctx, env.HotClient(), bigBuckBunnyHash, 10*time.Second,
		"Big Buck Bunny should be deleted")
	env.WaitForTorrentDeleted(ctx, env.HotClient(), sintelHash, 10*time.Second,
		"Sintel should be deleted (hardlink group)")

	t.Log("Both hardlinked torrents were deleted together as expected!")

	env.CleanupBothSides(ctx, bigBuckBunnyHash, sintelHash)
}

// TestE2E_ColdHardlinkDeduplication tests that the cold server creates hardlinks
// for files that share the same inode as previously synced files.
// Scenario:
//  1. Sync Torrent A to cold (registers file inodes in cold's inode tracking)
//  2. Create a fake Torrent B on hot with files hardlinked to Torrent A's files
//  3. Initialize Torrent B on cold - cold should detect the inodes and create hardlinks
//  4. Verify InitTorrentResponse shows hardlinked=true and piecesCovered=true
//  5. Verify files on cold are actual hardlinks (same inode)
func TestE2E_ColdHardlinkDeduplication(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := SetupTestEnv(t)
	ctx := context.Background()

	env.CleanupBothSides(ctx, bigBuckBunnyHash)

	// Step 1: Sync Big Buck Bunny to cold (this registers the file inodes)
	t.Log("Adding Big Buck Bunny torrent to hot...")
	torrent := env.DownloadTorrentOnHot(ctx, bigBuckBunnyURL, bigBuckBunnyHash, 5*time.Minute)

	// Get file info and inodes from HOT filesystem BEFORE sync
	// These are the SOURCE inodes that will be registered on cold during sync
	hotFiles, err := env.HotClient().GetFilesInformationCtx(ctx, bigBuckBunnyHash)
	require.NoError(t, err)
	require.NotNil(t, hotFiles)
	require.NotEmpty(t, *hotFiles)

	t.Log("Getting inodes of files on HOT filesystem...")
	type fileWithInode struct {
		path  string
		size  int64
		inode uint64
	}
	var hotFileInodes []fileWithInode

	for _, f := range *hotFiles {
		// f.Name from qBittorrent API includes the torrent folder (e.g. "Big Buck Bunny/file.mp4")
		filePath := filepath.Join(env.HotPath(), f.Name)
		inode, inodeErr := utils.GetInode(filePath)
		require.NoError(t, inodeErr, "should be able to get inode for %s", filePath)
		hotFileInodes = append(hotFileInodes, fileWithInode{
			path:  f.Name,
			size:  f.Size,
			inode: inode,
		})
		t.Logf("  HOT File: %s, Size: %d, Inode: %d", f.Name, f.Size, inode)
	}

	// Create hot task and run orchestrator to sync to cold
	cfg := env.CreateHotConfig()
	task, dest, err := env.CreateHotTask(cfg)
	require.NoError(t, err)
	defer dest.Close()

	orchestratorCtx, cancelOrchestrator := context.WithTimeout(ctx, 3*time.Minute)
	defer cancelOrchestrator()

	orchestratorDone := make(chan error, 1)
	go func() {
		orchestratorDone <- task.Run(orchestratorCtx)
	}()

	// Wait for sync to complete (torrent complete on cold qBittorrent)
	t.Log("Waiting for Big Buck Bunny to sync to cold...")
	env.WaitForTorrentCompleteOnCold(ctx, bigBuckBunnyHash, 3*time.Minute, "torrent should be complete on cold")

	// Stop orchestrator
	cancelOrchestrator()
	<-orchestratorDone

	t.Log("Big Buck Bunny synced to cold - HOT inodes should now be registered")

	// Step 2: Create a second gRPC destination and initialize a "fake" torrent B
	// with the same HOT file inodes as torrent A
	dest2, err := env.CreateGRPCDestination()
	require.NoError(t, err)
	defer dest2.Close()

	// Build file info with the SAME HOT inodes as the synced files
	var pbFiles []*pb.FileInfo
	var offset int64
	for _, f := range hotFileInodes {
		// f.path is like "Big Buck Bunny/file.mp4", use just the base filename
		baseName := filepath.Base(f.path)
		pbFiles = append(pbFiles, &pb.FileInfo{
			Path:   "FakeTorrentB/" + baseName, // Different path, same HOT inode
			Size:   f.size,
			Offset: offset,
			Inode:  f.inode, // Same HOT inode - this triggers hardlink detection!
		})
		offset += f.size
	}

	// Get properties for piece info
	props, err := env.HotClient().GetTorrentPropertiesCtx(ctx, bigBuckBunnyHash)
	require.NoError(t, err)

	fakeTorrentHash := "fffffffffffffffffffffffffffffffffffffffb" // Fake hash for torrent B

	t.Log("Initializing fake Torrent B with same inodes on cold...")
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

	// All files should be hardlinked since they have matching HOT inodes
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

	// Step 5: Verify the files on cold filesystem are actual hardlinks
	t.Log("Verifying files on cold filesystem are hardlinks...")
	for i, f := range hotFileInodes {
		originalPath := filepath.Join(env.ColdPath(), f.path)
		// The fake torrent B uses "FakeTorrentB/" prefix instead of the original torrent folder
		// f.path is like "Big Buck Bunny/file.mp4", so we need the base filename
		baseName := filepath.Base(f.path)
		hardlinkPath := filepath.Join(env.ColdPath(), "FakeTorrentB", baseName)

		// Verify hardlink exists
		_, statErr := os.Stat(hardlinkPath)
		require.NoError(t, statErr, "hardlinked file should exist: %s", hardlinkPath)

		// Verify they share the same cold inode (are actually hardlinked on cold)
		originalInode, err := utils.GetInode(originalPath)
		require.NoError(t, err)
		hardlinkInode, err := utils.GetInode(hardlinkPath)
		require.NoError(t, err)

		assert.Equal(t, originalInode, hardlinkInode,
			"file %d should have same cold inode (original=%d, hardlink=%d)",
			i, originalInode, hardlinkInode)

		// Verify using AreHardlinked utility
		linked, err := utils.AreHardlinked(originalPath, hardlinkPath)
		require.NoError(t, err)
		assert.True(t, linked, "files should be detected as hardlinked")
	}

	t.Log("Cold hardlink deduplication test completed successfully!")
	t.Logf("  - Files hardlinked: %d/%d", hardlinkedCount, len(pbFiles))
	t.Logf("  - Pieces covered: %d/%d", coveredCount, len(initResult.PiecesNeeded))
	t.Log("  - All hardlinks verified on filesystem")

	env.CleanupBothSides(ctx, bigBuckBunnyHash)
}

// TestE2E_NonHardlinkedDeletedIndependently verifies that non-hardlinked torrents
// are deleted independently based on their individual priority.
func TestE2E_NonHardlinkedDeletedIndependently(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := SetupTestEnv(t)
	ctx := context.Background()

	// Cleanup
	env.CleanupTorrent(ctx, env.HotClient(), bigBuckBunnyHash)
	env.CleanupTorrent(ctx, env.HotClient(), sintelHash)

	// Add both torrents
	t.Log("Adding Big Buck Bunny and Sintel torrents...")
	err := env.AddTorrentToHot(ctx, bigBuckBunnyURL, nil)
	require.NoError(t, err)
	err = env.AddTorrentToHot(ctx, testTorrentURL, nil)
	require.NoError(t, err)

	// Wait for both to appear
	env.WaitForTorrent(env.HotClient(), bigBuckBunnyHash, 30*time.Second)
	env.WaitForTorrent(env.HotClient(), sintelHash, 30*time.Second)

	// Wait for both to download
	t.Log("Waiting for torrents to download...")
	env.WaitForTorrentComplete(env.HotClient(), bigBuckBunnyHash, 5*time.Minute)
	env.WaitForTorrentComplete(env.HotClient(), sintelHash, 10*time.Minute)
	t.Log("Both torrents downloaded")

	// Sync both to cold via orchestrator (so they actually exist on cold qBittorrent)
	cfg := env.CreateHotConfig()
	syncTask, syncDest, err := env.CreateHotTask(cfg)
	require.NoError(t, err)

	orchestratorCtx, cancelOrchestrator := context.WithTimeout(ctx, 5*time.Minute)
	defer cancelOrchestrator()

	orchestratorDone := make(chan error, 1)
	go func() {
		orchestratorDone <- syncTask.Run(orchestratorCtx)
	}()

	t.Log("Waiting for both torrents to sync to cold...")
	env.WaitForTorrentCompleteOnCold(ctx, bigBuckBunnyHash, 3*time.Minute, "BBB should be complete on cold")
	env.WaitForTorrentCompleteOnCold(ctx, sintelHash, 5*time.Minute, "Sintel should be complete on cold")

	cancelOrchestrator()
	<-orchestratorDone
	syncDest.Close()
	t.Log("Both torrents synced to cold")

	// Verify they're NOT hardlinked (they shouldn't be - different files)
	torrents, err := env.HotClient().GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
		Hashes: []string{bigBuckBunnyHash, sintelHash},
	})
	require.NoError(t, err)
	require.Len(t, torrents, 2)

	// Create task with Force mode
	forceCfg := env.CreateHotConfig(WithForce(true), WithMinSeedingTime(0))
	task, dest, err := env.CreateHotTask(forceCfg)
	require.NoError(t, err)
	defer dest.Close()

	// Mark both as complete on cold (new task instance needs to know about cold state)
	task.MarkCompletedOnCold(bigBuckBunnyHash)
	task.MarkCompletedOnCold(sintelHash)

	err = task.Login(ctx)
	require.NoError(t, err)

	// Run maybeMoveToCold
	t.Log("Running maybeMoveToCold...")
	err = task.MaybeMoveToCold(ctx)
	require.NoError(t, err)

	// Both should be deleted (independently, not as a group)
	env.WaitForTorrentDeleted(ctx, env.HotClient(), bigBuckBunnyHash, 10*time.Second,
		"Big Buck Bunny should be deleted")
	env.WaitForTorrentDeleted(ctx, env.HotClient(), sintelHash, 10*time.Second,
		"Sintel should be deleted")

	t.Log("Non-hardlinked torrents were deleted independently as expected!")

	env.CleanupBothSides(ctx, bigBuckBunnyHash, sintelHash)
}

// TestE2E_FullSyncFlowSintel tests the full sync flow with Sintel,
// which has 11 files (video + multiple subtitle languages).
// This validates multi-file torrent handling more thoroughly.
func TestE2E_FullSyncFlowSintel(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := SetupTestEnv(t)
	ctx := context.Background()

	env.CleanupBothSides(ctx, sintelHash)

	t.Log("Adding Sintel torrent to hot (11 files)...")
	err := env.AddTorrentToHot(ctx, testTorrentURL, nil)
	require.NoError(t, err)

	torrent := env.WaitForTorrent(env.HotClient(), sintelHash, 30*time.Second)
	require.NotNil(t, torrent)
	t.Logf("Torrent added: %s", torrent.Name)

	// Get file count to verify it's multi-file
	files, err := env.HotClient().GetFilesInformationCtx(ctx, sintelHash)
	require.NoError(t, err)
	require.NotNil(t, files)
	t.Logf("Torrent has %d files", len(*files))
	assert.GreaterOrEqual(t, len(*files), 10, "Sintel should have at least 10 files")

	// Wait for torrent to complete downloading
	t.Log("Waiting for torrent to complete downloading...")
	env.WaitForTorrentComplete(env.HotClient(), sintelHash, 10*time.Minute)
	t.Log("Torrent download complete")

	// Create hot task and run the orchestrator
	cfg := env.CreateHotConfig()
	task, dest, err := env.CreateHotTask(cfg)
	require.NoError(t, err)
	defer dest.Close()

	// Run the orchestrator in background with timeout
	orchestratorCtx, cancelOrchestrator := context.WithTimeout(ctx, 5*time.Minute)
	defer cancelOrchestrator()

	orchestratorDone := make(chan error, 1)
	go func() {
		orchestratorDone <- task.Run(orchestratorCtx)
	}()

	// Wait for torrent to be synced (complete on cold qBittorrent)
	t.Log("Waiting for torrent to be synced to cold...")
	env.WaitForTorrentCompleteOnCold(ctx, sintelHash, 5*time.Minute, "torrent should be complete on cold")

	t.Log("Torrent is complete on cold qBittorrent")

	// Stop the orchestrator
	cancelOrchestrator()
	<-orchestratorDone

	env.AssertTorrentCompleteOnCold(ctx, sintelHash)

	// Verify all files exist on cold
	coldFiles, err := env.ColdClient().GetFilesInformationCtx(ctx, sintelHash)
	require.NoError(t, err)
	require.NotNil(t, coldFiles)
	assert.Equal(t, len(*files), len(*coldFiles), "cold should have same number of files as hot")

	t.Logf("Full sync flow with Sintel (%d files) completed successfully!", len(*coldFiles))

	env.CleanupBothSides(ctx, sintelHash)
}

// TestE2E_OrphanCleanupOnTorrentRemoval tests that partial files are cleaned up
// when a torrent is removed from hot qBittorrent mid-stream.
// This verifies the active cleanup path: hot detects removal -> notifies cold -> cold aborts.
func TestE2E_OrphanCleanupOnTorrentRemoval(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := SetupTestEnv(t)
	ctx := context.Background()

	env.CleanupBothSides(ctx, bigBuckBunnyHash)

	t.Log("Adding Big Buck Bunny torrent to hot...")
	env.DownloadTorrentOnHot(ctx, bigBuckBunnyURL, bigBuckBunnyHash, 5*time.Minute)

	// Create hot task
	cfg := env.CreateHotConfig()
	task, dest, err := env.CreateHotTask(cfg)
	require.NoError(t, err)
	defer dest.Close()

	// Run orchestrator in background
	orchestratorCtx, cancelOrchestrator := context.WithTimeout(ctx, 3*time.Minute)
	defer cancelOrchestrator()

	orchestratorDone := make(chan error, 1)
	go func() {
		orchestratorDone <- task.Run(orchestratorCtx)
	}()

	// Wait for streaming to start and make some progress (but not complete)
	t.Log("Waiting for streaming to start...")
	var initialProgress int
	require.Eventually(t, func() bool {
		progress, progressErr := task.Progress(ctx, bigBuckBunnyHash)
		if progressErr != nil {
			return false
		}
		initialProgress = progress.Streamed
		t.Logf("Streaming progress: %d/%d pieces", progress.Streamed, progress.TotalPieces)
		// Wait for at least some pieces to be streamed
		return progress.Streamed > 0 && progress.Streamed < progress.TotalPieces
	}, 2*time.Minute, time.Second, "streaming should start")

	t.Logf("Streaming started with %d pieces transferred, now removing torrent from hot...", initialProgress)

	// Check that partial files exist on cold before removal
	coldMetaDir := filepath.Join(env.ColdPath(), ".qbsync", bigBuckBunnyHash)
	_, metaErr := os.Stat(coldMetaDir)
	require.NoError(t, metaErr, "cold meta directory should exist during streaming")
	t.Logf("Cold meta directory exists: %s", coldMetaDir)

	// Delete the torrent from hot qBittorrent mid-stream
	// This should trigger: PieceMonitor detects removal -> orchestrator handles -> calls AbortTorrent
	t.Log("Deleting torrent from hot qBittorrent...")
	err = env.HotClient().DeleteTorrentsCtx(ctx, []string{bigBuckBunnyHash}, true)
	require.NoError(t, err)
	t.Log("Torrent deleted from hot")

	// Wait for cleanup to propagate to cold
	// The orchestrator should detect the removal and call AbortTorrent on cold
	t.Log("Waiting for cold server to clean up partial files...")
	require.Eventually(t, func() bool {
		// Check if meta directory has been removed
		_, statErr := os.Stat(coldMetaDir)
		if os.IsNotExist(statErr) {
			return true
		}
		t.Log("Meta directory still exists, waiting...")
		return false
	}, 30*time.Second, time.Second, "cold meta directory should be cleaned up after torrent removal")

	t.Log("Cold meta directory cleaned up successfully")

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

	// Verify torrent is NOT on cold qBittorrent (was never finalized)
	coldTorrents, err := env.ColdClient().GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
		Hashes: []string{bigBuckBunnyHash},
	})
	require.NoError(t, err)
	assert.Empty(t, coldTorrents, "torrent should NOT exist on cold qBittorrent (was aborted)")

	// Stop the orchestrator gracefully
	cancelOrchestrator()
	<-orchestratorDone

	t.Log("Orphan cleanup on torrent removal test completed successfully!")

	env.CleanupBothSides(ctx, bigBuckBunnyHash)
}

// TestE2E_CategoryWithATM tests that torrents with a category and Automatic Torrent
// Management (ATM) enabled sync correctly. When ATM is enabled, qBittorrent moves
// files into a category-specific subdirectory (e.g., /downloads/movies/), which means
// torrent.SavePath differs from the base DataPath. The orchestrator must handle this
// path difference when reading files for streaming and when constructing paths for
// hardlink detection.
func TestE2E_CategoryWithATM(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := SetupTestEnv(t)
	ctx := context.Background()

	env.CleanupBothSides(ctx, bigBuckBunnyHash)

	// Step 1: Create a category "movies" with save path /downloads/movies on hot qBittorrent.
	// This causes ATM-managed torrents in this category to save under /downloads/movies/.
	t.Log("Creating 'movies' category on hot qBittorrent...")
	err := env.HotClient().CreateCategoryCtx(ctx, "movies", "/downloads/movies")
	require.NoError(t, err)

	// Step 2: Add torrent with category "movies" and ATM enabled.
	t.Log("Adding Big Buck Bunny torrent with category 'movies' and ATM...")
	err = env.HotClient().AddTorrentFromUrlCtx(ctx, bigBuckBunnyURL, map[string]string{
		"category": "movies",
		"autoTMM":  "true",
	})
	require.NoError(t, err)

	torrent := env.WaitForTorrent(env.HotClient(), bigBuckBunnyHash, torrentAppearTimeout)
	require.NotNil(t, torrent)

	// Verify ATM placed the torrent in the category subdirectory.
	t.Logf("Torrent SavePath: %s", torrent.SavePath)
	t.Logf("Torrent ContentPath: %s", torrent.ContentPath)
	t.Logf("Torrent Category: %s", torrent.Category)
	assert.Equal(t, "movies", torrent.Category, "torrent should have 'movies' category")
	assert.Contains(t, torrent.SavePath, "movies",
		"ATM should set save path to category subdirectory")

	// Wait for download to complete.
	t.Log("Waiting for torrent to complete downloading...")
	env.WaitForTorrentComplete(env.HotClient(), bigBuckBunnyHash, torrentDownloadTimeout)
	t.Log("Torrent download complete")

	// Verify files exist at the category subdirectory on the host filesystem.
	files, err := env.HotClient().GetFilesInformationCtx(ctx, bigBuckBunnyHash)
	require.NoError(t, err)
	require.NotNil(t, files)
	require.NotEmpty(t, *files)

	// With ATM + category "movies", files are under hotPath/movies/ on the host.
	firstFile := (*files)[0]
	expectedPath := filepath.Join(env.HotPath(), "movies", firstFile.Name)
	_, statErr := os.Stat(expectedPath)
	require.NoError(t, statErr, "file should exist at category subdirectory: %s", expectedPath)
	t.Logf("Verified file exists at: %s", expectedPath)

	// Also verify the file does NOT exist at the base path (without category subdir).
	wrongPath := filepath.Join(env.HotPath(), firstFile.Name)
	_, statErr = os.Stat(wrongPath)
	assert.True(t, os.IsNotExist(statErr),
		"file should NOT exist at base path (without category subdir): %s", wrongPath)

	// Step 3: Run the orchestrator and verify the sync completes.
	cfg := env.CreateHotConfig()
	task, dest, err := env.CreateHotTask(cfg)
	require.NoError(t, err)
	defer dest.Close()

	orchestratorCtx, cancelOrchestrator := context.WithTimeout(ctx, orchestratorTimeout)
	defer cancelOrchestrator()

	orchestratorDone := make(chan error, 1)
	go func() {
		orchestratorDone <- task.Run(orchestratorCtx)
	}()

	t.Log("Waiting for torrent to be synced to cold...")
	env.WaitForTorrentCompleteOnCold(ctx, bigBuckBunnyHash, syncCompleteTimeout,
		"torrent with ATM + category should sync to cold")

	cancelOrchestrator()
	<-orchestratorDone

	env.AssertTorrentCompleteOnCold(ctx, bigBuckBunnyHash)

	// Verify category was preserved on cold.
	coldTorrents, err := env.ColdClient().GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
		Hashes: []string{bigBuckBunnyHash},
	})
	require.NoError(t, err)
	require.Len(t, coldTorrents, 1)
	assert.Equal(t, "movies", coldTorrents[0].Category,
		"category should be preserved on cold")

	t.Log("Category with ATM sync flow completed successfully!")

	env.CleanupBothSides(ctx, bigBuckBunnyHash)
}
