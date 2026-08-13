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
	"github.com/arsac/qb-sync/internal/utils"
)

// stageContentOnDestination copies every file of the torrent from the source
// data path to the matching location under the destination data path, producing
// the layout a sync would have produced. This is the "data is already there"
// starting state: an *arr hardlinked it, cross-seed automation put it in place,
// or an operator copied it by hand.
//
// Returns relative path -> inode so callers can prove the files were adopted
// rather than re-transferred.
func stageContentOnDestination(t *testing.T, env *TestEnv, hash string) map[string]uint64 {
	t.Helper()

	staged := make(map[string]uint64)
	for _, srcPath := range torrentContentPaths(t, env, hash) {
		rel, relErr := filepath.Rel(env.SourcePath(), srcPath)
		require.NoError(t, relErr, "relativizing %s", srcPath)

		dstPath := filepath.Join(env.DestinationPath(), rel)
		require.NoError(t, os.MkdirAll(filepath.Dir(dstPath), 0o755))

		data, readErr := os.ReadFile(srcPath)
		require.NoError(t, readErr, "reading %s", srcPath)
		require.NoError(t, os.WriteFile(dstPath, data, 0o644), "writing %s", dstPath)

		staged[rel] = fileInode(t, dstPath)
	}

	require.NotEmpty(t, staged, "expected to stage at least one content file")
	t.Logf("Staged %d content files on the destination before syncing", len(staged))
	return staged
}

func fileInode(t *testing.T, path string) uint64 {
	t.Helper()
	_, ino, err := utils.GetFileID(path)
	require.NoError(t, err, "stat %s", path)
	return ino
}

// assertContentAdopted checks every staged file still carries the inode it had
// before the sync ran. Streaming the data would have written a .partial file and
// renamed it into place, replacing the inode - so an unchanged inode is direct
// evidence the destination reused the operator's copy instead of re-sending
// 53 MB it already had.
func assertContentAdopted(t *testing.T, env *TestEnv, staged map[string]uint64) {
	t.Helper()
	for rel, ino := range staged {
		path := filepath.Join(env.DestinationPath(), rel)
		assert.Equal(t, ino, fileInode(t, path),
			"%s was replaced, so its data was re-streamed instead of adopted", rel)
	}
	assert.Empty(t, env.PartialFiles(),
		"no .partial files should exist: nothing needed streaming")
}

// waitForParkedOnDestination blocks until destination qB reports the torrent in
// a download-side stopped state, which is the wedge the sync has to break out of.
func waitForParkedOnDestination(t *testing.T, env *TestEnv, hash string) qbittorrent.TorrentState {
	t.Helper()
	var last qbittorrent.TorrentState
	require.Eventually(t, func() bool {
		torrents, err := env.DestinationClient().GetTorrentsCtx(context.Background(),
			qbittorrent.TorrentFilterOptions{Hashes: []string{hash}})
		if err != nil || len(torrents) == 0 {
			return false
		}
		last = torrents[0].State
		return last == qbittorrent.TorrentStateStoppedDl || last == qbittorrent.TorrentStatePausedDl
	}, 30*time.Second, time.Second,
		"destination qB should park the torrent in a download-side stopped state, last saw %q", &last)
	return last
}

// TestE2E_DataAlreadyOnDestination covers the case where the destination
// already holds every byte of the torrent but nothing in its qBittorrent
// references it. The sync must be a no-op on the wire, adopt the files as they
// are, and still register and complete the torrent on destination qB.
func TestE2E_DataAlreadyOnDestination(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	t.Parallel()

	env := SetupTestEnv(t)
	ctx := context.Background()

	env.CleanupBothSides(ctx, wiredCDHash)

	t.Log("Downloading Wired CD on source...")
	env.DownloadTorrentOnSource(ctx, testTorrentURL, wiredCDHash, 5*time.Minute)

	staged := stageContentOnDestination(t, env, wiredCDHash)

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

	env.WaitForTorrentCompleteOnDestination(ctx, wiredCDHash, 3*time.Minute,
		"torrent should complete on destination from the data already on disk")
	env.WaitForSyncedTagOnSource(ctx, wiredCDHash, 30*time.Second,
		"source should be tagged synced once the destination finalizes")

	cancelOrchestrator()
	<-orchestratorDone

	env.AssertTorrentCompleteOnDestination(ctx, wiredCDHash)
	assertContentAdopted(t, env, staged)

	env.CleanupBothSides(ctx, wiredCDHash)
}

// TestE2E_DataAlreadyOnDestinationWithParkedTorrent is the regression for the
// wedge that tagged these torrents sync-failed.
//
// When the data is already on the destination it is usually because something
// else put the torrent in destination qB pointing at it - and a torrent added
// stopped sits in stoppedDL, never hash-checked. checkTorrentInQB reports that
// as absent (deliberately: only a seeding-ready torrent at 100% counts as
// complete), so the sync proceeds. AddTorrent is then skipped because qB does
// have the torrent, and before the recheck branch existed nothing else in the
// qB stage could move it: waitForTorrentReady polled a state that never
// changed, timed out, and every retry repeated it identically until the
// source's guard gave up.
//
// The qB poll budget is shrunk so a regression fails in a couple of minutes
// rather than sitting on the ten-minute production floor.
func TestE2E_DataAlreadyOnDestinationWithParkedTorrent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	t.Parallel()

	env := SetupTestEnv(t, WithDestinationServerConfig(func(c *destination.ServerConfig) {
		c.QB.PollInterval = time.Second
		c.QB.PollTimeout = 45 * time.Second
	}))
	ctx := context.Background()

	env.CleanupBothSides(ctx, wiredCDHash)

	t.Log("Downloading Wired CD on source...")
	env.DownloadTorrentOnSource(ctx, testTorrentURL, wiredCDHash, 5*time.Minute)

	staged := stageContentOnDestination(t, env, wiredCDHash)

	t.Log("Adding the torrent to destination qB stopped, so qB never checks it...")
	require.NoError(t, env.AddTorrentToDestination(ctx, testTorrentURL, map[string]string{
		"stopped": "true", // qB v5+
		"paused":  "true", // qB v4.x alias
	}))
	parked := waitForParkedOnDestination(t, env, wiredCDHash)
	t.Logf("Destination qB parked the torrent in %s", parked)

	cfg := env.CreateSourceConfig()
	task, dest, err := env.CreateSourceTask(cfg)
	require.NoError(t, err)
	defer dest.Close()

	orchestratorCtx, cancelOrchestrator := context.WithTimeout(ctx, 4*time.Minute)
	defer cancelOrchestrator()

	orchestratorDone := make(chan error, 1)
	go func() {
		orchestratorDone <- task.Run(orchestratorCtx)
	}()

	env.WaitForTorrentCompleteOnDestination(ctx, wiredCDHash, 4*time.Minute,
		"a parked torrent must be rechecked into a ready state, not waited on until the guard fires")
	env.WaitForSyncedTagOnSource(ctx, wiredCDHash, 30*time.Second,
		"source should be tagged synced, not sync-failed")

	cancelOrchestrator()
	<-orchestratorDone

	env.AssertTorrentCompleteOnDestination(ctx, wiredCDHash)
	assertContentAdopted(t, env, staged)

	assert.False(t, env.SourceTorrentHasTag(ctx, wiredCDHash, defaultSyncFailedTag),
		"torrent must not be quarantined: the data was on disk and the torrent was recoverable")

	env.CleanupBothSides(ctx, wiredCDHash)
}
