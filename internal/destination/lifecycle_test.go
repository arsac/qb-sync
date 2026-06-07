package destination

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/autobrr/go-qbittorrent"
	"github.com/bits-and-blooms/bitset"
	"github.com/stretchr/testify/require"
)

// TestFlushDirtyStates_ReleasesLockDuringIO verifies that flushDirtyStates
// releases state.mu while performing file I/O. Before the snapshot fix,
// state.mu was held during the entire saveState call. A slow or hung
// filesystem (e.g. NFS) would block all WritePiece and FinalizeTorrent
// calls for the same torrent, creating a liveness hazard.
//
// The test injects a saveState that blocks on a channel, simulating slow I/O.
// A concurrent goroutine tries to acquire state.mu during that window.
// With the old code this would deadlock; with the snapshot fix it succeeds.
func TestFlushDirtyStates_ReleasesLockDuringIO(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	logger := testLogger(t)

	written := bitset.New(100)
	written.Set(0)
	state := &serverTorrentState{
		written:          written,
		dirty:            true,
		statePath:        tmpDir + "/.state",
		piecesSinceFlush: 1,
	}

	store := newTorrentStore(tmpDir, logger)
	store.entries["deadlock-test"] = state
	s := &Server{
		config: ServerConfig{BasePath: tmpDir},
		logger: logger,
		store:  store,
	}

	// saveStateFunc blocks until unblockIO is closed, simulating slow disk I/O.
	unblockIO := make(chan struct{})
	ioStarted := make(chan struct{})
	s.saveStateFunc = func(_ string, _ *bitset.BitSet) error {
		close(ioStarted) // signal that we're in the I/O phase
		<-unblockIO      // block until test unblocks us
		return nil
	}

	// Start flush in background.
	flushDone := make(chan struct{})
	go func() {
		s.flushDirtyStates(context.Background())
		close(flushDone)
	}()

	// Wait for flush to enter the I/O phase (lock must be released by now).
	select {
	case <-ioStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for flush to start I/O")
	}

	// Try to acquire state.mu. With the snapshot fix, this succeeds immediately
	// because the lock was released before I/O. With the old code, the lock
	// would still be held and this would block until unblockIO is closed.
	lockAcquired := make(chan struct{})
	go func() {
		state.mu.Lock()
		close(lockAcquired)
		state.mu.Unlock()
	}()

	select {
	case <-lockAcquired:
		// Success: state.mu was free during I/O.
	case <-time.After(100 * time.Millisecond):
		t.Fatal("state.mu was blocked during flush I/O — lock held during file write (old behavior)")
	}

	// Unblock the I/O and let flush complete.
	close(unblockIO)
	<-flushDone

	// Verify state was marked clean.
	state.mu.Lock()
	if state.dirty {
		t.Error("expected dirty=false after successful flush")
	}
	if state.piecesSinceFlush != 0 {
		t.Errorf("expected piecesSinceFlush=0, got %d", state.piecesSinceFlush)
	}
	state.mu.Unlock()
}

// TestFlushDirtyStates_ConcurrentWritesDuringIO verifies that pieces written
// during a flush are not lost. The snapshot captures state at a point in time;
// writes that arrive during I/O must keep the dirty flag set.
func TestFlushDirtyStates_ConcurrentWritesDuringIO(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	logger := testLogger(t)

	written := bitset.New(100)
	for i := range uint(10) {
		written.Set(i)
	}
	state := &serverTorrentState{
		written:          written,
		dirty:            true,
		statePath:        tmpDir + "/.state",
		piecesSinceFlush: 5,
	}

	store := newTorrentStore(tmpDir, logger)
	store.entries["concurrent-test"] = state
	s := &Server{
		config: ServerConfig{BasePath: tmpDir},
		logger: logger,
		store:  store,
	}

	// During the I/O phase, simulate new pieces arriving.
	unblockIO := make(chan struct{})
	ioStarted := make(chan struct{})
	s.saveStateFunc = func(_ string, _ *bitset.BitSet) error {
		close(ioStarted)
		<-unblockIO
		return nil
	}

	var wg sync.WaitGroup

	// Start flush.
	wg.Go(func() {
		s.flushDirtyStates(context.Background())
	})

	// Wait for I/O phase, then simulate concurrent writes.
	<-ioStarted
	state.mu.Lock()
	state.written.Set(50)
	state.dirty = true
	state.piecesSinceFlush += 3 // 3 new pieces arrived during I/O
	state.mu.Unlock()

	// Unblock I/O and let flush complete.
	close(unblockIO)
	wg.Wait()

	// Verify: dirty must remain true because new writes arrived during I/O.
	state.mu.Lock()
	defer state.mu.Unlock()

	if !state.dirty {
		t.Error("dirty should remain true — pieces were written during flush I/O")
	}
	if state.piecesSinceFlush != 3 {
		t.Errorf("piecesSinceFlush should be 3 (new writes only), got %d", state.piecesSinceFlush)
	}
}

// TestCleanupOrphan_HealsQBOwnedCompleteTorrent pins the orphan self-heal:
// when stale unfinalized metadata exists but destination qB reports the
// torrent complete on the seeding side, the cleaner writes the .finalized
// marker (qB has verified that data — the marker is truthful) instead of
// skipping forever. Seeding-side INCLUDES stopped/paused: qb-sync's own
// success posture leaves torrents stopped until handoff, so the
// crash-window-between-add-and-marker case is exactly a stoppedUP torrent.
// Download-side, error, or sub-100% states must keep the skip-only behavior.
func TestCleanupOrphan_HealsQBOwnedCompleteTorrent(t *testing.T) {
	t.Parallel()

	newOrphanEnv := func(t *testing.T, mock *mockQBClient) (*Server, string, string) {
		t.Helper()
		s, tmpDir := newTestDestServer(t)
		s.qbClient = mock

		hash := "orphan-heal-test"
		metaDir := filepath.Join(tmpDir, metaDirName, hash)
		require.NoError(t, os.MkdirAll(metaDir, 0o755))
		// Stale state file, no .finalized marker.
		require.NoError(t, os.WriteFile(filepath.Join(metaDir, stateFileName), []byte("x"), 0o644))
		return s, hash, metaDir
	}

	t.Run("seeding-side complete (stopped) heals to finalized", func(t *testing.T) {
		t.Parallel()
		mock := &mockQBClient{torrents: []qbittorrent.Torrent{
			{Hash: "orphan-heal-test", State: qbittorrent.TorrentStateStoppedUp, Progress: 1.0},
		}}
		s, hash, metaDir := newOrphanEnv(t, mock)

		s.cleanupOrphan(context.Background(), hash)

		require.True(t, s.isFinalized(hash),
			"qB-owned seeding-side complete torrent must self-heal to finalized")
		_, statErr := os.Stat(filepath.Join(metaDir, stateFileName))
		require.True(t, os.IsNotExist(statErr),
			"markFinalized must clear working files, leaving only the marker")
	})

	t.Run("actively seeding complete heals to finalized", func(t *testing.T) {
		t.Parallel()
		mock := &mockQBClient{torrents: []qbittorrent.Torrent{
			{Hash: "orphan-heal-test", State: qbittorrent.TorrentStateUploading, Progress: 1.0},
		}}
		s, hash, _ := newOrphanEnv(t, mock)

		s.cleanupOrphan(context.Background(), hash)

		require.True(t, s.isFinalized(hash))
	})

	t.Run("error state skips without healing", func(t *testing.T) {
		t.Parallel()
		mock := &mockQBClient{torrents: []qbittorrent.Torrent{
			{Hash: "orphan-heal-test", State: qbittorrent.TorrentStateMissingFiles, Progress: 1.0},
		}}
		s, hash, metaDir := newOrphanEnv(t, mock)

		s.cleanupOrphan(context.Background(), hash)

		require.False(t, s.isFinalized(hash),
			"error-state torrent must NOT be marked finalized")
		_, statErr := os.Stat(filepath.Join(metaDir, stateFileName))
		require.NoError(t, statErr, "skip must not touch metadata")
	})

	t.Run("download-side stopped below 100% skips without healing", func(t *testing.T) {
		t.Parallel()
		mock := &mockQBClient{torrents: []qbittorrent.Torrent{
			{Hash: "orphan-heal-test", State: qbittorrent.TorrentStateStoppedDl, Progress: 0.5},
		}}
		s, hash, _ := newOrphanEnv(t, mock)

		s.cleanupOrphan(context.Background(), hash)

		require.False(t, s.isFinalized(hash))
	})
}
