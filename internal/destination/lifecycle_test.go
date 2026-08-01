package destination

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/autobrr/go-qbittorrent"
	"github.com/bits-and-blooms/bitset"
	"github.com/stretchr/testify/require"

	pb "github.com/arsac/qb-sync/proto"
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
// when stale unfinalized metadata exists and destination qB reports the
// torrent complete on the seeding side AT QB-SYNC'S OWN SAVEPATH, the cleaner
// writes the .finalized marker (qB has verified that data — the marker is
// truthful) instead of skipping forever. Seeding-side INCLUDES stopped/paused:
// qb-sync's success posture leaves torrents stopped until handoff, so the
// crash window between add and marker is exactly a stoppedUP torrent.
//
// Everything else must keep the skip-only behavior: download-side / error /
// sub-100% states (data not known-good), savepath mismatches (qB's copy is
// not ours — healing would delete .meta and foreclose the only path that can
// ever reclaim qb-sync's files), and unreachable qB (fail closed).
func TestCleanupOrphan_HealsQBOwnedCompleteTorrent(t *testing.T) {
	t.Parallel()

	const hash = "orphan-heal-test"
	const subPath = "movies"

	// newOrphanEnv stages stale unfinalized metadata (.state + .meta with
	// SaveSubPath) for hash. The qB-visible savepath for qb-sync's copy is
	// <SavePath>/<subPath>.
	newOrphanEnv := func(t *testing.T, mock *mockQBClient) (*Server, string) {
		t.Helper()
		s, tmpDir := newTestDestServer(t)
		s.qbClient = mock
		s.config.SavePath = "/destination-data"

		metaDir := filepath.Join(tmpDir, metaDirName, hash)
		require.NoError(t, os.MkdirAll(metaDir, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(metaDir, stateFileName), []byte("x"), 0o644))
		require.NoError(t, savePersistedMeta(filepath.Join(metaDir, metaFileName), &pb.PersistedTorrentMeta{
			SchemaVersion: currentSchemaVersion,
			TorrentHash:   hash,
			SaveSubPath:   subPath,
		}))
		return s, metaDir
	}

	qbTorrent := func(state qbittorrent.TorrentState, progress float64, savePath string) []qbittorrent.Torrent {
		return []qbittorrent.Torrent{{Hash: hash, State: state, Progress: progress, SavePath: savePath}}
	}
	ownPath := filepath.Join("/destination-data", subPath)

	assertSkipped := func(t *testing.T, s *Server, metaDir string) {
		t.Helper()
		require.False(t, s.isFinalized(hash), "must not be marked finalized")
		_, statErr := os.Stat(filepath.Join(metaDir, stateFileName))
		require.NoError(t, statErr, "skip must not touch working files")
		_, statErr = os.Stat(filepath.Join(metaDir, metaFileName))
		require.NoError(t, statErr, "skip must preserve .meta — it is the only map to the data files")
	}

	assertHealed := func(t *testing.T, s *Server, metaDir string) {
		t.Helper()
		require.True(t, s.isFinalized(hash), "must self-heal to finalized")
		_, statErr := os.Stat(filepath.Join(metaDir, stateFileName))
		require.True(t, os.IsNotExist(statErr), "markFinalized must clear working files")
	}

	t.Run("seeding-side complete (stopped) at own savepath heals", func(t *testing.T) {
		t.Parallel()
		mock := &mockQBClient{torrents: qbTorrent(qbittorrent.TorrentStateStoppedUp, 1.0, ownPath)}
		s, metaDir := newOrphanEnv(t, mock)

		s.cleanupOrphan(context.Background(), hash)

		assertHealed(t, s, metaDir)
	})

	t.Run("actively seeding complete at own savepath heals", func(t *testing.T) {
		t.Parallel()
		mock := &mockQBClient{torrents: qbTorrent(qbittorrent.TorrentStateUploading, 1.0, ownPath)}
		s, metaDir := newOrphanEnv(t, mock)

		s.cleanupOrphan(context.Background(), hash)

		assertHealed(t, s, metaDir)
	})

	t.Run("savepath mismatch still heals: qB-complete anywhere counts as synced", func(t *testing.T) {
		t.Parallel()
		// Same hash present in qB seeding-complete at a DIFFERENT path (cross-seed).
		// The sync objective is met — heal, matching checkQBCompletion's
		// path-independent COMPLETE. No data is deleted; our copy just lingers.
		mock := &mockQBClient{torrents: qbTorrent(qbittorrent.TorrentStateStoppedUp, 1.0, "/somewhere-else")}
		s, metaDir := newOrphanEnv(t, mock)

		s.cleanupOrphan(context.Background(), hash)

		assertHealed(t, s, metaDir)
	})

	t.Run("seeding-side below 100 percent skips", func(t *testing.T) {
		t.Parallel()
		mock := &mockQBClient{torrents: qbTorrent(qbittorrent.TorrentStateStoppedUp, 0.99, ownPath)}
		s, metaDir := newOrphanEnv(t, mock)

		s.cleanupOrphan(context.Background(), hash)

		assertSkipped(t, s, metaDir)
	})

	t.Run("error state skips", func(t *testing.T) {
		t.Parallel()
		mock := &mockQBClient{torrents: qbTorrent(qbittorrent.TorrentStateMissingFiles, 1.0, ownPath)}
		s, metaDir := newOrphanEnv(t, mock)

		s.cleanupOrphan(context.Background(), hash)

		assertSkipped(t, s, metaDir)
	})

	t.Run("download-side stopped below 100 percent skips", func(t *testing.T) {
		t.Parallel()
		mock := &mockQBClient{torrents: qbTorrent(qbittorrent.TorrentStateStoppedDl, 0.5, ownPath)}
		s, metaDir := newOrphanEnv(t, mock)

		s.cleanupOrphan(context.Background(), hash)

		assertSkipped(t, s, metaDir)
	})

	t.Run("qB unreachable fails closed", func(t *testing.T) {
		t.Parallel()
		mock := &mockQBClient{loginErr: errors.New("connection refused")}
		s, metaDir := newOrphanEnv(t, mock)

		s.cleanupOrphan(context.Background(), hash)

		assertSkipped(t, s, metaDir)
	})

	t.Run("missing .meta still heals: old-format dir, qB reports complete", func(t *testing.T) {
		t.Parallel()
		// Legacy dir with no .meta. Heal no longer needs it (no savepath check);
		// qB-complete is sufficient. Nothing to foreclose — deleteOrphanFiles
		// couldn't reclaim without .meta either.
		mock := &mockQBClient{torrents: qbTorrent(qbittorrent.TorrentStateStoppedUp, 1.0, ownPath)}
		s, metaDir := newOrphanEnv(t, mock)
		require.NoError(t, os.Remove(filepath.Join(metaDir, metaFileName)))

		s.cleanupOrphan(context.Background(), hash)

		require.True(t, s.isFinalized(hash), "old-format dir with qB-complete torrent must heal")
	})

	t.Run("healed torrent is skipped by the next full scan", func(t *testing.T) {
		t.Parallel()
		mock := &mockQBClient{torrents: qbTorrent(qbittorrent.TorrentStateStoppedUp, 1.0, ownPath)}
		s, metaDir := newOrphanEnv(t, mock)
		s.config.OrphanTimeout = time.Nanosecond // everything counts as stale

		// First scan heals via the full path (isOrphanedTorrent -> cleanupOrphan).
		s.cleanupOrphanedTorrents(context.Background())
		assertHealed(t, s, metaDir)

		// Second scan: isFinalized short-circuits isOrphanedTorrent — the heal
		// must be a one-time event, not re-attempted hourly.
		require.False(t, s.isOrphanedTorrent(context.Background(), hash, time.Nanosecond),
			"finalized torrent must never be an orphan candidate again")
	})

	t.Run("concurrent cleanup is single-flight via BeginCleanup", func(t *testing.T) {
		t.Parallel()
		mock := &mockQBClient{torrents: qbTorrent(qbittorrent.TorrentStateStoppedUp, 1.0, ownPath)}
		s, _ := newOrphanEnv(t, mock)

		// Hold the cleanup registration, as a concurrent cleanupOrphan would.
		ch := make(chan struct{})
		require.True(t, s.store.BeginCleanup(hash, ch))

		s.cleanupOrphan(context.Background(), hash)

		require.False(t, s.isFinalized(hash),
			"second cleanup must bail at BeginCleanup, not heal concurrently")
		close(ch)
		s.store.EndCleanup(hash)
	})
}

// TestDeleteOrphanFiles_OnlyRemovesPartials is the safety property behind
// ADR-0002: reclamation may delete only the files this server wrote.
//
// On an unfinalized torrent, everything we wrote lives at a .partial path.
// A file at its final path is pre-existing operator data, a hardlink, or a
// deselected file, and deleting any of those is data loss. Before the fix,
// deleteOrphanFiles removed both paths unconditionally. That was close to
// harmless only because the orphan cleaner almost never ran; the reclamation
// work makes it run, which is what turns this from dormant into live.
func TestDeleteOrphanFiles_OnlyRemovesPartials(t *testing.T) {
	t.Parallel()

	const (
		hash    = "b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0"
		subPath = "movies"
	)

	s, tmpDir := newTestDestServer(t)

	// Two files: one we streamed (.partial), one the operator already had on
	// disk at the right size, which setupFile would have adopted as
	// PreExisting and never written to.
	const (
		streamedName    = "streamed.mkv"
		preExistingName = "operator-had-this.mkv"
	)

	contentDir := filepath.Join(tmpDir, subPath)
	require.NoError(t, os.MkdirAll(contentDir, 0o755))

	partialPath := filepath.Join(contentDir, streamedName) + partialSuffix
	preExistingPath := filepath.Join(contentDir, preExistingName)
	require.NoError(t, os.WriteFile(partialPath, []byte("partially streamed"), 0o644))
	require.NoError(t, os.WriteFile(preExistingPath, []byte("operator data"), 0o644))

	metaDir := filepath.Join(tmpDir, metaDirName, hash)
	require.NoError(t, os.MkdirAll(metaDir, 0o755))
	require.NoError(t, savePersistedMeta(filepath.Join(metaDir, metaFileName), &pb.PersistedTorrentMeta{
		SchemaVersion: currentSchemaVersion,
		TorrentHash:   hash,
		SaveSubPath:   subPath,
		Files: []*pb.PersistedFileInfo{
			{Path: streamedName, Selected: true},
			{Path: preExistingName, Selected: true},
		},
	}))

	deleted := s.deleteOrphanFiles(context.Background(), hash, metaDir)

	require.Equal(t, 1, deleted, "only the .partial file is ours to delete")

	_, statErr := os.Stat(partialPath)
	require.True(t, os.IsNotExist(statErr), "the .partial file we wrote must be reclaimed")

	require.FileExists(t, preExistingPath,
		"a file at its final path is operator data or a hardlink and must survive reclamation")

	data, readErr := os.ReadFile(preExistingPath)
	require.NoError(t, readErr)
	require.Equal(t, "operator data", string(data), "operator data must be untouched, not just present")
}
