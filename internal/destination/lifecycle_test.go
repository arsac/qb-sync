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

		s.cleanupOrphan(context.Background(), hash, defaultOrphanTimeout)

		assertHealed(t, s, metaDir)
	})

	t.Run("actively seeding complete at own savepath heals", func(t *testing.T) {
		t.Parallel()
		mock := &mockQBClient{torrents: qbTorrent(qbittorrent.TorrentStateUploading, 1.0, ownPath)}
		s, metaDir := newOrphanEnv(t, mock)

		s.cleanupOrphan(context.Background(), hash, defaultOrphanTimeout)

		assertHealed(t, s, metaDir)
	})

	t.Run("savepath mismatch still heals: qB-complete anywhere counts as synced", func(t *testing.T) {
		t.Parallel()
		// Same hash present in qB seeding-complete at a DIFFERENT path (cross-seed).
		// The sync objective is met — heal, matching checkQBCompletion's
		// path-independent COMPLETE. No data is deleted; our copy just lingers.
		mock := &mockQBClient{torrents: qbTorrent(qbittorrent.TorrentStateStoppedUp, 1.0, "/somewhere-else")}
		s, metaDir := newOrphanEnv(t, mock)

		s.cleanupOrphan(context.Background(), hash, defaultOrphanTimeout)

		assertHealed(t, s, metaDir)
	})

	t.Run("seeding-side below 100 percent skips", func(t *testing.T) {
		t.Parallel()
		mock := &mockQBClient{torrents: qbTorrent(qbittorrent.TorrentStateStoppedUp, 0.99, ownPath)}
		s, metaDir := newOrphanEnv(t, mock)

		s.cleanupOrphan(context.Background(), hash, defaultOrphanTimeout)

		assertSkipped(t, s, metaDir)
	})

	t.Run("error state skips", func(t *testing.T) {
		t.Parallel()
		mock := &mockQBClient{torrents: qbTorrent(qbittorrent.TorrentStateMissingFiles, 1.0, ownPath)}
		s, metaDir := newOrphanEnv(t, mock)

		s.cleanupOrphan(context.Background(), hash, defaultOrphanTimeout)

		assertSkipped(t, s, metaDir)
	})

	t.Run("download-side stopped below 100 percent skips", func(t *testing.T) {
		t.Parallel()
		mock := &mockQBClient{torrents: qbTorrent(qbittorrent.TorrentStateStoppedDl, 0.5, ownPath)}
		s, metaDir := newOrphanEnv(t, mock)

		s.cleanupOrphan(context.Background(), hash, defaultOrphanTimeout)

		assertSkipped(t, s, metaDir)
	})

	t.Run("qB unreachable fails closed", func(t *testing.T) {
		t.Parallel()
		mock := &mockQBClient{loginErr: errors.New("connection refused")}
		s, metaDir := newOrphanEnv(t, mock)

		s.cleanupOrphan(context.Background(), hash, defaultOrphanTimeout)

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

		s.cleanupOrphan(context.Background(), hash, defaultOrphanTimeout)

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

	t.Run("concurrent cleanup is single-flight via BeginReclaim", func(t *testing.T) {
		t.Parallel()
		mock := &mockQBClient{torrents: qbTorrent(qbittorrent.TorrentStateStoppedUp, 1.0, ownPath)}
		s, _ := newOrphanEnv(t, mock)

		// Hold the cleanup registration, as a concurrent cleanupOrphan would.
		ch := make(chan struct{})
		require.True(t, s.store.BeginReclaim(hash, ch, func(*serverTorrentState) bool { return true }))

		s.cleanupOrphan(context.Background(), hash, defaultOrphanTimeout)

		require.False(t, s.isFinalized(hash),
			"second cleanup must bail at BeginReclaim, not heal concurrently")
		close(ch)
		s.store.EndCleanup(hash)
	})
}

// TestDeleteOrphanFiles_OnlyRemovesPartials is the safety property behind
// ADR-0002: reclamation may delete only the files this server wrote.
//
// On an unfinalized torrent, everything we wrote lives at a .partial path.
// A file at its final path is pre-existing operator data, a hardlink, or a
// deselected file, and deleting any of those is data loss. Removing both paths
// unconditionally is harmless only while the orphan cleaner never runs, and
// reclamation exists to make it run.
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

// TestSweepEmptyMetaDir pins that the GC can collect its own crash mode - a
// directory left empty by an interrupted RemoveAll, which no other path
// reclaims. See sweepEmptyMetaDir for why.
func TestSweepEmptyMetaDir(t *testing.T) {
	t.Parallel()

	const hash = "abc123"

	newDir := func(t *testing.T, s *Server, files ...string) string {
		t.Helper()
		metaDir := filepath.Join(s.config.BasePath, metaDirName, hash)
		require.NoError(t, os.MkdirAll(metaDir, 0o755))
		for _, f := range files {
			require.NoError(t, os.WriteFile(filepath.Join(metaDir, f), nil, 0o644))
		}
		return metaDir
	}

	t.Run("sweeps a directory holding no metadata", func(t *testing.T) {
		t.Parallel()
		s, _ := newTestDestServer(t)
		metaDir := newDir(t, s)

		require.True(t, s.sweepEmptyMetaDir(context.Background(), hash))
		_, err := os.Stat(metaDir)
		require.True(t, os.IsNotExist(err), "an empty meta directory must be collectable")
	})

	for _, keep := range []string{finalizedFileName, metaFileName, stateFileName} {
		t.Run("keeps a directory holding "+keep, func(t *testing.T) {
			t.Parallel()
			s, _ := newTestDestServer(t)
			metaDir := newDir(t, s, keep)

			require.False(t, s.sweepEmptyMetaDir(context.Background(), hash))
			require.DirExists(t, metaDir)
		})
	}

	t.Run("leaves a directory repopulated with something unrecognised", func(t *testing.T) {
		t.Parallel()
		s, _ := newTestDestServer(t)
		metaDir := newDir(t, s, "something-else")

		// os.Remove, not RemoveAll: a non-empty directory fails harmlessly
		// rather than taking a live torrent's files with it.
		require.False(t, s.sweepEmptyMetaDir(context.Background(), hash))
		require.DirExists(t, metaDir)
	})
}

// TestCleanupOrphanedTorrents_SweepsEmptyDirs pins that the sweep is actually
// wired into the periodic scan, not merely implemented. Without the call site,
// an empty directory survives every cycle forever.
func TestCleanupOrphanedTorrents_SweepsEmptyDirs(t *testing.T) {
	t.Parallel()
	s, tmpDir := newTestDestServer(t)

	empty := filepath.Join(tmpDir, metaDirName, "emptyhash")
	require.NoError(t, os.MkdirAll(empty, 0o755))

	kept := filepath.Join(tmpDir, metaDirName, "finalizedhash")
	require.NoError(t, os.MkdirAll(kept, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(kept, finalizedFileName), nil, 0o644))

	s.cleanupOrphanedTorrents(context.Background())

	_, err := os.Stat(empty)
	require.True(t, os.IsNotExist(err), "the scan must collect a directory holding no metadata")
	require.DirExists(t, kept, "a finalized marker is not swept: no qB snapshot proves it redundant")
}

// TestRetireVanishedMetadata pins that a finalized torrent's metadata is retired
// only once every file it describes is confirmed gone.
//
// This replaces an earlier version keyed on qBittorrent reporting the torrent
// present and seeding, which was backwards: that is the strongest evidence the
// record should be KEPT, since .meta is what tells the inode registry this
// server holds those bytes.
func TestRetireVanishedMetadata(t *testing.T) {
	t.Parallel()

	const hash = "abc123"

	setup := func(t *testing.T, filesOnDisk []string) (*Server, string) {
		t.Helper()
		s, tmpDir := newTestDestServer(t)

		metaDir := filepath.Join(tmpDir, metaDirName, hash)
		require.NoError(t, os.MkdirAll(metaDir, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(metaDir, finalizedFileName), nil, 0o644))
		require.NoError(t, savePersistedMeta(filepath.Join(metaDir, metaFileName), &pb.PersistedTorrentMeta{
			TorrentHash: hash,
			SaveSubPath: "downloads",
			Files: []*pb.PersistedFileInfo{
				{Path: "a.mkv", Size: 1, Selected: true, SourceDevice: 66, SourceInode: 1},
				{Path: "b.mkv", Size: 1, Selected: true, SourceDevice: 66, SourceInode: 2},
			},
		}))

		require.NoError(t, os.MkdirAll(filepath.Join(tmpDir, "downloads"), 0o755))
		for _, f := range filesOnDisk {
			require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "downloads", f), []byte("x"), 0o644))
		}
		return s, metaDir
	}

	t.Run("retires when every file is gone", func(t *testing.T) {
		t.Parallel()
		s, metaDir := setup(t, nil)

		s.retireVanishedMetadata(context.Background())

		_, err := os.Stat(metaDir)
		require.True(t, os.IsNotExist(err), "nothing on disk means the record claims something untrue")
	})

	t.Run("keeps it while any file survives", func(t *testing.T) {
		t.Parallel()
		s, metaDir := setup(t, []string{"b.mkv"})

		s.retireVanishedMetadata(context.Background())

		require.DirExists(t, metaDir, "a surviving file is still a valid hardlink source")
	})

	t.Run("keeps it while all files survive", func(t *testing.T) {
		t.Parallel()
		s, metaDir := setup(t, []string{"a.mkv", "b.mkv"})

		s.retireVanishedMetadata(context.Background())

		require.DirExists(t, metaDir)
	})

	t.Run("leaves unfinalized torrents to the orphan scan", func(t *testing.T) {
		t.Parallel()
		s, metaDir := setup(t, nil)
		require.NoError(t, os.Remove(filepath.Join(metaDir, finalizedFileName)))

		s.retireVanishedMetadata(context.Background())

		require.DirExists(t, metaDir, "an unfinalized torrent is the orphan scan's business, not this pass's")
	})
}

// TestAllFilesVanished pins the fail-closed rule. Anything other than "not
// there" means we cannot tell, and a mount that blipped must not be read as an
// empty library.
func TestAllFilesVanished(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	meta := func(files ...*pb.PersistedFileInfo) *pb.PersistedTorrentMeta {
		return &pb.PersistedTorrentMeta{Files: files}
	}
	selected := func(path string) *pb.PersistedFileInfo {
		return &pb.PersistedFileInfo{Path: path, Selected: true}
	}

	require.True(t, allFilesVanished(tmpDir, meta(selected("gone.mkv"))))

	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "here.mkv"), []byte("x"), 0o644))
	require.False(t, allFilesVanished(tmpDir, meta(selected("here.mkv"))))
	require.False(t, allFilesVanished(tmpDir, meta(selected("gone.mkv"), selected("here.mkv"))))

	// A file the destination never creates proves nothing about the rest.
	require.False(t, allFilesVanished(tmpDir, meta(&pb.PersistedFileInfo{Path: "skip.mkv"})),
		"a metadata record with no selected files must not be read as vanished")

	// Not-there vs cannot-tell: a path whose parent is a file yields ENOTDIR.
	require.False(t, allFilesVanished(tmpDir, meta(selected("here.mkv/child.mkv"))),
		"a stat failing for any reason other than ErrNotExist means we cannot tell")
}
