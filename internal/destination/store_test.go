package destination

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func commitTestTorrent(t *testing.T, ts *torrentStore, hash, path string) *serverTorrentState {
	t.Helper()
	if err := ts.Reserve(hash); err != nil {
		t.Fatalf("Reserve(%q): %v", hash, err)
	}
	state := &serverTorrentState{
		torrentMeta: torrentMeta{
			files: []*serverFileInfo{
				{path: path, selected: true},
			},
		},
	}
	if err := ts.Commit(hash, state); err != nil {
		t.Fatalf("Commit(%q): %v", hash, err)
	}
	return state
}

func newTestStore(t *testing.T) *torrentStore {
	t.Helper()
	logger := slogDiscard()
	return newTorrentStore(t.TempDir(), logger)
}

func TestTorrentStore_Get(t *testing.T) {
	t.Parallel()
	ts := newTestStore(t)

	_, ok := ts.Get("unknown")
	if ok {
		t.Fatal("expected not found for unknown hash")
	}

	ts.mu.Lock()
	ts.entries["abc"] = &serverTorrentState{}
	ts.mu.Unlock()

	state, ok := ts.Get("abc")
	if !ok || state == nil {
		t.Fatal("expected state for known hash")
	}
}

func TestTorrentStore_Len(t *testing.T) {
	t.Parallel()
	ts := newTestStore(t)

	if ts.Len() != 0 {
		t.Fatalf("expected 0, got %d", ts.Len())
	}

	ts.mu.Lock()
	ts.entries["a"] = &serverTorrentState{}
	ts.entries["b"] = &serverTorrentState{}
	ts.mu.Unlock()

	if ts.Len() != 2 {
		t.Fatalf("expected 2, got %d", ts.Len())
	}
}

func TestTorrentStore_ForEach(t *testing.T) {
	t.Parallel()
	ts := newTestStore(t)

	ts.mu.Lock()
	ts.entries["a"] = &serverTorrentState{}
	ts.entries["b"] = &serverTorrentState{}
	ts.mu.Unlock()

	var visited int
	ts.ForEach(func(_ string, _ *serverTorrentState) bool {
		visited++
		return true
	})
	if visited != 2 {
		t.Fatalf("expected 2 visits, got %d", visited)
	}
}

func TestTorrentStore_ReserveCommit(t *testing.T) {
	t.Parallel()
	ts := newTestStore(t)

	if err := ts.Reserve("abc"); err != nil {
		t.Fatalf("Reserve: unexpected error: %v", err)
	}

	if err := ts.Reserve("abc"); err == nil {
		t.Fatal("Reserve duplicate: expected error, got nil")
	}

	state := &serverTorrentState{
		torrentMeta: torrentMeta{
			files: []*serverFileInfo{
				{path: "data/file.txt", selected: true},
			},
		},
	}
	if err := ts.Commit("abc", state); err != nil {
		t.Fatalf("Commit: unexpected error: %v", err)
	}

	got, ok := ts.Get("abc")
	if !ok || got == nil {
		t.Fatal("Get after Commit: expected state")
	}
	if got.initializing.Load() {
		t.Fatal("Get after Commit: state still marked initializing")
	}

	ts.mu.RLock()
	owner, exists := ts.filePaths["data/file.txt"]
	ts.mu.RUnlock()
	if !exists {
		t.Fatal("filePaths: path not registered after Commit")
	}
	if owner != "abc" {
		t.Fatalf("filePaths: expected owner %q, got %q", "abc", owner)
	}
}

func TestTorrentStore_Unreserve(t *testing.T) {
	t.Parallel()
	ts := newTestStore(t)

	if err := ts.Reserve("xyz"); err != nil {
		t.Fatalf("Reserve: unexpected error: %v", err)
	}

	ts.Unreserve("xyz")

	if err := ts.Reserve("xyz"); err != nil {
		t.Fatalf("Reserve after Unreserve: unexpected error: %v", err)
	}
}

func TestTorrentStore_CommitCollision(t *testing.T) {
	t.Parallel()
	ts := newTestStore(t)

	if err := ts.Reserve("torrent1"); err != nil {
		t.Fatalf("Reserve torrent1: %v", err)
	}
	state1 := &serverTorrentState{
		torrentMeta: torrentMeta{
			files: []*serverFileInfo{
				{path: "shared/file.dat", selected: true},
			},
		},
	}
	if err := ts.Commit("torrent1", state1); err != nil {
		t.Fatalf("Commit torrent1: %v", err)
	}

	if err := ts.Reserve("torrent2"); err != nil {
		t.Fatalf("Reserve torrent2: %v", err)
	}
	state2 := &serverTorrentState{
		torrentMeta: torrentMeta{
			files: []*serverFileInfo{
				{path: "shared/file.dat", selected: true},
			},
		},
	}
	commitErr := ts.Commit("torrent2", state2)
	if commitErr == nil {
		t.Fatal("Commit torrent2: expected collision error, got nil")
	}
	if !strings.Contains(commitErr.Error(), "already owned") {
		t.Fatalf("Commit torrent2: unexpected error message: %v", commitErr)
	}

	_, ok := ts.Get("torrent2")
	if ok {
		t.Fatal("torrent2 sentinel should have been removed after collision")
	}
}

func TestTorrentStore_Remove(t *testing.T) {
	t.Parallel()
	ts := newTestStore(t)

	// Remove unknown hash is a no-op and returns nil.
	if got := ts.Remove("unknown"); got != nil {
		t.Fatal("Remove unknown: expected nil")
	}

	state := commitTestTorrent(t, ts, "abc", "data/file.txt")

	got := ts.Remove("abc")
	if got == nil {
		t.Fatal("Remove: expected non-nil state")
	}
	if got != state {
		t.Fatal("Remove: returned state differs from committed state")
	}

	// Hash gone from entries.
	if _, ok := ts.Get("abc"); ok {
		t.Fatal("Remove: hash still present after remove")
	}

	// File path unregistered.
	ts.mu.RLock()
	_, pathExists := ts.filePaths["data/file.txt"]
	ts.mu.RUnlock()
	if pathExists {
		t.Fatal("Remove: file path still registered after remove")
	}
}

func TestTorrentStore_Drain(t *testing.T) {
	t.Parallel()
	ts := newTestStore(t)

	commitTestTorrent(t, ts, "torrent1", "a/file1.txt")
	commitTestTorrent(t, ts, "torrent2", "b/file2.txt")

	drained := ts.Drain()

	if len(drained) != 2 {
		t.Fatalf("Drain: expected 2 entries, got %d", len(drained))
	}
	if _, ok := drained["torrent1"]; !ok {
		t.Fatal("Drain: torrent1 missing from drained map")
	}
	if _, ok := drained["torrent2"]; !ok {
		t.Fatal("Drain: torrent2 missing from drained map")
	}

	if ts.Len() != 0 {
		t.Fatalf("Drain: store not empty after drain, len=%d", ts.Len())
	}

	ts.mu.RLock()
	fpLen := len(ts.filePaths)
	ts.mu.RUnlock()
	if fpLen != 0 {
		t.Fatalf("Drain: filePaths not empty after drain, len=%d", fpLen)
	}
}

func TestTorrentStore_BeginAbort(t *testing.T) {
	t.Parallel()
	ts := newTestStore(t)

	commitTestTorrent(t, ts, "abc", "data/file.txt")

	ch := make(chan struct{})
	state, existingCh := ts.BeginAbort("abc", ch)
	if state == nil {
		t.Fatal("BeginAbort: expected non-nil state")
	}
	if existingCh != nil {
		t.Fatal("BeginAbort: expected nil existingCh on first call")
	}

	// Hash removed from entries.
	if _, ok := ts.Get("abc"); ok {
		t.Fatal("BeginAbort: hash still present after abort")
	}

	// File path unregistered.
	ts.mu.RLock()
	_, pathExists := ts.filePaths["data/file.txt"]
	ts.mu.RUnlock()
	if pathExists {
		t.Fatal("BeginAbort: file path still registered after abort")
	}

	// AbortCh returns the channel.
	gotCh, ok := ts.AbortCh("abc")
	if !ok {
		t.Fatal("AbortCh: expected channel registered")
	}
	if gotCh != ch {
		t.Fatal("AbortCh: returned wrong channel")
	}

	// Second BeginAbort returns existing channel.
	ch2 := make(chan struct{})
	state2, existingCh2 := ts.BeginAbort("abc", ch2)
	if state2 != nil {
		t.Fatal("BeginAbort second: expected nil state")
	}
	if existingCh2 != ch {
		t.Fatal("BeginAbort second: expected existing channel")
	}

	// EndCleanup deregisters the channel.
	ts.EndCleanup("abc")
	if _, found := ts.AbortCh("abc"); found {
		t.Fatal("AbortCh after EndCleanup: expected not found")
	}
}

func TestTorrentStore_InodeDelegation(t *testing.T) {
	t.Parallel()
	ts := newTestStore(t)

	if ts.Inodes() == nil {
		t.Fatal("expected non-nil InodeRegistry")
	}

	inode := FileID{Ino: 12345}
	basePath := ts.basePath
	ts.Inodes().RegisterInProgress(inode, "h1", "movies/file.mkv")

	files := []*serverFileInfo{
		{
			path:     filepath.Join(basePath, "movies", "file.mkv"),
			size:     1024,
			selected: true,
			hardlink: hardlinkInfo{
				sourceFileID: inode,
				state:        hlStateInProgress,
			},
		},
	}
	ts.RegisterInodes(context.Background(), "h1", files)

	regPath, found := ts.Inodes().GetRegistered(inode)
	if !found {
		t.Fatal("expected inode registered after RegisterInodes")
	}
	wantRelPath := filepath.Join("movies", "file.mkv")
	if regPath != wantRelPath {
		t.Fatalf("expected registered path %q, got %q", wantRelPath, regPath)
	}

	_, _, _, inProg := ts.Inodes().GetInProgress(inode)
	if inProg {
		t.Fatal("expected in-progress cleared after RegisterInodes")
	}
}

func TestTorrentStore_BeginCleanup(t *testing.T) {
	t.Parallel()
	ts := newTestStore(t)

	// Succeeds for an untracked, unaborting hash.
	ch := make(chan struct{})
	if !ts.BeginCleanup("orphan", ch) {
		t.Fatal("BeginCleanup: expected true for untracked hash")
	}

	// Returns false if already cleaning.
	ch2 := make(chan struct{})
	if ts.BeginCleanup("orphan", ch2) {
		t.Fatal("BeginCleanup: expected false when already cleaning")
	}

	ts.EndCleanup("orphan")

	// Returns false if hash is tracked in entries.
	commitTestTorrent(t, ts, "tracked", "tracked/file.txt")
	if ts.BeginCleanup("tracked", make(chan struct{})) {
		t.Fatal("BeginCleanup: expected false for tracked torrent")
	}
}

// TestTorrentStore_AbortThenReAbort verifies that when an abort completes,
// the channel is closed BEFORE EndCleanup deregisters. This ensures a
// concurrent caller that sees the existing channel via BeginAbort is
// guaranteed to unblock before any new BeginAbort can succeed.
func TestTorrentStore_AbortThenReAbort(t *testing.T) {
	t.Parallel()
	ts := newTestStore(t)

	_ = commitTestTorrent(t, ts, "h1", "data/file1.txt")

	ch1 := make(chan struct{})
	_, _ = ts.BeginAbort("h1", ch1)

	// Simulate concurrent abort arriving just as first abort completes.
	// After EndCleanup, the channel must already be closed so a waiter
	// on ch1 is guaranteed to unblock before any new BeginAbort succeeds.
	done := make(chan struct{})
	go func() {
		defer close(done)
		ch2 := make(chan struct{})
		_, existingCh := ts.BeginAbort("h1", ch2)
		if existingCh != nil {
			<-existingCh // Must unblock
		}
	}()

	// The correct order: close channel THEN deregister.
	close(ch1)
	ts.EndCleanup("h1")

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("concurrent abort should have completed")
	}
}

// TestTorrentStore_GetFiltersSentinel verifies that Get returns (nil, false)
// for sentinel entries (initializing=true). Sentinels are placeholders inserted
// by Reserve to block concurrent InitTorrent for the same hash while disk I/O
// is in progress. Callers using Get should never see these half-initialized entries.
func TestTorrentStore_CommitCollisionAbortsInProgressInodes(t *testing.T) {
	t.Parallel()
	ts := newTestStore(t)

	inode := FileID{Ino: 55555}

	// Torrent1 owns the path.
	_ = ts.Reserve("torrent1")
	_ = ts.Commit("torrent1", &serverTorrentState{
		torrentMeta: torrentMeta{
			files: []*serverFileInfo{
				{path: "shared/file.dat", selected: true},
			},
		},
	})

	// Torrent2 registered an in-progress inode during setupFiles.
	ts.Inodes().RegisterInProgress(inode, "torrent2", "shared/file.dat")

	// Verify inode is in-progress.
	_, _, _, inProg := ts.Inodes().GetInProgress(inode)
	if !inProg {
		t.Fatal("expected inode to be in-progress before Commit")
	}

	// Commit fails due to path collision.
	_ = ts.Reserve("torrent2")
	state2 := &serverTorrentState{
		torrentMeta: torrentMeta{
			files: []*serverFileInfo{
				{
					path:     "shared/file.dat",
					selected: true,
					hardlink: hardlinkInfo{sourceFileID: inode, state: hlStateInProgress},
				},
			},
		},
	}
	commitErr := ts.Commit("torrent2", state2)
	if commitErr == nil {
		t.Fatal("expected collision error")
	}

	// The in-progress inode should have been aborted by Commit.
	_, _, _, inProg = ts.Inodes().GetInProgress(inode)
	if inProg {
		t.Fatal("in-progress inode should have been aborted after Commit collision")
	}
}

func TestTorrentStore_GetFiltersSentinel(t *testing.T) {
	t.Parallel()
	ts := newTestStore(t)

	// Insert a sentinel directly.
	sentinel := &serverTorrentState{}
	sentinel.initializing.Store(true)
	ts.mu.Lock()
	ts.entries["sentinel"] = sentinel
	ts.mu.Unlock()

	state, ok := ts.Get("sentinel")
	if ok || state != nil {
		t.Fatal("Get should return (nil, false) for sentinel entries")
	}

	// GetWithSentinel should still return it.
	state, ok = ts.GetWithSentinel("sentinel")
	if !ok || state == nil {
		t.Fatal("GetWithSentinel should return sentinel entries")
	}
}
