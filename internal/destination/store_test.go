package destination

import (
	"context"
	"path/filepath"
	"strings"
	"sync/atomic"
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

func TestTorrentStore_BeginReclaim(t *testing.T) {
	t.Parallel()
	ts := newTestStore(t)

	stale := func(*serverTorrentState) bool { return true }
	active := func(*serverTorrentState) bool { return false }

	// Succeeds for a hash with no entry; pred is not consulted because there is
	// nothing to re-test.
	ch := make(chan struct{})
	if !ts.BeginReclaim("orphan", ch, func(*serverTorrentState) bool {
		t.Error("pred must not run for a hash with no entry")
		return true
	}) {
		t.Fatal("BeginReclaim: expected true for a hash with no entry")
	}

	// Returns false if already cleaning.
	if ts.BeginReclaim("orphan", make(chan struct{}), stale) {
		t.Fatal("BeginReclaim: expected false when already cleaning")
	}
	ts.EndCleanup("orphan")

	// An entry that pred rejects is left alone: this is the resume race, where
	// a source picked the torrent back up after the scan judged it stale.
	commitTestTorrent(t, ts, "resumed", "resumed/file.txt")
	if ts.BeginReclaim("resumed", make(chan struct{}), active) {
		t.Fatal("BeginReclaim: must refuse an entry pred rejects")
	}
	if _, present := ts.peek("resumed"); !present {
		t.Fatal("a refused reclaim must leave the entry in place")
	}

	// An entry pred accepts is dropped and registered in one step.
	commitTestTorrent(t, ts, "orphan", "orphan/file.txt")
	if !ts.BeginReclaim("orphan", make(chan struct{}), stale) {
		t.Fatal("BeginReclaim: expected true for a stale entry")
	}
	if _, present := ts.peek("orphan"); present {
		t.Fatal("an accepted reclaim must drop the entry")
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

// registerStandInPreVerify attaches a pre-verification pass to state that does
// not finish instantly once cancelled, standing in for a real pass unwinding
// its worker queue. A retire path that only cancelled without joining would
// return with exited still false.
func registerStandInPreVerify(state *serverTorrentState) *atomic.Bool {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var exited atomic.Bool

	state.mu.Lock()
	state.preVerifyCancel = cancel
	state.preVerifyDone = done
	state.mu.Unlock()

	go func() {
		defer close(done)
		<-ctx.Done()
		time.Sleep(50 * time.Millisecond)
		exited.Store(true)
	}()

	return &exited
}

// TestTorrentStore_DropRetiresPreVerify pins that every path which drops a
// torrent's entry also stops its pre-verification pass. Each caller either
// deletes the torrent's files (abort, orphan reclaim) or re-creates them from
// scratch (re-sync), so a pass left running reads files that are about to
// disappear - and holds NFS handles that turn the unlink into a silly-rename.
func TestTorrentStore_DropRetiresPreVerify(t *testing.T) {
	t.Parallel()

	drops := map[string]func(*torrentStore, string){
		"Remove": func(ts *torrentStore, hash string) {
			ts.Remove(hash)
		},
		"Drain": func(ts *torrentStore, _ string) {
			ts.Drain()
		},
		"BeginAbort": func(ts *torrentStore, hash string) {
			if state, existing := ts.BeginAbort(hash, make(chan struct{})); state == nil || existing != nil {
				t.Errorf("BeginAbort(%q) did not take the entry", hash)
			}
		},
		"BeginReclaim": func(ts *torrentStore, hash string) {
			ok := ts.BeginReclaim(hash, make(chan struct{}), func(*serverTorrentState) bool { return true })
			if !ok {
				t.Errorf("BeginReclaim(%q) refused the entry", hash)
			}
		},
	}

	for name, drop := range drops {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ts := newTestStore(t)
			state := commitTestTorrent(t, ts, "hash", filepath.Join(t.TempDir(), "file.bin"))
			exited := registerStandInPreVerify(state)

			drop(ts, "hash")

			if !exited.Load() {
				t.Error("entry dropped while its pre-verification pass was still running")
			}
			state.mu.Lock()
			cancelLeft, doneLeft := state.preVerifyCancel, state.preVerifyDone
			state.mu.Unlock()
			if cancelLeft != nil || doneLeft != nil {
				t.Error("pre-verification pass left registered on a dropped entry")
			}
		})
	}
}
