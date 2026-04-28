package destination

import (
	"strings"
	"testing"
)

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

	// Reserve succeeds for a new hash.
	if err := ts.Reserve("abc"); err != nil {
		t.Fatalf("Reserve: unexpected error: %v", err)
	}

	// Duplicate Reserve while sentinel is present returns error.
	if err := ts.Reserve("abc"); err == nil {
		t.Fatal("Reserve duplicate: expected error, got nil")
	}

	// Commit replaces the sentinel with real state.
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

	// State is accessible via Get and is no longer a sentinel.
	got, ok := ts.Get("abc")
	if !ok || got == nil {
		t.Fatal("Get after Commit: expected state")
	}
	if got.initializing {
		t.Fatal("Get after Commit: state still marked initializing")
	}

	// File path was registered.
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

	// After Unreserve the hash is free; Reserve must succeed again.
	if err := ts.Reserve("xyz"); err != nil {
		t.Fatalf("Reserve after Unreserve: unexpected error: %v", err)
	}
}

func TestTorrentStore_CommitCollision(t *testing.T) {
	t.Parallel()
	ts := newTestStore(t)

	// Commit the first torrent so it owns "shared/file.dat".
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

	// Reserve the second torrent and try to Commit with the same path — collision.
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

	// Sentinel for torrent2 must have been cleaned up (not present in entries).
	_, ok := ts.Get("torrent2")
	if ok {
		t.Fatal("torrent2 sentinel should have been removed after collision")
	}
}
