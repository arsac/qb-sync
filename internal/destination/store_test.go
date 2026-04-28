package destination

import (
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
