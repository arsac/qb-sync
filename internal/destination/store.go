package destination

import (
	"log/slog"
	"sync"
)

// TorrentStore owns all shared mutable state for the destination server:
// torrent tracking, file path ownership, inode registry, and abort coordination.
//
// Lock ordering: store.mu -> state.mu -> InodeRegistry internal locks.
// Callers never acquire store.mu directly.
type torrentStore struct {
	mu        sync.RWMutex
	entries   map[string]*serverTorrentState
	filePaths map[string]string        // target path -> owning hash
	aborting  map[string]chan struct{} // hash -> completion signal
	inodes    *InodeRegistry           // owned, not shared
	basePath  string
	logger    *slog.Logger
}

// NewTorrentStore creates a new store. Call Load to restore persisted inode state.
func newTorrentStore(basePath string, logger *slog.Logger) *torrentStore {
	return &torrentStore{
		entries:   make(map[string]*serverTorrentState),
		filePaths: make(map[string]string),
		aborting:  make(map[string]chan struct{}),
		inodes:    NewInodeRegistry(basePath, logger),
		basePath:  basePath,
		logger:    logger,
	}
}

// Get returns a torrent's state. The pointer is safe to use outside the store
// lock — callers use state.mu for mutable fields.
func (ts *torrentStore) Get(hash string) (*serverTorrentState, bool) {
	ts.mu.RLock()
	state, ok := ts.entries[hash]
	ts.mu.RUnlock()
	return state, ok
}

// ForEach snapshots entries under RLock, then iterates outside the lock.
// The callback receives each hash and state; return false to stop early.
func (ts *torrentStore) ForEach(fn func(hash string, state *serverTorrentState) bool) {
	ts.mu.RLock()
	refs := make([]torrentRef, 0, len(ts.entries))
	for hash, state := range ts.entries {
		refs = append(refs, torrentRef{hash: hash, state: state})
	}
	ts.mu.RUnlock()

	for _, ref := range refs {
		if !fn(ref.hash, ref.state) {
			break
		}
	}
}

// Len returns the number of tracked torrents.
func (ts *torrentStore) Len() int {
	ts.mu.RLock()
	n := len(ts.entries)
	ts.mu.RUnlock()
	return n
}
