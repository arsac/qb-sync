package destination

import (
	"fmt"
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

// Reserve inserts a sentinel to prevent concurrent initialization.
// Returns error if hash is already tracked or initializing.
func (ts *torrentStore) Reserve(hash string) error {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if state, exists := ts.entries[hash]; exists {
		if state.initializing {
			return fmt.Errorf("torrent %s initialization already in progress", hash)
		}
		return fmt.Errorf("torrent %s already tracked", hash)
	}
	ts.entries[hash] = &serverTorrentState{initializing: true}
	return nil
}

// Commit replaces the sentinel with real state. Checks file path collisions
// and registers paths atomically. On collision, removes the sentinel.
func (ts *torrentStore) Commit(hash string, state *serverTorrentState) error {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if err := checkPathCollisions(ts.filePaths, hash, state.files); err != nil {
		delete(ts.entries, hash)
		return err
	}
	ts.entries[hash] = state
	registerFilePaths(ts.filePaths, hash, state.files)
	return nil
}

// Unreserve removes the sentinel on initialization failure.
func (ts *torrentStore) Unreserve(hash string) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if state, exists := ts.entries[hash]; exists && state.initializing {
		delete(ts.entries, hash)
	}
}
