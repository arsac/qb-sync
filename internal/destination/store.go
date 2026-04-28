package destination

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
)

// torrentStore owns all shared mutable state for the destination server:
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

// Get returns a torrent's state, safe to use outside the lock.
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

// Remove deletes a torrent, unregisters file paths, and aborts any in-progress
// inodes for the hash. Returns the old state for caller cleanup. No-op if not found.
func (ts *torrentStore) Remove(hash string) *serverTorrentState {
	ts.mu.Lock()
	state, exists := ts.entries[hash]
	if exists {
		unregisterFilePaths(ts.filePaths, hash, state.files)
		delete(ts.entries, hash)
	}
	ts.mu.Unlock()
	if exists {
		for _, fi := range state.files {
			ts.inodes.AbortInProgress(context.Background(), fi.hardlink.sourceInode, hash)
		}
	}
	return state
}

// Drain removes all entries and returns them. Used for shutdown.
func (ts *torrentStore) Drain() map[string]*serverTorrentState {
	ts.mu.Lock()
	old := ts.entries
	ts.entries = make(map[string]*serverTorrentState)
	ts.filePaths = make(map[string]string)
	ts.mu.Unlock()
	return old
}

// BeginAbort atomically removes the torrent from the store and registers a
// cleanup channel. If an abort is already in progress, returns (nil, existingCh).
// Caller closes ch after cleanup, then calls EndCleanup.
func (ts *torrentStore) BeginAbort(hash string, ch chan struct{}) (*serverTorrentState, chan struct{}) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if existing, alreadyAborting := ts.aborting[hash]; alreadyAborting {
		return nil, existing
	}
	ts.aborting[hash] = ch
	state, exists := ts.entries[hash]
	if exists {
		unregisterFilePaths(ts.filePaths, hash, state.files)
		delete(ts.entries, hash)
	}
	return state, nil
}

// BeginCleanup registers a cleanup channel for an untracked hash (orphan cleanup).
// Returns false if the hash is tracked or already being cleaned.
func (ts *torrentStore) BeginCleanup(hash string, ch chan struct{}) bool {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if _, tracked := ts.entries[hash]; tracked {
		return false
	}
	if _, cleaning := ts.aborting[hash]; cleaning {
		return false
	}
	ts.aborting[hash] = ch
	return true
}

// EndCleanup deregisters the cleanup/abort channel.
func (ts *torrentStore) EndCleanup(hash string) {
	ts.mu.Lock()
	delete(ts.aborting, hash)
	ts.mu.Unlock()
}

// AbortCh returns the abort/cleanup channel if one is registered.
func (ts *torrentStore) AbortCh(hash string) (chan struct{}, bool) {
	ts.mu.RLock()
	ch, ok := ts.aborting[hash]
	ts.mu.RUnlock()
	return ch, ok
}

// Inodes returns the underlying InodeRegistry for fine-grained lookups.
func (ts *torrentStore) Inodes() *InodeRegistry {
	return ts.inodes
}

// RegisterInodes registers finalized file inodes in the persistent registry
// and signals any in-progress waiters.
func (ts *torrentStore) RegisterInodes(ctx context.Context, hash string, files []*serverFileInfo) {
	var registered int
	for _, fi := range files {
		if fi.skipForWriteData() || fi.hardlink.sourceInode == 0 {
			continue
		}

		finalPath, _ := strings.CutSuffix(fi.path, partialSuffix)
		relPath, relErr := filepath.Rel(ts.basePath, finalPath)
		if relErr != nil {
			ts.logger.WarnContext(ctx, "failed to get relative path for inode registration",
				"hash", hash, "path", finalPath, "error", relErr)
			continue
		}

		ts.inodes.Register(fi.hardlink.sourceInode, relPath)
		ts.inodes.CompleteInProgress(fi.hardlink.sourceInode, hash)
		registered++
	}

	if registered > 0 {
		ts.logger.InfoContext(ctx, "registered finalized inodes",
			"hash", hash,
			"count", registered,
		)
		if saveErr := ts.inodes.Save(); saveErr != nil {
			ts.logger.WarnContext(ctx, "failed to save inode map", "error", saveErr)
		}
	}
}
