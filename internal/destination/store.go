package destination

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
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

// newTorrentStore creates a new store.
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

// Get returns a torrent's state, safe to use outside the lock, and stamps it
// as contacted.
// Sentinel entries (initializing=true) are filtered out — callers see (nil, false).
// Use GetWithSentinel when the distinction between "not found" and "initializing" matters.
//
// The contact stamp lives here rather than at each RPC entry point on purpose:
// every request path resolves state through these accessors, so a new RPC gets
// it for free. Forgetting to stamp would make a live transfer look like an orphan
// and have its data reclaimed, so a safe default matters more than the
// accessor being free of side effects. Scanning callers that must NOT refresh
// the stamp use peek.
func (ts *torrentStore) Get(hash string) (*serverTorrentState, bool) {
	state, ok := ts.peek(hash)
	if !ok || state.initializing.Load() {
		return nil, false
	}
	state.touch()
	return state, true
}

// GetWithSentinel returns a torrent's state including sentinel entries, and
// stamps it as contacted.
// Only use when distinguishing "not found" from "initializing" is required (e.g., InitTorrent).
func (ts *torrentStore) GetWithSentinel(hash string) (*serverTorrentState, bool) {
	state, ok := ts.peek(hash)
	if ok {
		state.touch()
	}
	return state, ok
}

// peek returns a torrent's state without stamping contact. Used by orphan
// scanning, which would otherwise refresh the very timestamp it is testing.
func (ts *torrentStore) peek(hash string) (*serverTorrentState, bool) {
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
		if state.initializing.Load() {
			return fmt.Errorf("torrent %s initialization already in progress", hash)
		}
		return fmt.Errorf("torrent %s already tracked", hash)
	}
	sentinel := &serverTorrentState{}
	sentinel.initializing.Store(true)
	ts.entries[hash] = sentinel
	return nil
}

// abortInodesForFiles aborts in-progress inode registrations for every file
// in the slice. Called outside the store lock to match the documented lock
// ordering (store.mu -> InodeRegistry locks).
func (ts *torrentStore) abortInodesForFiles(hash string, files []*serverFileInfo) {
	for _, fi := range files {
		ts.inodes.AbortInProgress(context.Background(), fi.hardlink.sourceFileID, hash)
	}
}

// Commit replaces the sentinel with real state. Checks file path collisions
// and registers paths atomically. On collision, removes the sentinel and
// aborts any in-progress inode registrations from setupFiles.
func (ts *torrentStore) Commit(hash string, state *serverTorrentState) error {
	ts.mu.Lock()
	if err := checkPathCollisions(ts.filePaths, hash, state.files); err != nil {
		delete(ts.entries, hash)
		ts.mu.Unlock()
		// Clean up in-progress inode registrations made during setupFiles
		// so pending waiters aren't stuck on a doneCh for a dead torrent.
		ts.abortInodesForFiles(hash, state.files)
		return err
	}
	// Stamp at creation, not just on later access. Both creating paths reach
	// here, and an unstamped entry measures its age from process start: on a
	// server whose uptime already exceeds the orphan timeout, a torrent that
	// InitTorrent has only just committed would read as abandoned and have its
	// .partial files reclaimed before the first WritePiece stamped it.
	state.touch()
	ts.entries[hash] = state
	registerFilePaths(ts.filePaths, hash, state.files)
	ts.mu.Unlock()
	return nil
}

// Unreserve removes the sentinel on initialization failure.
func (ts *torrentStore) Unreserve(hash string) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if state, exists := ts.entries[hash]; exists && state.initializing.Load() {
		delete(ts.entries, hash)
	}
}

// dropEntryLocked removes a torrent's entry and unregisters its file paths.
// Caller must hold ts.mu. Returns the removed state so the caller can abort its
// in-progress inodes AFTER unlocking, which the documented lock ordering
// (store.mu -> InodeRegistry locks) requires. Spelled once because all three
// removal paths must agree: a miss leaks filePaths entries or strands
// in-progress inode registrations on a doneCh nobody will close.
func (ts *torrentStore) dropEntryLocked(hash string) (*serverTorrentState, bool) {
	state, exists := ts.entries[hash]
	if !exists {
		return nil, false
	}
	unregisterFilePaths(ts.filePaths, hash, state.files)
	delete(ts.entries, hash)
	return state, true
}

// Remove deletes a torrent, unregisters file paths, and aborts any in-progress
// inodes for the hash. Returns the old state for caller cleanup. No-op if not found.
func (ts *torrentStore) Remove(hash string) *serverTorrentState {
	ts.mu.Lock()
	state, exists := ts.dropEntryLocked(hash)
	ts.mu.Unlock()
	if exists {
		ts.abortInodesForFiles(hash, state.files)
	}
	return state
}

// Drain removes all entries and returns them. Used for shutdown.
// Aborts in-progress inodes for all drained entries, matching Remove behavior.
func (ts *torrentStore) Drain() map[string]*serverTorrentState {
	ts.mu.Lock()
	old := ts.entries
	ts.entries = make(map[string]*serverTorrentState)
	ts.filePaths = make(map[string]string)
	ts.mu.Unlock()

	for hash, state := range old {
		ts.abortInodesForFiles(hash, state.files)
	}

	return old
}

// BeginAbort atomically removes the torrent from the store and registers a
// cleanup channel. If an abort is already in progress, returns (nil, existingCh).
// Caller closes ch after cleanup, then calls EndCleanup.
func (ts *torrentStore) BeginAbort(hash string, ch chan struct{}) (*serverTorrentState, chan struct{}) {
	ts.mu.Lock()
	if existing, alreadyAborting := ts.aborting[hash]; alreadyAborting {
		ts.mu.Unlock()
		return nil, existing
	}
	ts.aborting[hash] = ch
	state, exists := ts.dropEntryLocked(hash)
	ts.mu.Unlock()

	// Abort in-progress inodes outside the lock (matching Remove behavior).
	if exists {
		ts.abortInodesForFiles(hash, state.files)
	}

	return state, nil
}

// BeginReclaim registers a cleanup channel for orphan reclamation, dropping the
// torrent's in-memory entry when it has one.
//
// Orphan-ness no longer depends on store membership, so a reclaimable torrent
// may or may not hold state. Both cases register the same channel, which is
// what blocks a concurrent InitTorrent from recreating files about to be
// deleted.
//
// pred runs while the store lock is held. That is the point: a source can
// resume a torrent between the scan judging it stale and this call, and testing
// staleness outside the lock would delete files from under an active transfer.
// A torrent with no entry has nothing to re-test, so pred is not consulted.
//
// Returns false when another cleanup or abort is already registered, or when
// pred rejects the entry.
func (ts *torrentStore) BeginReclaim(
	hash string,
	ch chan struct{},
	pred func(*serverTorrentState) bool,
) bool {
	ts.mu.Lock()
	if _, busy := ts.aborting[hash]; busy {
		ts.mu.Unlock()
		return false
	}

	state, exists := ts.entries[hash]
	if exists {
		if !pred(state) {
			ts.mu.Unlock()
			return false
		}
		ts.dropEntryLocked(hash)
	}
	ts.aborting[hash] = ch
	ts.mu.Unlock()

	if exists {
		ts.abortInodesForFiles(hash, state.files)
	}
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
		if fi.skipForWriteData() || fi.hardlink.sourceFileID.IsZero() {
			continue
		}

		finalPath := targetPath(fi)
		relPath, relErr := filepath.Rel(ts.basePath, finalPath)
		if relErr != nil {
			ts.logger.WarnContext(ctx, "failed to get relative path for inode registration",
				"hash", hash, "path", finalPath, "error", relErr)
			continue
		}

		ts.inodes.Register(fi.hardlink.sourceFileID, relPath)
		ts.inodes.CompleteInProgress(fi.hardlink.sourceFileID, hash)
		registered++
	}

	if registered > 0 {
		ts.logger.InfoContext(ctx, "registered finalized inodes",
			"hash", hash,
			"count", registered,
		)
	}
}
