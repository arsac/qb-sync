# TorrentStore Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Centralize destination server state management behind a single `TorrentStore` type, eliminating 17 scattered map-access sites across 6 files, and add startup recovery from persisted metadata.

**Architecture:** Extract `s.torrents`, `s.filePaths`, `s.abortingHashes`, and `s.inodes` from `Server` into a composed `TorrentStore` with a small method API. All locking, file-path registration, and inode coordination moves inside the store. `initNewTorrent` becomes idempotent so startup recovery can feed synthetic requests through the same code path. The `Server` keeps business logic (disk I/O, piece writing, finalization).

**Tech Stack:** Go 1.25, bitset, protobuf, gRPC

**Spec:** `docs/superpowers/specs/2026-04-27-torrent-store-design.md`

---

## File Structure

| File | Action | Responsibility |
|------|--------|---------------|
| `internal/destination/store.go` | Create | `TorrentStore` type, constructor, all methods (Get, Reserve, Commit, Unreserve, Remove, Drain, ForEach, Len, BeginAbort, BeginCleanup, EndCleanup, AbortCh, RegisterInodes, Inodes, SaveInodes) |
| `internal/destination/store_test.go` | Create | Unit tests for TorrentStore methods |
| `internal/destination/file_paths.go` | Modify | Move `checkPathCollisions`, `registerFilePaths`, `unregisterFilePaths` to accept map parameter (store-internal). Keep `targetPath` exported. |
| `internal/destination/types.go` | Modify | Remove `info *pb.InitTorrentRequest` from `serverTorrentState` |
| `internal/destination/server.go` | Modify | Replace `torrents`, `filePaths`, `abortingHashes`, `inodes`, `mu` with single `store *TorrentStore`. Update `NewServer`, `cleanup`, remove `collectTorrents`. |
| `internal/destination/init.go` | Modify | Replace direct map/lock access with store methods. Make `setupMetadataDir` idempotent. |
| `internal/destination/write.go` | Modify | Replace `s.mu.RLock` + `s.torrents` with `s.store.Get` |
| `internal/destination/finalize.go` | Modify | Replace `getState`, `cleanupFinalizedTorrent`, `registerFinalizedInodes` with store calls |
| `internal/destination/abort.go` | Modify | Replace abort channel + map management with `s.store.BeginAbort`/`EndCleanup` |
| `internal/destination/lifecycle.go` | Modify | Replace `collectTorrents`, `isOrphanedTorrent`, `cleanupOrphan` map access with store methods |
| `internal/destination/recovery.go` | Create | `recoverInFlightTorrents` — startup scan, synthetic request construction |
| `internal/destination/recovery_test.go` | Create | Recovery tests — valid, finalized, stale, missing metadata dirs |
| `internal/destination/helpers_test.go` | Modify | Update `newTestDestServer` to use `TorrentStore` |
| `internal/destination/server_test.go` | Modify | Replace direct `s.torrents`/`s.mu` access with store methods |
| `internal/destination/bugfix_test.go` | Modify | Replace direct `s.filePaths`/`s.mu` access with store methods |

---

### Task 1: Create `TorrentStore` with read operations

**Files:**
- Create: `internal/destination/store.go`
- Create: `internal/destination/store_test.go`

- [ ] **Step 1: Write failing tests for Get, Len, ForEach**

```go
// internal/destination/store_test.go
package destination

import (
	"testing"
)

func newTestStore(t *testing.T) *TorrentStore {
	t.Helper()
	logger := slogDiscard()
	return NewTorrentStore(t.TempDir(), logger)
}

func TestTorrentStore_Get(t *testing.T) {
	t.Parallel()
	ts := newTestStore(t)

	// Unknown hash returns nil.
	_, ok := ts.Get("unknown")
	if ok {
		t.Fatal("expected not found for unknown hash")
	}

	// Manually insert for read test (will use Reserve/Commit once available).
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `GOROOT=/Users/mailoarsac/.local/share/mise/installs/go/1.25.7 go test ./internal/destination/... -run "TestTorrentStore_Get|TestTorrentStore_Len|TestTorrentStore_ForEach" -count=1`

Expected: compilation error — `TorrentStore` not defined.

- [ ] **Step 3: Implement TorrentStore struct and read methods**

```go
// internal/destination/store.go
package destination

import (
	"context"
	"log/slog"
	"sync"
)

// TorrentStore owns all shared mutable state for the destination server:
// torrent tracking, file path ownership, inode registry, and abort coordination.
//
// Lock ordering: store.mu -> state.mu -> InodeRegistry internal locks.
// Callers never acquire store.mu directly.
type TorrentStore struct {
	mu        sync.RWMutex
	entries   map[string]*serverTorrentState
	filePaths map[string]string       // target path -> owning hash
	aborting  map[string]chan struct{} // hash -> completion signal
	inodes    *InodeRegistry          // owned, not shared
	basePath  string
	logger    *slog.Logger
}

// NewTorrentStore creates a new store. Call Load to restore persisted inode state.
func NewTorrentStore(basePath string, logger *slog.Logger) *TorrentStore {
	return &TorrentStore{
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
func (ts *TorrentStore) Get(hash string) (*serverTorrentState, bool) {
	ts.mu.RLock()
	state, ok := ts.entries[hash]
	ts.mu.RUnlock()
	return state, ok
}

// ForEach snapshots entries under RLock, then iterates outside the lock.
// The callback receives each hash and state; return false to stop early.
func (ts *TorrentStore) ForEach(fn func(hash string, state *serverTorrentState) bool) {
	ts.mu.RLock()
	refs := make([]struct {
		hash  string
		state *serverTorrentState
	}, 0, len(ts.entries))
	for hash, state := range ts.entries {
		refs = append(refs, struct {
			hash  string
			state *serverTorrentState
		}{hash, state})
	}
	ts.mu.RUnlock()

	for _, ref := range refs {
		if !fn(ref.hash, ref.state) {
			break
		}
	}
}

// Len returns the number of tracked torrents.
func (ts *TorrentStore) Len() int {
	ts.mu.RLock()
	n := len(ts.entries)
	ts.mu.RUnlock()
	return n
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `GOROOT=/Users/mailoarsac/.local/share/mise/installs/go/1.25.7 go test ./internal/destination/... -run "TestTorrentStore_Get|TestTorrentStore_Len|TestTorrentStore_ForEach" -count=1 -v`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/destination/store.go internal/destination/store_test.go
git commit -m "feat(destination): add TorrentStore with read operations (Get, ForEach, Len)"
```

---

### Task 2: Add two-phase creation (Reserve/Commit/Unreserve)

**Files:**
- Modify: `internal/destination/store.go`
- Modify: `internal/destination/store_test.go`

- [ ] **Step 1: Write failing tests for Reserve, Commit, Unreserve**

```go
// Add to internal/destination/store_test.go

func TestTorrentStore_ReserveCommit(t *testing.T) {
	t.Parallel()
	ts := newTestStore(t)

	// Reserve succeeds for new hash.
	if err := ts.Reserve("h1"); err != nil {
		t.Fatalf("Reserve failed: %v", err)
	}

	// Reserve again fails (already initializing).
	if err := ts.Reserve("h1"); err == nil {
		t.Fatal("expected error for duplicate Reserve")
	}

	// Commit replaces sentinel.
	state := &serverTorrentState{
		torrentMeta: torrentMeta{
			files: []*serverFileInfo{
				{path: "/data/movies/file.mkv", selected: true},
			},
		},
	}
	if err := ts.Commit("h1", state); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// State is accessible.
	got, ok := ts.Get("h1")
	if !ok || got != state {
		t.Fatal("expected committed state")
	}

	// File path registered.
	ts.mu.RLock()
	owner := ts.filePaths["/data/movies/file.mkv"]
	ts.mu.RUnlock()
	if owner != "h1" {
		t.Fatalf("expected file path owner h1, got %q", owner)
	}
}

func TestTorrentStore_Unreserve(t *testing.T) {
	t.Parallel()
	ts := newTestStore(t)

	if err := ts.Reserve("h1"); err != nil {
		t.Fatalf("Reserve failed: %v", err)
	}

	ts.Unreserve("h1")

	// Hash is free again.
	if err := ts.Reserve("h1"); err != nil {
		t.Fatalf("Reserve after Unreserve failed: %v", err)
	}
}

func TestTorrentStore_CommitCollision(t *testing.T) {
	t.Parallel()
	ts := newTestStore(t)

	// Set up existing torrent owning a path.
	if err := ts.Reserve("h1"); err != nil {
		t.Fatal(err)
	}
	state1 := &serverTorrentState{
		torrentMeta: torrentMeta{
			files: []*serverFileInfo{
				{path: "/data/movies/file.mkv", selected: true},
			},
		},
	}
	if err := ts.Commit("h1", state1); err != nil {
		t.Fatal(err)
	}

	// Second torrent tries to claim same path.
	if err := ts.Reserve("h2"); err != nil {
		t.Fatal(err)
	}
	state2 := &serverTorrentState{
		torrentMeta: torrentMeta{
			files: []*serverFileInfo{
				{path: "/data/movies/file.mkv", selected: true},
			},
		},
	}
	err := ts.Commit("h2", state2)
	if err == nil {
		t.Fatal("expected collision error")
	}

	// Sentinel should be cleaned up after collision.
	_, ok := ts.Get("h2")
	if ok {
		t.Fatal("sentinel should be removed after collision")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `GOROOT=/Users/mailoarsac/.local/share/mise/installs/go/1.25.7 go test ./internal/destination/... -run "TestTorrentStore_Reserve|TestTorrentStore_Unreserve|TestTorrentStore_CommitCollision" -count=1`

Expected: compilation error — `Reserve`, `Commit`, `Unreserve` not defined.

- [ ] **Step 3: Implement Reserve, Commit, Unreserve**

```go
// Add to internal/destination/store.go

// Reserve inserts a sentinel to prevent concurrent initialization.
// Returns error if hash is already tracked or initializing.
func (ts *TorrentStore) Reserve(hash string) error {
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
func (ts *TorrentStore) Commit(hash string, state *serverTorrentState) error {
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
func (ts *TorrentStore) Unreserve(hash string) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	if state, exists := ts.entries[hash]; exists && state.initializing {
		delete(ts.entries, hash)
	}
}
```

Add `"fmt"` to the imports in `store.go`.

- [ ] **Step 4: Update `file_paths.go` to accept map parameter**

Change the three functions from `Server` methods to standalone functions accepting the map:

```go
// internal/destination/file_paths.go

// checkPathCollisions checks if any selected file's target path is already
// owned by another active torrent.
func checkPathCollisions(filePaths map[string]string, hash string, files []*serverFileInfo) error {
	for _, fi := range files {
		if !fi.selected {
			continue
		}
		tp := targetPath(fi)
		if owner, exists := filePaths[tp]; exists && owner != hash {
			return fmt.Errorf("file path %s already owned by torrent %s", tp, owner)
		}
	}
	return nil
}

// registerFilePaths records ownership of all selected file paths for a torrent.
func registerFilePaths(filePaths map[string]string, hash string, files []*serverFileInfo) {
	for _, fi := range files {
		if !fi.selected {
			continue
		}
		filePaths[targetPath(fi)] = hash
	}
}

// unregisterFilePaths removes ownership records for a torrent's file paths.
func unregisterFilePaths(filePaths map[string]string, hash string, files []*serverFileInfo) {
	for _, fi := range files {
		tp := targetPath(fi)
		if owner, exists := filePaths[tp]; exists && owner == hash {
			delete(filePaths, tp)
		}
	}
}
```

Remove the `nil` guards (the store always initializes the map). Remove the `(s *Server)` receiver. The `fmt` import and `strings` import remain.

- [ ] **Step 5: Run tests to verify they pass**

Run: `GOROOT=/Users/mailoarsac/.local/share/mise/installs/go/1.25.7 go test ./internal/destination/... -run "TestTorrentStore" -count=1 -v`

Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add internal/destination/store.go internal/destination/store_test.go internal/destination/file_paths.go
git commit -m "feat(destination): add TorrentStore Reserve/Commit/Unreserve with path collision checks"
```

---

### Task 3: Add Remove, Drain, and abort coordination

**Files:**
- Modify: `internal/destination/store.go`
- Modify: `internal/destination/store_test.go`

- [ ] **Step 1: Write failing tests for Remove, Drain, BeginAbort, BeginCleanup, EndCleanup, AbortCh**

```go
// Add to internal/destination/store_test.go

func TestTorrentStore_Remove(t *testing.T) {
	t.Parallel()
	ts := newTestStore(t)

	// Insert a torrent.
	_ = ts.Reserve("h1")
	state := &serverTorrentState{
		torrentMeta: torrentMeta{
			files: []*serverFileInfo{
				{path: "/data/file.mkv", selected: true},
			},
		},
	}
	_ = ts.Commit("h1", state)

	// Remove returns old state.
	old := ts.Remove("h1")
	if old != state {
		t.Fatal("expected removed state")
	}

	// No longer tracked.
	if _, ok := ts.Get("h1"); ok {
		t.Fatal("expected hash removed")
	}

	// File path unregistered.
	ts.mu.RLock()
	_, owned := ts.filePaths["/data/file.mkv"]
	ts.mu.RUnlock()
	if owned {
		t.Fatal("expected file path unregistered")
	}

	// Remove of unknown hash returns nil.
	if ts.Remove("unknown") != nil {
		t.Fatal("expected nil for unknown hash")
	}
}

func TestTorrentStore_Drain(t *testing.T) {
	t.Parallel()
	ts := newTestStore(t)

	_ = ts.Reserve("a")
	_ = ts.Commit("a", &serverTorrentState{})
	_ = ts.Reserve("b")
	_ = ts.Commit("b", &serverTorrentState{})

	drained := ts.Drain()
	if len(drained) != 2 {
		t.Fatalf("expected 2 drained, got %d", len(drained))
	}
	if ts.Len() != 0 {
		t.Fatal("expected empty store after drain")
	}
}

func TestTorrentStore_BeginAbort(t *testing.T) {
	t.Parallel()
	ts := newTestStore(t)

	// Insert torrent.
	_ = ts.Reserve("h1")
	state := &serverTorrentState{}
	_ = ts.Commit("h1", state)

	// BeginAbort removes state and registers channel.
	ch := make(chan struct{})
	got, existingCh := ts.BeginAbort("h1", ch)
	if got != state {
		t.Fatal("expected removed state")
	}
	if existingCh != nil {
		t.Fatal("expected no existing abort")
	}

	// Hash no longer tracked.
	if _, ok := ts.Get("h1"); ok {
		t.Fatal("expected hash removed after abort")
	}

	// Second abort returns existing channel.
	ch2 := make(chan struct{})
	_, existingCh = ts.BeginAbort("h1", ch2)
	if existingCh != ch {
		t.Fatal("expected existing abort channel")
	}

	// EndCleanup deregisters.
	ts.EndCleanup("h1")
	_, ok := ts.AbortCh("h1")
	if ok {
		t.Fatal("expected abort channel cleared")
	}
}

func TestTorrentStore_BeginCleanup(t *testing.T) {
	t.Parallel()
	ts := newTestStore(t)

	// BeginCleanup succeeds for untracked hash.
	ch := make(chan struct{})
	if !ts.BeginCleanup("orphan", ch) {
		t.Fatal("expected cleanup registered")
	}

	// Returns false if already cleaning.
	ch2 := make(chan struct{})
	if ts.BeginCleanup("orphan", ch2) {
		t.Fatal("expected false for duplicate cleanup")
	}

	// Returns false if hash is tracked.
	_ = ts.Reserve("tracked")
	_ = ts.Commit("tracked", &serverTorrentState{})
	ch3 := make(chan struct{})
	if ts.BeginCleanup("tracked", ch3) {
		t.Fatal("expected false for tracked hash")
	}

	ts.EndCleanup("orphan")
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `GOROOT=/Users/mailoarsac/.local/share/mise/installs/go/1.25.7 go test ./internal/destination/... -run "TestTorrentStore_Remove|TestTorrentStore_Drain|TestTorrentStore_BeginAbort|TestTorrentStore_BeginCleanup" -count=1`

Expected: compilation error — `Remove`, `Drain`, `BeginAbort`, `BeginCleanup`, `EndCleanup`, `AbortCh` not defined.

- [ ] **Step 3: Implement Remove, Drain, and abort coordination**

```go
// Add to internal/destination/store.go

// Remove deletes a torrent, unregisters file paths, and aborts any in-progress
// inodes for the hash. Returns the old state for caller cleanup. No-op if not found.
func (ts *TorrentStore) Remove(hash string) *serverTorrentState {
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
func (ts *TorrentStore) Drain() map[string]*serverTorrentState {
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
func (ts *TorrentStore) BeginAbort(hash string, ch chan struct{}) (
	state *serverTorrentState,
	existingCh chan struct{},
) {
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
func (ts *TorrentStore) BeginCleanup(hash string, ch chan struct{}) bool {
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
func (ts *TorrentStore) EndCleanup(hash string) {
	ts.mu.Lock()
	delete(ts.aborting, hash)
	ts.mu.Unlock()
}

// AbortCh returns the abort/cleanup channel if one is registered.
func (ts *TorrentStore) AbortCh(hash string) (chan struct{}, bool) {
	ts.mu.RLock()
	ch, ok := ts.aborting[hash]
	ts.mu.RUnlock()
	return ch, ok
}
```

Add `"context"` to imports.

- [ ] **Step 4: Run tests to verify they pass**

Run: `GOROOT=/Users/mailoarsac/.local/share/mise/installs/go/1.25.7 go test ./internal/destination/... -run "TestTorrentStore" -count=1 -v`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/destination/store.go internal/destination/store_test.go
git commit -m "feat(destination): add TorrentStore Remove, Drain, and abort coordination"
```

---

### Task 4: Add inode delegation methods

**Files:**
- Modify: `internal/destination/store.go`
- Modify: `internal/destination/store_test.go`

- [ ] **Step 1: Write failing test for RegisterInodes, Inodes, SaveInodes**

```go
// Add to internal/destination/store_test.go

func TestTorrentStore_InodeDelegation(t *testing.T) {
	t.Parallel()
	ts := newTestStore(t)

	// Inodes returns the underlying registry.
	if ts.Inodes() == nil {
		t.Fatal("expected non-nil InodeRegistry")
	}

	// RegisterInodes registers and signals.
	inode := Inode(12345)
	basePath := ts.basePath
	ts.Inodes().RegisterInProgress(inode, "h1", "movies/file.mkv")

	files := []*serverFileInfo{
		{
			path:     filepath.Join(basePath, "movies", "file.mkv"),
			size:     1024,
			selected: true,
			hardlink: hardlinkInfo{
				sourceInode: inode,
				state:       hlStateInProgress,
			},
		},
	}
	ts.RegisterInodes(context.Background(), "h1", files)

	// Inode should be registered now.
	regPath, found := ts.Inodes().GetRegistered(inode)
	if !found {
		t.Fatal("expected inode registered after RegisterInodes")
	}
	wantRelPath := filepath.Join("movies", "file.mkv")
	if regPath != wantRelPath {
		t.Fatalf("expected registered path %q, got %q", wantRelPath, regPath)
	}

	// In-progress should be cleared.
	_, _, _, inProg := ts.Inodes().GetInProgress(inode)
	if inProg {
		t.Fatal("expected in-progress cleared after RegisterInodes")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `GOROOT=/Users/mailoarsac/.local/share/mise/installs/go/1.25.7 go test ./internal/destination/... -run TestTorrentStore_InodeDelegation -count=1`

Expected: compilation error — `RegisterInodes`, `Inodes`, `SaveInodes` not defined.

- [ ] **Step 3: Implement inode delegation**

```go
// Add to internal/destination/store.go

// Inodes returns the underlying InodeRegistry for fine-grained inode lookups
// during setupFiles. These use the registry's internal locks.
func (ts *TorrentStore) Inodes() *InodeRegistry {
	return ts.inodes
}

// RegisterInodes registers finalized file inodes in the persistent registry
// and signals any in-progress waiters. Mirrors the logic from registerFinalizedInodes.
func (ts *TorrentStore) RegisterInodes(ctx context.Context, hash string, files []*serverFileInfo) {
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
		if saveErr := ts.inodes.Save(); saveErr != nil {
			ts.logger.WarnContext(ctx, "failed to save inode map", "error", saveErr)
		}
	}
}

// SaveInodes persists the inode registry to disk.
func (ts *TorrentStore) SaveInodes() error {
	return ts.inodes.Save()
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `GOROOT=/Users/mailoarsac/.local/share/mise/installs/go/1.25.7 go test ./internal/destination/... -run TestTorrentStore_InodeDelegation -count=1 -v`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/destination/store.go internal/destination/store_test.go
git commit -m "feat(destination): add TorrentStore inode delegation (RegisterInodes, Inodes, SaveInodes)"
```

---

### Task 5: Wire TorrentStore into Server, remove dead `state.info`

This is the core migration task. Replace all direct map/lock access in Server with store method calls. This task changes many files but each change is mechanical — replacing one pattern with another.

**Files:**
- Modify: `internal/destination/types.go`
- Modify: `internal/destination/server.go`
- Modify: `internal/destination/init.go`
- Modify: `internal/destination/write.go`
- Modify: `internal/destination/finalize.go`
- Modify: `internal/destination/abort.go`
- Modify: `internal/destination/lifecycle.go`
- Modify: `internal/destination/helpers_test.go`

- [ ] **Step 1: Remove `info` field from `serverTorrentState`**

In `internal/destination/types.go`, remove line 127:
```go
info      *pb.InitTorrentRequest // Original init request
```

In `internal/destination/init.go`, remove `info: req,` from the state construction at line 238.

In `internal/destination/server_test.go`, remove any `info:` field assignments in test state construction (lines 2039, 2146).

- [ ] **Step 2: Replace Server fields with `store *TorrentStore`**

In `internal/destination/server.go`:

Replace fields `torrents`, `mu`, `abortingHashes`, `filePaths`, `inodes` with:
```go
store *TorrentStore
```

Update `NewServer` to construct the store:
```go
s := &Server{
    config:      config,
    logger:      logger,
    store:       NewTorrentStore(config.BasePath, logger),
    memBudget:   semaphore.NewWeighted(bufferBytes),
    finalizeSem: semaphore.NewWeighted(1),
    bgCtx:       bgCtx,
    bgCancel:    bgCancel,
}
```

Load inodes from store:
```go
if loadErr := s.store.Inodes().Load(); loadErr != nil {
    logger.Warn("failed to load inode map, starting fresh", "error", loadErr)
}
```

Remove `collectTorrents` method.

Replace `cleanup` method body to use `Drain`:
```go
func (s *Server) cleanup() {
    drained := s.store.Drain()
    for hash, state := range drained {
        state.mu.Lock()
        if state.dirty {
            if err := s.doSaveState(state.statePath, state.written); err != nil {
                s.logger.Warn("failed to save state during cleanup", "hash", hash, "error", err)
            }
        }
        state.mu.Unlock()
    }
    if err := s.store.SaveInodes(); err != nil {
        s.logger.Warn("failed to save inode map during cleanup", "error", err)
    }
}
```

- [ ] **Step 3: Update `init.go` to use store methods**

Replace `InitTorrent` lock/map access:
- `s.waitForAbortComplete`: replace `s.mu.RLock()` + `s.abortingHashes[hash]` with `s.store.AbortCh(hash)`
- Existing state check: replace `s.mu.Lock()` + `s.torrents[hash]` with `s.store.Get(hash)` + check `initializing`
- Sentinel insert: replace `s.torrents[hash] = sentinel` + `s.mu.Unlock()` with `s.store.Reserve(hash)`
- Error cleanup: replace `s.mu.Lock()` + `delete` with `s.store.Unreserve(hash)`
- Swap sentinel: replace `s.mu.Lock()` + `checkPathCollisions` + `registerFilePaths` + `s.torrents[hash] = state` with `s.store.Commit(hash, state)`

Replace `cleanupForResync`:
- Replace `s.mu.Lock()` + `unregisterFilePaths` + `delete` with `s.store.Remove(hash)`

Replace `s.inodes` references with `s.store.Inodes()`.

Replace `s.checkPathCollisions` and `s.registerFilePaths` calls — these now happen inside `Commit`.

- [ ] **Step 4: Update `write.go` to use store methods**

Replace `writePiece` state lookup:
```go
state, exists := s.store.Get(torrentHash)
if !exists {
    return &pb.PieceAck{...PIECE_ERROR_NOT_INITIALIZED...}
}
if state.initializing {
    return &pb.PieceAck{...PIECE_ERROR_NOT_INITIALIZED...}
}
```

Remove `s.mu.RLock/RUnlock`.

- [ ] **Step 5: Update `finalize.go` to use store methods**

Replace `getState`:
```go
func (s *Server) getState(hash string) (*serverTorrentState, error) {
    state, ok := s.store.Get(hash)
    if !ok {
        return nil, fmt.Errorf("torrent not found: %s", hash)
    }
    if state.initializing {
        return nil, fmt.Errorf("torrent %s still initializing", hash)
    }
    return state, nil
}
```

Replace `cleanupFinalizedTorrent`:
```go
func (s *Server) cleanupFinalizedTorrent(hash string) {
    s.store.Remove(hash)
}
```

Replace `registerFinalizedInodes` to use `s.store.RegisterInodes(ctx, hash, state.files)` and `s.store.SaveInodes()`.

- [ ] **Step 6: Update `abort.go` to use store methods**

Replace the abort channel + map management block with:
```go
abortCh := make(chan struct{})
defer func() {
    s.store.EndCleanup(hash)
    close(abortCh)
}()

state, existingCh := s.store.BeginAbort(hash, abortCh)
if existingCh != nil {
    <-existingCh
    return ...
}
```

Remove direct `s.mu`, `s.torrents`, `s.abortingHashes`, `s.unregisterFilePaths` access.

Keep `s.store.Inodes().AbortInProgress(...)` calls for per-file cleanup (these happen after state removal, using the returned state pointer).

- [ ] **Step 7: Update `lifecycle.go` to use store methods**

`flushDirtyStates`: Replace `s.mu.RLock()` + `s.collectTorrents()` with:
```go
metrics.ActiveTorrents.WithLabelValues(metrics.ModeDestination).Set(float64(s.store.Len()))
s.store.ForEach(func(hash string, state *serverTorrentState) bool {
    // ... existing per-torrent flush logic ...
    return true
})
```

`isOrphanedTorrent`: Replace `s.mu.RLock()` + `s.torrents[hash]` with:
```go
_, tracked := s.store.Get(hash)
```

`cleanupOrphan`: Replace direct map access with:
```go
cleanupCh := make(chan struct{})
if !s.store.BeginCleanup(hash, cleanupCh) {
    return // tracked or already cleaning
}
defer func() {
    s.store.EndCleanup(hash)
    close(cleanupCh)
}()
```

- [ ] **Step 8: Update `helpers_test.go`**

Replace test server construction to use `TorrentStore`:
```go
func newTestDestServer(t *testing.T) (*Server, string) {
    t.Helper()
    tmpDir := t.TempDir()
    logger := slogDiscard()
    s := &Server{
        config:      ServerConfig{BasePath: tmpDir},
        logger:      logger,
        store:       NewTorrentStore(tmpDir, logger),
        memBudget:   semaphore.NewWeighted(512 * 1024 * 1024),
        finalizeSem: semaphore.NewWeighted(1),
        bgCtx:       context.Background(),
        bgCancel:    func() {},
    }
    return s, tmpDir
}
```

- [ ] **Step 9: Run full unit test suite**

Run: `GOROOT=/Users/mailoarsac/.local/share/mise/installs/go/1.25.7 go test ./internal/destination/... -short -count=1`

Expected: PASS (some test files may need further updates — see Task 6)

- [ ] **Step 10: Commit**

```bash
git add internal/destination/
git commit -m "refactor(destination): wire TorrentStore into Server, remove dead state.info"
```

---

### Task 6: Update remaining test files for store API

Test files directly access `s.torrents`, `s.mu`, `s.filePaths`, `s.inodes`. These need to use the store API or access `s.store` fields for test setup.

**Files:**
- Modify: `internal/destination/server_test.go`
- Modify: `internal/destination/bugfix_test.go`

- [ ] **Step 1: Update `server_test.go`**

Replace all patterns:
- `s.mu.Lock(); s.torrents[hash] = state; s.mu.Unlock()` → `s.store.Reserve(hash); s.store.Commit(hash, state)` (or for tests that need to bypass collision checks, use `s.store.mu.Lock(); s.store.entries[hash] = state; s.store.mu.Unlock()`)
- `s.mu.RLock(); state := s.torrents[hash]; s.mu.RUnlock()` → `state, _ := s.store.Get(hash)`
- `s.mu.Lock(); s.abortingHashes[hash] = ch; s.mu.Unlock()` → use `s.store.BeginAbort` or direct `s.store.mu.Lock(); s.store.aborting[hash] = ch; s.store.mu.Unlock()` for test setup
- `s.inodes.Register(...)` → `s.store.Inodes().Register(...)`
- Remove `info: &pb.InitTorrentRequest{...}` from state construction

- [ ] **Step 2: Update `bugfix_test.go`**

Replace:
- `s.filePaths[path] = hash` → access via `s.store.mu.Lock(); s.store.filePaths[path] = hash; s.store.mu.Unlock()`
- `s.mu.RLock(); owner := s.filePaths[path]; s.mu.RUnlock()` → `s.store.mu.RLock(); owner := s.store.filePaths[path]; s.store.mu.RUnlock()`
- `s.inodes` → `s.store.Inodes()`

- [ ] **Step 3: Run full test suite**

Run: `GOROOT=/Users/mailoarsac/.local/share/mise/installs/go/1.25.7 go test ./internal/destination/... -short -count=1 -v`

Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add internal/destination/server_test.go internal/destination/bugfix_test.go
git commit -m "test(destination): update tests for TorrentStore API"
```

---

### Task 7: Make `setupMetadataDir` idempotent

**Files:**
- Modify: `internal/destination/init.go`

- [ ] **Step 1: Write test for idempotent setup**

```go
// Add to internal/destination/server_test.go

func TestSetupMetadataDir_Idempotent(t *testing.T) {
	t.Parallel()
	s, _ := newTestDestServer(t)

	torrentData := []byte("d4:infod4:name4:test12:piece lengthi16384e6:pieces20:" +
		strings.Repeat("A", 20) + "6:lengthi1024eee")

	// First call creates everything.
	metaDir1, torrentPath1, statePath1, err := s.setupMetadataDir("abc", "TestTorrent", torrentData)
	if err != nil {
		t.Fatalf("first call failed: %v", err)
	}

	// Verify files exist.
	if _, statErr := os.Stat(torrentPath1); statErr != nil {
		t.Fatalf("torrent file not created: %v", statErr)
	}

	// Second call with same inputs succeeds without error.
	metaDir2, torrentPath2, statePath2, err := s.setupMetadataDir("abc", "TestTorrent", torrentData)
	if err != nil {
		t.Fatalf("second call failed: %v", err)
	}

	if metaDir1 != metaDir2 || torrentPath1 != torrentPath2 || statePath1 != statePath2 {
		t.Fatal("paths should be identical on idempotent call")
	}

	// Third call with nil torrent data (recovery path) also succeeds.
	_, _, _, err = s.setupMetadataDir("abc", "TestTorrent", nil)
	if err != nil {
		t.Fatalf("nil torrent data call failed: %v", err)
	}
}
```

- [ ] **Step 2: Run test**

Run: `GOROOT=/Users/mailoarsac/.local/share/mise/installs/go/1.25.7 go test ./internal/destination/... -run TestSetupMetadataDir_Idempotent -count=1 -v`

Expected: PASS (current code already handles most of this — `MkdirAll` is idempotent, `len(torrentFile) > 0` guard exists). If it fails, fix in step 3.

- [ ] **Step 3: Add idempotency guard for `.torrent` file write**

In `setupMetadataDir`, guard the torrent write:

```go
if len(torrentFile) > 0 {
    // Skip write if file already exists with correct size.
    if info, statErr := os.Stat(torrentPath); statErr != nil || info.Size() != int64(len(torrentFile)) {
        if err := atomicWriteFile(torrentPath, torrentFile); err != nil {
            return "", "", "", fmt.Errorf("writing .torrent file: %w", err)
        }
    }
}
```

- [ ] **Step 4: Run test to verify**

Run: `GOROOT=/Users/mailoarsac/.local/share/mise/installs/go/1.25.7 go test ./internal/destination/... -run TestSetupMetadataDir_Idempotent -count=1 -v`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/destination/init.go internal/destination/server_test.go
git commit -m "fix(destination): make setupMetadataDir idempotent for recovery path"
```

---

### Task 8: Add startup recovery

**Files:**
- Create: `internal/destination/recovery.go`
- Create: `internal/destination/recovery_test.go`
- Modify: `internal/destination/server.go` (call from Start)

- [ ] **Step 1: Write failing test for `recoverInFlightTorrents`**

```go
// internal/destination/recovery_test.go
package destination

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/bits-and-blooms/bitset"
)

func TestRecoverInFlightTorrents(t *testing.T) {
	t.Parallel()
	s, tmpDir := newTestDestServer(t)
	ctx := context.Background()

	hash := "abc123def456"
	metaDir := filepath.Join(tmpDir, metaDirName, hash)
	_ = os.MkdirAll(metaDir, 0o755)

	// Write a minimal .torrent file (single-file torrent).
	torrentData := createTestTorrentFile(t, "TestMovie", 1024, 16384)
	_ = os.WriteFile(filepath.Join(metaDir, "TestMovie.torrent"), torrentData, 0o644)

	// Write version file.
	_ = os.WriteFile(filepath.Join(metaDir, versionFileName), []byte(metaVersion), 0o644)

	// Write .state with 1 piece written.
	written := bitset.New(1)
	written.Set(0)
	stateData, _ := written.MarshalBinary()
	_ = os.WriteFile(filepath.Join(metaDir, ".state"), stateData, 0o644)

	// Write .subpath.
	_ = os.WriteFile(filepath.Join(metaDir, subPathFileName), []byte("movies"), 0o644)

	// Write .selected (all selected).
	_ = os.WriteFile(filepath.Join(metaDir, selectedFileName), []byte{1}, 0o644)

	// Run recovery.
	if err := s.recoverInFlightTorrents(ctx); err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	// Verify torrent is tracked.
	state, ok := s.store.Get(hash)
	if !ok {
		t.Fatal("expected torrent to be tracked after recovery")
	}

	// Verify written bitmap loaded.
	state.mu.Lock()
	count := state.written.Count()
	state.mu.Unlock()
	if count == 0 {
		t.Fatal("expected written bitmap to have pieces from .state")
	}
}

func TestRecoverInFlightTorrents_SkipsFinalized(t *testing.T) {
	t.Parallel()
	s, tmpDir := newTestDestServer(t)

	hash := "finalized123"
	metaDir := filepath.Join(tmpDir, metaDirName, hash)
	_ = os.MkdirAll(metaDir, 0o755)

	// Write .finalized marker.
	_ = os.WriteFile(filepath.Join(metaDir, finalizedFileName), nil, 0o644)
	_ = os.WriteFile(filepath.Join(metaDir, versionFileName), []byte(metaVersion), 0o644)

	torrentData := createTestTorrentFile(t, "Done", 512, 16384)
	_ = os.WriteFile(filepath.Join(metaDir, "Done.torrent"), torrentData, 0o644)

	if err := s.recoverInFlightTorrents(context.Background()); err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	if _, ok := s.store.Get(hash); ok {
		t.Fatal("finalized torrent should not be recovered")
	}
}

func TestRecoverInFlightTorrents_SkipsStaleVersion(t *testing.T) {
	t.Parallel()
	s, tmpDir := newTestDestServer(t)

	hash := "stale123"
	metaDir := filepath.Join(tmpDir, metaDirName, hash)
	_ = os.MkdirAll(metaDir, 0o755)

	_ = os.WriteFile(filepath.Join(metaDir, versionFileName), []byte("1"), 0o644)
	torrentData := createTestTorrentFile(t, "Old", 512, 16384)
	_ = os.WriteFile(filepath.Join(metaDir, "Old.torrent"), torrentData, 0o644)

	if err := s.recoverInFlightTorrents(context.Background()); err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	if _, ok := s.store.Get(hash); ok {
		t.Fatal("stale version torrent should not be recovered")
	}
}

// createTestTorrentFile builds a minimal bencoded .torrent for testing.
func createTestTorrentFile(t *testing.T, name string, fileSize, pieceLen int64) []byte {
	t.Helper()
	numPieces := (fileSize + pieceLen - 1) / pieceLen
	pieces := make([]byte, numPieces*20)
	// Single-file torrent bencode:
	// d4:infod6:lengthi<size>e4:name<len>:<name>12:piece lengthi<plen>e6:pieces<plen>:<data>ee
	data := fmt.Sprintf("d4:infod6:lengthi%de4:name%d:%s12:piece lengthi%de6:pieces%d:%see",
		fileSize, len(name), name, pieceLen, len(pieces), string(pieces))
	return []byte(data)
}
```

Note: `createTestTorrentFile` needs `"fmt"` imported in the test file.

- [ ] **Step 2: Run tests to verify they fail**

Run: `GOROOT=/Users/mailoarsac/.local/share/mise/installs/go/1.25.7 go test ./internal/destination/... -run "TestRecoverInFlightTorrents" -count=1`

Expected: compilation error — `recoverInFlightTorrents` not defined.

- [ ] **Step 3: Implement `recoverInFlightTorrents`**

```go
// internal/destination/recovery.go
package destination

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	pb "github.com/arsac/qb-sync/proto"
)

// recoverInFlightTorrents scans persisted metadata directories and rebuilds
// in-memory state for in-flight torrents. Called once during Server.Start
// before accepting gRPC requests.
func (s *Server) recoverInFlightTorrents(ctx context.Context) error {
	// Purge recycled inodes before any hardlink checks.
	s.store.Inodes().CleanupStale(ctx)

	metaRoot := filepath.Join(s.config.BasePath, metaDirName)
	entries, err := os.ReadDir(metaRoot)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("reading metadata root: %w", err)
	}

	var recovered int
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		hash := entry.Name()
		metaDir := filepath.Join(metaRoot, hash)

		if err := s.recoverTorrent(ctx, hash, metaDir); err != nil {
			s.logger.WarnContext(ctx, "skipping recovery for torrent",
				"hash", hash, "error", err)
			continue
		}
		recovered++
	}

	if recovered > 0 {
		s.logger.InfoContext(ctx, "recovered in-flight torrents",
			"count", recovered)
	}
	return nil
}

// recoverTorrent rebuilds state for a single torrent from its persisted metadata.
func (s *Server) recoverTorrent(ctx context.Context, hash, metaDir string) error {
	// Skip finalized torrents.
	if _, err := os.Stat(filepath.Join(metaDir, finalizedFileName)); err == nil {
		return fmt.Errorf("already finalized")
	}

	// Skip stale metadata version.
	if !checkMetaVersion(metaDir) {
		return fmt.Errorf("stale metadata version")
	}

	// Find and parse the .torrent file.
	torrentPath, err := findTorrentFile(metaDir)
	if err != nil {
		return fmt.Errorf("finding torrent file: %w", err)
	}
	torrentData, err := os.ReadFile(torrentPath)
	if err != nil {
		return fmt.Errorf("reading torrent file: %w", err)
	}
	parsed, err := parseTorrentFile(torrentData)
	if err != nil {
		return fmt.Errorf("parsing torrent file: %w", err)
	}

	// Load persisted sub-path and selection state.
	saveSubPath := loadSubPathFile(metaDir)
	numFiles := len(parsed.Files)
	selected := loadSelectedFile(metaDir, numFiles)

	// Build synthetic InitTorrentRequest.
	files := make([]*pb.FileInfo, numFiles)
	for i, f := range parsed.Files {
		sel := true
		if selected != nil && i < len(selected) {
			sel = selected[i]
		}
		files[i] = &pb.FileInfo{
			Path:     f.Path,
			Size:     f.Size,
			Offset:   f.Offset,
			Selected: sel,
		}
	}

	req := &pb.InitTorrentRequest{
		TorrentHash: hash,
		Name:        parsed.Name,
		PieceSize:   parsed.PieceLength,
		TotalSize:   parsed.TotalSize,
		NumPieces:   int32(parsed.NumPieces),
		Files:       files,
		PieceHashes: parsed.PieceHashes,
		SaveSubPath: saveSubPath,
		// TorrentFile intentionally nil — file already exists on disk.
	}

	resp := s.initNewTorrent(ctx, hash, req)
	if !resp.GetSuccess() {
		return fmt.Errorf("init failed: %s", resp.GetError())
	}

	s.logger.InfoContext(ctx, "recovered torrent from disk",
		"hash", hash,
		"name", parsed.Name,
		"piecesHave", resp.GetPiecesHaveCount(),
	)
	return nil
}
```

- [ ] **Step 4: Call recovery from Server.Start**

In `internal/destination/server.go`, find the `Start` method and add the recovery call before starting background goroutines:

```go
func (s *Server) Start(ctx context.Context) {
	// Recover in-flight torrents from persisted metadata.
	if err := s.recoverInFlightTorrents(ctx); err != nil {
		s.logger.ErrorContext(ctx, "failed to recover torrents", "error", err)
	}

	// ... existing goroutine starts ...
}
```

- [ ] **Step 5: Run recovery tests**

Run: `GOROOT=/Users/mailoarsac/.local/share/mise/installs/go/1.25.7 go test ./internal/destination/... -run "TestRecoverInFlightTorrents" -count=1 -v`

Expected: PASS

- [ ] **Step 6: Run full unit test suite**

Run: `GOROOT=/Users/mailoarsac/.local/share/mise/installs/go/1.25.7 go test ./internal/destination/... -short -count=1`

Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add internal/destination/recovery.go internal/destination/recovery_test.go internal/destination/server.go
git commit -m "feat(destination): add startup recovery from persisted metadata"
```

---

### Task 9: Run full test suite and lint

**Files:** None (verification only)

- [ ] **Step 1: Run all unit tests**

Run: `GOROOT=/Users/mailoarsac/.local/share/mise/installs/go/1.25.7 go test ./internal/... -short -count=1`

Expected: PASS across all packages

- [ ] **Step 2: Run linter**

Run: `GOROOT=/Users/mailoarsac/.local/share/mise/installs/go/1.25.7 golangci-lint run --fix`

Expected: 0 issues (fix any that appear)

- [ ] **Step 3: Run E2E vet**

Run: `GOROOT=/Users/mailoarsac/.local/share/mise/installs/go/1.25.7 go vet -tags=e2e ./test/e2e/...`

Expected: No errors

- [ ] **Step 4: Commit any lint fixes**

```bash
git add -A
git commit -m "fix: address lint issues from TorrentStore refactor"
```
