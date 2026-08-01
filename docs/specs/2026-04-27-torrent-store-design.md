# TorrentStore: Centralized State Management

## Problem

Destination server state is scattered across four independent systems on the `Server` struct:

1. `s.torrents` map + `s.mu` — per-torrent tracking
2. `s.filePaths` map + `s.mu` — file path ownership
3. `s.inodes` (`InodeRegistry`) — hardlink deduplication
4. `s.abortingHashes` map + `s.mu` — abort coordination

These are accessed from 17 sites across 6 files. Each site manages its own locking and invariants. Lifecycle operations (create, remove, abort) must coordinate across all four systems, but that coordination is scattered across callers. This produces edge-case bugs:

- **Restart loses progress**: `CheckTorrentStatus` returns `-1` for torrents with disk state because no startup recovery exists.
- **Inconsistent cleanup**: 5 deletion paths each do slightly different subsets of unregister-file-paths + delete-from-map + abort-inodes.
- **Stale hardlinks**: Inode cleanup is not coordinated with torrent removal.
- **Dead artifacts**: `.selected` file written but never loaded; `state.info` stored but never read.

## Design

### TorrentStore

A single type that owns all shared mutable state. All callers interact through methods — no direct map or lock access.

```go
// TorrentStore owns all shared mutable state for the destination server:
// torrent tracking, file path ownership, inode registry, and abort coordination.
//
// Lock ordering: store.mu -> state.mu -> InodeRegistry internal locks.
// Callers never acquire store.mu directly.
type TorrentStore struct {
    mu        sync.RWMutex
    entries   map[string]*serverTorrentState
    filePaths map[string]string         // target path -> owning hash
    aborting  map[string]chan struct{}   // hash -> completion signal
    inodes    *InodeRegistry            // owned, not shared
    basePath  string
    logger    *slog.Logger
}
```

#### Read operations

```go
// Get returns a torrent's state. The pointer is safe to use outside
// the store lock — callers use state.mu for mutable fields.
func (ts *TorrentStore) Get(hash string) (*serverTorrentState, bool)

// ForEach snapshots entries under RLock, then iterates outside the lock.
func (ts *TorrentStore) ForEach(fn func(hash string, state *serverTorrentState) bool)

// Len returns the count of tracked torrents.
func (ts *TorrentStore) Len() int
```

Replaces: `s.torrents[hash]` reads in write.go, finalize.go, lifecycle.go. `collectTorrents` in lifecycle.go. `len(s.torrents)` for metrics.

#### Two-phase creation

```go
// Reserve inserts a sentinel to prevent concurrent initialization.
// Returns error if hash is already tracked or initializing.
func (ts *TorrentStore) Reserve(hash string) error

// Commit replaces the sentinel with real state. Checks file path
// collisions and registers paths atomically. On collision, removes
// the sentinel and returns the error.
func (ts *TorrentStore) Commit(hash string, state *serverTorrentState) error

// Unreserve removes the sentinel on initialization failure.
func (ts *TorrentStore) Unreserve(hash string)
```

Replaces: Sentinel insert at init.go:94, swap at init.go:245-253, error cleanup at init.go:102-107. `checkPathCollisions` and `registerFilePaths` move inside `Commit`.

#### Removal

```go
// Remove deletes a torrent, unregisters file paths, and aborts any
// in-progress inodes for the hash. Returns the old state for caller
// cleanup (close handles, save dirty state). No-op if not found.
func (ts *TorrentStore) Remove(hash string) *serverTorrentState

// Drain removes all entries and returns them. Used for shutdown.
func (ts *TorrentStore) Drain() map[string]*serverTorrentState
```

Replaces: 5 separate deletion paths in `cleanupFinalizedTorrent`, `AbortTorrent`, `cleanupForResync`, `initNewTorrent` error path, and `cleanup` shutdown. Each currently does its own `lock -> unregister -> delete -> unlock` sequence. `Remove` also calls `InodeRegistry.AbortInProgress` for each file with a non-zero source inode, coordinating cleanup that was previously scattered.

#### Abort coordination

```go
// BeginAbort atomically removes the torrent from the store and registers
// a cleanup channel. If an abort is already in progress, returns the
// existing channel (caller should wait on it). The caller closes the
// returned channel after cleanup completes, then calls EndCleanup.
func (ts *TorrentStore) BeginAbort(hash string, ch chan struct{}) (
    state *serverTorrentState,
    existingCh chan struct{},
)

// BeginCleanup registers a cleanup channel for an untracked hash
// (orphan cleanup). Returns false if the hash is tracked or already
// being cleaned.
func (ts *TorrentStore) BeginCleanup(hash string, ch chan struct{}) bool

// EndCleanup deregisters the cleanup channel.
func (ts *TorrentStore) EndCleanup(hash string)

// AbortCh returns the abort/cleanup channel if one is registered.
// Used by InitTorrent to block during an in-progress abort.
func (ts *TorrentStore) AbortCh(hash string) (chan struct{}, bool)
```

Replaces: `s.abortingHashes` management in abort.go, lifecycle.go, init.go.

#### Inode delegation

```go
// RegisterInodes registers finalized file inodes in the persistent
// registry and signals any in-progress waiters. Called after successful
// verification during finalization.
func (ts *TorrentStore) RegisterInodes(ctx context.Context, hash string, files []*serverFileInfo)

// Inodes returns the underlying InodeRegistry for read-only operations
// during setupFiles (GetRegistered, GetInProgress, RegisterInProgress).
// These operations use the registry's internal fine-grained locks and
// do not require the store lock.
func (ts *TorrentStore) Inodes() *InodeRegistry

// SaveInodes persists the inode registry to disk.
func (ts *TorrentStore) SaveInodes() error
```

`setupFiles` calls `ts.Inodes()` for fine-grained inode lookups during file setup. Lifecycle operations (`Remove`, `RegisterInodes`) go through the store for coordinated cleanup.

The `InodeRegistry` keeps its own `registeredMu` and `inProgressMu` — inode lookups during `setupFiles` should not block torrent-level operations.

### Startup Recovery

Recovery is a `Server`-level operation — it calls `initNewTorrent` (which uses `setupFiles`, `buildWrittenBitmap`, and the store). The store provides the state primitives; the server orchestrates.

```go
// recoverInFlightTorrents scans persisted metadata directories and
// rebuilds in-memory state for in-flight torrents. Called once during
// Server.Start before accepting gRPC requests.
func (s *Server) recoverInFlightTorrents(ctx context.Context) error
```

Implementation:
1. `s.store.Inodes().CleanupStale(ctx)` — purge recycled inodes before any hardlink checks.
2. Scan `basePath/.qbsync/*/` for metadata directories.
3. For each directory:
   - Skip if `.finalized` marker exists (already complete).
   - Skip if `.version` doesn't match `metaVersion` (stale format).
   - Skip if no `.torrent` file found (incomplete metadata).
   - Parse `.torrent` via `parseTorrentFile` -> name, files, pieceHashes, pieceSize, totalSize.
   - Load `.subpath` -> saveSubPath.
   - Load `.selected` -> file selection bitmap.
   - Construct synthetic `pb.InitTorrentRequest` from parsed data.
   - Call `s.initNewTorrent` with the synthetic request (idempotent setup handles existing files).
4. Log recovered torrent count.

After recovery, `CheckTorrentStatus` for any recovered torrent hits `resumeTorrent` (state exists in the store) and returns real `PiecesNeeded`. The `len(req.GetFiles()) == 0` guard remains as a correct fallback for truly unknown torrents.

### Idempotent `initNewTorrent`

`initNewTorrent` becomes safe to call on already-initialized metadata:

- `setupMetadataDir`: `MkdirAll` is already idempotent. Guard `.torrent` write with existence check. Guard `.version` write with content check. Accept harmless re-writes of `.subpath` and `.selected` (atomic writes to existing files are safe).
- `setupFile`: already handles existing files and `.partial` files. `MkdirAll` is idempotent.
- `buildWrittenBitmap`: already loads existing `.state`, merges hardlink coverage, clears stale pieces. Fully idempotent.

With idempotent setup, startup recovery feeds synthetic requests through the same code path as source-initiated initialization. One creation path, one set of invariants.

### Remove dead `state.info`

The `info *pb.InitTorrentRequest` field on `serverTorrentState` is stored at init.go:238 but never read in production code. Remove it. This eliminates the only coupling between state construction and the protobuf wire format, making disk-based recovery straightforward (no need to construct a full protobuf request with fields like `TorrentFile` that aren't needed after init).

Note: startup recovery still constructs a synthetic `pb.InitTorrentRequest` to feed through `initNewTorrent`, but only the fields that `initNewTorrent` actually reads: `TorrentHash`, `Name`, `Files`, `PieceHashes`, `PieceSize`, `TotalSize`, `SaveSubPath`. The `TorrentFile` field can be nil (the file already exists on disk; `setupMetadataDir` skips the write when `len(torrentFile) == 0`).

## What moves inside the store

| Currently scattered | Now in store |
|---|---|
| `s.mu.Lock/RLock/Unlock` (17 sites) | Internal to store methods |
| `s.torrents` map | `ts.entries` |
| `s.filePaths` map | `ts.filePaths` |
| `s.abortingHashes` map | `ts.aborting` |
| `s.inodes` (InodeRegistry) | `ts.inodes` (composed) |
| `checkPathCollisions` | Inside `Commit` |
| `registerFilePaths` | Inside `Commit` |
| `unregisterFilePaths` | Inside `Remove` / `BeginAbort` |
| `collectTorrents` | Inside `ForEach` |
| Sentinel insert/swap/cleanup | `Reserve` / `Commit` / `Unreserve` |
| Abort channel management | `BeginAbort` / `BeginCleanup` / `EndCleanup` |
| Inode abort on torrent removal | Inside `Remove` |
| Inode registration on finalization | `RegisterInodes` |

## What callers still do

- `state.mu` locking for mutable per-torrent fields — unchanged.
- Disk I/O (`setupFiles`, `buildWrittenBitmap`, `setupMetadataDir`) — stays in init.go.
- Piece writing, finalization, verification — stays in their respective files.
- `setupFiles` calls `ts.Inodes()` for fine-grained inode lookups — registry's internal locks.

The store is state management. Business logic stays in the server methods.

## What this fixes

- **Restart loses progress**: `Recover` rebuilds state from disk. `CheckTorrentStatus` returns real `PiecesNeeded`.
- **Partial selection stuck at 100%**: Recovered state includes `.selected`, so piece counting and finalization checks work correctly.
- **Inconsistent cleanup**: One `Remove` method, one set of invariants (unregister paths, abort inodes, delete entry).
- **Stale hardlinks at startup**: `Recover` calls `CleanupStale` before processing any torrents.
- **Dead artifacts**: `.selected` is now load-bearing (read during recovery). `state.info` removed.

## What doesn't change

- `serverTorrentState` struct (minus `info`) — same fields, same `state.mu`.
- `torrentMeta` — unchanged.
- Lock ordering: `store.mu` (was `s.mu`) -> `state.mu` -> InodeRegistry locks.
- `WritePiece`, `FinalizeTorrent`, `AbortTorrent` logic — same, just call store methods.
- State flushing — `ForEach` replaces `collectTorrents`, same pattern.
- Orphan cleanup — same logic, store methods replace direct map access.
- E2E tests — same behavior, different internal structure.

## Testing

### TorrentStore unit tests
- `Create`: `Reserve` + `Commit` enforces collision check, registers file paths.
- `Remove`: unregisters file paths, aborts in-progress inodes, returns old state.
- `Get`: returns nil for unknown hash, returns state for known hash.
- `ForEach`: snapshot iteration, safe concurrent modification.
- `BeginAbort`: returns existing channel if already aborting, removes state otherwise.
- `BeginCleanup`: returns false for tracked hash, registers channel for untracked.
- Concurrent access: multiple goroutines calling Get/Reserve/Commit/Remove.

### Recovery tests
- Write `.torrent` + `.state` + `.subpath` + `.selected` to temp dirs, call `Recover`, verify state accuracy and written bitmap.
- Finalized dir (`.finalized` marker): skipped.
- Stale version dir: skipped.
- Missing `.torrent`: skipped.
- Multiple dirs: only valid in-progress ones recovered.

### Idempotency tests
- Call `initNewTorrent` twice with same inputs, verify state is identical.
- Call `initNewTorrent` after `Recover` loaded the same torrent, verify no corruption.

### Existing tests
- Update to use store API instead of direct map access.
- E2E tests pass unchanged (external behavior is the same).
