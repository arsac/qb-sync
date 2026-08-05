# Stale Destination State Recovery

## Status

Implemented. The original design proposed disk-based recovery in `recoverTorrentState`
/ `getOrRecoverState`. During implementation, the parallel recovery path was removed
in favor of a simpler architecture: the source re-initializes the torrent via
`InitTorrent` after receiving `FINALIZE_ERROR_NOT_FOUND`.

## Problem

When the destination server restarts, it does not recover torrent state from disk
automatically. If the source calls `FinalizeTorrent` before re-initializing the
torrent via `InitTorrent`, the destination has no in-memory state and cannot proceed.
Additionally, if a stale `.state` file claims pieces are written but the data files
were externally deleted, `initNewTorrent` would trust the stale bitmap and report
all pieces as written, causing finalization to fail in a retry loop.

### Observed in production

- 7 torrents stuck for 2+ hours with ~240 retries each
- 5 were cross-seed hardlinks (directories created, hardlinks never materialized)
- 2 were radarr files externally deleted after sync

## Implemented Design

Two layers: validate during init, communicate via structured error code, handle on
source side.

### Layer 1: Validate file existence during init

**File:** `internal/destination/persistence.go` -- `validateDataFiles`

Called from `initNewTorrent` before loading the persisted `.state` file. Checks that
every selected file exists on disk (as either the final path or `.partial`). If any
selected file is missing, the stale `.state` file is removed so `buildWrittenBitmap`
starts with a clean bitmap.

**File:** `internal/destination/init.go` -- `initNewTorrent`

```go
if validateErr := validateDataFiles(files); validateErr != nil {
    _ = os.Remove(statePath)
}
```

**File:** `internal/destination/init.go` -- `setupMetadataDir`

Stale metadata directories (missing or wrong version file) are nuked before
re-creating, preventing loading of incompatible state files.

### Layer 2: Structured error code for not-found

**File:** `proto/qbsync.proto`

```protobuf
enum FinalizeErrorCode {
  FINALIZE_ERROR_NONE = 0;
  FINALIZE_ERROR_INCOMPLETE = 1;
  FINALIZE_ERROR_NOT_FOUND = 2;  // Torrent state not found or stale
}
```

**File:** `internal/destination/finalize.go` -- `getState`

Simple in-memory lookup (no disk recovery). Returns error if torrent is not tracked
or still initializing. `FinalizeTorrent` maps this to `FINALIZE_ERROR_NOT_FOUND`.

**File:** `internal/streaming/grpc_destination.go`

Sentinel error `ErrFinalizeNotFound` and switch-based error dispatch on
`FinalizeErrorCode`.

**File:** `internal/source/finalization.go`

`handleNotFoundFinalization` untracks the torrent via `stopTracking` so the next poll
cycle re-discovers and re-initializes it from scratch.

## Self-healing flow

```
Source calls FinalizeTorrent after destination restart
  -> destination has no in-memory state -> FINALIZE_ERROR_NOT_FOUND
  -> source untracks torrent (handleNotFoundFinalization -> stopTracking)
  -> next poll cycle re-discovers torrent -> InitTorrent -> fresh init
  -> stale .state detected by validateDataFiles -> removed -> clean bitmap
  -> streaming resumes -> finalize -> success
```

## Test coverage

- `TestValidateDataFiles` -- file validation logic
- `TestFinalizeTorrent_NotFound_ReturnsErrorCode` -- destination returns NOT_FOUND
- `TestFinalizeTorrent_ErrFinalizeNotFoundPropagates` -- source-side sentinel
- `TestHandleNotFoundFinalization_Untracks` -- untracking behavior
- `TestInitTorrent_StaleMetadata_NukedBeforeInit` -- stale metadata cleanup

## Files changed

| File | Change |
|------|--------|
| `proto/qbsync.proto` | Add `FINALIZE_ERROR_NOT_FOUND = 2` |
| `proto/qbsync.pb.go` | Regenerate |
| `internal/destination/persistence.go` | Add `validateDataFiles`; remove `recoverTorrentState`, `reconstructFiles`, `restoreFileSelection`, `loadOrReconstructState` |
| `internal/destination/init.go` | Call `validateDataFiles` to detect stale state; stale metadata nuke in `setupMetadataDir` |
| `internal/destination/finalize.go` | Simplify `getOrRecoverState` to `getState` (memory-only); use `FINALIZE_ERROR_NOT_FOUND` |
| `internal/streaming/grpc_destination.go` | Add `ErrFinalizeNotFound` sentinel and switch-based error dispatch |
| `internal/source/finalization.go` | Handle `ErrFinalizeNotFound` via `handleNotFoundFinalization` -> `stopTracking` |
