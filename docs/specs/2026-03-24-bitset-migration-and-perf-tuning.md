# Bitset Migration & Throughput Tuning

## Scope

Two interleaved changes that touch overlapping files:

1. **Bitset migration** — Replace `written []bool` with `*bitset.BitSet`, eliminate `writtenCount`, bump `metaVersion`
2. **Throughput tuning** — Fix 6 bottlenecks limiting link utilization on 1Gbps home network with NFS storage

## Part A: Bitset Migration

### Dependency

`github.com/bits-and-blooms/bitset` — stable (v1 since 2011), zero transitive deps.

### Core changes

| Before | After |
|--------|-------|
| `written []bool` | `written *bitset.BitSet` |
| `writtenCount int` (manual inc/dec at 4 sites) | Deleted — use `written.Count()` (O(n/64) popcount, ~150ns for 10K pieces) |
| `len(written)` for capacity | `written.Len()` |
| `written[i] = true/false` | `written.Set(i)` / `written.Clear(i)` |
| `written[i]` read | `written.Test(i)` |
| `written[i] \|\| covered[i]` loop | `written.InPlaceUnion(covered)` |
| `make([]bool, n)` | `bitset.New(n)` |
| `copy(snapshot, written)` | `written.Clone()` |
| `countWritten()` helper | Deleted — use `.Count()` |

### Serialization

Use `MarshalBinary`/`UnmarshalBinary` (length-prefixed, big-endian, portable). Not raw `Bytes()` which returns internal slice by reference and is byte-order dependent.

After `UnmarshalBinary`, resize if `bs.Len() < numPieces` using the set-then-clear idiom.

### Version bump

`metaVersion` "1" → "2". Existing `.state` files auto-nuked on next `InitTorrent` — torrents re-stream from scratch. Acceptable per user.

### Proto boundary

`piecesNeeded []bool` in gRPC responses stays `[]bool`. Add private helper `boolSliceToBitSet` for the `calculatePiecesCovered` → `InPlaceUnion` conversion. `calculatePiecesNeeded` converts bitset → `[]bool` for the response.

### int/uint boundary

`bitset` uses `uint` for indices. Piece indices arrive as `int32` from proto. Add defensive `pieceIndex < 0` check at the RPC boundary in `writePiece` before casting to `uint`.

### `saveStateFunc` test injection

Change signature from `func(string, []bool) error` to `func(string, *bitset.BitSet) error`. Update all test lambdas directly.

## Part B: Throughput Tuning

### Fix 1 (Critical): Move `writePieceData` outside `state.mu`

**File:** `write.go:116-149`

Currently `state.mu` is held across the `writePieceData` call (NFS `pwrite(2)`) and `markPieceWritten`. With 8 stream workers all serializing on this lock for disk I/O, effective write concurrency = 1.

**Fix:** Do disk I/O outside the lock. Re-acquire lock only for bitmap bookkeeping.

```
// Outside lock: hash verify (already done), writePieceData (MOVE HERE)
// Inside lock: check finalization.active, check already-written, markPieceWritten, checkFileCompletions
```

Safe because `pwrite(2)` at non-overlapping offsets is POSIX-safe. Two workers writing the same piece concurrently write identical data at the same offset — idempotent.

### Fix 2: Remove inline `saveState` from `markPieceWritten`

**File:** `write.go:47-64`

The count-based flush (`piecesSinceFlush >= flushCount`) calls `saveState` inside `state.mu`, stalling all writers during NFS sync. The background `runStateFlusher` (30s interval) already handles persistence.

**Fix:** Remove the `shouldFlush` branch from `markPieceWritten`. Keep the `dirty` flag and `piecesSinceFlush` counter for the background flusher. The only inline flush that stays is the "all pieces written" completeness trigger (ensures state is persisted before finalization).

### Fix 3: Increase `workCh` buffer

**File:** `streaming.go:28`

`workCh := make(chan *pb.WritePieceRequest, numWorkers)` — buffer = 8. When all workers block on disk I/O, the receiver stalls and triggers HTTP/2 flow control.

**Fix:** `workCh := make(chan *pb.WritePieceRequest, numWorkers*2)` — 16 slots. Allows receiver to stay ahead of disk latency. Memory bounded by `memBudget` semaphore (512MB default).

### Fix 4: Reduce piece poll interval

**File:** `piece_monitor.go:26`

`defaultPollInterval = 2s` creates up to 2-second dispatch latency per completed piece batch. On 1Gbps link, 2s idle = ~250MB wasted.

**Fix:** `defaultPollInterval = 500 * time.Millisecond`. The `idleSlowFactor = 5` already backs off for idle torrents. Cost: one `GetPieceStates` HTTP call per torrent per 500ms — cheap vector-of-uint8 response.

### Fix 5: Acquire window slot before disk read

**File:** `streaming_queue.go:466-548` (`sendPiecePool`)

Currently: read piece from disk → try window slot → if full, waste the read. TOCTOU between `CanSend()` check and `TrySend()`.

**Fix:** Acquire window slot first, then read from disk. If window full, fail fast without wasting NFS I/O. Add `TrySelectAndSend(key)` to `StreamPool` that atomically selects stream + acquires slot.

### Fix 6: Raise initial congestion window

**File:** `congestion/window.go:82`

`DefaultInitialWindow = 64` pieces. On LAN reconnect, takes several seconds to ramp back via slow-start. Wastes link capacity.

**Fix:** `DefaultInitialWindow = 512`. CUBIC `OnFail` will reduce immediately on any loss event. Safe for home LAN.

## Files Changed

| File | Part A (bitset) | Part B (perf) |
|------|----------------|---------------|
| `go.mod` | Add bitset dep | — |
| `config.go` | Bump metaVersion | — |
| `types.go` | `written *bitset.BitSet`, delete `writtenCount` | — |
| `persistence.go` | Rewrite load/save for bitset, delete `countWritten` | — |
| `init.go` | `buildWrittenBitmap` returns bitset, `InPlaceUnion` | — |
| `write.go` | Set/Clear/Test, delete writtenCount mutations | Move writePieceData outside lock, remove inline saveState |
| `finalize.go` | Test/Clear, delete writtenCount mutations | — |
| `state.go` | Update calculatePiecesNeeded, countSelectedPiecesTotal | — |
| `lifecycle.go` | Clone() snapshots, Count() | — |
| `server.go` | saveStateFunc signature | — |
| `streaming.go` | — | Increase workCh buffer |
| `piece_monitor.go` | — | Reduce poll interval |
| `streaming_queue.go` | — | Window slot before disk read |
| `congestion/window.go` | — | Raise initial window |
| All test files | Update []bool → bitset in tests | Update tests for new write.go lock scope |

## Build Sequence

1. `go get github.com/bits-and-blooms/bitset`
2. Add `boolSliceToBitSet` helper
3. Migrate `persistence.go` (loadState/saveState with MarshalBinary)
4. Migrate `types.go` (written field, delete writtenCount, update method signatures)
5. Migrate `state.go` (delete countWritten, update calculatePiecesNeeded)
6. Migrate `write.go` — bitset AND perf fixes 1+2 together (move I/O outside lock, remove inline flush, Set/Clear/Test)
7. Migrate `finalize.go` (Test/Clear, delete writtenCount)
8. Migrate `init.go` (buildWrittenBitmap returns bitset, InPlaceUnion)
9. Migrate `lifecycle.go` (Clone snapshots)
10. Bump `metaVersion` in `config.go`
11. Update `server.go` (saveStateFunc)
12. Apply perf fix 3: `streaming.go` workCh buffer
13. Apply perf fix 4: `piece_monitor.go` poll interval
14. Apply perf fix 5: `streaming_queue.go` window-before-read
15. Apply perf fix 6: `congestion/window.go` initial window
16. Update all test files
17. Run tests, lint, vet e2e

## Testing

- Existing unit tests updated for bitset API
- Existing e2e tests validate end-to-end correctness (version bump causes full re-stream — exercises fresh init path)
- Manual verification: deploy and monitor throughput metrics before/after
