# Bitset Migration & Throughput Tuning Implementation Plan

> **Historical record.** This plan describes work that has already shipped. It is kept for context on why the code looks the way it does; the checkboxes reflect the original build order and are not outstanding work.

**Goal:** Replace `written []bool` with `*bitset.BitSet`, eliminate `writtenCount` consistency hazard, and fix 6 throughput bottlenecks to maximize link utilization.

**Architecture:** The bitset migration touches 10 production files and 5 test files (26 production sites, 45 test sites). Changes are layered bottom-up: persistence → types → state → write → finalize → init → lifecycle. Perf fixes are applied to `write.go` during the bitset migration (since both modify the same lock scope) and to 4 independent files afterwards. Version bump from "1" to "2" nukes all existing `.state` files.

**Tech Stack:** Go 1.25, `github.com/bits-and-blooms/bitset`, gRPC, NFS

**Spec:** `docs/specs/2026-03-24-bitset-migration-and-perf-tuning.md`

---

### Task 1: Add bitset dependency and helpers

**Files:**
- Modify: `go.mod`
- Create: `internal/destination/bitmap.go`

- [ ] **Step 1: Add dependency**

```bash
go get github.com/bits-and-blooms/bitset
```

- [ ] **Step 2: Create bitmap.go with conversion helpers**

Create `internal/destination/bitmap.go`:

```go
package destination

import "github.com/bits-and-blooms/bitset"

// boolSliceToBitSet converts a []bool to a *bitset.BitSet.
// Used at the boundary between proto responses ([]bool) and internal state (*bitset.BitSet).
func boolSliceToBitSet(bs []bool) *bitset.BitSet {
	result := bitset.New(uint(len(bs)))
	for i, v := range bs {
		if v {
			result.Set(uint(i))
		}
	}
	return result
}

// ensureBitSetLength extends a bitset to at least n bits if it's too short.
// Used after UnmarshalBinary to handle truncated or shorter-than-expected state files.
func ensureBitSetLength(bs *bitset.BitSet, n uint) *bitset.BitSet {
	if bs.Len() >= n {
		return bs
	}
	// Set and clear the last bit to force allocation to the right length
	bs.Set(n - 1)
	bs.Clear(n - 1)
	return bs
}
```

- [ ] **Step 3: Verify it compiles**

```bash
go build ./internal/destination/...
```

- [ ] **Step 4: Commit**

```bash
git add go.mod go.sum internal/destination/bitmap.go
git commit -m "feat: add bitset dependency and conversion helpers"
```

---

### Task 2: Migrate persistence layer (loadState / saveState)

**Files:**
- Modify: `internal/destination/persistence.go:15-50` (loadState, saveState, doSaveState)
- Modify: `internal/destination/persistence.go:126-159` (clearStalePieces)
- Modify: `internal/destination/server.go:96-97` (saveStateFunc field)

- [ ] **Step 1: Rewrite loadState to return *bitset.BitSet**

`persistence.go:15-26` — Replace the manual byte→bool loop with `UnmarshalBinary`:

```go
func (s *Server) loadState(path string, numPieces int) (*bitset.BitSet, error) {
	data, readErr := os.ReadFile(path)
	if readErr != nil {
		return nil, readErr
	}

	bs := bitset.New(0)
	if unmarshalErr := bs.UnmarshalBinary(data); unmarshalErr != nil {
		return nil, fmt.Errorf("unmarshaling state: %w", unmarshalErr)
	}
	return ensureBitSetLength(bs, uint(numPieces)), nil
}
```

- [ ] **Step 2: Rewrite saveState to accept *bitset.BitSet**

`persistence.go:34-42` — Replace the manual bool→byte loop with `MarshalBinary`:

```go
func (s *Server) saveState(path string, written *bitset.BitSet) error {
	data, marshalErr := written.MarshalBinary()
	if marshalErr != nil {
		return fmt.Errorf("marshaling state: %w", marshalErr)
	}
	return atomicWriteFile(path, data)
}
```

- [ ] **Step 3: Update doSaveState and saveStateFunc signatures**

`persistence.go:45-50`:
```go
func (s *Server) doSaveState(path string, written *bitset.BitSet) error {
```

`server.go:96-97`:
```go
saveStateFunc func(path string, written *bitset.BitSet) error
```

- [ ] **Step 4: Update clearStalePieces to use bitset**

`persistence.go:126-159` — Change `written []bool` parameter to `*bitset.BitSet`, use `Test`/`Clear`:

```go
func (s *Server) clearStalePieces(
	ctx context.Context,
	hash string,
	written *bitset.BitSet,
	files []*serverFileInfo,
) {
	for _, fi := range files {
		if !fi.selected || fi.earlyFinalized {
			continue
		}
		if fi.hardlink.state == hlStatePending || fi.hardlink.state == hlStateComplete {
			continue
		}
		if _, err := os.Stat(fi.path); err == nil {
			continue
		}

		cleared := 0
		for p := fi.firstPiece; p <= fi.lastPiece; p++ {
			if written.Test(uint(p)) {
				written.Clear(uint(p))
				cleared++
			}
		}
		if cleared > 0 {
			s.logger.WarnContext(ctx, "cleared stale pieces for missing file",
				"hash", hash,
				"file", fi.path,
				"pieces", cleared,
			)
		}
	}
}
```

- [ ] **Step 5: Add bitset import to persistence.go**

```go
import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/bits-and-blooms/bitset"

	"github.com/arsac/qb-sync/internal/utils"
)
```

Note: The code will not compile yet — callers still pass `[]bool`. That's expected; we fix callers in subsequent tasks.

- [ ] **Step 6: Commit**

```bash
git add internal/destination/persistence.go internal/destination/server.go
git commit -m "refactor(persistence): migrate loadState/saveState to bitset.BitSet"
```

---

### Task 3: Migrate types and state helpers

**Files:**
- Modify: `internal/destination/types.go:89,131-133,262-269`
- Modify: `internal/destination/state.go:95-127,172-211`

- [ ] **Step 1: Update serverTorrentState in types.go**

Line 131-132: Replace `written []bool` and delete `writtenCount`:

```go
written *bitset.BitSet // Piece-level write tracking (protected by mu)
// writtenCount DELETED — use written.Count()
```

Add `"github.com/bits-and-blooms/bitset"` to types.go imports.

- [ ] **Step 2: Update initFilePieceCounts signature**

`types.go:89`:
```go
func (m *torrentMeta) initFilePieceCounts(written *bitset.BitSet) {
```

- [ ] **Step 3: Update recalcPiecesWritten**

`types.go:262-269`:
```go
func (f *serverFileInfo) recalcPiecesWritten(written *bitset.BitSet) {
	f.piecesWritten = 0
	for p := f.firstPiece; p <= f.lastPiece; p++ {
		if written.Test(uint(p)) {
			f.piecesWritten++
		}
	}
}
```

- [ ] **Step 4: Delete countWritten in state.go**

Delete `state.go:202-211` entirely.

- [ ] **Step 5: Update calculatePiecesNeeded**

`state.go:172-184` — Accept `*bitset.BitSet`, return `[]bool` for proto:

```go
func calculatePiecesNeeded(written *bitset.BitSet) ([]bool, int32, int32) {
	n := int(written.Len())
	piecesNeeded := make([]bool, n)
	var needCount, haveCount int32
	for i := range n {
		if written.Test(uint(i)) {
			haveCount++
		} else {
			piecesNeeded[i] = true
			needCount++
		}
	}
	return piecesNeeded, needCount, haveCount
}
```

- [ ] **Step 6: Update countSelectedPiecesTotal**

`state.go:119-127`:
```go
func (s *serverTorrentState) countSelectedPiecesTotal() int {
	count := 0
	for i := uint(0); i < s.written.Len(); i++ {
		if s.classifyPiece(int(i)) != pieceNoSelectedOverlap {
			count++
		}
	}
	return count
}
```

- [ ] **Step 7: Commit**

```bash
git add internal/destination/types.go internal/destination/state.go
git commit -m "refactor(types): migrate written field to *bitset.BitSet, delete writtenCount"
```

---

### Task 4: Migrate write.go (bitset + perf fixes 1 & 2)

This task combines the bitset migration with perf fixes 1 (move writePieceData outside lock) and 2 (remove inline saveState). These all touch the same functions.

**Files:**
- Modify: `internal/destination/write.go:37-149,202-306`

- [ ] **Step 1: Rewrite markPieceWritten — bitset + remove inline flush**

`write.go:37-64`. Key changes: `Set()` instead of `[i]=true`, delete `writtenCount++`, remove inline saveState branch. Keep `dirty` flag and `piecesSinceFlush` for background flusher.

```go
func (s *Server) markPieceWritten(ctx context.Context, hash string, state *serverTorrentState, pieceIndex int32) {
	idx := uint(pieceIndex)
	if idx >= state.written.Len() {
		return
	}
	state.written.Set(idx)
	state.dirty = true
	state.piecesSinceFlush++
	// Inline flush removed — background runStateFlusher handles persistence.
	// The all-pieces-written flush is handled by the caller after checkFileCompletions.
}
```

- [ ] **Step 2: Restructure writePiece — move writePieceData outside lock (perf fix 1)**

`write.go:67-149`. The critical change: do disk I/O BEFORE acquiring `state.mu`. Re-acquire lock only for bitmap bookkeeping.

The key restructure:
1. Early `state.mu.Lock()` check for `already-written` (line 88-93) — stays, uses `written.Test()`
2. Hash verification — stays outside lock (already is)
3. `writePieceData` — MOVE outside the lock (was inside at line 116+)
4. Re-acquire `state.mu.Lock()` for `markPieceWritten` and `checkFileCompletions`
5. Add defensive `pieceIndex < 0` check at top

Update all `state.written[pieceIndex]` → `state.written.Test(uint(pieceIndex))`, and `len(state.written)` → `int(state.written.Len())`.

- [ ] **Step 3: Update earlyFinalizeFile**

`write.go:231-306`. Update `state.written[p] = false; state.writtenCount--` to `state.written.Clear(uint(p))`. Delete `state.writtenCount--`. Use `state.written.Test()` for reads.

- [ ] **Step 4: Update checkFileCompletions**

`write.go:202-222` — No bitset changes needed (doesn't access `written` directly), but verify it still works with the new lock scope.

- [ ] **Step 5: Commit**

```bash
git add internal/destination/write.go
git commit -m "refactor(write): migrate to bitset, move disk I/O outside state.mu, remove inline flush

Perf fix 1: writePieceData now runs outside state.mu, enabling concurrent
NFS writes across stream workers.

Perf fix 2: Inline saveState removed from markPieceWritten. Background
runStateFlusher handles persistence."
```

---

### Task 5: Migrate finalize.go

**Files:**
- Modify: `internal/destination/finalize.go:55,507,757-784`

- [ ] **Step 1: Update FinalizeTorrent completeness check**

Line 55: `writtenCount := state.writtenCount` → `writtenCount := int(state.written.Count())`

- [ ] **Step 2: Update recoverVerificationFailure**

Lines 757-759: `state.written[p] = false; state.writtenCount--` → `state.written.Clear(uint(p))`. Delete `state.writtenCount--`.

Line 769: `s.doSaveState(state.statePath, state.written)` — signature already matches from Task 2.

Line ~784 log: replace `state.writtenCount` with `int(state.written.Count())`.

Line ~821: `fi.recalcPiecesWritten(state.written)` — signature already matches from Task 3.

- [ ] **Step 3: Update flushWrittenState call**

Line 507: `s.saveState(state.statePath, state.written)` — signature already matches from Task 2.

- [ ] **Step 4: Commit**

```bash
git add internal/destination/finalize.go
git commit -m "refactor(finalize): migrate to bitset, delete writtenCount usage"
```

---

### Task 6: Migrate init.go

**Files:**
- Modify: `internal/destination/init.go:196-244,277-316`

- [ ] **Step 1: Update buildWrittenBitmap to return *bitset.BitSet**

`init.go:277-316`. Key changes:
- Return type `[]bool` → `*bitset.BitSet`
- `make([]bool, numPieces)` → `bitset.New(uint(numPieces))`
- `loadState` now returns `*bitset.BitSet` directly
- Replace `for i, covered := range piecesCovered { written[i] = written[i] || covered }` with `written.InPlaceUnion(boolSliceToBitSet(piecesCovered))`
- Log `haveCount` using `written.Count()` instead of `calculatePiecesNeeded`

```go
func (s *Server) buildWrittenBitmap(
	ctx context.Context,
	hash, statePath string,
	meta *torrentMeta,
) *bitset.BitSet {
	numPieces := uint(meta.numPieces())
	piecesCovered := meta.calculatePiecesCovered()

	written := bitset.New(numPieces)
	if existingState, loadErr := s.loadState(statePath, int(numPieces)); loadErr == nil {
		written = existingState
		s.logger.InfoContext(ctx, "resumed torrent state",
			"hash", hash,
			"written", written.Count(),
			"total", numPieces,
		)
	}

	written.InPlaceUnion(boolSliceToBitSet(piecesCovered))

	meta.computeFilePieceRanges()
	for _, fi := range meta.files {
		if fi.skipForWriteData() {
			fi.earlyFinalized = true
		}
	}

	s.clearStalePieces(ctx, hash, written, meta.files)

	meta.initFilePieceCounts(written)

	return written
}
```

- [ ] **Step 2: Update initNewTorrent struct literal**

Line 214: `writtenCount := countWritten(written)` → `writtenCount := int(written.Count())`
Line 235+: Remove `writtenCount: writtenCount` from `serverTorrentState` struct literal (field deleted).

Update line ~218: `if writtenCount > 0` → `if written.Count() > 0` for the state persistence check.

- [ ] **Step 3: Commit**

```bash
git add internal/destination/init.go
git commit -m "refactor(init): migrate buildWrittenBitmap to bitset, use InPlaceUnion"
```

---

### Task 7: Migrate lifecycle.go

**Files:**
- Modify: `internal/destination/lifecycle.go:60-126`

- [ ] **Step 1: Update snapshot pattern**

Lines 81-85: Replace `make([]bool, ...)` + `copy(...)` with `Clone()`:

```go
snapshot := t.state.written.Clone()
```

- [ ] **Step 2: Update countWritten call**

Line 122: `countWritten(snapshot)` → `snapshot.Count()`

- [ ] **Step 3: Update any `state.writtenCount` references in cleanup**

In `server.go:255-261` (cleanup flush): `s.saveState(t.state.statePath, t.state.written)` — signature already matches.

- [ ] **Step 4: Commit**

```bash
git add internal/destination/lifecycle.go internal/destination/server.go
git commit -m "refactor(lifecycle): migrate snapshot to bitset.Clone()"
```

---

### Task 8: Bump metaVersion and update config

**Files:**
- Modify: `internal/destination/config.go:27`

- [ ] **Step 1: Bump metaVersion**

```go
metaVersion = "2"
```

- [ ] **Step 2: Commit**

```bash
git add internal/destination/config.go
git commit -m "feat: bump metaVersion to 2 for bitset state format"
```

---

### Task 9: Update all test files for bitset

**Files:**
- Modify: `internal/destination/lifecycle_test.go` (~7 occurrences)
- Modify: `internal/destination/early_finalize_test.go` (~14 occurrences)
- Modify: `internal/destination/finalize_test.go` (~14 occurrences)
- Modify: `internal/destination/selection_test.go` (~6 occurrences)
- Modify: `internal/destination/server_test.go` (~4 occurrences)

- [ ] **Step 1: Update lifecycle_test.go**

Replace all `state.written = make([]bool, N)` → `state.written = bitset.New(N)`,
`state.written[i] = true` → `state.written.Set(uint(i))`,
`state.writtenCount++` → delete,
`saveStateFunc` lambda signatures.

- [ ] **Step 2: Update early_finalize_test.go**

Same pattern. Also update any `state.written[i]` reads → `state.written.Test(uint(i))`.
Delete all `state.writtenCount` references.

- [ ] **Step 3: Update finalize_test.go**

Same pattern. Update `saveStateFunc` lambdas, `state.written` access, delete `writtenCount`.

- [ ] **Step 4: Update selection_test.go**

Update `clearStalePieces` test to use bitset. Update struct literals.

- [ ] **Step 5: Update server_test.go**

Update `saveStateFunc` lambda signatures and `savedWritten` type.

- [ ] **Step 6: Verify all destination tests pass**

```bash
go test ./internal/destination/... -short -count=1 -v
```

- [ ] **Step 7: Commit**

```bash
git add internal/destination/*_test.go
git commit -m "test: update all destination tests for bitset migration"
```

---

### Task 10: Perf fix 3 — Increase workCh buffer

**Files:**
- Modify: `internal/destination/streaming.go:28`

- [ ] **Step 1: Increase buffer**

```go
workCh := make(chan *pb.WritePieceRequest, numWorkers*2)
```

- [ ] **Step 2: Commit**

```bash
git add internal/destination/streaming.go
git commit -m "perf(streaming): increase workCh buffer to 2x workers

Allows the gRPC receiver to stay ahead of NFS write latency, keeping
the network link busy while workers are blocked on disk I/O."
```

---

### Task 11: Perf fix 4 — Reduce piece poll interval

**Files:**
- Modify: `internal/streaming/piece_monitor.go:25`

- [ ] **Step 1: Reduce interval**

```go
defaultPollInterval = 500 * time.Millisecond
```

- [ ] **Step 2: Update any tests that assert on the old 2s value**

Check `piece_monitor_test.go` for hardcoded 2s expectations.

- [ ] **Step 3: Commit**

```bash
git add internal/streaming/piece_monitor.go
git commit -m "perf(monitor): reduce piece poll interval from 2s to 500ms

Reduces piece dispatch latency from up to 2s to 500ms. The idleSlowFactor
already backs off for idle torrents. Cost: one GetPieceStates HTTP call
per active torrent per 500ms (cheap vector-of-uint8 response)."
```

---

### Task 12: Perf fix 5 — Acquire window slot before disk read

**Files:**
- Modify: `internal/streaming/streaming_queue.go:467-548` (sendPiecePool)
- Modify: `internal/streaming/stream_pool.go` (add TryAcquireSlot)

- [ ] **Step 1: Restructure sendPiecePool**

Move window slot acquisition before `source.ReadPiece`. The key change is reordering: acquire window → read disk → send.

Read the full `sendPiecePool` function and `StreamPool.SelectStream` / `TrySend` to understand the exact restructure needed. The stream selection and window acquisition should happen as one atomic step before the expensive disk read.

- [ ] **Step 2: Run streaming tests**

```bash
go test ./internal/streaming/... -short -count=1
```

- [ ] **Step 3: Commit**

```bash
git add internal/streaming/streaming_queue.go internal/streaming/stream_pool.go
git commit -m "perf(streaming): acquire window slot before disk read

Prevents wasting NFS reads when the congestion window is full. Previously
the piece was read from disk first, then the window slot was checked —
if full, the read was wasted."
```

---

### Task 13: Perf fix 6 — Raise initial congestion window

**Files:**
- Modify: `internal/congestion/window.go:15`
- Modify: `internal/congestion/window_test.go` (if it asserts on old value)

- [ ] **Step 1: Raise initial window**

```go
DefaultInitialWindow = 512
```

- [ ] **Step 2: Update tests that assert on the old value (64)**

Check `window_test.go` for `DefaultInitialWindow` assertions and update.

- [ ] **Step 3: Run congestion tests**

```bash
go test ./internal/congestion/... -short -count=1
```

- [ ] **Step 4: Commit**

```bash
git add internal/congestion/window.go internal/congestion/window_test.go
git commit -m "perf(congestion): raise initial window from 64 to 512

On a home LAN with sub-ms RTT, the old 64-piece initial window caused
multi-second ramp-up after reconnects. CUBIC OnFail still reduces
immediately on any loss event."
```

---

### Task 14: Full test suite + lint + e2e

- [ ] **Step 1: Run full unit tests**

```bash
go test ./internal/... -short -count=1
```

Expected: All pass.

- [ ] **Step 2: Run linter**

```bash
golangci-lint run --fix
```

Expected: 0 issues.

- [ ] **Step 3: Vet e2e**

```bash
go vet -tags=e2e ./test/e2e/...
```

Expected: Clean.

- [ ] **Step 4: Run e2e tests**

```bash
go test -tags=e2e -parallel 4 -timeout 30m ./test/e2e/...
```

Expected: All pass. The metaVersion bump causes all existing state to be nuked — torrents re-stream from scratch, exercising the fresh init path.

- [ ] **Step 5: Fix any failures, re-run**

Iterate until all green.

- [ ] **Step 6: Final commit if any fixups needed**

```bash
git add -A
git commit -m "fix: address test/lint issues from bitset migration"
```
