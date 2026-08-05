# Two-Stage Finalization Implementation Plan

> **Historical record.** This plan describes work that has already shipped. It is kept for context on why the code looks the way it does; the checkboxes reflect the original build order and are not outstanding work.

**Goal:** Remove the destination finalization throughput ceiling by splitting the single-flight finalize semaphore into a disk stage and a qB stage, fixing the 30-min/6-h timeout incoherence, and making congestion (queue/recheck timeouts) stop burning the source's sync-failed retry budget via a new `FINALIZE_ERROR_BUSY` code.

**Architecture:** Destination `runBackgroundFinalization` becomes two sequential stages in the same `bgWg`-tracked goroutine: a disk stage (weight-1 semaphore: dir sync → piece verify → inode registration) and a qB stage (new configurable-weight semaphore, default 1: AddTorrent → recheck wait → stop → marker). Disk-stage completion is recorded in-memory (`diskStageDone`) so retries skip re-verification. The qB-stage context budget is `2×computePollTimeout(totalSize) + 5min margin` (two `waitForTorrentReady` calls: initial + post-recheck). Congestion outcomes (`queue timeout`, `qB still checking at budget expiry`) return `FINALIZE_ERROR_BUSY`; the source treats BUSY like "still verifying" (no retry-budget burn) with an 8-hour wall-clock guard.

**Design review:** validated by skeptic / constraint-guardian / user-advocate / arbiter (disposition: APPROVED after two amendments — `diskStageDone` flag and 2× poll budget — both included below).

**Tech Stack:** Go, gRPC/protobuf (buf codegen), Prometheus (promauto), Cobra/Viper, golangci-lint v2.7.1, lefthook.

**Deploy order note (must land in docs):** destination must be deployed before source for BUSY to take effect. Old source + new destination degrades to today's behavior (no regression).

---

## File Map

| File | Change |
|---|---|
| `proto/qbsync.proto` | Add `FINALIZE_ERROR_BUSY = 3` to `FinalizeErrorCode` |
| `proto/*.pb.go` | Regenerated via `buf generate` |
| `internal/streaming/grpc_destination.go` | `ErrFinalizeBusy` sentinel + mapping in `FinalizeTorrent` |
| `internal/source/backoff.go` | Busy-streak tracking (`firstBusy`, `RecordBusy`, injectable `now`) |
| `internal/source/orchestrator.go` | `busyGuardDuration` constant |
| `internal/source/finalization.go` | BUSY branch in `handleFinalizeError` |
| `internal/metrics/metrics.go` | Queue-wait + stage-duration histograms, queue-depth gauge, busy counter, extended `FinalizationDuration` buckets, new label constants |
| `METRICS.md` | Document all metric changes + alert candidate |
| `internal/destination/config.go` | `diskStageTimeout` (renamed), `defaultQBStageTimeoutMargin`, `maxQBFinalizeConcurrency`, `ServerConfig.QBFinalizeConcurrency` + validation |
| `internal/destination/types.go` | `finalizationState.diskStageDone` |
| `internal/destination/server.go` | `qbStageSem`, `finalizeQueueWait` test-override field |
| `internal/destination/finalize.go` | Two-stage `runBackgroundFinalization`, `acquireStageSlot`, BUSY classification, `diskStageDone` invalidation on relocate |
| `internal/destination/qbittorrent.go` | `qbStageTimeout`, `isBusyWaitError` |
| `internal/destination/helpers_test.go` | Init `qbStageSem` in test server |
| `internal/config/config.go` | `qb-finalize-concurrency` flag + bind + load + validate |
| `cmd/qbsync/main.go` | Wire `QBFinalizeConcurrency`, startup log |
| `README.md` | Upgrade note (deploy order, new knob) |

Tests: `internal/source/backoff_test.go` (may need creation), `internal/source/orchestrator_test.go`, `internal/destination/qbittorrent_test.go`, `internal/destination/finalize_test.go`, `internal/config/config_test.go`.

---

### Task 1: Proto — add FINALIZE_ERROR_BUSY

**Files:**
- Modify: `proto/qbsync.proto:49-53`
- Regenerate: `proto/qbsync.pb.go` (via `buf generate`)

- [ ] **Step 1: Edit the enum**

In `proto/qbsync.proto`, change:

```proto
// FinalizeErrorCode categorizes finalization failures for retry decisions.
enum FinalizeErrorCode {
  FINALIZE_ERROR_NONE = 0;       // Default / unknown
  FINALIZE_ERROR_INCOMPLETE = 1; // Not all pieces written
  FINALIZE_ERROR_NOT_FOUND = 2;  // Torrent state not found or stale (data files missing)
  FINALIZE_ERROR_BUSY = 3;       // Destination congested (finalize queue timeout, or qB still checking at budget expiry); retry without penalty
}
```

- [ ] **Step 2: Regenerate**

Run: `buf generate`
Expected: exit 0; `proto/qbsync.pb.go` diff contains `FinalizeErrorCode_FINALIZE_ERROR_BUSY FinalizeErrorCode = 3`.

- [ ] **Step 3: Build**

Run: `go build ./...`
Expected: success.

- [ ] **Step 4: Commit**

```bash
git add proto/
git commit -m "feat(proto): add FINALIZE_ERROR_BUSY for congestion-vs-failure distinction"
```

---

### Task 2: Streaming client — ErrFinalizeBusy sentinel

**Files:**
- Modify: `internal/streaming/grpc_destination.go:56-70` (sentinel block) and `:388-400` (switch in `FinalizeTorrent`)
- Test: `internal/streaming/grpc_destination_test.go` (add to existing file if present; create the test alongside existing streaming tests otherwise)

- [ ] **Step 1: Write the failing test**

Find the existing tests for `FinalizeTorrent` response mapping (`grep -rn "ErrFinalizeIncomplete" internal/streaming/*_test.go`). Follow the same pattern; if the existing tests exercise the error-code switch via a fake `pb.FinalizeTorrentResponse`, mirror that. If no direct unit test of the switch exists, add this minimal test of the sentinel-and-wrap behavior (the switch itself is exercised in Task 4's source-level test):

```go
func TestErrFinalizeBusyIsDistinct(t *testing.T) {
	wrapped := fmt.Errorf("%w: finalization queue timeout", ErrFinalizeBusy)
	if !errors.Is(wrapped, ErrFinalizeBusy) {
		t.Fatal("wrapped busy error must match ErrFinalizeBusy")
	}
	if errors.Is(wrapped, ErrFinalizeVerifying) || errors.Is(wrapped, ErrFinalizeIncomplete) {
		t.Fatal("busy must not match other finalize sentinels")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/streaming/ -run TestErrFinalizeBusy -count=1`
Expected: FAIL — `undefined: ErrFinalizeBusy`.

- [ ] **Step 3: Implement**

In the sentinel block at `internal/streaming/grpc_destination.go` (after `ErrFinalizeNotFound`):

```go
	// ErrFinalizeBusy is returned by FinalizeTorrent when the destination is
	// congested: the finalization queue timed out, or destination qBittorrent
	// was still rechecking when the wait budget expired. The caller should
	// retry later without counting this toward the per-torrent failure cap —
	// congestion is destination-wide, not a per-torrent fault.
	ErrFinalizeBusy = errors.New("finalization deferred: destination busy")
```

In the `switch resp.GetErrorCode()` inside `FinalizeTorrent` (before `default:`):

```go
		case pb.FinalizeErrorCode_FINALIZE_ERROR_BUSY:
			// Destination congestion (queue saturated or qB still checking).
			// Sentinel lets the orchestrator poll again without penalty.
			return fmt.Errorf("%w: %s", ErrFinalizeBusy, resp.GetError())
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/streaming/ -run TestErrFinalizeBusy -count=1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/streaming/
git commit -m "feat(streaming): map FINALIZE_ERROR_BUSY to ErrFinalizeBusy sentinel"
```

---

### Task 3: Source — busy-streak tracking in BackoffTracker

**Files:**
- Modify: `internal/source/backoff.go`
- Test: `internal/source/backoff_test.go` (check whether it exists: `ls internal/source/backoff_test.go`; create if missing)

- [ ] **Step 1: Write the failing tests**

```go
package source

import (
	"testing"
	"time"
)

func TestRecordBusyTracksStreakDuration(t *testing.T) {
	b := NewBackoffTracker()
	now := time.Now()
	b.now = func() time.Time { return now }

	if d := b.RecordBusy("h1"); d != 0 {
		t.Fatalf("first busy must report zero elapsed, got %v", d)
	}

	now = now.Add(3 * time.Hour)
	if d := b.RecordBusy("h1"); d != 3*time.Hour {
		t.Fatalf("expected 3h busy streak, got %v", d)
	}
}

func TestRecordBusyDoesNotAffectFailureCapOrBackoff(t *testing.T) {
	b := NewBackoffTracker()
	b.RecordBusy("h1")

	// Busy must not create backoff delay: ShouldAttempt stays true.
	if !b.ShouldAttempt("h1") {
		t.Fatal("busy streak must not delay finalize attempts")
	}
	// Busy must not count as a failure.
	if got := b.RecordFailure("h1"); got != 1 {
		t.Fatalf("first real failure after busy must be 1, got %d", got)
	}
}

func TestClearResetsBusyStreak(t *testing.T) {
	b := NewBackoffTracker()
	now := time.Now()
	b.now = func() time.Time { return now }

	b.RecordBusy("h1")
	b.Clear("h1")

	now = now.Add(10 * time.Hour)
	if d := b.RecordBusy("h1"); d != 0 {
		t.Fatalf("Clear must reset the busy streak, got %v", d)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/source/ -run "TestRecordBusy|TestClearResetsBusy" -count=1`
Expected: FAIL — `undefined: b.now`, `b.RecordBusy`.

- [ ] **Step 3: Implement**

In `internal/source/backoff.go`:

```go
// finalizeBackoff tracks exponential backoff state for finalization retries.
type finalizeBackoff struct {
	failures    int
	lastAttempt time.Time
	firstBusy   time.Time // Start of a continuous BUSY streak; zero when not busy
}

// BackoffTracker manages exponential backoff for finalization retries.
// Thread-safe with internal locking.
type BackoffTracker struct {
	backoffs map[string]*finalizeBackoff
	mu       sync.Mutex
	now      func() time.Time // Injectable for tests
}

// NewBackoffTracker creates a new BackoffTracker.
func NewBackoffTracker() *BackoffTracker {
	return &BackoffTracker{
		backoffs: make(map[string]*finalizeBackoff),
		now:      time.Now,
	}
}

// RecordBusy notes a BUSY (destination congested) response and returns how
// long this hash has been continuously busy. Busy streaks do not count toward
// the failure cap and do not delay attempts (lastAttempt is untouched); the
// streak resets on Clear (success) or naturally when the entry is removed.
func (b *BackoffTracker) RecordBusy(hash string) time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()

	backoff, exists := b.backoffs[hash]
	if !exists {
		backoff = &finalizeBackoff{}
		b.backoffs[hash] = backoff
	}
	if backoff.firstBusy.IsZero() {
		backoff.firstBusy = b.now()
	}
	return b.now().Sub(backoff.firstBusy)
}
```

(Leave `ShouldAttempt`, `RecordFailure`, `Clear`, `Count` unchanged — `Clear` already deletes the whole entry, which resets `firstBusy`.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/source/ -run "TestRecordBusy|TestClearResetsBusy" -count=1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/source/backoff.go internal/source/backoff_test.go
git commit -m "feat(source): track BUSY streak duration in BackoffTracker"
```

---

### Task 4: Source — BUSY branch in handleFinalizeError

**Files:**
- Modify: `internal/source/orchestrator.go` (constant, near `maxVerificationRetries` at :35-37)
- Modify: `internal/source/finalization.go:56-97` (`handleFinalizeError`)
- Test: `internal/source/orchestrator_test.go` (alongside `TestHandleFinalizeError_DefaultBranchCapsAtMaxRetries` at :2914)

- [ ] **Step 1: Write the failing test**

Add to `internal/source/orchestrator_test.go`, reusing the `newCapTask` helper pattern from `TestHandleFinalizeError_DefaultBranchCapsAtMaxRetries` (copy the helper into the new test function — it is function-local):

```go
func TestHandleFinalizeError_BusyDoesNotBurnRetryBudget(t *testing.T) {
	logger := testLogger(t)

	newBusyTask := func(hash, name string) (*QBTask, *mockQBClient) {
		mockClient := &mockQBClient{}
		task := &QBTask{
			cfg:       &config.SourceConfig{SyncFailedTag: "sync-failed"},
			logger:    logger,
			srcClient: mockClient,
			grpcDest:  &mockDest{},
			source:    qbclient.NewSource(nil, ""),
			tracker: streaming.NewPieceMonitor(
				nil, &mockPieceSource{numPieces: 1}, logger, streaming.DefaultPieceMonitorConfig(),
			),
			tracked:  NewTrackedSet(),
			backoffs: NewBackoffTracker(),
		}
		task.tracked.Add(hash, TrackedTorrent{Name: name})
		return task, mockClient
	}

	t.Run("BUSY within guard never counts toward the cap", func(t *testing.T) {
		hash := "busy-hash"
		task, mockClient := newBusyTask(hash, "busy-torrent")

		busyErr := fmt.Errorf("%w: finalization queue timeout", streaming.ErrFinalizeBusy)

		for range maxVerificationRetries * 5 {
			task.handleFinalizeError(context.Background(), hash, busyErr)
		}

		if !task.tracked.Has(hash) {
			t.Error("BUSY must not untrack the torrent")
		}
		if mockClient.addTagsCalled {
			t.Error("BUSY must not trigger sync-failed tagging")
		}
	})

	t.Run("BUSY beyond the wall-clock guard counts as failure", func(t *testing.T) {
		hash := "wedged-hash"
		task, mockClient := newBusyTask(hash, "wedged-torrent")

		// Simulate a streak that started busyGuardDuration+1h ago.
		now := time.Now()
		task.backoffs.now = func() time.Time { return now }
		task.backoffs.RecordBusy(hash) // starts streak at `now`
		task.backoffs.now = func() time.Time { return now.Add(busyGuardDuration + time.Hour) }

		busyErr := fmt.Errorf("%w: finalization queue timeout", streaming.ErrFinalizeBusy)

		for range maxVerificationRetries {
			task.handleFinalizeError(context.Background(), hash, busyErr)
		}

		if task.tracked.Has(hash) {
			t.Errorf("torrent must be untracked after %d post-guard BUSY failures", maxVerificationRetries)
		}
		if mockClient.addTagsTag != "sync-failed" {
			t.Errorf("expected sync-failed tag after guard expiry; got %q", mockClient.addTagsTag)
		}
	})
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/source/ -run TestHandleFinalizeError_BusyDoesNotBurnRetryBudget -count=1`
Expected: FAIL — `undefined: busyGuardDuration` (and, once that compiles, the first subtest fails because BUSY currently falls into the generic `RecordFailure` path).

- [ ] **Step 3: Implement**

In `internal/source/orchestrator.go`, next to `maxVerificationRetries`:

```go
	// busyGuardDuration bounds how long BUSY (destination congested) responses
	// are tolerated per torrent before they start counting toward
	// maxVerificationRetries. Must exceed the destination's worst-case budget:
	// finalizeQueueTimeout (2h) + qB-stage budget (up to ~2×6h poll cap).
	// In-memory only — a source restart resets the streak (acceptable: the
	// guard catches permanently wedged destinations, not crash recovery).
	busyGuardDuration = 8 * time.Hour
```

In `internal/source/finalization.go`, at the top of `handleFinalizeError` (before the existing `switch`):

```go
	// BUSY = destination-wide congestion (finalize queue saturated, or qB
	// still rechecking at budget expiry) — not a per-torrent fault. Poll
	// again without burning the retry budget, but bound it with a wall-clock
	// guard so a permanently wedged destination still surfaces as sync-failed.
	if errors.Is(finalizeErr, streaming.ErrFinalizeBusy) {
		if busyFor := t.backoffs.RecordBusy(hash); busyFor < busyGuardDuration {
			t.logger.WarnContext(ctx, "destination finalization busy, will retry",
				"hash", hash,
				"busyFor", busyFor.Round(time.Second),
				"guard", busyGuardDuration,
			)
			return false
		}
		t.logger.ErrorContext(ctx, "destination busy beyond wall-clock guard, counting as failure",
			"hash", hash,
			"guard", busyGuardDuration,
		)
		// Fall through to the generic failure accounting below.
	}
```

Note: BUSY errors are plain wrapped errors (no gRPC status), so the later `IsTransientError` check stays false and post-guard BUSY reaches `RecordFailure` — that is intentional.

- [ ] **Step 4: Run tests — new and existing**

Run: `go test ./internal/source/ -run "TestHandleFinalizeError" -count=1`
Expected: PASS (including the pre-existing `TestHandleFinalizeError_DefaultBranchCapsAtMaxRetries`, which is unaffected because its errors don't match `ErrFinalizeBusy`).

- [ ] **Step 5: Commit**

```bash
git add internal/source/
git commit -m "feat(source): treat FINALIZE_ERROR_BUSY as congestion with 8h wall-clock guard"
```

---

### Task 5: Metrics — new instruments + METRICS.md

**Files:**
- Modify: `internal/metrics/metrics.go` (label constants block near top; `FinalizationDuration` at :807-816; new instruments after it)
- Modify: `METRICS.md`

- [ ] **Step 1: Add label/value constants**

In the constants block of `internal/metrics/metrics.go` (where `LabelResult`/`LabelMode`/`LabelOperation` live — find with `grep -n "LabelResult " internal/metrics/metrics.go`):

```go
	// LabelStage distinguishes finalization stages.
	LabelStage = "stage"
	// LabelReason distinguishes BUSY causes.
	LabelReason = "reason"

	// StageDisk is the disk-bound finalization stage (verify + inode registration).
	StageDisk = "disk"
	// StageQB is the qBittorrent integration stage (add + recheck wait).
	StageQB = "qb"

	// ReasonQueueTimeout marks BUSY caused by a stage-queue timeout.
	ReasonQueueTimeout = "queue_timeout"
	// ReasonQBChecking marks BUSY caused by qB still checking at budget expiry.
	ReasonQBChecking = "qb_checking"
```

- [ ] **Step 2: Extend FinalizationDuration buckets and add new instruments**

Replace the `FinalizationDuration` buckets (currently topping out at 120 — wrong since queue wait alone can reach 2h) and add the new instruments right after it:

```go
	// FinalizationDuration tracks the total time to finalize a torrent,
	// INCLUDING stage-queue wait. Use FinalizeStageDuration for work-only time.
	FinalizationDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "finalization_duration_seconds",
			Help:      "Total time to finalize a torrent, including queue wait",
			Buckets:   []float64{1, 5, 15, 60, 300, 900, 1800, 3600, 7200, 21600, 43200},
		},
		[]string{LabelResult},
	)

	// FinalizeQueueWaitSeconds tracks time spent waiting for a finalization stage slot.
	FinalizeQueueWaitSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "finalize_queue_wait_seconds",
			Help:      "Time a finalization waited for a stage slot",
			Buckets:   []float64{1, 5, 15, 60, 300, 900, 1800, 3600, 7200},
		},
		[]string{LabelStage},
	)

	// FinalizeStageDuration tracks per-stage finalization work time (excludes queue wait).
	FinalizeStageDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "finalize_stage_duration_seconds",
			Help:      "Finalization stage work time, excluding queue wait",
			Buckets:   []float64{1, 5, 15, 60, 300, 900, 1800, 3600, 7200, 21600},
		},
		[]string{LabelStage, LabelResult},
	)

	// FinalizationQueueDepth tracks torrents currently waiting for a stage slot.
	FinalizationQueueDepth = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "finalization_queue_depth",
			Help:      "Torrents currently waiting for a finalization stage slot",
		},
		[]string{LabelStage},
	)

	// FinalizeBusyTotal counts BUSY responses returned to the source.
	FinalizeBusyTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "finalize_busy_total",
			Help:      "BUSY (congestion) responses returned to source, by reason",
		},
		[]string{LabelReason},
	)
```

- [ ] **Step 3: Build**

Run: `go build ./...`
Expected: success.

- [ ] **Step 4: Update METRICS.md**

Follow the file's existing table format. Required edits:
1. Update the `qbsync_finalization_duration_seconds` row: note it includes queue wait; new bucket range.
2. Add rows for `qbsync_finalize_queue_wait_seconds` (histogram, label `stage`: `disk|qb`), `qbsync_finalize_stage_duration_seconds` (histogram, labels `stage`,`result`), `qbsync_finalization_queue_depth` (gauge, label `stage`), `qbsync_finalize_busy_total` (counter, label `reason`: `queue_timeout|qb_checking`).
3. Add an alert candidate: `rate(qbsync_finalize_busy_total[10m]) > 0` sustained for >30 min ⇒ destination finalization saturated (torrents are waiting, NOT failed — no sync-failed tag will appear).
4. If METRICS.md has an `qbsync_active_finalization_backoffs` alert note, add the caveat that BUSY torrents do not appear in it; use `finalize_busy_total` instead.

- [ ] **Step 5: Commit**

```bash
git add internal/metrics/metrics.go METRICS.md
git commit -m "feat(metrics): per-stage finalization instrumentation and busy counter"
```

---

### Task 6: Destination — constants, config knob, timeout/classification helpers

**Files:**
- Modify: `internal/destination/config.go`
- Modify: `internal/destination/qbittorrent.go`
- Test: `internal/destination/qbittorrent_test.go`

- [ ] **Step 1: Write the failing tests**

Add to `internal/destination/qbittorrent_test.go`:

```go
func TestQBStageTimeout(t *testing.T) {
	const gb = int64(1024 * 1024 * 1024)

	tests := []struct {
		name      string
		totalSize int64
		qbCfg     *QBConfig
		want      time.Duration
	}{
		{
			name:      "small torrent uses 2x base floor plus margin",
			totalSize: 1 * gb,
			want:      2*(defaultQBPollTimeoutBase+1*defaultQBPollTimeoutPerGB) + defaultQBStageTimeoutMargin,
		},
		{
			name:      "huge torrent capped at 2x max plus margin",
			totalSize: 1000 * gb,
			want:      2*defaultQBPollTimeoutMax + defaultQBStageTimeoutMargin,
		},
		{
			name:      "explicit PollTimeout override is doubled plus margin",
			totalSize: 1000 * gb,
			qbCfg:     &QBConfig{PollTimeout: 10 * time.Minute},
			want:      2*(10*time.Minute) + defaultQBStageTimeoutMargin,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{config: ServerConfig{QB: tt.qbCfg}}
			if got := s.qbStageTimeout(tt.totalSize); got != tt.want {
				t.Errorf("qbStageTimeout(%d) = %v, want %v", tt.totalSize, got, tt.want)
			}
		})
	}
}

func TestIsBusyWaitError(t *testing.T) {
	tests := []struct {
		name       string
		finalState qbittorrent.TorrentState
		err        error
		want       bool
	}{
		{"timeout while checking is busy", qbittorrent.TorrentStateCheckingUp, utils.ErrTimeout, true},
		{"deadline while checking is busy", qbittorrent.TorrentStateCheckingDl, context.DeadlineExceeded, true},
		{"timeout in error state is not busy", qbittorrent.TorrentStateMissingFiles, utils.ErrTimeout, false},
		{"error-state failure is not busy", qbittorrent.TorrentStateError, errors.New("torrent in error state: error"), false},
		{"timeout in stalled state is not busy", qbittorrent.TorrentStateStalledDl, utils.ErrTimeout, false},
		{"nil error is not busy", qbittorrent.TorrentStateCheckingUp, nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isBusyWaitError(tt.finalState, tt.err); got != tt.want {
				t.Errorf("isBusyWaitError(%q, %v) = %v, want %v", tt.finalState, tt.err, got, tt.want)
			}
		})
	}
}

func TestQBFinalizeConcurrencyValidation(t *testing.T) {
	base := ServerConfig{BasePath: "/tmp/x", ListenAddr: ":1"}

	cfg := base
	cfg.QBFinalizeConcurrency = 9
	if err := cfg.Validate(); err == nil {
		t.Error("concurrency above the cap must fail validation")
	}

	cfg = base
	cfg.QBFinalizeConcurrency = -1
	if err := cfg.Validate(); err == nil {
		t.Error("negative concurrency must fail validation")
	}

	cfg = base
	cfg.QBFinalizeConcurrency = 0
	if err := cfg.Validate(); err != nil {
		t.Errorf("zero (default) must validate: %v", err)
	}
	if got := cfg.GetQBFinalizeConcurrency(); got != 1 {
		t.Errorf("zero must normalize to 1, got %d", got)
	}
}
```

(Check the test file's existing imports; add `time`, `context`, `errors`, `github.com/autobrr/go-qbittorrent`, `github.com/arsac/qb-sync/internal/utils` as needed.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/destination/ -run "TestQBStageTimeout|TestIsBusyWaitError|TestQBFinalizeConcurrencyValidation" -count=1`
Expected: FAIL — undefined `defaultQBStageTimeoutMargin`, `qbStageTimeout`, `isBusyWaitError`, `QBFinalizeConcurrency`.

- [ ] **Step 3: Implement config changes**

In `internal/destination/config.go`:

Rename the constant `backgroundFinalizeTimeout` → `diskStageTimeout` (update the doc comment — it now bounds only the disk stage; the 60s idle watchdog catches stalls, this cap catches slow-but-continuous verification):

```go
	// diskStageTimeout is the upper-bound timeout for the disk-bound
	// finalization stage (parent-dir sync + piece verification + inode
	// registration). Complementary to verifyIdleTimeout: the watchdog catches
	// a stalled verifier; this cap catches slow-but-continuous progress.
	// Starts after the disk-stage semaphore is acquired.
	diskStageTimeout = 30 * time.Minute

	// defaultQBStageTimeoutMargin pads the qB-stage budget beyond the two
	// waitForTorrentReady poll budgets (initial + post-recheck). Covers
	// AddTorrent itself, login retries, and the partial-selection
	// priority-verify loop (priorityVerifyTimeout) — none of which are part
	// of the poll budget.
	defaultQBStageTimeoutMargin = 5 * time.Minute

	// maxQBFinalizeConcurrency caps the qB-stage semaphore weight. Above this,
	// concurrent qB rechecks compete for disk I/O and the API burst rate can
	// trip the qbclient circuit breaker (5 failures / 30s).
	maxQBFinalizeConcurrency = 8
```

Add to `ServerConfig`:

```go
	// QBFinalizeConcurrency is how many torrents may concurrently occupy the
	// qBittorrent integration stage (add + recheck wait). 0 = default 1.
	// Values >1 increase destination qB API and disk load; raising it is only
	// recommended on SSD-backed storage. Capped at maxQBFinalizeConcurrency.
	QBFinalizeConcurrency int
```

Add the getter and extend `Validate()`:

```go
// GetQBFinalizeConcurrency returns the qB-stage semaphore weight, defaulting to 1.
func (c *ServerConfig) GetQBFinalizeConcurrency() int {
	if c.QBFinalizeConcurrency <= 0 {
		return 1
	}
	return c.QBFinalizeConcurrency
}
```

In `Validate()` (after the orphan-timeout check):

```go
	if c.QBFinalizeConcurrency < 0 || c.QBFinalizeConcurrency > maxQBFinalizeConcurrency {
		return fmt.Errorf("qb finalize concurrency must be between 0 and %d (0 = default 1)", maxQBFinalizeConcurrency)
	}
```

- [ ] **Step 4: Implement the helpers**

In `internal/destination/qbittorrent.go` (near `computePollTimeout`):

```go
// qbStageTimeout bounds the qB integration stage. addAndVerifyTorrent can run
// waitForTorrentReady twice (initial wait + post-error-state recheck wait), so
// the budget is two poll timeouts plus a margin for AddTorrent, login retries,
// and the partial-selection priority-verify loop.
func (s *Server) qbStageTimeout(totalSize int64) time.Duration {
	poll := computePollTimeout(totalSize)
	if s.config.QB != nil && s.config.QB.PollTimeout > 0 {
		poll = s.config.QB.PollTimeout
	}
	return 2*poll + defaultQBStageTimeoutMargin
}

// isBusyWaitError reports whether a qB-stage failure is congestion — qB was
// still actively checking when the wait budget expired — rather than a real
// failure. Error states and non-timeout failures are genuine.
func isBusyWaitError(finalState qbittorrent.TorrentState, err error) bool {
	if err == nil {
		return false
	}
	timedOut := errors.Is(err, utils.ErrTimeout) || errors.Is(err, context.DeadlineExceeded)
	return timedOut && isCheckingState(finalState)
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./internal/destination/ -run "TestQBStageTimeout|TestIsBusyWaitError|TestQBFinalizeConcurrencyValidation" -count=1`
Expected: PASS. (The rename will break `finalize.go` compilation only in Task 7's territory — if `go build ./...` fails on `backgroundFinalizeTimeout`, update the single reference at `finalize.go:170` to `diskStageTimeout` now; Task 7 restructures it anyway.)

- [ ] **Step 6: Commit**

```bash
git add internal/destination/config.go internal/destination/qbittorrent.go internal/destination/qbittorrent_test.go internal/destination/finalize.go
git commit -m "feat(destination): qB-stage timeout derivation, BUSY classification, concurrency knob"
```

---

### Task 7: Destination — diskStageDone flag

**Files:**
- Modify: `internal/destination/types.go:159-191` (`finalizationState`)
- Modify: `internal/destination/finalize.go` (`FinalizeTorrent` relocation branch at :74-78)
- Test: `internal/destination/finalize_test.go`

- [ ] **Step 1: Write the failing test**

```go
func TestFinalizationStateDiskStageDoneSurvivesReset(t *testing.T) {
	var f finalizationState

	f.start()
	f.diskStageDone = true
	f.storeResult(&finalizeResult{err: "queue timeout", errorCode: pb.FinalizeErrorCode_FINALIZE_ERROR_BUSY})
	f.reset()

	if f.active || f.result != nil || f.done != nil {
		t.Error("reset must clear the active/result/done lifecycle fields")
	}
	if !f.diskStageDone {
		t.Error("diskStageDone must survive reset so retries skip re-verification")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/destination/ -run TestFinalizationStateDiskStageDone -count=1`
Expected: FAIL — `unknown field diskStageDone`.

- [ ] **Step 3: Implement**

In `internal/destination/types.go`, add the field to `finalizationState`:

```go
type finalizationState struct {
	active bool            // True during FinalizeTorrent to prevent concurrent writes
	done   chan struct{}   // Closed when background finalization completes
	result *finalizeResult // Result of background finalization (nil = not started)

	// diskStageDone records that the disk stage (sync + verify + inode
	// registration) completed for the current on-disk file layout. It
	// intentionally SURVIVES reset() so a retry after a qB-stage failure
	// skips straight to the qB stage instead of re-reading every piece.
	// Invalidated when files move (relocateForSubPathChange). In-memory only:
	// a destination restart re-verifies once, which is safe.
	diskStageDone bool
}
```

Do NOT touch `reset()` — it already only clears `active`/`result`/`done`.

In `internal/destination/finalize.go`, inside the `FinalizeTorrent` relocation branch (the `if newSubPath := req.GetSaveSubPath(); ...` block), after a successful `relocateForSubPathChange`:

```go
	if newSubPath := req.GetSaveSubPath(); req.GetSaveSubPathExplicit() && newSubPath != state.saveSubPath {
		if relocErr := s.relocateForSubPathChange(ctx, hash, state, newSubPath); relocErr != nil {
			return failureResponse(relocErr.Error(), pb.FinalizeErrorCode_FINALIZE_ERROR_NONE), nil
		}
		// Files moved: prior disk-stage results (verified paths, registered
		// inode paths) are stale. Force the disk stage to re-run.
		state.mu.Lock()
		state.finalization.diskStageDone = false
		state.mu.Unlock()
	}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/destination/ -run TestFinalizationStateDiskStageDone -count=1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/destination/types.go internal/destination/finalize.go internal/destination/finalize_test.go
git commit -m "feat(destination): record disk-stage completion to skip re-verification on retry"
```

---

### Task 8: Destination — two-stage runBackgroundFinalization

**Files:**
- Modify: `internal/destination/server.go` (fields at :77-80, init at :111-112)
- Modify: `internal/destination/finalize.go:120-239` (`runBackgroundFinalization` → split)
- Modify: `internal/destination/helpers_test.go` (init `qbStageSem`)
- Test: `internal/destination/finalize_test.go`

- [ ] **Step 1: Add server fields**

In `internal/destination/server.go`, next to `finalizeSem`:

```go
	// finalizeSem serializes the disk-bound finalization stage (parent-dir
	// sync + piece verification + inode registration) so concurrent torrents
	// don't saturate disk I/O.
	finalizeSem *semaphore.Weighted

	// qbStageSem bounds the qBittorrent integration stage (AddTorrent +
	// recheck wait). Separate from finalizeSem so the mostly-idle qB recheck
	// wait of torrent N doesn't block the disk verification of torrent N+1.
	qbStageSem *semaphore.Weighted

	// finalizeQueueWait overrides finalizeQueueTimeout in tests (0 = default).
	finalizeQueueWait time.Duration
```

In the constructor where `finalizeSem: semaphore.NewWeighted(1)` is set:

```go
		finalizeSem: semaphore.NewWeighted(1),
		qbStageSem:  semaphore.NewWeighted(int64(config.GetQBFinalizeConcurrency())),
```

In `internal/destination/helpers_test.go` (`newTestDestServer`), add alongside `finalizeSem`:

```go
		qbStageSem:  semaphore.NewWeighted(1),
```

- [ ] **Step 2: Write the failing behavioral tests**

Add to `internal/destination/finalize_test.go`. Look at the existing tests in that file first (`grep -n "func Test" internal/destination/finalize_test.go`) and reuse the established way of building a `serverTorrentState` with real piece data on disk (there are existing helpers/tests that write `.partial` files and finalize them — mirror the closest one, e.g. a test that drives `FinalizeTorrent` end-to-end with `qbClient == nil`). Three behaviors to pin:

```go
// 1. Disk stage skipped when diskStageDone is set: corrupt a piece on disk
//    AFTER setting diskStageDone=true; finalization must still succeed
//    (qbClient nil → "finalized") because verification was skipped.
func TestRunBackgroundFinalization_SkipsDiskStageWhenDone(t *testing.T)

// 2. Disk-stage queue timeout returns BUSY: pre-acquire s.finalizeSem,
//    set s.finalizeQueueWait = 50*time.Millisecond, call FinalizeTorrent,
//    poll handleExistingFinalization via a second FinalizeTorrent call until
//    the result lands; assert ErrorCode == FINALIZE_ERROR_BUSY.
func TestRunBackgroundFinalization_DiskQueueTimeoutReturnsBusy(t *testing.T)

// 3. Stage overlap: pre-acquire s.qbStageSem (so torrent A would block in the
//    qB stage), then finalize torrent B with qbClient == nil — B must complete
//    (proves disk stage doesn't wait on the qB-stage semaphore).
//    NOTE: with qbClient == nil the qB stage short-circuits BEFORE acquiring
//    qbStageSem (see implementation), so test 3 instead asserts:
//    pre-acquire finalizeSem, release it after 100ms, and assert a full
//    finalize completes — i.e. the disk semaphore alone gates the disk stage.
func TestRunBackgroundFinalization_DiskStageIndependentOfQBSem(t *testing.T)
```

Write all three as real tests following the file's existing state-construction pattern (real temp files, real piece hashes). For test 2, the polling loop:

```go
	deadline := time.After(5 * time.Second)
	for {
		resp, err := s.FinalizeTorrent(ctx, req)
		if err != nil {
			t.Fatalf("FinalizeTorrent: %v", err)
		}
		if resp.GetState() == grpcutil.FinalizeStateVerifying {
			select {
			case <-deadline:
				t.Fatal("timed out waiting for queue-timeout result")
			case <-time.After(20 * time.Millisecond):
			}
			continue
		}
		if resp.GetSuccess() {
			t.Fatal("expected failure result after queue timeout")
		}
		if resp.GetErrorCode() != pb.FinalizeErrorCode_FINALIZE_ERROR_BUSY {
			t.Fatalf("expected BUSY, got %v (%s)", resp.GetErrorCode(), resp.GetError())
		}
		return
	}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `go test ./internal/destination/ -run TestRunBackgroundFinalization_ -count=1`
Expected: FAIL — `unknown field qbStageSem` / behaviors absent.

- [ ] **Step 4: Implement the split**

Replace `runBackgroundFinalization` in `internal/destination/finalize.go`:

```go
// runBackgroundFinalization runs the two finalization stages independently of
// the RPC context. Stage 1 (disk): parent-dir sync, piece verification, inode
// registration — serialized by finalizeSem. Stage 2 (qB): AddTorrent, recheck
// wait, marker — bounded by qbStageSem. Splitting the stages lets the disk
// verification of one torrent overlap the (mostly idle) qB recheck wait of
// another. On completion, the result is stored in state.finalization and done
// is closed.
func (s *Server) runBackgroundFinalization(
	hash string,
	state *serverTorrentState,
	req *pb.FinalizeTorrentRequest,
	startTime time.Time,
	done chan struct{},
) {
	defer close(done)

	// storeFailure records failure metrics and stores the error for the next poll.
	// errorCode is included in the result so source can make retry decisions.
	storeFailure := func(errMsg string, errorCode pb.FinalizeErrorCode) {
		metrics.FinalizationDuration.WithLabelValues(metrics.ResultFailure).Observe(time.Since(startTime).Seconds())
		metrics.FinalizationErrorsTotal.WithLabelValues(metrics.ModeDestination).Inc()
		state.mu.Lock()
		state.finalization.storeResult(&finalizeResult{err: errMsg, errorCode: errorCode})
		state.mu.Unlock()
	}

	state.mu.Lock()
	diskDone := state.finalization.diskStageDone
	state.mu.Unlock()

	if diskDone {
		s.logger.InfoContext(s.bgCtx, "disk stage already complete, skipping re-verification",
			"hash", hash,
		)
	} else if !s.runDiskStage(hash, state, storeFailure) {
		return
	}

	s.runQBStage(hash, state, req, startTime, storeFailure)
}

// acquireStageSlot waits for a slot on sem, tracking queue depth and wait time
// for the given stage label. Returns false on queue timeout or shutdown; the
// caller stores a BUSY failure so the source retries without penalty.
func (s *Server) acquireStageSlot(sem *semaphore.Weighted, stage, hash string) bool {
	queueTimeout := finalizeQueueTimeout
	if s.finalizeQueueWait > 0 {
		queueTimeout = s.finalizeQueueWait
	}

	queueStart := time.Now()
	metrics.FinalizationQueueDepth.WithLabelValues(stage).Inc()
	defer metrics.FinalizationQueueDepth.WithLabelValues(stage).Dec()

	waitCtx, waitCancel := context.WithTimeout(s.bgCtx, queueTimeout)
	defer waitCancel()
	acquireErr := sem.Acquire(waitCtx, 1)
	metrics.FinalizeQueueWaitSeconds.WithLabelValues(stage).Observe(time.Since(queueStart).Seconds())
	if acquireErr != nil {
		metrics.FinalizeBusyTotal.WithLabelValues(metrics.ReasonQueueTimeout).Inc()
		s.logger.WarnContext(s.bgCtx, "finalization deferred: stage queue saturated, source will retry",
			"hash", hash,
			"stage", stage,
			"waited", time.Since(queueStart).Round(time.Second),
			"reason", "queue_timeout",
		)
		return false
	}

	s.logger.DebugContext(s.bgCtx, "acquired finalization slot",
		"hash", hash,
		"stage", stage,
		"queueWait", time.Since(queueStart).Round(time.Millisecond),
	)
	return true
}

// runDiskStage performs the disk-bound half of finalization under finalizeSem:
// parent-dir sync, full piece verification, and inode registration. Returns
// true when the qB stage may proceed.
func (s *Server) runDiskStage(
	hash string,
	state *serverTorrentState,
	storeFailure func(string, pb.FinalizeErrorCode),
) bool {
	if !s.acquireStageSlot(s.finalizeSem, metrics.StageDisk, hash) {
		storeFailure("finalization queue timeout (disk stage)", pb.FinalizeErrorCode_FINALIZE_ERROR_BUSY)
		return false
	}
	defer s.finalizeSem.Release(1)

	stageStart := time.Now()

	// Work timeout starts after acquiring the semaphore — queue wait doesn't
	// eat into the verification budget. Derived from s.bgCtx so server
	// shutdown cancels in-flight work.
	ctx, cancel := context.WithTimeout(s.bgCtx, diskStageTimeout)
	defer cancel()

	// Sync parent directories before verification to ensure NFS has flushed
	// file data and renames to the server. Without this, verification can
	// read stale data from the NFS client cache, causing false hash mismatches.
	s.syncFileParentDirs(ctx, hash, state)

	failedPieces, verifyErr := s.verifyFinalizedPieces(ctx, hash, state)
	if verifyErr != nil {
		// System-level error (context cancel, idle timeout)
		s.logger.ErrorContext(ctx, "background verification failed",
			"hash", hash,
			"error", verifyErr,
		)
		metrics.FinalizeStageDuration.WithLabelValues(metrics.StageDisk, metrics.ResultFailure).
			Observe(time.Since(stageStart).Seconds())
		storeFailure(
			fmt.Sprintf("verification failed: %v", verifyErr),
			pb.FinalizeErrorCode_FINALIZE_ERROR_NONE,
		)
		return false
	}
	if len(failedPieces) > 0 {
		// Piece corruption — recover and signal incomplete to source.
		s.recoverVerificationFailure(ctx, hash, state, failedPieces)
		s.abortInProgressInodes(ctx, hash, state)
		metrics.VerificationRecoveriesTotal.Inc()
		metrics.FinalizeStageDuration.WithLabelValues(metrics.StageDisk, metrics.ResultFailure).
			Observe(time.Since(stageStart).Seconds())
		storeFailure(
			fmt.Sprintf("verification failed: %d pieces corrupted, will re-stream", len(failedPieces)),
			pb.FinalizeErrorCode_FINALIZE_ERROR_INCOMPLETE,
		)
		return false
	}

	// Register inodes for files we wrote (not hardlinked) and signal waiters.
	// MUST stay in the disk stage: pending-hardlink torrents block on the
	// doneCh signalled here, and making them wait through a qB recheck would
	// exhaust their hardlink wait budget.
	s.registerFinalizedInodes(ctx, hash, state)

	state.mu.Lock()
	state.finalization.diskStageDone = true
	state.mu.Unlock()

	metrics.FinalizeStageDuration.WithLabelValues(metrics.StageDisk, metrics.ResultSuccess).
		Observe(time.Since(stageStart).Seconds())
	return true
}

// runQBStage performs the qBittorrent integration half of finalization under
// qbStageSem: AddTorrent, recheck wait, synced tag, and the finalized marker.
func (s *Server) runQBStage(
	hash string,
	state *serverTorrentState,
	req *pb.FinalizeTorrentRequest,
	startTime time.Time,
	storeFailure func(string, pb.FinalizeErrorCode),
) {
	// No qB integration configured (or dry-run): finalize immediately.
	if s.qbClient == nil || s.config.DryRun {
		s.storeSuccessResult(s.bgCtx, hash, state, "finalized", startTime)
		return
	}

	if !s.acquireStageSlot(s.qbStageSem, metrics.StageQB, hash) {
		storeFailure("finalization queue timeout (qB stage)", pb.FinalizeErrorCode_FINALIZE_ERROR_BUSY)
		return
	}
	defer s.qbStageSem.Release(1)

	stageStart := time.Now()

	// Budget covers both waitForTorrentReady calls (initial + post-recheck).
	ctx, cancel := context.WithTimeout(s.bgCtx, s.qbStageTimeout(state.totalSize))
	defer cancel()

	finalState, qbErr := s.addAndVerifyTorrent(ctx, hash, state, req)
	if qbErr != nil {
		metrics.FinalizeStageDuration.WithLabelValues(metrics.StageQB, metrics.ResultFailure).
			Observe(time.Since(stageStart).Seconds())
		if isBusyWaitError(finalState, qbErr) {
			// qB was still actively checking when the budget expired —
			// congestion, not failure. diskStageDone is set, so the retry
			// goes straight back to this stage.
			metrics.FinalizeBusyTotal.WithLabelValues(metrics.ReasonQBChecking).Inc()
			s.logger.WarnContext(ctx, "finalization deferred: qB still checking at budget expiry, source will retry",
				"hash", hash,
				"lastState", finalState,
				"reason", "qb_checking",
			)
			storeFailure(
				fmt.Sprintf("qBittorrent still checking: %v", qbErr),
				pb.FinalizeErrorCode_FINALIZE_ERROR_BUSY,
			)
			return
		}
		s.logger.ErrorContext(ctx, "background qBittorrent integration failed",
			"hash", hash,
			"error", qbErr,
		)
		storeFailure(
			fmt.Sprintf("qBittorrent: %v", qbErr),
			pb.FinalizeErrorCode_FINALIZE_ERROR_NONE,
		)
		return
	}

	// Apply synced tag for visibility (not used as source of truth).
	if s.config.SyncedTag != "" {
		if tagErr := s.qbClient.AddTagsCtx(ctx, []string{hash}, s.config.SyncedTag); tagErr != nil {
			metrics.TagApplicationErrorsTotal.WithLabelValues(metrics.ModeDestination).Inc()
			s.logger.ErrorContext(ctx, "failed to add synced tag",
				"hash", hash,
				"tag", s.config.SyncedTag,
				"error", tagErr,
			)
		}
	}

	metrics.FinalizeStageDuration.WithLabelValues(metrics.StageQB, metrics.ResultSuccess).
		Observe(time.Since(stageStart).Seconds())
	s.storeSuccessResult(ctx, hash, state, string(finalState), startTime)
}
```

Delete the old single-semaphore body (the `finalizeSem.Acquire` block, old `waitCtx`, old `backgroundFinalizeTimeout` context, and the trailing `if s.qbClient != nil && !s.config.DryRun` block — all replaced above).

- [ ] **Step 5: Run the new tests and the full destination package**

Run: `go test ./internal/destination/ -short -count=1`
Expected: PASS, including pre-existing finalize tests. If an existing test asserted the exact old failure string `"finalization queue timeout: <err>"`, update it to the new per-stage message.

- [ ] **Step 6: Commit**

```bash
git add internal/destination/
git commit -m "feat(destination): split finalization into disk and qB stages"
```

---

### Task 9: Config plumbing — flag, env, main wiring

**Files:**
- Modify: `internal/config/config.go` (`DestinationConfig` ~:160-197, `SetupDestinationFlags` :282, `BindDestinationFlags` :331, `LoadDestination` ~:397)
- Modify: `cmd/qbsync/main.go` (~:137-160)
- Test: `internal/config/config_test.go` (follow existing test patterns in that file)

- [ ] **Step 1: Write the failing test**

Check existing config tests first (`grep -n "func Test" internal/config/config_test.go`) and follow their setup pattern. The test must cover the full env-var round trip (the review flagged that missing the bind-list entry is a silent bug):

```go
func TestQBFinalizeConcurrencyEnvBinding(t *testing.T) {
	t.Setenv("QBSYNC_DESTINATION_QB_FINALIZE_CONCURRENCY", "2")
	t.Setenv("QBSYNC_DESTINATION_DATA", t.TempDir())

	cmd := &cobra.Command{Use: "destination"}
	SetupDestinationFlags(cmd)

	v := viper.New()
	if err := BindDestinationFlags(cmd, v); err != nil {
		t.Fatalf("BindDestinationFlags: %v", err)
	}

	cfg, err := LoadDestination(v)
	if err != nil {
		t.Fatalf("LoadDestination: %v", err)
	}
	if cfg.QBFinalizeConcurrency != 2 {
		t.Errorf("QBFinalizeConcurrency = %d, want 2 (env var not bound?)", cfg.QBFinalizeConcurrency)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/config/ -run TestQBFinalizeConcurrencyEnvBinding -count=1`
Expected: FAIL — unknown field / flag.

- [ ] **Step 3: Implement**

`internal/config/config.go`:

1. `DestinationConfig` — add field after `MaxStreamBufferMB`:

```go
	// QBFinalizeConcurrency is how many torrents may concurrently occupy the
	// destination qB add/recheck stage (0 = default 1, max 8).
	QBFinalizeConcurrency int
```

2. `Validate()` — after the `MaxStreamBufferMB` check:

```go
	if c.QBFinalizeConcurrency < 0 || c.QBFinalizeConcurrency > 8 {
		return errors.New("qb finalize concurrency must be between 0 and 8 (0 = default 1)")
	}
```

3. `SetupDestinationFlags` — after the `max-stream-buffer` flag:

```go
	flags.Int(
		"qb-finalize-concurrency",
		0,
		"Max torrents concurrently in the destination qB add/recheck stage (0 = default 1, max 8). "+
			"Values >1 increase qB API and disk load; on NFS/spinning rust concurrent rechecks compete "+
			"for I/O — raise only on SSD-backed storage",
	)
```

4. `BindDestinationFlags` — add `"qb-finalize-concurrency"` to the bound-flag list (after `"max-stream-buffer"`).

5. `LoadDestination` — add to the struct literal:

```go
		QBFinalizeConcurrency: v.GetInt("qb-finalize-concurrency"),
```

`cmd/qbsync/main.go`:

6. Startup log — add to the `log.Info("starting destination server", ...)` attrs:

```go
		"qbFinalizeConcurrency", cfg.QBFinalizeConcurrency,
```

7. `serverCfg` literal — add:

```go
		QBFinalizeConcurrency: cfg.QBFinalizeConcurrency,
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/config/ -run TestQBFinalizeConcurrencyEnvBinding -count=1 && go build ./...`
Expected: PASS, build success.

- [ ] **Step 5: Commit**

```bash
git add internal/config/ cmd/
git commit -m "feat(config): wire qb-finalize-concurrency flag/env through to destination"
```

---

### Task 10: Docs — upgrade notes

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Add an "Upgrading" note**

Find the appropriate section (configuration or operations). Add:

```markdown
### Upgrading to two-stage finalization

- **Deploy order:** upgrade the **destination first**, then the source. The new
  `FINALIZE_ERROR_BUSY` congestion signal only takes effect once both sides are
  upgraded; an old source talking to a new destination behaves exactly as before.
- **Behavior change:** congestion (finalization queue timeouts, qB rechecks that
  outlast their budget) no longer counts toward the `sync-failed` retry cap. A
  torrent waiting on a saturated destination shows WARN logs
  (`destination finalization busy, will retry`) and increments
  `qbsync_finalize_busy_total` — it will NOT get the sync-failed tag unless it
  has been continuously busy for over 8 hours.
- **New knob:** `--qb-finalize-concurrency` / `QBSYNC_DESTINATION_QB_FINALIZE_CONCURRENCY`
  (default 1) controls how many torrents may concurrently occupy the destination
  qB add/recheck stage. The default preserves existing behavior; raise it only on
  SSD-backed storage.
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: upgrade notes for two-stage finalization and BUSY semantics"
```

---

### Task 11: Full verification + simplification pass

- [ ] **Step 1: Unit tests**

Run: `go test ./internal/... -short -count=1`
Expected: all PASS.

- [ ] **Step 2: Lint**

Run: `golangci-lint run --fix`
Expected: no remaining issues (the repo enables ~70 linters; pay attention to `exhaustive` on the new proto enum switch — the existing switch has a `//nolint:exhaustive` comment that covers the new case).

- [ ] **Step 3: E2E vet**

Run: `go vet -tags=e2e ./test/e2e/...`
Expected: clean. (Full e2e run `go test -tags=e2e -parallel 4 -timeout 30m ./test/e2e/...` is optional here but recommended before merge — default concurrency 1 keeps e2e behavior unchanged.)

- [ ] **Step 4: Code simplifier (required by CLAUDE.md)**

Dispatch the `code-simplifier:code-simplifier` agent over the modified files:
`proto/qbsync.proto`, `internal/streaming/grpc_destination.go`, `internal/source/backoff.go`, `internal/source/finalization.go`, `internal/source/orchestrator.go`, `internal/metrics/metrics.go`, `internal/destination/{config,types,server,finalize,qbittorrent}.go`, `internal/config/config.go`, `cmd/qbsync/main.go`.

- [ ] **Step 5: Re-run tests after simplification**

Run: `go test ./internal/... -short -count=1 && golangci-lint run`
Expected: all PASS.

- [ ] **Step 6: Final commit**

```bash
git add -A
git commit -m "refactor: simplification pass over two-stage finalization"
```

---

## Decision Log (from the structured design review)

| Decision | Alternatives considered | Objection | Resolution |
|---|---|---|---|
| Two semaphores, sequential acquire in one goroutine, disk sem released before qB sem acquired | One wider semaphore; separate goroutine per stage | Hardlink deadlock concern (Skeptic) | No cycle exists: hardlink waits happen pre-semaphore in `finalizeFiles`; `registerFinalizedInodes` pinned to disk stage |
| qB-stage default weight **1** | Default 2 for out-of-box throughput | Behavior change for NFS/spinning-rust operators (Guardian + Advocate, independently) | Default 1 — the stage split alone yields the overlap win; knob capped at 8 |
| In-memory `diskStageDone` (survives `reset()`, invalidated on relocate) | Durable `.disk_finalized` marker (Guardian's ask) | Guardian called qB-stage timeout recovery a VIOLATION | Arbiter verified the "full re-stream" claim was overstated (congestion never produces `FINALIZE_ERROR_NOT_FOUND`); durable marker REJECTED as disproportionate, in-memory flag REQUIRED |
| qB-stage budget = `2×computePollTimeout + 5min margin` | `1×poll + 60s margin` | `addAndVerifyTorrent` runs `waitForTorrentReady` twice (Arbiter) | 2× budget + 5min named constant (covers AddTorrent, login retries, priority-verify loop) |
| BUSY wall-clock guard 8h, in-memory | Persisted guard state; no guard | BUSY-forever on wedged destination (Skeptic+Guardian, BLOCKING) | 8h > 2h queue timeout + ~6h poll cap; restart-resets-clock documented as acceptable |
| BUSY classification: timeout AND `isCheckingState` only | Classify all qB-stage timeouts as BUSY | Error states (missingFiles) must keep burning the budget | Genuine error states stay `FINALIZE_ERROR_NONE` |
| No code-level serialization of concurrent filePrio ops | Mutex around `applyAndVerifyDeselectedPriorities` | qB silently drops filePrio under load (Skeptic) | Doc-only at default weight 1; flag help text carries the warning |
| Operator visibility: busy counter + queue-depth gauge + WARN logs | New qB tag for busy torrents | BUSY-stuck torrents invisible (Advocate, MUST) | Metrics + distinct WARN wording; tag-churn avoided |
| Out of scope: pre-existing unbound `PriorityVerify*` config fields | Fix in this branch | Scope creep | Deferred to separate issue |

Arbiter disposition: **APPROVED** after amendments #4 (in-memory flag) and #6 (2× budget) — both incorporated above.

---

## Out of Scope (tracked separately — do NOT include in this branch)

- Partial-selection triple-read (`skip_checking` can't be used with absent files) — needs its own design discussion.
- Binding the pre-existing unbound `PriorityVerifyInterval`/`PriorityVerifyTimeout` config fields — separate issue.
- The source-side empty-fingerprint spurious-resync bug (`MarkWithFingerprint(hash, "")` at `internal/source/tracking.go:171` vs `recheckFileSelections`) — separate fix/PR.
- `LoginCtx`-per-call and poll-interval micro-optimizations.
