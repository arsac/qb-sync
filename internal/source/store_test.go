package source

import (
	"log/slog"
	"testing"
	"time"

	"github.com/autobrr/go-qbittorrent"

	"github.com/arsac/qb-sync/internal/config"
	"github.com/arsac/qb-sync/internal/metrics"
)

func testStoreLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

func newTestStore(t *testing.T) *torrentStore {
	t.Helper()
	return newTorrentStore("", testStoreLogger())
}

// --- Finalization retry accounting ------------------------------------------
//
// Ported from the former BackoffTracker tests. The busy streak is separate
// from the failure count: congestion on the destination is not a per-torrent
// fault and must neither delay attempts nor count toward the failure cap.

func TestRecordBusyTracksStreakDuration(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	now := time.Now()
	s.now = func() time.Time { return now }

	if d := s.RecordBusy("h1"); d != 0 {
		t.Fatalf("first busy must report zero elapsed, got %v", d)
	}

	now = now.Add(3 * time.Hour)
	if d := s.RecordBusy("h1"); d != 3*time.Hour {
		t.Fatalf("expected 3h busy streak, got %v", d)
	}
}

func TestRecordBusyDoesNotAffectFailureCapOrBackoff(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	s.RecordBusy("h1")

	if !s.ShouldAttempt("h1") {
		t.Fatal("busy streak must not delay finalize attempts")
	}
	if got, _ := s.RecordFailure("h1"); got != 1 {
		t.Fatalf("first real failure after busy must be 1, got %d", got)
	}
}

func TestClearBackoffResetsBusyStreak(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	now := time.Now()
	s.now = func() time.Time { return now }

	s.RecordBusy("h1")
	s.ClearBackoff("h1")

	now = now.Add(10 * time.Hour)
	if d := s.RecordBusy("h1"); d != 0 {
		t.Fatalf("ClearBackoff must reset the busy streak, got %v", d)
	}
}

// --- Consolidation invariants ------------------------------------------------

// TestStoreRecordIsDroppedWhenEmpty covers the one hazard the consolidation
// introduces. The three former holders each deleted their map entry when a
// torrent left them. A single shared map only reclaims an entry once every
// concern has released it, so a missed gc call would leak a record per torrent
// for the lifetime of the process.
func TestStoreRecordIsDroppedWhenEmpty(t *testing.T) {
	t.Parallel()

	release := map[string]func(s *torrentStore, hash string){
		"untrack":         func(s *torrentStore, h string) { s.Untrack(h) },
		"untrackAndGet":   func(s *torrentStore, h string) { s.UntrackAndGet(h) },
		"forgetComplete":  func(s *torrentStore, h string) { s.ForgetComplete(h) },
		"forgetAll":       func(s *torrentStore, h string) { s.ForgetCompleteAll([]string{h}) },
		"clearBackoff":    func(s *torrentStore, h string) { s.ClearBackoff(h) },
		"trackIfAbsentNo": func(s *torrentStore, h string) { s.TrackIfAbsent(h, TrackedTorrent{}) },
	}

	for name, fn := range release {
		t.Run(name+" alone leaves nothing behind", func(t *testing.T) {
			t.Parallel()
			s := newTestStore(t)

			// Give the record exactly the state this releaser clears.
			switch name {
			case "untrack", "untrackAndGet", "trackIfAbsentNo":
				s.Track("h1", TrackedTorrent{Name: "x"})
			case "forgetComplete", "forgetAll":
				s.MarkComplete("h1", "0,1")
			case "clearBackoff":
				s.RecordFailure("h1")
			}

			if name == "trackIfAbsentNo" {
				// Already tracked, so this is a no-op that must not leak either.
				s.Untrack("h1")
			} else {
				fn(s, "h1")
			}

			s.mu.RLock()
			n := len(s.records)
			s.mu.RUnlock()
			if n != 0 {
				t.Fatalf("record leaked: expected 0 records, got %d", n)
			}
		})
	}
}

// TestStoreConcernsAreIndependent verifies that collapsing three maps into one
// did not couple them. A torrent can be tracked, complete, and in a retry
// streak in any combination, and releasing one concern must not disturb the
// others.
func TestStoreConcernsAreIndependent(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)

	s.Track("h1", TrackedTorrent{Name: "one"})
	s.MarkComplete("h1", "0,1")
	s.RecordFailure("h1")

	s.Untrack("h1")
	if s.IsTracked("h1") {
		t.Error("untrack should clear tracking")
	}
	if !s.IsComplete("h1") {
		t.Error("untrack must not clear completion")
	}
	if s.BackoffCount() != 1 {
		t.Error("untrack must not clear retry state")
	}

	s.ForgetComplete("h1")
	if s.IsComplete("h1") {
		t.Error("forgetComplete should clear completion")
	}
	if s.BackoffCount() != 1 {
		t.Error("forgetComplete must not clear retry state")
	}

	s.ClearBackoff("h1")
	if s.BackoffCount() != 0 {
		t.Error("clearBackoff should clear retry state")
	}

	s.mu.RLock()
	n := len(s.records)
	s.mu.RUnlock()
	if n != 0 {
		t.Fatalf("record should be gone once every concern released it, got %d", n)
	}
}

// TestStoreCountsAreScopedToTheirConcern guards the metrics gauges. All three
// used to be len() of a dedicated map; they are now filtered counts over a
// shared one, so a torrent present for one reason must not inflate the others.
func TestStoreCountsAreScopedToTheirConcern(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)

	s.Track("tracked-only", TrackedTorrent{})
	s.MarkComplete("complete-only", "")
	s.RecordFailure("failing-only")

	if got := s.TrackedCount(); got != 1 {
		t.Errorf("TrackedCount = %d, want 1", got)
	}
	if got := s.CompletedCount(); got != 1 {
		t.Errorf("CompletedCount = %d, want 1", got)
	}
	if got := s.BackoffCount(); got != 1 {
		t.Errorf("BackoffCount = %d, want 1", got)
	}

	if got := len(s.TrackedSnapshot()); got != 1 {
		t.Errorf("TrackedSnapshot size = %d, want 1", got)
	}
	if got := len(s.CompletedSnapshot()); got != 1 {
		t.Errorf("CompletedSnapshot size = %d, want 1", got)
	}
	if got := len(s.CompletedKeys()); got != 1 {
		t.Errorf("CompletedKeys size = %d, want 1", got)
	}
	if got := len(s.TrackedHashes()); got != 1 {
		t.Errorf("TrackedHashes size = %d, want 1", got)
	}
}

func TestStoreTrackIfAbsent(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)

	if !s.TrackIfAbsent("h1", TrackedTorrent{Name: "first"}) {
		t.Fatal("first TrackIfAbsent should report that it started tracking")
	}
	if s.TrackIfAbsent("h1", TrackedTorrent{Name: "second"}) {
		t.Fatal("second TrackIfAbsent should report already tracked")
	}

	info, ok := s.UntrackAndGet("h1")
	if !ok || info.Name != "first" {
		t.Fatalf("TrackIfAbsent must not overwrite: got %q, ok=%v", info.Name, ok)
	}
}

func TestStoreUntrackAndGetOnUntrackedTorrent(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)

	// Complete but never tracked: UntrackAndGet must report false and must not
	// disturb the completion state.
	s.MarkComplete("h1", "0")

	if _, ok := s.UntrackAndGet("h1"); ok {
		t.Error("UntrackAndGet should report false for an untracked torrent")
	}
	if !s.IsComplete("h1") {
		t.Error("UntrackAndGet must not clear completion")
	}
}

// --- Quarantine clocks --------------------------------------------------------

// TestRecordFailureNeedsBothAttemptsAndTime is the core of ADR-0001. Quarantine
// used to be reachable on attempt count alone, and because the backoff was
// shorter than the orchestrator cycle those attempts spanned about ninety
// seconds, so any two-minute fault sidelined every finalizing torrent.
func TestRecordFailureNeedsBothAttemptsAndTime(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	s.guard = time.Hour
	now := time.Now()
	s.now = func() time.Time { return now }

	for range 50 {
		now = now.Add(time.Second)
		if _, quarantine := s.RecordFailure("h1"); quarantine {
			t.Fatal("50 failures inside one minute must not quarantine")
		}
	}

	now = now.Add(time.Hour)
	if _, quarantine := s.RecordFailure("h1"); !quarantine {
		t.Fatal("a streak past the guard must quarantine")
	}
}

func TestRecordFailureNeedsEnoughAttempts(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	s.guard = time.Hour
	now := time.Now()
	s.now = func() time.Time { return now }

	// One failure, then a long silence. Time alone is not enough: a single
	// failure long ago says nothing about whether the torrent is still broken.
	s.RecordFailure("h1")
	now = now.Add(10 * time.Hour)

	if _, quarantine := s.RecordFailure("h1"); quarantine {
		t.Fatal("two failures must not quarantine even long past the guard")
	}
	if _, quarantine := s.RecordFailure("h1"); !quarantine {
		t.Fatal("the attempt threshold plus elapsed guard should quarantine")
	}
}

func TestObserveStallQuarantinesOnDuration(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	s.guard = time.Hour
	now := time.Now()
	s.now = func() time.Time { return now }

	// First observation only establishes the baseline count.
	s.ObserveStall("h1", 5, true)

	if _, q := s.ObserveStall("h1", 5, true); q {
		t.Fatal("a stall that just started must not quarantine")
	}
	now = now.Add(30 * time.Minute)
	if _, q := s.ObserveStall("h1", 5, true); q {
		t.Fatal("a stall inside the guard must not quarantine")
	}
	now = now.Add(31 * time.Minute)
	stalledFor, q := s.ObserveStall("h1", 5, true)
	if !q {
		t.Fatal("a stall past the guard must quarantine")
	}
	if stalledFor < time.Hour {
		t.Errorf("stalledFor = %v, want at least the guard", stalledFor)
	}
}

// TestObserveStallForgivesProgress covers the difference between a wedged
// torrent and a slow one: any genuine advance resets the clock entirely.
func TestObserveStallForgivesProgress(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	s.guard = time.Hour
	now := time.Now()
	s.now = func() time.Time { return now }

	s.ObserveStall("h1", 5, true) // baseline
	s.ObserveStall("h1", 5, true) // clock starts
	now = now.Add(59 * time.Minute)

	// One piece moved.
	if _, q := s.ObserveStall("h1", 6, true); q {
		t.Fatal("an advance must not quarantine")
	}

	// The clock restarted, so the old 59 minutes no longer count.
	now = now.Add(30 * time.Minute)
	if _, q := s.ObserveStall("h1", 6, true); q {
		t.Fatal("progress must reset the stall clock, not merely pause it")
	}
}

// TestObserveStallSurvivesRestart is why progress is judged by piece count
// rather than a timestamp. On restart a torrent resumes from the destination's
// bitmap, which against a timestamp would look like it had just advanced and
// would clear a streak that should have persisted.
func TestObserveStallSurvivesRestart(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	logger := testStoreLogger()
	path := dir + "/completed_on_dest.json"

	s := newTorrentStore(path, logger)
	s.guard = time.Hour
	now := time.Now()
	s.now = func() time.Time { return now }

	s.ObserveStall("h1", 40, true) // baseline at 40 of N pieces
	s.ObserveStall("h1", 40, true) // clock starts
	s.SaveStreaks()

	// New process: clocks reloaded, the torrent resumes at the same 40 pieces.
	s2 := newTorrentStore(path, logger)
	s2.guard = time.Hour
	s2.now = func() time.Time { return now.Add(2 * time.Hour) }
	s2.LoadStreaks()

	_, q := s2.ObserveStall("h1", 40, true)
	if !q {
		t.Fatal("a stall streak must survive a restart: resuming is not progress")
	}
}

func TestPruneStreaksDropsDepartedTorrents(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	s.RecordFailure("gone")
	s.ObserveStall("stays", 1, true) // baseline
	s.ObserveStall("stays", 1, true) // clock starts

	s.PruneStreaks(map[string]struct{}{"stays": {}})

	if s.count(func(r *torrentRecord) bool { return !r.firstFailure.IsZero() }) != 0 {
		t.Error("streaks for departed torrents should be dropped")
	}
	if s.StalledCount() != 1 {
		t.Error("streaks for present torrents must survive pruning")
	}
}

// --- Eligibility reporting ----------------------------------------------------

// TestExclusionReason pins the reason a torrent is reported as skipped. These
// torrents were previously invisible: one sitting in error or missingFiles on
// the source is silently never synced, with no log and no metric. A wrong label
// here is a silent observability bug, so the mapping is worth pinning.
func TestExclusionReason(t *testing.T) {
	t.Parallel()

	newTask := func() *QBTask {
		return &QBTask{
			cfg: &config.SourceConfig{
				SyncFailedTag:  "sync-failed",
				ExcludeSyncTag: "no-sync",
			},
			logger: testStoreLogger(),
			store:  newTorrentStore("", testStoreLogger()),
		}
	}

	// seeding is the eligible baseline; each case varies one thing from it.
	seeding := func(tags string) qbittorrent.Torrent {
		return qbittorrent.Torrent{
			Hash:     "a",
			State:    qbittorrent.TorrentStateUploading,
			Progress: 1,
			Tags:     tags,
		}
	}

	tests := []struct {
		name    string
		torrent qbittorrent.Torrent
		setup   func(*QBTask)
		want    string
	}{
		{
			name:    "error state",
			torrent: qbittorrent.Torrent{Hash: "a", State: qbittorrent.TorrentStateError, Progress: 1},
			want:    metrics.ReasonSkipNotSyncable,
		},
		{
			name:    "missing files",
			torrent: qbittorrent.Torrent{Hash: "a", State: qbittorrent.TorrentStateMissingFiles, Progress: 1},
			want:    metrics.ReasonSkipNotSyncable,
		},
		{
			name:    "nothing downloaded yet",
			torrent: qbittorrent.Torrent{Hash: "a", State: qbittorrent.TorrentStateDownloading, Progress: 0},
			want:    metrics.ReasonSkipZeroProgress,
		},
		{
			name:    "operator opted out",
			torrent: seeding("no-sync"),
			want:    metrics.ReasonSkipExcludeTag,
		},
		{
			name:    "quarantined",
			torrent: seeding("sync-failed"),
			want:    metrics.ReasonSkipQuarantined,
		},
		{
			name:    "already synced",
			torrent: seeding(""),
			setup:   func(task *QBTask) { task.store.MarkComplete("a", "") },
			want:    metrics.ReasonSkipAlreadySynced,
		},
		{
			name:    "eligible",
			torrent: seeding(""),
			want:    "",
		},
		{
			name:    "already tracked is not reported as skipped",
			torrent: seeding(""),
			setup:   func(task *QBTask) { task.store.Track("a", TrackedTorrent{}) },
			want:    "", // the steady state of a healthy sync, not a torrent left behind
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			task := newTask()
			if tc.setup != nil {
				tc.setup(task)
			}
			if got := task.exclusionReason(tc.torrent); got != tc.want {
				t.Errorf("exclusionReason = %q, want %q", got, tc.want)
			}
		})
	}
}
