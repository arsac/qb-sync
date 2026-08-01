package source

import (
	"log/slog"
	"testing"
	"time"
)

func newTestStore(t *testing.T) *torrentStore {
	t.Helper()
	return newTorrentStore("", slog.New(slog.DiscardHandler))
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
	if got := s.RecordFailure("h1"); got != 1 {
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
