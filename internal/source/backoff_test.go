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
