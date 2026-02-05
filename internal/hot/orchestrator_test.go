package hot

import (
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/arsac/qb-sync/internal/config"
)

// Tests for finalization backoff logic. These can be unit tested without mocking gRPC.
func TestFinalizeBackoff(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	t.Run("shouldAttemptFinalize returns true initially", func(t *testing.T) {
		task := &QBTask{
			cfg:              &config.HotConfig{},
			logger:           logger,
			finalizeBackoffs: make(map[string]*finalizeBackoff),
		}

		if !task.shouldAttemptFinalize("hash123") {
			t.Error("should allow finalization attempt initially")
		}
	})

	t.Run("shouldAttemptFinalize returns false after recent failure", func(t *testing.T) {
		task := &QBTask{
			cfg:              &config.HotConfig{},
			logger:           logger,
			finalizeBackoffs: make(map[string]*finalizeBackoff),
		}

		hash := "hash123"
		task.recordFinalizeFailure(hash)

		if task.shouldAttemptFinalize(hash) {
			t.Error("should not allow finalization attempt immediately after failure")
		}
	})

	t.Run("shouldAttemptFinalize returns true after backoff period", func(t *testing.T) {
		task := &QBTask{
			cfg:              &config.HotConfig{},
			logger:           logger,
			finalizeBackoffs: make(map[string]*finalizeBackoff),
		}

		hash := "hash123"

		// Manually set a past lastAttempt
		task.backoffMu.Lock()
		task.finalizeBackoffs[hash] = &finalizeBackoff{
			failures:    1,
			lastAttempt: time.Now().Add(-10 * time.Second), // Well past minFinalizeBackoff
		}
		task.backoffMu.Unlock()

		if !task.shouldAttemptFinalize(hash) {
			t.Error("should allow finalization attempt after backoff period")
		}
	})

	t.Run("clearFinalizeBackoff removes tracking", func(t *testing.T) {
		task := &QBTask{
			cfg:              &config.HotConfig{},
			logger:           logger,
			finalizeBackoffs: make(map[string]*finalizeBackoff),
		}

		hash := "hash123"
		task.recordFinalizeFailure(hash)

		task.clearFinalizeBackoff(hash)

		task.backoffMu.Lock()
		_, exists := task.finalizeBackoffs[hash]
		task.backoffMu.Unlock()

		if exists {
			t.Error("backoff should be cleared")
		}

		// Should be able to attempt immediately after clearing
		if !task.shouldAttemptFinalize(hash) {
			t.Error("should allow finalization attempt after clearing backoff")
		}
	})

	t.Run("backoff increases with failures", func(t *testing.T) {
		task := &QBTask{
			cfg:              &config.HotConfig{},
			logger:           logger,
			finalizeBackoffs: make(map[string]*finalizeBackoff),
		}

		hash := "hash123"

		// Record multiple failures
		for range 5 {
			task.recordFinalizeFailure(hash)
		}

		task.backoffMu.Lock()
		backoff := task.finalizeBackoffs[hash]
		failures := backoff.failures
		task.backoffMu.Unlock()

		if failures != 5 {
			t.Errorf("expected 5 failures recorded, got %d", failures)
		}
	})

	t.Run("backoff is capped at maxFinalizeBackoff", func(t *testing.T) {
		task := &QBTask{
			cfg:              &config.HotConfig{},
			logger:           logger,
			finalizeBackoffs: make(map[string]*finalizeBackoff),
		}

		hash := "hash123"

		// Simulate many failures to trigger cap
		task.backoffMu.Lock()
		task.finalizeBackoffs[hash] = &finalizeBackoff{
			failures:    100, // Large number
			lastAttempt: time.Now(),
		}
		task.backoffMu.Unlock()

		// The computed backoff should be capped, so waiting maxFinalizeBackoff should allow retry
		task.backoffMu.Lock()
		task.finalizeBackoffs[hash].lastAttempt = time.Now().Add(-maxFinalizeBackoff - time.Second)
		task.backoffMu.Unlock()

		if !task.shouldAttemptFinalize(hash) {
			t.Error("should allow finalization after max backoff period")
		}
	})
}

// Tests for tracking maps operations.
func TestTrackedTorrentsMap(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	t.Run("concurrent map access is safe", func(t *testing.T) {
		task := &QBTask{
			cfg:             &config.HotConfig{},
			logger:          logger,
			trackedTorrents: make(map[string]bool),
		}

		var wg sync.WaitGroup
		const numOps = 100

		// Concurrent writes
		for i := range numOps {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				hash := string(rune('a' + idx%26))
				task.trackedMu.Lock()
				task.trackedTorrents[hash] = true
				task.trackedMu.Unlock()
			}(i)
		}

		// Concurrent reads
		for range numOps {
			wg.Add(1)
			go func() {
				defer wg.Done()
				task.trackedMu.RLock()
				_ = len(task.trackedTorrents)
				task.trackedMu.RUnlock()
			}()
		}

		// Concurrent deletes
		for i := range numOps {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				hash := string(rune('a' + idx%26))
				task.trackedMu.Lock()
				delete(task.trackedTorrents, hash)
				task.trackedMu.Unlock()
			}(i)
		}

		wg.Wait()
	})
}

func TestConcurrentBackoffAccess(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	t.Run("concurrent backoff operations are safe", func(t *testing.T) {
		task := &QBTask{
			cfg:              &config.HotConfig{},
			logger:           logger,
			finalizeBackoffs: make(map[string]*finalizeBackoff),
		}

		var wg sync.WaitGroup
		const numOps = 100

		// Concurrent recordFinalizeFailure
		for i := range numOps {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				hash := string(rune('a' + idx%26))
				task.recordFinalizeFailure(hash)
			}(i)
		}

		// Concurrent shouldAttemptFinalize
		for i := range numOps {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				hash := string(rune('a' + idx%26))
				_ = task.shouldAttemptFinalize(hash)
			}(i)
		}

		// Concurrent clearFinalizeBackoff
		for i := range numOps {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				hash := string(rune('a' + idx%26))
				task.clearFinalizeBackoff(hash)
			}(i)
		}

		wg.Wait()
	})
}

// Tests for helper functions.
func TestHasSyncedTag(t *testing.T) {
	tests := []struct {
		name     string
		tags     string
		expected bool
	}{
		{"empty tags", "", false},
		{"only synced tag", "synced", true},
		{"synced with other tags", "tag1, synced, tag2", true},
		{"synced at start", "synced, other", true},
		{"synced at end", "other, synced", true},
		{"no synced tag", "tag1, tag2, tag3", false},
		{"partial match not counted", "synced-backup, other", false},
		{"synced with spaces", "  synced  ", true},
		{"case sensitive", "Synced, SYNCED", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := hasSyncedTag(tc.tags)
			if result != tc.expected {
				t.Errorf("hasSyncedTag(%q) = %v, want %v", tc.tags, result, tc.expected)
			}
		})
	}
}

func TestSplitTags(t *testing.T) {
	tests := []struct {
		name     string
		tags     string
		expected []string
	}{
		{"empty string", "", nil},
		{"single tag", "tag1", []string{"tag1"}},
		{"multiple tags", "tag1, tag2, tag3", []string{"tag1", "tag2", "tag3"}},
		{"tags with spaces", "  tag1  ,  tag2  ", []string{"tag1", "tag2"}},
		{"empty elements filtered", "tag1,,tag2,", []string{"tag1", "tag2"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := splitTags(tc.tags)
			if len(result) != len(tc.expected) {
				t.Errorf("splitTags(%q) length = %d, want %d", tc.tags, len(result), len(tc.expected))
				return
			}
			for i, v := range result {
				if v != tc.expected[i] {
					t.Errorf("splitTags(%q)[%d] = %q, want %q", tc.tags, i, v, tc.expected[i])
				}
			}
		})
	}
}

func TestConstants(t *testing.T) {
	t.Run("abortTorrentTimeout is reasonable", func(t *testing.T) {
		if abortTorrentTimeout < 5*time.Second {
			t.Errorf("abortTorrentTimeout too short: %v", abortTorrentTimeout)
		}
		if abortTorrentTimeout > 2*time.Minute {
			t.Errorf("abortTorrentTimeout too long: %v", abortTorrentTimeout)
		}
	})

	t.Run("backoff constants are valid", func(t *testing.T) {
		if minFinalizeBackoff <= 0 {
			t.Error("minFinalizeBackoff should be positive")
		}
		if maxFinalizeBackoff <= minFinalizeBackoff {
			t.Error("maxFinalizeBackoff should be greater than minFinalizeBackoff")
		}
	})

	t.Run("syncedTag is defined", func(t *testing.T) {
		if syncedTag == "" {
			t.Error("syncedTag should not be empty")
		}
	})
}

// Note: Tests for handleTorrentRemoval and listenForRemovals require mocking
// the gRPC destination which would require refactoring QBTask to use interfaces.
// For now, these scenarios should be covered by integration/e2e tests.
//
// Scenarios to test in integration tests:
// - handleTorrentRemoval removes from local tracking
// - handleTorrentRemoval calls AbortTorrent with correct parameters
// - handleTorrentRemoval handles gRPC errors gracefully
// - handleTorrentRemoval respects dry run mode
// - listenForRemovals processes removal notifications
// - listenForRemovals stops on context cancellation
// - listenForRemovals stops on channel close
