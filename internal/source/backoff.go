package source

import (
	"sync"
	"time"
)

// Finalization retry settings - exponential backoff.
const (
	minFinalizeBackoff = 2 * time.Second
	maxFinalizeBackoff = 30 * time.Second
)

// finalizeBackoff tracks exponential backoff state for finalization retries.
type finalizeBackoff struct {
	failures    int
	lastAttempt time.Time
}

// BackoffTracker manages exponential backoff for finalization retries.
// Thread-safe with internal locking.
type BackoffTracker struct {
	backoffs map[string]*finalizeBackoff
	mu       sync.Mutex
}

// NewBackoffTracker creates a new BackoffTracker.
func NewBackoffTracker() *BackoffTracker {
	return &BackoffTracker{
		backoffs: make(map[string]*finalizeBackoff),
	}
}

// ShouldAttempt checks if enough time has passed since the last failed
// finalization attempt. Returns true if we should try again.
func (b *BackoffTracker) ShouldAttempt(hash string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	backoff, exists := b.backoffs[hash]
	if !exists {
		return true
	}

	backoffDuration := min(
		minFinalizeBackoff*time.Duration(1<<uint(backoff.failures-1)),
		maxFinalizeBackoff,
	)

	return time.Since(backoff.lastAttempt) >= backoffDuration
}

// RecordFailure records a finalization failure for backoff tracking.
// Returns the total number of consecutive failures for this hash.
func (b *BackoffTracker) RecordFailure(hash string) int {
	b.mu.Lock()
	defer b.mu.Unlock()

	backoff, exists := b.backoffs[hash]
	if !exists {
		backoff = &finalizeBackoff{}
		b.backoffs[hash] = backoff
	}

	backoff.failures++
	backoff.lastAttempt = time.Now()
	return backoff.failures
}

// Clear removes backoff tracking for a successfully finalized torrent.
func (b *BackoffTracker) Clear(hash string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.backoffs, hash)
}

// Count returns the number of hashes currently in backoff state.
// Used by metrics collectors to emit active_finalization_backoffs at scrape time.
func (b *BackoffTracker) Count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.backoffs)
}
