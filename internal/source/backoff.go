package source

import (
	"sync"
	"time"

	"github.com/arsac/qb-sync/internal/metrics"
)

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
	metrics.ActiveFinalizationBackoffs.Set(float64(len(b.backoffs)))
	return backoff.failures
}

// Clear removes backoff tracking for a successfully finalized torrent.
func (b *BackoffTracker) Clear(hash string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.backoffs, hash)
	metrics.ActiveFinalizationBackoffs.Set(float64(len(b.backoffs)))
}
