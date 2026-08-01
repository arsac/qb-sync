package source

import (
	"encoding/json"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/arsac/qb-sync/internal/utils"
)

// Finalization retry settings - exponential backoff.
const (
	minFinalizeBackoff = 2 * time.Second
	maxFinalizeBackoff = 30 * time.Second
)

// torrentRecord is everything the source knows about one torrent. A torrent
// participates in three concerns at once - it is being streamed, it is known
// complete on the destination, and it may be in a finalization retry streak -
// and those used to live in three separate maps behind three separate locks.
// Nothing kept them consistent with each other, and every lifecycle transition
// had to remember which subset to update.
//
// A record exists while any concern applies to it and is dropped by gc once
// none do.
type torrentRecord struct {
	// Tracking: the torrent is actively being streamed.
	tracked bool
	info    TrackedTorrent

	// Completion: the destination has finalized the torrent. fingerprint is
	// the selected-file fingerprint at that moment, used to detect selection
	// changes. Persisted.
	complete    bool
	fingerprint string

	// Finalization retry accounting. failures and lastAttempt drive the
	// exponential backoff; firstBusy bounds how long destination congestion
	// is tolerated before it counts as a failure.
	failures    int
	lastAttempt time.Time
	firstBusy   time.Time
}

// hasBackoff reports whether any finalization retry state is recorded. This is
// what the old BackoffTracker expressed by the presence or absence of a map
// entry.
func (r *torrentRecord) hasBackoff() bool {
	return r.failures > 0 || !r.firstBusy.IsZero()
}

// isEmpty reports whether the record carries no remaining state and can be
// dropped.
func (r *torrentRecord) isEmpty() bool {
	return !r.tracked && !r.complete && !r.hasBackoff()
}

// torrentStore holds per-torrent source state behind a single lock, and
// persists the completion cache to disk.
//
// Thread-safe. Callers never see *torrentRecord; every accessor copies out
// what it needs under the lock.
type torrentStore struct {
	mu      sync.RWMutex
	records map[string]*torrentRecord

	path   string // completion cache path; empty disables persistence
	logger *slog.Logger

	now func() time.Time // injectable for tests
}

// newTorrentStore creates a store. Pass an empty path to disable persistence.
func newTorrentStore(path string, logger *slog.Logger) *torrentStore {
	return &torrentStore{
		records: make(map[string]*torrentRecord),
		path:    path,
		logger:  logger,
		now:     time.Now,
	}
}

// record returns the existing record for hash, creating it if absent.
// Caller must hold the write lock.
func (s *torrentStore) record(hash string) *torrentRecord {
	r, ok := s.records[hash]
	if !ok {
		r = &torrentRecord{}
		s.records[hash] = r
	}
	return r
}

// gc drops a record that no longer carries state. Caller must hold the write
// lock. Without this the map would grow for the lifetime of the process.
func (s *torrentStore) gc(hash string, r *torrentRecord) {
	if r.isEmpty() {
		delete(s.records, hash)
	}
}

// count returns how many records satisfy pred. Each concern's gauge is a
// filtered count over the shared map rather than the length of its own.
func (s *torrentStore) count(pred func(*torrentRecord) bool) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var n int
	for _, r := range s.records {
		if pred(r) {
			n++
		}
	}
	return n
}

func isTracked(r *torrentRecord) bool  { return r.tracked }
func isComplete(r *torrentRecord) bool { return r.complete }

// --- Tracking ---------------------------------------------------------------

// Track marks a torrent as being streamed, overwriting any existing entry.
func (s *torrentStore) Track(hash string, info TrackedTorrent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.record(hash)
	r.tracked = true
	r.info = info
}

// TrackIfAbsent marks a torrent as being streamed only if it is not already.
// Returns true if this call started tracking it.
func (s *torrentStore) TrackIfAbsent(hash string, info TrackedTorrent) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.record(hash)
	if r.tracked {
		s.gc(hash, r)
		return false
	}
	r.tracked = true
	r.info = info
	return true
}

// IsTracked reports whether the torrent is currently being streamed.
func (s *torrentStore) IsTracked(hash string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.records[hash]
	return ok && r.tracked
}

// Untrack stops tracking a torrent, leaving its other state intact.
func (s *torrentStore) Untrack(hash string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.records[hash]
	if !ok {
		return
	}
	r.tracked = false
	r.info = TrackedTorrent{}
	s.gc(hash, r)
}

// UntrackAndGet stops tracking a torrent and returns what was tracked.
func (s *torrentStore) UntrackAndGet(hash string) (TrackedTorrent, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.records[hash]
	if !ok || !r.tracked {
		return TrackedTorrent{}, false
	}
	info := r.info
	r.tracked = false
	r.info = TrackedTorrent{}
	s.gc(hash, r)
	return info, true
}

// TrackedSnapshot returns a copy of the currently tracked torrents.
func (s *torrentStore) TrackedSnapshot() map[string]TrackedTorrent {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]TrackedTorrent)
	for hash, r := range s.records {
		if r.tracked {
			out[hash] = r.info
		}
	}
	return out
}

// TrackedHashes returns the tracked hashes as a set, for membership checks.
func (s *torrentStore) TrackedHashes() map[string]struct{} {
	snapshot := s.TrackedSnapshot()
	out := make(map[string]struct{}, len(snapshot))
	for hash := range snapshot {
		out[hash] = struct{}{}
	}
	return out
}

// TrackedCount returns the number of torrents currently being streamed.
func (s *torrentStore) TrackedCount() int { return s.count(isTracked) }

// RangeTracked calls fn for each tracked torrent. Iteration stops if fn
// returns false. fn runs outside the store lock, so it may call back into the
// store without deadlocking.
func (s *torrentStore) RangeTracked(fn func(hash string, info TrackedTorrent) bool) {
	for hash, info := range s.TrackedSnapshot() {
		if !fn(hash, info) {
			return
		}
	}
}

// --- Completion -------------------------------------------------------------

// MarkComplete records that the destination has finalized the torrent, with
// the selected-file fingerprint captured at that moment.
func (s *torrentStore) MarkComplete(hash, fingerprint string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.record(hash)
	r.complete = true
	r.fingerprint = fingerprint
}

// IsComplete reports whether the destination is known to have finalized the
// torrent.
func (s *torrentStore) IsComplete(hash string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.records[hash]
	return ok && r.complete
}

// ForgetComplete drops the completion entry for a torrent.
func (s *torrentStore) ForgetComplete(hash string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.forgetCompleteLocked(hash)
}

// ForgetCompleteAll drops completion entries for several torrents.
func (s *torrentStore) ForgetCompleteAll(hashes []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, hash := range hashes {
		s.forgetCompleteLocked(hash)
	}
}

// forgetCompleteLocked clears completion state. Caller must hold the write lock.
func (s *torrentStore) forgetCompleteLocked(hash string) {
	r, ok := s.records[hash]
	if !ok {
		return
	}
	r.complete = false
	r.fingerprint = ""
	s.gc(hash, r)
}

// CompletedSnapshot returns hash to selection-fingerprint for every torrent
// known complete on the destination.
func (s *torrentStore) CompletedSnapshot() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.completedLocked()
}

// completedLocked builds the completion map. Caller must hold at least a read
// lock.
func (s *torrentStore) completedLocked() map[string]string {
	out := make(map[string]string)
	for hash, r := range s.records {
		if r.complete {
			out[hash] = r.fingerprint
		}
	}
	return out
}

// CompletedKeys returns the hashes known complete on the destination.
func (s *torrentStore) CompletedKeys() []string {
	return slices.Collect(maps.Keys(s.CompletedSnapshot()))
}

// CompletedCount returns how many torrents are known complete on the
// destination.
func (s *torrentStore) CompletedCount() int { return s.count(isComplete) }

// --- Finalization retry accounting ------------------------------------------

// ShouldAttempt reports whether enough time has passed since the last failed
// finalization attempt to try again.
func (s *torrentStore) ShouldAttempt(hash string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	r, ok := s.records[hash]
	if !ok || r.failures <= 0 {
		// No failures recorded. Records created by RecordBusy alone must not
		// delay attempts, and the shift below needs failures >= 1 to be
		// well-defined.
		return true
	}

	backoff := min(
		minFinalizeBackoff*time.Duration(1<<uint(r.failures-1)),
		maxFinalizeBackoff,
	)
	return s.now().Sub(r.lastAttempt) >= backoff
}

// RecordFailure records a finalization failure and returns the number of
// consecutive failures for this torrent.
func (s *torrentStore) RecordFailure(hash string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.record(hash)
	r.failures++
	r.lastAttempt = s.now()
	return r.failures
}

// RecordBusy notes a destination-congestion response and returns how long this
// torrent has been continuously busy. Busy streaks do not count toward the
// failure cap and do not delay attempts; the streak resets when ClearBackoff
// runs on success.
func (s *torrentStore) RecordBusy(hash string) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.record(hash)
	if r.firstBusy.IsZero() {
		r.firstBusy = s.now()
	}
	return s.now().Sub(r.firstBusy)
}

// ClearBackoff drops finalization retry state for a torrent.
func (s *torrentStore) ClearBackoff(hash string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.records[hash]
	if !ok {
		return
	}
	r.failures = 0
	r.lastAttempt = time.Time{}
	r.firstBusy = time.Time{}
	s.gc(hash, r)
}

// BackoffCount returns how many torrents currently carry finalization retry
// state.
func (s *torrentStore) BackoffCount() int { return s.count((*torrentRecord).hasBackoff) }

// --- Persistence ------------------------------------------------------------

// Load reads the persisted completion cache from disk. A missing or corrupt
// file is non-fatal; the cache repopulates as torrents are re-checked.
func (s *torrentStore) Load() {
	if s.path == "" {
		return
	}

	data, err := os.ReadFile(s.path)
	if err != nil {
		if !os.IsNotExist(err) {
			s.logger.Warn("failed to read completed cache, starting fresh",
				"path", s.path,
				"error", err,
			)
		}
		return
	}

	var fingerprints map[string]string
	if jsonErr := json.Unmarshal(data, &fingerprints); jsonErr != nil {
		s.logger.Warn("failed to parse completed cache, starting fresh",
			"path", s.path,
			"error", jsonErr,
		)
		return
	}

	s.mu.Lock()
	for hash, fingerprint := range fingerprints {
		r := s.record(hash)
		r.complete = true
		r.fingerprint = fingerprint
	}
	s.mu.Unlock()

	s.logger.Info("loaded completed-on-destination cache",
		"count", len(fingerprints),
		"path", s.path,
	)
}

// Save atomically persists the completion cache to disk.
func (s *torrentStore) Save() {
	if s.path == "" {
		return
	}

	s.mu.RLock()
	snapshot := s.completedLocked()
	s.mu.RUnlock()

	data, err := json.Marshal(snapshot)
	if err != nil {
		s.logger.Warn("failed to marshal completed cache", "error", err)
		return
	}

	dir := filepath.Dir(s.path)
	if mkErr := os.MkdirAll(dir, 0o750); mkErr != nil {
		s.logger.Warn("failed to create cache directory", "path", dir, "error", mkErr)
		return
	}

	if writeErr := utils.AtomicWriteFile(s.path, data, cacheFilePermissions); writeErr != nil {
		s.logger.Warn("failed to write completed cache", "error", writeErr)
	}
}
