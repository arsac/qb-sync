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

	"github.com/arsac/qb-sync/internal/config"
	"github.com/arsac/qb-sync/internal/utils"
)

// Finalization retry settings - exponential backoff.
const (
	minFinalizeBackoff = 2 * time.Second

	// maxFinalizeBackoffCeiling caps the derived backoff. The cap is computed
	// from the guard rather than fixed (see backoffCap): the backoff exists
	// only to bound how many attempts fit inside the guard window, so the two
	// must move together. A fixed 30s cap was shorter than the orchestrator
	// cycle interval, which made the backoff vestigial.
	maxFinalizeBackoffCeiling = 30 * time.Minute

	// backoffGuardDivisor sets how many backoff periods fit in a guard window,
	// and therefore roughly how many attempts a torrent gets before it is
	// quarantined.
	backoffGuardDivisor = 8

	// maxBackoffShift bounds the exponential so a long streak cannot overflow
	// the duration.
	maxBackoffShift = 32

	// streakFileName holds the persisted failure and stall clocks, alongside
	// the completion cache.
	streakFileName = "failure_streaks.json"
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
	//
	// firstBusy is persisted alongside the quarantine clocks for the same
	// reason: with it in memory only, a source restarting more often than the
	// busy guard could never surface a permanently wedged destination.
	failures    int
	lastAttempt time.Time
	firstBusy   time.Time

	// firstFailure starts the clock on a finalization failure streak, and
	// firstStalled on a stall. Quarantine is duration-based, so these are what
	// actually decide it; failures only gates how many attempts were made.
	//
	// They are modelled separately because they are different conditions. A
	// stall is one continuous state with no attempts to count, so "three
	// failures" is meaningless for it. Both are persisted: with in-memory
	// clocks, a source restarting more often than the guard could never
	// quarantine anything.
	//
	// firstStalled is cleared by any streaming advance. firstFailure is not,
	// because the INCOMPLETE path re-streams pieces between attempts and
	// clearing on advance would mean repeated verification failures never
	// quarantine at all.
	firstFailure time.Time
	firstStalled time.Time

	// lastStreamed is the highest streamed piece count seen for this torrent.
	// Persisted alongside firstStalled so a source restart cannot mistake a
	// resumed torrent for one that just made progress.
	lastStreamed int
}

// hasBackoff reports whether finalization retry state is recorded. This is
// what the old BackoffTracker expressed by the presence or absence of a map
// entry, and it is what the active-backoffs gauge counts. A stall is
// deliberately excluded: a stalled torrent is not retrying a finalization.
func (r *torrentRecord) hasBackoff() bool {
	return r.failures > 0 || !r.firstBusy.IsZero() || !r.firstFailure.IsZero()
}

// isStalled reports whether a stall clock is running.
func (r *torrentRecord) isStalled() bool {
	return !r.firstStalled.IsZero()
}

// hasStallState reports whether any stall bookkeeping is present. lastStreamed
// must keep a record alive on its own: dropping it would let a resumed torrent
// look like it had just advanced.
func (r *torrentRecord) hasStallState() bool {
	return r.isStalled() || r.lastStreamed > 0
}

// hasStreakState reports whether any persisted clock or streaming position is
// recorded. Spelled once so adding a clock cannot silently skip one of the
// load, save and prune sites that all need to agree.
func (r *torrentRecord) hasStreakState() bool {
	return !r.firstFailure.IsZero() || !r.firstStalled.IsZero() ||
		!r.firstBusy.IsZero() || r.lastStreamed > 0
}

// clearStreaks drops every persisted clock and the streaming position.
func (r *torrentRecord) clearStreaks() {
	r.firstFailure = time.Time{}
	r.firstStalled = time.Time{}
	r.firstBusy = time.Time{}
	r.lastStreamed = 0
}

// isEmpty reports whether the record carries no remaining state and can be
// dropped.
func (r *torrentRecord) isEmpty() bool {
	return !r.tracked && !r.complete && !r.hasBackoff() && !r.hasStallState()
}

// torrentStore holds per-torrent source state behind a single lock, and
// persists the completion cache to disk.
//
// Thread-safe. Callers never see *torrentRecord; every accessor copies out
// what it needs under the lock.
type torrentStore struct {
	mu      sync.RWMutex
	records map[string]*torrentRecord

	path       string // completion cache path; empty disables persistence
	streakPath string // streak sidecar path; empty disables persistence
	logger     *slog.Logger

	// guard is how long a fault must persist continuously before it
	// quarantines the torrent. It also derives the finalization backoff cap.
	guard time.Duration

	// streaksDirty records that a clock or streaming position changed since the
	// last save. It suppresses the write only on a genuinely idle cycle: any
	// torrent that advances marks the store dirty, so an active sync still
	// writes the sidecar every cycle. That is cheap and correct — it is not the
	// per-transition optimisation the name might suggest.
	streaksDirty bool

	now func() time.Time // injectable for tests
}

// newTorrentStore creates a store. Pass an empty path to disable persistence.
func newTorrentStore(path string, logger *slog.Logger) *torrentStore {
	streakPath := ""
	if path != "" {
		streakPath = filepath.Join(filepath.Dir(path), streakFileName)
	}
	return &torrentStore{
		records:    make(map[string]*torrentRecord),
		path:       path,
		streakPath: streakPath,
		logger:     logger,
		guard:      config.DefaultSyncFailedGuard,
		now:        time.Now,
	}
}

// Guard returns the duration a fault must persist before it quarantines a
// torrent. Callers log this rather than re-deriving it from config, so the
// value reported is always the one the decision used.
func (s *torrentStore) Guard() time.Duration { return s.guard }

// backoffCap derives the finalization backoff ceiling from the guard so that
// roughly backoffGuardDivisor attempts fit inside a guard window at any guard
// value. Without this the guard window would permit hundreds of attempts, and
// the disk-stage-error path re-verifies every piece while holding the
// destination's disk-stage semaphore.
func (s *torrentStore) backoffCap() time.Duration {
	return min(maxFinalizeBackoffCeiling, s.guard/backoffGuardDivisor)
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
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]struct{}, len(s.records))
	for hash, r := range s.records {
		if r.tracked {
			out[hash] = struct{}{}
		}
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

	// Shift is bounded so a long streak cannot overflow the duration.
	shift := min(r.failures-1, maxBackoffShift)
	backoff := min(
		minFinalizeBackoff*time.Duration(1<<uint(shift)),
		s.backoffCap(),
	)
	return s.now().Sub(r.lastAttempt) >= backoff
}

// RecordFailure records a finalization failure. It returns the number of
// consecutive failures and whether the torrent should now be quarantined.
//
// Quarantine needs both: enough attempts to show the failure is not a one-off,
// and enough elapsed time to show it is not a passing outage. The attempt count
// alone used to decide this, and because the backoff was shorter than the
// orchestrator cycle, three attempts spanned about ninety seconds.
func (s *torrentStore) RecordFailure(hash string) (int, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	r := s.record(hash)
	r.failures++
	r.lastAttempt = s.now()
	if r.firstFailure.IsZero() {
		r.firstFailure = s.now()
		s.streaksDirty = true
	}

	quarantine := r.failures >= minQuarantineAttempts &&
		s.now().Sub(r.firstFailure) >= s.guard
	return r.failures, quarantine
}

// ObserveStall reports a torrent's streaming position and whether it currently
// meets the stall condition, and returns how long it has been stalled and
// whether it should now be quarantined.
//
// There is no attempt count here: a stall is one continuous condition, not a
// series of failed attempts, so only its duration is meaningful.
//
// Progress is judged by the streamed piece count rather than a timestamp.
// A timestamp would break across a source restart, where a torrent resumes
// from the destination's bitmap and so appears to have just advanced; the
// count is persisted, so a resumed torrent has to exceed what it had reached
// before to count as advancing.
//
// The first observation of a torrent with pieces already streamed reads as an
// advance, because there is no earlier count to compare against. It only
// establishes the baseline, costing one cycle before the clock can start -
// immaterial against a guard measured in hours.
func (s *torrentStore) ObserveStall(
	hash string,
	streamed int,
	stalling bool,
) (time.Duration, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	r := s.record(hash)

	if streamed > r.lastStreamed {
		r.lastStreamed = streamed
		s.streaksDirty = true
		r.firstStalled = time.Time{}
		s.gc(hash, r)
		return 0, false
	}

	if !stalling {
		s.gc(hash, r)
		return 0, false
	}

	if r.firstStalled.IsZero() {
		r.firstStalled = s.now()
		s.streaksDirty = true
	}

	stalledFor := s.now().Sub(r.firstStalled)
	return stalledFor, stalledFor >= s.guard
}

// ClearStall forgets the stall clock and streaming position. Called when a
// torrent stops being tracked, so a later re-track starts clean.
func (s *torrentStore) ClearStall(hash string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.records[hash]
	if !ok || !r.hasStallState() {
		return
	}
	r.firstStalled = time.Time{}
	r.lastStreamed = 0
	s.streaksDirty = true
	s.gc(hash, r)
}

// StalledCount returns how many torrents currently have a stall clock running.
func (s *torrentStore) StalledCount() int { return s.count((*torrentRecord).isStalled) }

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
		s.streaksDirty = true
	}
	return s.now().Sub(r.firstBusy)
}

// ClearBackoff drops finalization retry state for a torrent, including the
// failure clock. Called when a torrent finalizes successfully or stops being
// tracked.
func (s *torrentStore) ClearBackoff(hash string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.records[hash]
	if !ok {
		return
	}
	if !r.firstFailure.IsZero() || !r.firstBusy.IsZero() {
		s.streaksDirty = true
	}
	r.failures = 0
	r.lastAttempt = time.Time{}
	r.firstBusy = time.Time{}
	r.firstFailure = time.Time{}
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

// persistedStreak is the on-disk form of a torrent's quarantine clocks.
type persistedStreak struct {
	FirstFailure time.Time `json:"firstFailure,omitzero"`
	FirstStalled time.Time `json:"firstStalled,omitzero"`
	FirstBusy    time.Time `json:"firstBusy,omitzero"`
	LastStreamed int       `json:"lastStreamed,omitempty"`
}

// LoadStreaks restores the persisted quarantine clocks. Without this a source
// restarting more often than the guard could never quarantine anything, which
// would silently deliver "never terminal" while the guard claims otherwise.
func (s *torrentStore) LoadStreaks() {
	if s.streakPath == "" {
		return
	}

	data, err := os.ReadFile(s.streakPath)
	if err != nil {
		if !os.IsNotExist(err) {
			s.logger.Warn("failed to read streak file, starting fresh",
				"path", s.streakPath, "error", err)
		}
		return
	}

	var streaks map[string]persistedStreak
	if jsonErr := json.Unmarshal(data, &streaks); jsonErr != nil {
		s.logger.Warn("failed to parse streak file, starting fresh",
			"path", s.streakPath, "error", jsonErr)
		return
	}

	s.mu.Lock()
	for hash, st := range streaks {
		r := s.record(hash)
		r.firstFailure = st.FirstFailure
		r.firstStalled = st.FirstStalled
		r.firstBusy = st.FirstBusy
		r.lastStreamed = st.LastStreamed
		// A row carrying nothing would otherwise leave an empty record behind.
		s.gc(hash, r)
	}
	s.mu.Unlock()

	s.logger.Info("loaded quarantine streak clocks",
		"count", len(streaks), "path", s.streakPath)
}

// SaveStreaks persists the quarantine clocks, but only when one has changed
// since the last save. Stall clocks are set and cleared frequently during
// normal congestion, so writing every cycle would be pointless disk traffic.
func (s *torrentStore) SaveStreaks() {
	if s.streakPath == "" {
		return
	}

	s.mu.Lock()
	if !s.streaksDirty {
		s.mu.Unlock()
		return
	}
	streaks := make(map[string]persistedStreak)
	for hash, r := range s.records {
		if !r.hasStreakState() {
			continue
		}
		streaks[hash] = persistedStreak{
			FirstFailure: r.firstFailure,
			FirstStalled: r.firstStalled,
			FirstBusy:    r.firstBusy,
			LastStreamed: r.lastStreamed,
		}
	}
	s.streaksDirty = false
	s.mu.Unlock()

	data, err := json.Marshal(streaks)
	if err != nil {
		s.logger.Warn("failed to marshal streak file", "error", err)
		return
	}

	if mkErr := os.MkdirAll(filepath.Dir(s.streakPath), 0o750); mkErr != nil {
		s.logger.Warn("failed to create cache directory", "error", mkErr)
		return
	}

	if writeErr := utils.AtomicWriteFile(s.streakPath, data, cacheFilePermissions); writeErr != nil {
		s.logger.Warn("failed to write streak file", "error", writeErr)
	}
}

// PruneStreaks drops streak clocks for torrents no longer present on the
// source, so the sidecar cannot grow without bound.
func (s *torrentStore) PruneStreaks(present map[string]struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for hash, r := range s.records {
		if _, ok := present[hash]; ok {
			continue
		}
		if !r.hasStreakState() {
			continue
		}
		r.clearStreaks()
		s.streaksDirty = true
		s.gc(hash, r)
	}
}
