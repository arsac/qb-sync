package source

import (
	"maps"
	"sync"
)

// TrackedSet is a thread-safe set of torrents currently being streamed.
type TrackedSet struct {
	torrents map[string]TrackedTorrent
	mu       sync.RWMutex
}

// NewTrackedSet creates a new TrackedSet.
func NewTrackedSet() *TrackedSet {
	return &TrackedSet{
		torrents: make(map[string]TrackedTorrent),
	}
}

// Add adds a torrent to the set. Overwrites if already present.
func (s *TrackedSet) Add(hash string, tt TrackedTorrent) {
	s.mu.Lock()
	s.torrents[hash] = tt
	s.mu.Unlock()
}

// Get returns the tracked torrent for the given hash and whether it exists.
func (s *TrackedSet) Get(hash string) (TrackedTorrent, bool) {
	s.mu.RLock()
	tt, ok := s.torrents[hash]
	s.mu.RUnlock()
	return tt, ok
}

// Delete removes a torrent from the set.
func (s *TrackedSet) Delete(hash string) {
	s.mu.Lock()
	delete(s.torrents, hash)
	s.mu.Unlock()
}

// Has returns true if the hash is in the set.
func (s *TrackedSet) Has(hash string) bool {
	s.mu.RLock()
	_, ok := s.torrents[hash]
	s.mu.RUnlock()
	return ok
}

// AddIfAbsent adds a torrent only if it doesn't already exist.
// Returns true if the torrent was added, false if it was already present.
func (s *TrackedSet) AddIfAbsent(hash string, tt TrackedTorrent) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.torrents[hash]; exists {
		return false
	}
	s.torrents[hash] = tt
	return true
}

// DeleteAndGet removes a torrent from the set and returns it.
func (s *TrackedSet) DeleteAndGet(hash string) (TrackedTorrent, bool) {
	s.mu.Lock()
	tt, ok := s.torrents[hash]
	delete(s.torrents, hash)
	s.mu.Unlock()
	return tt, ok
}

// Snapshot returns a shallow copy of the tracked torrents map.
func (s *TrackedSet) Snapshot() map[string]TrackedTorrent {
	s.mu.RLock()
	snapshot := maps.Clone(s.torrents)
	s.mu.RUnlock()
	return snapshot
}

// Count returns the number of tracked torrents.
func (s *TrackedSet) Count() int {
	s.mu.RLock()
	n := len(s.torrents)
	s.mu.RUnlock()
	return n
}

// Range calls fn for each tracked torrent while holding the read lock.
// If fn returns false, iteration stops.
func (s *TrackedSet) Range(fn func(hash string, tt TrackedTorrent) bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for hash, tt := range s.torrents {
		if !fn(hash, tt) {
			return
		}
	}
}
