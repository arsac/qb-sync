package source

import (
	"encoding/json"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/arsac/qb-sync/internal/metrics"
	"github.com/arsac/qb-sync/internal/utils"
)

// CompletionCache tracks torrents known to be complete on destination.
// Thread-safe with internal locking. Persists to disk as JSON.
type CompletionCache struct {
	completed map[string]string
	mu        sync.RWMutex
	path      string
	logger    *slog.Logger
}

// NewCompletionCache creates a new CompletionCache.
// Pass an empty path to disable persistence (useful for tests).
func NewCompletionCache(path string, logger *slog.Logger) *CompletionCache {
	return &CompletionCache{
		completed: make(map[string]string),
		path:      path,
		logger:    logger,
	}
}

// Keys returns all hashes known to be complete on destination.
func (c *CompletionCache) Keys() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return slices.Collect(maps.Keys(c.completed))
}

// Mark marks a torrent as complete on destination with an empty fingerprint.
func (c *CompletionCache) Mark(hash string) {
	c.mu.Lock()
	c.completed[hash] = ""
	c.mu.Unlock()
}

// IsComplete returns true if the hash is known to be complete on destination.
func (c *CompletionCache) IsComplete(hash string) bool {
	c.mu.RLock()
	_, ok := c.completed[hash]
	c.mu.RUnlock()
	return ok
}

// MarkWithFingerprint marks a torrent as complete on destination with the given
// selection fingerprint and updates the metric.
func (c *CompletionCache) MarkWithFingerprint(hash, fingerprint string) {
	c.mu.Lock()
	c.completed[hash] = fingerprint
	metrics.CompletedOnDestCacheSize.Set(float64(len(c.completed)))
	c.mu.Unlock()
}

// Remove removes a single hash from the cache and updates the metric.
func (c *CompletionCache) Remove(hash string) {
	c.mu.Lock()
	delete(c.completed, hash)
	metrics.CompletedOnDestCacheSize.Set(float64(len(c.completed)))
	c.mu.Unlock()
}

// RemoveAll removes multiple hashes from the cache and updates the metric.
func (c *CompletionCache) RemoveAll(hashes []string) {
	c.mu.Lock()
	for _, hash := range hashes {
		delete(c.completed, hash)
	}
	metrics.CompletedOnDestCacheSize.Set(float64(len(c.completed)))
	c.mu.Unlock()
}

// Snapshot returns a shallow copy of the completed map.
func (c *CompletionCache) Snapshot() map[string]string {
	c.mu.RLock()
	snapshot := maps.Clone(c.completed)
	c.mu.RUnlock()
	return snapshot
}

// Count returns the number of entries in the cache.
func (c *CompletionCache) Count() int {
	c.mu.RLock()
	n := len(c.completed)
	c.mu.RUnlock()
	return n
}

// Load reads the persisted completed-on-destination cache from disk.
// Format: JSON object mapping hash -> selection fingerprint.
// Missing or corrupt file is non-fatal (cache repopulates on next cycle).
func (c *CompletionCache) Load() {
	if c.path == "" {
		return
	}

	data, err := os.ReadFile(c.path)
	if err != nil {
		if !os.IsNotExist(err) {
			c.logger.Warn("failed to read completed cache, starting fresh",
				"path", c.path,
				"error", err,
			)
		}
		return
	}

	var fingerprints map[string]string
	if jsonErr := json.Unmarshal(data, &fingerprints); jsonErr != nil {
		c.logger.Warn("failed to parse completed cache, starting fresh",
			"path", c.path,
			"error", jsonErr,
		)
		return
	}

	c.mu.Lock()
	maps.Copy(c.completed, fingerprints)
	metrics.CompletedOnDestCacheSize.Set(float64(len(c.completed)))
	c.mu.Unlock()

	c.logger.Info("loaded completed-on-destination cache",
		"count", len(fingerprints),
		"path", c.path,
	)
}

// Save atomically persists the completed-on-destination cache to disk.
func (c *CompletionCache) Save() {
	if c.path == "" {
		return
	}

	snapshot := c.Snapshot()

	data, err := json.Marshal(snapshot)
	if err != nil {
		c.logger.Warn("failed to marshal completed cache", "error", err)
		return
	}

	dir := filepath.Dir(c.path)
	if mkErr := os.MkdirAll(dir, 0o750); mkErr != nil {
		c.logger.Warn("failed to create cache directory", "path", dir, "error", mkErr)
		return
	}

	if writeErr := utils.AtomicWriteFile(c.path, data, cacheFilePermissions); writeErr != nil {
		c.logger.Warn("failed to write completed cache", "error", writeErr)
	}
}

// Prune removes entries from the cache that are not present in sourceHashes.
// Returns the number of entries pruned.
func (c *CompletionCache) Prune(sourceHashes map[string]struct{}) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	var pruned int
	for hash := range c.completed {
		if _, exists := sourceHashes[hash]; !exists {
			delete(c.completed, hash)
			pruned++
		}
	}
	metrics.CompletedOnDestCacheSize.Set(float64(len(c.completed)))
	return pruned
}
