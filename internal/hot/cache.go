package hot

import (
	"context"
	"encoding/json"
	"maps"
	"os"
	"path/filepath"
	"slices"

	"github.com/autobrr/go-qbittorrent"

	"github.com/arsac/qb-sync/internal/metrics"
	"github.com/arsac/qb-sync/internal/utils"
)

// FetchCompletedOnCold returns torrents known to be complete on cold.
// Exported for testing (used by E2E tests).
func (t *QBTask) FetchCompletedOnCold() []string {
	t.completedMu.RLock()
	defer t.completedMu.RUnlock()
	return slices.Collect(maps.Keys(t.completedOnCold))
}

// MarkCompletedOnCold marks a torrent as complete on cold.
// Exported for testing only - allows tests to simulate synced state.
func (t *QBTask) MarkCompletedOnCold(hash string) {
	t.completedMu.Lock()
	t.completedOnCold[hash] = ""
	t.completedMu.Unlock()
}

// loadCompletedCache reads the persisted completed-on-cold cache from disk.
// Supports both the new format (JSON object: hash→fingerprint) and the legacy
// format (JSON array of hashes). Missing or corrupt file is non-fatal.
func (t *QBTask) loadCompletedCache() {
	data, err := os.ReadFile(t.completedCachePath)
	if err != nil {
		if !os.IsNotExist(err) {
			t.logger.Warn("failed to read completed cache, starting fresh",
				"path", t.completedCachePath,
				"error", err,
			)
		}
		return
	}

	// Try new format first (JSON object: hash → fingerprint)
	var fingerprints map[string]string
	if json.Unmarshal(data, &fingerprints) == nil {
		t.completedMu.Lock()
		maps.Copy(t.completedOnCold, fingerprints)
		metrics.CompletedOnColdCacheSize.Set(float64(len(t.completedOnCold)))
		t.completedMu.Unlock()

		t.logger.Info("loaded completed-on-cold cache",
			"count", len(fingerprints),
			"path", t.completedCachePath,
		)
		return
	}

	// Fall back to legacy format (JSON array of hashes)
	var hashes []string
	if jsonErr := json.Unmarshal(data, &hashes); jsonErr != nil {
		t.logger.Warn("failed to parse completed cache, starting fresh",
			"path", t.completedCachePath,
			"error", jsonErr,
		)
		return
	}

	t.completedMu.Lock()
	for _, hash := range hashes {
		t.completedOnCold[hash] = "" // empty fingerprint triggers re-check
	}
	metrics.CompletedOnColdCacheSize.Set(float64(len(t.completedOnCold)))
	t.completedMu.Unlock()

	t.logger.Info("loaded completed-on-cold cache (legacy format, will re-check fingerprints)",
		"count", len(hashes),
		"path", t.completedCachePath,
	)
}

// saveCompletedCache atomically persists the completed-on-cold cache to disk.
// Caller must NOT hold completedMu.
func (t *QBTask) saveCompletedCache() {
	t.completedMu.RLock()
	snapshot := maps.Clone(t.completedOnCold)
	t.completedMu.RUnlock()

	data, err := json.Marshal(snapshot)
	if err != nil {
		t.logger.Warn("failed to marshal completed cache", "error", err)
		return
	}

	dir := filepath.Dir(t.completedCachePath)
	if mkErr := os.MkdirAll(dir, 0o750); mkErr != nil {
		t.logger.Warn("failed to create cache directory", "path", dir, "error", mkErr)
		return
	}

	if writeErr := utils.AtomicWriteFile(t.completedCachePath, data, cacheFilePermissions); writeErr != nil {
		t.logger.Warn("failed to write completed cache", "error", writeErr)
	}
}

// markCompletedOnCold marks a torrent as complete on cold with the given
// selection fingerprint, updates the metric, and persists the cache.
func (t *QBTask) markCompletedOnCold(hash, fingerprint string) {
	t.completedMu.Lock()
	t.completedOnCold[hash] = fingerprint
	metrics.CompletedOnColdCacheSize.Set(float64(len(t.completedOnCold)))
	t.completedMu.Unlock()

	t.saveCompletedCache()
}

// pruneCompletedOnCold removes entries from the completedOnCold cache that are
// no longer present in hot qBittorrent. This prevents unbounded growth when
// torrents are deleted from hot after being synced to cold.
func (t *QBTask) pruneCompletedOnCold(ctx context.Context) {
	torrents, err := t.srcClient.GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{})
	if err != nil {
		t.logger.WarnContext(ctx, "failed to fetch torrents for cache pruning", "error", err)
		return
	}

	hotHashes := make(map[string]struct{}, len(torrents))
	for _, torrent := range torrents {
		hotHashes[torrent.Hash] = struct{}{}
	}

	t.completedMu.Lock()
	var pruned int
	for hash := range t.completedOnCold {
		if _, exists := hotHashes[hash]; !exists {
			delete(t.completedOnCold, hash)
			pruned++
		}
	}
	remaining := len(t.completedOnCold)
	metrics.CompletedOnColdCacheSize.Set(float64(remaining))
	t.completedMu.Unlock()

	if pruned > 0 {
		t.logger.InfoContext(ctx, "pruned completed-on-cold cache",
			"pruned", pruned,
			"remaining", remaining,
		)
		t.saveCompletedCache()
	}
}
