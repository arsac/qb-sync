package source

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

// FetchCompletedOnDestination returns torrents known to be complete on destination.
// Exported for testing (used by E2E tests).
func (t *QBTask) FetchCompletedOnDestination() []string {
	t.completedMu.RLock()
	defer t.completedMu.RUnlock()
	return slices.Collect(maps.Keys(t.completedOnDest))
}

// MarkCompletedOnDestination marks a torrent as complete on destination.
// Exported for testing only - allows tests to simulate synced state.
func (t *QBTask) MarkCompletedOnDestination(hash string) {
	t.completedMu.Lock()
	t.completedOnDest[hash] = ""
	t.completedMu.Unlock()
}

// loadCompletedCache reads the persisted completed-on-destination cache from disk.
// Format: JSON object mapping hash → selection fingerprint.
// Missing or corrupt file is non-fatal (cache repopulates on next cycle).
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

	var fingerprints map[string]string
	if jsonErr := json.Unmarshal(data, &fingerprints); jsonErr != nil {
		t.logger.Warn("failed to parse completed cache, starting fresh",
			"path", t.completedCachePath,
			"error", jsonErr,
		)
		return
	}

	t.completedMu.Lock()
	maps.Copy(t.completedOnDest, fingerprints)
	metrics.CompletedOnDestCacheSize.Set(float64(len(t.completedOnDest)))
	t.completedMu.Unlock()

	t.logger.Info("loaded completed-on-destination cache",
		"count", len(fingerprints),
		"path", t.completedCachePath,
	)
}

// saveCompletedCache atomically persists the completed-on-destination cache to disk.
// Caller must NOT hold completedMu.
func (t *QBTask) saveCompletedCache() {
	t.completedMu.RLock()
	snapshot := maps.Clone(t.completedOnDest)
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

// markCompletedOnDest marks a torrent as complete on destination with the given
// selection fingerprint, updates the metric, and persists the cache.
func (t *QBTask) markCompletedOnDest(hash, fingerprint string) {
	t.completedMu.Lock()
	t.completedOnDest[hash] = fingerprint
	metrics.CompletedOnDestCacheSize.Set(float64(len(t.completedOnDest)))
	t.completedMu.Unlock()

	t.saveCompletedCache()
}

// pruneCompletedOnDest removes entries from the completedOnDest cache that are
// no longer present in source qBittorrent. This prevents unbounded growth when
// torrents are deleted from source after being synced to destination.
func (t *QBTask) pruneCompletedOnDest(ctx context.Context) {
	torrents, err := t.srcClient.GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{})
	if err != nil {
		t.logger.WarnContext(ctx, "failed to fetch torrents for cache pruning", "error", err)
		return
	}

	sourceHashes := make(map[string]struct{}, len(torrents))
	for _, torrent := range torrents {
		sourceHashes[torrent.Hash] = struct{}{}
	}

	t.completedMu.Lock()
	var pruned int
	for hash := range t.completedOnDest {
		if _, exists := sourceHashes[hash]; !exists {
			delete(t.completedOnDest, hash)
			pruned++
		}
	}
	remaining := len(t.completedOnDest)
	metrics.CompletedOnDestCacheSize.Set(float64(remaining))
	t.completedMu.Unlock()

	if pruned > 0 {
		t.logger.InfoContext(ctx, "pruned completed-on-destination cache",
			"pruned", pruned,
			"remaining", remaining,
		)
		t.saveCompletedCache()
	}
}
