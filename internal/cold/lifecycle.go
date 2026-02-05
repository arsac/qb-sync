package cold

import (
	"context"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/arsac/qb-sync/internal/metrics"
)

// runPeriodic runs a function periodically with optional initial delay.
// If initialDelay is true, waits interval before first execution.
// Returns when ctx is cancelled.
func runPeriodic(ctx context.Context, interval time.Duration, initialDelay bool, fn func(context.Context)) {
	if initialDelay {
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		fn(ctx)

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// runPeriodicWaitFirst runs a function periodically, waiting before each execution.
// Returns when ctx is cancelled.
func runPeriodicWaitFirst(ctx context.Context, interval time.Duration, fn func(context.Context)) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fn(ctx)
		}
	}
}

// runStateFlusher periodically flushes dirty state to disk.
func (s *Server) runStateFlusher(ctx context.Context) {
	interval := s.config.StateFlushInterval
	if interval == 0 {
		interval = defaultStateFlushInterval
	}

	runPeriodicWaitFirst(ctx, interval, s.flushDirtyStates)
}

// flushDirtyStates saves state for all torrents marked as dirty.
// Uses consistent lock ordering: collect references with s.mu, then acquire state.mu individually.
func (s *Server) flushDirtyStates(ctx context.Context) {
	// Collect all torrent references while holding s.mu (no state locks here)
	s.mu.RLock()
	torrents := s.collectTorrents()
	s.mu.RUnlock()

	// Process each torrent individually with only state.mu held
	for _, t := range torrents {
		t.state.mu.Lock()
		if t.state.dirty && t.state.statePath != "" {
			if saveErr := s.saveState(t.state.statePath, t.state.written); saveErr != nil {
				s.logger.WarnContext(ctx, "failed to flush state",
					"hash", t.hash,
					"error", saveErr,
				)
			} else {
				t.state.dirty = false
				t.state.piecesSinceFlush = 0
				s.logger.DebugContext(ctx, "flushed state",
					"hash", t.hash,
					"written", countWritten(t.state.written),
				)
			}
		}
		t.state.mu.Unlock()
	}
}

// runOrphanCleaner periodically scans for and cleans up orphaned torrents.
// A torrent is considered orphaned if:
// 1. It's not actively tracked in memory (not in s.torrents)
// 2. Its state file hasn't been modified for longer than OrphanTimeout
// This handles cases where hot crashes or loses connection unexpectedly.
func (s *Server) runOrphanCleaner(ctx context.Context) {
	interval := s.config.OrphanCleanupInterval
	if interval == 0 {
		interval = defaultOrphanCleanupInterval
	}

	runPeriodic(ctx, interval, true, s.cleanupOrphanedTorrents)
}

// cleanupOrphanedTorrents scans the .meta directory for orphaned torrents.
func (s *Server) cleanupOrphanedTorrents(ctx context.Context) {
	timeout := s.config.OrphanTimeout
	if timeout == 0 {
		timeout = defaultOrphanTimeout
	}

	metaDir := filepath.Join(s.config.BasePath, ".meta")

	entries, readErr := os.ReadDir(metaDir)
	if readErr != nil {
		if !os.IsNotExist(readErr) {
			s.logger.WarnContext(ctx, "failed to read meta directory for orphan cleanup",
				"error", readErr,
			)
		}
		return
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		hash := entry.Name()
		if s.isOrphanedTorrent(ctx, hash, timeout) {
			s.cleanupOrphan(ctx, hash)
		}
	}
}

// isOrphanedTorrent checks if a torrent should be considered orphaned.
func (s *Server) isOrphanedTorrent(ctx context.Context, hash string, timeout time.Duration) bool {
	// Check if actively tracked in memory
	s.mu.RLock()
	_, tracked := s.torrents[hash]
	s.mu.RUnlock()

	if tracked {
		return false
	}

	// Check state file modification time
	statePath := filepath.Join(s.config.BasePath, ".meta", hash, ".state")
	info, statErr := os.Stat(statePath)
	if statErr != nil {
		if os.IsNotExist(statErr) {
			// No state file - check files.json for creation time
			filesPath := filepath.Join(s.config.BasePath, ".meta", hash, filesInfoFileName)
			filesInfo, filesStatErr := os.Stat(filesPath)
			if filesStatErr != nil {
				// No metadata at all - probably already cleaned up or corrupted
				return false
			}
			info = filesInfo
		} else {
			s.logger.DebugContext(ctx, "failed to stat state file",
				"hash", hash,
				"error", statErr,
			)
			return false
		}
	}

	lastModified := info.ModTime()
	age := time.Since(lastModified)

	if age > timeout {
		s.logger.InfoContext(ctx, "found orphaned torrent",
			"hash", hash,
			"lastModified", lastModified,
			"age", age.Round(time.Minute),
			"timeout", timeout,
		)
		return true
	}

	return false
}

// cleanupOrphan removes all data associated with an orphaned torrent.
// Uses abortingHashes to prevent race with concurrent InitTorrent calls.
func (s *Server) cleanupOrphan(ctx context.Context, hash string) {
	// Register cleanup to prevent concurrent InitTorrent from creating files
	// that we're about to delete. Uses same pattern as AbortTorrent.
	cleanupCh := make(chan struct{})

	s.mu.Lock()
	// Check if actively tracked
	if _, tracked := s.torrents[hash]; tracked {
		s.mu.Unlock()
		s.logger.DebugContext(ctx, "skipping orphan cleanup, torrent now tracked",
			"hash", hash,
		)
		return
	}
	// Check if already being cleaned up or aborted
	if _, cleaning := s.abortingHashes[hash]; cleaning {
		s.mu.Unlock()
		s.logger.DebugContext(ctx, "skipping orphan cleanup, cleanup already in progress",
			"hash", hash,
		)
		return
	}
	// Register that we're cleaning up this hash
	s.abortingHashes[hash] = cleanupCh
	s.mu.Unlock()

	// Ensure we clean up the abort registration when done
	defer func() {
		s.mu.Lock()
		delete(s.abortingHashes, hash)
		close(cleanupCh)
		s.mu.Unlock()
	}()

	metaDir := filepath.Join(s.config.BasePath, ".meta", hash)

	// Load file info to find partial files
	info, loadErr := s.loadFilesInfo(metaDir)
	if loadErr != nil {
		// Don't delete meta dir if we can't load file info - we'd leave partial files orphaned
		// with no way to find them. Leave meta dir for next cleanup cycle or manual intervention.
		s.logger.WarnContext(ctx, "skipping orphan cleanup, cannot load file info (may need manual cleanup)",
			"hash", hash,
			"metaDir", metaDir,
			"error", loadErr,
		)
		return
	}

	// Delete partial files
	var filesDeleted int
	for _, f := range info.Files {
		// f.Path is the partial file path
		if err := os.Remove(f.Path); err == nil {
			filesDeleted++
		} else if !os.IsNotExist(err) {
			s.logger.DebugContext(ctx, "failed to remove orphan partial file",
				"hash", hash,
				"path", f.Path,
				"error", err,
			)
		}

		// Also try to remove the non-partial version (may have been partially finalized)
		finalPath := strings.TrimSuffix(f.Path, partialSuffix)
		if finalPath != f.Path {
			if err := os.Remove(finalPath); err == nil {
				filesDeleted++
			}
		}
	}

	// Remove meta directory
	if err := os.RemoveAll(metaDir); err != nil && !os.IsNotExist(err) {
		s.logger.WarnContext(ctx, "failed to remove orphan meta directory",
			"hash", hash,
			"path", metaDir,
			"error", err,
		)
	}

	metrics.OrphanCleanupsTotal.Inc()

	s.logger.InfoContext(ctx, "cleaned up orphaned torrent",
		"hash", hash,
		"filesDeleted", filesDeleted,
	)
}

// runInodeCleaner periodically removes stale entries from the inode-to-path map.
// An entry is stale if the file no longer exists on disk (e.g., was deleted externally).
// This prevents unbounded memory growth in long-running servers.
func (s *Server) runInodeCleaner(ctx context.Context) {
	interval := s.config.InodeCleanupInterval
	if interval == 0 {
		interval = defaultInodeCleanupInterval
	}

	runPeriodic(ctx, interval, true, s.cleanupStaleInodes)
}

// cleanupStaleInodes removes entries from inodeToPath where the file no longer exists.
func (s *Server) cleanupStaleInodes(ctx context.Context) {
	// Collect all entries to check (avoid holding lock during file stat operations)
	s.inodeMu.RLock()
	entries := make(map[uint64]string, len(s.inodeToPath))
	maps.Copy(entries, s.inodeToPath)
	s.inodeMu.RUnlock()

	if len(entries) == 0 {
		return
	}

	// Check each entry
	staleInodes := make([]uint64, 0)
	for inode, relPath := range entries {
		fullPath := filepath.Join(s.config.BasePath, relPath)
		if _, err := os.Stat(fullPath); os.IsNotExist(err) {
			staleInodes = append(staleInodes, inode)
		}
	}

	if len(staleInodes) == 0 {
		return
	}

	// Remove stale entries
	s.inodeMu.Lock()
	for _, inode := range staleInodes {
		// Double-check the path hasn't changed since we read it
		if currentPath, ok := s.inodeToPath[inode]; ok && currentPath == entries[inode] {
			delete(s.inodeToPath, inode)
		}
	}
	s.inodeMu.Unlock()

	s.logger.InfoContext(ctx, "cleaned up stale inode entries",
		"removed", len(staleInodes),
		"remaining", len(s.inodeToPath),
	)
}
