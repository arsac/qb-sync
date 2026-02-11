package cold

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"runtime/debug"
	"time"

	"github.com/arsac/qb-sync/internal/metrics"
)

// runPeriodic runs fn periodically, waiting interval before each execution.
// Returns when ctx is cancelled. Recovers from panics to keep the background
// loop running — a panic in one tick should not crash the server.
func runPeriodic(
	ctx context.Context,
	interval time.Duration,
	logger *slog.Logger,
	name string,
	fn func(context.Context),
) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			func() {
				defer func() {
					if r := recover(); r != nil {
						logger.ErrorContext(ctx, "panic in periodic task",
							"task", name,
							"panic", r,
							"stack", string(debug.Stack()),
						)
					}
				}()
				fn(ctx)
			}()
		}
	}
}

// runStateFlusher periodically flushes dirty state to disk.
func (s *Server) runStateFlusher(ctx context.Context) {
	interval := s.config.StateFlushInterval
	if interval == 0 {
		interval = defaultStateFlushInterval
	}

	runPeriodic(ctx, interval, s.logger, "state-flusher", s.flushDirtyStates)
}

// flushDirtyStates saves state for all torrents marked as dirty.
// Uses consistent lock ordering: collect references with s.mu, then acquire state.mu individually.
func (s *Server) flushDirtyStates(ctx context.Context) {
	// Collect all torrent references while holding s.mu (no state locks here)
	s.mu.RLock()
	metrics.ActiveTorrents.WithLabelValues(metrics.ModeCold).Set(float64(len(s.torrents)))
	torrents := s.collectTorrents()
	s.mu.RUnlock()

	// Process each torrent: snapshot state under lock, then do I/O outside it.
	// This prevents a slow/hung filesystem from holding state.mu and blocking
	// WritePiece or FinalizeTorrent for the same torrent.
	var dirtyAfterFlush int
	for _, t := range torrents {
		t.state.mu.Lock()
		if !t.state.dirty || t.state.statePath == "" {
			if t.state.dirty {
				dirtyAfterFlush++
			}
			t.state.mu.Unlock()
			continue
		}
		statePath := t.state.statePath
		snapshot := make([]bool, len(t.state.written))
		copy(snapshot, t.state.written)
		flushedCount := t.state.piecesSinceFlush
		t.state.mu.Unlock()

		flushStart := time.Now()
		if saveErr := s.doSaveState(statePath, snapshot); saveErr != nil {
			metrics.StateSaveErrorsTotal.WithLabelValues(metrics.ModeCold).Inc()
			s.logger.WarnContext(ctx, "failed to flush state",
				"hash", t.hash,
				"error", saveErr,
			)
			dirtyAfterFlush++
			continue
		}

		metrics.StateFlushDuration.Observe(time.Since(flushStart).Seconds())

		t.state.mu.Lock()
		t.state.piecesSinceFlush -= flushedCount
		if t.state.piecesSinceFlush <= 0 {
			t.state.dirty = false
			t.state.piecesSinceFlush = 0
		} else {
			dirtyAfterFlush++
		}
		t.state.mu.Unlock()

		s.logger.DebugContext(ctx, "flushed state",
			"hash", t.hash,
			"written", countWritten(snapshot),
		)
	}
	metrics.TorrentsWithDirtyState.Set(float64(dirtyAfterFlush))
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

	runPeriodic(ctx, interval, s.logger, "orphan-cleaner", s.cleanupOrphanedTorrents)
}

// cleanupOrphanedTorrents scans the metadata directory for orphaned torrents.
func (s *Server) cleanupOrphanedTorrents(ctx context.Context) {
	timeout := s.config.OrphanTimeout
	if timeout == 0 {
		timeout = defaultOrphanTimeout
	}

	metaDir := filepath.Join(s.config.BasePath, metaDirName)

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
	metaDir := filepath.Join(s.config.BasePath, metaDirName, hash)
	info, statErr := s.statOrphanMetadata(metaDir)
	if statErr != nil {
		if !os.IsNotExist(statErr) {
			s.logger.DebugContext(ctx, "failed to stat metadata for orphan check",
				"hash", hash,
				"error", statErr,
			)
		}
		return false
	}

	age := time.Since(info.ModTime())
	if age <= timeout {
		return false
	}

	s.logger.InfoContext(ctx, "found orphaned torrent",
		"hash", hash,
		"lastModified", info.ModTime(),
		"age", age.Round(time.Minute),
		"timeout", timeout,
	)
	return true
}

// statOrphanMetadata returns FileInfo for the torrent's metadata, checking
// .state first and falling back to the .torrent file. Returns [os.ErrNotExist]
// when neither file exists.
func (s *Server) statOrphanMetadata(metaDir string) (os.FileInfo, error) {
	statePath := filepath.Join(metaDir, ".state")
	info, err := os.Stat(statePath)
	if err == nil {
		return info, nil
	}
	if !os.IsNotExist(err) {
		return nil, err
	}

	// No state file - check for .torrent file as fallback timestamp
	torrentPath, findErr := findTorrentFile(metaDir)
	if findErr != nil {
		return nil, os.ErrNotExist
	}
	return os.Stat(torrentPath)
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

	metaDir := filepath.Join(s.config.BasePath, metaDirName, hash)

	filesDeleted := s.deleteOrphanFiles(ctx, hash, metaDir)

	// Always remove meta directory to prevent unbounded growth
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

// deleteOrphanFiles parses the .torrent file in metaDir to locate and remove
// data files (both .partial and finalized versions). Returns the number of files deleted.
func (s *Server) deleteOrphanFiles(ctx context.Context, hash, metaDir string) int {
	torrentPath, findErr := findTorrentFile(metaDir)
	if findErr != nil {
		s.logger.WarnContext(ctx, "orphan cleanup without torrent file, .partial files may remain on disk",
			"hash", hash, "metaDir", metaDir, "error", findErr)
		return 0
	}

	torrentData, readErr := os.ReadFile(torrentPath)
	if readErr != nil {
		s.logger.WarnContext(ctx, "orphan cleanup: failed to read torrent file",
			"hash", hash, "error", readErr)
		return 0
	}

	parsed, parseErr := parseTorrentFile(torrentData)
	if parseErr != nil {
		s.logger.WarnContext(ctx, "orphan cleanup: failed to parse torrent file",
			"hash", hash, "error", parseErr)
		return 0
	}

	var deleted int
	subPath := loadSubPathFile(metaDir)
	for _, f := range parsed.Files {
		filePath := filepath.Join(s.config.BasePath, subPath, f.Path)

		// Try to remove .partial version
		partialPath := filePath + partialSuffix
		if err := os.Remove(partialPath); err == nil {
			deleted++
		} else if !os.IsNotExist(err) {
			s.logger.DebugContext(ctx, "failed to remove orphan partial file",
				"hash", hash, "path", partialPath, "error", err)
		}

		// Also try to remove the finalized version (may have been partially finalized)
		if err := os.Remove(filePath); err == nil {
			deleted++
		}
	}

	return deleted
}

// runInodeCleaner periodically removes stale entries from the inode-to-path map.
// An entry is stale if the file no longer exists on disk (e.g., was deleted externally).
// This prevents unbounded memory growth in long-running servers.
func (s *Server) runInodeCleaner(ctx context.Context) {
	interval := s.config.InodeCleanupInterval
	if interval == 0 {
		interval = defaultInodeCleanupInterval
	}

	runPeriodic(ctx, interval, s.logger, "inode-cleaner", func(ctx context.Context) {
		s.inodes.CleanupStale(ctx)
		if saveErr := s.inodes.Save(); saveErr != nil {
			s.logger.WarnContext(ctx, "failed to persist inode map after cleanup", "error", saveErr)
		}
	})
}
