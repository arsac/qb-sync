package destination

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"runtime/debug"
	"time"

	"github.com/autobrr/go-qbittorrent"

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
// Uses consistent lock ordering: collect references via store.ForEach, then acquire state.mu individually.
func (s *Server) flushDirtyStates(ctx context.Context) {
	// Process each torrent: snapshot state under lock, then do I/O outside it.
	// This prevents a slow/hung filesystem from holding state.mu and blocking
	// WritePiece or FinalizeTorrent for the same torrent.
	s.store.ForEach(func(hash string, state *serverTorrentState) bool {
		state.mu.Lock()
		if !state.dirty || state.statePath == "" {
			state.mu.Unlock()
			return true
		}
		statePath := state.statePath
		snapshot := state.written.Clone()
		flushedCount := state.piecesSinceFlush
		snapshotGen := state.flushGen
		state.mu.Unlock()

		flushStart := time.Now()
		if saveErr := s.doSaveState(statePath, snapshot); saveErr != nil {
			metrics.StateSaveErrorsTotal.WithLabelValues(metrics.ModeDestination).Inc()
			s.logger.WarnContext(ctx, "failed to flush state",
				"hash", hash,
				"error", saveErr,
			)
			return true
		}

		metrics.StateFlushDuration.Observe(time.Since(flushStart).Seconds())

		state.mu.Lock()
		// If an inline flush occurred while we were writing, our snapshot is stale.
		// The inline flush already wrote a newer state to disk -- skip bookkeeping
		// to avoid clearing dirty/piecesSinceFlush for pieces not in our snapshot.
		if state.flushGen != snapshotGen {
			state.mu.Unlock()
			return true
		}
		state.flushGen++
		state.piecesSinceFlush -= flushedCount
		if state.piecesSinceFlush <= 0 {
			state.dirty = false
			state.piecesSinceFlush = 0
		}
		state.mu.Unlock()

		s.logger.DebugContext(ctx, "flushed state",
			"hash", hash,
			"written", snapshot.Count(),
		)
		return true
	})
}

// runOrphanCleaner periodically scans for and cleans up orphaned torrents.
// A torrent is considered orphaned if:
// 1. It's not actively tracked in memory (not in the torrent store)
// 2. Its state file hasn't been modified for longer than OrphanTimeout
// This handles cases where source crashes or loses connection unexpectedly.
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
			s.cleanupOrphan(ctx, hash, timeout)
		}
	}
}

// isStaleState reports whether in-memory state shows no source interest for
// longer than timeout. Shared by the orphan scan and the re-test taken under
// the store lock, so the two cannot drift apart.
func (s *Server) isStaleState(state *serverTorrentState, timeout time.Duration) bool {
	if state.initializing.Load() {
		return false // mid-init; its files are being created right now
	}

	state.mu.Lock()
	finalizing := state.finalization.active
	state.mu.Unlock()
	if finalizing {
		// Verification of a large torrent can run for a long time with no
		// source contact. Reclaiming it would delete data mid-check.
		return false
	}

	return state.contactAge(s.processStart) > timeout
}

// isOrphanedTorrent checks if a torrent should be considered orphaned.
//
// Orphan-ness is judged on how long it has been since a source asked about the
// torrent, not on whether it is present in the store. Membership cannot answer
// the question: startup recovery repopulates the store from every unfinalized
// metadata directory, so an orphan is put straight back and shielded again.
// Judging on membership would leave this check unable to fire for the very case
// it exists for - a source that crashed or was decommissioned.
//
// Metadata mtime is only a fallback for torrents with no in-memory state. It
// cannot be the primary signal because it records flushes, not activity: a
// healthy, fully streamed torrent has a frozen mtime while it waits through the
// finalization queue and a congestion streak, which can run to hours.
func (s *Server) isOrphanedTorrent(ctx context.Context, hash string, timeout time.Duration) bool {
	// Finalized marker means the torrent was successfully synced — not an orphan.
	if s.isFinalized(hash) {
		return false
	}

	// peek, not Get: Get stamps contact, which would refresh the very timestamp
	// this check is testing and guarantee nothing is ever reclaimed.
	if state, present := s.store.peek(hash); present {
		if !s.isStaleState(state, timeout) {
			return false
		}
		s.logger.InfoContext(ctx, "found orphaned torrent",
			"hash", hash,
			"sinceLastContact", state.contactAge(s.processStart).Round(time.Second),
			"timeout", timeout,
		)
		return true
	}

	// No in-memory state: fall back to metadata mtime.
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
		"age", age.Round(time.Second),
		"timeout", timeout,
	)
	return true
}

// statOrphanMetadata returns FileInfo for the torrent's metadata, checking
// .state first, then .meta. Returns [os.ErrNotExist] when no metadata file
// is found.
func (s *Server) statOrphanMetadata(metaDir string) (os.FileInfo, error) {
	statePath := filepath.Join(metaDir, stateFileName)
	info, err := os.Stat(statePath)
	if err == nil {
		return info, nil
	}
	if !os.IsNotExist(err) {
		return nil, err
	}

	metaPath := filepath.Join(metaDir, metaFileName)
	return os.Stat(metaPath)
}

// cleanupOrphan removes all data associated with an orphaned torrent.
// Uses abortingHashes to prevent race with concurrent InitTorrent calls.
func (s *Server) cleanupOrphan(ctx context.Context, hash string, timeout time.Duration) {
	// Register cleanup to prevent concurrent InitTorrent from creating files
	// that we're about to delete. Uses same pattern as AbortTorrent.
	//
	// An orphan may still hold in-memory state, because store membership no
	// longer decides orphan-ness (see isOrphanedTorrent). BeginReclaim drops
	// the entry when there is one, so the store cannot shield an orphan the way
	// it did before this change.
	cleanupCh := make(chan struct{})
	registered := s.store.BeginReclaim(hash, cleanupCh, func(st *serverTorrentState) bool {
		// Re-tested under the store lock: a source may have resumed the torrent
		// since the scan decided it was stale, and deleting its files now would
		// pull them from under an active transfer.
		return s.isStaleState(st, timeout)
	})
	if !registered {
		s.logger.DebugContext(ctx, "skipping orphan cleanup, torrent is active or already cleaning",
			"hash", hash,
		)
		return
	}

	// Ensure we clean up the abort registration when done
	defer func() {
		close(cleanupCh) // Signal waiters before deregistering
		s.store.EndCleanup(hash)
	}()

	// Final safety check: if the torrent exists in destination qBittorrent,
	// it was successfully added at some point — do not delete its files.
	// This covers the narrow crash window between addAndVerifyTorrent and
	// markFinalized where the .finalized marker was not written.
	// Fail-closed: if QB is unreachable, skip cleanup to avoid data loss.
	// Metric is labeled by reason so operators can distinguish "qB owns it"
	// (healthy skip) from "qB unreachable, orphans accumulating" (broken).
	if s.qbClient != nil {
		torrent, found, qbErr := s.getQBTorrent(ctx, hash)
		switch {
		case qbErr != nil:
			metrics.OrphanCleanupSkippedTotal.WithLabelValues(metrics.ReasonOrphanQBUnreachable).Inc()
			s.logger.WarnContext(ctx, "skipping orphan cleanup, destination qBittorrent unreachable",
				"hash", hash, "error", qbErr,
			)
			return
		case found && isReadyState(torrent.State) && torrent.Progress >= 1.0:
			s.healOrphan(ctx, hash, torrent)
			return
		case found:
			// Download-side, checking, or error states: qB owns the entry but
			// the data is not known-good — never write the marker, never
			// delete. The source's retry/sync-failed flow owns recovery.
			metrics.OrphanCleanupSkippedTotal.WithLabelValues(metrics.ReasonOrphanInQB).Inc()
			s.logger.InfoContext(ctx, "skipping orphan cleanup, torrent exists in destination qBittorrent",
				"hash", hash,
				"state", torrent.State,
			)
			return
		}
	}

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

// deleteOrphanFiles loads the .meta file in metaDir to locate and remove the
// torrent's .partial files. Returns the number of files deleted.
//
// Only .partial paths are removed, never a file at its final path. Everything
// this server writes lives at a .partial path until finalizeFiles renames it,
// so on an unfinalized torrent - and an orphan is by definition unfinalized -
// a file sitting at its final path is one of:
//
//   - pre-existing operator data that setupFile adopted at the right size,
//   - a hardlink to another torrent's file, or
//   - a deselected file we never created.
//
// None of those are ours to delete. AbortTorrent can be more precise because it
// holds the in-memory hardlink results; orphan cleanup only has the persisted
// metadata, which does not record provenance, so it stays conservative. See
// docs/adr/0002-destination-reclamation.md.
//
// The cost is that files left at their final path by a crashed mid-finalization
// rename are not reclaimed here. They are adopted as pre-existing on any retry,
// so they are reusable rather than garbage.
func (s *Server) deleteOrphanFiles(ctx context.Context, hash, metaDir string) int {
	metaPath := filepath.Join(metaDir, metaFileName)
	meta, loadErr := loadPersistedMeta(metaPath)
	if loadErr != nil {
		s.logger.WarnContext(ctx, "cannot load metadata for orphan cleanup",
			"hash", hash, "error", loadErr)
		return 0
	}

	var deleted int
	subPath := meta.GetSaveSubPath()
	for _, f := range meta.GetFiles() {
		partialPath := filepath.Join(s.config.BasePath, subPath, f.GetPath()) + partialSuffix

		if err := os.Remove(partialPath); err == nil {
			deleted++
		} else if !os.IsNotExist(err) {
			s.logger.DebugContext(ctx, "failed to remove orphan partial file",
				"hash", hash, "path", partialPath, "error", err)
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
		s.store.Inodes().CleanupStale(ctx)
	})
}

// healOrphan converts a stale unfinalized orphan into a finalized torrent when
// destination qB reports it seeding-side complete (the caller checked that).
// The sync objective — data seeding in destination qB — is met regardless of
// which path qB seeds from, matching the path-independent COMPLETE that
// checkQBCompletion already returns to the source. So heal unconditionally:
// no savepath comparison, no .meta load. Writing the marker makes this a
// one-time event instead of an hourly skip.
//
// This deletes no data: markFinalized only clears the metadata sidecar, never
// torrent files. In the rare cross-seed case (same hash added independently at
// a different path) our streamed copy simply lingers as reclaimable disk —
// wasted space, never lost data — which is a better trade than an eternal skip
// log and an inconsistency with the source's own completion check.
func (s *Server) healOrphan(ctx context.Context, hash string, torrent *qbittorrent.Torrent) {
	metaDir := filepath.Join(s.config.BasePath, metaDirName, hash)
	s.markFinalized(metaDir, hash)
	if !s.isFinalized(hash) {
		// Marker write failed (markFinalized logs the cause). Don't count a
		// heal that didn't happen — the next orphan scan will retry.
		return
	}

	metrics.OrphanCleanupHealedTotal.Inc()
	s.logger.InfoContext(ctx, "healed orphan: destination qBittorrent reports torrent complete",
		"hash", hash,
		"state", torrent.State,
		"qbSavePath", torrent.SavePath,
	)
}
