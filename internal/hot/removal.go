package hot

import (
	"context"

	"github.com/arsac/qb-sync/internal/metrics"
)

// listenForRemovals watches for torrents removed from hot qBittorrent
// and triggers cleanup on the cold server.
func (t *QBTask) listenForRemovals(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case hash, ok := <-t.tracker.Removed():
			if !ok {
				// Channel closed, tracker shutting down
				return nil
			}
			t.handleTorrentRemoval(ctx, hash)
		}
	}
}

// handleTorrentRemoval cleans up when a torrent is removed from hot qBittorrent.
// It notifies the cold server to abort the in-progress transfer and delete partial files.
func (t *QBTask) handleTorrentRemoval(ctx context.Context, hash string) {
	t.logger.InfoContext(ctx, "torrent removed from hot, cleaning up cold",
		"hash", hash,
	)

	t.trackedMu.Lock()
	tt, wasTracked := t.trackedTorrents[hash]
	delete(t.trackedTorrents, hash)
	t.trackedMu.Unlock()

	// Read completedOnCold but don't delete yet -- only remove after
	// StartTorrent succeeds so pruneCompletedOnCold can retry on failure.
	t.completedMu.RLock()
	wasCompletedOnCold := t.completedOnCold[hash]
	t.completedMu.RUnlock()

	t.clearFinalizeBackoff(hash)

	if wasTracked {
		metrics.OldestPendingSyncSeconds.DeleteLabelValues(hash, tt.name)
	} else {
		t.logger.DebugContext(ctx, "removed torrent was not in tracked list",
			"hash", hash,
		)
	}

	if t.cfg.DryRun {
		if wasCompletedOnCold {
			t.logger.InfoContext(ctx, "[dry-run] would start/tag torrent on cold",
				"hash", hash, "tag", t.cfg.SourceRemovedTag,
			)
		} else {
			t.logger.InfoContext(ctx, "[dry-run] would abort torrent on cold",
				"hash", hash,
			)
		}
		return
	}

	if wasCompletedOnCold {
		startCtx, cancel := context.WithTimeout(ctx, coldRPCTimeout)
		defer cancel()
		if startErr := t.grpcDest.StartTorrent(startCtx, hash, t.cfg.SourceRemovedTag); startErr != nil {
			t.logger.WarnContext(ctx, "failed to start/tag torrent on cold after removal (will retry via prune)",
				"hash", hash, "error", startErr,
			)
			return
		}

		t.completedMu.Lock()
		delete(t.completedOnCold, hash)
		metrics.CompletedOnColdCacheSize.Set(float64(len(t.completedOnCold)))
		t.completedMu.Unlock()
		t.saveCompletedCache()

		t.logger.InfoContext(ctx, "started and tagged torrent on cold after hot removal",
			"hash", hash, "tag", t.cfg.SourceRemovedTag,
		)
		return
	}

	abortCtx, cancel := context.WithTimeout(ctx, coldRPCTimeout)
	defer cancel()

	filesDeleted, err := t.grpcDest.AbortTorrent(abortCtx, hash, true)
	if err != nil {
		t.logger.WarnContext(ctx, "failed to abort torrent on cold (periodic cleanup will handle)",
			"hash", hash,
			"error", err,
		)
		return
	}

	t.logger.InfoContext(ctx, "aborted torrent on cold",
		"hash", hash,
		"filesDeleted", filesDeleted,
	)
}
