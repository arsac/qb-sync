package source

import (
	"context"

	"github.com/arsac/qb-sync/internal/metrics"
)

// listenForRemovals watches for torrents removed from source qBittorrent
// and triggers cleanup on the destination server.
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

// handleTorrentRemoval cleans up when a torrent is removed from source qBittorrent.
// It notifies the destination server to abort the in-progress transfer and delete partial files.
func (t *QBTask) handleTorrentRemoval(ctx context.Context, hash string) {
	t.logger.InfoContext(ctx, "torrent removed from source, cleaning up destination",
		"hash", hash,
	)

	t.trackedMu.Lock()
	tt, wasTracked := t.trackedTorrents[hash]
	delete(t.trackedTorrents, hash)
	t.trackedMu.Unlock()

	// Read completedOnDest but don't delete yet -- only remove after
	// StartTorrent succeeds so pruneCompletedOnDest can retry on failure.
	t.completedMu.RLock()
	_, wasCompletedOnDest := t.completedOnDest[hash]
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
		if wasCompletedOnDest {
			t.logger.InfoContext(ctx, "[dry-run] would start/tag torrent on destination",
				"hash", hash, "tag", t.cfg.SourceRemovedTag,
			)
		} else {
			t.logger.InfoContext(ctx, "[dry-run] would abort torrent on destination",
				"hash", hash,
			)
		}
		return
	}

	if wasCompletedOnDest {
		startCtx, cancel := context.WithTimeout(ctx, destRPCTimeout)
		defer cancel()
		if startErr := t.grpcDest.StartTorrent(startCtx, hash, t.cfg.SourceRemovedTag); startErr != nil {
			t.logger.WarnContext(ctx, "failed to start/tag torrent on destination after removal (will retry via prune)",
				"hash", hash, "error", startErr,
			)
			return
		}

		t.completedMu.Lock()
		delete(t.completedOnDest, hash)
		metrics.CompletedOnDestCacheSize.Set(float64(len(t.completedOnDest)))
		t.completedMu.Unlock()
		t.saveCompletedCache()

		t.logger.InfoContext(ctx, "started and tagged torrent on destination after source removal",
			"hash", hash, "tag", t.cfg.SourceRemovedTag,
		)
		return
	}

	abortCtx, cancel := context.WithTimeout(ctx, destRPCTimeout)
	defer cancel()

	filesDeleted, err := t.grpcDest.AbortTorrent(abortCtx, hash, true)
	if err != nil {
		t.logger.WarnContext(ctx, "failed to abort torrent on destination (periodic cleanup will handle)",
			"hash", hash,
			"error", err,
		)
		return
	}

	t.logger.InfoContext(ctx, "aborted torrent on destination",
		"hash", hash,
		"filesDeleted", filesDeleted,
	)
}
