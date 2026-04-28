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

	tt, wasTracked := t.tracked.DeleteAndGet(hash)

	// Read completedOnDest but don't delete yet -- only remove after
	// StartTorrent succeeds so pruneCompletedOnDest can retry on failure.
	wasCompletedOnDest := t.completed.IsComplete(hash)

	t.backoffs.Clear(hash)

	if wasTracked {
		metrics.OldestPendingSyncSeconds.DeleteLabelValues(hash, tt.Name)
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
		startCtx, cancel := withDestRPCTimeout(ctx)
		defer cancel()
		if startErr := t.grpcDest.StartTorrent(startCtx, hash, t.cfg.SourceRemovedTag); startErr != nil {
			t.logger.WarnContext(ctx, "failed to start/tag torrent on destination after removal (will retry via prune)",
				"hash", hash, "error", startErr,
			)
			return
		}

		t.completed.Remove(hash)
		t.completed.Save()

		t.logger.InfoContext(ctx, "started and tagged torrent on destination after source removal",
			"hash", hash, "tag", t.cfg.SourceRemovedTag,
		)
		return
	}

	// Safety check: if streaming was fully complete on destination but finalization
	// hadn't run yet, aborting would delete fully-streamed data. Try to finalize
	// first to avoid this narrow race between streaming completion and finalization.
	if wasTracked && t.tryFinalizeFullyStreamed(ctx, hash) {
		return
	}

	abortCtx, cancel := withDestRPCTimeout(ctx)
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

// tryFinalizeFullyStreamed handles the race where streaming finished on
// destination but finalization hasn't run yet, when the torrent has just been
// removed from source. Returns true if the path was taken (caller should not
// fall through to abort), false if the torrent isn't fully streamed and
// abort should proceed.
//
// Without this path, the subsequent abort would delete fully-streamed data;
// if we instead finalize and then start, the destination retains the torrent
// tagged as source-removed.
func (t *QBTask) tryFinalizeFullyStreamed(ctx context.Context, hash string) bool {
	checkCtx, checkCancel := withDestRPCTimeout(ctx)
	result, checkErr := t.grpcDest.CheckTorrentStatus(checkCtx, hash)
	checkCancel()

	if checkErr != nil || result.PiecesNeededCount != 0 {
		return false
	}

	t.logger.InfoContext(ctx, "torrent fully streamed on destination, finalizing instead of aborting",
		"hash", hash,
	)

	// finalizeTorrent fetches source metadata via GetTorrentsCtx; if the
	// torrent is already gone from source (the common case once the user
	// has deleted it), the call fails and the .partial files remain on
	// destination for the orphan cleaner to handle.
	finalizeCtx, finalizeCancel := withDestRPCTimeout(ctx)
	finalizeErr := t.finalizeTorrent(finalizeCtx, hash)
	finalizeCancel()
	if finalizeErr != nil {
		t.logger.WarnContext(ctx, "failed to finalize fully-streamed torrent on removal",
			"hash", hash, "error", finalizeErr,
		)
		return true
	}

	startCtx, startCancel := withDestRPCTimeout(ctx)
	defer startCancel()
	if startErr := t.grpcDest.StartTorrent(startCtx, hash, t.cfg.SourceRemovedTag); startErr != nil {
		t.logger.WarnContext(ctx, "failed to start fully-streamed torrent on destination (will retry via prune)",
			"hash", hash, "error", startErr,
		)
	}
	return true
}
