package hot

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"time"

	"github.com/autobrr/go-qbittorrent"

	"github.com/arsac/qb-sync/internal/metrics"
	"github.com/arsac/qb-sync/internal/streaming"
)

// finalizeCompletedStreams checks for streams where all pieces are streamed
// and calls FinalizeTorrent on the cold server.
//
//nolint:unparam // error return kept for interface consistency; errors handled internally
func (t *QBTask) finalizeCompletedStreams(ctx context.Context) error {
	t.trackedMu.RLock()
	tracked := make(map[string]trackedTorrent, len(t.trackedTorrents))
	maps.Copy(tracked, t.trackedTorrents)
	t.trackedMu.RUnlock()

	for hash := range tracked {
		progress, err := t.tracker.GetProgress(hash)
		if err != nil {
			t.logger.DebugContext(ctx, "GetProgress failed",
				"hash", hash,
				"error", err,
			)
			continue
		}

		t.logger.DebugContext(ctx, "checking stream progress",
			"hash", hash,
			"streamed", progress.Streamed,
			"total", progress.TotalPieces,
			"complete", progress.Complete,
		)

		if !progress.Complete {
			continue
		}

		if !t.shouldAttemptFinalize(hash) {
			continue
		}

		if finalizeErr := t.finalizeTorrent(ctx, hash); finalizeErr != nil {
			if errors.Is(finalizeErr, streaming.ErrFinalizeVerifying) {
				t.logger.InfoContext(ctx, "cold server still verifying, will poll again",
					"hash", hash,
				)
				continue
			}

			// Cold says pieces are missing — hot's streamed state diverged from
			// cold's written state (e.g., cold restarted with stale flush).
			// Re-sync: clear init cache, re-init to get actual PiecesNeeded,
			// and reset tracker so missing pieces get re-streamed.
			if errors.Is(finalizeErr, streaming.ErrFinalizeIncomplete) {
				t.logger.WarnContext(ctx, "cold reports incomplete, re-syncing streamed state",
					"hash", hash,
					"error", finalizeErr,
				)
				t.resyncWithCold(ctx, hash)
				continue
			}

			metrics.FinalizationErrorsTotal.WithLabelValues(metrics.ModeHot).Inc()
			t.logger.ErrorContext(ctx, "finalize failed",
				"hash", hash,
				"error", finalizeErr,
			)
			t.recordFinalizeFailure(hash)
			if streaming.IsTransientError(finalizeErr) {
				t.logger.WarnContext(ctx, "cold server unreachable, skipping remaining finalizations",
					"error", finalizeErr,
				)
				break
			}
			continue
		}

		t.markTorrentSynced(ctx, hash, tracked[hash])
	}

	return nil
}

// markTorrentSynced handles post-finalization bookkeeping: clears backoff, updates
// caches and metrics, removes tracking state, and applies the synced tag.
func (t *QBTask) markTorrentSynced(ctx context.Context, hash string, tt trackedTorrent) {
	t.clearFinalizeBackoff(hash)

	metrics.TorrentSyncLatencySeconds.Observe(time.Since(tt.completionTime).Seconds())
	t.markCompletedOnCold(hash)

	t.tracker.Untrack(hash)
	t.source.EvictCache(hash)
	t.trackedMu.Lock()
	delete(t.trackedTorrents, hash)
	t.trackedMu.Unlock()

	metrics.TorrentsSyncedTotal.WithLabelValues(metrics.ModeHot, hash, tt.name).Inc()
	metrics.TorrentBytesSyncedTotal.WithLabelValues(hash, tt.name).Add(float64(tt.size))
	metrics.OldestPendingSyncSeconds.DeleteLabelValues(hash, tt.name)

	t.logger.InfoContext(ctx, "torrent synced successfully", "hash", hash)

	if t.cfg.SyncedTag != "" && !t.cfg.DryRun {
		if tagErr := t.srcClient.AddTagsCtx(ctx, []string{hash}, t.cfg.SyncedTag); tagErr != nil {
			metrics.TagApplicationErrorsTotal.WithLabelValues(metrics.ModeHot).Inc()
			t.logger.ErrorContext(ctx, "failed to add synced tag",
				"hash", hash,
				"tag", t.cfg.SyncedTag,
				"error", tagErr,
			)
		}
	}
}

// finalizeTorrent calls the cold server to finalize the torrent.
func (t *QBTask) finalizeTorrent(ctx context.Context, hash string) error {
	torrents, err := t.srcClient.GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
		Hashes: []string{hash},
	})
	if err != nil {
		return fmt.Errorf("getting torrent info: %w", err)
	}
	if len(torrents) == 0 {
		return fmt.Errorf("torrent not found: %s", hash)
	}

	torrent := torrents[0]
	saveSubPath := t.source.ResolveSubPath(torrent.SavePath)

	t.logger.InfoContext(ctx, "finalizing torrent on cold",
		"name", torrent.Name,
		"hash", hash,
		"savePath", torrent.SavePath,
		"saveSubPath", saveSubPath,
	)

	if t.cfg.DryRun {
		return nil
	}

	return t.grpcDest.FinalizeTorrent(ctx, hash, torrent.SavePath, torrent.Category, torrent.Tags, saveSubPath)
}

// shouldAttemptFinalize checks if enough time has passed since the last failed
// finalization attempt. Returns true if we should try again.
func (t *QBTask) shouldAttemptFinalize(hash string) bool {
	t.backoffMu.Lock()
	defer t.backoffMu.Unlock()

	backoff, exists := t.finalizeBackoffs[hash]
	if !exists {
		return true
	}

	backoffDuration := min(minFinalizeBackoff*time.Duration(1<<uint(backoff.failures-1)), maxFinalizeBackoff)

	return time.Since(backoff.lastAttempt) >= backoffDuration
}

// recordFinalizeFailure records a finalization failure for backoff tracking.
func (t *QBTask) recordFinalizeFailure(hash string) {
	t.backoffMu.Lock()
	defer t.backoffMu.Unlock()

	backoff, exists := t.finalizeBackoffs[hash]
	if !exists {
		backoff = &finalizeBackoff{}
		t.finalizeBackoffs[hash] = backoff
	}

	backoff.failures++
	backoff.lastAttempt = time.Now()
	metrics.ActiveFinalizationBackoffs.Set(float64(len(t.finalizeBackoffs)))
}

// clearFinalizeBackoff removes backoff tracking for a successfully finalized torrent.
func (t *QBTask) clearFinalizeBackoff(hash string) {
	t.backoffMu.Lock()
	defer t.backoffMu.Unlock()
	delete(t.finalizeBackoffs, hash)
	metrics.ActiveFinalizationBackoffs.Set(float64(len(t.finalizeBackoffs)))
}

// invertPiecesNeeded converts PiecesNeeded (true=missing) to written (true=have).
func invertPiecesNeeded(piecesNeeded []bool) []bool {
	written := make([]bool, len(piecesNeeded))
	for i, needed := range piecesNeeded {
		written[i] = !needed
	}
	return written
}

// resyncWithCold re-initializes a torrent on cold to discover which pieces are
// actually written, then resets the tracker's streamed state to match. This
// recovers from divergence after a cold restart where flushed state was stale.
func (t *QBTask) resyncWithCold(ctx context.Context, hash string) {
	t.grpcDest.ClearInitResult(hash)

	meta, ok := t.tracker.GetTorrentMetadata(hash)
	if !ok {
		t.logger.ErrorContext(ctx, "resync failed: torrent metadata not found",
			"hash", hash,
		)
		return
	}

	result, initErr := t.grpcDest.InitTorrent(ctx, meta.InitTorrentRequest)
	if initErr != nil {
		t.logger.ErrorContext(ctx, "resync failed: InitTorrent error",
			"hash", hash,
			"error", initErr,
		)
		return
	}

	if result == nil || len(result.PiecesNeeded) == 0 {
		t.logger.InfoContext(ctx, "resync: cold reports all pieces written",
			"hash", hash,
		)
		return
	}

	writtenOnCold := invertPiecesNeeded(result.PiecesNeeded)
	reset := t.tracker.ResyncStreamed(hash, writtenOnCold)

	t.logger.InfoContext(ctx, "resync complete, pieces will be re-streamed",
		"hash", hash,
		"piecesReset", reset,
		"coldHas", result.PiecesHaveCount,
		"coldNeeds", result.PiecesNeededCount,
	)
}
