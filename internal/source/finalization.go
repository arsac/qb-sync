package source

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/autobrr/go-qbittorrent"

	"github.com/arsac/qb-sync/internal/metrics"
	"github.com/arsac/qb-sync/internal/streaming"
)

// finalizeCompletedStreams checks for streams where all pieces are streamed
// and calls FinalizeTorrent on the destination server.
//
//nolint:unparam // error return kept for interface consistency
func (t *QBTask) finalizeCompletedStreams(ctx context.Context) error {
	tracked := t.store.TrackedSnapshot()

	for hash := range tracked {
		progress, err := t.tracker.GetProgress(hash)
		if err != nil {
			t.logger.DebugContext(ctx, "GetProgress failed", "hash", hash, "error", err)
			continue
		}

		t.logger.DebugContext(ctx, "checking stream progress",
			"hash", hash,
			"streamed", progress.Streamed,
			"total", progress.TotalPieces,
			"complete", progress.Complete,
		)

		if !progress.Complete || !t.store.ShouldAttempt(hash) {
			continue
		}

		finalizeErr := t.finalizeTorrent(ctx, hash)
		if finalizeErr == nil {
			t.markTorrentSynced(ctx, hash, tracked[hash])
			continue
		}
		if t.handleFinalizeError(ctx, hash, finalizeErr) {
			break
		}
	}

	return nil
}

// handleFinalizeError dispatches a non-nil finalize error to the appropriate
// handler. Returns true if the caller should stop iterating (e.g. destination
// is unreachable and remaining finalizations would just pile up errors).
func (t *QBTask) handleFinalizeError(ctx context.Context, hash string, finalizeErr error) bool {
	// BUSY = destination-wide congestion (finalize queue saturated, or qB
	// still rechecking at budget expiry) — not a per-torrent fault. Poll
	// again without burning the retry budget, but bound it with a wall-clock
	// guard so a permanently wedged destination still surfaces as sync-failed.
	if errors.Is(finalizeErr, streaming.ErrFinalizeBusy) {
		if busyFor := t.store.RecordBusy(hash); busyFor < busyGuardDuration {
			t.logger.WarnContext(ctx, "destination finalization busy, will retry",
				"hash", hash,
				"busyFor", busyFor.Round(time.Second),
				"guard", busyGuardDuration,
			)
			return false
		}
		t.logger.ErrorContext(ctx, "destination busy beyond wall-clock guard, counting as failure",
			"hash", hash,
			"guard", busyGuardDuration,
		)
		// Fall through to the generic failure accounting below. BUSY errors
		// are plain wrapped errors (no gRPC status), so IsTransientError
		// stays false and the failure reaches RecordFailure.
	}

	switch {
	case errors.Is(finalizeErr, streaming.ErrFinalizeVerifying):
		t.logger.InfoContext(ctx, "destination server still verifying, will poll again", "hash", hash)
		return false
	case errors.Is(finalizeErr, streaming.ErrFinalizeIncomplete):
		t.handleIncompleteFinalization(ctx, hash)
		return false
	case errors.Is(finalizeErr, streaming.ErrFinalizeNotFound):
		t.handleNotFoundFinalization(ctx, hash)
		return false
	}

	metrics.FinalizationErrorsTotal.WithLabelValues(metrics.ModeSource).Inc()
	t.logger.ErrorContext(ctx, "finalize failed", "hash", hash, "error", finalizeErr)

	// Transient gRPC errors are destination-wide outages, not per-torrent
	// failures. Don't record a failure (cycle interval is the backoff) and
	// don't count toward the per-torrent retry cap — otherwise a brief dest
	// outage would mark every in-flight torrent as sync-failed.
	if streaming.IsTransientError(finalizeErr) {
		t.logger.WarnContext(ctx, "destination server unreachable, skipping remaining finalizations",
			"error", finalizeErr,
		)
		return true
	}

	// Persistent per-torrent failure (e.g. destination qB stuck in missingFiles).
	// Shares the failure streak with handleIncompleteFinalization so the torrent
	// surfaces as sync-failed instead of looping in tracked forever.
	failures, quarantine := t.store.RecordFailure(hash)
	if quarantine {
		t.logger.ErrorContext(ctx, "finalize failing past the guard, marking torrent as sync-failed",
			"hash", hash,
			"failures", failures,
			"guard", t.syncFailedGuard(),
			"error", finalizeErr,
		)
		t.markSyncFailed(ctx, hash)
	}
	return false
}

// syncFailedGuard returns the configured quarantine guard, falling back to the
// default when unset (tests construct QBTask directly).
func (t *QBTask) syncFailedGuard() time.Duration {
	if t.cfg != nil && t.cfg.SyncFailedGuard > 0 {
		return t.cfg.SyncFailedGuard
	}
	return defaultSyncFailedGuard
}

// stallGracePeriod is how long a tracked torrent may sit without advancing
// before the stall clock starts. It only has to outlast normal jitter - the
// queue draining, a congested destination, a slow disk read - because the
// guard, not this, decides quarantine.
func (t *QBTask) stallGracePeriod() time.Duration {
	const (
		minGrace     = time.Minute
		cyclesOfSlop = 2 // tolerate a couple of quiet cycles before starting the clock
	)
	if t.cfg != nil && t.cfg.SleepInterval > 0 {
		return max(minGrace, cyclesOfSlop*t.cfg.SleepInterval)
	}
	return minGrace
}

// checkStalledStreams quarantines torrents that have pieces waiting on the
// source but are not moving them.
//
// This closes the hole that made an unreadable piece unrecoverable. A failed
// send only sets a bit in a bitmap nothing reads, and the next poll re-queues
// the piece, so the torrent never completes, never reaches finalization, and
// therefore never reaches the failure streak that would quarantine it. It stays
// tracked forever, retrying the same unreadable piece.
//
// The available-pieces condition is what separates a wedged torrent from one
// whose source is merely slow to download: a source waiting on peers has
// nothing available, so it can never be judged stalled.
func (t *QBTask) checkStalledStreams(ctx context.Context) {
	grace := t.stallGracePeriod()

	for hash := range t.store.TrackedSnapshot() {
		progress, err := t.tracker.GetProgress(hash)
		if err != nil {
			continue
		}

		// No IsZero guard: the monitor stamps lastAdvance when tracking starts,
		// so a torrent that has never streamed a piece is the wedged case, not
		// an unknown one. Excluding it here would skip the very torrents this
		// check exists to catch.
		stalling := !progress.Complete &&
			progress.Available > 0 &&
			time.Since(progress.LastAdvance) >= grace

		stalledFor, quarantine := t.store.ObserveStall(hash, progress.Streamed, stalling)
		if !quarantine {
			continue
		}

		t.logger.ErrorContext(ctx, "torrent stalled past the guard, marking as sync-failed",
			"hash", hash,
			"stalledFor", stalledFor.Round(time.Second),
			"guard", t.syncFailedGuard(),
			"streamed", progress.Streamed,
			"total", progress.TotalPieces,
			"available", progress.Available,
			// Failed is the count of pieces whose send or read failed. It is
			// the first thing to look at when diagnosing a stall: a high count
			// points at unreadable source data rather than a wedged pipeline.
			"failed", progress.Failed,
		)
		t.markSyncFailed(ctx, hash)
	}
}

// markTorrentSynced handles post-finalization bookkeeping: clears backoff, updates
// caches and metrics, removes tracking state, and applies the synced tag.
func (t *QBTask) markTorrentSynced(ctx context.Context, hash string, tt TrackedTorrent) {
	// Compute fingerprint before evicting source cache
	fingerprint := t.computeSelectionFingerprint(ctx, hash)

	metrics.TorrentSyncLatencySeconds.Observe(time.Since(tt.CompletionTime).Seconds())
	t.store.MarkComplete(hash, fingerprint)
	t.store.Save()

	t.releaseTorrent(hash)

	selection := t.computeSelectionLabel(ctx, hash)
	metrics.SyncOutcomesTotal.WithLabelValues(metrics.ModeSource, metrics.ResultSynced, selection).Inc()
	metrics.BytesSyncedTotal.WithLabelValues(metrics.ModeSource, selection).Add(float64(tt.Size))

	t.logger.InfoContext(ctx, "torrent synced successfully", "hash", hash)

	t.applySyncedTag(ctx, hash)
}

// applySyncedTag adds the configured synced tag to the source torrent.
// Non-fatal: logs and increments a metric on failure so the torrent is retried
// on the next cleanup cycle (fetchTorrentsCompletedOnDest skips untagged entries
// and calls applySyncedTag again, so transient API errors self-heal).
func (t *QBTask) applySyncedTag(ctx context.Context, hash string) {
	if t.cfg.SyncedTag == "" || t.cfg.DryRun {
		return
	}
	if tagErr := t.srcClient.AddTagsCtx(ctx, []string{hash}, t.cfg.SyncedTag); tagErr != nil {
		metrics.TagApplicationErrorsTotal.WithLabelValues(metrics.ModeSource).Inc()
		t.logger.ErrorContext(ctx, "failed to add synced tag",
			"hash", hash,
			"tag", t.cfg.SyncedTag,
			"error", tagErr,
		)
	}
}

// finalizeTorrent calls the destination server to finalize the torrent.
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

	// Derive saveSubPath from ContentPath + file root rather than torrent.SavePath:
	// SavePath drifts from disk reality after Auto-TMM moves or Set Location.
	qbFiles, filesErr := t.cycleFilesFor(ctx, hash)
	if filesErr != nil {
		return fmt.Errorf("getting torrent files: %w", filesErr)
	}
	saveSubPath := t.source.CanonicalSubPath(torrent, qbFiles)

	t.logger.InfoContext(ctx, "finalizing torrent on destination",
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

// handleIncompleteFinalization handles a FINALIZE_ERROR_INCOMPLETE response from
// the destination. Tracks the failure streak to prevent infinite verify→re-stream
// loops. Once the streak outlasts the guard, tags the torrent as sync-failed so
// the user can investigate. Removing the tag re-enables sync.
func (t *QBTask) handleIncompleteFinalization(ctx context.Context, hash string) {
	failures, quarantine := t.store.RecordFailure(hash)
	if quarantine {
		t.logger.ErrorContext(ctx, "verification failing past the guard, marking torrent as sync-failed",
			"hash", hash,
			"failures", failures,
			"guard", t.syncFailedGuard(),
		)
		t.markSyncFailed(ctx, hash)
		return
	}
	t.logger.WarnContext(ctx, "destination reports incomplete, re-syncing streamed state",
		"hash", hash,
		"attempt", failures,
		"maxRetries", maxVerificationRetries,
	)
	t.resyncWithDest(ctx, hash)
}

// handleNotFoundFinalization handles a FINALIZE_ERROR_NOT_FOUND response from
// the destination. The destination has no state for this torrent (metadata missing
// or data files externally deleted). Untrack so the next poll cycle re-discovers
// and re-initializes the torrent from scratch.
func (t *QBTask) handleNotFoundFinalization(ctx context.Context, hash string) {
	metrics.FinalizeNotFoundTotal.Inc()
	t.logger.WarnContext(ctx, "destination has no state for torrent, untracking for re-init",
		"hash", hash,
	)
	t.stopTracking(hash)
}

// markSyncFailed tags the torrent as sync-failed on source qBittorrent and stops
// tracking it. The user can remove the tag to re-enable sync.
func (t *QBTask) markSyncFailed(ctx context.Context, hash string) {
	if tag := t.cfg.SyncFailedTag; tag != "" && !t.cfg.DryRun {
		if tagErr := t.srcClient.AddTagsCtx(ctx, []string{hash}, tag); tagErr != nil {
			metrics.TagApplicationErrorsTotal.WithLabelValues(metrics.ModeSource).Inc()
			t.logger.ErrorContext(ctx, "failed to apply sync-failed tag",
				"hash", hash,
				"tag", tag,
				"error", tagErr,
			)
		}
	}

	// Capture selection before untracking, so the outcome metric carries the
	// label. Defaults to "full" on API error — preserves the historical
	// behavior where sync_failed_total had no label distinction.
	selection := t.computeSelectionLabel(ctx, hash)

	t.releaseDestination(ctx, hash)

	// Stop tracking so the torrent is not re-streamed.
	// It will be picked up again if the user removes the tag.
	t.stopTracking(hash)

	metrics.SyncOutcomesTotal.WithLabelValues(metrics.ModeSource, metrics.ResultFailed, selection).Inc()
}

// releaseDestination tells the destination to drop its in-memory hold on a
// quarantined torrent while keeping the bytes it has already received.
//
// Quarantine previously told the destination nothing at all, so its partial
// data sat there with no path to reclamation. Deleting it here would be worse:
// the common cause of quarantine is a long transient fault, where the partial
// data is perfectly good, and a release would then re-copy the whole torrent.
// Keeping the bytes means a release within the reclamation window resumes from
// the persisted piece bitmap instead.
//
// Best-effort. If the call fails the tag is still applied, and the
// destination's own reclamation is the backstop.
func (t *QBTask) releaseDestination(ctx context.Context, hash string) {
	if t.cfg.DryRun {
		return
	}

	abortCtx, cancel := withDestRPCTimeout(ctx)
	defer cancel()

	if _, abortErr := t.grpcDest.AbortTorrent(abortCtx, hash, false); abortErr != nil {
		t.logger.WarnContext(ctx, "failed to release quarantined torrent on destination, reclamation will handle it",
			"hash", hash,
			"error", abortErr,
		)
		return
	}
	t.logger.InfoContext(ctx, "released quarantined torrent on destination, data preserved for retry",
		"hash", hash,
	)
}

// stopTracking tears down all tracking state for a torrent: unregisters from the
// piece monitor, evicts file-handle and init caches, removes from TrackedTorrents,
// and clears finalization backoff. Callers handle their own metrics and RPC cleanup.
func (t *QBTask) stopTracking(hash string) {
	t.releaseTorrent(hash)
}

// releaseTorrent tears down every piece of per-torrent streaming state in one
// operation.
//
// These six releases used to be spelled out at each lifecycle transition in
// five different combinations, so every new path had to remember which subset
// applied. That is not a hypothetical hazard: markTorrentSynced omitted the
// stall release, which left lastStreamed set on every successfully synced
// torrent and kept its record alive for the lifetime of the process.
//
// Completion state is deliberately NOT released here. Whether a torrent stays
// known-complete genuinely differs by caller — quiesceExcludedCompleted must
// preserve it so a later removal takes the safe handoff path, while
// resyncFileSelection must drop it — so it stays explicit at each site.
func (t *QBTask) releaseTorrent(hash string) {
	t.tracker.Untrack(hash)
	t.store.Untrack(hash)
	t.store.ClearBackoff(hash)
	t.store.ClearStall(hash)
	t.releaseCaches(hash)
}

// releaseCaches drops the source-side memos for a torrent without untracking
// it, so the next cycle re-derives them from scratch.
func (t *QBTask) releaseCaches(hash string) {
	t.source.EvictCache(hash)
	t.grpcDest.ClearInitResult(hash)
}

// invertPiecesNeeded converts PiecesNeeded (true=missing) to written (true=have).
func invertPiecesNeeded(piecesNeeded []bool) []bool {
	written := make([]bool, len(piecesNeeded))
	for i, needed := range piecesNeeded {
		written[i] = !needed
	}
	return written
}

// resyncWithDest re-initializes a torrent on destination to discover which pieces are
// actually written, then resets the tracker's streamed state to match. This
// recovers from divergence after a destination restart where flushed state was stale.
func (t *QBTask) resyncWithDest(ctx context.Context, hash string) {
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

	// Bug 3 fix: use PiecesNeededCount (0 = all written) instead of len(PiecesNeeded),
	// which would be non-zero even for an uninitialized result (-1 sentinel).
	if result == nil || result.PiecesNeededCount == 0 {
		t.logger.InfoContext(ctx, "resync: destination reports all pieces written",
			"hash", hash,
		)
		return
	}

	writtenOnDest := invertPiecesNeeded(result.PiecesNeeded)
	reset := t.tracker.ResyncStreamed(hash, writtenOnDest)

	// Bug 2 fix: re-apply the deselected piece mask so pieces that can never be
	// read from source (priority-0 files) are not reset to un-streamed by ResyncStreamed.
	// Destination correctly omits deselected pieces from its written bitmap, so without
	// this, those pieces get un-marked and progress.Complete is never reached.
	if mask := streaming.DeselectedPieceMask(
		meta.GetFiles(), meta.GetNumPieces(), meta.GetPieceSize(), meta.GetTotalSize(),
	); mask != nil {
		restored := t.tracker.MarkStreamedBatch(hash, mask)
		t.logger.InfoContext(ctx, "re-applied deselected piece mask after resync",
			"hash", hash,
			"restored", restored,
		)
	}

	t.logger.InfoContext(ctx, "resync complete, pieces will be re-streamed",
		"hash", hash,
		"piecesReset", reset,
		"destHas", result.PiecesHaveCount,
		"destNeeds", result.PiecesNeededCount,
	)
}
