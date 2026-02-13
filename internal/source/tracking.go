package source

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/autobrr/go-qbittorrent"

	"github.com/arsac/qb-sync/internal/metrics"
	"github.com/arsac/qb-sync/internal/streaming"
	pb "github.com/arsac/qb-sync/proto"
)

// completionTimeOrNow converts a qBittorrent CompletionOn unix timestamp to [time.Time].
// Returns [time.Now] as fallback when the value is invalid (qBittorrent uses -1 for
// torrents that were never tracked for completion).
func completionTimeOrNow(completionOn int64) time.Time {
	if completionOn > 0 {
		return time.Unix(completionOn, 0)
	}
	return time.Now()
}

// candidateTorrent pairs a torrent with its destination status for priority sorting.
type candidateTorrent struct {
	torrent    qbittorrent.Torrent
	destResult *streaming.InitTorrentResult
}

// isSyncableState returns true for torrent states where pieces can be read and synced.
func isSyncableState(state qbittorrent.TorrentState) bool {
	switch state { //nolint:exhaustive // Only positive matches matter; all other states are non-syncable.
	case qbittorrent.TorrentStateDownloading,
		qbittorrent.TorrentStateStalledDl,
		qbittorrent.TorrentStateQueuedDl,
		qbittorrent.TorrentStateForcedDl,
		qbittorrent.TorrentStateUploading,
		qbittorrent.TorrentStateStalledUp,
		qbittorrent.TorrentStateQueuedUp,
		qbittorrent.TorrentStateForcedUp:
		return true
	default:
		return false
	}
}

// trackNewTorrents starts tracking new syncable torrents (downloading or completed).
// Uses a two-pass approach: first queries destination for status, then sorts candidates
// by progress (most pieces on destination first) before tracking. This ensures
// partially-streamed torrents are prioritized after restart.
func (t *QBTask) trackNewTorrents(ctx context.Context) error {
	torrents, err := t.srcClient.GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{})
	if err != nil {
		return fmt.Errorf("fetching torrents: %w", err)
	}

	t.cycleTorrents = torrents

	var candidates []candidateTorrent
	for _, torrent := range torrents {
		if t.isExcludedFromTracking(torrent) {
			continue
		}

		result, destErr := t.queryDestStatus(ctx, torrent)
		if destErr != nil {
			if errors.Is(destErr, errSkipTorrent) {
				continue
			}
			t.logger.WarnContext(ctx, "destination server unreachable, skipping remaining torrents",
				"error", destErr,
			)
			break
		}
		candidates = append(candidates, candidateTorrent{torrent: torrent, destResult: result})
	}

	slices.SortFunc(candidates, func(a, b candidateTorrent) int {
		return cmp.Compare(b.destResult.PiecesHaveCount, a.destResult.PiecesHaveCount)
	})

	for _, c := range candidates {
		if t.startTrackingReady(ctx, c.torrent, c.destResult) {
			t.logger.InfoContext(ctx, "started tracking torrent",
				"name", c.torrent.Name,
				"hash", c.torrent.Hash,
				"piecesOnDest", c.destResult.PiecesHaveCount,
			)
		}
	}

	return nil
}

// isExcludedFromTracking returns true if the torrent should be skipped during tracking:
// non-syncable state, zero progress, sync-failed tag, already complete, or already tracked.
func (t *QBTask) isExcludedFromTracking(torrent qbittorrent.Torrent) bool {
	if !isSyncableState(torrent.State) || torrent.Progress <= 0 {
		return true
	}

	if t.cfg.SyncFailedTag != "" && hasTag(torrent.Tags, t.cfg.SyncFailedTag) {
		return true
	}

	t.completedMu.RLock()
	_, knownComplete := t.completedOnDest[torrent.Hash]
	t.completedMu.RUnlock()
	if knownComplete {
		return true
	}

	t.trackedMu.RLock()
	_, alreadyTracked := t.trackedTorrents[torrent.Hash]
	t.trackedMu.RUnlock()
	return alreadyTracked
}

// queryDestStatus checks a torrent's status on destination without starting tracking.
// Returns:
//   - (result, nil): READY status — caller should collect as candidate
//   - (nil, err): transient error — caller should short-circuit
//   - (nil, errSkipTorrent): non-transient error, already tracking, COMPLETE, or VERIFYING — skip
func (t *QBTask) queryDestStatus(
	ctx context.Context,
	torrent qbittorrent.Torrent,
) (*streaming.InitTorrentResult, error) {
	if t.tracker.IsTracking(torrent.Hash) {
		t.trackedMu.Lock()
		t.trackedTorrents[torrent.Hash] = trackedTorrent{
			completionTime: completionTimeOrNow(torrent.CompletionOn),
			name:           torrent.Name,
			size:           torrent.Size,
		}
		t.trackedMu.Unlock()
		t.logger.DebugContext(ctx, "synced tracker state to orchestrator",
			"name", torrent.Name,
			"hash", torrent.Hash,
		)
		return nil, errSkipTorrent
	}

	initResp, err := t.grpcDest.CheckTorrentStatus(ctx, torrent.Hash)
	if err != nil {
		if streaming.IsTransientError(err) {
			return nil, err
		}
		t.logger.WarnContext(ctx, "failed to check torrent status on destination",
			"name", torrent.Name,
			"hash", torrent.Hash,
			"error", err,
		)
		return nil, errSkipTorrent
	}

	switch initResp.Status {
	case pb.TorrentSyncStatus_SYNC_STATUS_COMPLETE:
		t.markCompletedOnDest(torrent.Hash, "")
		t.logger.InfoContext(ctx, "torrent already complete on destination",
			"name", torrent.Name,
			"hash", torrent.Hash,
		)
		return nil, errSkipTorrent

	case pb.TorrentSyncStatus_SYNC_STATUS_VERIFYING:
		t.logger.InfoContext(ctx, "torrent verifying on destination, will retry",
			"name", torrent.Name,
			"hash", torrent.Hash,
		)
		return nil, errSkipTorrent

	case pb.TorrentSyncStatus_SYNC_STATUS_READY:
		return initResp, nil

	default:
		t.logger.WarnContext(ctx, "unknown status from destination CheckTorrentStatus",
			"name", torrent.Name,
			"hash", torrent.Hash,
			"status", initResp.Status,
		)
		return nil, errSkipTorrent
	}
}

// startTrackingReady handles the READY status: converts destination's pieces_needed
// into resume data and starts tracking the torrent for streaming.
func (t *QBTask) startTrackingReady(
	ctx context.Context,
	torrent qbittorrent.Torrent,
	resp *streaming.InitTorrentResult,
) bool {
	if t.trackingOrderHook != nil {
		t.trackingOrderHook(torrent.Hash)
	}

	switch resp.PiecesNeededCount {
	case -1:
		t.logger.DebugContext(ctx, "torrent not initialized on destination, queuing for streaming",
			"name", torrent.Name,
			"hash", torrent.Hash,
		)
	case 0:
		t.logger.InfoContext(ctx, "all pieces already on destination, will finalize",
			"name", torrent.Name,
			"hash", torrent.Hash,
		)
	}

	alreadyWritten := invertPiecesNeeded(resp.PiecesNeeded)

	if trackErr := t.tracker.TrackTorrentWithResume(ctx, torrent.Hash, alreadyWritten); trackErr != nil {
		t.logger.WarnContext(ctx, "failed to track torrent",
			"name", torrent.Name,
			"hash", torrent.Hash,
			"error", trackErr,
		)
		return false
	}

	t.trackedMu.Lock()
	if _, exists := t.trackedTorrents[torrent.Hash]; exists {
		t.trackedMu.Unlock()
		return false
	}
	t.trackedTorrents[torrent.Hash] = trackedTorrent{
		completionTime: completionTimeOrNow(torrent.CompletionOn),
		name:           torrent.Name,
		size:           torrent.Size,
	}
	t.trackedMu.Unlock()

	return true
}

// selectedFingerprint computes a fingerprint from the selected (priority > 0) file indices.
// The result is a sorted, comma-separated string of indices, e.g. "0,1,3".
// Indices are sorted to produce a deterministic fingerprint regardless of API response order.
func selectedFingerprint(files qbittorrent.TorrentFiles) string {
	var indices []int
	for _, f := range files {
		if f.Priority > 0 {
			indices = append(indices, f.Index)
		}
	}
	slices.Sort(indices)
	result := make([]string, len(indices))
	for i, idx := range indices {
		result[i] = strconv.Itoa(idx)
	}
	return strings.Join(result, ",")
}

// computeSelectionFingerprint fetches file info from qBittorrent and returns
// the selection fingerprint. Returns "" on error (will re-check next cycle).
func (t *QBTask) computeSelectionFingerprint(ctx context.Context, hash string) string {
	qbFiles, err := t.srcClient.GetFilesInformationCtx(ctx, hash)
	if err != nil {
		return ""
	}
	return selectedFingerprint(*qbFiles)
}

// findTorrentByHash looks up a torrent from the per-cycle cache.
func (t *QBTask) findTorrentByHash(hash string) *qbittorrent.Torrent {
	for i := range t.cycleTorrents {
		if t.cycleTorrents[i].Hash == hash {
			return &t.cycleTorrents[i]
		}
	}
	return nil
}

// recheckFileSelections compares stored fingerprints against current qBittorrent
// file priorities. On change: evicts caches, calls InitTorrent with resync=true,
// and starts tracking for the newly-selected pieces.
func (t *QBTask) recheckFileSelections(ctx context.Context) {
	t.completedMu.RLock()
	completed := maps.Clone(t.completedOnDest)
	t.completedMu.RUnlock()

	var changed bool
	for hash, storedFingerprint := range completed {
		qbFiles, err := t.srcClient.GetFilesInformationCtx(ctx, hash)
		if err != nil {
			continue // torrent may have been removed; pruneCompletedOnDest handles that
		}

		currentFingerprint := selectedFingerprint(*qbFiles)
		if currentFingerprint == storedFingerprint {
			continue
		}

		t.logger.InfoContext(ctx, "file selection changed, initiating re-sync",
			"hash", hash,
			"oldFingerprint", storedFingerprint,
			"newFingerprint", currentFingerprint,
		)
		metrics.FileSelectionResyncsTotal.Inc()
		changed = true

		t.resyncFileSelection(ctx, hash, currentFingerprint)
	}

	if changed {
		t.saveCompletedCache()
	}
}

// resyncFileSelection evicts caches, re-initializes the torrent on destination with
// resync=true, and starts tracking any newly-needed pieces for streaming.
func (t *QBTask) resyncFileSelection(ctx context.Context, hash, fingerprint string) {
	// Evict caches so next InitTorrent gets fresh metadata
	t.completedMu.Lock()
	delete(t.completedOnDest, hash)
	t.completedMu.Unlock()
	t.source.EvictCache(hash)
	t.grpcDest.ClearInitResult(hash)

	// Get fresh metadata with updated file selection
	meta, metaErr := t.source.GetTorrentMetadata(ctx, hash)
	if metaErr != nil {
		t.logger.WarnContext(ctx, "re-sync: failed to get metadata", "hash", hash, "error", metaErr)
		return
	}

	// Call InitTorrent with resync=true to clear stale qBittorrent entry
	meta.InitTorrentRequest.Resync = true
	result, initErr := t.grpcDest.InitTorrent(ctx, meta.InitTorrentRequest)
	if initErr != nil {
		t.logger.WarnContext(ctx, "re-sync: InitTorrent failed", "hash", hash, "error", initErr)
		return
	}

	if result.PiecesNeededCount <= 0 {
		// All selected pieces already on destination — mark complete directly
		t.markCompletedOnDest(hash, fingerprint)
		return
	}

	// Start tracking for streaming
	alreadyWritten := invertPiecesNeeded(result.PiecesNeeded)
	if trackErr := t.tracker.TrackTorrentWithResume(ctx, hash, alreadyWritten); trackErr != nil {
		t.logger.WarnContext(ctx, "re-sync: failed to track", "hash", hash, "error", trackErr)
		return
	}

	tt := trackedTorrent{completionTime: time.Now(), name: hash}
	if torrent := t.findTorrentByHash(hash); torrent != nil {
		tt = trackedTorrent{
			completionTime: completionTimeOrNow(torrent.CompletionOn),
			name:           torrent.Name,
			size:           torrent.Size,
		}
	}
	t.trackedMu.Lock()
	t.trackedTorrents[hash] = tt
	t.trackedMu.Unlock()
}
