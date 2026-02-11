package hot

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/autobrr/go-qbittorrent"

	"github.com/arsac/qb-sync/internal/streaming"
	pb "github.com/arsac/qb-sync/proto"
)

// completionTimeOrNow converts a qBittorrent CompletionOn unix timestamp to time.Time.
// Returns time.Now() as fallback when the value is invalid (qBittorrent uses -1 for
// torrents that were never tracked for completion).
func completionTimeOrNow(completionOn int64) time.Time {
	if completionOn > 0 {
		return time.Unix(completionOn, 0)
	}
	return time.Now()
}

// candidateTorrent pairs a torrent with its cold status for priority sorting.
type candidateTorrent struct {
	torrent    qbittorrent.Torrent
	coldResult *streaming.InitTorrentResult
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
// Uses a two-pass approach: first queries cold for status, then sorts candidates
// by progress (most pieces on cold first) before tracking. This ensures
// partially-streamed torrents are prioritized after restart.
func (t *QBTask) trackNewTorrents(ctx context.Context) error {
	torrents, err := t.srcClient.GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{})
	if err != nil {
		return fmt.Errorf("fetching torrents: %w", err)
	}

	t.cycleTorrents = torrents

	var candidates []candidateTorrent
	for _, torrent := range torrents {
		if !isSyncableState(torrent.State) {
			continue
		}

		if torrent.Progress <= 0 {
			continue
		}

		t.completedMu.RLock()
		knownComplete := t.completedOnCold[torrent.Hash]
		t.completedMu.RUnlock()
		if knownComplete {
			continue
		}

		t.trackedMu.RLock()
		_, alreadyTracked := t.trackedTorrents[torrent.Hash]
		t.trackedMu.RUnlock()
		if alreadyTracked {
			continue
		}

		result, coldErr := t.queryColdStatus(ctx, torrent)
		if coldErr != nil {
			if errors.Is(coldErr, errSkipTorrent) {
				continue
			}
			t.logger.WarnContext(ctx, "cold server unreachable, skipping remaining torrents",
				"error", coldErr,
			)
			break
		}
		candidates = append(candidates, candidateTorrent{torrent: torrent, coldResult: result})
	}

	slices.SortFunc(candidates, func(a, b candidateTorrent) int {
		return cmp.Compare(b.coldResult.PiecesHaveCount, a.coldResult.PiecesHaveCount)
	})

	for _, c := range candidates {
		if t.startTrackingReady(ctx, c.torrent, c.coldResult) {
			t.logger.InfoContext(ctx, "started tracking torrent",
				"name", c.torrent.Name,
				"hash", c.torrent.Hash,
				"piecesOnCold", c.coldResult.PiecesHaveCount,
			)
		}
	}

	return nil
}

// queryColdStatus checks a torrent's status on cold without starting tracking.
// Returns:
//   - (result, nil): READY status — caller should collect as candidate
//   - (nil, err): transient error — caller should short-circuit
//   - (nil, errSkipTorrent): non-transient error, already tracking, COMPLETE, or VERIFYING — skip
func (t *QBTask) queryColdStatus(
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
		t.logger.WarnContext(ctx, "failed to check torrent status on cold",
			"name", torrent.Name,
			"hash", torrent.Hash,
			"error", err,
		)
		return nil, errSkipTorrent
	}

	switch initResp.Status {
	case pb.TorrentSyncStatus_SYNC_STATUS_COMPLETE:
		t.markCompletedOnCold(torrent.Hash)
		t.logger.InfoContext(ctx, "torrent already complete on cold",
			"name", torrent.Name,
			"hash", torrent.Hash,
		)
		return nil, errSkipTorrent

	case pb.TorrentSyncStatus_SYNC_STATUS_VERIFYING:
		t.logger.InfoContext(ctx, "torrent verifying on cold, will retry",
			"name", torrent.Name,
			"hash", torrent.Hash,
		)
		return nil, errSkipTorrent

	case pb.TorrentSyncStatus_SYNC_STATUS_READY:
		return initResp, nil

	default:
		t.logger.WarnContext(ctx, "unknown status from cold CheckTorrentStatus",
			"name", torrent.Name,
			"hash", torrent.Hash,
			"status", initResp.Status,
		)
		return nil, errSkipTorrent
	}
}

// startTrackingReady handles the READY status: converts cold's pieces_needed
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
		t.logger.DebugContext(ctx, "torrent not initialized on cold, queuing for streaming",
			"name", torrent.Name,
			"hash", torrent.Hash,
		)
	case 0:
		t.logger.InfoContext(ctx, "all pieces already on cold, will finalize",
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
