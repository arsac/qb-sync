package source

import (
	"context"
	"time"

	"github.com/arsac/qb-sync/internal/metrics"
	"github.com/arsac/qb-sync/internal/streaming"
)

// updateSyncAgeGauge sets the oldest_pending_sync_seconds gauge per tracked torrent.
// Resets first to clear stale labels from previously finalized/removed torrents.
func (t *QBTask) updateSyncAgeGauge() {
	metrics.OldestPendingSyncSeconds.Reset()
	t.tracked.Range(func(hash string, tt TrackedTorrent) bool {
		age := time.Since(tt.CompletionTime).Seconds()
		metrics.OldestPendingSyncSeconds.WithLabelValues(hash, tt.Name).Set(age)
		return true
	})
}

// updateTorrentProgressGauges sets per-torrent progress gauges for Grafana dashboards.
// Resets first to clear stale labels from previously finalized/removed torrents.
func (t *QBTask) updateTorrentProgressGauges() {
	metrics.TorrentPieces.Reset()
	metrics.TorrentPiecesStreamed.Reset()
	metrics.TorrentSizeBytes.Reset()

	t.tracked.Range(func(hash string, tt TrackedTorrent) bool {
		progress, err := t.tracker.GetProgress(hash)
		if err != nil {
			return true
		}
		metrics.TorrentPieces.WithLabelValues(hash, tt.Name).Set(float64(progress.TotalPieces))
		metrics.TorrentPiecesStreamed.WithLabelValues(hash, tt.Name).Set(float64(progress.Streamed))
		metrics.TorrentSizeBytes.WithLabelValues(hash, tt.Name).Set(float64(tt.Size))
		return true
	})
}

// Progress returns the streaming progress for a torrent.
// Exported for testing (used by E2E tests).
func (t *QBTask) Progress(_ context.Context, hash string) (streaming.StreamProgress, error) {
	return t.tracker.GetProgress(hash)
}
