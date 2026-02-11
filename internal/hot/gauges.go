package hot

import (
	"context"
	"time"

	"github.com/arsac/qb-sync/internal/metrics"
	"github.com/arsac/qb-sync/internal/streaming"
)

// updateSyncAgeGauge sets the oldest_pending_sync_seconds gauge per tracked torrent.
// Resets first to clear stale labels from previously finalized/removed torrents.
func (t *QBTask) updateSyncAgeGauge() {
	t.trackedMu.RLock()
	defer t.trackedMu.RUnlock()
	metrics.OldestPendingSyncSeconds.Reset()
	for hash, tt := range t.trackedTorrents {
		age := time.Since(tt.completionTime).Seconds()
		metrics.OldestPendingSyncSeconds.WithLabelValues(hash, tt.name).Set(age)
	}
}

// updateTorrentProgressGauges sets per-torrent progress gauges for Grafana dashboards.
// Resets first to clear stale labels from previously finalized/removed torrents.
func (t *QBTask) updateTorrentProgressGauges() {
	t.trackedMu.RLock()
	defer t.trackedMu.RUnlock()

	metrics.TorrentPieces.Reset()
	metrics.TorrentPiecesStreamed.Reset()
	metrics.TorrentSizeBytes.Reset()

	for hash, tt := range t.trackedTorrents {
		progress, err := t.tracker.GetProgress(hash)
		if err != nil {
			continue
		}
		metrics.TorrentPieces.WithLabelValues(hash, tt.name).Set(float64(progress.TotalPieces))
		metrics.TorrentPiecesStreamed.WithLabelValues(hash, tt.name).Set(float64(progress.Streamed))
		metrics.TorrentSizeBytes.WithLabelValues(hash, tt.name).Set(float64(tt.size))
	}
}

// Progress returns the streaming progress for a torrent.
// Exported for testing (used by E2E tests).
func (t *QBTask) Progress(_ context.Context, hash string) (streaming.StreamProgress, error) {
	return t.tracker.GetProgress(hash)
}
