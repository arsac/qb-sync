package source

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/arsac/qb-sync/internal/metrics"
)

// MetricsCollector emits state-derived gauges at scrape time. Pulling values
// directly from the QBTask removes the orchestrator-cycle lag that per-cycle
// Set/Reset suffered from, and per-torrent label series drop on their own
// once a torrent leaves `tracked` (the next scrape simply doesn't emit them).
type MetricsCollector struct {
	task *QBTask
}

// NewMetricsCollector wires a Prometheus collector to the live QBTask.
func NewMetricsCollector(task *QBTask) *MetricsCollector {
	return &MetricsCollector{task: task}
}

// Describe implements prometheus.Collector.
func (c *MetricsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- metrics.ActiveTorrentsDesc
	ch <- metrics.OldestPendingSyncSecondsDesc
	ch <- metrics.TorrentPiecesDesc
	ch <- metrics.TorrentPiecesStreamedDesc
	ch <- metrics.TorrentSizeBytesDesc
	ch <- metrics.TorrentProgressRatioDesc
	ch <- metrics.TorrentBytesStreamedDesc
	ch <- metrics.CompletedOnDestCacheSizeDesc
	ch <- metrics.ActiveFinalizationBackoffsDesc
}

// Collect implements prometheus.Collector. Iterations over c.task.tracked use
// [sync.Map.Range] so this is safe to call alongside ongoing finalize/track/untrack.
func (c *MetricsCollector) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(metrics.ActiveTorrentsDesc, prometheus.GaugeValue,
		float64(c.task.tracked.Count()), metrics.ModeSource)

	ch <- prometheus.MustNewConstMetric(metrics.CompletedOnDestCacheSizeDesc, prometheus.GaugeValue,
		float64(c.task.completed.Count()))

	ch <- prometheus.MustNewConstMetric(metrics.ActiveFinalizationBackoffsDesc, prometheus.GaugeValue,
		float64(c.task.backoffs.Count()))

	c.task.tracked.Range(func(hash string, tt TrackedTorrent) bool {
		c.emitPerTorrent(ch, hash, tt)
		return true
	})
}

// emitPerTorrent emits all per-torrent gauges for a single tracked torrent.
// Torrents whose progress is unavailable are skipped — the next scrape will
// pick them up once the tracker has a snapshot.
func (c *MetricsCollector) emitPerTorrent(ch chan<- prometheus.Metric, hash string, tt TrackedTorrent) {
	progress, err := c.task.tracker.GetProgress(hash)
	if err != nil {
		return
	}

	ch <- prometheus.MustNewConstMetric(metrics.OldestPendingSyncSecondsDesc, prometheus.GaugeValue,
		time.Since(tt.CompletionTime).Seconds(), hash, tt.Name)

	total := float64(progress.TotalPieces)
	streamed := float64(progress.Streamed)
	ch <- prometheus.MustNewConstMetric(metrics.TorrentPiecesDesc, prometheus.GaugeValue,
		total, hash, tt.Name)
	ch <- prometheus.MustNewConstMetric(metrics.TorrentPiecesStreamedDesc, prometheus.GaugeValue,
		streamed, hash, tt.Name)
	ch <- prometheus.MustNewConstMetric(metrics.TorrentSizeBytesDesc, prometheus.GaugeValue,
		float64(tt.Size), hash, tt.Name)

	ratio := 0.0
	if total > 0 {
		ratio = streamed / total
	}
	ch <- prometheus.MustNewConstMetric(metrics.TorrentProgressRatioDesc, prometheus.GaugeValue,
		ratio, hash, tt.Name)
	ch <- prometheus.MustNewConstMetric(metrics.TorrentBytesStreamedDesc, prometheus.GaugeValue,
		ratio*float64(tt.Size), hash, tt.Name)
}

var _ prometheus.Collector = (*MetricsCollector)(nil)
