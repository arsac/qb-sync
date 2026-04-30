package destination

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/arsac/qb-sync/internal/metrics"
)

// MetricsCollector emits state-derived gauges for the destination server at
// scrape time. Reading the Server's live state on Collect removes drift
// between state mutation and the value Prometheus sees.
type MetricsCollector struct {
	server *Server
}

// NewMetricsCollector wires a Prometheus collector to the destination server.
func NewMetricsCollector(server *Server) *MetricsCollector {
	return &MetricsCollector{server: server}
}

// Describe implements prometheus.Collector.
func (c *MetricsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- metrics.ActiveTorrentsDesc
	ch <- metrics.InodeRegistrySizeDesc
	ch <- metrics.TorrentsWithDirtyStateDesc
}

// Collect implements prometheus.Collector. The store and InodeRegistry use
// RLock-protected reads for these accessors, so it's safe to call alongside
// Add/Remove/Register/Evict.
func (c *MetricsCollector) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(metrics.ActiveTorrentsDesc, prometheus.GaugeValue,
		float64(c.server.store.Len()), metrics.ModeDestination)

	ch <- prometheus.MustNewConstMetric(metrics.InodeRegistrySizeDesc, prometheus.GaugeValue,
		float64(c.server.store.Inodes().Len()))

	ch <- prometheus.MustNewConstMetric(metrics.TorrentsWithDirtyStateDesc, prometheus.GaugeValue,
		float64(c.dirtyStateCount()))
}

// dirtyStateCount counts torrents with unflushed state. Mirrors the
// per-torrent locking pattern used by flushDirtyStates so it doesn't block
// or skew the writer path.
func (c *MetricsCollector) dirtyStateCount() int {
	var dirty int
	c.server.store.ForEach(func(_ string, state *serverTorrentState) bool {
		state.mu.Lock()
		if state.dirty {
			dirty++
		}
		state.mu.Unlock()
		return true
	})
	return dirty
}

var _ prometheus.Collector = (*MetricsCollector)(nil)
