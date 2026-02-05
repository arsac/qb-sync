// Package metrics provides Prometheus metrics for qb-sync observability.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const namespace = "qbsync"

// Label constants for consistent labeling across metrics.
const (
	LabelMode      = "mode"      // hot, cold
	LabelResult    = "result"    // success, failure
	LabelOperation = "operation" // GetTorrents, Login, etc.
	LabelComponent = "component" // qb_client, stream_queue
)

// Counters track cumulative values that only increase.
var (
	// TorrentsSyncedTotal counts torrents successfully synced.
	TorrentsSyncedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "torrents_synced_total",
			Help:      "Total torrents successfully synced",
		},
		[]string{LabelMode},
	)

	// FinalizationErrorsTotal counts finalization failures.
	FinalizationErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "finalization_errors_total",
			Help:      "Total finalization failures",
		},
		[]string{LabelMode},
	)

	// OrphanCleanupsTotal counts orphaned torrents cleaned up on cold.
	OrphanCleanupsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "orphan_cleanups_total",
			Help:      "Total orphan torrents cleaned up on cold server",
		},
	)

	// PiecesSentTotal counts pieces sent from hot server.
	PiecesSentTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "pieces_sent_total",
			Help:      "Total pieces sent from hot server",
		},
	)

	// PiecesAckedTotal counts pieces successfully acknowledged.
	PiecesAckedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "pieces_acked_total",
			Help:      "Total pieces successfully acknowledged",
		},
	)

	// PiecesFailedTotal counts piece transfer failures.
	PiecesFailedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "pieces_failed_total",
			Help:      "Total piece transfer failures",
		},
	)

	// PiecesReceivedTotal counts pieces received on cold server.
	PiecesReceivedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "pieces_received_total",
			Help:      "Total pieces received on cold server",
		},
	)

	// BytesSentTotal counts bytes sent from hot server.
	BytesSentTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "bytes_sent_total",
			Help:      "Total bytes sent from hot server",
		},
	)

	// BytesReceivedTotal counts bytes received on cold server.
	BytesReceivedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "bytes_received_total",
			Help:      "Total bytes received on cold server",
		},
	)

	// QBClientRetriesTotal counts qBittorrent API retries.
	QBClientRetriesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "qb_client_retries_total",
			Help:      "Total qBittorrent API retries",
		},
		[]string{LabelMode, LabelOperation},
	)

	// CircuitBreakerTripsTotal counts circuit breaker trips.
	CircuitBreakerTripsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "circuit_breaker_trips_total",
			Help:      "Total circuit breaker trips",
		},
		[]string{LabelMode, LabelComponent},
	)

	// StreamReconnectsTotal counts stream reconnections.
	StreamReconnectsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "stream_reconnects_total",
			Help:      "Total stream reconnections",
		},
	)
)

// Gauges track values that can go up or down.
var (
	// ActiveTorrents tracks torrents currently being synced.
	ActiveTorrents = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "active_torrents",
			Help:      "Torrents currently being tracked/synced",
		},
		[]string{LabelMode},
	)

	// InflightPieces tracks pieces currently in-flight (sent but not acked).
	InflightPieces = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "inflight_pieces",
			Help:      "Pieces currently in-flight (sent but not acked)",
		},
	)

	// StreamPoolSize tracks the current number of active streams.
	StreamPoolSize = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "stream_pool_size",
			Help:      "Current number of active streams in the pool",
		},
	)

	// StreamPoolMaxSize tracks the maximum configured streams.
	StreamPoolMaxSize = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "stream_pool_max_size",
			Help:      "Maximum configured streams in the pool",
		},
	)

	// AdaptiveWindowSize tracks the average window size across streams.
	AdaptiveWindowSize = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "adaptive_window_size",
			Help:      "Average adaptive window size across streams",
		},
	)

	// CircuitBreakerState tracks the circuit breaker state (0=closed, 1=open, 2=half-open).
	CircuitBreakerState = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "circuit_breaker_state",
			Help:      "Circuit breaker state (0=closed, 1=open, 2=half-open)",
		},
		[]string{LabelMode, LabelComponent},
	)

	// StreamPoolScalingPaused tracks whether pool scaling is paused (1=paused, 0=active).
	StreamPoolScalingPaused = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "stream_pool_scaling_paused",
			Help:      "Whether stream pool scaling is paused (1=paused, 0=active)",
		},
	)
)

// Histograms track distributions of values.
var (
	// PieceSendDuration tracks the time to send a piece.
	PieceSendDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "piece_send_duration_seconds",
			Help:      "Time to send a piece",
			Buckets:   []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		},
	)

	// PieceRTTSeconds tracks the round-trip time for piece acknowledgment.
	PieceRTTSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "piece_rtt_seconds",
			Help:      "Round-trip time for piece acknowledgment",
			Buckets:   []float64{0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
		},
	)

	// FinalizationDuration tracks the time to finalize a torrent.
	FinalizationDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "finalization_duration_seconds",
			Help:      "Time to finalize a torrent",
			Buckets:   []float64{0.1, 0.5, 1, 2.5, 5, 10, 30, 60, 120},
		},
		[]string{LabelResult},
	)

	// QBAPICallDuration tracks qBittorrent API call latency.
	QBAPICallDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "qb_api_call_duration_seconds",
			Help:      "qBittorrent API call latency",
			Buckets:   []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		},
		[]string{LabelMode, LabelOperation},
	)
)

// Circuit breaker state constants.
const (
	CircuitStateClosed   = 0
	CircuitStateOpen     = 1
	CircuitStateHalfOpen = 2
)

// CircuitStateToFloat converts a circuit breaker state string to a float64.
func CircuitStateToFloat(state string) float64 {
	switch state {
	case "closed":
		return CircuitStateClosed
	case "open":
		return CircuitStateOpen
	case "half-open":
		return CircuitStateHalfOpen
	default:
		return -1
	}
}
