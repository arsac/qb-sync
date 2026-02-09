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
	LabelReason    = "reason"    // exit reason for goroutines
	LabelHash      = "hash"      // torrent info hash
	LabelName      = "name"      // torrent name
)

// Label value constants for consistent usage across the codebase.
const (
	ModeHot  = "hot"
	ModeCold = "cold"

	ResultSuccess        = "success"
	ResultFailure        = "failure"
	ResultSkippedSeeding = "skipped_seeding"
	ResultHit            = "hit"
	ResultMiss           = "miss"

	ComponentStreamQueue = "stream_queue"

	ReasonContextCancel    = "context_cancel"
	ReasonEOF              = "eof"
	ReasonStreamError      = "error"
	ReasonAckChannelBlocked = "ack_channel_blocked"
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
		[]string{LabelMode, LabelHash, LabelName},
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

	// TorrentStopErrorsTotal counts failures when stopping torrents before finalization.
	TorrentStopErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "torrent_stop_errors_total",
			Help:      "Total failures stopping torrents before finalization",
		},
		[]string{LabelMode},
	)

	// TorrentResumeErrorsTotal counts failures when resuming torrents after finalization failure.
	TorrentResumeErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "torrent_resume_errors_total",
			Help:      "Total failures resuming torrents after finalization failure",
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
	QBClientRetriesTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "qb_client_retries_total",
			Help:      "Total qBittorrent API retries",
		},
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

	// StalePiecesTotal counts pieces that timed out in-flight.
	StalePiecesTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "stale_pieces_total",
			Help:      "Total pieces that timed out in-flight",
		},
	)

	// DrainTimeoutPiecesLostTotal counts pieces lost due to drain timeout at shutdown.
	DrainTimeoutPiecesLostTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "drain_timeout_pieces_lost_total",
			Help:      "Total pieces lost due to drain timeout at shutdown",
		},
	)

	// HardlinksCreatedTotal counts hardlinks created on cold server.
	HardlinksCreatedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "hardlinks_created_total",
			Help:      "Total hardlinks created on cold server",
		},
	)

	// PieceHashMismatchTotal counts pieces rejected due to hash mismatch (retried automatically).
	PieceHashMismatchTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "piece_hash_mismatch_total",
			Help:      "Total pieces rejected due to hash mismatch on cold (retried automatically)",
		},
	)

	// TagApplicationErrorsTotal counts failures when applying tags to torrents.
	TagApplicationErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "tag_application_errors_total",
			Help:      "Total failures applying tags to torrents in qBittorrent",
		},
		[]string{LabelMode},
	)

	// PieceWriteErrorsTotal counts piece write failures (file open, truncate, or write errors).
	PieceWriteErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "piece_write_errors_total",
			Help:      "Total piece write failures (file open, truncate, or write errors)",
		},
		[]string{LabelMode},
	)

	// StateSaveErrorsTotal counts failures saving torrent state to disk.
	StateSaveErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "state_save_errors_total",
			Help:      "Total failures saving torrent state to disk",
		},
		[]string{LabelMode},
	)

	// FileSyncErrorsTotal counts file sync or close failures before finalization rename.
	FileSyncErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "file_sync_errors_total",
			Help:      "Total file sync or close failures before finalization rename",
		},
		[]string{LabelMode},
	)

	// VerificationErrorsTotal counts piece verification failures during finalization.
	VerificationErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "verification_errors_total",
			Help:      "Total piece verification failures during finalization (read or hash)",
		},
		[]string{LabelMode},
	)

	// HardlinkErrorsTotal counts hardlink creation failures.
	HardlinkErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "hardlink_errors_total",
			Help:      "Total hardlink creation failures",
		},
		[]string{LabelMode},
	)

	// StreamOpenErrorsTotal counts stream open failures or poll errors.
	StreamOpenErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "stream_open_errors_total",
			Help:      "Total stream open failures or poll errors",
		},
		[]string{LabelMode},
	)

	// HotCleanupGroupsTotal counts groups processed during hot cleanup cycles.
	HotCleanupGroupsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "hot_cleanup_groups_total",
			Help:      "Total groups processed during hot cleanup cycles",
		},
		[]string{LabelResult},
	)

	// HotCleanupTorrentsHandedOffTotal counts torrents handed off from hot to cold.
	HotCleanupTorrentsHandedOffTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "hot_cleanup_torrents_handed_off_total",
			Help:      "Total torrents handed off from hot to cold",
		},
	)

	// QBAPICallsTotal counts qBittorrent API calls by operation.
	QBAPICallsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "qb_api_calls_total",
			Help:      "Total qBittorrent API calls",
		},
		[]string{LabelMode, LabelOperation},
	)

	// IdlePollSkipsTotal counts piece poll skips due to idle detection.
	IdlePollSkipsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "idle_poll_skips_total",
			Help:      "Total piece poll skips due to idle torrent detection",
		},
	)

	// CycleCacheHitsTotal counts times fetchTorrentsCompletedOnCold reused the per-cycle cache.
	CycleCacheHitsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "cycle_cache_hits_total",
			Help:      "Total times the per-cycle completed torrents cache was reused",
		},
	)

	// TorrentBytesSyncedTotal counts bytes synced per torrent for Grafana completed-transfers table.
	TorrentBytesSyncedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "torrent_bytes_synced_total",
			Help:      "Total bytes synced per torrent from hot to cold",
		},
		[]string{LabelHash, LabelName},
	)

	// HealthCheckCacheTotal counts health check cache hits and misses.
	HealthCheckCacheTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "health_check_cache_total",
			Help:      "Total health check cache hits and misses",
		},
		[]string{LabelResult},
	)

	// WindowFullTotal counts times the sender blocked because all stream windows were saturated.
	WindowFullTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "window_full_total",
			Help:      "Total times the sender blocked waiting for congestion window capacity",
		},
	)

	// SendTimeoutTotal counts times Send() timed out waiting for gRPC stream.Send to complete.
	// A spike indicates the receiver has stalled (cold stopped consuming / HTTP/2 flow control full).
	SendTimeoutTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "send_timeout_total",
			Help:      "Total times Send() timed out on HTTP/2 flow control backpressure",
		},
	)

	// ReceiveAcksExitTotal counts receiveAcks goroutine exits by reason.
	// Reasons: context_cancel, eof, error, ack_channel_blocked.
	ReceiveAcksExitTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "receive_acks_exit_total",
			Help:      "Total receiveAcks goroutine exits by reason",
		},
		[]string{LabelReason},
	)

	// AckChannelBlockedTotal counts times the ack channel was full for longer than the write timeout,
	// forcing receiveAcks to exit. Indicates forwardAcks is too slow draining acks.
	AckChannelBlockedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "ack_channel_blocked_total",
			Help:      "Total times receiveAcks exited because the ack channel was blocked too long",
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

	// TransferThroughputBytesPerSecond tracks current transfer throughput.
	TransferThroughputBytesPerSecond = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "transfer_throughput_bytes_per_second",
			Help:      "Current transfer throughput in bytes per second",
		},
	)

	// TorrentsWithDirtyState tracks torrents with unflushed state on cold.
	TorrentsWithDirtyState = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "torrents_with_dirty_state",
			Help:      "Torrents with state not yet flushed to disk on cold server",
		},
	)

	// ActiveFinalizationBackoffs tracks torrents in finalization backoff on hot.
	ActiveFinalizationBackoffs = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "active_finalization_backoffs",
			Help:      "Torrents currently in finalization backoff on hot server",
		},
	)

	// OldestPendingSyncSeconds tracks the age of the longest-waiting torrent sync.
	OldestPendingSyncSeconds = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "oldest_pending_sync_seconds",
			Help:      "Age in seconds of the oldest torrent waiting to sync from hot to cold",
		},
		[]string{LabelHash, LabelName},
	)

	// CompletedOnColdCacheSize tracks the size of the completed-on-cold cache on hot.
	CompletedOnColdCacheSize = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "completed_on_cold_cache_size",
			Help:      "Number of torrents cached as complete on cold",
		},
	)

	// InodeRegistrySize tracks the number of registered inodes on cold.
	InodeRegistrySize = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "inode_registry_size",
			Help:      "Number of registered inodes for hardlink deduplication",
		},
	)

	// ColdWorkerQueueDepth tracks pieces waiting for a worker on cold.
	ColdWorkerQueueDepth = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "cold_worker_queue_depth",
			Help:      "Pieces queued waiting for a cold server write worker",
		},
	)

	// ColdWorkersBusy tracks the number of cold workers currently writing.
	ColdWorkersBusy = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "cold_workers_busy",
			Help:      "Number of cold server write workers currently processing a piece",
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

	// PieceWriteDuration tracks the time to write a piece on cold (hash verify + disk write).
	PieceWriteDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "piece_write_duration_seconds",
			Help:      "Time to verify and write a piece on cold server",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
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

	// QBAPICallDuration tracks qBittorrent API call latency (including retries).
	QBAPICallDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "qb_api_call_duration_seconds",
			Help:      "qBittorrent API call latency (including retries)",
			Buckets:   []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		},
		[]string{LabelMode, LabelOperation},
	)

	// StateFlushDuration tracks the time to flush dirty state to disk on cold.
	StateFlushDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "state_flush_duration_seconds",
			Help:      "Time to flush dirty torrent state to disk",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5},
		},
	)

	// TorrentSyncLatencySeconds tracks end-to-end sync duration from download completion to cold finalization.
	TorrentSyncLatencySeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "torrent_sync_latency_seconds",
			Help:      "End-to-end sync duration from download completion on hot to finalization on cold",
			Buckets:   []float64{10, 30, 60, 120, 300, 600, 1800, 3600, 7200},
		},
	)
)

// Circuit breaker state constants.
const (
	CircuitStateClosed   = 0
	CircuitStateOpen     = 1
	CircuitStateHalfOpen = 2
)

