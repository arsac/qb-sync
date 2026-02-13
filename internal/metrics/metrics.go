// Package metrics provides Prometheus metrics for qb-sync observability.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const namespace = "qbsync"

// Label constants for consistent labeling across metrics.
const (
	LabelMode       = "mode"       // source, destination
	LabelResult     = "result"     // success, failure
	LabelOperation  = "operation"  // GetTorrents, Login, etc.
	LabelComponent  = "component"  // qb_client, stream_queue
	LabelReason     = "reason"     // exit reason for goroutines
	LabelHash       = "hash"       // torrent info hash
	LabelName       = "name"       // torrent name
	LabelConnection = "connection" // gRPC connection index
	LabelDirection  = "direction"  // scaling direction (up, down)
)

// Label value constants for consistent usage across the codebase.
const (
	ModeSource      = "source"
	ModeDestination = "destination"

	ResultSuccess        = "success"
	ResultFailure        = "failure"
	ResultSkippedSeeding = "skipped_seeding"
	ResultHit            = "hit"
	ResultMiss           = "miss"

	ComponentStreamQueue = "stream_queue"

	DirectionUp   = "up"
	DirectionDown = "down"

	ReasonContextCancel     = "context_cancel"
	ReasonEOF               = "eof"
	ReasonStreamError       = "error"
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

	// OrphanCleanupsTotal counts orphaned torrents cleaned up on destination.
	OrphanCleanupsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "orphan_cleanups_total",
			Help:      "Total orphan torrents cleaned up on destination server",
		},
	)

	// PiecesSentTotal counts pieces sent from source server, per gRPC connection.
	PiecesSentTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "pieces_sent_total",
			Help:      "Total pieces sent from source server",
		},
		[]string{LabelConnection},
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

	// PiecesReceivedTotal counts pieces received on destination server.
	PiecesReceivedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "pieces_received_total",
			Help:      "Total pieces received on destination server",
		},
	)

	// BytesSentTotal counts bytes sent from source server, per gRPC connection.
	BytesSentTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "bytes_sent_total",
			Help:      "Total bytes sent from source server",
		},
		[]string{LabelConnection},
	)

	// BytesReceivedTotal counts bytes received on destination server.
	BytesReceivedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "bytes_received_total",
			Help:      "Total bytes received on destination server",
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

	// HardlinksCreatedTotal counts hardlinks created on destination server.
	HardlinksCreatedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "hardlinks_created_total",
			Help:      "Total hardlinks created on destination server",
		},
	)

	// PieceHashMismatchTotal counts pieces rejected due to hash mismatch (retried automatically).
	PieceHashMismatchTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "piece_hash_mismatch_total",
			Help:      "Total pieces rejected due to hash mismatch on destination (retried automatically)",
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

	// SourceCleanupGroupsTotal counts groups processed during source cleanup cycles.
	SourceCleanupGroupsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "cleanup_groups_total",
			Help:      "Total groups processed during source cleanup cycles",
		},
		[]string{LabelResult},
	)

	// SourceCleanupTorrentsHandedOffTotal counts torrents handed off from source to destination.
	SourceCleanupTorrentsHandedOffTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "cleanup_torrents_handed_off_total",
			Help:      "Total torrents handed off from source to destination",
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

	// CycleCacheHitsTotal counts times fetchTorrentsCompletedOnDest reused the per-cycle cache.
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
			Help:      "Total bytes synced per torrent from source to destination",
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
	// A spike indicates the receiver has stalled (destination stopped consuming / HTTP/2 flow control full).
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

	// FileHandleCacheTotal counts file handle cache lookups by result (hit/miss).
	FileHandleCacheTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "file_handle_cache_total",
			Help:      "Total file handle cache lookups by result (hit/miss)",
		},
		[]string{LabelResult},
	)

	// FileHandleEvictionsTotal counts file handle evictions from the cache.
	FileHandleEvictionsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "file_handle_evictions_total",
			Help:      "Total file handle evictions (stale handle retry, fallback promotion, or full evict)",
		},
	)

	// ConnectionScaleEventsTotal counts TCP connection scaling events by direction.
	ConnectionScaleEventsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "connection_scale_events_total",
			Help:      "Total TCP connection scaling events",
		},
		[]string{LabelDirection},
	)

	// FilesEarlyFinalizedTotal counts files synced, closed, and renamed before torrent finalization.
	FilesEarlyFinalizedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "files_early_finalized_total",
			Help:      "Files synced, closed, and renamed before torrent finalization",
		},
	)

	// FileSelectionResyncsTotal counts re-syncs triggered by file selection changes.
	FileSelectionResyncsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "file_selection_resyncs_total",
			Help:      "Number of re-syncs triggered by file selection changes on source",
		},
	)

	// EarlyFinalizeVerifyFailuresTotal counts files that failed read-back verification
	// during early finalization.
	EarlyFinalizeVerifyFailuresTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "early_finalize_verify_failures_total",
			Help:      "Files that failed read-back verification during early finalization",
		},
	)

	// VerificationRecoveriesTotal counts torrents recovered from verification failure
	// by marking pieces for re-streaming.
	VerificationRecoveriesTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "verification_recoveries_total",
			Help:      "Torrents recovered from verification failure by marking pieces for re-streaming",
		},
	)

	// SyncFailedTotal counts torrents that exhausted verification retries
	// and were tagged as sync-failed on source.
	SyncFailedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "sync_failed_total",
			Help:      "Torrents that failed verification repeatedly and were tagged as sync-failed",
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

	// TorrentsWithDirtyState tracks torrents with unflushed state on destination.
	TorrentsWithDirtyState = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "torrents_with_dirty_state",
			Help:      "Torrents with state not yet flushed to disk on destination server",
		},
	)

	// ActiveFinalizationBackoffs tracks torrents in finalization backoff on source.
	ActiveFinalizationBackoffs = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "active_finalization_backoffs",
			Help:      "Torrents currently in finalization backoff on source server",
		},
	)

	// OldestPendingSyncSeconds tracks the age of the longest-waiting torrent sync.
	OldestPendingSyncSeconds = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "oldest_pending_sync_seconds",
			Help:      "Age in seconds of the oldest torrent waiting to sync from source to destination",
		},
		[]string{LabelHash, LabelName},
	)

	// TorrentPieces tracks the total number of pieces per tracked torrent.
	TorrentPieces = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "torrent_pieces",
			Help:      "Total number of pieces per tracked torrent",
		},
		[]string{LabelHash, LabelName},
	)

	// TorrentPiecesStreamed tracks the number of pieces synced to destination per tracked torrent.
	TorrentPiecesStreamed = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "torrent_pieces_streamed",
			Help:      "Number of pieces synced to destination per tracked torrent",
		},
		[]string{LabelHash, LabelName},
	)

	// TorrentSizeBytes tracks the total size in bytes per tracked torrent.
	TorrentSizeBytes = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "torrent_size_bytes",
			Help:      "Total size in bytes per tracked torrent",
		},
		[]string{LabelHash, LabelName},
	)

	// CompletedOnDestCacheSize tracks the size of the completed-on-destination cache on source.
	CompletedOnDestCacheSize = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "completed_on_dest_cache_size",
			Help:      "Number of torrents cached as complete on destination",
		},
	)

	// InodeRegistrySize tracks the number of registered inodes on destination.
	InodeRegistrySize = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "inode_registry_size",
			Help:      "Number of registered inodes for hardlink deduplication",
		},
	)

	// DestWorkerQueueDepth tracks pieces waiting for a worker on destination.
	DestWorkerQueueDepth = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "write_worker_queue_depth",
			Help:      "Pieces queued waiting for a destination server write worker",
		},
	)

	// DestWorkersBusy tracks the number of destination workers currently writing.
	DestWorkersBusy = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "write_workers_busy",
			Help:      "Number of destination server write workers currently processing a piece",
		},
	)

	// Draining tracks whether the source server is currently draining (1=draining, 0=normal).
	Draining = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "draining",
			Help:      "Whether the source server is draining synced torrents on shutdown (1=draining, 0=normal)",
		},
	)

	// GRPCConnectionsConfigured tracks the maximum configured TCP connections to the destination server.
	GRPCConnectionsConfigured = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "grpc_connections_configured",
			Help:      "Maximum TCP connections configured for gRPC streaming to destination server",
		},
	)

	// GRPCConnectionsActive tracks the current number of active TCP connections to the destination server.
	GRPCConnectionsActive = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "grpc_connections_active",
			Help:      "Current number of active TCP connections to destination server",
		},
	)

	// SenderWorkersConfigured tracks the number of concurrent sender goroutines.
	SenderWorkersConfigured = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "sender_workers_configured",
			Help:      "Number of concurrent sender workers configured for streaming",
		},
	)
)

// Histograms track distributions of values.
var (
	// PieceSendDuration tracks the time to send a piece, per gRPC connection.
	PieceSendDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "piece_send_duration_seconds",
			Help:      "Time to send a piece",
			Buckets:   []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		},
		[]string{LabelConnection},
	)

	// PieceReadDuration tracks the time to read a piece from disk on source.
	PieceReadDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "piece_read_duration_seconds",
			Help:      "Time to read a piece from disk on source server",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
		},
	)

	// PieceWriteDuration tracks the time to write a piece on destination (hash verify + disk write).
	PieceWriteDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "piece_write_duration_seconds",
			Help:      "Time to verify and write a piece on destination server",
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

	// StateFlushDuration tracks the time to flush dirty state to disk on destination.
	StateFlushDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "state_flush_duration_seconds",
			Help:      "Time to flush dirty torrent state to disk",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5},
		},
	)

	// TorrentSyncLatencySeconds tracks end-to-end sync duration from download completion to destination finalization.
	TorrentSyncLatencySeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "torrent_sync_latency_seconds",
			Help:      "End-to-end sync duration from download completion on source to finalization on destination",
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
