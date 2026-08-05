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
	LabelSelection  = "selection"  // partial, full
	LabelStage      = "stage"      // finalization stage (disk, qb)
	LabelInstance   = "instance"   // arr instance name (radarr, sonarr)
	LabelKind       = "kind"       // arr lookup error kind
	LabelOutcome    = "outcome"    // arr decision outcome (synced, skipped, failed_open)
	LabelCode       = "code"       // gRPC status code
)

// Arr decision outcomes labelling ArrDecisionsTotal.
const (
	OutcomeArrSynced     = "synced"
	OutcomeArrSkipped    = "skipped"
	OutcomeArrFailedOpen = "failed_open"
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
	ResultSynced         = "synced" // sync_outcomes_total
	ResultFailed         = "failed" // sync_outcomes_total

	// ResultDrainStarted and its siblings label shutdown_drain_outcomes_total.
	// The two skip reasons are kept apart because they need different
	// responses: not_allowed is the gate working as configured, whereas
	// check_failed means the gate could not be evaluated and the drain was
	// dropped on the fail-closed path.
	ResultDrainStarted         = "started"
	ResultDrainSkippedNotAllow = "skipped_not_allowed"
	ResultDrainSkippedFailed   = "skipped_check_failed"

	SelectionPartial = "partial"
	SelectionFull    = "full"

	ComponentStreamQueue = "stream_queue"

	DirectionUp   = "up"
	DirectionDown = "down"

	ReasonContextCancel     = "context_cancel"
	ReasonEOF               = "eof"
	ReasonStreamError       = "error"
	ReasonAckChannelBlocked = "ack_channel_blocked"

	ReasonAbortInQB        = "in_qb"        // AbortFileDeletionsSkippedTotal: torrent already in destination qB
	ReasonAbortPreExisting = "pre_existing" // AbortFileDeletionsSkippedTotal: setupFile reused operator data
	ReasonAbortUnselected  = "unselected"   // AbortFileDeletionsSkippedTotal: deselected file we never wrote

	// StageDisk is the disk-bound finalization stage (verify + inode registration).
	StageDisk = "disk"
	// StageQB is the qBittorrent integration stage (add + recheck wait).
	StageQB = "qb"

	// ReasonQueueTimeout marks BUSY caused by a stage-queue timeout.
	ReasonQueueTimeout = "queue_timeout"
	// ReasonQBChecking marks BUSY caused by qB still checking at budget expiry.
	ReasonQBChecking = "qb_checking"

	// ReasonSkipNotSyncable and its siblings label SkippedTorrents: why a source
	// torrent is not eligible for sync. Without these, a torrent broken on the
	// source simply never syncs, with no log and no metric to say so.
	ReasonSkipNotSyncable   = "not_syncable_state" // error, missingFiles, download-side paused
	ReasonSkipZeroProgress  = "zero_progress"      // nothing downloaded yet
	ReasonSkipExcludeTag    = "exclude_tag"        // operator opted the torrent out
	ReasonSkipQuarantined   = "quarantined"        // carries the sync-failed tag
	ReasonSkipAlreadySynced = "already_synced"     // destination already has it

	ReasonOrphanInQB          = "in_qb"          // OrphanCleanupSkippedTotal: torrent in destination qB but not healable (non-seeding state, <100%, or savepath is not qb-sync's copy)
	ReasonOrphanQBUnreachable = "qb_unreachable" // OrphanCleanupSkippedTotal: destination qB unreachable during safety check
)

// Counters track cumulative values that only increase.
var (
	// SyncOutcomesTotal counts torrents reaching a terminal sync outcome
	// (synced or sync-failed). Replaces the prior split torrents_synced_total
	// + sync_failed_total. Selection label distinguishes partial vs full
	// torrents so operators can see whether partial-selection finalization
	// succeeds at the same rate as full.
	SyncOutcomesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "sync_outcomes_total",
			Help:      "Torrent sync outcomes: result=synced (succeeded) or result=failed (sync-failed tagged)",
		},
		[]string{LabelMode, LabelResult, LabelSelection},
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

	// OrphanCleanupSkippedTotal counts orphan-cleanup attempts that were
	// suppressed by safety checks. Reasons:
	//   - in_qb: torrent is currently registered in destination qBittorrent
	//     (do not delete data qB still owns).
	//   - qb_unreachable: destination qB returned an error during the safety
	//     check; cleanup is skipped fail-closed. Sustained increments here
	//     indicate destination qB is offline / misconfigured and orphans are
	//     accumulating on disk.
	OrphanCleanupSkippedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "orphan_cleanup_skipped_total",
			Help:      "Orphan-cleanup attempts suppressed by safety checks (reason: in_qb / qb_unreachable)",
		},
		[]string{LabelReason},
	)

	// OrphanCleanupHealedTotal counts orphans self-healed to finalized: stale
	// unfinalized metadata whose torrent destination qB reports complete on the
	// seeding side at qb-sync's own savepath (qB verified qb-sync's data, so
	// the marker is truthful). Typically the crash window between
	// addAndVerifyTorrent and markFinalized.
	OrphanCleanupHealedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "orphan_cleanup_healed_total",
			Help:      "Orphans self-healed to finalized because destination qB reports the torrent complete",
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

	// StaleInodeEvictionsTotal counts inode registry entries evicted due to
	// size mismatch (recycled source inode).
	StaleInodeEvictionsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "stale_inode_evictions_total",
			Help:      "Total inode registry entries evicted due to recycled source inodes",
		},
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

	// BytesSyncedTotal counts bytes synced from source to destination, broken
	// down by mode and selection. Per-torrent breakdown previously available
	// via {hash, name} labels was dropped: hash+name is unbounded over time
	// and operators reading per-torrent totals belong in logs, not Prometheus.
	BytesSyncedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "bytes_synced_total",
			Help:      "Total bytes synced from source to destination",
		},
		[]string{LabelMode, LabelSelection},
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

	// PostAddRechecksTotal counts auto-triggered qB rechecks fired when a
	// freshly-added torrent landed in an error state. Typical cause: the
	// destination qB's mount (commonly NFS) has a stale attribute cache and
	// hasn't yet seen the renames we just performed; a recheck forces qB to
	// re-walk the savepath and pick up the correct file sizes.
	PostAddRechecksTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "post_add_rechecks_total",
			Help:      "Auto-triggered qB rechecks for torrents that landed in an error state right after AddTorrent",
		},
	)

	// ExcludeSyncAbortTotal counts torrents aborted due to exclude-sync tag applied mid-sync.
	ExcludeSyncAbortTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "exclude_sync_abort_total",
			Help:      "Total torrents aborted due to exclude-sync tag applied mid-sync",
		},
	)

	// AbortFileDeletionsSkippedTotal counts deletions suppressed by AbortTorrent's
	// safety guards. Reasons:
	//   - in_qb: destination qB already has the torrent (typically the
	//     finalization-completion race); incremented once per AbortTorrent call
	//     where the guard fires, suppressing the entire deletion path.
	//   - pre_existing: a file the operator had on disk before sync started and
	//     that setupFile reused without writing; incremented per skipped file.
	//   - unselected: a deselected file that qb-sync never wrote; incremented
	//     per skipped file.
	AbortFileDeletionsSkippedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "abort_file_deletions_skipped_total",
			Help:      "AbortTorrent file deletions suppressed by safety guards (reason: in_qb / pre_existing / unselected)",
		},
		[]string{LabelReason},
	)

	// FinalizeNotFoundTotal counts torrents where the destination had no state
	// (metadata missing or data files deleted) and the source untracked for re-init.
	FinalizeNotFoundTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "finalize_not_found_total",
			Help:      "Torrents untracked because destination had no state (will re-initialize)",
		},
	)

	// StaleBitmapPiecesClearedTotal counts piece bits cleared from the written bitmap
	// during init because the backing data file was missing from disk.
	StaleBitmapPiecesClearedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "stale_bitmap_pieces_cleared_total",
			Help:      "Piece bits cleared from written bitmap because backing data file was missing",
		},
	)

	// PartialSelectionRecoveryTotal counts attempts to recover stuck
	// partial-selection torrents on destination — qB silently dropped the
	// initial filePrio change, leaving the torrent in stoppedDl with default
	// priorities. Result label distinguishes successful recovery (qB persisted
	// priorities on the second attempt) from budget-exhausted failure (the
	// source-side cap then surfaces it as sync-failed).
	PartialSelectionRecoveryTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "partial_selection_recovery_total",
			Help:      "Recovery attempts for stuck partial-selection torrents on destination",
		},
		[]string{LabelResult},
	)
)

// Descriptors for state-derived gauges emitted via per-binary Collectors at
// scrape time. Reading state directly on Collect eliminates drift between
// state mutation and metric value, and removes the orchestrator-cycle lag
// operators previously saw on dashboards.
var (
	ActiveTorrentsDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "active_torrents"),
		"Torrents currently being tracked/synced",
		[]string{LabelMode}, nil,
	)

	OldestPendingSyncSecondsDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "oldest_pending_sync_seconds"),
		"Age in seconds of the oldest torrent waiting to sync from source to destination",
		[]string{LabelHash, LabelName}, nil,
	)

	TorrentPiecesDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "torrent_pieces"),
		"Total number of pieces per tracked torrent",
		[]string{LabelHash, LabelName}, nil,
	)

	TorrentPiecesStreamedDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "torrent_pieces_streamed"),
		"Number of pieces synced to destination per tracked torrent",
		[]string{LabelHash, LabelName}, nil,
	)

	TorrentSizeBytesDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "torrent_size_bytes"),
		"Total size in bytes per tracked torrent",
		[]string{LabelHash, LabelName}, nil,
	)

	// TorrentProgressRatioDesc and TorrentBytesStreamedDesc are derived from the
	// same scrape-time snapshot as the raw piece counters, so they can't drift
	// against TorrentPiecesStreamedDesc / TorrentPiecesDesc. They also save
	// dashboards from a divide-by-zero-guarded join across multiple metrics.
	TorrentProgressRatioDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "torrent_progress_ratio"),
		"Streaming progress per tracked torrent as a ratio in [0,1]",
		[]string{LabelHash, LabelName}, nil,
	)

	TorrentBytesStreamedDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "torrent_bytes_streamed"),
		"Approximate bytes streamed to destination per tracked torrent",
		[]string{LabelHash, LabelName}, nil,
	)

	CompletedOnDestCacheSizeDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "completed_on_dest_cache_size"),
		"Number of torrents cached as complete on destination",
		nil, nil,
	)

	// StalledTorrentsDesc counts torrents whose stall clock is running: pieces
	// are available on the source but nothing is moving. Reaching the guard
	// quarantines them, so a rising value is the early warning.
	StalledTorrentsDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "stalled_torrents"),
		"Torrents with pieces available on source but not advancing",
		nil, nil,
	)

	ActiveFinalizationBackoffsDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "active_finalization_backoffs"),
		"Torrents currently in finalization backoff on source server",
		nil, nil,
	)

	InodeRegistrySizeDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "inode_registry_size"),
		"Number of registered inodes for hardlink deduplication",
		nil, nil,
	)

	TorrentsWithDirtyStateDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "torrents_with_dirty_state"),
		"Torrents with state not yet flushed to disk on destination server",
		nil, nil,
	)
)

// Gauges track values that can go up or down. Event-driven gauges live here
// (set inline at the state-change site); state-derived gauges live above as
// prometheus.Desc + Collector emission.
var (
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

	// QuarantinedTorrents is the standing population carrying the sync-failed
	// tag. sync_outcomes_total gives the rate of new failures but never how
	// many are sitting quarantined right now, which is what needs an alert.
	//
	// It is kept as a separate, label-free series so the alert rule in METRICS.md
	// does not depend on a label value that a future refactor could rename.
	//
	// It is not the same population as SkippedTorrents{reason="quarantined"}:
	// that label reports one reason per torrent and ranks source state ahead of
	// the marker, so a quarantined torrent also sitting in error or missingFiles
	// counts under not_syncable_state there. This gauge counts the tag itself and
	// so includes those, which is what the alert needs.
	QuarantinedTorrents = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "quarantined_torrents",
			Help:      "Source torrents currently carrying the sync-failed tag",
		},
	)

	// SkippedTorrents counts source torrents excluded from sync, by reason.
	SkippedTorrents = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "skipped_torrents",
			Help:      "Source torrents not eligible for sync, by reason",
		},
		[]string{LabelReason},
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

	// FinalizationQueueDepth tracks torrents currently waiting for a finalization stage slot.
	FinalizationQueueDepth = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "finalization_queue_depth",
			Help:      "Torrents currently waiting for a finalization stage slot",
		},
		[]string{LabelStage},
	)

	// Draining tracks whether the source server is currently draining (1=draining, 0=normal).
	Draining = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "draining",
			Help:      "Whether the source server is draining synced torrents on shutdown (1=draining, 0=normal)",
		},
	)

	// ShutdownDrainOutcomesTotal records how the shutdown drain resolved.
	//
	// Draining only reports a drain that started, so a drain skipped at the
	// annotation gate is indistinguishable from one that never ran at all, and
	// from a pod that simply died before the last scrape. This counter makes
	// the skip explicit, which matters because a skip is silent by nature: it
	// happens at SIGTERM, in a pod that is about to disappear.
	//
	// Scrape timing at shutdown is unreliable, so treat the logs as the
	// authoritative record and this as the aggregate signal.
	ShutdownDrainOutcomesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "shutdown_drain_outcomes_total",
			Help:      "Shutdown drain outcomes by result (source)",
		},
		[]string{LabelResult},
	)

	// ArrDecisionsTotal counts arr filter decisions by outcome. failed_open is
	// the one to watch: the filter deliberately syncs when it cannot reach
	// *arr, so a rising failed_open means torrents are syncing unfiltered
	// rather than that anything is broken in the sync path itself.
	ArrDecisionsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "arr_decisions_total",
			Help:      "Total arr filter decisions (outcome: synced, skipped, failed_open)",
		},
		[]string{LabelInstance, LabelOutcome},
	)

	// ArrSkipTotal counts pre-sync skips by the verdict that caused them.
	ArrSkipTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "arr_skip_total",
			Help:      "Total pre-sync skips driven by arr verdict (reason: download_ignored, download_failed)",
		},
		[]string{LabelInstance, LabelReason},
	)

	// ArrAbortedTotal counts in-flight syncs abandoned because the verdict
	// flipped after the transfer had already started. Distinct from ArrSkipTotal
	// because this one means work was thrown away, not merely never begun.
	ArrAbortedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "arr_aborted_total",
			Help:      "Total in-progress syncs aborted because arr's verdict flipped to skip",
		},
		[]string{LabelInstance, LabelReason},
	)

	// ArrLookupErrorsTotal counts arr lookup errors by kind.
	ArrLookupErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "arr_lookup_errors_total",
			Help:      "Total arr lookup errors (kind: timeout, http_5xx, unauthorized, network, rate_limited)",
		},
		[]string{LabelInstance, LabelKind},
	)

	// ArrLookupSkippedBudgetTotal counts torrents that bypassed the lookup
	// because the per-cycle time budget was already spent, and therefore synced
	// without a verdict.
	ArrLookupSkippedBudgetTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "arr_lookup_skipped_budget_total",
			Help:      "Total torrents that bypassed arr lookup because the per-cycle budget was exhausted",
		},
	)

	// ArrRelayErrorsTotal counts source-side failures to reach the destination
	// for an *arr verdict, labelled by gRPC code.
	//
	// Separate from ArrDecisionsTotal{outcome="failed_open"}: that one is emitted
	// by the destination and covers *arr being unreachable from there. These are
	// failures the destination never sees, so without this counter a source that
	// cannot reach the destination at all would look like a destination that is
	// simply deciding to sync everything.
	ArrRelayErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "arr_relay_errors_total",
			Help:      "Source-side failures to obtain an arr verdict from the destination, by gRPC code",
		},
		[]string{LabelCode},
	)

	// ArrCategoryRefreshErrorsTotal counts failures to rediscover which
	// categories an *arr instance claims.
	//
	// Worth alerting on: the routing is discovered rather than configured, so
	// sustained failures mean qb-sync is filtering against a stale map. It keeps
	// the last good one rather than clearing it, which is the safe choice but
	// also the silent one.
	ArrCategoryRefreshErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "arr_category_refresh_errors_total",
			Help:      "Failures discovering the categories an arr instance claims",
		},
		[]string{LabelInstance},
	)

	// ArrCircuitBreakerState tracks the arr circuit breaker state per instance.
	ArrCircuitBreakerState = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "arr_circuit_breaker_state",
			Help:      "Arr circuit breaker state (0=closed, 1=open, 2=half-open)",
		},
		[]string{LabelInstance},
	)

	// ArrRoutedCategories is how many categories each instance currently claims.
	// Zero means the filter is inert for that instance: nothing routes to it, so
	// nothing is ever checked against it.
	ArrRoutedCategories = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "arr_routed_categories",
			Help:      "Categories currently routed to each arr instance (0 = filter inert for it)",
		},
		[]string{LabelInstance},
	)

	// ArrLookupSeconds is the per-call HTTP latency for arr lookups.
	ArrLookupSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "arr_lookup_seconds",
			Help:      "Per-call latency of arr history lookups",
			Buckets:   []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 3, 5},
		},
		[]string{LabelInstance},
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

	// FinalizationDuration tracks the total time to finalize a torrent,
	// INCLUDING stage-queue wait. Use FinalizeStageDuration for work-only time.
	FinalizationDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "finalization_duration_seconds",
			Help:      "Total time to finalize a torrent, including queue wait",
			Buckets:   []float64{1, 5, 15, 60, 300, 900, 1800, 3600, 7200, 21600, 43200},
		},
		[]string{LabelResult},
	)

	// FinalizeQueueWaitSeconds tracks time spent waiting for a finalization stage slot.
	FinalizeQueueWaitSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "finalize_queue_wait_seconds",
			Help:      "Time a finalization waited for a stage slot",
			Buckets:   []float64{1, 5, 15, 60, 300, 900, 1800, 3600, 7200},
		},
		[]string{LabelStage},
	)

	// FinalizeStageDuration tracks per-stage finalization work time (excludes queue wait).
	FinalizeStageDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "finalize_stage_duration_seconds",
			Help:      "Finalization stage work time, excluding queue wait",
			Buckets:   []float64{1, 5, 15, 60, 300, 900, 1800, 3600, 7200, 21600},
		},
		[]string{LabelStage, LabelResult},
	)

	// FinalizeBusyTotal counts BUSY (congestion) responses returned to the source.
	FinalizeBusyTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "finalize_busy_total",
			Help:      "BUSY (congestion) responses returned to source, by reason",
		},
		[]string{LabelReason},
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
