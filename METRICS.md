# Metrics Reference

All metrics use the `qbsync_` namespace and are exposed via Prometheus at `/metrics`.

## Labels

| Label | Values | Description |
|-------|--------|-------------|
| `mode` | `source`, `destination` | Which side emits the metric |
| `result` | `success`, `failure`, `skipped_seeding`, `hit`, `miss` | Outcome of an operation |
| `operation` | `GetTorrents`, `Login`, ... | qBittorrent API operation name |
| `component` | `qb_client`, `stream_queue` | Internal component |
| `reason` | `context_cancel`, `eof`, `error`, `ack_channel_blocked` | Goroutine exit reason |
| `connection` | gRPC connection index | Per-TCP-connection identifier |
| `hash` | torrent info hash | Per-torrent identifier |
| `name` | torrent name | Per-torrent human-readable name |
| `direction` | `up`, `down` | Connection scaling direction |

## Counters

| Metric | Labels | Description |
|--------|--------|-------------|
| `qbsync_torrents_synced_total` | `mode`, `hash`, `name` | Torrents successfully synced |
| `qbsync_torrent_bytes_synced_total` | `hash`, `name` | Bytes synced per torrent (source only, for completed-transfers table) |
| `qbsync_finalization_errors_total` | `mode` | Finalization failures |
| `qbsync_torrent_stop_errors_total` | `mode` | Failures stopping torrents before handoff |
| `qbsync_torrent_resume_errors_total` | `mode` | Failures resuming torrents after handoff rollback |
| `qbsync_orphan_cleanups_total` | | Orphan torrents cleaned up on destination |
| `qbsync_pieces_sent_total` | `connection` | Pieces sent from source |
| `qbsync_pieces_acked_total` | | Pieces acknowledged by destination |
| `qbsync_pieces_failed_total` | | Piece transfer failures |
| `qbsync_pieces_received_total` | | Pieces received on destination |
| `qbsync_bytes_sent_total` | `connection` | Bytes sent from source |
| `qbsync_bytes_received_total` | | Bytes received on destination (aggregate) |
| `qbsync_qb_client_retries_total` | | qBittorrent API retries |
| `qbsync_qb_api_calls_total` | `mode`, `operation` | qBittorrent API calls by operation |
| `qbsync_circuit_breaker_trips_total` | `mode`, `component` | Circuit breaker trips |
| `qbsync_stream_reconnects_total` | | gRPC stream reconnections |
| `qbsync_stale_pieces_total` | | Pieces that timed out in-flight |
| `qbsync_drain_timeout_pieces_lost_total` | | Pieces lost due to drain timeout at shutdown |
| `qbsync_hardlinks_created_total` | | Hardlinks created on destination |
| `qbsync_piece_hash_mismatch_total` | | Pieces rejected on destination due to hash mismatch (retried automatically) |
| `qbsync_tag_application_errors_total` | `mode` | Failures applying tags in qBittorrent |
| `qbsync_piece_write_errors_total` | `mode` | Piece write failures (file open/truncate/write) |
| `qbsync_state_save_errors_total` | `mode` | Failures saving torrent state to disk |
| `qbsync_file_sync_errors_total` | `mode` | File sync/close failures before finalization rename |
| `qbsync_verification_errors_total` | `mode` | Piece verification failures during finalization |
| `qbsync_hardlink_errors_total` | `mode` | Hardlink creation failures |
| `qbsync_stream_open_errors_total` | `mode` | Stream open or poll failures |
| `qbsync_cleanup_groups_total` | `result` | Hardlink groups processed during source cleanup |
| `qbsync_cleanup_torrents_handed_off_total` | | Torrents handed off from source to destination |
| `qbsync_idle_poll_skips_total` | | Piece poll skips due to idle torrent detection |
| `qbsync_cycle_cache_hits_total` | | Per-cycle completed-torrents cache reuses |
| `qbsync_health_check_cache_total` | `result` | Health check cache hits/misses |
| `qbsync_file_handle_cache_total` | `result` | File handle cache lookups (hit/miss) on source |
| `qbsync_file_handle_evictions_total` | | File handle evictions (stale retry, fallback, or full evict) |
| `qbsync_window_full_total` | | Sender blocked waiting for congestion window capacity |
| `qbsync_send_timeout_total` | | Send() timed out on HTTP/2 flow control backpressure |
| `qbsync_receive_acks_exit_total` | `reason` | receiveAcks goroutine exits by reason |
| `qbsync_ack_channel_blocked_total` | | receiveAcks exited because ack channel was blocked too long |
| `qbsync_connection_scale_events_total` | `direction` | TCP connection scaling events (up/down) |
| `qbsync_files_early_finalized_total` | | Files synced, closed, and renamed before torrent finalization |
| `qbsync_file_selection_resyncs_total` | | Re-syncs triggered by file selection changes (source) |
| `qbsync_early_finalize_verify_failures_total` | | Files that failed read-back verification during early finalization (destination) |
| `qbsync_verification_recoveries_total` | | Torrents recovered from verification failure by marking pieces for re-streaming (destination) |
| `qbsync_sync_failed_total` | | Torrents that failed verification repeatedly and were tagged as sync-failed (source) |
| `qbsync_exclude_sync_abort_total` | | Torrents aborted due to exclude-sync tag applied mid-sync (source) |

## Gauges

| Metric | Labels | Description |
|--------|--------|-------------|
| `qbsync_active_torrents` | `mode` | Torrents currently being tracked/synced |
| `qbsync_inflight_pieces` | | Pieces in-flight (sent but not acked) |
| `qbsync_stream_pool_size` | | Active streams in the pool |
| `qbsync_stream_pool_max_size` | | Maximum configured streams |
| `qbsync_adaptive_window_size` | | Average adaptive window size across streams |
| `qbsync_circuit_breaker_state` | `mode`, `component` | Circuit breaker state: 0=closed, 1=open, 2=half-open |
| `qbsync_stream_pool_scaling_paused` | | Pool scaling paused: 1=paused, 0=active |
| `qbsync_transfer_throughput_bytes_per_second` | | Current transfer throughput |
| `qbsync_torrents_with_dirty_state` | | Torrents with state not yet flushed to disk (destination) |
| `qbsync_active_finalization_backoffs` | | Torrents in finalization backoff (source) |
| `qbsync_oldest_pending_sync_seconds` | `hash`, `name` | Age of each torrent waiting to sync |
| `qbsync_torrent_pieces` | `hash`, `name` | Total pieces per tracked torrent |
| `qbsync_torrent_pieces_streamed` | `hash`, `name` | Pieces synced to destination per tracked torrent |
| `qbsync_torrent_size_bytes` | `hash`, `name` | Total size in bytes per tracked torrent |
| `qbsync_completed_on_dest_cache_size` | | Torrents cached as complete on destination (source) |
| `qbsync_inode_registry_size` | | Registered inodes for hardlink deduplication (destination) |
| `qbsync_write_worker_queue_depth` | | Pieces queued waiting for a destination write worker |
| `qbsync_write_workers_busy` | | Destination write workers currently processing |
| `qbsync_grpc_connections_configured` | | Maximum TCP connections configured for gRPC streaming (source) |
| `qbsync_grpc_connections_active` | | Current active TCP connections to destination server (source) |
| `qbsync_sender_workers_configured` | | Concurrent sender workers configured (source) |
| `qbsync_draining` | | Shutdown drain in progress: 1=draining, 0=normal (source) |

## Histograms

| Metric | Labels | Buckets | Description |
|--------|--------|---------|-------------|
| `qbsync_piece_read_duration_seconds` | | 1ms .. 5s | Time to read a piece from disk on source |
| `qbsync_piece_send_duration_seconds` | `connection` | 10ms .. 10s | Time to send a piece from source |
| `qbsync_piece_write_duration_seconds` | | 1ms .. 5s | Time to verify and write a piece on destination |
| `qbsync_piece_rtt_seconds` | | 10ms .. 5s | Round-trip time for piece acknowledgment |
| `qbsync_finalization_duration_seconds` | `result` | 100ms .. 120s | Time to finalize a torrent on destination |
| `qbsync_qb_api_call_duration_seconds` | `mode`, `operation` | 10ms .. 10s | qBittorrent API call latency (including retries) |
| `qbsync_state_flush_duration_seconds` | | 1ms .. 2.5s | Time to flush dirty torrent state to disk (destination) |
| `qbsync_torrent_sync_latency_seconds` | | 10s .. 7200s | End-to-end sync duration from download completion to destination finalization |

## Grafana Tips

**Completed transfers table** using per-torrent metrics:

| Column | PromQL |
|--------|--------|
| Name | `qbsync_torrent_bytes_synced_total` label `name` |
| Bytes | `qbsync_torrent_bytes_synced_total` |
| Synced at | `timestamp(qbsync_torrents_synced_total{mode="source"})` |

**Live per-torrent progress** (download list view):

| Panel | PromQL |
|-------|--------|
| Progress bar per torrent | `qbsync_torrent_pieces_streamed / qbsync_torrent_pieces` |
| Pieces remaining | `qbsync_torrent_pieces - qbsync_torrent_pieces_streamed` |
| Torrent size | `qbsync_torrent_size_bytes` |
| Bytes synced (estimated) | `qbsync_torrent_pieces_streamed / qbsync_torrent_pieces * qbsync_torrent_size_bytes` |
| Sync rate | `rate(qbsync_torrent_pieces_streamed[5m])` per torrent |
| Active torrent list | Table panel filtering on `qbsync_torrent_pieces > 0` |

**Sync latency distribution** (aggregated across all torrents):

| Panel | PromQL |
|-------|--------|
| p50 | `histogram_quantile(0.50, sum(rate(qbsync_torrent_sync_latency_seconds_bucket[$__rate_interval])) by (le))` |
| p99 | `histogram_quantile(0.99, sum(rate(qbsync_torrent_sync_latency_seconds_bucket[$__rate_interval])) by (le))` |

**Key alert candidates:**

- `rate(qbsync_finalization_errors_total[5m]) > 0` -- finalization failing
- `qbsync_active_finalization_backoffs > 3` -- multiple torrents stuck
- `qbsync_circuit_breaker_state == 1` -- circuit breaker open
- `rate(qbsync_piece_hash_mismatch_total[5m]) > 0.1` -- data integrity issues
- `qbsync_oldest_pending_sync_seconds > 3600` -- torrent stuck syncing for over an hour
- `qbsync_draining == 1` for > 5m -- shutdown drain taking too long
