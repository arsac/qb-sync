package streaming

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/arsac/qb-sync/internal/congestion"
	"github.com/arsac/qb-sync/internal/metrics"
	pb "github.com/arsac/qb-sync/proto"

	"golang.org/x/time/rate"
)

const (
	defaultStreamRetryDelay = 5 * time.Second

	// streamingRateLimiterBurst is the burst size for rate limiting (1MB).
	// This allows short bursts of traffic up to 1MB before rate limiting kicks in.
	streamingRateLimiterBurst = 1024 * 1024

	drainTimeout                  = 30 * time.Second
	reconnectBaseDelay            = 1 * time.Second
	reconnectMaxDelay             = 30 * time.Second
	reconnectBackoffFactor        = 2
	defaultMaxConsecutiveFailures = 10               // Circuit breaker: max failures before longer pause
	defaultCircuitBreakerPause    = 5 * time.Minute  // Longer pause after max failures
	windowStatsInterval           = 5 * time.Second  // How often to log window stats
	staleCheckInterval            = 10 * time.Second // How often to check for stale in-flight pieces
)

// BidiQueueConfig configures the bidirectional streaming work queue.
type BidiQueueConfig struct {
	MaxBytesPerSec int64         // Rate limit in bytes per second (0 = unlimited)
	RetryDelay     time.Duration // Delay before retrying failed pieces

	// Multi-stream configuration
	NumStreams    int  // Initial number of streams (default: 2 with adaptive, 4 without)
	MaxNumStreams int  // Maximum streams for adaptive scaling (default: 16)
	AdaptivePool  bool // Enable adaptive stream scaling based on throughput (default: true)

	// Circuit breaker configuration
	MaxConsecutiveFailures int           // Max failures before circuit breaker triggers (default: 10)
	CircuitBreakerPause    time.Duration // Pause duration when circuit breaker triggers (default: 5min)

	// Adaptive window configuration (applied to each stream's congestion control)
	AdaptiveWindow congestion.Config
}

// DefaultBidiQueueConfig returns sensible defaults with adaptive scaling enabled.
func DefaultBidiQueueConfig() BidiQueueConfig {
	return BidiQueueConfig{
		MaxBytesPerSec:         0, // unlimited
		RetryDelay:             defaultStreamRetryDelay,
		NumStreams:             MinPoolSize, // Start small with adaptive
		MaxNumStreams:          MaxPoolSize,
		AdaptivePool:           true, // Enable adaptive scaling
		MaxConsecutiveFailures: defaultMaxConsecutiveFailures,
		CircuitBreakerPause:    defaultCircuitBreakerPause,
		AdaptiveWindow:         congestion.DefaultConfig(),
	}
}

// BidiQueue manages piece streaming using bidirectional gRPC streaming.
// Unlike Queue which uses unary calls, this maintains a pool of persistent
// streams for maximum throughput. Uses adaptive congestion control per stream
// to optimize throughput without saturating the network link.
type BidiQueue struct {
	source  PieceSource
	dest    *GRPCDestination
	tracker *PieceMonitor
	logger  *slog.Logger
	config  BidiQueueConfig

	limiter *rate.Limiter

	// Track which stream each piece was sent on for correct window updates.
	// Key: pieceKey(hash, index), Value: pointer to the PooledStream
	pieceStreams   map[string]*PooledStream
	pieceStreamsMu sync.RWMutex

	bytesSent  atomic.Int64
	piecesOK   atomic.Int64
	piecesFail atomic.Int64
}

// NewBidiQueue creates a new bidirectional streaming work queue.
func NewBidiQueue(
	source PieceSource,
	dest *GRPCDestination,
	tracker *PieceMonitor,
	logger *slog.Logger,
	config BidiQueueConfig,
) *BidiQueue {
	// Apply defaults.
	if config.MaxConsecutiveFailures <= 0 {
		config.MaxConsecutiveFailures = defaultMaxConsecutiveFailures
	}
	if config.CircuitBreakerPause <= 0 {
		config.CircuitBreakerPause = defaultCircuitBreakerPause
	}
	if config.NumStreams <= 0 {
		config.NumStreams = DefaultPoolSize
	}

	q := &BidiQueue{
		source:       source,
		dest:         dest,
		tracker:      tracker,
		logger:       logger,
		config:       config,
		pieceStreams: make(map[string]*PooledStream),
	}

	if config.MaxBytesPerSec > 0 {
		q.limiter = rate.NewLimiter(rate.Limit(config.MaxBytesPerSec), streamingRateLimiterBurst)
	}

	return q
}

// BidiQueueStats contains current queue statistics.
type BidiQueueStats struct {
	NumStreams int
	InFlight   int
	BytesSent  int64
	PiecesOK   int64
	PiecesFail int64
}

// Stats returns current queue statistics.
// Note: Detailed per-stream stats are available via pool.Stats() during runStream.
func (q *BidiQueue) Stats() BidiQueueStats {
	return BidiQueueStats{
		NumStreams: q.config.NumStreams,
		BytesSent:  q.bytesSent.Load(),
		PiecesOK:   q.piecesOK.Load(),
		PiecesFail: q.piecesFail.Load(),
	}
}

// setPieceStream records which stream a piece was sent on.
func (q *BidiQueue) setPieceStream(key string, ps *PooledStream) {
	q.pieceStreamsMu.Lock()
	q.pieceStreams[key] = ps
	q.pieceStreamsMu.Unlock()
}

// getPieceStream retrieves and removes the stream a piece was sent on.
// Returns nil if the piece is not tracked.
func (q *BidiQueue) getPieceStream(key string) *PooledStream {
	q.pieceStreamsMu.Lock()
	ps := q.pieceStreams[key]
	delete(q.pieceStreams, key)
	q.pieceStreamsMu.Unlock()
	return ps
}

// deletePieceStream removes a piece from stream tracking without returning it.
func (q *BidiQueue) deletePieceStream(key string) {
	q.pieceStreamsMu.Lock()
	delete(q.pieceStreams, key)
	q.pieceStreamsMu.Unlock()
}

// clearPieceStreams removes multiple pieces from stream tracking.
func (q *BidiQueue) clearPieceStreams(keys []string) {
	q.pieceStreamsMu.Lock()
	for _, key := range keys {
		delete(q.pieceStreams, key)
	}
	q.pieceStreamsMu.Unlock()
}

// Run processes pieces from the tracker using bidirectional streaming.
// Automatically reconnects on transient failures with exponential backoff.
// Includes circuit breaker to avoid hammering a persistently failing endpoint.
func (q *BidiQueue) Run(ctx context.Context) error {
	reconnectDelay := reconnectBaseDelay
	consecutiveFailures := 0

	for {
		err := q.runStream(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if err == nil {
			reconnectDelay = reconnectBaseDelay
			consecutiveFailures = 0
			continue
		}

		consecutiveFailures++

		// Circuit breaker: after too many consecutive failures, pause longer.
		if consecutiveFailures >= q.config.MaxConsecutiveFailures {
			metrics.CircuitBreakerTripsTotal.WithLabelValues("hot", "stream_queue").Inc()
			metrics.CircuitBreakerState.WithLabelValues("hot", "stream_queue").Set(metrics.CircuitStateOpen)
			q.logger.ErrorContext(ctx, "circuit breaker triggered, pausing reconnection",
				"failures", consecutiveFailures,
				"pause", q.config.CircuitBreakerPause,
			)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(q.config.CircuitBreakerPause):
			}
			metrics.CircuitBreakerState.WithLabelValues("hot", "stream_queue").Set(metrics.CircuitStateClosed)
			consecutiveFailures = 0
			reconnectDelay = reconnectBaseDelay
			continue
		}

		metrics.StreamReconnectsTotal.Inc()
		q.logger.WarnContext(ctx, "stream disconnected, reconnecting",
			"error", err,
			"delay", reconnectDelay,
			"consecutiveFailures", consecutiveFailures,
		)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(reconnectDelay):
		}

		reconnectDelay = min(reconnectDelay*reconnectBackoffFactor, reconnectMaxDelay)
	}
}

// runStream runs a single streaming session using multiple parallel streams.
func (q *BidiQueue) runStream(ctx context.Context) error {
	poolConfig := StreamPoolConfig{
		NumStreams:     q.config.NumStreams,
		MaxNumStreams:  q.config.MaxNumStreams,
		AdaptiveWindow: q.config.AdaptiveWindow,
		Adaptive:       q.config.AdaptivePool,
		ScaleInterval:  defaultScaleInterval,
	}
	pool := NewStreamPool(q.dest, q.logger, poolConfig)

	if err := pool.Open(ctx, q.config.NumStreams); err != nil {
		return fmt.Errorf("opening stream pool: %w", err)
	}
	defer pool.Close()

	q.logger.InfoContext(ctx, "stream pool opened",
		"numStreams", pool.StreamCount(),
	)

	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	stopSender := make(chan struct{})
	streamErr := make(chan error, 1)

	wg.Go(func() {
		q.runSenderPool(streamCtx, pool, stopSender)
	})

	wg.Go(func() {
		q.runAckProcessorPool(streamCtx, pool, stopSender, streamErr)
	})

	wg.Wait()
	q.drainInFlightPool(ctx, pool)

	select {
	case streamError := <-streamErr:
		return streamError
	default:
		return nil
	}
}

// runSenderPool reads pieces from the tracker and distributes them across the stream pool.
func (q *BidiQueue) runSenderPool(ctx context.Context, pool *StreamPool, stopSender <-chan struct{}) {
	statsTicker := time.NewTicker(windowStatsInterval)
	defer statsTicker.Stop()

	for {
		// Wait for any stream to have capacity.
		for !pool.CanSend() {
			select {
			case <-ctx.Done():
				return
			case <-stopSender:
				return
			case <-pool.AckReady():
			}
		}

		select {
		case <-ctx.Done():
			return

		case <-stopSender:
			return

		case <-statsTicker.C:
			stats := pool.Stats()
			q.logger.InfoContext(ctx, "stream pool stats",
				"streams", stats.StreamCount,
				"maxStreams", stats.MaxStreams,
				"totalInFlight", stats.TotalInFlight,
				"throughputMBps", stats.ThroughputMBps,
				"adaptiveEnabled", stats.AdaptiveEnabled,
				"scalingPaused", stats.ScalingPaused,
			)

		case piece, ok := <-q.tracker.Completed():
			if !ok {
				return
			}

			if err := q.sendPiecePool(ctx, pool, piece); err != nil {
				q.logger.ErrorContext(ctx, "failed to send piece",
					"hash", piece.GetTorrentHash(),
					"piece", piece.GetIndex(),
					"error", err,
				)
				q.tracker.MarkFailed(piece.GetTorrentHash(), int(piece.GetIndex()))
				q.piecesFail.Add(1)
			}
		}
	}
}

// sendPiecePool reads piece data and sends it over the best available stream.
func (q *BidiQueue) sendPiecePool(ctx context.Context, pool *StreamPool, piece *pb.Piece) error {
	sendStart := time.Now()
	hash := piece.GetTorrentHash()
	index := piece.GetIndex()
	key := pieceKey(hash, index)

	if !q.dest.IsInitialized(hash) {
		meta, ok := q.tracker.GetTorrentMetadata(hash)
		if !ok {
			return fmt.Errorf("torrent metadata not found: %s", hash)
		}
		result, initErr := q.dest.InitTorrent(ctx, meta.InitTorrentRequest)
		if initErr != nil {
			return fmt.Errorf("initializing torrent: %w", initErr)
		}

		// Mark pieces covered by hardlinks as already streamed
		if result != nil && len(result.PiecesCovered) > 0 {
			var coveredCount int
			for i, covered := range result.PiecesCovered {
				if covered {
					q.tracker.MarkStreamed(hash, i)
					coveredCount++
				}
			}
			if coveredCount > 0 {
				q.logger.InfoContext(ctx, "marked hardlink-covered pieces as streamed",
					"hash", hash,
					"count", coveredCount,
				)
			}
		}
	}

	// Check if this piece is already covered (hardlinked on cold)
	// This can happen if the piece was queued before InitTorrent completed
	progress, _ := q.tracker.GetProgress(hash)
	if progress.TotalPieces > 0 && int(index) < progress.TotalPieces {
		// If the piece is marked as streamed after the hardlink marking above,
		// we can skip it
		if q.tracker.IsPieceStreamed(hash, int(index)) {
			q.logger.DebugContext(ctx, "skipping piece covered by hardlink",
				"hash", hash,
				"piece", index,
			)
			return nil
		}
	}

	data, err := q.source.ReadPiece(ctx, piece)
	if err != nil {
		return fmt.Errorf("reading piece: %w", err)
	}

	if q.limiter != nil {
		if waitErr := q.waitForRateLimit(ctx, len(data)); waitErr != nil {
			return fmt.Errorf("rate limit: %w", waitErr)
		}
	}

	// Select best stream (least loaded)
	ps, selectErr := pool.SelectStream()
	if selectErr != nil {
		return fmt.Errorf("selecting stream: %w", selectErr)
	}

	req := &pb.WritePieceRequest{
		TorrentHash: hash,
		PieceIndex:  index,
		Offset:      piece.GetOffset(),
		Size:        piece.GetSize(),
		PieceHash:   piece.GetHash(),
		Data:        data,
	}

	// Use TrySend for atomic check-and-record to avoid TOCTOU race.
	if !ps.window.TrySend(key) {
		q.logger.DebugContext(ctx, "window full after preparation, will retry piece",
			"hash", hash,
			"piece", index,
			"stream", ps.id,
		)
		return errors.New("window full")
	}

	// Track which stream this piece was sent on for correct ack handling
	q.setPieceStream(key, ps)

	if sendErr := ps.stream.Send(req); sendErr != nil {
		ps.window.OnFail(key)
		q.deletePieceStream(key)
		return fmt.Errorf("sending: %w", sendErr)
	}

	metrics.PiecesSentTotal.Inc()
	metrics.BytesSentTotal.Add(float64(len(data)))
	metrics.PieceSendDuration.Observe(time.Since(sendStart).Seconds())

	q.bytesSent.Add(int64(len(data)))
	ps.bytesSent.Add(int64(len(data)))

	q.logger.DebugContext(ctx, "sent piece",
		"hash", hash,
		"piece", index,
		"size", len(data),
		"stream", ps.id,
		"window", ps.window.Window(),
		"inflight", ps.window.InFlight(),
	)

	return nil
}

// runAckProcessorPool handles incoming acknowledgments from all streams.
func (q *BidiQueue) runAckProcessorPool(
	ctx context.Context,
	pool *StreamPool,
	stopSender chan<- struct{},
	streamErr chan<- error,
) {
	defer close(stopSender)

	staleTicker := time.NewTicker(staleCheckInterval)
	defer staleTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-pool.Done():
			q.markInFlightAsFailedPool(ctx, pool)
			return

		case err := <-pool.Errors():
			q.logger.ErrorContext(ctx, "stream pool error", "error", err)
			q.markInFlightAsFailedPool(ctx, pool)
			select {
			case streamErr <- fmt.Errorf("stream error: %w", err):
			default:
			}
			return

		case <-staleTicker.C:
			q.handleStalePiecesPool(ctx, pool)

		case ack := <-pool.Acks():
			q.processAck(ctx, ack)
		}
	}
}

// handleStalePiecesPool checks for pieces that have been in-flight too long across all streams.
func (q *BidiQueue) handleStalePiecesPool(ctx context.Context, pool *StreamPool) {
	staleKeys := pool.GetAllStaleKeys()
	if len(staleKeys) == 0 {
		return
	}

	q.logger.WarnContext(ctx, "found stale in-flight pieces, marking as failed",
		"count", len(staleKeys),
	)

	for _, key := range staleKeys {
		// Find which stream this piece was on and call OnFail on that stream's window
		if ps := q.getPieceStream(key); ps != nil {
			ps.window.OnFail(key)
		}
		q.requeuePieceByKey(ctx, key)
	}
}

// requeuePieceByKey parses a piece key and marks it as failed for retry.
func (q *BidiQueue) requeuePieceByKey(ctx context.Context, key string) {
	hash, index, ok := ParsePieceKey(key)
	if !ok {
		q.logger.WarnContext(ctx, "failed to parse piece key", "key", key)
		return
	}
	q.tracker.MarkFailed(hash, int(index))
	q.piecesFail.Add(1)
}

// markInFlightAsFailedPool clears in-flight tracking across all streams.
func (q *BidiQueue) markInFlightAsFailedPool(ctx context.Context, pool *StreamPool) {
	keys := pool.ClearAllInflight()
	if len(keys) == 0 {
		return
	}

	q.logger.WarnContext(ctx, "marking in-flight pieces as failed for retry",
		"count", len(keys),
	)

	// Clear piece-to-stream mapping in batch
	q.clearPieceStreams(keys)

	for _, key := range keys {
		q.requeuePieceByKey(ctx, key)
	}
}

// processAck handles a single acknowledgment using the correct stream's window.
func (q *BidiQueue) processAck(ctx context.Context, ack *pb.PieceAck) {
	hash := ack.GetTorrentHash()
	index := int(ack.GetPieceIndex())
	key := pieceKey(hash, int32(index))

	// Find which stream this piece was sent on and remove from tracking
	ps := q.getPieceStream(key)
	streamID := -1
	if ps != nil {
		streamID = ps.id
	}

	if ack.GetSuccess() {
		// Update adaptive window with RTT measurement
		if ps != nil {
			ps.window.OnAck(key)
			ps.piecesOK.Add(1)
		}

		metrics.PiecesAckedTotal.Inc()
		q.tracker.MarkStreamed(hash, index)
		q.piecesOK.Add(1)

		q.logger.DebugContext(ctx, "piece acknowledged",
			"hash", hash,
			"piece", index,
			"stream", streamID,
		)
	} else {
		// Reduce window on failure
		if ps != nil {
			ps.window.OnFail(key)
			ps.piecesFail.Add(1)
		}

		metrics.PiecesFailedTotal.Inc()
		q.tracker.MarkFailed(hash, index)
		q.piecesFail.Add(1)

		q.logger.WarnContext(ctx, "piece write failed",
			"hash", hash,
			"piece", index,
			"stream", streamID,
			"error", ack.GetError(),
		)
	}
}

// drainInFlightPool waits for in-flight pieces across all streams to be acknowledged.
func (q *BidiQueue) drainInFlightPool(ctx context.Context, pool *StreamPool) {
	totalInFlight := pool.TotalInFlight()
	if totalInFlight == 0 {
		return
	}

	q.logger.InfoContext(ctx, "draining in-flight pieces", "count", totalInFlight)

	drainCtx, cancel := context.WithTimeout(ctx, drainTimeout)
	defer cancel()

	for pool.TotalInFlight() > 0 {
		select {
		case <-drainCtx.Done():
			remaining := pool.TotalInFlight()
			q.logger.WarnContext(ctx, "drain timeout, marking remaining pieces as failed",
				"remaining", remaining,
			)
			q.markInFlightAsFailedPool(ctx, pool)
			return

		case <-pool.Done():
			if pool.TotalInFlight() > 0 {
				q.logger.WarnContext(ctx, "pool closed during drain, marking remaining pieces as failed",
					"remaining", pool.TotalInFlight(),
				)
				q.markInFlightAsFailedPool(ctx, pool)
			}
			return

		case err := <-pool.Errors():
			q.logger.WarnContext(ctx, "stream error during drain",
				"error", err,
				"remaining", pool.TotalInFlight(),
			)
			q.markInFlightAsFailedPool(ctx, pool)
			return

		case ack := <-pool.Acks():
			q.processAck(ctx, ack)
		}
	}

	q.logger.InfoContext(ctx, "drain complete")
}

func (q *BidiQueue) waitForRateLimit(ctx context.Context, bytes int) error {
	remaining := bytes
	for remaining > 0 {
		n := min(remaining, q.limiter.Burst())
		if err := q.limiter.WaitN(ctx, n); err != nil {
			return err
		}
		remaining -= n
	}
	return nil
}
