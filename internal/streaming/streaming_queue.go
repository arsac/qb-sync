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
	"github.com/arsac/qb-sync/internal/grpcutil"
	"github.com/arsac/qb-sync/internal/metrics"
	pb "github.com/arsac/qb-sync/proto"

	"golang.org/x/time/rate"
)

const (
	defaultStreamRetryDelay = 5 * time.Second

	// streamingRateLimiterBurst is the burst size for rate limiting (1MB).
	streamingRateLimiterBurst = grpcutil.BytesPerMB

	// senderRetryBackoff is the safety-net polling interval for sender workers
	// when no stream has capacity. Handles missed AckReady signals and stale-cleanup capacity changes.
	senderRetryBackoff = 50 * time.Millisecond

	drainTimeout                  = 30 * time.Second
	reconnectBaseDelay            = 1 * time.Second
	reconnectMaxDelay             = 30 * time.Second
	reconnectBackoffFactor        = 2
	defaultMaxConsecutiveFailures = 10               // Circuit breaker: max failures before longer pause
	defaultCircuitBreakerPause    = 5 * time.Minute  // Longer pause after max failures
	windowStatsInterval           = 5 * time.Second  // How often to log window stats
	staleCheckInterval            = 10 * time.Second // How often to check for stale in-flight pieces
	defaultNumSenders             = 4                // Concurrent sender workers (parallelizes ReadPiece + Send)
	maxPieceHashMismatches        = 5                // Per-piece hash mismatch limit before forcing finalization
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

	// Reconnect configuration
	ReconnectBaseDelay time.Duration // Initial reconnect delay (default: 1s)
	ReconnectMaxDelay  time.Duration // Maximum reconnect delay cap (default: 30s)

	// Sender parallelism
	NumSenders int // Concurrent sender workers (default: 4)

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
		NumSenders:             defaultNumSenders,
		MaxConsecutiveFailures: defaultMaxConsecutiveFailures,
		CircuitBreakerPause:    defaultCircuitBreakerPause,
		ReconnectBaseDelay:     reconnectBaseDelay,
		ReconnectMaxDelay:      reconnectMaxDelay,
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

	// Track per-piece hash mismatch failures to prevent infinite retry loops.
	// Key: pieceKey(hash, index), Value: consecutive mismatch count.
	pieceHashMismatches   map[string]int
	pieceHashMismatchesMu sync.Mutex

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
	if config.ReconnectBaseDelay <= 0 {
		config.ReconnectBaseDelay = reconnectBaseDelay
	}
	if config.ReconnectMaxDelay <= 0 {
		config.ReconnectMaxDelay = reconnectMaxDelay
	}
	if config.NumStreams <= 0 {
		config.NumStreams = DefaultPoolSize
	}
	if config.NumSenders <= 0 {
		config.NumSenders = defaultNumSenders
	}
	metrics.SenderWorkersConfigured.Set(float64(config.NumSenders))

	q := &BidiQueue{
		source:              source,
		dest:                dest,
		tracker:             tracker,
		logger:              logger,
		config:              config,
		pieceHashMismatches: make(map[string]int),
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

// (piece-to-stream mapping was previously here; acks now carry their source
// stream via AckEnvelope so no external lookup is needed.)

// clearHashMismatch removes any tracked hash-mismatch counter for a piece.
// Called when an ack comes back successful or when the mismatch limit is hit.
func (q *BidiQueue) clearHashMismatch(key string) {
	q.pieceHashMismatchesMu.Lock()
	delete(q.pieceHashMismatches, key)
	q.pieceHashMismatchesMu.Unlock()
}

// Run processes pieces from the tracker using bidirectional streaming.
// Automatically reconnects on transient failures with exponential backoff.
// Includes circuit breaker to avoid hammering a persistently failing endpoint.
func (q *BidiQueue) Run(ctx context.Context) error {
	reconnectDelay := q.config.ReconnectBaseDelay
	consecutiveFailures := 0

	for {
		err := q.runStream(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if err == nil {
			reconnectDelay = q.config.ReconnectBaseDelay
			consecutiveFailures = 0
			continue
		}

		consecutiveFailures++

		// Circuit breaker: after too many consecutive failures, pause longer.
		if consecutiveFailures >= q.config.MaxConsecutiveFailures {
			metrics.CircuitBreakerTripsTotal.WithLabelValues(metrics.ModeSource, metrics.ComponentStreamQueue).Inc()
			metrics.CircuitBreakerState.WithLabelValues(metrics.ModeSource, metrics.ComponentStreamQueue).
				Set(metrics.CircuitStateOpen)
			q.logger.ErrorContext(ctx, "circuit breaker triggered, pausing reconnection",
				"failures", consecutiveFailures,
				"pause", q.config.CircuitBreakerPause,
			)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(q.config.CircuitBreakerPause):
			}
			metrics.CircuitBreakerState.WithLabelValues(metrics.ModeSource, metrics.ComponentStreamQueue).
				Set(metrics.CircuitStateClosed)
			consecutiveFailures = 0
			reconnectDelay = q.config.ReconnectBaseDelay
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

		reconnectDelay = min(reconnectDelay*reconnectBackoffFactor, q.config.ReconnectMaxDelay)
	}
}

// runStream runs a single streaming session using multiple parallel streams.
func (q *BidiQueue) runStream(ctx context.Context) error {
	// Clear stale init cache before opening new streams. After a destination server
	// restart the old init state is invalid — without this, ensureTorrentInitialized
	// returns the cached result and skips InitTorrent, causing WritePiece to fail
	// with "not initialized". Clearing here (before pool.Open) is race-free because
	// no sender workers are running yet.
	q.dest.ClearInitCache()

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

	// Reset streaming gauges so they don't retain stale values while idle.
	metrics.InflightPieces.Set(0)
	metrics.StreamPoolSize.Set(0)
	metrics.AdaptiveWindowSize.Set(0)
	metrics.TransferThroughputBytesPerSecond.Set(0)
	metrics.StreamPoolScalingPaused.Set(0)

	select {
	case streamError := <-streamErr:
		return streamError
	default:
		return nil
	}
}

// runSenderPool spawns N sender workers that pull from tracker.Completed() concurrently,
// plus a dedicated stats reporter goroutine.
func (q *BidiQueue) runSenderPool(ctx context.Context, pool *StreamPool, stopSender <-chan struct{}) {
	numSenders := q.config.NumSenders
	q.logger.InfoContext(ctx, "starting sender workers", "count", numSenders)

	var wg sync.WaitGroup

	// Periodic stats reporter.
	wg.Go(func() {
		statsTicker := time.NewTicker(windowStatsInterval)
		defer statsTicker.Stop()
		for {
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
			}
		}
	})

	// Sender workers -- all pull from the same tracker.Completed() channel.
	for i := range numSenders {
		wg.Go(func() {
			q.senderWorker(ctx, pool, stopSender, i)
		})
	}

	wg.Wait()
}

// senderWorker is the per-piece send loop run by each sender goroutine.
func (q *BidiQueue) senderWorker(ctx context.Context, pool *StreamPool, stopSender <-chan struct{}, id int) {
	for {
		// Wait for any stream to have capacity.
		if !pool.CanSend() {
			metrics.WindowFullTotal.Inc()
			for !pool.CanSend() {
				select {
				case <-ctx.Done():
					return
				case <-stopSender:
					return
				case <-pool.AckReady():
					// Fast path: woken by ack arrival.
				case <-time.After(senderRetryBackoff):
					// Safety net: recheck capacity periodically.
					// Handles missed AckReady signals and stale-cleanup capacity changes.
				}
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-stopSender:
			return
		case piece, ok := <-q.tracker.Completed():
			if !ok {
				return
			}
			if err := q.sendPiecePool(ctx, pool, piece); err != nil {
				q.logger.ErrorContext(ctx, "failed to send piece",
					"hash", piece.GetTorrentHash(),
					"piece", piece.GetIndex(),
					"sender", id,
					"error", err,
				)
				q.tracker.MarkFailed(piece.GetTorrentHash(), int(piece.GetIndex()))
				q.piecesFail.Add(1)
			}
		}
	}
}

// ensureTorrentInitialized initializes the torrent on destination if not already done,
// and marks pieces already present on destination as streamed.
func (q *BidiQueue) ensureTorrentInitialized(ctx context.Context, hash string) error {
	if q.dest.IsInitialized(hash) {
		return nil
	}

	meta, ok := q.tracker.GetTorrentMetadata(hash)
	if !ok {
		return fmt.Errorf("torrent metadata not found: %s", hash)
	}
	result, initErr := q.dest.InitTorrent(ctx, meta.InitTorrentRequest)
	if initErr != nil {
		return fmt.Errorf("initializing torrent: %w", initErr)
	}

	if result == nil || len(result.PiecesNeeded) == 0 {
		return nil
	}

	// Mark pieces NOT needed (already on destination) as already streamed
	// PiecesNeeded[i] = false means piece i is already written on destination
	var alreadyOnCold int
	for i, needed := range result.PiecesNeeded {
		if !needed {
			q.tracker.MarkStreamed(hash, i)
			alreadyOnCold++
		}
	}
	if alreadyOnCold > 0 {
		q.logger.InfoContext(ctx, "marked pieces already on destination as streamed",
			"hash", hash,
			"count", alreadyOnCold,
		)
	}

	return nil
}

// sendPiecePool reads piece data and sends it over the best available stream.
func (q *BidiQueue) sendPiecePool(ctx context.Context, pool *StreamPool, piece *pb.Piece) error {
	sendStart := time.Now()
	hash := piece.GetTorrentHash()
	index := piece.GetIndex()
	key := pieceKey(hash, index)

	if err := q.ensureTorrentInitialized(ctx, hash); err != nil {
		return err
	}

	// Skip pieces already covered (e.g. hardlinked on destination before this piece was dequeued)
	if q.tracker.IsPieceStreamed(hash, int(index)) {
		q.logger.DebugContext(ctx, "skipping piece covered by hardlink",
			"hash", hash,
			"piece", index,
		)
		return nil
	}

	// Acquire window slot BEFORE the disk read so we fail fast when the
	// window is full, avoiding a wasted NFS I/O round-trip.
	ps, selectErr := pool.SelectStream()
	if selectErr != nil {
		return fmt.Errorf("selecting stream: %w", selectErr)
	}

	if !ps.window.TrySend(key) {
		q.logger.DebugContext(ctx, "window full, skipping disk read",
			"hash", hash,
			"piece", index,
			"stream", ps.id,
		)
		return errors.New("window full")
	}

	data, err := q.source.ReadPiece(ctx, piece)
	if err != nil {
		ps.window.OnFail(key)
		return fmt.Errorf("reading piece: %w", err)
	}

	if q.limiter != nil {
		if waitErr := q.waitForRateLimit(ctx, len(data)); waitErr != nil {
			ps.window.OnFail(key)
			return fmt.Errorf("rate limit: %w", waitErr)
		}
	}

	req := &pb.WritePieceRequest{
		TorrentHash: hash,
		PieceIndex:  index,
		Offset:      piece.GetOffset(),
		Size:        piece.GetSize(),
		Data:        data,
	}

	if sendErr := ps.stream.Send(req); sendErr != nil {
		ps.window.OnFail(key)
		return fmt.Errorf("sending: %w", sendErr)
	}

	metrics.PiecesSentTotal.WithLabelValues(ps.connLabel).Inc()
	metrics.BytesSentTotal.WithLabelValues(ps.connLabel).Add(float64(len(data)))
	metrics.PieceSendDuration.WithLabelValues(ps.connLabel).Observe(time.Since(sendStart).Seconds())

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

		case env := <-pool.Acks():
			q.processAck(ctx, env)
			pool.NotifyAckProcessed()
		}
	}
}

// handleStalePiecesPool checks for pieces that have been in-flight too long across all streams.
func (q *BidiQueue) handleStalePiecesPool(ctx context.Context, pool *StreamPool) {
	staleKeys := pool.GetAllStaleKeys()
	if len(staleKeys) == 0 {
		return
	}

	metrics.StalePiecesTotal.Add(float64(len(staleKeys)))
	q.logger.WarnContext(ctx, "found stale in-flight pieces, marking as failed",
		"count", len(staleKeys),
	)

	for _, sk := range staleKeys {
		// OnFail on the stream that actually owns the key in its window.
		sk.Stream.window.OnFail(sk.Key)
		q.requeuePieceByKey(ctx, sk.Key)
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

	for _, key := range keys {
		q.requeuePieceByKey(ctx, key)
	}
}

// processAck handles a single acknowledgment using the correct stream's window.
// The envelope carries the source stream so no external piece-to-stream
// lookup is needed.
func (q *BidiQueue) processAck(ctx context.Context, env AckEnvelope) {
	ack := env.Ack
	ps := env.Stream
	hash := ack.GetTorrentHash()
	index := int(ack.GetPieceIndex())
	key := pieceKey(hash, int32(index))

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

		// Clear any accumulated hash mismatch failures on success.
		q.clearHashMismatch(key)

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
		q.piecesFail.Add(1)
		q.tracker.MarkFailed(hash, index)

		switch ack.GetErrorCode() { //nolint:exhaustive // IO, FINALIZING, NONE handled by default.
		case pb.PieceErrorCode_PIECE_ERROR_HASH_MISMATCH:
			metrics.PieceHashMismatchTotal.Inc()
			q.handleHashMismatch(ctx, hash, index, key, streamID, ack.GetError())

		case pb.PieceErrorCode_PIECE_ERROR_NOT_INITIALIZED:
			// Clear init cache so next send triggers re-init
			q.dest.ClearInitResult(hash)
			q.logger.WarnContext(ctx, "piece write failed, torrent not initialized",
				"hash", hash,
				"piece", index,
				"stream", streamID,
				"error", ack.GetError(),
			)

		default: // IO, FINALIZING, NONE (old destination), unknown
			q.logger.WarnContext(ctx, "piece write failed",
				"hash", hash,
				"piece", index,
				"stream", streamID,
				"error", ack.GetError(),
				"errorCode", ack.GetErrorCode(),
			)
		}
	}
}

// handleHashMismatch tracks consecutive hash mismatch failures for a piece.
// After maxPieceHashMismatches, it forces the piece to streamed so finalization
// can surface an INCOMPLETE error rather than retrying a corrupt piece forever.
func (q *BidiQueue) handleHashMismatch(
	ctx context.Context,
	hash string,
	index int,
	key string,
	streamID int,
	errMsg string,
) {
	q.pieceHashMismatchesMu.Lock()
	q.pieceHashMismatches[key]++
	mismatches := q.pieceHashMismatches[key]
	if mismatches >= maxPieceHashMismatches {
		delete(q.pieceHashMismatches, key)
	}
	q.pieceHashMismatchesMu.Unlock()

	if mismatches >= maxPieceHashMismatches {
		q.tracker.MarkStreamed(hash, index)
		q.logger.ErrorContext(ctx, "piece hash mismatch limit reached, forcing finalization attempt",
			"hash", hash,
			"piece", index,
			"stream", streamID,
			"mismatches", mismatches,
		)
		return
	}

	q.logger.ErrorContext(ctx, "piece hash mismatch, will retry",
		"hash", hash,
		"piece", index,
		"stream", streamID,
		"mismatches", mismatches,
		"error", errMsg,
	)
}

// drainInFlightPool waits for in-flight pieces across all streams to be acknowledged.
func (q *BidiQueue) drainInFlightPool(ctx context.Context, pool *StreamPool) {
	totalInFlight := pool.TotalInFlight()
	if totalInFlight == 0 {
		return
	}

	q.logger.InfoContext(ctx, "draining in-flight pieces", "count", totalInFlight)

	// Use Background so the drain isn't immediately cancelled when the parent
	// context triggered the shutdown. The drainTimeout is the hard deadline.
	drainCtx, cancel := context.WithTimeout(context.Background(), drainTimeout)
	defer cancel()

	for pool.TotalInFlight() > 0 {
		select {
		case <-drainCtx.Done():
			remaining := pool.TotalInFlight()
			metrics.DrainTimeoutPiecesLostTotal.Add(float64(remaining))
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

		case env := <-pool.Acks():
			q.processAck(ctx, env)
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
