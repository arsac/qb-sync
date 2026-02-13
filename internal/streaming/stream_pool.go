package streaming

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"runtime/debug"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/arsac/qb-sync/internal/congestion"
	"github.com/arsac/qb-sync/internal/metrics"
	pb "github.com/arsac/qb-sync/proto"
)

const (
	// DefaultPoolSize is the default number of parallel streams.
	DefaultPoolSize = 8

	// MinPoolSize is the minimum allowed pool size.
	MinPoolSize = 4

	// MaxPoolSize is the maximum allowed pool size.
	MaxPoolSize = 32

	// DefaultAckChannelSize is the default buffer size for ack aggregation per stream.
	DefaultAckChannelSize = 1000

	// Adaptive scaling constants.
	defaultScaleInterval      = 5 * time.Second // How often to check for scaling
	defaultScaleUpThreshold   = 0.05            // 5% throughput increase to scale up
	defaultScaleDownThreshold = 0.15            // 15% throughput decrease to scale down
	defaultPlateauThreshold   = 0.03            // <3% change considered plateau
	defaultPlateauCount       = 3               // Consecutive plateaus before stopping
	scalingCooldownPeriod     = 2 * time.Minute // Cooldown before resuming scaling after pause

	// streamDrainTimeout is how long drainAndRemoveStream waits for in-flight
	// pieces to complete before closing the stream anyway.
	streamDrainTimeout = 30 * time.Second

	// drainPollInterval is how often drainAndRemoveStream checks whether
	// in-flight pieces have completed.
	drainPollInterval = 100 * time.Millisecond

	// Conversion constants.
	bytesPerMB      = 1024 * 1024
	percentMultiple = 100
)

// ErrPoolClosed is returned when an operation is attempted on a closed pool.
var ErrPoolClosed = errors.New("stream pool is closed")

// PooledStream wraps a PieceStream with its own AdaptiveWindow for
// independent congestion control per stream.
//
// Thread safety: PooledStream is owned by StreamPool. The stream and window
// fields are accessed via the parent pool's mutex. The atomic stat fields
// (bytesSent, piecesOK, piecesFail) may be read without holding the lock.
type PooledStream struct {
	stream    *PieceStream
	window    *congestion.AdaptiveWindow
	id        int
	connLabel string // Pre-computed gRPC connection index label for metrics

	// Graceful drain lifecycle
	draining atomic.Bool // Set true to stop sending new pieces; stream drains in-flight
	removed  atomic.Bool // Set true when fully drained; forwardAcks exits silently

	// Stats - atomic for lock-free reads
	bytesSent  atomic.Int64
	piecesOK   atomic.Int64
	piecesFail atomic.Int64
}

// StreamPool manages multiple parallel bidirectional streams for
// increased throughput. Each stream has independent congestion control.
// Supports adaptive scaling based on throughput measurements.
//
// Thread safety: All public methods are safe for concurrent use.
// The pool uses a RWMutex for stream slice access and atomic operations
// for lifecycle management.
type StreamPool struct {
	dest         *GRPCDestination
	logger       *slog.Logger
	windowConfig congestion.Config
	maxStreams   int

	streams []*PooledStream
	nextID  int // Next stream ID to assign
	mu      sync.RWMutex

	// Aggregated channels from all streams
	acks     chan *pb.PieceAck
	ackReady chan struct{}
	errs     chan error // Aggregated errors from all streams

	// Adaptive scaling state (protected by mu)
	adaptive          bool // Whether adaptive scaling is enabled
	scaleInterval     time.Duration
	lastThroughput    float64 // Bytes/sec from last interval
	lastBytesSent     int64   // Total bytes at last measurement
	removedBytesSent  int64   // Cumulative bytes from removed streams
	lastMeasureTime   time.Time
	plateauCount      int       // Consecutive intervals with <5% change
	scalingPaused     bool      // Stop scaling after saturation detected
	scalingPausedTime time.Time // When scaling was paused (for cooldown)

	// Connection-level scaling state (protected by mu)
	preConnectionThroughput     float64   // Throughput before last connection add
	connectionAddedTime         time.Time // When last connection was added
	connectionScaleCheckPending bool      // Awaiting diminishing returns check

	// Lifecycle
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	closed    atomic.Bool
	closeOnce sync.Once
}

// StreamPoolConfig configures the stream pool.
type StreamPoolConfig struct {
	// NumStreams is the initial number of streams (default: 2 if adaptive, 4 otherwise).
	// Renamed from PoolSize for consistency with BidiQueueConfig.
	NumStreams int

	// MaxNumStreams is the maximum streams for adaptive scaling (default: 16).
	// Renamed from MaxPoolSize for consistency with BidiQueueConfig.
	MaxNumStreams int

	// AckChannelSize is the buffer size for the aggregated ack channel per stream.
	// Default: 1000. Total buffer = AckChannelSize * MaxNumStreams.
	AckChannelSize int

	// AdaptiveWindow configures each stream's congestion control window.
	AdaptiveWindow congestion.Config

	// Adaptive enables adaptive stream scaling based on throughput (default: true).
	Adaptive bool

	// ScaleInterval is how often to check throughput for scaling decisions (default: 10s).
	ScaleInterval time.Duration
}

// DefaultStreamPoolConfig returns sensible defaults with adaptive scaling enabled.
func DefaultStreamPoolConfig() StreamPoolConfig {
	return StreamPoolConfig{
		NumStreams:     MinPoolSize, // Start small with adaptive
		MaxNumStreams:  MaxPoolSize,
		AckChannelSize: DefaultAckChannelSize,
		AdaptiveWindow: congestion.DefaultConfig(),
		Adaptive:       true,
		ScaleInterval:  defaultScaleInterval,
	}
}

// NewStreamPool creates a new stream pool.
func NewStreamPool(
	dest *GRPCDestination,
	logger *slog.Logger,
	config StreamPoolConfig,
) *StreamPool {
	// Apply defaults
	maxStreams := config.MaxNumStreams
	if maxStreams <= 0 {
		maxStreams = MaxPoolSize
	}
	maxStreams = min(maxStreams, MaxPoolSize)

	scaleInterval := config.ScaleInterval
	if scaleInterval <= 0 {
		scaleInterval = defaultScaleInterval
	}

	ackChannelSize := config.AckChannelSize
	if ackChannelSize <= 0 {
		ackChannelSize = DefaultAckChannelSize
	}

	return &StreamPool{
		dest:          dest,
		logger:        logger,
		windowConfig:  config.AdaptiveWindow,
		maxStreams:    maxStreams,
		streams:       make([]*PooledStream, 0, maxStreams),
		acks:          make(chan *pb.PieceAck, ackChannelSize*maxStreams),
		ackReady:      make(chan struct{}, maxStreams),
		errs:          make(chan error, maxStreams), // Aggregated error channel
		adaptive:      config.Adaptive,
		scaleInterval: scaleInterval,
	}
}

// Open opens initial streams in the pool and starts adaptive scaling if enabled.
func (p *StreamPool) Open(ctx context.Context, numStreams int) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Check closed inside lock to avoid TOCTOU race
	if p.closed.Load() {
		return ErrPoolClosed
	}

	p.ctx, p.cancel = context.WithCancel(ctx)

	// Clamp stream count
	numStreams = max(min(numStreams, p.maxStreams), MinPoolSize)

	p.logger.InfoContext(ctx, "opening stream pool",
		"initialStreams", numStreams,
		"maxStreams", p.maxStreams,
		"adaptive", p.adaptive,
	)

	for range numStreams {
		if err := p.addStreamLocked(); err != nil {
			p.closeStreamsLocked()
			return fmt.Errorf("opening initial streams: %w", err)
		}
	}

	// Set initial pool metrics
	metrics.StreamPoolSize.Set(float64(len(p.streams)))
	metrics.StreamPoolMaxSize.Set(float64(p.maxStreams))

	// Initialize throughput tracking
	p.lastMeasureTime = time.Now()
	p.lastBytesSent = p.getTotalBytesSentLocked()

	// Start throughput monitor (always runs; scaling decisions gated by p.adaptive)
	p.wg.Add(1)
	go p.runThroughputMonitor()

	return nil
}

// forwardAcks reads acks from a single stream and forwards them to the pool's
// aggregated channels. Also forwards errors from the stream.
func (p *StreamPool) forwardAcks(ps *PooledStream) { //nolint:gocognit // complexity from panic recovery
	defer p.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			p.logger.Error("panic in forwardAcks",
				"streamID", ps.id,
				"panic", r,
				"stack", string(debug.Stack()),
			)
			select {
			case p.errs <- fmt.Errorf("panic in forwardAcks (stream %d): %v", ps.id, r):
			default:
			}
		}
	}()

	for {
		select {
		case <-p.ctx.Done():
			return

		case <-ps.stream.Done():
			// Intentionally removed stream — exit silently without error.
			if ps.removed.Load() {
				return
			}

			// Stream ended. Drain any pending error so it gets forwarded
			// to the pool. Without this, a select race between Done() and
			// Errors() can silently drop the stream error, leaving the
			// ack processor unaware that the stream died.
			select {
			case err := <-ps.stream.Errors():
				select {
				case p.errs <- err:
				default:
					p.logger.WarnContext(p.ctx, "error channel full, dropping error on stream close",
						"streamID", ps.id,
						"error", err,
					)
				}
			default:
				// Stream closed without an explicit error (e.g., send timeout
				// cancelled context). Notify pool so ack processor can trigger
				// reconnection. Skip during clean shutdown (pool.Close cancels p.ctx).
				if p.ctx.Err() == nil {
					select {
					case p.errs <- fmt.Errorf("stream %d closed unexpectedly", ps.id):
					case <-p.ctx.Done():
						// Pool closing between the check and send — no need to report.
					default:
						p.logger.WarnContext(
							p.ctx,
							"error channel full, dropping synthetic error on silent stream close",
							"streamID",
							ps.id,
						)
					}
				}
			}
			return

		case err, ok := <-ps.stream.Errors():
			if !ok {
				continue
			}
			// Forward error to aggregated error channel (non-blocking)
			select {
			case p.errs <- err:
			default:
				// Error channel full, log and continue
				p.logger.WarnContext(p.ctx, "error channel full, dropping error",
					"streamID", ps.id,
					"error", err,
				)
			}

		case ack, ok := <-ps.stream.Acks():
			if !ok {
				return
			}

			// Forward to pool's aggregated ack channel (blocking with context)
			select {
			case p.acks <- ack:
				// Signal that an ack is ready (non-blocking)
				select {
				case p.ackReady <- struct{}{}:
				default:
				}
			case <-p.ctx.Done():
				return
			}
		}
	}
}

// addStreamLocked adds a new stream to the pool. Must hold p.mu write lock.
func (p *StreamPool) addStreamLocked() error {
	if len(p.streams) >= p.maxStreams {
		return errors.New("pool at maximum capacity")
	}

	stream, err := p.dest.OpenStream(p.ctx, p.logger)
	if err != nil {
		return fmt.Errorf("opening stream: %w", err)
	}

	ps := &PooledStream{
		stream:    stream,
		window:    congestion.NewAdaptiveWindow(p.windowConfig),
		id:        p.nextID,
		connLabel: strconv.Itoa(stream.connIdx),
	}
	p.nextID++
	p.streams = append(p.streams, ps)

	// Start ack forwarder for this stream
	p.wg.Add(1)
	go p.forwardAcks(ps)

	return nil
}

// AddStream dynamically adds a new stream to the pool.
// Returns error if pool is at maximum capacity or closed.
func (p *StreamPool) AddStream() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Check closed inside lock to avoid TOCTOU race
	if p.closed.Load() {
		return ErrPoolClosed
	}

	return p.addStreamLocked()
}

// RemoveStream removes the most recently added stream from the pool.
// Returns error if pool is at minimum capacity or closed.
func (p *StreamPool) RemoveStream() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Check closed inside lock to avoid TOCTOU race
	if p.closed.Load() {
		return ErrPoolClosed
	}

	return p.removeStreamLocked()
}

// runThroughputMonitor periodically measures throughput and adjusts stream count if adaptive.
func (p *StreamPool) runThroughputMonitor() {
	defer p.wg.Done()

	ticker := time.NewTicker(p.scaleInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			p.updateThroughput()
		}
	}
}

// updateThroughput measures throughput, updates the gauge, and optionally makes scaling decisions.
func (p *StreamPool) updateThroughput() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed.Load() {
		return
	}

	currentThroughput, ok := p.measureThroughput()
	if !ok {
		return
	}

	// Always update throughput gauge
	p.lastThroughput = currentThroughput
	metrics.TransferThroughputBytesPerSecond.Set(currentThroughput)

	// Only make scaling decisions if adaptive
	if p.adaptive && !p.isInCooldown() {
		p.applyScalingDecision(currentThroughput)
	}
}

// isInCooldown checks if scaling is paused and handles cooldown expiry.
// Must hold p.mu.
func (p *StreamPool) isInCooldown() bool {
	if !p.scalingPaused {
		return false
	}

	if time.Since(p.scalingPausedTime) < scalingCooldownPeriod {
		return true
	}

	// Resume scaling after cooldown
	p.scalingPaused = false
	p.plateauCount = 0
	metrics.StreamPoolScalingPaused.Set(0)
	p.logger.InfoContext(p.ctx, "scaling resumed after cooldown",
		"streams", len(p.streams),
	)
	return false
}

// measureThroughput calculates current throughput and updates baseline.
// Returns throughput and false if measurement should be skipped.
// Must hold p.mu.
func (p *StreamPool) measureThroughput() (float64, bool) {
	now := time.Now()
	elapsed := now.Sub(p.lastMeasureTime).Seconds()
	if elapsed < 1 {
		return 0, false // Too soon
	}

	currentBytes := p.getTotalBytesSentLocked()
	bytesDelta := currentBytes - p.lastBytesSent
	currentThroughput := float64(bytesDelta) / elapsed

	// Update measurement baseline
	p.lastMeasureTime = now
	p.lastBytesSent = currentBytes

	// Skip first interval (no baseline)
	if p.lastThroughput == 0 {
		p.lastThroughput = currentThroughput
		p.logger.InfoContext(p.ctx, "adaptive scaling baseline",
			"throughputMBps", currentThroughput/bytesPerMB,
			"streams", len(p.streams),
		)
		return 0, false
	}

	return currentThroughput, true
}

// applyScalingDecision applies scaling logic based on throughput change.
// Must hold p.mu.
func (p *StreamPool) applyScalingDecision(currentThroughput float64) {
	// Check diminishing returns from recent connection add
	if p.connectionScaleCheckPending && time.Since(p.connectionAddedTime) >= 2*p.scaleInterval {
		p.connectionScaleCheckPending = false
		var improvement float64
		if p.preConnectionThroughput > 0 {
			improvement = (currentThroughput - p.preConnectionThroughput) / p.preConnectionThroughput
		}
		if improvement < defaultScaleUpThreshold {
			p.logger.InfoContext(p.ctx, "diminishing returns from connection add",
				"improvementPercent", improvement*percentMultiple,
				"connections", p.dest.ConnectionCount(),
			)
			p.tryConnectionScaleDown()
			p.pauseScaling("diminishing returns")
			return
		}
		p.logger.InfoContext(p.ctx, "connection add effective",
			"improvementPercent", improvement*percentMultiple,
			"connections", p.dest.ConnectionCount(),
		)
	}

	var changeRatio float64
	if p.lastThroughput > 0 {
		changeRatio = (currentThroughput - p.lastThroughput) / p.lastThroughput
	}

	streamCount := len(p.streams)

	p.logger.DebugContext(p.ctx, "adaptive scaling check",
		"throughputMBps", currentThroughput/bytesPerMB,
		"lastThroughputMBps", p.lastThroughput/bytesPerMB,
		"changePercent", changeRatio*percentMultiple,
		"streams", streamCount,
		"plateauCount", p.plateauCount,
	)

	switch {
	case changeRatio > defaultScaleUpThreshold && streamCount < p.maxStreams:
		p.tryScaleUp()

	case changeRatio < -defaultScaleDownThreshold && streamCount > MinPoolSize:
		p.tryScaleDown()

	case math.Abs(changeRatio) < defaultPlateauThreshold:
		p.handlePlateau(currentThroughput)

	default:
		// Moderate change - reset plateau count but don't scale
		p.plateauCount = 0
	}
}

// tryScaleUp attempts to add a stream. Must hold p.mu.
func (p *StreamPool) tryScaleUp() {
	if err := p.addStreamLocked(); err != nil {
		metrics.StreamOpenErrorsTotal.WithLabelValues(metrics.ModeSource).Inc()
		p.logger.WarnContext(p.ctx, "failed to add stream", "error", err)
		return
	}
	metrics.StreamPoolSize.Set(float64(len(p.streams)))
	p.logger.InfoContext(p.ctx, "scaled up",
		"streams", len(p.streams),
		"reason", "throughput increased",
	)
	p.plateauCount = 0
}

// tryScaleDown attempts to remove a stream. If streams are at minimum,
// tries removing a TCP connection instead. Pauses scaling. Must hold p.mu.
func (p *StreamPool) tryScaleDown() {
	if err := p.removeStreamLocked(); err != nil {
		// Streams at minimum — try connection-level scale-down
		if p.dest.ConnectionCount() > p.dest.MinConnections() {
			p.tryConnectionScaleDown()
			p.pauseScaling("throughput decreased, removing connection")
			return
		}
		p.logger.WarnContext(p.ctx, "failed to remove stream", "error", err)
		return
	}
	metrics.StreamPoolSize.Set(float64(len(p.streams)))
	p.logger.InfoContext(p.ctx, "scaled down",
		"streams", len(p.streams),
		"reason", "throughput decreased",
	)
	p.pauseScaling("throughput decreased")
}

// handlePlateau handles throughput plateau detection. When stream scaling
// plateaus, attempts to add a TCP connection (two-level staircase).
// Must hold p.mu.
func (p *StreamPool) handlePlateau(currentThroughput float64) {
	p.plateauCount++
	if p.plateauCount < defaultPlateauCount {
		return
	}

	// Try connection-level scaling before giving up
	if p.dest.ConnectionCount() < p.dest.MaxConnections() {
		if err := p.dest.AddConnection(); err != nil {
			p.logger.WarnContext(p.ctx, "failed to add connection", "error", err)
			p.pauseScaling("connection add failed")
			return
		}

		p.preConnectionThroughput = currentThroughput
		p.connectionAddedTime = time.Now()
		p.connectionScaleCheckPending = true
		p.plateauCount = 0 // Resume stream scaling on new baseline

		connCount := p.dest.ConnectionCount()
		metrics.GRPCConnectionsActive.Set(float64(connCount))
		metrics.ConnectionScaleEventsTotal.WithLabelValues(metrics.DirectionUp).Inc()
		p.logger.InfoContext(p.ctx, "added TCP connection",
			"connections", connCount,
			"throughputMBps", currentThroughput/bytesPerMB,
		)
		return
	}

	// At max connections — full saturation, pause unconditionally
	p.logger.InfoContext(p.ctx, "full saturation detected",
		"streams", len(p.streams),
		"connections", p.dest.ConnectionCount(),
		"throughputMBps", currentThroughput/bytesPerMB,
	)
	p.pauseScaling("full saturation")
}

// pauseScaling pauses adaptive scaling with cooldown. Must hold p.mu.
func (p *StreamPool) pauseScaling(reason string) {
	p.scalingPaused = true
	p.scalingPausedTime = time.Now()
	metrics.StreamPoolScalingPaused.Set(1)
	p.logger.InfoContext(p.ctx, "scaling paused",
		"reason", reason,
		"cooldown", scalingCooldownPeriod,
	)
}

// tryConnectionScaleDown removes the last TCP connection and its streams.
// Must hold p.mu. The actual drain runs asynchronously.
func (p *StreamPool) tryConnectionScaleDown() {
	connCount := p.dest.ConnectionCount()
	if connCount <= p.dest.MinConnections() {
		return
	}

	connIdx := connCount - 1
	p.wg.Go(func() {
		p.removeConnectionStreams(connIdx)
	})
}

// removeConnectionStreams drains all streams on the given connection index,
// then removes the connection. Called as a goroutine.
func (p *StreamPool) removeConnectionStreams(connIdx int) {
	p.mu.Lock()
	var toDrain []*PooledStream
	for _, ps := range p.streams {
		if ps.stream.connIdx == connIdx {
			ps.draining.Store(true)
			toDrain = append(toDrain, ps)
		}
	}
	p.mu.Unlock()

	// Drain all streams on this connection
	var wg sync.WaitGroup
	for _, ps := range toDrain {
		wg.Go(func() {
			p.drainAndRemoveStream(ps)
		})
	}
	wg.Wait()

	// All streams drained — safe to close the connection
	if err := p.dest.RemoveConnection(connIdx); err != nil {
		p.logger.WarnContext(p.ctx, "failed to remove connection", "error", err)
		return
	}

	connCount := p.dest.ConnectionCount()
	metrics.GRPCConnectionsActive.Set(float64(connCount))
	metrics.ConnectionScaleEventsTotal.WithLabelValues(metrics.DirectionDown).Inc()
	metrics.StreamPoolSize.Set(float64(p.StreamCount()))
	p.logger.InfoContext(p.ctx, "removed TCP connection",
		"connections", connCount,
	)
}

// removeStreamLocked initiates graceful drain of the last stream. Must hold p.mu write lock.
// The stream stays in the slice during drain (so forwardAcks keeps running and acks flow back)
// but findLeastLoadedStream skips it. drainAndRemoveStream removes it after drain completes.
func (p *StreamPool) removeStreamLocked() error {
	if len(p.streams) <= MinPoolSize {
		return errors.New("pool at minimum capacity")
	}

	lastIdx := len(p.streams) - 1
	ps := p.streams[lastIdx]

	// Mark as draining before spawning goroutine so SelectStream() skips it
	// immediately when p.mu is released, avoiding a scheduling race.
	ps.draining.Store(true)

	// Don't remove from slice yet — drainAndRemoveStream does it after drain
	p.wg.Go(func() {
		p.drainAndRemoveStream(ps)
	})

	return nil
}

// drainAndRemoveStream gracefully drains a stream's in-flight pieces, then removes it from the pool.
// Called as a goroutine by removeStreamLocked and removeConnectionStreams.
func (p *StreamPool) drainAndRemoveStream(ps *PooledStream) {
	ps.draining.Store(true) // Idempotent — callers pre-set this under p.mu

	// Wait for in-flight pieces to drain (with timeout)
	deadline := time.NewTimer(streamDrainTimeout)
	ticker := time.NewTicker(drainPollInterval)
	defer deadline.Stop()
	defer ticker.Stop()

	for ps.window.InFlight() > 0 {
		select {
		case <-deadline.C:
			p.logger.WarnContext(p.ctx, "stream drain timeout, closing with in-flight pieces",
				"streamID", ps.id,
				"inFlight", ps.window.InFlight(),
			)
			goto remove
		case <-p.ctx.Done():
			return // Pool closing
		case <-ticker.C:
			// Re-check in-flight
		}
	}

remove:
	p.mu.Lock()
	ps.removed.Store(true)
	for i, s := range p.streams {
		if s == ps {
			p.streams = append(p.streams[:i], p.streams[i+1:]...)
			break
		}
	}
	p.removedBytesSent += ps.bytesSent.Load()
	p.mu.Unlock()

	ps.stream.Close()
}

// getTotalBytesSentLocked returns total bytes sent across all streams. Must hold p.mu.
// Includes bytes from streams that have been removed.
func (p *StreamPool) getTotalBytesSentLocked() int64 {
	total := p.removedBytesSent
	for _, ps := range p.streams {
		total += ps.bytesSent.Load()
	}
	return total
}

// SelectStream returns the best stream for sending a piece.
// Uses least-loaded selection (fewest in-flight pieces).
// If no stream can send, returns the least-loaded stream for waiting.
func (p *StreamPool) SelectStream() (*PooledStream, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.streams) == 0 {
		return nil, errors.New("no streams available")
	}

	// First pass: find stream with lowest in-flight count that can send
	if best := p.findLeastLoadedStream(true); best != nil {
		return best, nil
	}

	// Second pass: no stream can send, return least-loaded for waiting
	best := p.findLeastLoadedStream(false)
	if best == nil {
		// Should never happen if streams is non-empty, but be defensive
		return nil, errors.New("no streams available")
	}
	return best, nil
}

// findLeastLoadedStream finds the stream with lowest in-flight count.
// If requireCanSend is true, only considers streams that can accept new pieces.
// Must hold p.mu (read or write).
func (p *StreamPool) findLeastLoadedStream(requireCanSend bool) *PooledStream {
	var best *PooledStream
	bestInFlight := math.MaxInt

	for _, ps := range p.streams {
		if ps.draining.Load() {
			continue
		}
		if requireCanSend && !ps.window.CanSend() {
			continue
		}
		inFlight := ps.window.InFlight()
		if inFlight < bestInFlight {
			best = ps
			bestInFlight = inFlight
		}
	}

	return best
}

// CanSend returns true if any non-draining stream in the pool can accept a piece.
func (p *StreamPool) CanSend() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()

	for _, ps := range p.streams {
		if ps.draining.Load() {
			continue
		}
		if ps.window.CanSend() {
			return true
		}
	}
	return false
}

// TotalInFlight returns the total in-flight pieces across all streams.
func (p *StreamPool) TotalInFlight() int {
	p.mu.RLock()
	defer p.mu.RUnlock()

	total := 0
	for _, ps := range p.streams {
		total += ps.window.InFlight()
	}
	return total
}

// Acks returns the aggregated ack channel from all streams.
func (p *StreamPool) Acks() <-chan *pb.PieceAck {
	return p.acks
}

// AckReady returns a channel that signals when acks are available.
func (p *StreamPool) AckReady() <-chan struct{} {
	return p.ackReady
}

// NotifyAckProcessed signals that an ack has been processed and inflight
// count reduced. This wakes the sender to re-check CanSend().
//
// forwardAcks signals ackReady when an ack is enqueued, but the sender
// checks CanSend() which depends on OnAck having reduced inflight. Without
// this post-processing signal, the sender can consume the enqueue signal
// before OnAck fires, see CanSend()=false, and wait forever.
func (p *StreamPool) NotifyAckProcessed() {
	select {
	case p.ackReady <- struct{}{}:
	default:
	}
}

// Done returns a channel that's closed when any stream fails.
func (p *StreamPool) Done() <-chan struct{} {
	// Return context done - when cancelled, all streams should stop
	return p.ctx.Done()
}

// Errors returns the aggregated error channel from all streams.
// This channel never returns nil - it always returns a valid channel
// that will receive errors from any stream in the pool.
func (p *StreamPool) Errors() <-chan error {
	return p.errs
}

// StreamCount returns the number of active streams.
func (p *StreamPool) StreamCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.streams)
}

// Stats returns aggregated statistics from all streams.
func (p *StreamPool) Stats() StreamPoolStats {
	p.mu.RLock()
	defer p.mu.RUnlock()

	stats := StreamPoolStats{
		StreamCount:     len(p.streams),
		MaxStreams:      p.maxStreams,
		Streams:         make([]PooledStreamStats, len(p.streams)),
		AdaptiveEnabled: p.adaptive,
		ScalingPaused:   p.scalingPaused,
		ThroughputMBps:  p.lastThroughput / bytesPerMB,
	}

	var totalWindow int
	for i, ps := range p.streams {
		windowStats := ps.window.Stats()
		stats.Streams[i] = PooledStreamStats{
			ID:          ps.id,
			Window:      windowStats.Window,
			InFlight:    windowStats.InFlight,
			MinRTT:      windowStats.MinRTT,
			SmoothedRTT: windowStats.SmoothedRTT,
			BytesSent:   ps.bytesSent.Load(),
			PiecesOK:    ps.piecesOK.Load(),
			PiecesFail:  ps.piecesFail.Load(),
		}
		stats.TotalInFlight += windowStats.InFlight
		stats.TotalBytesSent += ps.bytesSent.Load()
		stats.TotalPiecesOK += ps.piecesOK.Load()
		stats.TotalPiecesFail += ps.piecesFail.Load()
		totalWindow += windowStats.Window
	}

	// Update Prometheus metrics
	metrics.InflightPieces.Set(float64(stats.TotalInFlight))
	if len(p.streams) > 0 {
		metrics.AdaptiveWindowSize.Set(float64(totalWindow) / float64(len(p.streams)))
	}

	return stats
}

// ClearAllInflight clears in-flight tracking from all streams.
// Returns all keys that were in-flight.
func (p *StreamPool) ClearAllInflight() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var allKeys []string
	for _, ps := range p.streams {
		keys := ps.window.ClearInflight()
		allKeys = append(allKeys, keys...)
	}
	return allKeys
}

// StaleKey pairs a stale piece key with the stream whose congestion window owns it.
type StaleKey struct {
	Key    string
	Stream *PooledStream
}

// GetAllStaleKeys returns stale keys from all streams, each paired with the
// owning stream. This avoids a TOCTOU race: between discovering the stale key
// and calling OnFail, a retry could overwrite the pieceStreams mapping, causing
// OnFail to target the wrong window and leaving the key permanently stuck.
func (p *StreamPool) GetAllStaleKeys() []StaleKey {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var allKeys []StaleKey
	for _, ps := range p.streams {
		for _, key := range ps.window.GetStaleKeys() {
			allKeys = append(allKeys, StaleKey{Key: key, Stream: ps})
		}
	}
	return allKeys
}

// Close closes all streams in the pool.
// Safe to call multiple times - subsequent calls are no-ops.
func (p *StreamPool) Close() {
	p.closeOnce.Do(func() {
		p.closed.Store(true)

		// Cancel context first to signal goroutines to stop
		if p.cancel != nil {
			p.cancel()
		}

		// Close all streams while holding lock
		p.mu.Lock()
		p.closeStreamsLocked()
		p.mu.Unlock()

		// Wait for all forwarder goroutines to finish
		// This ensures no more sends to channels after this point
		p.wg.Wait()

		// Now safe to close channels
		close(p.acks)
		close(p.ackReady)
		close(p.errs)
	})
}

// closeStreamsLocked closes all streams. Must hold p.mu.
func (p *StreamPool) closeStreamsLocked() {
	for _, ps := range p.streams {
		ps.stream.Close()
	}
	p.streams = p.streams[:0]
}

// StreamPoolStats contains aggregated pool statistics.
type StreamPoolStats struct {
	StreamCount     int
	MaxStreams      int
	TotalInFlight   int
	TotalBytesSent  int64
	TotalPiecesOK   int64
	TotalPiecesFail int64

	// Adaptive scaling state
	AdaptiveEnabled bool
	ScalingPaused   bool
	ThroughputMBps  float64

	Streams []PooledStreamStats
}

// PooledStreamStats contains per-stream statistics.
type PooledStreamStats struct {
	ID          int
	Window      int
	InFlight    int
	MinRTT      time.Duration
	SmoothedRTT time.Duration
	BytesSent   int64
	PiecesOK    int64
	PiecesFail  int64
}
