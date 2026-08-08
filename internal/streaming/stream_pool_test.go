package streaming

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/arsac/qb-sync/internal/congestion"
)

func TestStreamPoolConfig_Defaults(t *testing.T) {
	config := DefaultStreamPoolConfig()

	if config.MaxNumStreams != MaxPoolSize {
		t.Errorf("expected MaxNumStreams %d, got %d", MaxPoolSize, config.MaxNumStreams)
	}

	if config.AckChannelSize != DefaultAckChannelSize {
		t.Errorf("expected AckChannelSize %d, got %d", DefaultAckChannelSize, config.AckChannelSize)
	}

	if !config.Adaptive {
		t.Error("expected Adaptive to be true by default")
	}

	if config.ScaleInterval != defaultScaleInterval {
		t.Errorf("expected ScaleInterval %v, got %v", defaultScaleInterval, config.ScaleInterval)
	}
}

func TestStreamPoolConfig_Constants(t *testing.T) {
	// Verify constants have sensible values
	if MinPoolSize < 1 {
		t.Errorf("MinPoolSize should be at least 1, got %d", MinPoolSize)
	}

	if MaxPoolSize <= MinPoolSize {
		t.Errorf("MaxPoolSize (%d) should be greater than MinPoolSize (%d)", MaxPoolSize, MinPoolSize)
	}

	if DefaultPoolSize < MinPoolSize || DefaultPoolSize > MaxPoolSize {
		t.Errorf("DefaultPoolSize (%d) should be between MinPoolSize (%d) and MaxPoolSize (%d)",
			DefaultPoolSize, MinPoolSize, MaxPoolSize)
	}

	if DefaultAckChannelSize <= 0 {
		t.Errorf("DefaultAckChannelSize should be positive, got %d", DefaultAckChannelSize)
	}
}

func TestStreamPoolStats_ZeroValue(t *testing.T) {
	stats := StreamPoolStats{}

	if stats.StreamCount != 0 {
		t.Errorf("expected zero StreamCount, got %d", stats.StreamCount)
	}

	if stats.TotalInFlight != 0 {
		t.Errorf("expected zero TotalInFlight, got %d", stats.TotalInFlight)
	}

	if stats.AdaptiveEnabled {
		t.Error("expected AdaptiveEnabled to be false by default")
	}
}

func TestPooledStreamStats_ZeroValue(t *testing.T) {
	stats := PooledStreamStats{}

	if stats.ID != 0 {
		t.Errorf("expected zero ID, got %d", stats.ID)
	}

	if stats.Window != 0 {
		t.Errorf("expected zero Window, got %d", stats.Window)
	}

	if stats.InFlight != 0 {
		t.Errorf("expected zero InFlight, got %d", stats.InFlight)
	}
}

func TestErrPoolClosed(t *testing.T) {
	if ErrPoolClosed == nil {
		t.Error("ErrPoolClosed should not be nil")
	}

	if ErrPoolClosed.Error() != "stream pool is closed" {
		t.Errorf("unexpected error message: %s", ErrPoolClosed.Error())
	}
}

func TestScalingConstants(t *testing.T) {
	// Verify scaling constants are sensible
	if defaultScaleInterval <= 0 {
		t.Errorf("defaultScaleInterval should be positive, got %v", defaultScaleInterval)
	}

	if defaultScaleUpThreshold <= 0 || defaultScaleUpThreshold >= 1 {
		t.Errorf("defaultScaleUpThreshold should be between 0 and 1, got %v", defaultScaleUpThreshold)
	}

	if defaultScaleDownThreshold <= 0 || defaultScaleDownThreshold >= 1 {
		t.Errorf("defaultScaleDownThreshold should be between 0 and 1, got %v", defaultScaleDownThreshold)
	}

	if defaultPlateauThreshold <= 0 || defaultPlateauThreshold >= defaultScaleUpThreshold {
		t.Errorf("defaultPlateauThreshold (%v) should be between 0 and defaultScaleUpThreshold (%v)",
			defaultPlateauThreshold, defaultScaleUpThreshold)
	}

	if defaultPlateauCount <= 0 {
		t.Errorf("defaultPlateauCount should be positive, got %d", defaultPlateauCount)
	}

	if scalingCooldownPeriod <= 0 {
		t.Errorf("scalingCooldownPeriod should be positive, got %v", scalingCooldownPeriod)
	}
}

func TestBidiQueueConfig_Defaults(t *testing.T) {
	config := DefaultBidiQueueConfig()

	if config.MaxBytesPerSec != 0 {
		t.Errorf("expected unlimited rate (0), got %d", config.MaxBytesPerSec)
	}

	if config.RetryDelay != defaultStreamRetryDelay {
		t.Errorf("expected RetryDelay %v, got %v", defaultStreamRetryDelay, config.RetryDelay)
	}

	if config.NumStreams != MinPoolSize {
		t.Errorf("expected NumStreams %d, got %d", MinPoolSize, config.NumStreams)
	}

	if config.MaxNumStreams != MaxPoolSize {
		t.Errorf("expected MaxNumStreams %d, got %d", MaxPoolSize, config.MaxNumStreams)
	}

	if !config.AdaptivePool {
		t.Error("expected AdaptivePool to be true by default")
	}

	if config.MaxConsecutiveFailures != defaultMaxConsecutiveFailures {
		t.Errorf("expected MaxConsecutiveFailures %d, got %d",
			defaultMaxConsecutiveFailures, config.MaxConsecutiveFailures)
	}

	if config.CircuitBreakerPause != defaultCircuitBreakerPause {
		t.Errorf("expected CircuitBreakerPause %v, got %v",
			defaultCircuitBreakerPause, config.CircuitBreakerPause)
	}

	if config.NumSenders != 4 {
		t.Errorf("NumSenders = %d, want 4", config.NumSenders)
	}
}

func TestBidiQueueStats_ZeroValue(t *testing.T) {
	stats := BidiQueueStats{}

	if stats.NumStreams != 0 {
		t.Errorf("expected zero NumStreams, got %d", stats.NumStreams)
	}

	if stats.InFlight != 0 {
		t.Errorf("expected zero InFlight, got %d", stats.InFlight)
	}

	if stats.BytesSent != 0 {
		t.Errorf("expected zero BytesSent, got %d", stats.BytesSent)
	}

	if stats.PiecesOK != 0 {
		t.Errorf("expected zero PiecesOK, got %d", stats.PiecesOK)
	}

	if stats.PiecesFail != 0 {
		t.Errorf("expected zero PiecesFail, got %d", stats.PiecesFail)
	}
}

func TestStreamingConstants(t *testing.T) {
	// Verify streaming constants are sensible
	if streamingRateLimiterBurst <= 0 {
		t.Errorf("streamingRateLimiterBurst should be positive, got %d", streamingRateLimiterBurst)
	}

	// Should be 1MB
	expectedBurst := 1024 * 1024
	if streamingRateLimiterBurst != expectedBurst {
		t.Errorf("streamingRateLimiterBurst should be %d (1MB), got %d",
			expectedBurst, streamingRateLimiterBurst)
	}

	if drainTimeout <= 0 {
		t.Errorf("drainTimeout should be positive, got %v", drainTimeout)
	}

	if reconnectBaseDelay <= 0 {
		t.Errorf("reconnectBaseDelay should be positive, got %v", reconnectBaseDelay)
	}

	if reconnectMaxDelay <= reconnectBaseDelay {
		t.Errorf("reconnectMaxDelay (%v) should be greater than reconnectBaseDelay (%v)",
			reconnectMaxDelay, reconnectBaseDelay)
	}

	if reconnectBackoffFactor <= 1 {
		t.Errorf("reconnectBackoffFactor should be greater than 1, got %d", reconnectBackoffFactor)
	}
}

// newClaimTestStream builds a PooledStream whose window holds exactly `window`
// slots, `filled` of them already occupied by another sender's pieces.
func newClaimTestStream(id, window, filled int) *PooledStream {
	w := congestion.NewAdaptiveWindow(congestion.Config{
		MinWindow:     window,
		MaxWindow:     window,
		InitialWindow: window,
		PieceTimeout:  time.Minute,
	})
	for i := range filled {
		w.OnSend(congestion.PieceKey{Hash: "occupant", Index: int32(i)})
	}
	return &PooledStream{window: w, id: id}
}

// TestClaimStream_SelectsAndClaimsUnderOneLock pins the contract that replaced
// the old select-then-TrySend pair: the returned stream already holds the
// caller's slot (which is what keeps drainAndRemoveStream off it), saturated and
// draining streams are never returned, and a pool with nothing to give reports
// errWindowFull instead of a stream the caller would immediately fail on.
func TestClaimStream_SelectsAndClaimsUnderOneLock(t *testing.T) {
	t.Parallel()

	key := congestion.PieceKey{Hash: "abc", Index: 7}

	t.Run("claims the slot on the stream it returns", func(t *testing.T) {
		t.Parallel()

		ps := newClaimTestStream(0, 4, 0)
		pool := &StreamPool{streams: []*PooledStream{ps}, logger: testLogger}

		got, err := pool.ClaimStream(key)
		if err != nil {
			t.Fatalf("ClaimStream: %v", err)
		}
		if got != ps {
			t.Fatalf("returned stream id %d, want 0", got.id)
		}
		if inFlight := ps.window.InFlight(); inFlight != 1 {
			t.Fatalf("in-flight after claim = %d, want 1 (the slot must already be taken)", inFlight)
		}
		if claimed := ps.window.ClearInflight(); len(claimed) != 1 || claimed[0] != key {
			t.Errorf("claimed keys = %v, want exactly [%v]", claimed, key)
		}
	})

	t.Run("prefers the least loaded stream that can send", func(t *testing.T) {
		t.Parallel()

		// Stream 0 is the least loaded by in-flight count but is saturated,
		// so the claim has to fall through to stream 2.
		saturated := newClaimTestStream(0, 1, 1)
		busy := newClaimTestStream(1, 8, 5)
		best := newClaimTestStream(2, 8, 3)
		pool := &StreamPool{streams: []*PooledStream{saturated, busy, best}, logger: testLogger}

		got, err := pool.ClaimStream(key)
		if err != nil {
			t.Fatalf("ClaimStream: %v", err)
		}
		if got != best {
			t.Fatalf("returned stream id %d, want 2", got.id)
		}
		if inFlight := saturated.window.InFlight(); inFlight != 1 {
			t.Errorf("saturated stream in-flight = %d, want 1 (untouched)", inFlight)
		}
	})

	t.Run("never claims a draining stream", func(t *testing.T) {
		t.Parallel()

		draining := newClaimTestStream(0, 8, 0)
		draining.draining.Store(true)
		live := newClaimTestStream(1, 8, 6)
		pool := &StreamPool{streams: []*PooledStream{draining, live}, logger: testLogger}

		got, err := pool.ClaimStream(key)
		if err != nil {
			t.Fatalf("ClaimStream: %v", err)
		}
		if got != live {
			t.Fatalf("returned stream id %d, want 1", got.id)
		}
		if inFlight := draining.window.InFlight(); inFlight != 0 {
			t.Errorf("draining stream in-flight = %d, want 0 (a claim would hold up its drain)", inFlight)
		}
	})

	t.Run("reports errWindowFull without claiming anything", func(t *testing.T) {
		t.Parallel()

		a := newClaimTestStream(0, 2, 2)
		b := newClaimTestStream(1, 3, 3)
		pool := &StreamPool{streams: []*PooledStream{a, b}, logger: testLogger}

		got, err := pool.ClaimStream(key)
		if !errors.Is(err, errWindowFull) {
			t.Fatalf("ClaimStream error = %v, want errWindowFull", err)
		}
		if got != nil {
			t.Errorf("returned stream id %d, want nil", got.id)
		}
		if a.window.InFlight() != 2 || b.window.InFlight() != 3 {
			t.Errorf("in-flight counts changed: %d, %d; want 2, 3",
				a.window.InFlight(), b.window.InFlight())
		}
	})
}

// TestScalingDecisionThresholds tests the threshold logic for scaling decisions.
func TestScalingDecisionThresholds(t *testing.T) {
	// Test that thresholds are applied correctly
	tests := []struct {
		name            string
		changeRatio     float64
		shouldScaleUp   bool
		shouldScaleDown bool
		shouldPlateau   bool
	}{
		{
			name:          "large increase triggers scale up",
			changeRatio:   0.10, // 10% > 5% threshold
			shouldScaleUp: true,
		},
		{
			name:          "small increase does not trigger scale up",
			changeRatio:   0.03, // 3% < 5% threshold
			shouldScaleUp: false,
		},
		{
			name:            "large decrease triggers scale down",
			changeRatio:     -0.20, // 20% > 15% threshold
			shouldScaleDown: true,
		},
		{
			name:            "small decrease does not trigger scale down",
			changeRatio:     -0.10, // 10% < 15% threshold
			shouldScaleDown: false,
		},
		{
			name:          "very small change is plateau",
			changeRatio:   0.02, // 2% < 3% threshold
			shouldPlateau: true,
		},
		{
			name:          "moderate change is not plateau",
			changeRatio:   0.04, // 4% > 3% threshold
			shouldPlateau: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scaleUp := tt.changeRatio > defaultScaleUpThreshold
			scaleDown := tt.changeRatio < -defaultScaleDownThreshold
			plateau := abs(tt.changeRatio) < defaultPlateauThreshold

			if scaleUp != tt.shouldScaleUp {
				t.Errorf("scale up: got %v, want %v (ratio: %v, threshold: %v)",
					scaleUp, tt.shouldScaleUp, tt.changeRatio, defaultScaleUpThreshold)
			}
			if scaleDown != tt.shouldScaleDown {
				t.Errorf("scale down: got %v, want %v (ratio: %v, threshold: %v)",
					scaleDown, tt.shouldScaleDown, tt.changeRatio, defaultScaleDownThreshold)
			}
			if plateau != tt.shouldPlateau {
				t.Errorf("plateau: got %v, want %v (ratio: %v, threshold: %v)",
					plateau, tt.shouldPlateau, tt.changeRatio, defaultPlateauThreshold)
			}
		})
	}
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

// TestCooldownPeriod tests that the cooldown period is reasonable.
func TestCooldownPeriod(t *testing.T) {
	// Cooldown should be long enough to stabilize but not too long
	if scalingCooldownPeriod < 30*time.Second {
		t.Errorf("scalingCooldownPeriod (%v) seems too short", scalingCooldownPeriod)
	}
	if scalingCooldownPeriod > 10*time.Minute {
		t.Errorf("scalingCooldownPeriod (%v) seems too long", scalingCooldownPeriod)
	}
}

// newScaleDownTestPool builds a pool holding n real-enough streams, so
// removeStreamLocked's background drain can Close() what it picks.
func newScaleDownTestPool(t *testing.T, n int) *StreamPool {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	streams := make([]*PooledStream, n)
	for i := range streams {
		streams[i] = newDrainTestStream(ctx, i)
	}
	return &StreamPool{
		ctx:     ctx,
		cancel:  cancel,
		streams: streams,
		logger:  testLogger,
		errs:    make(chan error, 10),
		acks:    make(chan AckEnvelope, 10),
	}
}

// TestRemoveStreamLocked_IgnoresDrainingStreams pins that a scale-down landing
// while an earlier drain is still in flight neither re-targets the departing
// stream nor lets the pool's usable width fall below MinPoolSize. A drain runs
// up to streamDrainTimeout, which is the same order as the cooldown between
// scale-downs, so the overlap is reachable.
func TestRemoveStreamLocked_IgnoresDrainingStreams(t *testing.T) {
	t.Parallel()

	t.Run("picks the last stream still carrying traffic", func(t *testing.T) {
		t.Parallel()

		pool := newScaleDownTestPool(t, MinPoolSize+2)

		last := len(pool.streams) - 1
		pool.streams[last].draining.Store(true) // Already on its way out.
		wantPick := pool.streams[last-1]

		pool.mu.Lock()
		err := pool.removeStreamLocked()
		pool.mu.Unlock()
		if err != nil {
			t.Fatalf("removeStreamLocked: %v", err)
		}

		if !wantPick.draining.Load() {
			t.Errorf("stream %d (last non-draining) was not picked for removal", wantPick.id)
		}
	})

	t.Run("refuses when only MinPoolSize streams still carry traffic", func(t *testing.T) {
		t.Parallel()

		// Length is MinPoolSize+1, but one of those is already leaving, so
		// removing another would drop the usable pool to MinPoolSize-1.
		pool := newScaleDownTestPool(t, MinPoolSize+1)

		pool.streams[len(pool.streams)-1].draining.Store(true)

		pool.mu.Lock()
		err := pool.removeStreamLocked()
		pool.mu.Unlock()

		if err == nil {
			t.Fatal("expected removeStreamLocked to refuse, got nil error")
		}
		for _, ps := range pool.streams[:len(pool.streams)-1] {
			if ps.draining.Load() {
				t.Errorf("stream %d was marked draining despite the refusal", ps.id)
			}
		}
	})
}

// TestDrainAndRemoveStream_RetiresOnce pins that a stream reaching
// drainAndRemoveStream twice - which connection-level scale-down does whenever
// one of the connection's streams is already being drained by a stream-level
// scale-down - is accounted exactly once. Double-counting its bytes into
// removedBytesSent reads as a throughput spike on the next measurement
// interval, which is the input every scaling decision compares against.
func TestDrainAndRemoveStream_RetiresOnce(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ps := newDrainTestStream(ctx, 1)
	ps.bytesSent.Store(4096)

	pool := newDrainTestPool(ctx, cancel, ps)

	pool.drainAndRemoveStream(ps)
	pool.drainAndRemoveStream(ps) // e.g. the connection this stream sits on is now going away too

	pool.mu.RLock()
	defer pool.mu.RUnlock()

	if len(pool.streams) != 0 {
		t.Fatalf("expected the stream removed, got %d streams", len(pool.streams))
	}
	if pool.removedBytesSent != 4096 {
		t.Errorf("removedBytesSent = %d, want 4096 (counted once)", pool.removedBytesSent)
	}
}
