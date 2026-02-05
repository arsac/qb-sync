package streaming

import (
	"testing"
	"time"

	"github.com/arsac/qb-sync/internal/congestion"
)

func TestStreamPoolConfig_Defaults(t *testing.T) {
	config := DefaultStreamPoolConfig()

	if config.NumStreams != MinPoolSize {
		t.Errorf("expected NumStreams %d, got %d", MinPoolSize, config.NumStreams)
	}

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

func TestPieceKey(t *testing.T) {
	tests := []struct {
		hash     string
		index    int32
		expected string
	}{
		{"abc123", 0, "abc123:0"},
		{"abc123", 1, "abc123:1"},
		{"abc123", 100, "abc123:100"},
		{"", 0, ":0"},
	}

	for _, tt := range tests {
		result := pieceKey(tt.hash, tt.index)
		if result != tt.expected {
			t.Errorf("pieceKey(%q, %d) = %q, want %q", tt.hash, tt.index, result, tt.expected)
		}
	}
}

func TestParsePieceKey(t *testing.T) {
	tests := []struct {
		key           string
		expectedHash  string
		expectedIndex int32
		expectedOK    bool
	}{
		{"abc123:0", "abc123", 0, true},
		{"abc123:1", "abc123", 1, true},
		{"abc123:100", "abc123", 100, true},
		{":0", "", 0, true},
		{"abc123", "", 0, false},     // No colon
		{"abc123:", "", 0, false},    // No index
		{"abc123:abc", "", 0, false}, // Invalid index
		{"", "", 0, false},           // Empty string
		{"a:b:c", "a", 0, false},     // Multiple colons (b:c is not a valid int)
	}

	for _, tt := range tests {
		hash, index, ok := ParsePieceKey(tt.key)
		if ok != tt.expectedOK {
			t.Errorf("ParsePieceKey(%q) ok = %v, want %v", tt.key, ok, tt.expectedOK)
			continue
		}
		if ok {
			if hash != tt.expectedHash {
				t.Errorf("ParsePieceKey(%q) hash = %q, want %q", tt.key, hash, tt.expectedHash)
			}
			if index != tt.expectedIndex {
				t.Errorf("ParsePieceKey(%q) index = %d, want %d", tt.key, index, tt.expectedIndex)
			}
		}
	}
}

func TestPieceKeyRoundTrip(t *testing.T) {
	// Test that pieceKey and ParsePieceKey are inverses
	testCases := []struct {
		hash  string
		index int32
	}{
		{"abc123def456", 0},
		{"abc123def456", 42},
		{"abc123def456", 12345},
		{"", 0},
	}

	for _, tc := range testCases {
		key := pieceKey(tc.hash, tc.index)
		hash, index, ok := ParsePieceKey(key)
		if !ok {
			t.Errorf("ParsePieceKey(pieceKey(%q, %d)) failed", tc.hash, tc.index)
			continue
		}
		if hash != tc.hash || index != tc.index {
			t.Errorf("Round trip failed: (%q, %d) -> %q -> (%q, %d)",
				tc.hash, tc.index, key, hash, index)
		}
	}
}

// TestFindLeastLoadedStreamLogic tests the selection logic conceptually.
// Since we can't easily create a pool without a GRPCDestination,
// we test the AdaptiveWindow behavior that underlies the selection.
func TestFindLeastLoadedStreamLogic(t *testing.T) {
	// Create two windows with different in-flight counts
	config := congestion.DefaultConfig()
	w1 := congestion.NewAdaptiveWindow(config)
	w2 := congestion.NewAdaptiveWindow(config)

	// w1 has 2 in-flight, w2 has 1 in-flight
	w1.OnSend("piece:0")
	w1.OnSend("piece:1")
	w2.OnSend("piece:2")

	// Selection should prefer w2 (lower in-flight)
	if w1.InFlight() <= w2.InFlight() {
		t.Errorf("Test setup wrong: w1.InFlight()=%d should be > w2.InFlight()=%d",
			w1.InFlight(), w2.InFlight())
	}

	// Verify CanSend works correctly
	// With default window of 10, both should be able to send
	if !w1.CanSend() {
		t.Error("w1 should be able to send")
	}
	if !w2.CanSend() {
		t.Error("w2 should be able to send")
	}
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
			changeRatio:   0.15, // 15% > 10% threshold
			shouldScaleUp: true,
		},
		{
			name:          "small increase does not trigger scale up",
			changeRatio:   0.05, // 5% < 10% threshold
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
			changeRatio:   0.02, // 2% < 5% threshold
			shouldPlateau: true,
		},
		{
			name:          "moderate change is not plateau",
			changeRatio:   0.08, // 8% > 5% threshold
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
