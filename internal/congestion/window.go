package congestion

import (
	"sync"
	"time"

	"github.com/arsac/qb-sync/internal/metrics"
)

const (
	DefaultMinWindow     = 4
	DefaultMaxWindow     = 500
	DefaultInitialWindow = 16

	// RTT thresholds for window adjustment (optimized for stable WireGuard links).
	rttIncreaseThreshold = 1.25 // RTT can be 25% above min before we hold steady.
	rttDecreaseThreshold = 1.5  // RTT 1.5x min triggers window reduction (react earlier to queuing).

	// Window adjustment factors.
	windowIncreaseStep   = 1    // Additive increase.
	windowDecreaseFactor = 0.75 // Multiplicative decrease (75% of current).
	windowFailFactor     = 0.5  // Aggressive decrease on failure (50% of current).

	// EWMA smoothing factor (1/8 weight for new sample).
	rttSmoothingFactor = 8

	// minRTT expiry - reset periodically to adapt to route changes.
	// 30s is appropriate for stable WireGuard links.
	minRTTExpiry = 30 * time.Second

	// Maximum RTT ratio allowed when resetting minRTT after expiry.
	// Prevents accepting a congested RTT as the new baseline.
	minRTTResetThreshold = 1.5

	// Force-reset minRTT after this duration regardless of threshold.
	// 10 minutes avoids accepting congested RTT as baseline during sustained issues.
	minRTTForceResetExpiry = 10 * time.Minute

	// Minimum number of RTT samples before adjusting window.
	// Prevents anomalous first samples from affecting behavior.
	minRTTSamples = 3

	// DefaultPieceTimeout is the timeout for stale in-flight pieces.
	DefaultPieceTimeout = 60 * time.Second
)

// AdaptiveWindow implements delay-based congestion control similar to TCP Vegas.
// It adjusts the number of in-flight pieces based on measured round-trip times
// to maximize throughput without saturating the network link.
type AdaptiveWindow struct {
	minRTT      time.Duration // Minimum observed RTT (approximates propagation delay)
	minRTTTime  time.Time     // When minRTT was last updated
	smoothedRTT time.Duration // Exponential moving average of RTT
	rttSamples  int           // Number of RTT samples collected
	window      int           // Current window size (max in-flight pieces)
	minWindow   int           // Floor for window size
	maxWindow   int           // Ceiling for window size

	// Track in-flight pieces with send timestamps.
	// Uses originalSendTime to handle retries correctly.
	inflight         map[string]time.Time
	originalSendTime map[string]time.Time // Tracks first send time for RTT accuracy
	pieceTimeout     time.Duration
	mu               sync.Mutex

	// Stats.
	totalAcks    int64
	windowAdjust int64 // Positive = increases, negative = decreases.
}

// Config configures the adaptive window.
type Config struct {
	MinWindow     int           // Minimum window size (default: 4)
	MaxWindow     int           // Maximum window size (default: 500)
	InitialWindow int           // Starting window size (default: 16)
	PieceTimeout  time.Duration // Timeout for stale in-flight pieces (default: 60s)
}

// DefaultConfig returns sensible defaults.
func DefaultConfig() Config {
	return Config{
		MinWindow:     DefaultMinWindow,
		MaxWindow:     DefaultMaxWindow,
		InitialWindow: DefaultInitialWindow,
		PieceTimeout:  DefaultPieceTimeout,
	}
}

// NewAdaptiveWindow creates a new adaptive congestion window.
func NewAdaptiveWindow(config Config) *AdaptiveWindow {
	if config.MinWindow <= 0 {
		config.MinWindow = DefaultMinWindow
	}
	if config.MaxWindow <= 0 {
		config.MaxWindow = DefaultMaxWindow
	}
	if config.InitialWindow <= 0 {
		config.InitialWindow = DefaultInitialWindow
	}
	if config.PieceTimeout <= 0 {
		config.PieceTimeout = DefaultPieceTimeout
	}

	// Validate config constraints.
	if config.MinWindow > config.MaxWindow {
		config.MinWindow = config.MaxWindow
	}
	if config.InitialWindow < config.MinWindow {
		config.InitialWindow = config.MinWindow
	}
	if config.InitialWindow > config.MaxWindow {
		config.InitialWindow = config.MaxWindow
	}

	return &AdaptiveWindow{
		window:           config.InitialWindow,
		minWindow:        config.MinWindow,
		maxWindow:        config.MaxWindow,
		pieceTimeout:     config.PieceTimeout,
		inflight:         make(map[string]time.Time),
		originalSendTime: make(map[string]time.Time),
	}
}

// Window returns the current window size.
func (w *AdaptiveWindow) Window() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.window
}

// InFlight returns the number of pieces currently in flight.
func (w *AdaptiveWindow) InFlight() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.inflight)
}

// CanSend returns true if we can send another piece without exceeding the window.
func (w *AdaptiveWindow) CanSend() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.inflight) < w.window
}

// TrySend atomically checks if we can send and records the piece if so.
// Returns true if the piece was recorded, false if window is full.
// This avoids the TOCTOU race between CanSend and OnSend.
func (w *AdaptiveWindow) TrySend(key string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.inflight) >= w.window {
		return false
	}

	now := time.Now()
	w.inflight[key] = now
	// Track original send time for accurate RTT on retries.
	if _, exists := w.originalSendTime[key]; !exists {
		w.originalSendTime[key] = now
	}
	return true
}

// OnSend records that a piece was sent.
// Prefer TrySend for atomic check-and-send to avoid TOCTOU races.
func (w *AdaptiveWindow) OnSend(key string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	now := time.Now()
	w.inflight[key] = now
	// Track original send time for accurate RTT on retries.
	if _, exists := w.originalSendTime[key]; !exists {
		w.originalSendTime[key] = now
	}
}

// OnAck records that a piece was acknowledged and adjusts the window.
func (w *AdaptiveWindow) OnAck(key string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, ok := w.inflight[key]; !ok {
		return
	}
	delete(w.inflight, key)

	// Use original send time for accurate RTT (handles retries correctly).
	sendTime, hasOriginal := w.originalSendTime[key]
	delete(w.originalSendTime, key)

	if !hasOriginal {
		// Shouldn't happen, but handle gracefully.
		return
	}

	rtt := time.Since(sendTime)
	w.totalAcks++
	w.rttSamples++

	// Record RTT to Prometheus histogram
	metrics.PieceRTTSeconds.Observe(rtt.Seconds())

	w.updateMinRTT(rtt)
	w.updateSmoothedRTT(rtt)

	if w.rttSamples >= minRTTSamples {
		w.adjustWindow()
	}
}

// updateMinRTT updates the minimum RTT baseline with expiry logic.
// Must be called with lock held.
func (w *AdaptiveWindow) updateMinRTT(rtt time.Duration) {
	now := time.Now()
	timeSinceUpdate := now.Sub(w.minRTTTime)
	expired := timeSinceUpdate > minRTTExpiry
	forceExpired := timeSinceUpdate > minRTTForceResetExpiry
	withinThreshold := w.minRTT == 0 || float64(rtt) < float64(w.minRTT)*minRTTResetThreshold

	// Accept new minRTT based on conditions (evaluated in priority order).
	switch {
	case w.minRTT == 0 || rtt < w.minRTT:
		// First measurement or new lower minimum found.
		w.minRTT = rtt
		w.minRTTTime = now

	case forceExpired && (w.smoothedRTT == 0 || rtt < w.smoothedRTT*2):
		// Force-reset expiry: accept if RTT is not severely congested.
		// Use smoothedRTT as sanity check - new minRTT shouldn't be > 2x smoothed.
		w.minRTT = rtt
		w.minRTTTime = now

	case expired && withinThreshold:
		// Normal expiry: accept if within threshold (prevents locking in congested RTT).
		w.minRTT = rtt
		w.minRTTTime = now
	}
}

// updateSmoothedRTT updates the EWMA of RTT.
// Must be called with lock held.
func (w *AdaptiveWindow) updateSmoothedRTT(rtt time.Duration) {
	if w.smoothedRTT == 0 {
		w.smoothedRTT = rtt
	} else {
		w.smoothedRTT = (w.smoothedRTT*(rttSmoothingFactor-1) + rtt) / rttSmoothingFactor
	}
}

// OnFail records that a piece failed (timeout or error).
// This triggers a more aggressive window reduction only if the piece was in-flight.
func (w *AdaptiveWindow) OnFail(key string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Only reduce window if the piece was actually in-flight.
	// This prevents double-reduction if OnFail is called multiple times
	// or for pieces that were already acked/failed.
	if _, ok := w.inflight[key]; !ok {
		return
	}
	delete(w.inflight, key)
	delete(w.originalSendTime, key)

	// On failure, reduce window more aggressively.
	newWindow := max(int(float64(w.window)*windowFailFactor), w.minWindow)
	if newWindow != w.window {
		w.windowAdjust--
		w.window = newWindow
	}
}

// adjustWindow modifies the window based on current RTT measurements.
// Must be called with lock held.
func (w *AdaptiveWindow) adjustWindow() {
	if w.minRTT == 0 {
		return
	}

	rttRatio := float64(w.smoothedRTT) / float64(w.minRTT)

	switch {
	case rttRatio < rttIncreaseThreshold:
		// No queuing detected, safe to increase.
		newWindow := w.window + windowIncreaseStep
		if newWindow <= w.maxWindow {
			w.window = newWindow
			w.windowAdjust++
		}

	case rttRatio > rttDecreaseThreshold:
		// Queuing detected, reduce window.
		newWindow := max(int(float64(w.window)*windowDecreaseFactor), w.minWindow)
		if newWindow != w.window {
			w.window = newWindow
			w.windowAdjust--
		}
	}
}

// ClearInflight removes all in-flight tracking and returns the keys.
func (w *AdaptiveWindow) ClearInflight() []string {
	w.mu.Lock()
	defer w.mu.Unlock()

	keys := make([]string, 0, len(w.inflight))
	for key := range w.inflight {
		keys = append(keys, key)
	}
	w.inflight = make(map[string]time.Time)
	w.originalSendTime = make(map[string]time.Time)
	return keys
}

// GetStaleKeys returns keys that have been in-flight longer than the piece timeout.
// These should be marked as failed and retried.
func (w *AdaptiveWindow) GetStaleKeys() []string {
	w.mu.Lock()
	defer w.mu.Unlock()

	now := time.Now()
	var stale []string
	for key, sendTime := range w.inflight {
		if now.Sub(sendTime) > w.pieceTimeout {
			stale = append(stale, key)
		}
	}
	return stale
}

// Stats contains current window statistics.
type Stats struct {
	Window      int
	InFlight    int
	MinRTT      time.Duration
	SmoothedRTT time.Duration
	TotalAcks   int64
}

// Stats returns current window statistics.
func (w *AdaptiveWindow) Stats() Stats {
	w.mu.Lock()
	defer w.mu.Unlock()

	return Stats{
		Window:      w.window,
		InFlight:    len(w.inflight),
		MinRTT:      w.minRTT,
		SmoothedRTT: w.smoothedRTT,
		TotalAcks:   w.totalAcks,
	}
}
