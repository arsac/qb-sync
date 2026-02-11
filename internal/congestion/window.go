package congestion

import (
	"math"
	"sync"
	"time"

	"github.com/arsac/qb-sync/internal/metrics"
)

const (
	DefaultMinWindow     = 8
	DefaultMaxWindow     = 2000
	DefaultInitialWindow = 64

	// CUBIC constants (RFC 8312).
	cubicC           = 0.4  // Scaling constant.
	cubicBeta        = 0.7  // Multiplicative decrease on loss (keep 70%).
	cubicBetaLastMax = 0.85 // Extra backoff when competing flows detected.
	maxBurstPieces   = 3    // Burst allowance for isCwndLimited check.

	// TCP-friendly Reno alpha: 3*(1-beta)/(1+beta).
	renoAlpha = 3.0 * (1.0 - cubicBeta) / (1.0 + cubicBeta)

	// EWMA smoothing factor (1/8 weight for new sample).
	rttSmoothingFactor = 8

	// DefaultPieceTimeout is the timeout for stale in-flight pieces.
	DefaultPieceTimeout = 60 * time.Second
)

// AdaptiveWindow implements CUBIC congestion control (RFC 8312) adapted for
// piece-level streaming. It adjusts the number of in-flight pieces based on
// loss events rather than RTT ratios, making it robust on high-jitter links.
type AdaptiveWindow struct {
	minRTT      time.Duration // Minimum observed RTT (propagation delay estimate).
	smoothedRTT time.Duration // Exponential moving average of RTT.
	window      int           // Current congestion window (max in-flight pieces).
	minWindow   int           // Floor for window size.
	maxWindow   int           // Ceiling for window size.

	// CUBIC state.
	slowStartThreshold int       // ssthresh; starts at maxWindow (enter slow start).
	lastMaxWindow      int       // W_max: window before last loss event.
	cubicEpoch         time.Time // Start of current CUBIC growth epoch.
	cubicOriginWindow  int       // Origin point of cubic function (W_max after beta).
	cubicK             float64   // Time to origin: cbrt(W_max * (1-beta) / C).
	estimatedTCPWindow float64   // TCP Reno fallback for TCP-friendly mode.

	// Sequence tracking for recovery deduplication.
	lastSendSeq          int64 // Monotonic send counter.
	largestAckedSeq      int64 // Highest acked sequence.
	largestSentAtCutback int64 // Sequence at last loss reduction.

	// Track in-flight pieces with send timestamps.
	inflight         map[string]time.Time
	originalSendTime map[string]time.Time // Tracks first send time for RTT accuracy.
	pieceSeq         map[string]int64     // Piece key -> send sequence.
	pieceTimeout     time.Duration
	mu               sync.Mutex

	// Stats.
	totalAcks int64

	// For testability; defaults to time.Now.
	nowFunc func() time.Time
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
		window:               config.InitialWindow,
		minWindow:            config.MinWindow,
		maxWindow:            config.MaxWindow,
		slowStartThreshold:   config.MaxWindow, // Start in slow start.
		pieceTimeout:         config.PieceTimeout,
		largestAckedSeq:      -1,
		largestSentAtCutback: -1,
		inflight:             make(map[string]time.Time),
		originalSendTime:     make(map[string]time.Time),
		pieceSeq:             make(map[string]int64),
		nowFunc:              time.Now,
	}
}

// now returns the current time, using the mock clock if set.
func (w *AdaptiveWindow) now() time.Time {
	return w.nowFunc()
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

	now := w.now()
	w.inflight[key] = now
	// Track original send time for accurate RTT on retries.
	if _, exists := w.originalSendTime[key]; !exists {
		w.originalSendTime[key] = now
	}

	// Assign monotonic sequence.
	w.lastSendSeq++
	w.pieceSeq[key] = w.lastSendSeq

	return true
}

// OnSend records that a piece was sent.
// Prefer TrySend for atomic check-and-send to avoid TOCTOU races.
func (w *AdaptiveWindow) OnSend(key string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	now := w.now()
	w.inflight[key] = now
	// Track original send time for accurate RTT on retries.
	if _, exists := w.originalSendTime[key]; !exists {
		w.originalSendTime[key] = now
	}

	// Assign monotonic sequence.
	w.lastSendSeq++
	w.pieceSeq[key] = w.lastSendSeq
}

// OnAck records that a piece was acknowledged and adjusts the window.
func (w *AdaptiveWindow) OnAck(key string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, ok := w.inflight[key]; !ok {
		return
	}

	priorInFlight := len(w.inflight)
	delete(w.inflight, key)

	// Update largest acked sequence.
	if seq, ok := w.pieceSeq[key]; ok {
		if seq > w.largestAckedSeq {
			w.largestAckedSeq = seq
		}
		delete(w.pieceSeq, key)
	}

	// Use original send time for accurate RTT (handles retries correctly).
	sendTime, hasOriginal := w.originalSendTime[key]
	delete(w.originalSendTime, key)

	if !hasOriginal {
		return
	}

	rtt := w.now().Sub(sendTime)
	w.totalAcks++

	// Record RTT to Prometheus histogram.
	metrics.PieceRTTSeconds.Observe(rtt.Seconds())

	w.updateMinRTT(rtt)
	w.updateSmoothedRTT(rtt)

	// No growth during recovery.
	if w.inRecovery() {
		return
	}

	// No growth if not cwnd-limited (application limited).
	if !w.isCwndLimited(priorInFlight) {
		w.onApplicationLimited()
		return
	}

	// Slow start: exponential growth (+1 per ACK).
	if w.inSlowStart() {
		if w.window < w.maxWindow {
			w.window++
		}
		return
	}

	// Congestion avoidance: CUBIC growth.
	target := min(w.cubicCongestionWindowAfterAck(), w.maxWindow)
	if target > w.window {
		w.window = target
	}
}

// inRecovery returns true if we're still recovering from a loss event.
// During recovery, we don't grow the window.
func (w *AdaptiveWindow) inRecovery() bool {
	return w.largestSentAtCutback >= 0 &&
		w.largestAckedSeq <= w.largestSentAtCutback
}

// inSlowStart returns true if the window is below the slow start threshold.
func (w *AdaptiveWindow) inSlowStart() bool {
	return w.window < w.slowStartThreshold
}

// isCwndLimited returns true if the sender is actually using the congestion window.
// We only grow the window when it's the bottleneck.
func (w *AdaptiveWindow) isCwndLimited(priorInFlight int) bool {
	if priorInFlight >= w.window {
		return true
	}
	available := w.window - priorInFlight
	// In slow start, consider limited if using more than half.
	if w.inSlowStart() && priorInFlight > w.window/2 {
		return true
	}
	return available <= maxBurstPieces
}

// onApplicationLimited resets the CUBIC epoch when the sender is idle.
// This prevents the cubic function from jumping ahead after an idle period.
func (w *AdaptiveWindow) onApplicationLimited() {
	w.cubicEpoch = time.Time{}
}

// cubicCongestionWindowAfterAck computes the CUBIC target window.
// Implements W(t) = C*(t-K)^3 + W_max with TCP-friendly Reno fallback.
func (w *AdaptiveWindow) cubicCongestionWindowAfterAck() int {
	now := w.now()

	// Initialize epoch on first call after loss or app-limited reset.
	if w.cubicEpoch.IsZero() {
		w.cubicEpoch = now
		if w.window < w.lastMaxWindow {
			// Concave region: growing back toward W_max.
			w.cubicK = math.Cbrt(float64(w.lastMaxWindow-w.window) / cubicC)
			w.cubicOriginWindow = w.lastMaxWindow
		} else {
			// Convex region: already past W_max.
			w.cubicK = 0
			w.cubicOriginWindow = w.window
		}
		w.estimatedTCPWindow = float64(w.window)
	}

	// Time since epoch in seconds.
	t := now.Sub(w.cubicEpoch).Seconds()

	// Shift by minRTT to compensate for propagation delay.
	if w.minRTT > 0 {
		t += w.minRTT.Seconds()
	}

	// CUBIC: W(t) = C*(t-K)^3 + W_max.
	dt := t - w.cubicK
	cubicWindow := cubicC*dt*dt*dt + float64(w.cubicOriginWindow)

	// TCP-friendly mode: Reno linear growth as fallback.
	// W_tcp(t) += alpha * acked / W_tcp (approximately +alpha/W per ACK).
	w.estimatedTCPWindow += renoAlpha / w.estimatedTCPWindow

	// Use the larger of CUBIC and Reno (TCP-friendly requirement).
	target := cubicWindow
	if w.estimatedTCPWindow > target {
		target = w.estimatedTCPWindow
	}

	return int(math.Ceil(target))
}

// OnFail records that a piece failed (timeout or error).
// Implements CUBIC multiplicative decrease with recovery deduplication.
func (w *AdaptiveWindow) OnFail(key string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Only reduce window if the piece was actually in-flight.
	if _, ok := w.inflight[key]; !ok {
		return
	}
	delete(w.inflight, key)
	delete(w.originalSendTime, key)

	// Recovery deduplication: if this piece was sent before the last cutback,
	// it belongs to the same loss event — skip reduction.
	seq, hasSeq := w.pieceSeq[key]
	delete(w.pieceSeq, key)
	if hasSeq && w.largestSentAtCutback >= 0 && seq <= w.largestSentAtCutback {
		return
	}

	// Competing flow detection: if we haven't reached W_max, another flow
	// may be sharing the bottleneck. Use more conservative backoff.
	if w.lastMaxWindow > 0 && w.window+1 < w.lastMaxWindow {
		w.lastMaxWindow = int(float64(w.window) * cubicBetaLastMax)
	} else {
		w.lastMaxWindow = w.window
	}

	// Multiplicative decrease.
	newWindow := max(int(float64(w.window)*cubicBeta), w.minWindow)
	w.window = newWindow

	// Enter recovery.
	w.slowStartThreshold = w.window
	w.largestSentAtCutback = w.lastSendSeq
	w.cubicEpoch = time.Time{} // Reset epoch for next growth phase.
}

// updateMinRTT updates the minimum RTT baseline.
// CUBIC uses minRTT only for epoch origin shifting — no expiry needed.
func (w *AdaptiveWindow) updateMinRTT(rtt time.Duration) {
	if w.minRTT == 0 || rtt < w.minRTT {
		w.minRTT = rtt
	}
}

// updateSmoothedRTT updates the EWMA of RTT.
func (w *AdaptiveWindow) updateSmoothedRTT(rtt time.Duration) {
	if w.smoothedRTT == 0 {
		w.smoothedRTT = rtt
	} else {
		w.smoothedRTT = (w.smoothedRTT*(rttSmoothingFactor-1) + rtt) / rttSmoothingFactor
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
	w.pieceSeq = make(map[string]int64)
	return keys
}

// GetStaleKeys returns keys that have been in-flight longer than the piece timeout.
// These should be marked as failed and retried.
func (w *AdaptiveWindow) GetStaleKeys() []string {
	w.mu.Lock()
	defer w.mu.Unlock()

	now := w.now()
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
	InSlowStart bool // Whether currently in slow start phase.
	InRecovery  bool // Whether currently in loss recovery.
	SSThresh    int  // Slow start threshold.
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
		InSlowStart: w.inSlowStart(),
		InRecovery:  w.inRecovery(),
		SSThresh:    w.slowStartThreshold,
	}
}
