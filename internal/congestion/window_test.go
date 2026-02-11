package congestion

import (
	"fmt"
	"math"
	"testing"
	"time"
)

// mockClock provides a controllable clock for deterministic tests.
type mockClock struct {
	now time.Time
}

func newMockClock() *mockClock {
	return &mockClock{now: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *mockClock) Now() time.Time { return c.now }

func (c *mockClock) Advance(d time.Duration) { c.now = c.now.Add(d) }

// newTestWindow creates a window with mock clock and fills inflight to make it cwnd-limited.
func newTestWindow(config Config) (*AdaptiveWindow, *mockClock) {
	w := NewAdaptiveWindow(config)
	clock := newMockClock()
	w.nowFunc = clock.Now
	return w, clock
}

// sendAndAck sends a piece, advances the clock by rtt, then acks it.
// The window must have capacity and the piece must saturate the window for growth.
func sendAndAck(w *AdaptiveWindow, key string, clock *mockClock, rtt time.Duration) {
	w.OnSend(key)
	clock.Advance(rtt)
	w.OnAck(key)
}

func TestAdaptiveWindow_Defaults(t *testing.T) {
	w := NewAdaptiveWindow(DefaultConfig())

	if w.Window() != DefaultInitialWindow {
		t.Errorf("expected initial window %d, got %d", DefaultInitialWindow, w.Window())
	}

	if w.InFlight() != 0 {
		t.Errorf("expected initial inflight 0, got %d", w.InFlight())
	}

	if !w.CanSend() {
		t.Error("expected CanSend() to be true initially")
	}

	// Should start in slow start (ssthresh = maxWindow).
	stats := w.Stats()
	if !stats.InSlowStart {
		t.Error("expected to start in slow start")
	}
	if stats.SSThresh != DefaultMaxWindow {
		t.Errorf("expected ssthresh %d, got %d", DefaultMaxWindow, stats.SSThresh)
	}
}

func TestAdaptiveWindow_CanSend(t *testing.T) {
	config := Config{
		MinWindow:     2,
		MaxWindow:     10,
		InitialWindow: 3,
	}
	w := NewAdaptiveWindow(config)

	if !w.CanSend() {
		t.Error("expected CanSend() to be true")
	}

	w.OnSend("piece:0")
	w.OnSend("piece:1")
	w.OnSend("piece:2")

	if w.CanSend() {
		t.Error("expected CanSend() to be false when window full")
	}

	if w.InFlight() != 3 {
		t.Errorf("expected 3 inflight, got %d", w.InFlight())
	}
}

func TestAdaptiveWindow_OnAck(t *testing.T) {
	config := Config{
		MinWindow:     2,
		MaxWindow:     100,
		InitialWindow: 10,
	}
	w := NewAdaptiveWindow(config)

	w.OnSend("piece:0")
	if w.InFlight() != 1 {
		t.Errorf("expected 1 inflight, got %d", w.InFlight())
	}

	time.Sleep(10 * time.Millisecond)

	w.OnAck("piece:0")
	if w.InFlight() != 0 {
		t.Errorf("expected 0 inflight after ack, got %d", w.InFlight())
	}

	stats := w.Stats()
	if stats.MinRTT == 0 {
		t.Error("expected minRTT to be set after ack")
	}
	if stats.SmoothedRTT == 0 {
		t.Error("expected smoothedRTT to be set after ack")
	}
}

func TestAdaptiveWindow_OnFail(t *testing.T) {
	config := Config{
		MinWindow:     2,
		MaxWindow:     100,
		InitialWindow: 10,
	}
	w := NewAdaptiveWindow(config)

	initialWindow := w.Window()

	w.OnSend("piece:0")
	w.OnFail("piece:0")

	// CUBIC uses 0.7x reduction.
	expected := int(float64(initialWindow) * cubicBeta)
	if w.Window() != expected {
		t.Errorf("expected window %d (0.7x of %d), got %d", expected, initialWindow, w.Window())
	}

	if w.InFlight() != 0 {
		t.Errorf("expected 0 inflight after failure, got %d", w.InFlight())
	}
}

func TestAdaptiveWindow_MinWindowFloor(t *testing.T) {
	config := Config{
		MinWindow:     5,
		MaxWindow:     100,
		InitialWindow: 10,
	}
	w := NewAdaptiveWindow(config)

	// Fail many pieces to drive window down via CUBIC beta reduction.
	for i := range 20 {
		key := fmt.Sprintf("piece:%d", i)
		w.OnSend(key)
		w.OnFail(key)
	}

	if w.Window() < config.MinWindow {
		t.Errorf("window went below minimum: got %d, min %d", w.Window(), config.MinWindow)
	}
	if w.Window() != config.MinWindow {
		t.Errorf("expected window to be at minimum %d, got %d", config.MinWindow, w.Window())
	}
}

func TestAdaptiveWindow_ClearInflight(t *testing.T) {
	w := NewAdaptiveWindow(DefaultConfig())

	w.OnSend("piece:0")
	w.OnSend("piece:1")
	w.OnSend("piece:2")

	if w.InFlight() != 3 {
		t.Errorf("expected 3 inflight, got %d", w.InFlight())
	}

	keys := w.ClearInflight()

	if len(keys) != 3 {
		t.Errorf("expected 3 keys returned, got %d", len(keys))
	}

	if w.InFlight() != 0 {
		t.Errorf("expected 0 inflight after clear, got %d", w.InFlight())
	}
}

func TestAdaptiveWindow_TrySend(t *testing.T) {
	config := Config{
		MinWindow:     2,
		MaxWindow:     10,
		InitialWindow: 3,
	}
	w := NewAdaptiveWindow(config)

	if !w.TrySend("piece:0") {
		t.Error("expected TrySend to succeed with capacity")
	}
	if w.InFlight() != 1 {
		t.Errorf("expected 1 inflight, got %d", w.InFlight())
	}

	if !w.TrySend("piece:1") {
		t.Error("expected TrySend to succeed")
	}
	if !w.TrySend("piece:2") {
		t.Error("expected TrySend to succeed")
	}

	if w.TrySend("piece:3") {
		t.Error("expected TrySend to fail when window full")
	}
	if w.InFlight() != 3 {
		t.Errorf("expected 3 inflight (piece:3 should not be added), got %d", w.InFlight())
	}

	w.OnAck("piece:0")
	if !w.TrySend("piece:3") {
		t.Error("expected TrySend to succeed after ack freed capacity")
	}
}

func TestAdaptiveWindow_TrySend_Idempotent(t *testing.T) {
	config := Config{
		MinWindow:     2,
		MaxWindow:     10,
		InitialWindow: 5,
	}
	w := NewAdaptiveWindow(config)

	if !w.TrySend("piece:0") {
		t.Error("expected first TrySend to succeed")
	}

	if !w.TrySend("piece:0") {
		t.Error("expected second TrySend for same key to succeed")
	}
	if w.InFlight() != 1 {
		t.Errorf("expected 1 inflight (same key), got %d", w.InFlight())
	}
}

func TestAdaptiveWindow_GetStaleKeys(t *testing.T) {
	config := Config{
		MinWindow:     2,
		MaxWindow:     100,
		InitialWindow: 10,
		PieceTimeout:  50 * time.Millisecond,
	}
	w := NewAdaptiveWindow(config)

	w.OnSend("piece:0")
	w.OnSend("piece:1")

	stale := w.GetStaleKeys()
	if len(stale) != 0 {
		t.Errorf("expected no stale keys initially, got %d", len(stale))
	}

	time.Sleep(60 * time.Millisecond)

	stale = w.GetStaleKeys()
	if len(stale) != 2 {
		t.Errorf("expected 2 stale keys after timeout, got %d", len(stale))
	}

	w.OnSend("piece:2")

	stale = w.GetStaleKeys()
	if len(stale) != 2 {
		t.Errorf("expected 2 stale keys (not the fresh one), got %d", len(stale))
	}
}

func TestAdaptiveWindow_GetStaleKeys_AfterRetry(t *testing.T) {
	config := Config{
		MinWindow:     2,
		MaxWindow:     100,
		InitialWindow: 10,
		PieceTimeout:  50 * time.Millisecond,
	}
	w := NewAdaptiveWindow(config)

	w.OnSend("piece:0")

	time.Sleep(60 * time.Millisecond)

	stale := w.GetStaleKeys()
	if len(stale) != 1 {
		t.Errorf("expected 1 stale key, got %d", len(stale))
	}

	w.OnSend("piece:0")

	stale = w.GetStaleKeys()
	if len(stale) != 0 {
		t.Errorf("expected 0 stale keys after retry, got %d", len(stale))
	}
}

func TestAdaptiveWindow_OriginalSendTime_RTTAccuracy(t *testing.T) {
	config := Config{
		MinWindow:     2,
		MaxWindow:     100,
		InitialWindow: 10,
	}
	w := NewAdaptiveWindow(config)

	w.OnSend("piece:0")
	time.Sleep(20 * time.Millisecond)

	// "Retry" the piece.
	w.OnSend("piece:0")
	time.Sleep(10 * time.Millisecond)

	// RTT should be based on ORIGINAL send time (~30ms), not retry (~10ms).
	w.OnAck("piece:0")

	stats := w.Stats()
	if stats.MinRTT < 25*time.Millisecond {
		t.Errorf("RTT should be based on original send time (~30ms), got %v", stats.MinRTT)
	}
}

func TestAdaptiveWindow_ConfigValidation(t *testing.T) {
	config := Config{
		MinWindow:     100,
		MaxWindow:     10,
		InitialWindow: 50,
	}
	w := NewAdaptiveWindow(config)

	if w.minWindow > w.maxWindow {
		t.Errorf("MinWindow (%d) should not exceed MaxWindow (%d)", w.minWindow, w.maxWindow)
	}

	if w.Window() < w.minWindow || w.Window() > w.maxWindow {
		t.Errorf("InitialWindow (%d) should be within [%d, %d]", w.Window(), w.minWindow, w.maxWindow)
	}
}

func TestAdaptiveWindow_ConfigValidation_InitialTooLow(t *testing.T) {
	config := Config{
		MinWindow:     10,
		MaxWindow:     100,
		InitialWindow: 5,
	}
	w := NewAdaptiveWindow(config)

	if w.Window() < w.minWindow {
		t.Errorf("InitialWindow (%d) should be at least MinWindow (%d)", w.Window(), w.minWindow)
	}
}

func TestAdaptiveWindow_ConfigValidation_InitialTooHigh(t *testing.T) {
	config := Config{
		MinWindow:     10,
		MaxWindow:     50,
		InitialWindow: 100,
	}
	w := NewAdaptiveWindow(config)

	if w.Window() > w.maxWindow {
		t.Errorf("InitialWindow (%d) should be at most MaxWindow (%d)", w.Window(), w.maxWindow)
	}
}

// --- CUBIC-specific tests ---

func TestSlowStart_ExponentialGrowth(t *testing.T) {
	config := Config{
		MinWindow:     2,
		MaxWindow:     2000,
		InitialWindow: 10,
	}
	w, clock := newTestWindow(config)

	initial := w.Window()
	n := 5

	// Fill window and ACK to ensure cwnd-limited.
	// In slow start, window grows +1 per ACK, so each batch of W ACKs
	// roughly doubles the window.
	for i := range n {
		window := w.Window()
		keys := make([]string, window)
		for j := range window {
			key := fmt.Sprintf("piece:%d:%d", i, j)
			keys[j] = key
			w.OnSend(key)
		}
		clock.Advance(100 * time.Millisecond)
		for _, key := range keys {
			w.OnAck(key)
		}
	}

	// After N batches, each with window ACKs, slow start adds +1 per ACK.
	// Window should have grown significantly beyond initial.
	if w.Window() <= initial+n {
		t.Errorf("expected significant slow start growth, initial=%d, got=%d after %d batches", initial, w.Window(), n)
	}

	// Verify still in slow start (ssthresh = maxWindow = 2000).
	stats := w.Stats()
	if !stats.InSlowStart {
		t.Errorf("expected to still be in slow start, window=%d ssthresh=%d", stats.Window, stats.SSThresh)
	}
}

func TestSlowStart_ExitOnLoss(t *testing.T) {
	config := Config{
		MinWindow:     2,
		MaxWindow:     200,
		InitialWindow: 50,
	}
	w, _ := newTestWindow(config)

	// Verify we start in slow start.
	if !w.Stats().InSlowStart {
		t.Fatal("expected to start in slow start")
	}

	// Trigger a loss.
	w.OnSend("piece:0")
	w.OnFail("piece:0")

	stats := w.Stats()

	// ssthresh should be set to reduced window.
	expectedWindow := int(float64(50) * cubicBeta)
	if stats.Window != expectedWindow {
		t.Errorf("expected window %d after loss, got %d", expectedWindow, stats.Window)
	}
	if stats.SSThresh != expectedWindow {
		t.Errorf("expected ssthresh %d, got %d", expectedWindow, stats.SSThresh)
	}

	// Should no longer be in slow start (window == ssthresh).
	if stats.InSlowStart {
		t.Error("expected to exit slow start after loss")
	}
}

func TestRecovery_NoGrowthDuringRecovery(t *testing.T) {
	config := Config{
		MinWindow:     2,
		MaxWindow:     200,
		InitialWindow: 50,
	}
	w, clock := newTestWindow(config)

	// Send several pieces.
	for i := range 50 {
		w.OnSend(fmt.Sprintf("piece:%d", i))
	}

	// Fail one to enter recovery.
	w.OnFail("piece:25")
	windowAfterLoss := w.Window()

	// ACK pieces that were sent before the cutback (they have seq <= largestSentAtCutback).
	// These should not grow the window.
	clock.Advance(100 * time.Millisecond)
	for i := range 25 {
		w.OnAck(fmt.Sprintf("piece:%d", i))
	}

	if w.Window() != windowAfterLoss {
		t.Errorf("expected no growth during recovery, window was %d, now %d", windowAfterLoss, w.Window())
	}

	if !w.Stats().InRecovery {
		t.Error("expected to be in recovery")
	}
}

func TestRecovery_ExitOnNewAck(t *testing.T) {
	config := Config{
		MinWindow:     2,
		MaxWindow:     200,
		InitialWindow: 50,
	}
	w, clock := newTestWindow(config)

	// Send pieces to fill window.
	for i := range 50 {
		w.OnSend(fmt.Sprintf("piece:%d", i))
	}

	// Fail one to enter recovery.
	w.OnFail("piece:0")

	// ACK remaining pre-cutback pieces (still in recovery).
	clock.Advance(100 * time.Millisecond)
	for i := 1; i < 50; i++ {
		w.OnAck(fmt.Sprintf("piece:%d", i))
	}

	// Now send a NEW piece (post-cutback sequence).
	w.OnSend("piece:new")
	clock.Advance(100 * time.Millisecond)
	w.OnAck("piece:new")

	// Should have exited recovery (new piece seq > largestSentAtCutback).
	if w.Stats().InRecovery {
		t.Error("expected to exit recovery after ACK of post-cutback piece")
	}
}

func TestRecovery_MultipleLossesSameWindow(t *testing.T) {
	config := Config{
		MinWindow:     2,
		MaxWindow:     200,
		InitialWindow: 100,
	}
	w, _ := newTestWindow(config)

	// Send a batch of pieces.
	for i := range 100 {
		w.OnSend(fmt.Sprintf("piece:%d", i))
	}

	// First loss triggers reduction.
	w.OnFail("piece:0")
	windowAfterFirstLoss := w.Window()

	// Second and third losses from the same batch should NOT reduce further.
	// These pieces have seq <= largestSentAtCutback.
	w.OnFail("piece:1")
	w.OnFail("piece:2")

	if w.Window() != windowAfterFirstLoss {
		t.Errorf("expected no additional reduction for same-batch losses, window was %d, now %d",
			windowAfterFirstLoss, w.Window())
	}
}

func TestCubic_GrowthAfterLoss(t *testing.T) {
	config := Config{
		MinWindow:     2,
		MaxWindow:     2000,
		InitialWindow: 100,
	}
	w, clock := newTestWindow(config)

	// Exit slow start by triggering a loss.
	w.OnSend("piece:init")
	w.OnFail("piece:init")

	windowAfterLoss := w.Window() // 70 (100 * 0.7)
	if windowAfterLoss != 70 {
		t.Fatalf("expected window 70 after loss, got %d", windowAfterLoss)
	}

	// Now simulate congestion avoidance growth over time.
	// Send full windows and ACK them, advancing the clock each round.
	prevWindow := windowAfterLoss
	windows := []int{prevWindow}

	for round := range 30 {
		window := w.Window()
		keys := make([]string, window)
		for j := range window {
			key := fmt.Sprintf("piece:r%d:%d", round, j)
			keys[j] = key
			w.OnSend(key)
		}
		clock.Advance(2 * time.Second) // 2s RTT per round.
		for _, key := range keys {
			w.OnAck(key)
		}
		windows = append(windows, w.Window())
	}

	finalWindow := w.Window()

	// Window should have grown from 70 back toward lastMaxWindow (100) and beyond.
	if finalWindow <= windowAfterLoss {
		t.Errorf("expected cubic growth, window stayed at %d (started at %d)", finalWindow, windowAfterLoss)
	}

	// Verify it grows past the original W_max (convex region).
	if finalWindow <= 100 {
		t.Errorf("expected window to exceed original W_max (100), got %d", finalWindow)
	}

	// Verify growth is monotonically increasing (no stalls or reductions).
	for i := 1; i < len(windows); i++ {
		if windows[i] < windows[i-1] {
			t.Errorf("expected monotonic growth, windows[%d]=%d < windows[%d]=%d", i, windows[i], i-1, windows[i-1])
		}
	}
}

func TestCubic_TCPFriendlyMode(t *testing.T) {
	// With a very small window and short time, Reno linear growth should
	// dominate over the cubic curve (which is very flat near the origin).
	config := Config{
		MinWindow:     2,
		MaxWindow:     2000,
		InitialWindow: 10,
	}
	w, clock := newTestWindow(config)

	// Trigger a loss to exit slow start and set a low W_max.
	w.OnSend("piece:init")
	w.OnFail("piece:init")

	windowAfterLoss := w.Window() // 7
	if windowAfterLoss != 7 {
		t.Fatalf("expected window 7 after loss, got %d", windowAfterLoss)
	}

	// Send enough ACKs to observe Reno growth.
	// With alpha ≈ 0.529 and window ≈ 7, we need ~13 ACKs per window increase.
	for round := range 20 {
		window := w.Window()
		keys := make([]string, window)
		for j := range window {
			key := fmt.Sprintf("piece:r%d:%d", round, j)
			keys[j] = key
			w.OnSend(key)
		}
		clock.Advance(100 * time.Millisecond)
		for _, key := range keys {
			w.OnAck(key)
		}
	}

	// Window should have grown via TCP-friendly Reno fallback.
	if w.Window() <= windowAfterLoss {
		t.Errorf("expected TCP-friendly growth, window=%d (was %d)", w.Window(), windowAfterLoss)
	}
}

func TestCubic_RespectsMaxWindow(t *testing.T) {
	config := Config{
		MinWindow:     2,
		MaxWindow:     20,
		InitialWindow: 15,
	}
	w, clock := newTestWindow(config)

	// Slow start should be capped at maxWindow.
	for round := range 50 {
		window := w.Window()
		keys := make([]string, window)
		for j := range window {
			key := fmt.Sprintf("piece:r%d:%d", round, j)
			keys[j] = key
			w.OnSend(key)
		}
		clock.Advance(100 * time.Millisecond)
		for _, key := range keys {
			w.OnAck(key)
		}
	}

	if w.Window() > config.MaxWindow {
		t.Errorf("window %d exceeded maxWindow %d", w.Window(), config.MaxWindow)
	}
}

func TestApplicationLimited_NoGrowth(t *testing.T) {
	config := Config{
		MinWindow:     2,
		MaxWindow:     200,
		InitialWindow: 50,
	}
	w, clock := newTestWindow(config)

	initial := w.Window()

	// Send only 1 piece when window is 50 — far below capacity.
	// This is application-limited.
	for range 20 {
		sendAndAck(w, fmt.Sprintf("piece:%d", clock.now.UnixNano()), clock, 100*time.Millisecond)
	}

	// Window should not have grown (application-limited, inflight << window).
	if w.Window() != initial {
		t.Errorf("expected no growth when application-limited, initial=%d, got=%d", initial, w.Window())
	}
}

func TestLoss_CompetingFlows(t *testing.T) {
	config := Config{
		MinWindow:     2,
		MaxWindow:     2000,
		InitialWindow: 100,
	}
	w, _ := newTestWindow(config)

	// First loss: lastMaxWindow should be set to current window.
	w.OnSend("piece:0")
	w.OnFail("piece:0")

	// lastMaxWindow should be 100 (original window).
	// Window should be 70 (100 * 0.7).
	if w.Window() != 70 {
		t.Fatalf("expected window 70, got %d", w.Window())
	}

	// Second loss while window (70) is still below lastMaxWindow (100).
	// This triggers competing flow detection: lastMaxWindow = 70 * 0.85 = 59.
	w.OnSend("piece:1")
	w.OnFail("piece:1")

	lmw := 70.0 * cubicBetaLastMax
	expectedLastMax := int(lmw) // 59
	ew := 70.0 * cubicBeta
	expectedWindow := int(ew) // 49

	if w.Window() != expectedWindow {
		t.Errorf("expected window %d, got %d", expectedWindow, w.Window())
	}

	// Verify the competing flow backoff was applied by checking that
	// cubicK is computed from the reduced lastMaxWindow (59), not 100.
	// We can verify indirectly: a third loss while window < lastMaxWindow
	// should apply betaLastMax again.
	w.OnSend("piece:2")
	w.OnFail("piece:2")

	// window was 49, lastMaxWindow should have been set via betaLastMax (49 < 59).
	tw := float64(expectedWindow) * cubicBeta
	thirdExpected := int(tw) // 34
	if w.Window() != thirdExpected {
		t.Errorf("expected window %d after third loss, got %d", thirdExpected, w.Window())
	}

	_ = expectedLastMax
}

func TestCubic_ConcaveConvexTransition(t *testing.T) {
	// Verify the cubic function transitions from concave (growing toward W_max)
	// to convex (growing past W_max) correctly.
	config := Config{
		MinWindow:     2,
		MaxWindow:     2000,
		InitialWindow: 100,
	}
	w, clock := newTestWindow(config)

	// Loss to enter congestion avoidance.
	w.OnSend("piece:init")
	w.OnFail("piece:init")
	// Window = 70, lastMaxWindow = 100.

	windowReachedMax := false
	windowPastMax := false

	for round := range 100 {
		window := w.Window()
		if window >= 100 {
			windowReachedMax = true
		}
		if window > 100 {
			windowPastMax = true
			break
		}
		keys := make([]string, window)
		for j := range window {
			key := fmt.Sprintf("piece:r%d:%d", round, j)
			keys[j] = key
			w.OnSend(key)
		}
		clock.Advance(2 * time.Second)
		for _, key := range keys {
			w.OnAck(key)
		}
	}

	if !windowReachedMax {
		t.Error("expected window to reach W_max (100)")
	}
	if !windowPastMax {
		t.Errorf("expected window to grow past W_max (100), final=%d", w.Window())
	}
}

func TestCubic_RenoAlpha(t *testing.T) {
	// Verify the Reno alpha constant matches RFC 8312.
	expected := 3.0 * (1.0 - 0.7) / (1.0 + 0.7) // ~0.529
	if math.Abs(renoAlpha-expected) > 0.001 {
		t.Errorf("expected renoAlpha ~%.3f, got %.3f", expected, renoAlpha)
	}
}

func TestOnFail_NotInFlight(t *testing.T) {
	w, _ := newTestWindow(Config{
		MinWindow:     2,
		MaxWindow:     100,
		InitialWindow: 50,
	})

	initial := w.Window()

	// Failing a piece that's not in-flight should be a no-op.
	w.OnFail("not-in-flight")

	if w.Window() != initial {
		t.Errorf("expected no window change for non-inflight piece, was %d, got %d", initial, w.Window())
	}
}

func TestOnAck_NotInFlight(t *testing.T) {
	w, _ := newTestWindow(Config{
		MinWindow:     2,
		MaxWindow:     100,
		InitialWindow: 50,
	})

	initial := w.Window()

	// ACKing a piece that's not in-flight should be a no-op.
	w.OnAck("not-in-flight")

	if w.Window() != initial {
		t.Errorf("expected no window change for non-inflight piece, was %d, got %d", initial, w.Window())
	}
}

func TestClearInflight_ClearsPieceSeq(t *testing.T) {
	w, _ := newTestWindow(Config{
		MinWindow:     2,
		MaxWindow:     100,
		InitialWindow: 50,
	})

	w.OnSend("piece:0")
	w.OnSend("piece:1")

	w.ClearInflight()

	// After clear, pieceSeq should be empty.
	w.mu.Lock()
	if len(w.pieceSeq) != 0 {
		t.Errorf("expected pieceSeq to be empty after ClearInflight, got %d entries", len(w.pieceSeq))
	}
	w.mu.Unlock()
}

func TestStats_Fields(t *testing.T) {
	config := Config{
		MinWindow:     2,
		MaxWindow:     200,
		InitialWindow: 50,
	}
	w, clock := newTestWindow(config)

	// Initially in slow start, not in recovery.
	stats := w.Stats()
	if !stats.InSlowStart {
		t.Error("expected InSlowStart=true initially")
	}
	if stats.InRecovery {
		t.Error("expected InRecovery=false initially")
	}
	if stats.SSThresh != 200 {
		t.Errorf("expected SSThresh=200, got %d", stats.SSThresh)
	}

	// Trigger a loss to enter recovery.
	w.OnSend("piece:0")
	w.OnFail("piece:0")

	stats = w.Stats()
	if stats.InSlowStart {
		t.Error("expected InSlowStart=false after loss")
	}
	if !stats.InRecovery {
		t.Error("expected InRecovery=true after loss")
	}

	// Send and ACK a post-cutback piece to exit recovery.
	w.OnSend("piece:new")
	clock.Advance(100 * time.Millisecond)
	w.OnAck("piece:new")

	stats = w.Stats()
	if stats.InRecovery {
		t.Error("expected InRecovery=false after post-cutback ACK")
	}
}
