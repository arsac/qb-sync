package congestion

import (
	"testing"
	"time"
)

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
}

func TestAdaptiveWindow_CanSend(t *testing.T) {
	config := Config{
		MinWindow:     2,
		MaxWindow:     10,
		InitialWindow: 3,
	}
	w := NewAdaptiveWindow(config)

	// Should be able to send initially
	if !w.CanSend() {
		t.Error("expected CanSend() to be true")
	}

	// Fill up the window
	w.OnSend("piece:0")
	w.OnSend("piece:1")
	w.OnSend("piece:2")

	// Now should not be able to send
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

	// Send a piece
	w.OnSend("piece:0")
	if w.InFlight() != 1 {
		t.Errorf("expected 1 inflight, got %d", w.InFlight())
	}

	// Simulate some delay for RTT measurement
	time.Sleep(10 * time.Millisecond)

	// Ack the piece
	w.OnAck("piece:0")
	if w.InFlight() != 0 {
		t.Errorf("expected 0 inflight after ack, got %d", w.InFlight())
	}

	// Check stats have RTT measurements
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

	// Send a piece
	w.OnSend("piece:0")

	// Fail the piece
	w.OnFail("piece:0")

	// Window should be reduced
	if w.Window() >= initialWindow {
		t.Errorf("expected window to decrease on failure, got %d (was %d)", w.Window(), initialWindow)
	}

	// Inflight should be 0
	if w.InFlight() != 0 {
		t.Errorf("expected 0 inflight after failure, got %d", w.InFlight())
	}
}

func TestAdaptiveWindow_WindowIncrease(t *testing.T) {
	config := Config{
		MinWindow:     2,
		MaxWindow:     100,
		InitialWindow: 10,
	}
	w := NewAdaptiveWindow(config)

	// Send and ack several pieces quickly (low RTT)
	// This should cause window to increase
	initialWindow := w.Window()

	for i := range 20 {
		key := "piece:" + string(rune('0'+i))
		w.OnSend(key)
		// Very short delay to simulate fast RTT
		time.Sleep(1 * time.Millisecond)
		w.OnAck(key)
	}

	// Window should have increased
	if w.Window() <= initialWindow {
		t.Errorf("expected window to increase with low RTT, got %d (was %d)", w.Window(), initialWindow)
	}
}

func TestAdaptiveWindow_WindowDecrease(t *testing.T) {
	config := Config{
		MinWindow:     2,
		MaxWindow:     100,
		InitialWindow: 10,
	}
	w := NewAdaptiveWindow(config)

	// First establish a baseline minRTT
	w.OnSend("piece:baseline")
	time.Sleep(5 * time.Millisecond)
	w.OnAck("piece:baseline")

	initialWindow := w.Window()

	// Now send pieces with much higher RTT (simulating congestion)
	for i := range 10 {
		key := "piece:slow:" + string(rune('0'+i))
		w.OnSend(key)
		// Long delay to simulate high RTT (> 2x minRTT threshold)
		time.Sleep(20 * time.Millisecond)
		w.OnAck(key)
	}

	// Window should have decreased due to high RTT ratio
	if w.Window() >= initialWindow {
		t.Errorf("expected window to decrease with high RTT, got %d (was %d)", w.Window(), initialWindow)
	}
}

func TestAdaptiveWindow_MinWindowFloor(t *testing.T) {
	config := Config{
		MinWindow:     5,
		MaxWindow:     100,
		InitialWindow: 10,
	}
	w := NewAdaptiveWindow(config)

	// Fail many pieces to drive window down
	for i := range 20 {
		key := "piece:" + string(rune('0'+i))
		w.OnSend(key)
		w.OnFail(key)
	}

	// Window should not go below minWindow
	if w.Window() < config.MinWindow {
		t.Errorf("window went below minimum: got %d, min %d", w.Window(), config.MinWindow)
	}
}

func TestAdaptiveWindow_ClearInflight(t *testing.T) {
	w := NewAdaptiveWindow(DefaultConfig())

	// Send some pieces
	w.OnSend("piece:0")
	w.OnSend("piece:1")
	w.OnSend("piece:2")

	if w.InFlight() != 3 {
		t.Errorf("expected 3 inflight, got %d", w.InFlight())
	}

	// Clear all inflight
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

	// TrySend should succeed when window has capacity
	if !w.TrySend("piece:0") {
		t.Error("expected TrySend to succeed with capacity")
	}
	if w.InFlight() != 1 {
		t.Errorf("expected 1 inflight, got %d", w.InFlight())
	}

	// Fill the window
	if !w.TrySend("piece:1") {
		t.Error("expected TrySend to succeed")
	}
	if !w.TrySend("piece:2") {
		t.Error("expected TrySend to succeed")
	}

	// TrySend should fail when window is full
	if w.TrySend("piece:3") {
		t.Error("expected TrySend to fail when window full")
	}
	if w.InFlight() != 3 {
		t.Errorf("expected 3 inflight (piece:3 should not be added), got %d", w.InFlight())
	}

	// After ack, TrySend should succeed again
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

	// First send
	if !w.TrySend("piece:0") {
		t.Error("expected first TrySend to succeed")
	}

	// Sending same key again should still work (updates timestamp)
	// but should not increase inflight count
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
		PieceTimeout:  50 * time.Millisecond, // Short timeout for testing
	}
	w := NewAdaptiveWindow(config)

	// Send some pieces
	w.OnSend("piece:0")
	w.OnSend("piece:1")

	// Initially no stale keys
	stale := w.GetStaleKeys()
	if len(stale) != 0 {
		t.Errorf("expected no stale keys initially, got %d", len(stale))
	}

	// Wait for timeout
	time.Sleep(60 * time.Millisecond)

	// Now pieces should be stale
	stale = w.GetStaleKeys()
	if len(stale) != 2 {
		t.Errorf("expected 2 stale keys after timeout, got %d", len(stale))
	}

	// Send a fresh piece
	w.OnSend("piece:2")

	// Only the old pieces should be stale
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

	// Send a piece
	w.OnSend("piece:0")

	// Wait for it to become stale
	time.Sleep(60 * time.Millisecond)

	stale := w.GetStaleKeys()
	if len(stale) != 1 {
		t.Errorf("expected 1 stale key, got %d", len(stale))
	}

	// "Retry" the piece (re-send updates timestamp)
	w.OnSend("piece:0")

	// Should no longer be stale
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

	// Send a piece
	w.OnSend("piece:0")
	time.Sleep(20 * time.Millisecond)

	// "Retry" the piece (simulating failure and re-send)
	w.OnSend("piece:0")
	time.Sleep(10 * time.Millisecond)

	// Ack the piece - RTT should be based on ORIGINAL send time (~30ms)
	// not the retry time (~10ms)
	w.OnAck("piece:0")

	stats := w.Stats()
	// MinRTT should be around 30ms (original send time), not 10ms
	if stats.MinRTT < 25*time.Millisecond {
		t.Errorf("RTT should be based on original send time (~30ms), got %v", stats.MinRTT)
	}
}

func TestAdaptiveWindow_ConfigValidation(t *testing.T) {
	// Test MinWindow > MaxWindow is corrected
	config := Config{
		MinWindow:     100,
		MaxWindow:     10,
		InitialWindow: 50,
	}
	w := NewAdaptiveWindow(config)

	// MinWindow should be clamped to MaxWindow
	if w.minWindow > w.maxWindow {
		t.Errorf("MinWindow (%d) should not exceed MaxWindow (%d)", w.minWindow, w.maxWindow)
	}

	// InitialWindow should be within bounds
	if w.Window() < w.minWindow || w.Window() > w.maxWindow {
		t.Errorf("InitialWindow (%d) should be within [%d, %d]", w.Window(), w.minWindow, w.maxWindow)
	}
}

func TestAdaptiveWindow_ConfigValidation_InitialTooLow(t *testing.T) {
	config := Config{
		MinWindow:     10,
		MaxWindow:     100,
		InitialWindow: 5, // Below MinWindow
	}
	w := NewAdaptiveWindow(config)

	// InitialWindow should be raised to MinWindow
	if w.Window() < w.minWindow {
		t.Errorf("InitialWindow (%d) should be at least MinWindow (%d)", w.Window(), w.minWindow)
	}
}

func TestAdaptiveWindow_ConfigValidation_InitialTooHigh(t *testing.T) {
	config := Config{
		MinWindow:     10,
		MaxWindow:     50,
		InitialWindow: 100, // Above MaxWindow
	}
	w := NewAdaptiveWindow(config)

	// InitialWindow should be lowered to MaxWindow
	if w.Window() > w.maxWindow {
		t.Errorf("InitialWindow (%d) should be at most MaxWindow (%d)", w.Window(), w.maxWindow)
	}
}
