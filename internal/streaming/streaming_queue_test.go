package streaming

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arsac/qb-sync/internal/congestion"
	pb "github.com/arsac/qb-sync/proto"
)

// concurrencyTrackingSource is a mock PieceSource that tracks max concurrent
// ReadPiece calls and returns an error after a controlled delay.
type concurrencyTrackingSource struct {
	maxConcurrent atomic.Int32
	concurrent    atomic.Int32
	calls         atomic.Int64
	delay         time.Duration
}

func (s *concurrencyTrackingSource) ReadPiece(_ context.Context, _ *pb.Piece) ([]byte, error) {
	c := s.concurrent.Add(1)
	defer s.concurrent.Add(-1)
	s.calls.Add(1)

	for {
		old := s.maxConcurrent.Load()
		if c <= old || s.maxConcurrent.CompareAndSwap(old, c) {
			break
		}
	}

	time.Sleep(s.delay)
	return nil, errors.New("mock read error")
}

func (s *concurrencyTrackingSource) GetPieceStates(context.Context, string) ([]PieceState, error) {
	return nil, nil
}

func (s *concurrencyTrackingSource) GetPieceHashes(context.Context, string) ([]string, error) {
	return nil, nil
}

func (s *concurrencyTrackingSource) GetTorrentMetadata(context.Context, string) (*TorrentMetadata, error) {
	return nil, nil
}

// makeTestPoolWithInflight creates a StreamPool with one PooledStream that has
// in-flight pieces registered in its congestion window.
func makeTestPoolWithInflight(t *testing.T, keys []string) *StreamPool {
	t.Helper()

	logger := testLogger
	pool := NewStreamPool(nil, logger, StreamPoolConfig{
		MaxNumStreams:  1,
		AckChannelSize: 100,
	})
	pool.ctx, pool.cancel = context.WithCancel(context.Background())

	window := congestion.NewAdaptiveWindow(congestion.DefaultConfig())
	for _, key := range keys {
		window.OnSend(key)
	}

	ps := &PooledStream{
		window: window,
		id:     0,
	}
	pool.mu.Lock()
	pool.streams = append(pool.streams, ps)
	pool.mu.Unlock()

	return pool
}

// makeDrainTestQueue creates a minimal BidiQueue for drain tests.
// The tracker has no torrents — MarkStreamed/MarkFailed safely no-op.
func makeDrainTestQueue(t *testing.T) *BidiQueue {
	t.Helper()
	logger := testLogger
	return &BidiQueue{
		tracker:      NewPieceMonitor(nil, nil, logger, DefaultPieceMonitorConfig()),
		logger:       logger,
		config:       DefaultBidiQueueConfig(),
		pieceStreams: make(map[string]*PooledStream),
	}
}

func TestDrainInFlightPool_ProcessesAcksWithCancelledContext(t *testing.T) {
	// Regression: drainInFlightPool must process acks even when the parent
	// context is already cancelled (the shutdown case). Before the fix,
	// WithTimeout(ctx) derived from a cancelled context expired immediately.

	q := makeDrainTestQueue(t)

	hash := "testhash"
	idx := int32(0)
	key := pieceKey(hash, idx)
	pool := makeTestPoolWithInflight(t, []string{key})

	// Register piece→stream mapping so processAck finds the window
	ps := pool.streams[0]
	q.pieceStreamsMu.Lock()
	q.pieceStreams[key] = ps
	q.pieceStreamsMu.Unlock()

	if pool.TotalInFlight() != 1 {
		t.Fatalf("expected 1 in-flight, got %d", pool.TotalInFlight())
	}

	// Push an ack into the pool's channel
	pool.acks <- &pb.PieceAck{
		TorrentHash: hash,
		PieceIndex:  idx,
		Success:     true,
	}

	// Cancel the context BEFORE calling drain — simulates shutdown
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan struct{})
	go func() {
		q.drainInFlightPool(cancelledCtx, pool)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("drainInFlightPool blocked — likely using cancelled context for timeout")
	}

	if pool.TotalInFlight() != 0 {
		t.Errorf("expected 0 in-flight after drain, got %d", pool.TotalInFlight())
	}
}

func TestDrainInFlightPool_SkipsWhenNoInflight(t *testing.T) {
	q := makeDrainTestQueue(t)

	pool := NewStreamPool(nil, testLogger, StreamPoolConfig{
		MaxNumStreams:  1,
		AckChannelSize: 10,
	})

	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan struct{})
	go func() {
		q.drainInFlightPool(cancelledCtx, pool)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("drainInFlightPool should return immediately with no in-flight pieces")
	}
}

func TestDrainInFlightPool_MarksFailedOnPoolError(t *testing.T) {
	q := makeDrainTestQueue(t)

	hash := "testhash"
	key := pieceKey(hash, 0)
	pool := makeTestPoolWithInflight(t, []string{key})

	// Send a stream error instead of an ack
	pool.errs <- errors.New("stream broken")

	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan struct{})
	go func() {
		q.drainInFlightPool(cancelledCtx, pool)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("drainInFlightPool blocked on pool error")
	}

	if pool.TotalInFlight() != 0 {
		t.Errorf("expected 0 in-flight after error drain, got %d", pool.TotalInFlight())
	}
}

func TestSenderWorkersConcurrency(t *testing.T) {
	const numSenders = 4
	const numPieces = numSenders * 2

	logger := testLogger

	// Mock source: tracks concurrent ReadPiece calls with a delay to make
	// concurrency observable, then returns an error (avoids needing a real
	// gRPC stream for Send).
	source := &concurrencyTrackingSource{delay: 100 * time.Millisecond}

	// Pre-populate initResults so ensureTorrentInitialized succeeds.
	dest := &GRPCDestination{
		initResults: map[string]*InitTorrentResult{
			"testhash": {},
		},
	}

	// Tracker with no torrent state: IsPieceStreamed returns false,
	// MarkFailed safely no-ops.
	tracker := NewPieceMonitor(nil, nil, logger, DefaultPieceMonitorConfig())

	config := DefaultBidiQueueConfig()
	config.NumSenders = numSenders
	q := &BidiQueue{
		source:       source,
		dest:         dest,
		tracker:      tracker,
		logger:       logger,
		config:       config,
		pieceStreams: make(map[string]*PooledStream),
	}

	// Pool with one stream whose window has capacity (CanSend = true).
	// ReadPiece returns error before reaching SelectStream/Send, so the
	// stream's nil PieceStream is never touched.
	pool := makeTestPoolWithInflight(t, nil)

	// Push all pieces into the buffered completed channel, then close it
	// so workers drain the pieces and exit.
	for i := range numPieces {
		tracker.completed <- &pb.Piece{
			TorrentHash: "testhash",
			Index:       int32(i),
		}
	}
	close(tracker.completed)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stopSender := make(chan struct{})

	// Close stopSender after all pieces are fully processed so the stats
	// reporter goroutine exits promptly (it only listens for ctx.Done or
	// stopSender). We wait on piecesFail because it's the last thing
	// incremented per piece in senderWorker.
	go func() {
		for q.piecesFail.Load() < numPieces {
			time.Sleep(10 * time.Millisecond)
		}
		close(stopSender)
	}()

	done := make(chan struct{})
	go func() {
		q.runSenderPool(ctx, pool, stopSender)
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("runSenderPool did not complete in time")
	}

	// With 4 concurrent senders and 100ms delay per ReadPiece, multiple
	// workers should overlap. Require at least 2 concurrent calls.
	maxC := source.maxConcurrent.Load()
	if maxC < 2 {
		t.Errorf("expected concurrent ReadPiece calls, got max concurrency %d", maxC)
	}

	// All pieces should have been processed (each fails at ReadPiece).
	if got := source.calls.Load(); got != numPieces {
		t.Errorf("expected %d ReadPiece calls, got %d", numPieces, got)
	}

	// Each failed piece increments piecesFail.
	if got := q.piecesFail.Load(); got != numPieces {
		t.Errorf("expected %d piecesFail, got %d", numPieces, got)
	}
}

// makeTestPoolWithStaleKeys creates a StreamPool with the given number of
// streams, each with pieces that become stale after a short sleep.
func makeTestPoolWithStaleKeys(t *testing.T, streamKeys [][]string) (*StreamPool, []*PooledStream) {
	t.Helper()

	logger := testLogger
	pool := NewStreamPool(nil, logger, StreamPoolConfig{
		MaxNumStreams:  len(streamKeys),
		AckChannelSize: 100,
	})
	pool.ctx, pool.cancel = context.WithCancel(context.Background())

	streams := make([]*PooledStream, len(streamKeys))
	pool.mu.Lock()
	for i, keys := range streamKeys {
		window := congestion.NewAdaptiveWindow(congestion.Config{
			MinWindow:     2,
			MaxWindow:     100,
			InitialWindow: 10,
			PieceTimeout:  50 * time.Millisecond,
		})
		for _, key := range keys {
			window.OnSend(key)
		}
		ps := &PooledStream{window: window, id: i}
		pool.streams = append(pool.streams, ps)
		streams[i] = ps
	}
	pool.mu.Unlock()

	return pool, streams
}

func TestGetAllStaleKeys_PairsKeyWithOwningStream(t *testing.T) {
	// Two streams, each with one piece. Verify that GetAllStaleKeys pairs
	// each key with the correct owning PooledStream.
	pool, streams := makeTestPoolWithStaleKeys(t, [][]string{
		{"hash1:0"},
		{"hash2:0"},
	})
	defer pool.cancel()

	// Nothing stale yet.
	if got := pool.GetAllStaleKeys(); len(got) != 0 {
		t.Fatalf("expected 0 stale keys initially, got %d", len(got))
	}

	// Wait for pieces to become stale.
	time.Sleep(60 * time.Millisecond)

	stale := pool.GetAllStaleKeys()
	if len(stale) != 2 {
		t.Fatalf("expected 2 stale keys, got %d", len(stale))
	}

	// Build map for easier assertion.
	byKey := make(map[string]*PooledStream, len(stale))
	for _, sk := range stale {
		byKey[sk.Key] = sk.Stream
	}

	if byKey["hash1:0"] != streams[0] {
		t.Errorf("hash1:0 should be paired with stream 0, got stream %d", byKey["hash1:0"].id)
	}
	if byKey["hash2:0"] != streams[1] {
		t.Errorf("hash2:0 should be paired with stream 1, got stream %d", byKey["hash2:0"].id)
	}
}

func TestRemovePieceStreamIfMatch_DeletesOnMatch(t *testing.T) {
	q := makeDrainTestQueue(t)

	ps := &PooledStream{id: 1}
	q.pieceStreamsMu.Lock()
	q.pieceStreams["key1"] = ps
	q.pieceStreamsMu.Unlock()

	q.removePieceStreamIfMatch("key1", ps)

	q.pieceStreamsMu.RLock()
	_, exists := q.pieceStreams["key1"]
	q.pieceStreamsMu.RUnlock()

	if exists {
		t.Error("expected key1 to be deleted when stream matches")
	}
}

func TestRemovePieceStreamIfMatch_PreservesOnMismatch(t *testing.T) {
	q := makeDrainTestQueue(t)

	streamA := &PooledStream{id: 1}
	streamB := &PooledStream{id: 2}

	// Map points to streamB (simulating a retry overwrite).
	q.pieceStreamsMu.Lock()
	q.pieceStreams["key1"] = streamB
	q.pieceStreamsMu.Unlock()

	// Try to remove with streamA — should be a no-op.
	q.removePieceStreamIfMatch("key1", streamA)

	q.pieceStreamsMu.RLock()
	got := q.pieceStreams["key1"]
	q.pieceStreamsMu.RUnlock()

	if got != streamB {
		t.Errorf("expected mapping to be preserved (streamB), got %v", got)
	}
}

func TestRemovePieceStreamIfMatch_NoopOnMissingKey(t *testing.T) {
	q := makeDrainTestQueue(t)

	// Should not panic on missing key.
	q.removePieceStreamIfMatch("nonexistent", &PooledStream{id: 1})
}

func TestHandleStalePiecesPool_RemovesFromCorrectWindow(t *testing.T) {
	// Reproduce the race scenario:
	// 1. streamA has a stale key "hash:0"
	// 2. Between GetAllStaleKeys and OnFail, pieceStreams is overwritten to streamB
	// 3. Verify OnFail targets streamA (not streamB) and streamB mapping is preserved.

	pool, streams := makeTestPoolWithStaleKeys(t, [][]string{
		{"hash:0"}, // streamA — will have the stale key
		{},         // streamB — empty, will be the "retry" target
	})
	defer pool.cancel()

	streamA := streams[0]
	streamB := streams[1]

	q := makeDrainTestQueue(t)

	// Simulate the retry overwrite: pieceStreams maps hash:0 → streamB.
	// In the real race, this happens between GetAllStaleKeys and the loop.
	q.pieceStreamsMu.Lock()
	q.pieceStreams["hash:0"] = streamB
	q.pieceStreamsMu.Unlock()

	// Wait for the piece in streamA to become stale.
	time.Sleep(60 * time.Millisecond)

	// Verify the stale key is in streamA.
	if streamA.window.InFlight() != 1 {
		t.Fatalf("streamA should have 1 in-flight, got %d", streamA.window.InFlight())
	}

	q.handleStalePiecesPool(context.Background(), pool)

	// The fix: OnFail targeted streamA (the actual owner), not streamB.
	if streamA.window.InFlight() != 0 {
		t.Errorf("streamA should have 0 in-flight after stale cleanup, got %d",
			streamA.window.InFlight())
	}

	// streamB's mapping should be preserved (removePieceStreamIfMatch sees mismatch).
	q.pieceStreamsMu.RLock()
	got := q.pieceStreams["hash:0"]
	q.pieceStreamsMu.RUnlock()

	if got != streamB {
		t.Errorf("streamB mapping should be preserved, got %v", got)
	}
}
