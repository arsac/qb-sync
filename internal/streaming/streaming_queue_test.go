package streaming

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/arsac/qb-sync/internal/congestion"
	pb "github.com/arsac/qb-sync/proto"
)

func testQueueLogger(t *testing.T) *slog.Logger {
	t.Helper()
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// makeTestPoolWithInflight creates a StreamPool with one PooledStream that has
// in-flight pieces registered in its congestion window.
func makeTestPoolWithInflight(t *testing.T, keys []string) *StreamPool {
	t.Helper()

	logger := testQueueLogger(t)
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
	logger := testQueueLogger(t)
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

	pool := NewStreamPool(nil, testQueueLogger(t), StreamPoolConfig{
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
