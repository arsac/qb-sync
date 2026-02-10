package streaming

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/arsac/qb-sync/internal/congestion"
	pb "github.com/arsac/qb-sync/proto"
	"google.golang.org/grpc/metadata"
)

// mockBidiStream implements pb.QBSyncService_StreamPiecesBidiClient for testing
// PieceStream behavior without a real gRPC connection.
type mockBidiStream struct {
	sendFunc func(*pb.WritePieceRequest) error
	recvFunc func() (*pb.PieceAck, error)
	ctx      context.Context
}

func (m *mockBidiStream) Send(req *pb.WritePieceRequest) error {
	if m.sendFunc != nil {
		return m.sendFunc(req)
	}
	return nil
}

func (m *mockBidiStream) Recv() (*pb.PieceAck, error) {
	if m.recvFunc != nil {
		return m.recvFunc()
	}
	// Block until context cancelled (simulates idle stream).
	<-m.ctx.Done()
	return nil, m.ctx.Err()
}

func (m *mockBidiStream) Header() (metadata.MD, error) { return nil, nil }
func (m *mockBidiStream) Trailer() metadata.MD         { return nil }
func (m *mockBidiStream) CloseSend() error             { return nil }
func (m *mockBidiStream) Context() context.Context     { return m.ctx }
func (m *mockBidiStream) SendMsg(any) error            { return nil }
func (m *mockBidiStream) RecvMsg(any) error            { return nil }

var testLogger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

// newTestPieceStream creates a PieceStream with a mock stream for testing.
// The mock's ctx is set to the derived stream context so its default Recv
// unblocks when the stream is closed.
func newTestPieceStream(parentCtx context.Context, mock *mockBidiStream) *PieceStream {
	return newTestPieceStreamWithOptions(parentCtx, mock, DefaultAckChannelSize, 0, 0)
}

// newTestPieceStreamWithOptions creates a PieceStream with configurable ack channel
// buffer size and timeout overrides. Zero timeout means use the package-level const.
func newTestPieceStreamWithOptions(
	parentCtx context.Context,
	mock *mockBidiStream,
	ackBufSize int,
	ackTimeout, sndTimeout time.Duration,
) *PieceStream {
	streamCtx, streamCancel := context.WithCancel(parentCtx)
	mock.ctx = streamCtx

	ps := &PieceStream{
		ctx:                     streamCtx,
		cancel:                  streamCancel,
		stream:                  mock,
		logger:                  testLogger,
		acks:                    make(chan *pb.PieceAck, ackBufSize),
		ackReady:                make(chan struct{}, 1),
		done:                    make(chan struct{}),
		errors:                  make(chan error, 1),
		sendCh:                  make(chan *sendRequest),
		stopSend:                make(chan struct{}),
		sendDone:                make(chan struct{}),
		ackWriteTimeoutOverride: ackTimeout,
		sendTimeoutOverride:     sndTimeout,
	}

	go ps.receiveAcks()
	go ps.sendLoop()
	return ps
}

// TestSend_NormalSendSucceeds verifies that a non-blocking Send returns
// the underlying stream's result directly.
func TestSend_NormalSendSucceeds(t *testing.T) {
	t.Parallel()

	mock := &mockBidiStream{
		sendFunc: func(*pb.WritePieceRequest) error { return nil },
	}

	ps := newTestPieceStream(context.Background(), mock)
	defer ps.Close()

	if err := ps.Send(&pb.WritePieceRequest{TorrentHash: "test"}); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

// TestSend_StreamErrorPropagates verifies that a synchronous error from the
// underlying stream is returned to the caller without being swallowed.
func TestSend_StreamErrorPropagates(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("transport closing")
	mock := &mockBidiStream{
		sendFunc: func(*pb.WritePieceRequest) error { return expectedErr },
	}

	ps := newTestPieceStream(context.Background(), mock)
	defer ps.Close()

	err := ps.Send(&pb.WritePieceRequest{TorrentHash: "test"})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected %v, got %v", expectedErr, err)
	}
}

// TestSend_AfterCloseSendReturnsError verifies that calling Send after CloseSend
// returns an error instead of panicking. Before the stopSend fix, this would
// panic with "send on closed channel" because CloseSend closed sendCh directly.
func TestSend_AfterCloseSendReturnsError(t *testing.T) {
	t.Parallel()

	mock := &mockBidiStream{
		sendFunc: func(*pb.WritePieceRequest) error { return nil },
	}

	ps := newTestPieceStream(context.Background(), mock)
	defer ps.Close()

	// Close the send side first.
	if err := ps.CloseSend(); err != nil {
		t.Fatalf("CloseSend failed: %v", err)
	}

	// Send after CloseSend must return an error, not panic.
	err := ps.Send(&pb.WritePieceRequest{TorrentHash: "test"})
	if err == nil {
		t.Fatal("expected error from Send after CloseSend, got nil")
	}

	t.Logf("Send after CloseSend returned: %v", err)
}

// TestSend_ReceiveExitUnblocksSend verifies that when receiveAcks detects a
// stream error and exits, it cancels the stream context, which unblocks a
// concurrent Send stuck on HTTP/2 flow control.
//
// This reproduces the production deadlock: cold stops consuming → HTTP/2 flow
// control fills → Send blocks. The fix: receiveAcks detects stream death,
// cancels the per-stream context, gRPC resets the stream, Send unblocks.
//
// The mock's sendFunc simulates gRPC behavior by blocking until the stream
// context is cancelled (real gRPC Send unblocks when the transport resets
// the stream after context cancellation).
func TestSend_ReceiveExitUnblocksSend(t *testing.T) {
	t.Parallel()

	mock := &mockBidiStream{}
	mock.sendFunc = func(*pb.WritePieceRequest) error {
		// Simulate gRPC Send blocking on flow control until context cancel.
		// Real gRPC unblocks Send when the stream context is cancelled
		// because the transport sends RST_STREAM.
		<-mock.ctx.Done()
		return mock.ctx.Err()
	}
	mock.recvFunc = func() (*pb.PieceAck, error) {
		// Simulate stream death after a short delay.
		// receiveAcks will exit and call ps.cancel(), unblocking Send.
		time.Sleep(30 * time.Millisecond)
		return nil, errors.New("stream reset by peer")
	}

	ps := newTestPieceStream(context.Background(), mock)
	defer ps.Close()

	start := time.Now()
	err := ps.Send(&pb.WritePieceRequest{TorrentHash: "test"})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected error, got nil")
	}

	// Should unblock within ~100ms (the 30ms Recv delay + scheduling).
	if elapsed > 2*time.Second {
		t.Fatalf("Send took %v — not unblocked by receiveAcks context cancel", elapsed)
	}

	t.Logf("Send unblocked in %v with error: %v", elapsed, err)
}

// TestReceiveAcks_AckChannelBlockedTimeout verifies that when the ack channel
// is full and nobody is draining it, receiveAcks exits after the ack write
// timeout, calling ps.cancel() to unblock any stuck Send().
//
// This covers the deadlock scenario where forwardAcks is slow → ps.acks fills
// → receiveAcks blocks on channel write → never calls Recv() again → can't
// detect stream death → ps.cancel() never fires → Send() stuck forever.
func TestReceiveAcks_AckChannelBlockedTimeout(t *testing.T) {
	t.Parallel()

	const testTimeout = 100 * time.Millisecond

	ackIndex := int32(0)
	mock := &mockBidiStream{
		recvFunc: func() (*pb.PieceAck, error) {
			ackIndex++
			return &pb.PieceAck{TorrentHash: "test", PieceIndex: ackIndex}, nil
		},
	}

	// Unbuffered ack channel: first ack write blocks immediately.
	ps := newTestPieceStreamWithOptions(context.Background(), mock, 0, testTimeout, 0)
	defer ps.Close()

	start := time.Now()
	select {
	case <-ps.done:
		// receiveAcks exited due to ack write timeout — correct.
	case <-time.After(5 * time.Second):
		t.Fatal("receiveAcks didn't exit after ack channel blocked")
	}
	elapsed := time.Since(start)

	// Should exit after ~testTimeout, not before.
	if elapsed < testTimeout-10*time.Millisecond {
		t.Fatalf("receiveAcks exited too early: %v (expected >= %v)", elapsed, testTimeout)
	}

	// Stream context should be cancelled (defer ps.cancel() fired).
	if ps.ctx.Err() == nil {
		t.Fatal("stream context should be cancelled after ack write timeout")
	}

	t.Logf("receiveAcks exited in %v (timeout=%v)", elapsed, testTimeout)
}

// TestSend_TimeoutCancelsStream verifies that Send independently times out
// and cancels the stream context when stream.Send() blocks forever, even
// when receiveAcks is also stuck and can't call ps.cancel().
//
// This covers the deadlock scenario where both paths are stuck:
// - Send() blocked on HTTP/2 flow control (cold not consuming)
// - receiveAcks() blocked on ack channel write (forwardAcks slow)
// The send timeout is the independent safety net that breaks the cycle.
func TestSend_TimeoutCancelsStream(t *testing.T) {
	t.Parallel()

	const testTimeout = 100 * time.Millisecond

	mock := &mockBidiStream{}
	mock.sendFunc = func(*pb.WritePieceRequest) error {
		// Block until context is cancelled — simulates gRPC Send stuck
		// on HTTP/2 flow control. Real gRPC unblocks on context cancel.
		<-mock.ctx.Done()
		return mock.ctx.Err()
	}
	// Default recvFunc blocks until context cancel — simulates receiveAcks
	// being stuck (unable to detect stream death independently).

	ps := newTestPieceStreamWithOptions(context.Background(), mock, DefaultAckChannelSize, 0, testTimeout)
	defer ps.Close()

	start := time.Now()
	err := ps.Send(&pb.WritePieceRequest{TorrentHash: "test"})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected error from send timeout, got nil")
	}

	// Should return after ~testTimeout.
	if elapsed < testTimeout-10*time.Millisecond {
		t.Fatalf("Send returned too early: %v (expected >= %v)", elapsed, testTimeout)
	}
	if elapsed > testTimeout+500*time.Millisecond {
		t.Fatalf("Send took too long: %v (expected ~%v)", elapsed, testTimeout)
	}

	// Stream context should be cancelled by the send timeout.
	if ps.ctx.Err() == nil {
		t.Fatal("stream context should be cancelled by send timeout")
	}

	t.Logf("Send timed out in %v with error: %v", elapsed, err)
}

// TestReceiveAcks_NoTimeoutWhenAcksConsumed verifies that the ack write timeout
// does NOT fire when acks are being consumed normally (no false positives).
func TestReceiveAcks_NoTimeoutWhenAcksConsumed(t *testing.T) {
	t.Parallel()

	const (
		testTimeout = 100 * time.Millisecond
		numAcks     = 50
	)

	received := make(chan struct{})
	ackIndex := int32(0)
	mock := &mockBidiStream{
		recvFunc: func() (*pb.PieceAck, error) {
			ackIndex++
			if ackIndex > numAcks {
				<-received // Block after all acks sent
				return nil, errors.New("done")
			}
			return &pb.PieceAck{TorrentHash: "test", PieceIndex: ackIndex}, nil
		},
	}

	// Small buffer to create backpressure, but we drain fast enough.
	ps := newTestPieceStreamWithOptions(context.Background(), mock, 5, testTimeout, 0)
	defer ps.Close()

	// Consume all acks.
	for range numAcks {
		select {
		case <-ps.acks:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for ack")
		}
	}

	// Unblock the mock and let receiveAcks finish naturally.
	close(received)
	<-ps.done

	// If the ack write timeout fired spuriously, the context would be cancelled
	// with no error on the errors channel. Check that the stream ended due to
	// the Recv error, not the timeout.
	select {
	case err := <-ps.errors:
		if err == nil || err.Error() != "done" {
			t.Fatalf("expected 'done' error from Recv, got %v", err)
		}
	default:
		// No error means receiveAcks saw context cancel or EOF — also acceptable
		// since we closed the mock.
	}
}

// TestForwardAcks_NotifiesPoolOnSilentStreamDeath verifies that when a stream's
// Done() fires but no error is pending on the Errors() channel (e.g., send
// timeout cancelled the context), forwardAcks sends a synthetic error to
// pool.errs so the ack processor can trigger reconnection.
//
// Without this fix, forwardAcks exits silently and the ack processor never
// learns the stream died, causing a permanent sender deadlock.
func TestForwardAcks_NotifiesPoolOnSilentStreamDeath(t *testing.T) {
	t.Parallel()

	// Stream done, no error pending.
	streamDone := make(chan struct{})
	close(streamDone)

	ps := &PooledStream{
		stream: &PieceStream{
			done:     streamDone,
			errors:   make(chan error, 1), // empty — no error written
			acks:     make(chan *pb.PieceAck, 10),
			ackReady: make(chan struct{}, 1),
		},
		id: 42,
	}

	poolCtx, poolCancel := context.WithCancel(context.Background())
	defer poolCancel()

	pool := &StreamPool{
		ctx:      poolCtx,
		errs:     make(chan error, 10),
		acks:     make(chan *pb.PieceAck, 10),
		ackReady: make(chan struct{}, 10),
		logger:   testLogger,
	}

	pool.wg.Add(1)
	go pool.forwardAcks(ps)

	select {
	case err := <-pool.errs:
		if err == nil {
			t.Fatal("expected non-nil synthetic error")
		}
		t.Logf("received synthetic error: %v", err)
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for synthetic error — forwardAcks exited silently")
	}
}

// TestForwardAcks_NoSpuriousErrorOnCleanShutdown verifies that when the pool
// context is cancelled (clean shutdown via pool.Close), forwardAcks does NOT
// send a synthetic error to pool.errs. Only unexpected stream deaths should
// generate errors.
func TestForwardAcks_NoSpuriousErrorOnCleanShutdown(t *testing.T) {
	t.Parallel()

	// Stream done, no error pending.
	streamDone := make(chan struct{})
	close(streamDone)

	ps := &PooledStream{
		stream: &PieceStream{
			done:     streamDone,
			errors:   make(chan error, 1), // empty
			acks:     make(chan *pb.PieceAck, 10),
			ackReady: make(chan struct{}, 1),
		},
		id: 7,
	}

	// Pool context already cancelled — simulates clean shutdown.
	poolCtx, poolCancel := context.WithCancel(context.Background())
	poolCancel()

	pool := &StreamPool{
		ctx:      poolCtx,
		errs:     make(chan error, 10),
		acks:     make(chan *pb.PieceAck, 10),
		ackReady: make(chan struct{}, 10),
		logger:   testLogger,
	}

	pool.wg.Add(1)
	go pool.forwardAcks(ps)
	pool.wg.Wait()

	select {
	case err := <-pool.errs:
		t.Fatalf("unexpected error on clean shutdown: %v", err)
	default:
		// No error — correct.
	}
}

// TestForwardAcks_DrainsErrorOnStreamClose verifies that forwardAcks drains
// any pending error from the stream's error channel after Done() fires.
//
// Without the error drain fix, when both Done() and Errors() are ready
// simultaneously, Go's select picks randomly. If Done() wins, forwardAcks
// returns without forwarding the error, leaving the ack processor unaware
// of stream death. The fix makes the Done() case explicitly drain pending
// errors before returning.
//
// Multiple iterations make the select race deterministic: without the fix,
// ~50% of iterations would miss the error.
func TestForwardAcks_DrainsErrorOnStreamClose(t *testing.T) {
	t.Parallel()

	const iterations = 50

	for i := range iterations {
		func() {
			// Create a PieceStream with a pending error and closed done channel.
			streamDone := make(chan struct{})
			streamErrors := make(chan error, 1)
			streamErrors <- errors.New("stream reset by peer")
			close(streamDone)

			ps := &PooledStream{
				stream: &PieceStream{
					done:     streamDone,
					errors:   streamErrors,
					acks:     make(chan *pb.PieceAck, 10),
					ackReady: make(chan struct{}, 1),
				},
				id: i,
			}

			poolCtx, poolCancel := context.WithCancel(context.Background())
			defer poolCancel()

			pool := &StreamPool{
				ctx:      poolCtx,
				errs:     make(chan error, 10),
				acks:     make(chan *pb.PieceAck, 10),
				ackReady: make(chan struct{}, 10),
				logger:   slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
			}

			pool.wg.Add(1)
			go pool.forwardAcks(ps)

			// forwardAcks should forward the error regardless of select ordering.
			select {
			case err := <-pool.errs:
				if err == nil || err.Error() != "stream reset by peer" {
					t.Errorf("iteration %d: expected 'stream reset by peer', got %v", i, err)
				}
			case <-time.After(1 * time.Second):
				t.Fatalf("iteration %d: timed out waiting for error to be forwarded "+
					"(select race dropped the error)", i)
			}
		}()
	}
}

// newTestPoolWithWindow creates a StreamPool with a single PooledStream whose
// congestion window is configured by windowCfg. The stream is a stub (nil
// PieceStream fields) — only the window is used. Returns the pool, the
// PooledStream, and a cancel func.
func newTestPoolWithWindow(windowCfg congestion.Config) (*StreamPool, *PooledStream, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	ps := &PooledStream{
		stream: &PieceStream{
			done:     make(chan struct{}),
			errors:   make(chan error, 1),
			acks:     make(chan *pb.PieceAck, 10),
			ackReady: make(chan struct{}, 1),
		},
		window: congestion.NewAdaptiveWindow(windowCfg),
		id:     0,
	}
	pool := &StreamPool{
		ctx:      ctx,
		cancel:   cancel,
		errs:     make(chan error, 10),
		acks:     make(chan *pb.PieceAck, 10),
		ackReady: make(chan struct{}, 4),
		logger:   testLogger,
		streams:  []*PooledStream{ps},
	}
	return pool, ps, cancel
}

// TestSenderLoop_PollingFallbackUnblocks verifies that the sender's 1-second
// polling fallback wakes it up when CanSend() becomes true without any
// AckReady signal. This is the safety net for missed signals.
//
// Setup: window=2, 2 pieces in-flight (CanSend=false). After 200ms, OnFail
// frees a slot (CanSend=true) but nobody signals ackReady. The sender's wait
// loop should unblock via the 1s timer, not immediately and not never.
func TestSenderLoop_PollingFallbackUnblocks(t *testing.T) {
	t.Parallel()

	pool, ps, cancel := newTestPoolWithWindow(congestion.Config{
		InitialWindow: 2, MinWindow: 2, MaxWindow: 2,
	})
	defer cancel()

	// Fill window: 2 TrySend calls → CanSend()=false.
	ps.window.TrySend("a")
	ps.window.TrySend("b")
	if pool.CanSend() {
		t.Fatal("expected CanSend()=false after filling window")
	}

	// After 200ms, free capacity without signaling ackReady.
	go func() {
		time.Sleep(200 * time.Millisecond)
		ps.window.OnFail("a")
	}()

	// Replicate the sender's wait loop.
	start := time.Now()
	for !pool.CanSend() {
		select {
		case <-pool.AckReady():
			// Should NOT fire — nobody signals it.
			t.Fatal("unexpected AckReady signal")
		case <-time.After(1 * time.Second):
			// Polling fallback — this is the expected wake path.
		}
	}
	elapsed := time.Since(start)

	// Should unblock between ~1s (timer) and ~2s. If it took <500ms, the
	// timer wasn't the wake source. If it took >3s, something is wrong.
	if elapsed < 500*time.Millisecond {
		t.Fatalf("unblocked too fast (%v) — polling fallback didn't fire", elapsed)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("took too long (%v) — polling fallback didn't work", elapsed)
	}
	t.Logf("sender unblocked via polling fallback in %v", elapsed)
}

// TestSenderLoop_NotifyAckProcessedUnblocks verifies that NotifyAckProcessed
// wakes the sender immediately after OnAck reduces inflight, preventing the
// signal race where the sender consumes the enqueue-time ackReady signal
// before the ack processor has called OnAck.
//
// The race:
//  1. forwardAcks enqueues ack → signals ackReady
//  2. Sender wakes, checks CanSend()=false (OnAck hasn't fired yet)
//  3. Sender goes back to waiting — signal consumed
//  4. Ack processor calls OnAck → CanSend()=true
//  5. NotifyAckProcessed() signals ackReady again ← THIS is the fix
//  6. Sender wakes, checks CanSend()=true → proceeds
//
// Without NotifyAckProcessed, step 5 never happens and the sender waits
// until the 1s polling fallback fires.
func TestSenderLoop_NotifyAckProcessedUnblocks(t *testing.T) {
	t.Parallel()

	pool, ps, cancel := newTestPoolWithWindow(congestion.Config{
		InitialWindow: 2, MinWindow: 2, MaxWindow: 2,
	})
	defer cancel()

	// Fill window.
	ps.window.TrySend("a")
	ps.window.TrySend("b")
	if pool.CanSend() {
		t.Fatal("expected CanSend()=false after filling window")
	}

	// Simulate the race: enqueue-time signal arrives first, sender consumes
	// it, then ack processor calls OnAck + NotifyAckProcessed.
	go func() {
		// Step 1: forwardAcks signals ackReady (enqueue-time).
		pool.ackReady <- struct{}{}

		// Small delay to ensure the sender wakes and re-checks CanSend()=false.
		time.Sleep(50 * time.Millisecond)

		// Step 4-5: Ack processor calls OnAck, then NotifyAckProcessed.
		ps.window.OnFail("a") // Simulates OnAck reducing inflight.
		pool.NotifyAckProcessed()
	}()

	// Replicate the sender's wait loop.
	start := time.Now()
	for !pool.CanSend() {
		select {
		case <-pool.AckReady():
			// May fire from either the enqueue signal or NotifyAckProcessed.
		case <-time.After(1 * time.Second):
			// Polling fallback — if we reach here, NotifyAckProcessed didn't work.
		}
	}
	elapsed := time.Since(start)

	// Should unblock within ~100ms (50ms goroutine delay + scheduling), NOT
	// after the 1s polling fallback.
	if elapsed > 500*time.Millisecond {
		t.Fatalf("took %v — NotifyAckProcessed didn't wake the sender (fell through to polling fallback)", elapsed)
	}
	t.Logf("sender unblocked via NotifyAckProcessed in %v", elapsed)
}

// TestSenderLoop_NotifyAckProcessedWithoutPolling verifies that removing the
// polling fallback, the sender still unblocks via NotifyAckProcessed alone.
// This proves NotifyAckProcessed is sufficient for correctness independent
// of the polling safety net.
func TestSenderLoop_NotifyAckProcessedWithoutPolling(t *testing.T) {
	t.Parallel()

	pool, ps, cancel := newTestPoolWithWindow(congestion.Config{
		InitialWindow: 2, MinWindow: 2, MaxWindow: 2,
	})
	defer cancel()

	ps.window.TrySend("a")
	ps.window.TrySend("b")

	// Simulate: enqueue signal consumed, then OnAck + NotifyAckProcessed.
	go func() {
		time.Sleep(50 * time.Millisecond)
		ps.window.OnFail("a")
		pool.NotifyAckProcessed()
	}()

	// Wait loop WITHOUT polling fallback — only AckReady.
	start := time.Now()
	for !pool.CanSend() {
		select {
		case <-pool.AckReady():
		case <-time.After(5 * time.Second):
			t.Fatal("sender stuck — NotifyAckProcessed didn't fire")
		}
	}
	elapsed := time.Since(start)

	if elapsed > 500*time.Millisecond {
		t.Fatalf("took %v — too slow for NotifyAckProcessed alone", elapsed)
	}
	t.Logf("sender unblocked via NotifyAckProcessed (no polling) in %v", elapsed)
}

// TestSenderLoop_SignalRaceWithoutNotify demonstrates that without
// NotifyAckProcessed, the signal race causes the sender to miss the
// state change and fall through to the polling fallback (1s+ delay).
// This is a regression test — if NotifyAckProcessed were removed, this
// test would show the latency penalty.
func TestSenderLoop_SignalRaceWithoutNotify(t *testing.T) {
	t.Parallel()

	pool, ps, cancel := newTestPoolWithWindow(congestion.Config{
		InitialWindow: 2, MinWindow: 2, MaxWindow: 2,
	})
	defer cancel()

	ps.window.TrySend("a")
	ps.window.TrySend("b")

	// Simulate the race WITHOUT NotifyAckProcessed:
	// 1. Enqueue signal fires
	// 2. Sender wakes, CanSend()=false (OnAck hasn't happened)
	// 3. OnAck fires, but no new signal
	go func() {
		// Enqueue-time signal.
		pool.ackReady <- struct{}{}
		// Delay, then free capacity without signaling.
		time.Sleep(50 * time.Millisecond)
		ps.window.OnFail("a")
		// Deliberately NO NotifyAckProcessed() call.
	}()

	// Wait loop — will consume the enqueue signal, re-check CanSend()=false,
	// then fall through to the 1s polling timer.
	start := time.Now()
	attempts := 0
	for !pool.CanSend() {
		attempts++
		if attempts > 10 {
			t.Fatal("too many loop iterations")
		}
		select {
		case <-pool.AckReady():
			// Consumes the enqueue signal — but CanSend() is still false.
		case <-time.After(1 * time.Second):
			// Polling fallback rescues us.
		}
	}
	elapsed := time.Since(start)

	// Without NotifyAckProcessed, sender falls through to the 1s timer.
	// This proves the polling fallback is needed as a safety net.
	if elapsed < 500*time.Millisecond {
		// This can happen if scheduling allows OnFail to complete before
		// the sender re-checks — non-deterministic but acceptable.
		t.Logf("sender unblocked quickly (%v) — race went the other way this time", elapsed)
	} else {
		t.Logf("sender fell through to polling fallback (%v) — demonstrates the race", elapsed)
	}
	// Key assertion: must eventually unblock (not hang forever).
	if elapsed > 3*time.Second {
		t.Fatalf("took too long (%v) — polling fallback didn't work", elapsed)
	}

	_ = fmt.Sprintf("test uses fmt") // Ensure fmt import is used.
}
