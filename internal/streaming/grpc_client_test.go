package streaming

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"

	"github.com/arsac/qb-sync/internal/congestion"
	pb "github.com/arsac/qb-sync/proto"
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

// newAckSinkTestPool returns a minimal StreamPool wired up for poolAckSink
// tests with channel buffers sized for "small but non-blocking" use.
func newAckSinkTestPool(ctx context.Context) *StreamPool {
	return &StreamPool{
		ctx:          ctx,
		errs:         make(chan error, 10),
		acks:         make(chan AckEnvelope, 10),
		capacityWait: make(chan struct{}),
		logger:       testLogger,
	}
}

// chanAckSink stands in for the pool in PieceStream receive-loop tests: it
// collects acks and records the stream-end error. deliverAck gives up after
// timeout, mirroring poolAckSink's behaviour when the consumer stops draining.
type chanAckSink struct {
	acks    chan *pb.PieceAck
	ended   chan error
	timeout time.Duration
}

func newChanAckSink(ackBufSize int, timeout time.Duration) *chanAckSink {
	if timeout <= 0 {
		timeout = ackDeliverTimeout
	}
	return &chanAckSink{
		acks:    make(chan *pb.PieceAck, ackBufSize),
		ended:   make(chan error, 1),
		timeout: timeout,
	}
}

func (s *chanAckSink) deliverAck(ctx context.Context, ack *pb.PieceAck) bool {
	timer := time.NewTimer(s.timeout)
	defer timer.Stop()
	select {
	case s.acks <- ack:
		return true
	case <-ctx.Done():
		return false
	case <-timer.C:
		return false
	}
}

func (s *chanAckSink) streamEnded(err error) {
	select {
	case s.ended <- err:
	default:
	}
}

// newAdaptiveScalingTestPool builds a StreamPool configured with adaptive
// scaling enabled for handlePlateau / applyScalingDecision tests.
func newAdaptiveScalingTestPool(
	ctx context.Context, cancel context.CancelFunc, dest *GRPCDestination, streams []*PooledStream,
) *StreamPool {
	return &StreamPool{
		dest:          dest,
		ctx:           ctx,
		cancel:        cancel,
		logger:        testLogger,
		adaptive:      true,
		scaleInterval: defaultScaleInterval,
		streams:       streams,
		errs:          make(chan error, 10),
		acks:          make(chan AckEnvelope, 10),
		capacityWait:  make(chan struct{}),
		maxStreams:    MaxPoolSize,
	}
}

// newDrainTestStream builds a PooledStream wrapping a real test PieceStream,
// with a small adaptive window and the given id. Used by drainAndRemoveStream
// tests where the stream is meaningfully exercised but isolation from a real
// gRPC transport is desired.
func newDrainTestStream(ctx context.Context, id int) *PooledStream {
	stream, _ := newTestPieceStream(ctx, &mockBidiStream{})
	return &PooledStream{
		stream: stream,
		window: congestion.NewAdaptiveWindow(congestion.Config{
			InitialWindow: 10, MinWindow: 2, MaxWindow: 10,
		}),
		id: id,
	}
}

// newDrainTestPool builds a minimal StreamPool that owns a single PooledStream
// for drainAndRemoveStream tests.
func newDrainTestPool(ctx context.Context, cancel context.CancelFunc, ps *PooledStream) *StreamPool {
	return &StreamPool{
		ctx:     ctx,
		cancel:  cancel,
		streams: []*PooledStream{ps},
		logger:  testLogger,
		errs:    make(chan error, 10),
		acks:    make(chan AckEnvelope, 10),
	}
}

// newTestPieceStream creates a PieceStream with a mock stream for testing.
// The mock's ctx is set to the derived stream context so its default Recv
// unblocks when the stream is closed.
func newTestPieceStream(parentCtx context.Context, mock *mockBidiStream) (*PieceStream, *chanAckSink) {
	return newTestPieceStreamWithOptions(parentCtx, mock, DefaultAckChannelSize, 0, 0)
}

// newTestPieceStreamWithOptions creates a PieceStream with configurable sink ack
// buffer size and timeout overrides. Zero timeout means use the package-level
// const. Returns the stream and the sink its receive loop feeds.
func newTestPieceStreamWithOptions(
	parentCtx context.Context,
	mock *mockBidiStream,
	ackBufSize int,
	ackTimeout, sndTimeout time.Duration,
) (*PieceStream, *chanAckSink) {
	streamCtx, streamCancel := context.WithCancel(parentCtx)
	mock.ctx = streamCtx

	ps := &PieceStream{
		ctx:                 streamCtx,
		cancel:              streamCancel,
		stream:              mock,
		logger:              testLogger,
		done:                make(chan struct{}),
		sendTimeoutOverride: sndTimeout,
	}

	sink := newChanAckSink(ackBufSize, ackTimeout)
	go ps.receiveAcks(sink)
	return ps, sink
}

// TestSend_NormalSendSucceeds verifies that a non-blocking Send returns
// the underlying stream's result directly.
func TestSend_NormalSendSucceeds(t *testing.T) {
	t.Parallel()

	mock := &mockBidiStream{
		sendFunc: func(*pb.WritePieceRequest) error { return nil },
	}

	ps, _ := newTestPieceStream(context.Background(), mock)
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

	ps, _ := newTestPieceStream(context.Background(), mock)
	defer ps.Close()

	err := ps.Send(&pb.WritePieceRequest{TorrentHash: "test"})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected %v, got %v", expectedErr, err)
	}
}

// TestSend_SerializesConcurrentSenders verifies that concurrent Send callers
// never overlap inside stream.Send. gRPC allows only one sender goroutine per
// stream, and the sender pool routinely has several workers on one stream.
func TestSend_SerializesConcurrentSenders(t *testing.T) {
	t.Parallel()

	const (
		senders       = 8
		sendsPerActor = 25
	)

	var inFlight, maxInFlight atomic.Int32
	mock := &mockBidiStream{
		sendFunc: func(*pb.WritePieceRequest) error {
			now := inFlight.Add(1)
			for {
				peak := maxInFlight.Load()
				if now <= peak || maxInFlight.CompareAndSwap(peak, now) {
					break
				}
			}
			// Widen the overlap window so an unserialized send is caught by
			// the counter, not just by the race detector.
			time.Sleep(50 * time.Microsecond)
			inFlight.Add(-1)
			return nil
		},
	}

	ps, _ := newTestPieceStream(context.Background(), mock)
	defer ps.Close()

	var sendErrs atomic.Int32
	done := make(chan struct{})
	for range senders {
		go func() {
			defer func() { done <- struct{}{} }()
			for range sendsPerActor {
				if err := ps.Send(&pb.WritePieceRequest{TorrentHash: "test"}); err != nil {
					sendErrs.Add(1)
				}
			}
		}()
	}
	for range senders {
		<-done
	}

	if peak := maxInFlight.Load(); peak != 1 {
		t.Fatalf("concurrent stream.Send calls: peak in-flight = %d, want 1", peak)
	}
	if n := sendErrs.Load(); n != 0 {
		t.Fatalf("expected no send errors, got %d", n)
	}
}

// TestSend_AfterCloseSendReturnsError verifies that calling Send after CloseSend
// returns an error rather than pushing a piece onto a half-closed stream.
func TestSend_AfterCloseSendReturnsError(t *testing.T) {
	t.Parallel()

	mock := &mockBidiStream{
		sendFunc: func(*pb.WritePieceRequest) error { return nil },
	}

	ps, _ := newTestPieceStream(context.Background(), mock)
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
// This reproduces the production deadlock: destination stops consuming → HTTP/2 flow
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

	ps, _ := newTestPieceStream(context.Background(), mock)
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

// TestReceiveAcks_AckChannelBlockedTimeout verifies that when the sink stops
// accepting acks, receiveAcks exits, calling ps.cancel() to unblock any stuck
// Send().
//
// This covers the deadlock scenario where the ack consumer stalls → the sink
// stops accepting → receiveAcks would otherwise block and never call Recv()
// again → can't detect stream death → ps.cancel() never fires → Send() stuck
// forever.
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

	// Unbuffered sink: the first ack delivery blocks immediately.
	ps, _ := newTestPieceStreamWithOptions(context.Background(), mock, 0, testTimeout, 0)
	defer ps.Close()

	start := time.Now()
	select {
	case <-ps.done:
		// receiveAcks exited because the sink stopped accepting — correct.
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
// - Send() blocked on HTTP/2 flow control (destination not consuming)
// - receiveAcks() blocked delivering an ack (the ack processor is slow)
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

	ps, _ := newTestPieceStreamWithOptions(context.Background(), mock, DefaultAckChannelSize, 0, testTimeout)
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
	ps, sink := newTestPieceStreamWithOptions(context.Background(), mock, 5, testTimeout, 0)
	defer ps.Close()

	// Consume all acks.
	for range numAcks {
		select {
		case <-sink.acks:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for ack")
		}
	}

	// Unblock the mock and let receiveAcks finish naturally.
	close(received)
	<-ps.done

	// If the delivery timeout fired spuriously, the stream would have ended with
	// no error reported. Check that it ended on the Recv error instead.
	select {
	case err := <-sink.ended:
		if err == nil || err.Error() != "done" {
			t.Fatalf("expected 'done' error from Recv, got %v", err)
		}
	default:
		t.Fatal("stream ended without reporting the Recv error to the sink")
	}
}

// newAckSinkFor builds a poolAckSink for a bare PooledStream with the given id.
func newAckSinkFor(pool *StreamPool, id int) *poolAckSink {
	return newPoolAckSink(pool, &PooledStream{id: id})
}

// TestDeliverAck_StreamCancelReleasesBlockedDelivery verifies that a delivery
// parked on a full aggregated channel is released by the stream's own context,
// not only by the pool's. drainAndRemoveStream closes a scaled-down stream
// while the pool keeps running, and Close waits on the receive loop - without
// the stream-context escape that wait would park for the whole delivery
// timeout.
func TestDeliverAck_StreamCancelReleasesBlockedDelivery(t *testing.T) {
	t.Parallel()

	pool := newAckSinkTestPool(t.Context())
	pool.ackDeliveryTimeoutOverride = time.Hour // Only a cancel can release this.
	for len(pool.acks) < cap(pool.acks) {
		pool.acks <- AckEnvelope{}
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	sink := newAckSinkFor(pool, 5)

	returned := make(chan bool, 1)
	go func() { returned <- sink.deliverAck(streamCtx, &pb.PieceAck{}) }()

	select {
	case <-returned:
		t.Fatal("deliverAck returned before the stream was cancelled")
	case <-time.After(50 * time.Millisecond):
	}

	streamCancel()
	select {
	case ok := <-returned:
		if ok {
			t.Fatal("deliverAck reported success without delivering the ack")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("deliverAck stayed parked after the stream was cancelled")
	}
}

// TestStreamEnded_NotifiesPoolOnSilentStreamDeath verifies that when a stream's
// receive loop exits without an error of its own (e.g. a send timeout cancelled
// the context), the sink publishes a synthetic error to pool.errs so the ack
// processor can trigger reconnection.
//
// Without it the stream dies silently and the ack processor never learns, which
// is a permanent sender deadlock.
func TestStreamEnded_NotifiesPoolOnSilentStreamDeath(t *testing.T) {
	t.Parallel()

	pool := newAckSinkTestPool(t.Context())
	newAckSinkFor(pool, 42).streamEnded(nil)

	select {
	case err := <-pool.errs:
		if err == nil {
			t.Fatal("expected non-nil synthetic error")
		}
		t.Logf("received synthetic error: %v", err)
	default:
		t.Fatal("stream died silently — no error published to the pool")
	}
}

// TestStreamEnded_NoSpuriousErrorOnCleanShutdown verifies that when the pool
// context is cancelled (clean shutdown via pool.Close), a stream ending without
// an error of its own does NOT produce a synthetic error. Only unexpected
// stream deaths should generate errors.
func TestStreamEnded_NoSpuriousErrorOnCleanShutdown(t *testing.T) {
	t.Parallel()

	poolCtx, poolCancel := context.WithCancel(context.Background())
	poolCancel()

	pool := newAckSinkTestPool(poolCtx)
	newAckSinkFor(pool, 7).streamEnded(nil)

	select {
	case err := <-pool.errs:
		t.Fatalf("unexpected error on clean shutdown: %v", err)
	default:
		// No error — correct.
	}
}

// TestStreamEnded_ForwardsRealStreamError verifies that a stream error is
// published verbatim rather than replaced by the synthetic one, and that it is
// published even while the pool context is cancelled — a real failure racing
// shutdown still has to reach the ack processor.
func TestStreamEnded_ForwardsRealStreamError(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name       string
		cancelPool bool
	}{
		{name: "pool_running"},
		{name: "pool_closing", cancelPool: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			poolCtx, poolCancel := context.WithCancel(context.Background())
			defer poolCancel()
			if tc.cancelPool {
				poolCancel()
			}

			pool := newAckSinkTestPool(poolCtx)
			newAckSinkFor(pool, 3).streamEnded(errors.New("stream reset by peer"))

			select {
			case err := <-pool.errs:
				if err == nil || err.Error() != "stream reset by peer" {
					t.Fatalf("expected 'stream reset by peer', got %v", err)
				}
			default:
				t.Fatal("stream error was dropped instead of published to the pool")
			}
		})
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
			done: make(chan struct{}),
		},
		window: congestion.NewAdaptiveWindow(windowCfg),
		id:     0,
	}
	pool := &StreamPool{
		ctx:          ctx,
		cancel:       cancel,
		errs:         make(chan error, 10),
		acks:         make(chan AckEnvelope, 10),
		capacityWait: make(chan struct{}),
		logger:       testLogger,
		streams:      []*PooledStream{ps},
	}
	return pool, ps, cancel
}

// TestCapacityWait_ClosesTheChannelHandedOutBefore pins the close-and-replace
// contract awaitCapacity relies on: the channel a sender took before testing
// CanSend is the one a later release closes, so a release published between
// that test and the sender's select cannot be missed.
func TestCapacityWait_ClosesTheChannelHandedOutBefore(t *testing.T) {
	t.Parallel()

	pool, _, cancel := newTestPoolWithWindow(congestion.DefaultConfig())
	defer cancel()

	held := pool.CapacityWait()
	select {
	case <-held:
		t.Fatal("capacity channel was already closed before any release")
	default:
	}

	pool.publishCapacity()

	select {
	case <-held:
	default:
		t.Fatal("release did not close the channel the waiter was already holding")
	}

	select {
	case <-pool.CapacityWait():
		t.Fatal("replacement channel is closed, so the next wait would spin")
	default:
	}
}

// TestAwaitCapacity_EveryReleasePathWakesAParkedSender covers each pool
// operation that frees a congestion-window slot. Nothing polls behind
// awaitCapacity, so a path that releases capacity without publishing leaves
// the sender parked forever - which is what each subtest fails on.
func TestAwaitCapacity_EveryReleasePathWakesAParkedSender(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		release func(pool *StreamPool, ps *PooledStream)
	}{
		{"ack", func(pool *StreamPool, ps *PooledStream) {
			pool.AckPiece(ps, congestion.PieceKey{Hash: "hash", Index: 0})
		}},
		{"fail", func(pool *StreamPool, ps *PooledStream) {
			pool.FailPiece(ps, congestion.PieceKey{Hash: "hash", Index: 0})
		}},
		{"clear inflight", func(pool *StreamPool, _ *PooledStream) { pool.ClearAllInflight() }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			pool, ps, cancel := newTestPoolWithWindow(congestion.Config{
				InitialWindow: 2, MinWindow: 2, MaxWindow: 2,
			})
			defer cancel()

			ps.window.TrySend(congestion.PieceKey{Hash: "hash", Index: 0})
			ps.window.TrySend(congestion.PieceKey{Hash: "hash", Index: 1})
			if pool.CanSend() {
				t.Fatal("window should be full before the sender parks")
			}

			q := &BidiQueue{logger: testLogger}
			stopSender := make(chan struct{})
			defer close(stopSender)

			returned := make(chan bool, 1)
			go func() {
				returned <- q.awaitCapacity(context.Background(), pool, stopSender)
			}()

			select {
			case <-returned:
				t.Fatal("awaitCapacity returned while the window was still full")
			case <-time.After(100 * time.Millisecond):
			}

			tc.release(pool, ps)

			select {
			case ok := <-returned:
				if !ok {
					t.Fatal("awaitCapacity reported worker exit, want capacity available")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("awaitCapacity stayed parked: this release path published no capacity signal")
			}
		})
	}
}

// TestStreamEnded_SilentExitOnRemovedFlag verifies that a stream the pool
// deliberately drained out reports nothing, even though closing it produces a
// context error the receive loop hands to the sink.
func TestStreamEnded_SilentExitOnRemovedFlag(t *testing.T) {
	t.Parallel()

	pool := newAckSinkTestPool(t.Context())
	ps := &PooledStream{id: 99}
	ps.removed.Store(true) // Mark as intentionally removed

	newPoolAckSink(pool, ps).streamEnded(context.Canceled)

	select {
	case err := <-pool.errs:
		t.Fatalf("expected no error for removed stream, got: %v", err)
	default:
		// No error — correct.
	}
}

// TestCanSend_SkipsDrainingStreams verifies that CanSend returns false
// when all non-draining streams are full, even if draining streams have capacity.
func TestCanSend_SkipsDrainingStreams(t *testing.T) {
	t.Parallel()

	pool, ps, cancel := newTestPoolWithWindow(congestion.Config{
		InitialWindow: 10, MinWindow: 2, MaxWindow: 10,
	})
	defer cancel()

	// ps has capacity — CanSend should be true
	if !pool.CanSend() {
		t.Fatal("expected CanSend()=true with available stream")
	}

	// Mark the only stream as draining
	ps.draining.Store(true)

	// Even though ps.window.CanSend() is true, pool.CanSend() should skip it
	if pool.CanSend() {
		t.Fatal("expected CanSend()=false when only stream is draining")
	}
}

// TestDrainAndRemoveStream_HappyPath verifies that drainAndRemoveStream
// waits for in-flight pieces to drain, then removes the stream from the pool.
func TestDrainAndRemoveStream_HappyPath(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ps := newDrainTestStream(ctx, 1)

	// Put one piece in-flight
	ps.window.TrySend(congestion.PieceKey{Hash: "hash", Index: 0})
	if ps.window.InFlight() != 1 {
		t.Fatalf("expected 1 in-flight, got %d", ps.window.InFlight())
	}

	pool := newDrainTestPool(ctx, cancel, ps)

	// Start drain in background
	done := make(chan struct{})
	go func() {
		pool.drainAndRemoveStream(ps)
		close(done)
	}()

	// Verify draining flag was set
	time.Sleep(50 * time.Millisecond)
	if !ps.draining.Load() {
		t.Fatal("expected draining flag to be set")
	}

	// Simulate in-flight piece completing
	ps.window.OnFail(congestion.PieceKey{Hash: "hash", Index: 0})

	// Drain should complete
	select {
	case <-done:
		// Success
	case <-time.After(5 * time.Second):
		t.Fatal("drainAndRemoveStream didn't complete after in-flight drained")
	}

	// Stream should be removed from pool
	pool.mu.RLock()
	if len(pool.streams) != 0 {
		t.Fatalf("expected 0 streams after drain, got %d", len(pool.streams))
	}
	pool.mu.RUnlock()

	// Removed flag should be set
	if !ps.removed.Load() {
		t.Fatal("expected removed flag to be set")
	}
}

// TestDrainAndRemoveStream_Timeout verifies that drainAndRemoveStream removes
// the stream after the drain timeout even if in-flight pieces haven't completed.
func TestDrainAndRemoveStream_Timeout(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ps := newDrainTestStream(ctx, 2)

	// Put pieces in-flight that will never complete
	ps.window.TrySend(congestion.PieceKey{Hash: "hash", Index: 0})
	ps.window.TrySend(congestion.PieceKey{Hash: "hash", Index: 1})

	pool := newDrainTestPool(ctx, cancel, ps)

	// streamDrainTimeout (30s) is too long to wait for a real timeout in
	// unit tests, so this test exercises the early-exit-on-context-cancel
	// path instead. The happy path covers the in-flight=0 path.
	done := make(chan struct{})
	go func() {
		pool.drainAndRemoveStream(ps)
		close(done)
	}()

	// Verify draining flag set immediately
	time.Sleep(50 * time.Millisecond)
	if !ps.draining.Load() {
		t.Fatal("expected draining flag to be set")
	}

	// Cancel context to simulate pool shutdown (triggers early exit)
	cancel()

	select {
	case <-done:
		// drainAndRemoveStream should exit on context cancel
	case <-time.After(5 * time.Second):
		t.Fatal("drainAndRemoveStream didn't exit on context cancel")
	}
}

// TestDrainAndRemoveStream_AccumulatesBytesSent verifies that bytes from
// a drained stream are accumulated into removedBytesSent.
func TestDrainAndRemoveStream_AccumulatesBytesSent(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ps := newDrainTestStream(ctx, 3)
	ps.bytesSent.Store(12345)

	pool := newDrainTestPool(ctx, cancel, ps)

	// No in-flight pieces — drain completes immediately
	pool.drainAndRemoveStream(ps)

	pool.mu.RLock()
	if pool.removedBytesSent != 12345 {
		t.Fatalf("removedBytesSent = %d, want 12345", pool.removedBytesSent)
	}
	pool.mu.RUnlock()
}

// TestHandlePlateau_TriggersConnectionAdd verifies that when stream scaling
// plateaus (3 consecutive), handlePlateau adds a new TCP connection.
func TestHandlePlateau_TriggersConnectionAdd(t *testing.T) {
	t.Parallel()

	addr := startTestGRPCServerAddr(t, func(stream pb.QBSyncService_StreamPiecesBidiServer) error {
		<-stream.Context().Done()
		return stream.Context().Err()
	})

	dest, err := NewGRPCDestination(addr, 1, 4)
	if err != nil {
		t.Fatalf("NewGRPCDestination: %v", err)
	}
	defer dest.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool := newAdaptiveScalingTestPool(ctx, cancel, dest, make([]*PooledStream, 0))

	if dest.ConnectionCount() != 1 {
		t.Fatalf("initial connections = %d, want 1", dest.ConnectionCount())
	}

	// Simulate 3 consecutive plateaus
	pool.mu.Lock()
	pool.plateauCount = defaultPlateauCount - 1 // One more will trigger
	pool.handlePlateau(100.0)
	pool.mu.Unlock()

	if dest.ConnectionCount() != 2 {
		t.Fatalf("expected 2 connections after plateau, got %d", dest.ConnectionCount())
	}

	// Verify the connection was added on trial, not kept unconditionally
	pool.mu.Lock()
	if pool.probe == nil {
		t.Fatal("a probe should be armed after connection add")
	}
	if pool.probe.unit != unitConnection {
		t.Errorf("probe unit = %q, want %q", pool.probe.unit, unitConnection)
	}
	if pool.probe.baseline != 100.0 {
		t.Errorf("probe baseline = %f, want 100.0", pool.probe.baseline)
	}
	if pool.plateauCount != 0 {
		t.Errorf("plateauCount should be reset to 0, got %d", pool.plateauCount)
	}
	pool.mu.Unlock()
}

// TestProbeConnection_PinsAStreamToTheNewConnection verifies that a probed
// connection arrives with a stream on it. Existing streams keep the connection
// they were opened on, so a bare connection would carry no traffic at all and
// its probe could only ever measure a flat interval and undo itself.
func TestProbeConnection_PinsAStreamToTheNewConnection(t *testing.T) {
	t.Parallel()

	newProbePool := func(t *testing.T) (*GRPCDestination, *StreamPool) {
		t.Helper()
		addr := startTestGRPCServerAddr(t, func(stream pb.QBSyncService_StreamPiecesBidiServer) error {
			<-stream.Context().Done()
			return stream.Context().Err()
		})

		dest, err := NewGRPCDestination(addr, 1, 4)
		if err != nil {
			t.Fatalf("NewGRPCDestination: %v", err)
		}
		t.Cleanup(func() { _ = dest.Close() })

		ctx, cancel := context.WithCancel(context.Background())
		pool := newAdaptiveScalingTestPool(ctx, cancel, dest, newStubPooledStreams(MinPoolSize))

		// Cleanups run LIFO, so the join must be registered before the cancel
		// that releases the probed stream's receive loop.
		t.Cleanup(pool.wg.Wait)
		t.Cleanup(cancel)
		return dest, pool
	}

	t.Run("stream lands on the added connection", func(t *testing.T) {
		t.Parallel()
		dest, pool := newProbePool(t)

		pool.mu.Lock()
		added := pool.probeConnection(100.0)
		streams := slices.Clone(pool.streams)
		pool.mu.Unlock()

		if !added {
			t.Fatal("probeConnection reported no add")
		}
		if dest.ConnectionCount() != 2 {
			t.Fatalf("connections = %d, want 2", dest.ConnectionCount())
		}
		if len(streams) != MinPoolSize+1 {
			t.Fatalf("streams = %d, want %d", len(streams), MinPoolSize+1)
		}
		// Round-robin would hand out connection 0 here (streamIdx is untouched
		// by the stub streams), so index 1 pins that the stream is pinned.
		if got := streams[len(streams)-1].stream.connIdx; got != 1 {
			t.Fatalf("new stream connIdx = %d, want 1 (the connection just added)", got)
		}
	})

	t.Run("declines without touching the destination when no stream slot is free", func(t *testing.T) {
		t.Parallel()
		dest, pool := newProbePool(t)

		// The stream-open failure would revert the connection anyway, so what
		// the up-front check buys is that a routine at-max-streams plateau
		// neither dials the destination nor reports a stream-open error.
		var logs bytes.Buffer
		pool.logger = slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn}))

		pool.mu.Lock()
		pool.maxStreams = len(pool.streams)
		added := pool.probeConnection(100.0)
		probe := pool.probe
		streamCount := len(pool.streams)
		pool.mu.Unlock()

		if added {
			t.Fatal("probeConnection added a connection with no stream slot to drive it")
		}
		if probe != nil {
			t.Fatal("no probe should be armed when nothing was added")
		}
		if dest.ConnectionCount() != 1 {
			t.Fatalf("connections = %d, want 1", dest.ConnectionCount())
		}
		if streamCount != MinPoolSize {
			t.Fatalf("streams = %d, want %d", streamCount, MinPoolSize)
		}
		if logs.Len() != 0 {
			t.Fatalf("a full stream pool is not an error, but probeConnection logged: %s", logs.String())
		}
	})

	t.Run("undoing the probe sheds the stream with the connection", func(t *testing.T) {
		t.Parallel()
		dest, pool := newProbePool(t)

		pool.mu.Lock()
		if !pool.probeConnection(100.0) {
			pool.mu.Unlock()
			t.Fatal("probeConnection reported no add")
		}
		pool.probe.addedAt = time.Now().Add(-(probeSettleIntervals + 1) * defaultScaleInterval)
		// Flat throughput: the add did not pay for itself.
		pool.evaluateProbe(100.0)
		pool.mu.Unlock()

		// The drain runs asynchronously; poll the observables rather than
		// joining pool.wg, which also holds every stream's receive loop.
		deadline := time.Now().Add(5 * time.Second)
		for dest.ConnectionCount() != 1 || pool.StreamCount() != MinPoolSize {
			if time.Now().After(deadline) {
				t.Fatalf("after undo: connections = %d (want 1), streams = %d (want %d)",
					dest.ConnectionCount(), pool.StreamCount(), MinPoolSize)
			}
			time.Sleep(10 * time.Millisecond)
		}
	})
}

// newStubPooledStreams creates n PooledStreams with stub PieceStreams
// assigned to connection index 0. Useful for tests that need non-nil
// streams but don't exercise actual stream I/O.
func newStubPooledStreams(n int) []*PooledStream {
	streams := make([]*PooledStream, n)
	for i := range n {
		streams[i] = &PooledStream{
			stream: &PieceStream{
				connIdx: 0,
				done:    make(chan struct{}),
			},
			window: congestion.NewAdaptiveWindow(congestion.DefaultConfig()),
			id:     i,
		}
	}
	return streams
}

// measureInterval drives one throughput measurement: it attributes totalBytes
// cumulative bytes to the pool's first stream and backdates the measurement
// baseline by two seconds, so updateThroughput sees a full interval.
func measureInterval(pool *StreamPool, totalBytes int64) {
	pool.mu.Lock()
	pool.streams[0].bytesSent.Store(totalBytes)
	pool.lastMeasureTime = time.Now().Add(-2 * time.Second)
	pool.mu.Unlock()

	pool.updateThroughput()
}

// TestUpdateThroughput_ScalesOnMeasuredChange pins the wiring between the
// throughput measurement and the scaling decision: the decision has to compare
// a new measurement against the PREVIOUS interval's, so a caller that publishes
// the new value as lastThroughput first leaves every change ratio at zero and
// the pool permanently stuck on the plateau path. The applyScalingDecision
// tests seed lastThroughput by hand and so cannot see that.
func TestUpdateThroughput_ScalesOnMeasuredChange(t *testing.T) {
	t.Parallel()

	addr := startTestGRPCServerAddr(t, func(stream pb.QBSyncService_StreamPiecesBidiServer) error {
		<-stream.Context().Done()
		return stream.Context().Err()
	})

	t.Run("increase scales up", func(t *testing.T) {
		t.Parallel()

		dest, err := NewGRPCDestination(addr, 1, 4)
		if err != nil {
			t.Fatalf("NewGRPCDestination: %v", err)
		}
		defer dest.Close()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		pool := newAdaptiveScalingTestPool(ctx, cancel, dest, newStubPooledStreams(MinPoolSize))

		measureInterval(pool, 10<<20) // baseline: 5 MB/s
		measureInterval(pool, 30<<20) // 10 MB/s, +100%

		pool.mu.Lock()
		defer pool.mu.Unlock()
		if got := len(pool.streams); got != MinPoolSize+1 {
			t.Errorf("streams = %d, want %d: a doubling of throughput did not scale the pool up", got, MinPoolSize+1)
		}
	})

	t.Run("decrease scales down", func(t *testing.T) {
		t.Parallel()

		dest, err := NewGRPCDestination(addr, 1, 4)
		if err != nil {
			t.Fatalf("NewGRPCDestination: %v", err)
		}
		defer dest.Close()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		streams := make([]*PooledStream, MinPoolSize+1)
		for i := range streams {
			streams[i] = newDrainTestStream(ctx, i)
		}
		pool := newAdaptiveScalingTestPool(ctx, cancel, dest, streams)

		measureInterval(pool, 30<<20) // baseline: 15 MB/s
		measureInterval(pool, 40<<20) // 5 MB/s, -67%

		pool.wg.Wait() // drainAndRemoveStream

		pool.mu.Lock()
		defer pool.mu.Unlock()
		if got := len(pool.streams); got != MinPoolSize {
			t.Errorf("streams = %d, want %d: a collapse in throughput did not scale the pool down", got, MinPoolSize)
		}
		if !pool.scalingPaused {
			t.Error("scaling should be paused after a scale-down")
		}
	})
}

// TestScaleUp_ProbedStreamMustPayForItself pins the mechanism that keeps the
// pool from ratcheting: a stream added because throughput rose is on trial, and
// the interval after it settles decides whether it stays. Without the trial the
// pool adds above +5% but only sheds below -15%, so link noise alone walks it
// to maxStreams regardless of what the added streams carry.
func TestScaleUp_ProbedStreamMustPayForItself(t *testing.T) {
	t.Parallel()

	addr := startTestGRPCServerAddr(t, func(stream pb.QBSyncService_StreamPiecesBidiServer) error {
		<-stream.Context().Done()
		return stream.Context().Err()
	})

	// scaleUpFromBaseline drives the two measurement intervals that add a stream
	// and arm its probe, and returns the pool sitting on that pending verdict.
	scaleUpFromBaseline := func(t *testing.T) *StreamPool {
		t.Helper()

		dest, err := NewGRPCDestination(addr, 1, 4)
		if err != nil {
			t.Fatalf("NewGRPCDestination: %v", err)
		}
		t.Cleanup(func() { _ = dest.Close() })

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		pool := newAdaptiveScalingTestPool(ctx, cancel, dest, newStubPooledStreams(MinPoolSize))

		measureInterval(pool, 10<<20) // baseline: 5 MB/s
		measureInterval(pool, 30<<20) // 10 MB/s, +100% -> probe a stream

		pool.mu.Lock()
		defer pool.mu.Unlock()
		if len(pool.streams) != MinPoolSize+1 {
			t.Fatalf("streams = %d, want %d after scale-up", len(pool.streams), MinPoolSize+1)
		}
		if pool.probe == nil || pool.probe.unit != unitStream {
			t.Fatalf("probe = %+v, want a stream probe", pool.probe)
		}
		return pool
	}

	// settleProbe backdates the pending probe past its settle window so the next
	// measurement delivers the verdict.
	settleProbe := func(pool *StreamPool) {
		pool.mu.Lock()
		defer pool.mu.Unlock()
		pool.probe.addedAt = time.Now().Add(-(probeSettleIntervals + 1) * defaultScaleInterval)
	}

	t.Run("verdict waits out the settle window", func(t *testing.T) {
		t.Parallel()

		pool := scaleUpFromBaseline(t)

		// A doubling one interval in is not yet attributable to the new stream,
		// and acting on it would add a second stream whose contribution then
		// pollutes the first one's verdict.
		measureInterval(pool, 70<<20)

		pool.mu.Lock()
		defer pool.mu.Unlock()
		if got := len(pool.streams); got != MinPoolSize+1 {
			t.Errorf("streams = %d, want %d: the pool scaled while a probe was pending", got, MinPoolSize+1)
		}
		if pool.probe == nil {
			t.Error("probe should still be pending before its settle window elapses")
		}
	})

	t.Run("unhelpful add is undone", func(t *testing.T) {
		t.Parallel()

		pool := scaleUpFromBaseline(t)
		settleProbe(pool)

		measureInterval(pool, 50<<20) // still 10 MB/s: the added stream carried nothing

		// drainAndRemoveStream runs as a goroutine. Poll rather than joining
		// pool.wg, which also holds the added stream's receive loop and so would
		// hang instead of failing if the stream is never drained.
		deadline := time.Now().Add(2 * time.Second)
		for pool.StreamCount() > MinPoolSize && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}

		pool.mu.Lock()
		defer pool.mu.Unlock()
		if got := len(pool.streams); got != MinPoolSize {
			t.Errorf("streams = %d, want %d: a stream that added no throughput was kept", got, MinPoolSize)
		}
		if pool.probe != nil {
			t.Error("probe should be cleared once its verdict is in")
		}
		if !pool.scalingPaused {
			t.Error("scaling should be paused after an add is undone")
		}
	})

	t.Run("paying add is kept and earns another", func(t *testing.T) {
		t.Parallel()

		pool := scaleUpFromBaseline(t)
		settleProbe(pool)

		measureInterval(pool, 70<<20) // 20 MB/s: the added stream doubled throughput

		pool.mu.Lock()
		defer pool.mu.Unlock()
		if got := len(pool.streams); got != MinPoolSize+2 {
			t.Errorf("streams = %d, want %d: an effective add did not earn a staircase step", got, MinPoolSize+2)
		}
		if pool.probe == nil || pool.probe.unit != unitStream {
			t.Fatalf("probe = %+v, want a fresh stream probe", pool.probe)
		}
		if pool.scalingPaused {
			t.Error("scaling should not be paused while the staircase is still climbing")
		}
	})
}

// TestHandlePlateau_FullSaturationPauses verifies that when at max connections
// and max streams, handlePlateau pauses scaling unconditionally.
func TestHandlePlateau_FullSaturationPauses(t *testing.T) {
	t.Parallel()

	addr := startTestGRPCServerAddr(t, func(stream pb.QBSyncService_StreamPiecesBidiServer) error {
		<-stream.Context().Done()
		return stream.Context().Err()
	})

	dest, err := NewGRPCDestination(addr, 2, 2) // Already at max
	if err != nil {
		t.Fatalf("NewGRPCDestination: %v", err)
	}
	defer dest.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool := newAdaptiveScalingTestPool(ctx, cancel, dest, newStubPooledStreams(MinPoolSize))

	pool.mu.Lock()
	pool.plateauCount = defaultPlateauCount // Trigger plateau handling
	pool.handlePlateau(100.0)

	if !pool.scalingPaused {
		t.Error("scaling should be paused at full saturation")
	}
	pool.mu.Unlock()
}

// TestDiminishingReturns_TriggersScaleDown verifies that when a connection
// add yields < 5% throughput improvement, scaling is paused and scale-down
// is attempted.
func TestDiminishingReturns_TriggersScaleDown(t *testing.T) {
	t.Parallel()

	addr := startTestGRPCServerAddr(t, func(stream pb.QBSyncService_StreamPiecesBidiServer) error {
		<-stream.Context().Done()
		return stream.Context().Err()
	})

	dest, err := NewGRPCDestination(addr, 1, 4)
	if err != nil {
		t.Fatalf("NewGRPCDestination: %v", err)
	}
	defer dest.Close()

	// Add a connection so we can observe scale-down attempt
	if _, addErr := dest.AddConnection(); addErr != nil {
		t.Fatalf("AddConnection: %v", addErr)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create stub streams on connection 0 (not connection 1 which will be removed)
	pool := newAdaptiveScalingTestPool(ctx, cancel, dest, newStubPooledStreams(MinPoolSize))

	pool.mu.Lock()

	// Simulate: connection was added on trial, barely any improvement since
	pool.probe = &scaleProbe{
		unit:     unitConnection,
		baseline: 100.0,
		addedAt:  time.Now().Add(-3 * defaultScaleInterval), // Past check window
	}

	// Current throughput: 102 MB/s, only 2% above the probe's baseline,
	// below the 5% threshold that would call the connection add effective.
	pool.applyScalingDecision(102.0, 102.0)

	if !pool.scalingPaused {
		t.Error("scaling should be paused after diminishing returns")
	}
	if pool.probe != nil {
		t.Error("probe should be cleared")
	}
	pool.mu.Unlock()

	// Wait for the tryConnectionScaleDown goroutine to complete
	pool.wg.Wait()

	// Connection should have been removed (no streams were on conn 1)
	if dest.ConnectionCount() != 1 {
		t.Fatalf("expected 1 connection after scale-down, got %d", dest.ConnectionCount())
	}
}

// TestDiminishingReturns_GoodImprovement verifies that a connection add
// yielding >= 5% improvement is kept and earns another step of the staircase:
// a second connection, itself on trial against the improved baseline.
func TestDiminishingReturns_GoodImprovement(t *testing.T) {
	t.Parallel()

	addr := startTestGRPCServerAddr(t, func(stream pb.QBSyncService_StreamPiecesBidiServer) error {
		<-stream.Context().Done()
		return stream.Context().Err()
	})

	dest, err := NewGRPCDestination(addr, 1, 4)
	if err != nil {
		t.Fatalf("NewGRPCDestination: %v", err)
	}
	defer dest.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool := newAdaptiveScalingTestPool(ctx, cancel, dest, newStubPooledStreams(MinPoolSize+1))

	pool.mu.Lock()

	// Simulate: connection added on trial, 10% improvement (above 5% threshold)
	pool.probe = &scaleProbe{
		unit:     unitConnection,
		baseline: 100.0,
		addedAt:  time.Now().Add(-3 * defaultScaleInterval),
	}

	pool.applyScalingDecision(110.0, 110.0)

	if pool.scalingPaused {
		t.Error("scaling should NOT be paused after good improvement")
	}
	if pool.probe == nil {
		t.Fatal("an effective add should earn another probe, not clear scaling state")
	}
	if pool.probe.unit != unitConnection {
		t.Errorf("next probe unit = %q, want %q", pool.probe.unit, unitConnection)
	}
	if pool.probe.baseline != 110.0 {
		t.Errorf("next probe baseline = %f, want 110.0 (the improved throughput)", pool.probe.baseline)
	}
	pool.mu.Unlock()

	if dest.ConnectionCount() != 2 {
		t.Fatalf("connections = %d, want 2 after the staircase step", dest.ConnectionCount())
	}
}

// stubClient is a minimal QBSyncServiceClient for testing round-robin distribution.
// Each instance has a unique id for identity checks.
type stubClient struct {
	pb.QBSyncServiceClient

	id int
}

// TestStreamConnIdx_RoundRobin verifies that streamConnIdx distributes calls
// evenly across all connections using atomic round-robin.
func TestStreamConnIdx_RoundRobin(t *testing.T) {
	t.Parallel()

	clients := []pb.QBSyncServiceClient{
		&stubClient{id: 0},
		&stubClient{id: 1},
		&stubClient{id: 2},
	}

	d := &GRPCDestination{
		clients: clients,
	}

	counts := make([]int, 3)
	for range 9 {
		idx := d.streamConnIdx()
		counts[idx]++
	}

	for i, count := range counts {
		if count != 3 {
			t.Errorf("connection %d selected %d times, want 3", i, count)
		}
	}
}

// TestStreamConnIdx_SingleConn verifies the fast-path: with 1 connection,
// streamConnIdx always returns 0 without touching the atomic.
func TestStreamConnIdx_SingleConn(t *testing.T) {
	t.Parallel()

	d := &GRPCDestination{
		clients: []pb.QBSyncServiceClient{&stubClient{id: 0}},
	}

	for range 10 {
		idx := d.streamConnIdx()
		if idx != 0 {
			t.Fatalf("expected index 0 for single-conn fast path, got %d", idx)
		}
	}
}

// TestClient_AlwaysReturnsFirst verifies that client() always returns
// the first connection (for unary RPCs).
func TestClient_AlwaysReturnsFirst(t *testing.T) {
	t.Parallel()

	clients := []pb.QBSyncServiceClient{
		&stubClient{id: 0},
		&stubClient{id: 1},
		&stubClient{id: 2},
	}

	d := &GRPCDestination{
		clients: clients,
	}

	for range 10 {
		c := d.client()
		sc := c.(*stubClient)
		if sc.id != 0 {
			t.Fatalf("client() returned client %d, want 0", sc.id)
		}
	}
}

// TestStreamConnIdx_RoundRobin_Concurrent verifies that round-robin is safe
// under concurrent access (no races, even distribution).
func TestStreamConnIdx_RoundRobin_Concurrent(t *testing.T) {
	t.Parallel()

	const numClients = 3
	const numGoroutines = 6
	const callsPerGoroutine = 100

	clients := make([]pb.QBSyncServiceClient, numClients)
	for i := range numClients {
		clients[i] = &stubClient{id: i}
	}

	d := &GRPCDestination{
		clients: clients,
	}

	var counts [numClients]atomic.Int64
	done := make(chan struct{})

	for range numGoroutines {
		go func() {
			for range callsPerGoroutine {
				idx := d.streamConnIdx()
				counts[idx].Add(1)
			}
			done <- struct{}{}
		}()
	}

	for range numGoroutines {
		<-done
	}

	total := numGoroutines * callsPerGoroutine
	expected := total / numClients
	for i := range numClients {
		got := int(counts[i].Load())
		if got != expected {
			t.Errorf("connection %d: got %d calls, want %d", i, got, expected)
		}
	}
}

// TestClose_Idempotent verifies that Close() can be called multiple times
// safely and returns the same error each time via [sync.Once].
func TestClose_Idempotent(t *testing.T) {
	t.Parallel()

	d, err := NewGRPCDestination("localhost:0", 2, 2)
	if err != nil {
		t.Fatalf("NewGRPCDestination: %v", err)
	}

	err1 := d.Close()
	err2 := d.Close()

	if err1 != nil {
		t.Fatalf("first Close: %v", err1)
	}
	if !errors.Is(err1, err2) {
		t.Fatalf("Close not idempotent: first=%v, second=%v", err1, err2)
	}
}

func TestErrFinalizeBusyIsDistinct(t *testing.T) {
	wrapped := fmt.Errorf("%w: finalization queue timeout", ErrFinalizeBusy)
	if !errors.Is(wrapped, ErrFinalizeBusy) {
		t.Fatal("wrapped busy error must match ErrFinalizeBusy")
	}
	if errors.Is(wrapped, ErrFinalizeVerifying) || errors.Is(wrapped, ErrFinalizeIncomplete) {
		t.Fatal("busy must not match other finalize sentinels")
	}
}
