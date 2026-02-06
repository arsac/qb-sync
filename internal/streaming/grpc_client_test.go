package streaming

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

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

// newTestPieceStream creates a PieceStream with a mock stream for testing.
// The mock's ctx is set to the derived stream context so its default Recv
// unblocks when the stream is closed.
func newTestPieceStream(
	parentCtx context.Context,
	mock *mockBidiStream,
	sendTimeout time.Duration,
) *PieceStream {
	streamCtx, streamCancel := context.WithCancel(parentCtx)
	mock.ctx = streamCtx

	ps := &PieceStream{
		ctx:         streamCtx,
		cancel:      streamCancel,
		stream:      mock,
		logger:      slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
		sendTimeout: sendTimeout,
		acks:        make(chan *pb.PieceAck, DefaultAckChannelSize),
		ackReady:    make(chan struct{}, 1),
		done:        make(chan struct{}),
		errors:      make(chan error, 1),
	}

	go ps.receiveAcks()
	return ps
}

// TestSend_NormalSendSucceeds verifies that a non-blocking Send returns
// the underlying stream's result directly.
func TestSend_NormalSendSucceeds(t *testing.T) {
	t.Parallel()

	mock := &mockBidiStream{
		sendFunc: func(*pb.WritePieceRequest) error { return nil },
	}

	ps := newTestPieceStream(context.Background(), mock, DefaultSendTimeout)
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

	ps := newTestPieceStream(context.Background(), mock, DefaultSendTimeout)
	defer ps.Close()

	err := ps.Send(&pb.WritePieceRequest{TorrentHash: "test"})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected %v, got %v", expectedErr, err)
	}
}

// TestSend_BlockingStreamTimesOut verifies that Send returns ErrSendTimeout
// when the underlying stream.Send blocks longer than sendTimeout.
//
// This is the primary safety net for the production deadlock: when cold stops
// consuming data, HTTP/2 flow control fills and Send blocks indefinitely.
// The timeout breaks the deadlock by returning an error and cancelling the
// stream context.
func TestSend_BlockingStreamTimesOut(t *testing.T) {
	t.Parallel()

	// blockForever simulates HTTP/2 flow control blocking Send.
	// Closed in defer to let the leaked goroutine exit after the test.
	blockForever := make(chan struct{})
	defer close(blockForever)

	mock := &mockBidiStream{
		sendFunc: func(*pb.WritePieceRequest) error {
			<-blockForever
			return nil
		},
	}

	ps := newTestPieceStream(context.Background(), mock, 50*time.Millisecond)
	defer ps.Close()

	start := time.Now()
	err := ps.Send(&pb.WritePieceRequest{TorrentHash: "test"})
	elapsed := time.Since(start)

	if !errors.Is(err, ErrSendTimeout) {
		t.Fatalf("expected ErrSendTimeout, got %v", err)
	}

	if elapsed > 1*time.Second {
		t.Fatalf("Send took %v, expected ~50ms timeout", elapsed)
	}

	// Verify the stream context was cancelled (ps.cancel was called).
	select {
	case <-ps.ctx.Done():
	default:
		t.Fatal("stream context should be cancelled after send timeout")
	}
}

// TestSend_ReceiveExitUnblocksSend verifies that when receiveAcks detects a
// stream error and exits, it cancels the stream context, which unblocks a
// concurrent Send stuck on HTTP/2 flow control.
//
// This tests the primary deadlock fix end-to-end: the receive-side context
// cancel is what breaks the Send deadlock in production, before the timeout
// safety net has a chance to fire.
func TestSend_ReceiveExitUnblocksSend(t *testing.T) {
	t.Parallel()

	// blockSend never completes normally — only the context cancel should
	// unblock it. Closed in defer for goroutine cleanup.
	blockSend := make(chan struct{})
	defer close(blockSend)

	mock := &mockBidiStream{
		sendFunc: func(*pb.WritePieceRequest) error {
			<-blockSend
			return nil
		},
		recvFunc: func() (*pb.PieceAck, error) {
			// Simulate stream death after a short delay.
			// receiveAcks will exit and call ps.cancel(), unblocking Send.
			time.Sleep(30 * time.Millisecond)
			return nil, errors.New("stream reset by peer")
		},
	}

	// Long timeout — we expect context cancel to unblock Send, not the timeout.
	ps := newTestPieceStream(context.Background(), mock, 10*time.Second)
	defer ps.Close()

	start := time.Now()
	err := ps.Send(&pb.WritePieceRequest{TorrentHash: "test"})
	elapsed := time.Since(start)

	// Should NOT be a send timeout — context cancel should beat the 10s timer.
	if errors.Is(err, ErrSendTimeout) {
		t.Fatal("expected context cancellation to unblock Send, got ErrSendTimeout")
	}

	if err == nil {
		t.Fatal("expected error, got nil")
	}

	// Should unblock within ~100ms (the 30ms Recv delay + scheduling).
	// Well before the 10s timeout.
	if elapsed > 2*time.Second {
		t.Fatalf("Send took %v — not unblocked by receiveAcks context cancel", elapsed)
	}

	t.Logf("Send unblocked in %v with error: %v", elapsed, err)
}

// TestSend_AlreadyCancelledContext verifies the fast path: when the stream
// context is already cancelled, Send returns immediately without launching
// a goroutine or calling the underlying stream.
func TestSend_AlreadyCancelledContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel before creating the PieceStream

	sendCalled := false
	mock := &mockBidiStream{
		sendFunc: func(*pb.WritePieceRequest) error {
			sendCalled = true
			return nil
		},
	}

	ps := newTestPieceStream(ctx, mock, DefaultSendTimeout)
	defer ps.Close()

	err := ps.Send(&pb.WritePieceRequest{TorrentHash: "test"})
	if err == nil {
		t.Fatal("expected error on cancelled context")
	}

	if sendCalled {
		t.Error("underlying stream.Send should not be called when context is already cancelled")
	}
}

// TestForwardAcks_DrainsErrorOnStreamClose verifies that forwardAcks drains
// any pending error from the stream's error channel after Done() fires.
//
// Without Fix 2, when both Done() and Errors() are ready simultaneously,
// Go's select picks randomly. If Done() wins, forwardAcks returns without
// forwarding the error, leaving the ack processor unaware of stream death.
// With Fix 2, the Done() case explicitly drains pending errors.
//
// This test puts an error in the channel, closes done, and verifies the
// error is always forwarded. Multiple iterations make the select race
// deterministic: without the fix, ~50% of iterations would miss the error.
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
