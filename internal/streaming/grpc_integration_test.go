package streaming

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/arsac/qb-sync/proto"
)

// testGRPCServer is a minimal gRPC server for integration testing.
// Only StreamPiecesBidi is controllable; all other RPCs return unimplemented.
type testGRPCServer struct {
	pb.UnimplementedQBSyncServiceServer
	handler func(pb.QBSyncService_StreamPiecesBidiServer) error
}

func (s *testGRPCServer) StreamPiecesBidi(stream pb.QBSyncService_StreamPiecesBidiServer) error {
	return s.handler(stream)
}

// testWindowSize constrains the HTTP/2 flow control window so that a stalled
// server causes Send to block after a predictable amount of data.
const testWindowSize int32 = 64 * 1024 // 64 KB

// startTestGRPCServer starts a gRPC server on localhost with a controllable
// StreamPiecesBidi handler and returns a connected client. Both use small
// HTTP/2 windows so flow control kicks in quickly during stall tests.
// The server is stopped when the test completes.
func startTestGRPCServer(t *testing.T, handler func(pb.QBSyncService_StreamPiecesBidiServer) error) pb.QBSyncServiceClient {
	t.Helper()

	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	srv := grpc.NewServer(
		grpc.InitialWindowSize(testWindowSize),
		grpc.InitialConnWindowSize(testWindowSize),
	)
	pb.RegisterQBSyncServiceServer(srv, &testGRPCServer{handler: handler})

	go srv.Serve(lis) //nolint:errcheck // test server
	t.Cleanup(func() { srv.Stop() })

	conn, err := grpc.NewClient(
		lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithInitialWindowSize(testWindowSize),
		grpc.WithInitialConnWindowSize(testWindowSize),
	)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	return pb.NewQBSyncServiceClient(conn)
}

// openStreamWithTimeouts opens a real gRPC bidirectional stream and wraps it
// in a PieceStream with configurable timeouts for integration testing.
func openStreamWithTimeouts(
	t *testing.T,
	ctx context.Context,
	client pb.QBSyncServiceClient,
	ackBufSize int,
	ackTimeout, sndTimeout time.Duration,
) *PieceStream {
	t.Helper()

	streamCtx, streamCancel := context.WithCancel(ctx)
	stream, err := client.StreamPiecesBidi(streamCtx)
	if err != nil {
		streamCancel()
		t.Fatalf("failed to open stream: %v", err)
	}

	ps := &PieceStream{
		ctx:                     streamCtx,
		cancel:                  streamCancel,
		stream:                  stream,
		logger:                  testLogger,
		acks:                    make(chan *pb.PieceAck, ackBufSize),
		ackReady:                make(chan struct{}, 1),
		done:                    make(chan struct{}),
		errors:                  make(chan error, 1),
		ackWriteTimeoutOverride: ackTimeout,
		sendTimeoutOverride:     sndTimeout,
	}
	go ps.receiveAcks()
	return ps
}

// TestIntegration_SendRecvAckRoundtrip verifies the full Send → server Recv →
// server Send ack → client Recv ack path over real gRPC transport.
// This is the happy-path sanity check that our PieceStream wiring works
// correctly with a real HTTP/2 connection.
func TestIntegration_SendRecvAckRoundtrip(t *testing.T) {
	t.Parallel()

	const numPieces = 10

	client := startTestGRPCServer(t, func(stream pb.QBSyncService_StreamPiecesBidiServer) error {
		for {
			req, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				return nil
			}
			if err != nil {
				return err
			}
			if sendErr := stream.Send(&pb.PieceAck{
				TorrentHash: req.GetTorrentHash(),
				PieceIndex:  req.GetPieceIndex(),
				Success:     true,
			}); sendErr != nil {
				return sendErr
			}
		}
	})

	ps := openStreamWithTimeouts(t, context.Background(), client, DefaultAckChannelSize, 0, 0)
	defer ps.Close()

	for i := int32(0); i < numPieces; i++ {
		if err := ps.Send(&pb.WritePieceRequest{
			TorrentHash: "test",
			PieceIndex:  i,
			Data:        []byte("piece-data"),
		}); err != nil {
			t.Fatalf("Send(%d) failed: %v", i, err)
		}
	}

	for i := int32(0); i < numPieces; i++ {
		select {
		case ack := <-ps.Acks():
			if !ack.GetSuccess() {
				t.Errorf("ack %d: expected success, got error: %s", ack.GetPieceIndex(), ack.GetError())
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for ack %d/%d", i, numPieces)
		}
	}
}

// TestIntegration_SendTimeoutOnStallServer verifies that when the server stops
// reading from the stream (simulating cold going silent), HTTP/2 flow control
// fills up and the client's Send timeout fires, cancelling the stream.
//
// This is the core deadlock scenario: without the send timeout, Send blocks
// forever on a stalled server because gRPC Send doesn't accept a context.
func TestIntegration_SendTimeoutOnStallServer(t *testing.T) {
	t.Parallel()

	const testSendTimeout = 500 * time.Millisecond

	serverReady := make(chan struct{})

	client := startTestGRPCServer(t, func(stream pb.QBSyncService_StreamPiecesBidiServer) error {
		// Read one piece to establish the stream, then stall.
		if _, err := stream.Recv(); err != nil {
			return err
		}
		close(serverReady)
		// Block until the client gives up and the stream context is cancelled.
		<-stream.Context().Done()
		return stream.Context().Err()
	})

	ps := openStreamWithTimeouts(t, context.Background(), client, DefaultAckChannelSize, 0, testSendTimeout)
	defer ps.Close()

	// First send succeeds — server reads it.
	if err := ps.Send(&pb.WritePieceRequest{
		TorrentHash: "test",
		PieceIndex:  0,
		Data:        make([]byte, 1024),
	}); err != nil {
		t.Fatalf("first send failed: %v", err)
	}
	<-serverReady

	// Flood sends to exhaust the HTTP/2 flow control window.
	// With testWindowSize=64KB and 32KB pieces, the window fills after 2-3 sends.
	// BDP estimation may increase the window, so we send up to 200 pieces (6.4 MB)
	// which exceeds any reasonable dynamic window.
	var sendErr error
	for i := int32(1); i < 200; i++ {
		sendErr = ps.Send(&pb.WritePieceRequest{
			TorrentHash: "test",
			PieceIndex:  i,
			Data:        make([]byte, 32*1024),
		})
		if sendErr != nil {
			t.Logf("Send(%d) failed: %v", i, sendErr)
			break
		}
	}

	if sendErr == nil {
		t.Fatal("expected Send to fail when server stalled and flow control window exhausted")
	}

	if ps.ctx.Err() == nil {
		t.Fatal("stream context should be cancelled by send timeout")
	}
}

// TestIntegration_AckChannelBlockedRecovery verifies that when the client
// doesn't consume acks and the ack channel fills up, receiveAcks exits via
// the ack write timeout, cancelling the stream context.
//
// This covers the scenario where forwardAcks is slow → ps.acks fills →
// receiveAcks can't call Recv → can't detect stream death → deadlock.
// The ack write timeout breaks this cycle.
func TestIntegration_AckChannelBlockedRecovery(t *testing.T) {
	t.Parallel()

	const (
		testAckTimeout = 500 * time.Millisecond
		ackBufSize     = 1 // Tiny buffer — second ack blocks
	)

	client := startTestGRPCServer(t, func(stream pb.QBSyncService_StreamPiecesBidiServer) error {
		for {
			req, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				return nil
			}
			if err != nil {
				return err
			}
			if sendErr := stream.Send(&pb.PieceAck{
				TorrentHash: req.GetTorrentHash(),
				PieceIndex:  req.GetPieceIndex(),
				Success:     true,
			}); sendErr != nil {
				return sendErr
			}
		}
	})

	ps := openStreamWithTimeouts(t, context.Background(), client, ackBufSize, testAckTimeout, 0)
	defer ps.Close()

	// Send several pieces. Server acks them immediately.
	// With ackBufSize=1, the channel fills after 1 ack, and receiveAcks
	// blocks trying to write the 2nd ack.
	for i := int32(0); i < 5; i++ {
		err := ps.Send(&pb.WritePieceRequest{
			TorrentHash: "test",
			PieceIndex:  i,
			Data:        []byte("data"),
		})
		if err != nil {
			break // Stream may already be cancelled by receiveAcks timeout
		}
	}

	// Don't consume acks. receiveAcks should timeout and exit.
	select {
	case <-ps.Done():
		// receiveAcks exited — correct behavior.
	case <-time.After(5 * time.Second):
		t.Fatal("receiveAcks didn't exit after ack channel blocked")
	}

	if ps.ctx.Err() == nil {
		t.Fatal("stream context should be cancelled by ack write timeout")
	}
}

// TestIntegration_ServerErrorDetection verifies that when the server handler
// returns an error mid-stream, the client's receiveAcks detects it via Recv
// and cancels the stream context cleanly.
func TestIntegration_ServerErrorDetection(t *testing.T) {
	t.Parallel()

	client := startTestGRPCServer(t, func(stream pb.QBSyncService_StreamPiecesBidiServer) error {
		// Read one piece, then crash.
		if _, err := stream.Recv(); err != nil {
			return err
		}
		return errors.New("server internal error")
	})

	ps := openStreamWithTimeouts(t, context.Background(), client, DefaultAckChannelSize, 0, 0)
	defer ps.Close()

	// Trigger the server's read + error return.
	_ = ps.Send(&pb.WritePieceRequest{
		TorrentHash: "test",
		PieceIndex:  0,
		Data:        []byte("data"),
	})

	// receiveAcks should detect the stream error via Recv and exit.
	select {
	case <-ps.Done():
		// Stream ended — correct.
	case <-time.After(5 * time.Second):
		t.Fatal("receiveAcks didn't detect server error")
	}

	// Verify an error was captured.
	select {
	case err := <-ps.Errors():
		t.Logf("detected server error: %v", err)
	default:
		// Error may have been consumed by the context cancel path — acceptable.
	}
}

// newTestPool creates a minimal StreamPool for integration tests with a real
// PieceStream wrapped in a PooledStream. It starts the forwardAcks goroutine
// and returns a cleanup function that cancels the pool context and waits for
// goroutines to exit.
func newTestPool(t *testing.T, ps *PieceStream, id int) (*StreamPool, func()) {
	t.Helper()

	poolCtx, poolCancel := context.WithCancel(context.Background())
	pool := &StreamPool{
		ctx:      poolCtx,
		cancel:   poolCancel,
		errs:     make(chan error, 10),
		acks:     make(chan *pb.PieceAck, 100),
		ackReady: make(chan struct{}, 10),
		logger:   slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}

	pooled := &PooledStream{stream: ps, id: id}
	pool.wg.Add(1)
	go pool.forwardAcks(pooled)

	cleanup := func() {
		poolCancel()
		pool.wg.Wait()
	}
	return pool, cleanup
}

// TestIntegration_PoolErrorPropagation verifies that a stream error from a
// crashing server propagates through the full path: real gRPC → receiveAcks
// detects error → writes to ps.errors → closes ps.done → forwardAcks drains
// and forwards to pool.errs.
//
// This exercises the error drain in forwardAcks (lines 250-261 of stream_pool.go)
// with a real gRPC transport, ensuring the pool always learns about stream death.
func TestIntegration_PoolErrorPropagation(t *testing.T) {
	t.Parallel()

	client := startTestGRPCServer(t, func(stream pb.QBSyncService_StreamPiecesBidiServer) error {
		// Read one piece, then crash.
		if _, err := stream.Recv(); err != nil {
			return err
		}
		return errors.New("server crash")
	})

	ps := openStreamWithTimeouts(t, context.Background(), client, DefaultAckChannelSize, 0, 0)
	defer ps.Close()

	pool, cleanup := newTestPool(t, ps, 1)
	defer cleanup()

	// Trigger the server's read + error return.
	if err := ps.Send(&pb.WritePieceRequest{
		TorrentHash: "test",
		PieceIndex:  0,
		Data:        []byte("data"),
	}); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	// The error should propagate through forwardAcks to pool.errs.
	select {
	case err := <-pool.errs:
		t.Logf("pool received error: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for error to propagate to pool")
	}

	// Stream's Done channel should be closed.
	select {
	case <-ps.Done():
		// Correct — receiveAcks exited.
	default:
		t.Fatal("stream Done() should be closed after server crash")
	}
}

// TestIntegration_SendTimeoutErrorReachesPool verifies the full deadlock
// detection chain end-to-end: cold stops consuming → HTTP/2 flow control fills
// → Send timeout fires → stream context cancelled → receiveAcks exits →
// forwardAcks detects stream death via Done() → synthetic error reaches pool.
//
// This is the production deadlock scenario. Without the send timeout, Send
// blocks forever. The send timeout cancels the stream context, which causes
// receiveAcks to exit (closing Done()), which causes forwardAcks to detect
// silent stream death and send a synthetic error to pool.errs.
func TestIntegration_SendTimeoutErrorReachesPool(t *testing.T) {
	t.Parallel()

	const testSendTimeout = 500 * time.Millisecond

	serverReady := make(chan struct{})

	client := startTestGRPCServer(t, func(stream pb.QBSyncService_StreamPiecesBidiServer) error {
		// Read one piece to establish the stream, then stall.
		if _, err := stream.Recv(); err != nil {
			return err
		}
		close(serverReady)
		// Block until the client gives up.
		<-stream.Context().Done()
		return stream.Context().Err()
	})

	ps := openStreamWithTimeouts(t, context.Background(), client, DefaultAckChannelSize, 0, testSendTimeout)
	defer ps.Close()

	pool, cleanup := newTestPool(t, ps, 1)
	defer cleanup()

	// First send succeeds — server reads it.
	if err := ps.Send(&pb.WritePieceRequest{
		TorrentHash: "test",
		PieceIndex:  0,
		Data:        make([]byte, 1024),
	}); err != nil {
		t.Fatalf("first send failed: %v", err)
	}
	<-serverReady

	// Flood sends to exhaust the HTTP/2 flow control window.
	for i := int32(1); i < 200; i++ {
		err := ps.Send(&pb.WritePieceRequest{
			TorrentHash: "test",
			PieceIndex:  i,
			Data:        make([]byte, 32*1024),
		})
		if err != nil {
			t.Logf("Send(%d) failed as expected: %v", i, err)
			break
		}
	}

	// Stream's Done channel must close — this is the primary signal that the
	// deadlock detection chain worked (send timeout → context cancel →
	// receiveAcks exits → close(done)).
	select {
	case <-ps.Done():
		t.Log("stream Done() closed — deadlock detection chain worked")
	case <-time.After(5 * time.Second):
		t.Fatal("stream Done() not closed — send timeout didn't break the deadlock")
	}

	// With the silent-death fix, forwardAcks now sends a synthetic error
	// when the stream closes without an explicit error. This must arrive
	// reliably — it's what triggers the ack processor to reconnect.
	select {
	case err := <-pool.errs:
		t.Logf("pool received error: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for error on pool.errs — forwardAcks silent-death fix not working")
	}
}
