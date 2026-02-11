package streaming

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
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
func startTestGRPCServer(
	t *testing.T,
	handler func(pb.QBSyncService_StreamPiecesBidiServer) error,
) pb.QBSyncServiceClient {
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

	go srv.Serve(lis)
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
	ctx context.Context,
	t *testing.T,
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

	ps := openStreamWithTimeouts(context.Background(), t, client, DefaultAckChannelSize, 0, 0)
	defer ps.Close()

	for i := range int32(numPieces) {
		if err := ps.Send(&pb.WritePieceRequest{
			TorrentHash: "test",
			PieceIndex:  i,
			Data:        []byte("piece-data"),
		}); err != nil {
			t.Fatalf("Send(%d) failed: %v", i, err)
		}
	}

	for i := range int32(numPieces) {
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

	ps := openStreamWithTimeouts(context.Background(), t, client, DefaultAckChannelSize, 0, testSendTimeout)
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

	ps := openStreamWithTimeouts(context.Background(), t, client, ackBufSize, testAckTimeout, 0)
	defer ps.Close()

	// Send several pieces. Server acks them immediately.
	// With ackBufSize=1, the channel fills after 1 ack, and receiveAcks
	// blocks trying to write the 2nd ack.
	for i := range int32(5) {
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

	ps := openStreamWithTimeouts(context.Background(), t, client, DefaultAckChannelSize, 0, 0)
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

	ps := openStreamWithTimeouts(context.Background(), t, client, DefaultAckChannelSize, 0, 0)
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

	ps := openStreamWithTimeouts(context.Background(), t, client, DefaultAckChannelSize, 0, testSendTimeout)
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

// startTestGRPCServerAddr starts a gRPC server on localhost with a controllable
// StreamPiecesBidi handler and returns the listener address string.
// This allows callers to connect via NewGRPCDestination with numConns.
func startTestGRPCServerAddr(t *testing.T, handler func(pb.QBSyncService_StreamPiecesBidiServer) error) string {
	t.Helper()

	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	srv := grpc.NewServer()
	pb.RegisterQBSyncServiceServer(srv, &testGRPCServer{handler: handler})

	go srv.Serve(lis)
	t.Cleanup(func() { srv.Stop() })

	return lis.Addr().String()
}

// TestIntegration_NewGRPCDestination_MultiConn verifies that NewGRPCDestination
// creates the requested number of independent connections and clients.
func TestIntegration_NewGRPCDestination_MultiConn(t *testing.T) {
	t.Parallel()

	addr := startTestGRPCServerAddr(t, func(stream pb.QBSyncService_StreamPiecesBidiServer) error {
		<-stream.Context().Done()
		return stream.Context().Err()
	})

	d, err := NewGRPCDestination(addr, 3, 3)
	if err != nil {
		t.Fatalf("NewGRPCDestination: %v", err)
	}
	defer d.Close()

	if len(d.conns) != 3 {
		t.Errorf("len(conns) = %d, want 3", len(d.conns))
	}
	if len(d.clients) != 3 {
		t.Errorf("len(clients) = %d, want 3", len(d.clients))
	}

	// Verify all connections are distinct
	for i := range len(d.conns) {
		for j := i + 1; j < len(d.conns); j++ {
			if d.conns[i] == d.conns[j] {
				t.Errorf("conns[%d] == conns[%d], want distinct", i, j)
			}
		}
	}

	if closeErr := d.Close(); closeErr != nil {
		t.Fatalf("Close: %v", closeErr)
	}
}

// TestIntegration_NewGRPCDestination_ZeroDefaultsToOne verifies that numConns=0
// defaults to 1 connection.
func TestIntegration_NewGRPCDestination_ZeroDefaultsToOne(t *testing.T) {
	t.Parallel()

	addr := startTestGRPCServerAddr(t, func(stream pb.QBSyncService_StreamPiecesBidiServer) error {
		<-stream.Context().Done()
		return stream.Context().Err()
	})

	d, err := NewGRPCDestination(addr, 0, 0)
	if err != nil {
		t.Fatalf("NewGRPCDestination: %v", err)
	}
	defer d.Close()

	if len(d.conns) != 1 {
		t.Errorf("len(conns) = %d, want 1", len(d.conns))
	}
	if len(d.clients) != 1 {
		t.Errorf("len(clients) = %d, want 1", len(d.clients))
	}
}

// TestIntegration_NewGRPCDestination_NegativeDefaultsToOne verifies that
// negative numConns defaults to 1 connection.
func TestIntegration_NewGRPCDestination_NegativeDefaultsToOne(t *testing.T) {
	t.Parallel()

	addr := startTestGRPCServerAddr(t, func(stream pb.QBSyncService_StreamPiecesBidiServer) error {
		<-stream.Context().Done()
		return stream.Context().Err()
	})

	d, err := NewGRPCDestination(addr, -1, -1)
	if err != nil {
		t.Fatalf("NewGRPCDestination: %v", err)
	}
	defer d.Close()

	if len(d.conns) != 1 {
		t.Errorf("len(conns) = %d, want 1", len(d.conns))
	}
}

// TestIntegration_AddConnection_Success verifies that AddConnection creates a
// new TCP connection and increments ConnectionCount.
func TestIntegration_AddConnection_Success(t *testing.T) {
	t.Parallel()

	addr := startTestGRPCServerAddr(t, func(stream pb.QBSyncService_StreamPiecesBidiServer) error {
		<-stream.Context().Done()
		return stream.Context().Err()
	})

	d, err := NewGRPCDestination(addr, 1, 4)
	if err != nil {
		t.Fatalf("NewGRPCDestination: %v", err)
	}
	defer d.Close()

	if d.ConnectionCount() != 1 {
		t.Fatalf("initial ConnectionCount = %d, want 1", d.ConnectionCount())
	}

	if addErr := d.AddConnection(); addErr != nil {
		t.Fatalf("AddConnection: %v", addErr)
	}

	if d.ConnectionCount() != 2 {
		t.Fatalf("ConnectionCount after add = %d, want 2", d.ConnectionCount())
	}

	// Verify all connections are distinct
	d.mu.RLock()
	if d.conns[0] == d.conns[1] {
		t.Error("new connection should be distinct from existing")
	}
	d.mu.RUnlock()
}

// TestIntegration_AddConnection_AtMax verifies that AddConnection returns an
// error when already at maximum connections.
func TestIntegration_AddConnection_AtMax(t *testing.T) {
	t.Parallel()

	addr := startTestGRPCServerAddr(t, func(stream pb.QBSyncService_StreamPiecesBidiServer) error {
		<-stream.Context().Done()
		return stream.Context().Err()
	})

	d, err := NewGRPCDestination(addr, 2, 2)
	if err != nil {
		t.Fatalf("NewGRPCDestination: %v", err)
	}
	defer d.Close()

	addErr := d.AddConnection()
	if addErr == nil {
		t.Fatal("expected error when adding connection at max, got nil")
	}

	if d.ConnectionCount() != 2 {
		t.Fatalf("ConnectionCount should remain 2, got %d", d.ConnectionCount())
	}
}

// TestIntegration_RemoveConnection_Success verifies that RemoveConnection
// removes the connection at the expected index and decrements ConnectionCount.
func TestIntegration_RemoveConnection_Success(t *testing.T) {
	t.Parallel()

	addr := startTestGRPCServerAddr(t, func(stream pb.QBSyncService_StreamPiecesBidiServer) error {
		<-stream.Context().Done()
		return stream.Context().Err()
	})

	d, err := NewGRPCDestination(addr, 1, 4)
	if err != nil {
		t.Fatalf("NewGRPCDestination: %v", err)
	}
	defer d.Close()

	// Add a connection first so we can remove it
	if addErr := d.AddConnection(); addErr != nil {
		t.Fatalf("AddConnection: %v", addErr)
	}
	if d.ConnectionCount() != 2 {
		t.Fatalf("ConnectionCount = %d, want 2", d.ConnectionCount())
	}

	// Remove the last connection (index 1)
	if rmErr := d.RemoveConnection(1); rmErr != nil {
		t.Fatalf("RemoveConnection: %v", rmErr)
	}

	if d.ConnectionCount() != 1 {
		t.Fatalf("ConnectionCount after remove = %d, want 1", d.ConnectionCount())
	}
}

// TestIntegration_RemoveConnection_AtMin verifies that RemoveConnection returns
// an error when at minimum connection count.
func TestIntegration_RemoveConnection_AtMin(t *testing.T) {
	t.Parallel()

	addr := startTestGRPCServerAddr(t, func(stream pb.QBSyncService_StreamPiecesBidiServer) error {
		<-stream.Context().Done()
		return stream.Context().Err()
	})

	d, err := NewGRPCDestination(addr, 2, 4)
	if err != nil {
		t.Fatalf("NewGRPCDestination: %v", err)
	}
	defer d.Close()

	rmErr := d.RemoveConnection(1)
	if rmErr == nil {
		t.Fatal("expected error when removing connection at min, got nil")
	}

	if d.ConnectionCount() != 2 {
		t.Fatalf("ConnectionCount should remain 2, got %d", d.ConnectionCount())
	}
}

// TestIntegration_RemoveConnection_IndexMismatch verifies that RemoveConnection
// returns an error when the expected index doesn't match the current last index.
// This prevents removing the wrong connection when a concurrent add has occurred.
func TestIntegration_RemoveConnection_IndexMismatch(t *testing.T) {
	t.Parallel()

	addr := startTestGRPCServerAddr(t, func(stream pb.QBSyncService_StreamPiecesBidiServer) error {
		<-stream.Context().Done()
		return stream.Context().Err()
	})

	d, err := NewGRPCDestination(addr, 1, 4)
	if err != nil {
		t.Fatalf("NewGRPCDestination: %v", err)
	}
	defer d.Close()

	// Add two connections (total: 3)
	if addErr := d.AddConnection(); addErr != nil {
		t.Fatalf("AddConnection 1: %v", addErr)
	}
	if addErr := d.AddConnection(); addErr != nil {
		t.Fatalf("AddConnection 2: %v", addErr)
	}

	// Try to remove index 1 when last is actually 2
	rmErr := d.RemoveConnection(1)
	if rmErr == nil {
		t.Fatal("expected error for index mismatch, got nil")
	}

	// Connection count should be unchanged
	if d.ConnectionCount() != 3 {
		t.Fatalf("ConnectionCount should remain 3, got %d", d.ConnectionCount())
	}

	// Correct index should work
	if rmErr = d.RemoveConnection(2); rmErr != nil {
		t.Fatalf("RemoveConnection with correct index: %v", rmErr)
	}
	if d.ConnectionCount() != 2 {
		t.Fatalf("ConnectionCount after correct remove = %d, want 2", d.ConnectionCount())
	}
}

// TestIntegration_AddRemoveConnection_Concurrent verifies that AddConnection
// and RemoveConnection are thread-safe under concurrent access.
func TestIntegration_AddRemoveConnection_Concurrent(t *testing.T) {
	t.Parallel()

	addr := startTestGRPCServerAddr(t, func(stream pb.QBSyncService_StreamPiecesBidiServer) error {
		<-stream.Context().Done()
		return stream.Context().Err()
	})

	d, err := NewGRPCDestination(addr, 1, 10)
	if err != nil {
		t.Fatalf("NewGRPCDestination: %v", err)
	}
	defer d.Close()

	// Concurrent adds
	var wg sync.WaitGroup
	for range 5 {
		wg.Go(func() {
			_ = d.AddConnection()
		})
	}
	wg.Wait()

	// Should have between 1 and 6 connections (some may fail at max)
	count := d.ConnectionCount()
	if count < 1 || count > 6 {
		t.Fatalf("ConnectionCount = %d, expected between 1 and 6", count)
	}

	// Concurrent removes (some will fail at min or index mismatch — that's OK)
	for range 5 {
		wg.Go(func() {
			c := d.ConnectionCount()
			_ = d.RemoveConnection(c - 1)
		})
	}
	wg.Wait()

	// Should still have at least minConns
	if d.ConnectionCount() < d.MinConnections() {
		t.Fatalf("ConnectionCount %d < MinConnections %d", d.ConnectionCount(), d.MinConnections())
	}
}

// TestIntegration_MinMaxConnections verifies the accessor methods.
func TestIntegration_MinMaxConnections(t *testing.T) {
	t.Parallel()

	addr := startTestGRPCServerAddr(t, func(stream pb.QBSyncService_StreamPiecesBidiServer) error {
		<-stream.Context().Done()
		return stream.Context().Err()
	})

	d, err := NewGRPCDestination(addr, 2, 8)
	if err != nil {
		t.Fatalf("NewGRPCDestination: %v", err)
	}
	defer d.Close()

	if d.MinConnections() != 2 {
		t.Errorf("MinConnections = %d, want 2", d.MinConnections())
	}
	if d.MaxConnections() != 8 {
		t.Errorf("MaxConnections = %d, want 8", d.MaxConnections())
	}
}

// TestIntegration_OpenStream_DistributesAcrossConns verifies that OpenStream
// distributes streams across all connections via round-robin, and that each
// stream can send and receive independently.
func TestIntegration_OpenStream_DistributesAcrossConns(t *testing.T) {
	t.Parallel()

	addr := startTestGRPCServerAddr(t, func(stream pb.QBSyncService_StreamPiecesBidiServer) error {
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

	d, err := NewGRPCDestination(addr, 2, 2)
	if err != nil {
		t.Fatalf("NewGRPCDestination: %v", err)
	}
	defer d.Close()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	const numStreams = 4
	streams := make([]*PieceStream, numStreams)
	for i := range numStreams {
		s, openErr := d.OpenStream(context.Background(), logger)
		if openErr != nil {
			t.Fatalf("OpenStream(%d): %v", i, openErr)
		}
		streams[i] = s
	}

	// Send a piece on each stream and verify we get an ack back.
	var wg sync.WaitGroup
	for i, s := range streams {
		wg.Add(1)
		go func(idx int, ps *PieceStream) {
			defer wg.Done()

			sendErr := ps.Send(&pb.WritePieceRequest{
				TorrentHash: "test",
				PieceIndex:  int32(idx),
				Data:        []byte("piece-data"),
			})
			if sendErr != nil {
				t.Errorf("stream %d Send: %v", idx, sendErr)
				return
			}

			select {
			case ack := <-ps.Acks():
				if !ack.GetSuccess() {
					t.Errorf("stream %d ack: not success", idx)
				}
				if ack.GetPieceIndex() != int32(idx) {
					t.Errorf("stream %d ack index = %d, want %d", idx, ack.GetPieceIndex(), idx)
				}
			case <-time.After(5 * time.Second):
				t.Errorf("stream %d: timed out waiting for ack", idx)
			}
		}(i, s)
	}

	wg.Wait()

	for _, s := range streams {
		s.Close()
	}
}
