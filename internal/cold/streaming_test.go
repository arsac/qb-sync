package cold

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc/metadata"

	pb "github.com/arsac/qb-sync/proto"
)

// mockBidiStream implements pb.QBSyncService_StreamPiecesBidiServer for testing.
type mockBidiStream struct {
	ctx     context.Context
	recvCh  chan *pb.WritePieceRequest // Feed requests via this channel; close for EOF
	sendCh  chan *pb.PieceAck          // Acks are sent here
	sendErr error                      // If non-nil, Send returns this error
}

func (m *mockBidiStream) Recv() (*pb.WritePieceRequest, error) {
	select {
	case req, ok := <-m.recvCh:
		if !ok {
			return nil, io.EOF
		}
		return req, nil
	case <-m.ctx.Done():
		return nil, m.ctx.Err()
	}
}

func (m *mockBidiStream) Send(ack *pb.PieceAck) error {
	if m.sendErr != nil {
		return m.sendErr
	}
	select {
	case m.sendCh <- ack:
		return nil
	case <-m.ctx.Done():
		return m.ctx.Err()
	}
}

func (m *mockBidiStream) Context() context.Context     { return m.ctx }
func (m *mockBidiStream) SetHeader(metadata.MD) error  { return nil }
func (m *mockBidiStream) SendHeader(metadata.MD) error { return nil }
func (m *mockBidiStream) SetTrailer(metadata.MD)       {}
func (m *mockBidiStream) SendMsg(any) error            { return nil }
func (m *mockBidiStream) RecvMsg(any) error            { return nil }

// newTestServer creates a minimal Server with a configured memBudget for testing.
func newTestServer(t *testing.T, budgetBytes int64) *Server {
	t.Helper()
	tmpDir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	return &Server{
		config: ServerConfig{
			BasePath:      tmpDir,
			ListenAddr:    ":50051",
			StreamWorkers: 2,
		},
		logger:         logger,
		torrents:       make(map[string]*serverTorrentState),
		abortingHashes: make(map[string]chan struct{}),
		inodes:         NewInodeRegistry(tmpDir, logger),
		memBudget:      semaphore.NewWeighted(budgetBytes),
		finalizeSem:    semaphore.NewWeighted(1),
	}
}

func TestStreamReceiver_AcquiresBudget(t *testing.T) {
	t.Parallel()

	budgetBytes := int64(1024) // 1 KB budget
	s := newTestServer(t, budgetBytes)

	ctx := t.Context()

	workCh := make(chan *pb.WritePieceRequest, 10)
	stream := &mockBidiStream{
		ctx:    ctx,
		recvCh: make(chan *pb.WritePieceRequest, 5),
	}

	// Send a 512-byte piece
	data := make([]byte, 512)
	stream.recvCh <- &pb.WritePieceRequest{
		TorrentHash: "abc",
		PieceIndex:  0,
		Data:        data,
	}
	close(stream.recvCh) // EOF after one piece

	_, err := s.streamReceiver(ctx, stream, workCh)
	if err != nil {
		t.Fatalf("streamReceiver returned error: %v", err)
	}

	// The piece should be in workCh
	select {
	case req := <-workCh:
		if len(req.GetData()) != 512 {
			t.Errorf("expected 512 bytes, got %d", len(req.GetData()))
		}
	default:
		t.Fatal("expected piece in workCh")
	}

	// 512 bytes should be acquired from the budget (512 remaining out of 1024)
	if !s.memBudget.TryAcquire(512) {
		t.Fatal("expected 512 bytes remaining in budget")
	}
	if s.memBudget.TryAcquire(1) {
		t.Fatal("expected no more budget available")
	}
	s.memBudget.Release(512) // clean up
}

func TestStreamReceiver_ZeroDataSkipsBudget(t *testing.T) {
	t.Parallel()

	budgetBytes := int64(100)
	s := newTestServer(t, budgetBytes)

	ctx := t.Context()

	workCh := make(chan *pb.WritePieceRequest, 10)
	stream := &mockBidiStream{
		ctx:    ctx,
		recvCh: make(chan *pb.WritePieceRequest, 5),
	}

	// Send a request with no data
	stream.recvCh <- &pb.WritePieceRequest{
		TorrentHash: "abc",
		PieceIndex:  0,
		Data:        nil,
	}
	close(stream.recvCh)

	_, err := s.streamReceiver(ctx, stream, workCh)
	if err != nil {
		t.Fatalf("streamReceiver returned error: %v", err)
	}

	// Full budget should still be available
	if !s.memBudget.TryAcquire(budgetBytes) {
		t.Fatal("expected full budget available for zero-data request")
	}
	s.memBudget.Release(budgetBytes)
}

func TestStreamReceiver_BackpressureOnBudgetExhaustion(t *testing.T) {
	t.Parallel()

	// Budget of 100 bytes; send a 100-byte piece then a 50-byte piece.
	// The second piece should block until budget is freed.
	budgetBytes := int64(100)
	s := newTestServer(t, budgetBytes)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	workCh := make(chan *pb.WritePieceRequest, 10)
	stream := &mockBidiStream{
		ctx:    ctx,
		recvCh: make(chan *pb.WritePieceRequest, 5),
	}

	// Send two pieces totaling 150 bytes (more than 100 budget)
	stream.recvCh <- &pb.WritePieceRequest{
		TorrentHash: "abc",
		PieceIndex:  0,
		Data:        make([]byte, 100),
	}
	stream.recvCh <- &pb.WritePieceRequest{
		TorrentHash: "abc",
		PieceIndex:  1,
		Data:        make([]byte, 50),
	}
	// Don't close yet — receiver should block on second acquire

	var recvErr error
	recvDone := make(chan struct{})
	go func() {
		defer close(recvDone)
		_, recvErr = s.streamReceiver(ctx, stream, workCh)
	}()

	// Wait a moment to let the first piece through
	time.Sleep(50 * time.Millisecond)

	// First piece should be queued
	select {
	case <-workCh:
	default:
		t.Fatal("expected first piece in workCh")
	}

	// Second piece should NOT be queued yet (blocked on Acquire)
	select {
	case <-workCh:
		t.Fatal("second piece should be blocked on budget")
	case <-time.After(50 * time.Millisecond):
		// Expected: receiver is blocked
	}

	// Free the budget (simulate worker releasing)
	s.memBudget.Release(100)

	// Now the second piece should come through
	select {
	case <-workCh:
		// Good, second piece is now queued
	case <-time.After(500 * time.Millisecond):
		t.Fatal("second piece should be unblocked after budget release")
	}

	// Close stream to trigger EOF
	close(stream.recvCh)

	select {
	case <-recvDone:
	case <-time.After(time.Second):
		t.Fatal("streamReceiver should have returned after EOF")
	}

	if recvErr != nil {
		t.Fatalf("streamReceiver returned error: %v", recvErr)
	}
}

func TestStreamReceiver_ReleasesOnContextCancel(t *testing.T) {
	t.Parallel()

	budgetBytes := int64(1024)
	s := newTestServer(t, budgetBytes)

	ctx, cancel := context.WithCancel(context.Background())

	// Use a full workCh so the select blocks, then cancel context
	workCh := make(chan *pb.WritePieceRequest) // unbuffered — will block
	stream := &mockBidiStream{
		ctx:    ctx,
		recvCh: make(chan *pb.WritePieceRequest, 5),
	}

	data := make([]byte, 200)
	stream.recvCh <- &pb.WritePieceRequest{
		TorrentHash: "abc",
		PieceIndex:  0,
		Data:        data,
	}

	recvDone := make(chan error, 1)
	go func() {
		_, err := s.streamReceiver(ctx, stream, workCh)
		recvDone <- err
	}()

	// Wait for receiver to acquire budget and block on workCh send
	time.Sleep(50 * time.Millisecond)

	// Cancel context — receiver should release the 200 bytes and return
	cancel()

	select {
	case err := <-recvDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("streamReceiver should have returned after cancel")
	}

	// Budget should be fully restored (200 was acquired then released)
	if !s.memBudget.TryAcquire(budgetBytes) {
		t.Fatal("expected full budget restored after context cancel")
	}
	s.memBudget.Release(budgetBytes)
}

func TestStreamWorker_ReleasesBudget(t *testing.T) {
	t.Parallel()

	budgetBytes := int64(1024)
	s := newTestServer(t, budgetBytes)

	// Pre-register a torrent so WritePiece can look it up
	hash := "test123"
	state := &serverTorrentState{
		written: make([]bool, 10),
		files:   []*serverFileInfo{},
	}
	s.torrents[hash] = state

	ctx := context.Background()
	workCh := make(chan *pb.WritePieceRequest, 5)
	ackCh := make(chan *pb.PieceAck, 10)

	// Simulate acquired budget
	dataLen := int64(256)
	s.memBudget.Acquire(ctx, dataLen)

	// Send piece to work channel then close
	workCh <- &pb.WritePieceRequest{
		TorrentHash: hash,
		PieceIndex:  0,
		Data:        make([]byte, 256),
	}
	close(workCh)

	s.streamWorker(ctx, workCh, ackCh)

	// Budget should be fully released
	if !s.memBudget.TryAcquire(budgetBytes) {
		t.Fatal("expected full budget after worker released")
	}
	s.memBudget.Release(budgetBytes)

	// Should have produced an ack
	select {
	case ack := <-ackCh:
		if ack.GetTorrentHash() != hash {
			t.Errorf("expected hash %q, got %q", hash, ack.GetTorrentHash())
		}
	default:
		t.Fatal("expected ack from worker")
	}
}

func TestStreamPiecesBidi_BudgetFullyRestoredAfterCancel(t *testing.T) {
	t.Parallel()

	// Verify that after StreamPiecesBidi returns (via cancellation),
	// all acquired budget is released — whether by workers processing pieces
	// or by the drain loop cleaning up unprocessed pieces.
	budgetBytes := int64(1024)
	s := newTestServer(t, budgetBytes)
	s.config.StreamWorkers = 1

	ctx, cancel := context.WithCancel(context.Background())
	stream := &mockBidiStream{
		ctx:    ctx,
		recvCh: make(chan *pb.WritePieceRequest, 5),
		sendCh: make(chan *pb.PieceAck, 100), // buffered so sends don't block
	}

	// Send pieces then EOF
	stream.recvCh <- &pb.WritePieceRequest{
		TorrentHash: "test",
		PieceIndex:  0,
		Data:        make([]byte, 100),
	}
	stream.recvCh <- &pb.WritePieceRequest{
		TorrentHash: "test",
		PieceIndex:  1,
		Data:        make([]byte, 200),
	}
	close(stream.recvCh)

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.StreamPiecesBidi(stream)
	}()

	// Cancel after giving some time for pieces to be received
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("StreamPiecesBidi should have returned after cancel")
	}

	// All budget should be released regardless of which path was taken
	if !s.memBudget.TryAcquire(budgetBytes) {
		t.Fatal("expected full budget restored after cancel")
	}
	s.memBudget.Release(budgetBytes)
}

func TestStreamPiecesBidi_DrainReleasesUnprocessedBudget(t *testing.T) {
	t.Parallel()

	// Directly test the drain loop pattern: pre-acquire budget for items
	// in a channel, close the channel, drain and release.
	budgetBytes := int64(1024)
	s := newTestServer(t, budgetBytes)

	ctx := context.Background()

	// Simulate: receiver acquired budget for 3 pieces, queued them
	pieces := []*pb.WritePieceRequest{
		{Data: make([]byte, 100)},
		{Data: make([]byte, 200)},
		{Data: make([]byte, 300)},
	}

	workCh := make(chan *pb.WritePieceRequest, len(pieces))
	for _, p := range pieces {
		dataLen := int64(len(p.GetData()))
		if err := s.memBudget.Acquire(ctx, dataLen); err != nil {
			t.Fatal(err)
		}
		workCh <- p
	}
	close(workCh)

	// 600 bytes acquired, 424 remaining
	if s.memBudget.TryAcquire(425) {
		t.Fatal("expected only 424 bytes remaining")
	}

	// Drain loop (same pattern as StreamPiecesBidi)
	for req := range workCh {
		if dataLen := int64(len(req.GetData())); dataLen > 0 {
			s.memBudget.Release(dataLen)
		}
	}

	// Full budget should be restored
	if !s.memBudget.TryAcquire(budgetBytes) {
		t.Fatal("expected full budget restored after drain")
	}
	s.memBudget.Release(budgetBytes)
}

func TestStreamPiecesBidi_MultipleConcurrentStreams(t *testing.T) {
	t.Parallel()

	// 512 bytes budget shared between two streams each sending 300 bytes.
	// One must wait for the other to release.
	budgetBytes := int64(512)
	s := newTestServer(t, budgetBytes)
	s.config.StreamWorkers = 1

	// Register a torrent so WritePiece won't fail with "not initialized"
	hash := "concurrent"
	s.torrents[hash] = &serverTorrentState{
		written: make([]bool, 10),
		files:   []*serverFileInfo{},
	}

	var stream1Done, stream2Done atomic.Bool

	runStream := func(pieceIdx int32, done *atomic.Bool) {
		ctx := context.Background()
		stream := &mockBidiStream{
			ctx:    ctx,
			recvCh: make(chan *pb.WritePieceRequest, 1),
			sendCh: make(chan *pb.PieceAck, 10),
		}
		stream.recvCh <- &pb.WritePieceRequest{
			TorrentHash: hash,
			PieceIndex:  pieceIdx,
			Data:        make([]byte, 300),
		}
		close(stream.recvCh)

		_ = s.StreamPiecesBidi(stream)
		done.Store(true)
	}

	// Run both concurrently
	var wg sync.WaitGroup
	wg.Go(func() { runStream(0, &stream1Done) })
	wg.Go(func() { runStream(1, &stream2Done) })

	// Both should complete (budget is shared, but released between streams)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent streams should both complete")
	}

	if !stream1Done.Load() || !stream2Done.Load() {
		t.Fatal("both streams should have completed")
	}

	// Full budget should be available
	if !s.memBudget.TryAcquire(budgetBytes) {
		t.Fatal("expected full budget restored after both streams complete")
	}
	s.memBudget.Release(budgetBytes)
}
