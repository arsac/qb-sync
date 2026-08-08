package streaming

import (
	"context"
	"errors"
	"slices"
	"sync"
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
func makeTestPoolWithInflight(t *testing.T, keys []congestion.PieceKey) *StreamPool {
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
		tracker:             NewPieceMonitor(nil, nil, logger, DefaultPieceMonitorConfig()),
		logger:              logger,
		config:              DefaultBidiQueueConfig(),
		pieceHashMismatches: make(map[congestion.PieceKey]int),
	}
}

func TestDrainInFlightPool_ProcessesAcksWithCancelledContext(t *testing.T) {
	// Regression: drainInFlightPool must process acks even when the parent
	// context is already cancelled (the shutdown case). Before the fix,
	// WithTimeout(ctx) derived from a cancelled context expired immediately.

	q := makeDrainTestQueue(t)

	hash := "testhash"
	idx := int32(0)
	key := congestion.PieceKey{Hash: hash, Index: idx}
	pool := makeTestPoolWithInflight(t, []congestion.PieceKey{key})

	// The ack envelope carries the source stream so processAck updates the
	// right window without an external lookup.
	ps := pool.streams[0]

	if pool.TotalInFlight() != 1 {
		t.Fatalf("expected 1 in-flight, got %d", pool.TotalInFlight())
	}

	// Push an ack into the pool's channel paired with the producing stream.
	pool.acks <- AckEnvelope{
		Ack: &pb.PieceAck{
			TorrentHash: hash,
			PieceIndex:  idx,
			Success:     true,
		},
		Stream: ps,
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
	key := congestion.PieceKey{Hash: hash, Index: 0}
	pool := makeTestPoolWithInflight(t, []congestion.PieceKey{key})

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

	// Tracker with a stub torrent state for "testhash" — sendPiecePool
	// drops pieces whose torrent isn't tracked (IsTracked check), so the
	// test must register the torrent for piece flow to reach ReadPiece.
	// IsPieceStreamed returns false for an empty streamed slice; MarkFailed
	// safely no-ops because failed slice is sized to numPieces.
	tracker := NewPieceMonitor(nil, nil, logger, DefaultPieceMonitorConfig())
	tracker.torrents["testhash"] = &torrentState{
		streamed: make([]bool, numPieces),
		failed:   make([]bool, numPieces),
	}

	config := DefaultBidiQueueConfig()
	config.NumSenders = numSenders
	q := &BidiQueue{
		source:              source,
		dest:                dest,
		tracker:             tracker,
		logger:              logger,
		config:              config,
		pieceHashMismatches: make(map[congestion.PieceKey]int),
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

// indexRecordingSource is a mock PieceSource that records the index of every
// piece it is asked to read, then fails the read so the test never needs a
// real gRPC stream.
type indexRecordingSource struct {
	mu      sync.Mutex
	indices []int32
}

func (s *indexRecordingSource) ReadPiece(_ context.Context, p *pb.Piece) ([]byte, error) {
	s.mu.Lock()
	s.indices = append(s.indices, p.GetIndex())
	s.mu.Unlock()
	return nil, errors.New("mock read error")
}

func (s *indexRecordingSource) read() []int32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.indices)
}

func (s *indexRecordingSource) GetPieceStates(context.Context, string) ([]PieceState, error) {
	return nil, nil
}

func (s *indexRecordingSource) GetPieceHashes(context.Context, string) ([]string, error) {
	return nil, nil
}

func (s *indexRecordingSource) GetTorrentMetadata(context.Context, string) (*TorrentMetadata, error) {
	return nil, nil
}

// TestDeliverPiece_RetriesAfterLosingWindowSlot pins that a piece rejected
// because the congestion window was full is kept by the sender and sent once
// capacity frees, rather than handed back to the tracker - which would delay it
// by a full poll interval and count it as a failure.
//
// Deterministic: the window is sized to one slot and pre-filled by the test, so
// the first attempt is guaranteed to lose the race, and the slot is only
// released after the assertion that no read has happened yet.
func TestDeliverPiece_RetriesAfterLosingWindowSlot(t *testing.T) {
	const (
		hash       = "testhash"
		pieceIndex = int32(7)
		numPieces  = 16
	)

	source := &indexRecordingSource{}
	dest := &GRPCDestination{
		initResults: map[string]*InitTorrentResult{hash: {}},
	}
	tracker := NewPieceMonitor(nil, nil, testLogger, DefaultPieceMonitorConfig())
	tracker.torrents[hash] = &torrentState{
		streamed: make([]bool, numPieces),
		failed:   make([]bool, numPieces),
	}

	q := &BidiQueue{
		source:              source,
		dest:                dest,
		tracker:             tracker,
		logger:              testLogger,
		config:              DefaultBidiQueueConfig(),
		pieceHashMismatches: make(map[congestion.PieceKey]int),
	}

	// One stream with a single-slot window, already occupied by another
	// sender's piece: TrySend must fail on the first attempt.
	pool := NewStreamPool(nil, testLogger, StreamPoolConfig{MaxNumStreams: 1, AckChannelSize: 10})
	pool.ctx, pool.cancel = context.WithCancel(context.Background())
	defer pool.cancel()

	window := congestion.NewAdaptiveWindow(congestion.Config{
		MinWindow:     1,
		MaxWindow:     1,
		InitialWindow: 1,
		PieceTimeout:  time.Minute,
	})
	occupant := congestion.PieceKey{Hash: "otherhash", Index: 0}
	window.OnSend(occupant)
	ps := &PooledStream{window: window, id: 0}
	pool.mu.Lock()
	pool.streams = append(pool.streams, ps)
	pool.mu.Unlock()

	if pool.CanSend() {
		t.Fatal("pool should be at window capacity before the test starts")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stopSender := make(chan struct{})
	defer close(stopSender)

	piece := &pb.Piece{TorrentHash: hash, Index: pieceIndex}
	returned := make(chan bool, 1)
	go func() { returned <- q.deliverPiece(ctx, pool, stopSender, piece, 0) }()

	// While the window stays full the piece must be held, not read and not
	// counted as failed. Long enough for a drop-and-return regression to have
	// finished the call.
	time.Sleep(250 * time.Millisecond)
	if got := source.read(); len(got) != 0 {
		t.Fatalf("read the piece while the window was full: %v", got)
	}
	if got := q.piecesFail.Load(); got != 0 {
		t.Fatalf("counted a window-full rejection as a failure: piecesFail=%d", got)
	}
	select {
	case <-returned:
		t.Fatal("deliverPiece gave up the piece instead of waiting for window capacity")
	default:
	}

	// Free the slot: the retry should now claim it and read the same piece.
	pool.AckPiece(ps, occupant)

	select {
	case ok := <-returned:
		if !ok {
			t.Fatal("deliverPiece reported worker exit, want the piece delivered")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("deliverPiece did not retry after window capacity freed")
	}

	if got := source.read(); !slices.Equal(got, []int32{pieceIndex}) {
		t.Errorf("read indices = %v, want exactly [%d]", got, pieceIndex)
	}
	// The retry's read failure - not the window-full rejection - is the one
	// failure this piece is allowed to record.
	if got := q.piecesFail.Load(); got != 1 {
		t.Errorf("piecesFail = %d, want 1 (the read error only)", got)
	}
}

// newInFlightSkipFixture builds the minimum a sendPiecePool call needs: a
// tracked torrent, an initialized destination, and a pool of one stream whose
// window the test drives directly.
func newInFlightSkipFixture(t *testing.T, hash string, numPieces int) (
	*BidiQueue, *StreamPool, *PooledStream, *indexRecordingSource,
) {
	t.Helper()

	source := &indexRecordingSource{}
	tracker := NewPieceMonitor(nil, nil, testLogger, DefaultPieceMonitorConfig())
	tracker.torrents[hash] = &torrentState{
		streamed: make([]bool, numPieces),
		failed:   make([]bool, numPieces),
	}

	q := &BidiQueue{
		source:              source,
		dest:                &GRPCDestination{initResults: map[string]*InitTorrentResult{hash: {}}},
		tracker:             tracker,
		logger:              testLogger,
		config:              DefaultBidiQueueConfig(),
		pieceHashMismatches: make(map[congestion.PieceKey]int),
	}

	pool := NewStreamPool(nil, testLogger, StreamPoolConfig{MaxNumStreams: 1, AckChannelSize: 10})
	pool.ctx, pool.cancel = context.WithCancel(context.Background())
	t.Cleanup(pool.cancel)

	ps := &PooledStream{id: 0, window: congestion.NewAdaptiveWindow(congestion.Config{
		MinWindow:     1,
		MaxWindow:     4,
		InitialWindow: 4,
		PieceTimeout:  time.Minute,
	})}
	pool.mu.Lock()
	pool.streams = append(pool.streams, ps)
	pool.mu.Unlock()

	return q, pool, ps, source
}

// TestSendPiecePool_SkipsPieceAlreadyInFlight pins that a piece the pool already
// has on the wire is dropped instead of read off disk and sent a second time.
// The piece monitor re-offers every un-streamed piece on each poll tick, and a
// piece is only streamed once its ack lands, so without this the tail of every
// torrent re-sends its in-flight pieces once per poll interval.
func TestSendPiecePool_SkipsPieceAlreadyInFlight(t *testing.T) {
	const (
		hash       = "testhash"
		pieceIndex = int32(3)
		numPieces  = 16
	)
	key := congestion.PieceKey{Hash: hash, Index: pieceIndex}

	t.Run("in flight is skipped without a read or a second claim", func(t *testing.T) {
		q, pool, ps, source := newInFlightSkipFixture(t, hash, numPieces)
		ps.window.OnSend(key)

		piece := &pb.Piece{TorrentHash: hash, Index: pieceIndex}
		if err := q.sendPiecePool(t.Context(), pool, piece); err != nil {
			t.Fatalf("sendPiecePool returned %v, want nil for an in-flight piece", err)
		}
		if got := source.read(); len(got) != 0 {
			t.Errorf("read a piece that was already in flight: %v", got)
		}
		if got := ps.window.InFlight(); got != 1 {
			t.Errorf("inFlight = %d, want 1 (no second claim for the same piece)", got)
		}
	})

	t.Run("a draining stream still counts as in flight", func(t *testing.T) {
		q, pool, ps, source := newInFlightSkipFixture(t, hash, numPieces)
		ps.window.OnSend(key)
		ps.draining.Store(true)

		piece := &pb.Piece{TorrentHash: hash, Index: pieceIndex}
		if err := q.sendPiecePool(t.Context(), pool, piece); err != nil {
			t.Fatalf("sendPiecePool returned %v, want nil for an in-flight piece", err)
		}
		if got := source.read(); len(got) != 0 {
			t.Errorf("read a piece still outstanding on a draining stream: %v", got)
		}
	})

	t.Run("a retired piece is sent", func(t *testing.T) {
		q, pool, ps, source := newInFlightSkipFixture(t, hash, numPieces)
		ps.window.OnSend(key)
		pool.FailPiece(ps, key)

		piece := &pb.Piece{TorrentHash: hash, Index: pieceIndex}
		// The mock source fails every read, so reaching it is the assertion.
		if err := q.sendPiecePool(t.Context(), pool, piece); err == nil {
			t.Fatal("sendPiecePool returned nil, want the mock read error")
		}
		if got := source.read(); !slices.Equal(got, []int32{pieceIndex}) {
			t.Errorf("read indices = %v, want exactly [%d]", got, pieceIndex)
		}
	})
}

// makeTestPoolWithStaleKeys creates a StreamPool with the given number of
// streams, each with pieces that become stale after a short sleep.
func makeTestPoolWithStaleKeys(t *testing.T, streamKeys [][]congestion.PieceKey) (*StreamPool, []*PooledStream) {
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
	hash1Piece := congestion.PieceKey{Hash: "hash1", Index: 0}
	hash2Piece := congestion.PieceKey{Hash: "hash2", Index: 0}
	pool, streams := makeTestPoolWithStaleKeys(t, [][]congestion.PieceKey{
		{hash1Piece},
		{hash2Piece},
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
	byKey := make(map[congestion.PieceKey]*PooledStream, len(stale))
	for _, sk := range stale {
		byKey[sk.Key] = sk.Stream
	}

	if byKey[hash1Piece] != streams[0] {
		t.Errorf("hash1 piece 0 should be paired with stream 0, got stream %d", byKey[hash1Piece].id)
	}
	if byKey[hash2Piece] != streams[1] {
		t.Errorf("hash2 piece 0 should be paired with stream 1, got stream %d", byKey[hash2Piece].id)
	}
}

func TestHandleStalePiecesPool_RemovesFromCorrectWindow(t *testing.T) {
	// Stale pieces are reclaimed from the window of the stream that actually
	// owns them, sourced via pool.GetAllStaleKeys() (which iterates each
	// stream's own inflight map). No external piece-to-stream map is involved.
	pool, streams := makeTestPoolWithStaleKeys(t, [][]congestion.PieceKey{
		{{Hash: "hash", Index: 0}}, // streamA — will have the stale key
		{},                         // streamB — empty
	})
	defer pool.cancel()

	streamA := streams[0]
	q := makeDrainTestQueue(t)

	// Wait for the piece in streamA to become stale.
	time.Sleep(60 * time.Millisecond)

	if streamA.window.InFlight() != 1 {
		t.Fatalf("streamA should have 1 in-flight, got %d", streamA.window.InFlight())
	}

	q.handleStalePiecesPool(context.Background(), pool)

	if streamA.window.InFlight() != 0 {
		t.Errorf("streamA should have 0 in-flight after stale cleanup, got %d",
			streamA.window.InFlight())
	}
}

// TestRequeuePiece_MarksTheKeysOwnPiece pins that a piece leaving the
// congestion window without an ack is requeued as itself. The key carries the
// hash and index the sender put in flight, so a requeue that reads the wrong
// field re-offers some other piece and abandons this one forever.
func TestRequeuePiece_MarksTheKeysOwnPiece(t *testing.T) {
	const piecesPerTorrent = 8

	q := makeDrainTestQueue(t)
	for _, hash := range []string{"hashA", "hashB"} {
		q.tracker.torrents[hash] = &torrentState{
			streamed: make([]bool, piecesPerTorrent),
			failed:   make([]bool, piecesPerTorrent),
		}
	}

	// Distinct index per torrent, so reading the wrong field of the key marks
	// a piece that is not in flight rather than landing on the right one.
	inFlight := []congestion.PieceKey{
		{Hash: "hashA", Index: 2},
		{Hash: "hashB", Index: 5},
	}
	pool := makeTestPoolWithInflight(t, inFlight)
	defer pool.cancel()

	q.markInFlightAsFailedPool(context.Background(), pool)

	for _, key := range inFlight {
		state := q.tracker.torrents[key.Hash]
		state.mu.Lock()
		failed := slices.Clone(state.failed)
		state.mu.Unlock()

		for i, isFailed := range failed {
			want := i == int(key.Index)
			if isFailed != want {
				t.Errorf("%s piece %d failed = %v, want %v", key.Hash, i, isFailed, want)
			}
		}
	}
}

// parkingSource is a mock PieceSource whose ReadPiece blocks until release is
// closed, so the number of senders that got past the dequeue is observable as
// the number of calls parked inside it.
type parkingSource struct {
	release   chan struct{}
	entered   atomic.Int32
	inFlight  atomic.Int32
	maxInFlig atomic.Int32
}

func (s *parkingSource) ReadPiece(_ context.Context, _ *pb.Piece) ([]byte, error) {
	s.entered.Add(1)
	c := s.inFlight.Add(1)
	defer s.inFlight.Add(-1)
	for {
		old := s.maxInFlig.Load()
		if c <= old || s.maxInFlig.CompareAndSwap(old, c) {
			break
		}
	}
	<-s.release
	return nil, errors.New("mock read error")
}

func (s *parkingSource) GetPieceStates(context.Context, string) ([]PieceState, error) {
	return nil, nil
}
func (s *parkingSource) GetPieceHashes(context.Context, string) ([]string, error) { return nil, nil }

func (s *parkingSource) GetTorrentMetadata(context.Context, string) (*TorrentMetadata, error) {
	return nil, nil
}

// TestSenderWorkers_TrackTheStreamCount pins the rule that makes the pool's
// adaptive stream scaling mean anything: a sender only dequeues while there is
// a stream for it to drive, and a stream added to the pool wakes the sender
// that was parked for want of one.
//
// Two senders are started against a one-stream pool with NumSenders=1, so the
// second is outside the active set. Both pieces are already queued, so without
// the gate both senders would be inside ReadPiece immediately.
func TestSenderWorkers_TrackTheStreamCount(t *testing.T) {
	const numPieces = 2

	// Unwind order matters: the senders have to be released from ReadPiece and
	// told to stop before the test can join them.
	var wg sync.WaitGroup
	defer wg.Wait()

	source := &parkingSource{release: make(chan struct{})}
	defer close(source.release)

	dest := &GRPCDestination{
		initResults: map[string]*InitTorrentResult{"testhash": {}},
	}
	tracker := NewPieceMonitor(nil, nil, testLogger, DefaultPieceMonitorConfig())
	tracker.torrents["testhash"] = &torrentState{
		streamed: make([]bool, numPieces),
		failed:   make([]bool, numPieces),
	}

	config := DefaultBidiQueueConfig()
	config.NumSenders = 1
	q := &BidiQueue{
		source:              source,
		dest:                dest,
		tracker:             tracker,
		logger:              testLogger,
		config:              config,
		pieceHashMismatches: make(map[congestion.PieceKey]int),
	}

	pool := makeTestPoolWithInflight(t, nil)
	defer pool.cancel()

	for i := range numPieces {
		tracker.completed <- &pb.Piece{TorrentHash: "testhash", Index: int32(i)}
	}

	ctx := t.Context()
	stopSender := make(chan struct{})
	defer close(stopSender)

	for id := range 2 {
		wg.Go(func() { q.senderWorker(ctx, pool, stopSender, id) })
	}

	waitFor(t, "a sender to reach ReadPiece", func() bool {
		return source.inFlight.Load() > 0
	})
	// Give the parked sender every chance to dequeue anyway.
	time.Sleep(100 * time.Millisecond)
	if got := source.maxInFlig.Load(); got != 1 {
		t.Fatalf("senders inside ReadPiece with one stream = %d, want 1", got)
	}

	// Scale the pool up the way probeStream does, including the wakeup.
	pool.mu.Lock()
	pool.streams = append(pool.streams, &PooledStream{
		window: congestion.NewAdaptiveWindow(congestion.DefaultConfig()),
		id:     1,
	})
	pool.mu.Unlock()
	pool.publishCapacity()

	waitFor(t, "the parked sender to start driving the new stream", func() bool {
		return source.maxInFlig.Load() == 2
	})

	// A draining stream is not claimable, so it must not entitle a sender to a
	// turn either.
	pool.mu.RLock()
	pool.streams[1].draining.Store(true)
	pool.mu.RUnlock()
	if got := pool.SendableStreamCount(); got != 1 {
		t.Errorf("SendableStreamCount with one stream draining = %d, want 1", got)
	}
}

// waitFor polls cond until it holds or the deadline passes.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
