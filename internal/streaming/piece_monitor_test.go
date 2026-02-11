package streaming

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/autobrr/go-qbittorrent"

	pb "github.com/arsac/qb-sync/proto"
)

func TestPieceMonitor_Removed_Channel(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	t.Run("channel is created with correct buffer size", func(t *testing.T) {
		monitor := &PieceMonitor{
			logger:    logger,
			torrents:  make(map[string]*torrentState),
			completed: make(chan *pb.Piece, completedChannelBufSize),
			removed:   make(chan string, removedChannelBufSize),
		}

		// Channel should be non-nil and readable
		ch := monitor.Removed()
		if ch == nil {
			t.Error("Removed() should return non-nil channel")
		}

		// Verify it's a receive-only channel by attempting to read (should not block with empty)
		select {
		case <-ch:
			t.Error("channel should be empty initially")
		default:
			// Expected: channel is empty
		}
	})

	t.Run("channel can receive removal notifications", func(t *testing.T) {
		monitor := &PieceMonitor{
			logger:    logger,
			torrents:  make(map[string]*torrentState),
			completed: make(chan *pb.Piece, completedChannelBufSize),
			removed:   make(chan string, removedChannelBufSize),
		}

		// Send a test notification
		testHash := "abc123"
		monitor.removed <- testHash

		// Receive via Removed()
		select {
		case hash := <-monitor.Removed():
			if hash != testHash {
				t.Errorf("expected hash %q, got %q", testHash, hash)
			}
		case <-time.After(time.Second):
			t.Error("timeout waiting for removal notification")
		}
	})

	t.Run("multiple notifications are buffered", func(t *testing.T) {
		monitor := &PieceMonitor{
			logger:    logger,
			torrents:  make(map[string]*torrentState),
			completed: make(chan *pb.Piece, completedChannelBufSize),
			removed:   make(chan string, removedChannelBufSize),
		}

		// Send multiple notifications up to buffer size
		hashes := []string{"hash1", "hash2", "hash3"}
		for _, h := range hashes {
			monitor.removed <- h
		}

		// Verify all can be received
		for i, expected := range hashes {
			select {
			case hash := <-monitor.Removed():
				if hash != expected {
					t.Errorf("notification %d: expected %q, got %q", i, expected, hash)
				}
			case <-time.After(time.Second):
				t.Errorf("timeout waiting for notification %d", i)
			}
		}
	})
}

func TestPieceMonitor_CloseChannels(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	t.Run("closes channels exactly once", func(t *testing.T) {
		monitor := &PieceMonitor{
			logger:    logger,
			torrents:  make(map[string]*torrentState),
			completed: make(chan *pb.Piece, completedChannelBufSize),
			removed:   make(chan string, removedChannelBufSize),
		}

		// First close should succeed
		monitor.closeChannels()

		// closed flag should be set
		if !monitor.closed.Load() {
			t.Error("closed flag should be true after closeChannels()")
		}

		// Removed channel should be closed
		select {
		case _, ok := <-monitor.Removed():
			if ok {
				t.Error("expected channel to be closed")
			}
		default:
			t.Error("channel should be closed, not blocking")
		}

		// Second close should be safe (sync.Once protects)
		monitor.closeChannels() // Should not panic
	})

	t.Run("closed flag prevents sends", func(t *testing.T) {
		monitor := &PieceMonitor{
			logger:    logger,
			torrents:  make(map[string]*torrentState),
			completed: make(chan *pb.Piece, completedChannelBufSize),
			removed:   make(chan string, removedChannelBufSize),
		}

		// Check the closed flag before closing
		if monitor.closed.Load() {
			t.Error("closed flag should be false initially")
		}

		monitor.closeChannels()

		// Verify closed flag is now true
		if !monitor.closed.Load() {
			t.Error("closed flag should be true after closeChannels()")
		}
	})

	t.Run("concurrent closes are safe", func(t *testing.T) {
		monitor := &PieceMonitor{
			logger:    logger,
			torrents:  make(map[string]*torrentState),
			completed: make(chan *pb.Piece, completedChannelBufSize),
			removed:   make(chan string, removedChannelBufSize),
		}

		var wg sync.WaitGroup
		for range 10 {
			wg.Go(func() {
				monitor.closeChannels() // Should not panic
			})
		}
		wg.Wait()

		// Channel should be closed
		if !monitor.closed.Load() {
			t.Error("closed flag should be true")
		}
	})
}

func TestPieceMonitor_Untrack(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	t.Run("removes torrent from tracking", func(t *testing.T) {
		monitor := &PieceMonitor{
			logger:    logger,
			torrents:  make(map[string]*torrentState),
			completed: make(chan *pb.Piece, completedChannelBufSize),
			removed:   make(chan string, removedChannelBufSize),
		}

		hash := "abc123"
		monitor.torrents[hash] = &torrentState{}

		// Verify torrent is tracked
		monitor.mu.RLock()
		_, exists := monitor.torrents[hash]
		monitor.mu.RUnlock()
		if !exists {
			t.Error("torrent should be tracked initially")
		}

		// Untrack
		monitor.Untrack(hash)

		// Verify torrent is no longer tracked
		monitor.mu.RLock()
		_, exists = monitor.torrents[hash]
		monitor.mu.RUnlock()
		if exists {
			t.Error("torrent should not be tracked after Untrack()")
		}
	})

	t.Run("untracking non-existent torrent is safe", func(_ *testing.T) {
		monitor := &PieceMonitor{
			logger:    logger,
			torrents:  make(map[string]*torrentState),
			completed: make(chan *pb.Piece, completedChannelBufSize),
			removed:   make(chan string, removedChannelBufSize),
		}

		// Should not panic
		monitor.Untrack("nonexistent")
	})
}

func TestPieceMonitor_MarkStreamed(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	t.Run("marks piece as streamed", func(t *testing.T) {
		monitor := &PieceMonitor{
			logger:    logger,
			torrents:  make(map[string]*torrentState),
			completed: make(chan *pb.Piece, completedChannelBufSize),
			removed:   make(chan string, removedChannelBufSize),
		}

		hash := "abc123"
		numPieces := 10
		monitor.torrents[hash] = &torrentState{
			streamed: make([]bool, numPieces),
			failed:   make([]bool, numPieces),
		}

		// Mark piece 5 as streamed
		monitor.MarkStreamed(hash, 5)

		// Verify
		state := monitor.torrents[hash]
		state.mu.RLock()
		defer state.mu.RUnlock()

		if !state.streamed[5] {
			t.Error("piece 5 should be marked as streamed")
		}
		if state.failed[5] {
			t.Error("piece 5 failed flag should be false")
		}
	})

	t.Run("clears failed flag when marking streamed", func(t *testing.T) {
		monitor := &PieceMonitor{
			logger:    logger,
			torrents:  make(map[string]*torrentState),
			completed: make(chan *pb.Piece, completedChannelBufSize),
			removed:   make(chan string, removedChannelBufSize),
		}

		hash := "abc123"
		numPieces := 10
		monitor.torrents[hash] = &torrentState{
			streamed: make([]bool, numPieces),
			failed:   make([]bool, numPieces),
		}

		// Set failed flag first
		state := monitor.torrents[hash]
		state.failed[3] = true

		// Mark as streamed
		monitor.MarkStreamed(hash, 3)

		// Verify failed flag is cleared
		state.mu.RLock()
		defer state.mu.RUnlock()

		if state.failed[3] {
			t.Error("failed flag should be cleared when marking streamed")
		}
	})

	t.Run("marking untracked torrent is safe", func(_ *testing.T) {
		monitor := &PieceMonitor{
			logger:    logger,
			torrents:  make(map[string]*torrentState),
			completed: make(chan *pb.Piece, completedChannelBufSize),
			removed:   make(chan string, removedChannelBufSize),
		}

		// Should not panic
		monitor.MarkStreamed("nonexistent", 0)
	})
}

func TestPieceMonitor_MarkStreamedBatch(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	t.Run("marks multiple pieces as streamed", func(t *testing.T) {
		monitor := &PieceMonitor{
			logger:    logger,
			torrents:  make(map[string]*torrentState),
			completed: make(chan *pb.Piece, completedChannelBufSize),
			removed:   make(chan string, removedChannelBufSize),
		}

		hash := "abc123"
		numPieces := 10
		monitor.torrents[hash] = &torrentState{
			streamed: make([]bool, numPieces),
			failed:   make([]bool, numPieces),
		}

		// Mark pieces 0, 2, 4, 6 as written
		written := make([]bool, numPieces)
		written[0] = true
		written[2] = true
		written[4] = true
		written[6] = true

		count := monitor.MarkStreamedBatch(hash, written)

		if count != 4 {
			t.Errorf("expected 4 pieces marked, got %d", count)
		}

		// Verify
		state := monitor.torrents[hash]
		state.mu.RLock()
		defer state.mu.RUnlock()

		for i, expected := range written {
			if state.streamed[i] != expected {
				t.Errorf("piece %d: expected streamed=%v, got %v", i, expected, state.streamed[i])
			}
		}
	})

	t.Run("handles mismatched array sizes", func(t *testing.T) {
		monitor := &PieceMonitor{
			logger:    logger,
			torrents:  make(map[string]*torrentState),
			completed: make(chan *pb.Piece, completedChannelBufSize),
			removed:   make(chan string, removedChannelBufSize),
		}

		hash := "abc123"
		numPieces := 5
		monitor.torrents[hash] = &torrentState{
			streamed: make([]bool, numPieces),
			failed:   make([]bool, numPieces),
		}

		// Written array is larger than torrent's piece count
		written := make([]bool, 10)
		for i := range written {
			written[i] = true
		}

		count := monitor.MarkStreamedBatch(hash, written)

		// Should only mark up to numPieces
		if count != numPieces {
			t.Errorf("expected %d pieces marked, got %d", numPieces, count)
		}
	})

	t.Run("returns 0 for untracked torrent", func(t *testing.T) {
		monitor := &PieceMonitor{
			logger:    logger,
			torrents:  make(map[string]*torrentState),
			completed: make(chan *pb.Piece, completedChannelBufSize),
			removed:   make(chan string, removedChannelBufSize),
		}

		count := monitor.MarkStreamedBatch("nonexistent", []bool{true, true})

		if count != 0 {
			t.Errorf("expected 0 for untracked torrent, got %d", count)
		}
	})
}

func TestPieceMonitor_GetProgress(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	t.Run("returns progress for tracked torrent", func(t *testing.T) {
		monitor := &PieceMonitor{
			logger:    logger,
			torrents:  make(map[string]*torrentState),
			completed: make(chan *pb.Piece, completedChannelBufSize),
			removed:   make(chan string, removedChannelBufSize),
		}

		hash := "abc123"
		numPieces := 10
		monitor.torrents[hash] = &torrentState{
			meta: &TorrentMetadata{
				InitTorrentRequest: &pb.InitTorrentRequest{
					NumPieces: int32(numPieces),
				},
			},
			streamed: make([]bool, numPieces),
			failed:   make([]bool, numPieces),
		}

		// Mark some pieces as streamed and failed
		state := monitor.torrents[hash]
		state.streamed[0] = true
		state.streamed[1] = true
		state.streamed[2] = true
		state.failed[3] = true

		progress, err := monitor.GetProgress(hash)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if progress.TotalPieces != numPieces {
			t.Errorf("expected TotalPieces %d, got %d", numPieces, progress.TotalPieces)
		}
		if progress.Streamed != 3 {
			t.Errorf("expected Streamed 3, got %d", progress.Streamed)
		}
		if progress.Failed != 1 {
			t.Errorf("expected Failed 1, got %d", progress.Failed)
		}
		if progress.Complete {
			t.Error("should not be complete")
		}
	})

	t.Run("returns error for untracked torrent", func(t *testing.T) {
		monitor := &PieceMonitor{
			logger:    logger,
			torrents:  make(map[string]*torrentState),
			completed: make(chan *pb.Piece, completedChannelBufSize),
			removed:   make(chan string, removedChannelBufSize),
		}

		_, err := monitor.GetProgress("nonexistent")
		if !errors.Is(err, ErrTorrentNotTracked) {
			t.Errorf("expected ErrTorrentNotTracked, got %v", err)
		}
	})

	t.Run("reports complete when all pieces streamed", func(t *testing.T) {
		monitor := &PieceMonitor{
			logger:    logger,
			torrents:  make(map[string]*torrentState),
			completed: make(chan *pb.Piece, completedChannelBufSize),
			removed:   make(chan string, removedChannelBufSize),
		}

		hash := "abc123"
		numPieces := 5
		streamed := make([]bool, numPieces)
		for i := range streamed {
			streamed[i] = true
		}

		monitor.torrents[hash] = &torrentState{
			meta: &TorrentMetadata{
				InitTorrentRequest: &pb.InitTorrentRequest{
					NumPieces: int32(numPieces),
				},
			},
			streamed: streamed,
			failed:   make([]bool, numPieces),
		}

		progress, err := monitor.GetProgress(hash)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !progress.Complete {
			t.Error("should be complete when all pieces streamed")
		}
		if progress.Streamed != numPieces {
			t.Errorf("expected all %d pieces streamed, got %d", numPieces, progress.Streamed)
		}
	})
}

func TestPieceMonitor_IsDownloadingState(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	monitor := &PieceMonitor{
		logger:    logger,
		torrents:  make(map[string]*torrentState),
		completed: make(chan *pb.Piece, completedChannelBufSize),
		removed:   make(chan string, removedChannelBufSize),
	}

	// States that should be considered downloading
	downloadingStates := []struct {
		state    qbittorrent.TorrentState
		expected bool
	}{
		{qbittorrent.TorrentStateDownloading, true},
		{qbittorrent.TorrentStateStalledDl, true},
		{qbittorrent.TorrentStateQueuedDl, true},
		{qbittorrent.TorrentStateForcedDl, true},
		{qbittorrent.TorrentStateAllocating, true},
		{qbittorrent.TorrentStateMetaDl, true},
		{qbittorrent.TorrentStateUploading, false},
		{qbittorrent.TorrentStatePausedUp, false},
		{qbittorrent.TorrentStatePausedDl, false},
		{qbittorrent.TorrentStateError, false},
		{qbittorrent.TorrentStateMissingFiles, false},
		{qbittorrent.TorrentStateCheckingDl, false},
		{qbittorrent.TorrentStateCheckingUp, false},
		{qbittorrent.TorrentStateMoving, false},
		{qbittorrent.TorrentStateUnknown, false},
	}

	for _, tc := range downloadingStates {
		t.Run(string(tc.state), func(t *testing.T) {
			result := monitor.isDownloadingState(tc.state)
			if result != tc.expected {
				t.Errorf("isDownloadingState(%q) = %v, want %v", tc.state, result, tc.expected)
			}
		})
	}
}

func TestPieceMonitor_RetryFailed(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	newMonitor := func(chanBuf int) *PieceMonitor {
		return &PieceMonitor{
			logger:    logger,
			torrents:  make(map[string]*torrentState),
			completed: make(chan *pb.Piece, chanBuf),
			removed:   make(chan string, removedChannelBufSize),
		}
	}

	newState := func(numPieces int, pieceSize int64) *torrentState {
		return &torrentState{
			meta: &TorrentMetadata{
				InitTorrentRequest: &pb.InitTorrentRequest{
					TorrentHash: "abc123",
					NumPieces:   int32(numPieces),
					PieceSize:   pieceSize,
					TotalSize:   int64(numPieces) * pieceSize,
				},
			},
			hashes:   make([]string, numPieces),
			streamed: make([]bool, numPieces),
			failed:   make([]bool, numPieces),
		}
	}

	t.Run("re-queues failed pieces", func(t *testing.T) {
		monitor := newMonitor(completedChannelBufSize)
		state := newState(10, 1024)
		state.failed[2] = true
		state.failed[7] = true

		hash := "abc123"
		monitor.torrents[hash] = state

		err := monitor.RetryFailed(t.Context(), hash)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Both pieces should be queued
		var received []int32
		for range 2 {
			select {
			case p := <-monitor.completed:
				received = append(received, p.GetIndex())
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for piece")
			}
		}

		if len(received) != 2 {
			t.Fatalf("expected 2 pieces, got %d", len(received))
		}
		if received[0] != 2 || received[1] != 7 {
			t.Errorf("expected pieces [2, 7], got %v", received)
		}

		// Failed flags should be cleared
		state.mu.RLock()
		if state.failed[2] || state.failed[7] {
			t.Error("failed flags should be cleared after successful send")
		}
		state.mu.RUnlock()
	})

	t.Run("channel full leaves pieces marked failed", func(t *testing.T) {
		// Buffer of 1 — second piece can't be sent
		monitor := newMonitor(1)
		state := newState(10, 1024)
		state.failed[1] = true
		state.failed[3] = true
		state.failed[5] = true

		hash := "abc123"
		monitor.torrents[hash] = state

		err := monitor.RetryFailed(t.Context(), hash)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// One piece should have been sent
		select {
		case <-monitor.completed:
		case <-time.After(time.Second):
			t.Fatal("expected at least one piece in channel")
		}

		// Remaining pieces should still be marked failed
		state.mu.RLock()
		failedCount := 0
		for _, f := range state.failed {
			if f {
				failedCount++
			}
		}
		state.mu.RUnlock()

		if failedCount < 1 {
			t.Error("pieces that couldn't be sent should remain marked failed")
		}
	})

	t.Run("no failed pieces is a no-op", func(t *testing.T) {
		monitor := newMonitor(completedChannelBufSize)
		state := newState(5, 1024)

		hash := "abc123"
		monitor.torrents[hash] = state

		err := monitor.RetryFailed(t.Context(), hash)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Channel should be empty
		select {
		case p := <-monitor.completed:
			t.Errorf("expected no pieces, got index %d", p.GetIndex())
		default:
		}
	})

	t.Run("returns error for untracked torrent", func(t *testing.T) {
		monitor := newMonitor(completedChannelBufSize)

		err := monitor.RetryFailed(t.Context(), "nonexistent")
		if !errors.Is(err, ErrTorrentNotTracked) {
			t.Errorf("expected ErrTorrentNotTracked, got %v", err)
		}
	})

	t.Run("returns context error on cancellation", func(t *testing.T) {
		monitor := newMonitor(0) // unbuffered — all sends will fail
		state := newState(5, 1024)
		state.failed[0] = true

		hash := "abc123"
		monitor.torrents[hash] = state

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // cancel immediately

		err := monitor.RetryFailed(ctx, hash)
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	})
}

func TestPieceMonitor_ResyncStreamed(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	newMonitor := func() *PieceMonitor {
		return &PieceMonitor{
			logger:    logger,
			torrents:  make(map[string]*torrentState),
			completed: make(chan *pb.Piece, completedChannelBufSize),
			removed:   make(chan string, removedChannelBufSize),
		}
	}

	t.Run("resets streamed pieces missing on cold", func(t *testing.T) {
		monitor := newMonitor()
		hash := "abc123"
		numPieces := 10
		monitor.torrents[hash] = &torrentState{
			streamed: make([]bool, numPieces),
			failed:   make([]bool, numPieces),
		}

		// Simulate: hot thinks all 10 are streamed
		state := monitor.torrents[hash]
		for i := range state.streamed {
			state.streamed[i] = true
		}

		// Cold only has 7 pieces (0-6)
		writtenOnCold := make([]bool, numPieces)
		for i := range 7 {
			writtenOnCold[i] = true
		}

		reset := monitor.ResyncStreamed(hash, writtenOnCold)

		if reset != 3 {
			t.Errorf("expected 3 pieces reset, got %d", reset)
		}

		state.mu.RLock()
		defer state.mu.RUnlock()
		for i := range numPieces {
			expected := i < 7
			if state.streamed[i] != expected {
				t.Errorf("piece %d: expected streamed=%v, got %v", i, expected, state.streamed[i])
			}
		}
	})

	t.Run("clears failed flag for pieces cold has", func(t *testing.T) {
		monitor := newMonitor()
		hash := "abc123"
		numPieces := 5
		monitor.torrents[hash] = &torrentState{
			streamed: make([]bool, numPieces),
			failed:   make([]bool, numPieces),
		}

		state := monitor.torrents[hash]
		// Pieces 2 and 3 are failed
		state.failed[2] = true
		state.failed[3] = true

		// Cold has piece 2 but not 3
		writtenOnCold := []bool{false, false, true, false, false}
		monitor.ResyncStreamed(hash, writtenOnCold)

		state.mu.RLock()
		defer state.mu.RUnlock()
		if state.failed[2] {
			t.Error("piece 2 should have failed cleared (cold has it)")
		}
		if state.streamed[2] != true {
			t.Error("piece 2 should be marked streamed")
		}
		// Piece 3: cold doesn't have it, so failed stays as-is (not touched by resync)
	})

	t.Run("no-op when already in sync", func(t *testing.T) {
		monitor := newMonitor()
		hash := "abc123"
		numPieces := 5
		monitor.torrents[hash] = &torrentState{
			streamed: make([]bool, numPieces),
			failed:   make([]bool, numPieces),
		}

		state := monitor.torrents[hash]
		state.streamed[0] = true
		state.streamed[2] = true

		writtenOnCold := []bool{true, false, true, false, false}
		reset := monitor.ResyncStreamed(hash, writtenOnCold)

		if reset != 0 {
			t.Errorf("expected 0 pieces reset, got %d", reset)
		}
	})

	t.Run("handles mismatched sizes", func(t *testing.T) {
		monitor := newMonitor()
		hash := "abc123"
		numPieces := 5
		monitor.torrents[hash] = &torrentState{
			streamed: make([]bool, numPieces),
			failed:   make([]bool, numPieces),
		}

		state := monitor.torrents[hash]
		for i := range state.streamed {
			state.streamed[i] = true
		}

		// Cold reports fewer pieces than tracker has
		writtenOnCold := []bool{true, true, true}
		reset := monitor.ResyncStreamed(hash, writtenOnCold)

		// Pieces 3 and 4 should be reset (out of range = cold doesn't have)
		if reset != 2 {
			t.Errorf("expected 2 pieces reset, got %d", reset)
		}
	})

	t.Run("returns 0 for untracked torrent", func(t *testing.T) {
		monitor := newMonitor()
		reset := monitor.ResyncStreamed("nonexistent", []bool{true})
		if reset != 0 {
			t.Errorf("expected 0 for untracked torrent, got %d", reset)
		}
	})
}

func TestPieceMonitor_RemovalNotification_Integration(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	t.Run("removal notification blocks until received", func(t *testing.T) {
		monitor := &PieceMonitor{
			logger:    logger,
			torrents:  make(map[string]*torrentState),
			completed: make(chan *pb.Piece, completedChannelBufSize),
			removed:   make(chan string, removedChannelBufSize),
		}

		ctx := t.Context()

		// Track a torrent
		hash := "test123"
		monitor.torrents[hash] = &torrentState{}

		// Simulate removal notification in goroutine (like pollMainData would do)
		go func() {
			// This simulates the blocking send in pollMainData
			if !monitor.closed.Load() {
				select {
				case monitor.removed <- hash:
				case <-ctx.Done():
				}
			}
		}()

		// Receive the notification
		select {
		case receivedHash := <-monitor.Removed():
			if receivedHash != hash {
				t.Errorf("expected hash %q, got %q", hash, receivedHash)
			}
		case <-time.After(time.Second):
			t.Error("timeout waiting for removal notification")
		}
	})

	t.Run("closed flag prevents new sends", func(t *testing.T) {
		monitor := &PieceMonitor{
			logger:    logger,
			torrents:  make(map[string]*torrentState),
			completed: make(chan *pb.Piece, completedChannelBufSize),
			removed:   make(chan string, removedChannelBufSize),
		}

		// Close channels
		monitor.closeChannels()

		// Verify closed flag prevents sends
		if !monitor.closed.Load() {
			t.Error("closed flag should be true")
		}

		// Attempting to read from closed channel should return immediately
		select {
		case _, ok := <-monitor.Removed():
			if ok {
				t.Error("channel should be closed")
			}
		default:
			t.Error("reading from closed channel should not block")
		}
	})
}
