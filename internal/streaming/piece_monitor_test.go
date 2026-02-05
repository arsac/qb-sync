package streaming

import (
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
