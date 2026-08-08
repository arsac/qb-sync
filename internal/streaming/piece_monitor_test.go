package streaming

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/autobrr/go-qbittorrent"

	pb "github.com/arsac/qb-sync/proto"
)

// newTestMonitor builds a minimal PieceMonitor for unit tests.
func newTestMonitor() *PieceMonitor {
	completedBuf := completedChannelBufSize
	return &PieceMonitor{
		logger:    testLogger,
		torrents:  make(map[string]*torrentState),
		completed: make(chan *pb.Piece, completedBuf),
		removed:   make(chan string, removedChannelBufSize),
	}
}

// newTestState builds a torrentState with empty streamed/failed slices.
// numPieces sets both slice lengths and the meta NumPieces field.
func newTestState(numPieces int) *torrentState {
	return &torrentState{
		meta: &TorrentMetadata{
			InitTorrentRequest: &pb.InitTorrentRequest{
				NumPieces: int32(numPieces),
			},
		},
		streamed: make([]bool, numPieces),
		failed:   make([]bool, numPieces),
	}
}

func TestPieceMonitor_Removed_Channel(t *testing.T) {
	t.Run("channel is created with correct buffer size", func(t *testing.T) {
		monitor := newTestMonitor()

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
		monitor := newTestMonitor()

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
		monitor := newTestMonitor()

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
	t.Run("closes channels exactly once", func(t *testing.T) {
		monitor := newTestMonitor()

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
		monitor := newTestMonitor()

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
		monitor := newTestMonitor()

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
	t.Run("removes torrent from tracking", func(t *testing.T) {
		monitor := newTestMonitor()

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
		monitor := newTestMonitor()

		// Should not panic
		monitor.Untrack("nonexistent")
	})
}

func TestPieceMonitor_MarkStreamed(t *testing.T) {
	t.Run("marks piece as streamed", func(t *testing.T) {
		monitor := newTestMonitor()

		hash := "abc123"
		numPieces := 10
		monitor.torrents[hash] = newTestState(numPieces)

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
		monitor := newTestMonitor()

		hash := "abc123"
		numPieces := 10
		monitor.torrents[hash] = newTestState(numPieces)

		// Set failed flag first
		state := monitor.torrents[hash]
		state.markFailed(3)

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
		monitor := newTestMonitor()

		// Should not panic
		monitor.MarkStreamed("nonexistent", 0)
	})
}

func TestPieceMonitor_MarkStreamedBatch(t *testing.T) {
	t.Run("marks multiple pieces as streamed", func(t *testing.T) {
		monitor := newTestMonitor()

		hash := "abc123"
		numPieces := 10
		monitor.torrents[hash] = newTestState(numPieces)

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
		monitor := newTestMonitor()

		hash := "abc123"
		numPieces := 5
		monitor.torrents[hash] = newTestState(numPieces)

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
		monitor := newTestMonitor()

		count := monitor.MarkStreamedBatch("nonexistent", []bool{true, true})

		if count != 0 {
			t.Errorf("expected 0 for untracked torrent, got %d", count)
		}
	})
}

func TestPieceMonitor_GetProgress(t *testing.T) {
	t.Run("returns progress for tracked torrent", func(t *testing.T) {
		monitor := newTestMonitor()

		hash := "abc123"
		numPieces := 10
		monitor.torrents[hash] = newTestState(numPieces)

		// Mark some pieces as streamed and failed
		state := monitor.torrents[hash]
		state.markStreamed(0)
		state.markStreamed(1)
		state.markStreamed(2)
		state.markFailed(3)

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
		monitor := newTestMonitor()

		_, err := monitor.GetProgress("nonexistent")
		if !errors.Is(err, ErrTorrentNotTracked) {
			t.Errorf("expected ErrTorrentNotTracked, got %v", err)
		}
	})

	t.Run("reports complete when all pieces streamed", func(t *testing.T) {
		monitor := newTestMonitor()

		hash := "abc123"
		numPieces := 5
		state := newTestState(numPieces)
		for i := range state.streamed {
			state.markStreamed(i)
		}
		monitor.torrents[hash] = state

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
	monitor := newTestMonitor()

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

func TestPieceMonitor_ResyncStreamed(t *testing.T) {
	t.Run("resets streamed pieces missing on destination", func(t *testing.T) {
		monitor := newTestMonitor()
		hash := "abc123"
		numPieces := 10
		monitor.torrents[hash] = newTestState(numPieces)

		// Simulate: source thinks all 10 are streamed
		state := monitor.torrents[hash]
		for i := range state.streamed {
			state.markStreamed(i)
		}

		// Destination only has 7 pieces (0-6)
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

	t.Run("clears failed flag for pieces destination has", func(t *testing.T) {
		monitor := newTestMonitor()
		hash := "abc123"
		numPieces := 5
		monitor.torrents[hash] = newTestState(numPieces)

		state := monitor.torrents[hash]
		// Pieces 2 and 3 are failed
		state.markFailed(2)
		state.markFailed(3)

		// Destination has piece 2 but not 3
		writtenOnCold := []bool{false, false, true, false, false}
		monitor.ResyncStreamed(hash, writtenOnCold)

		state.mu.RLock()
		defer state.mu.RUnlock()
		if state.failed[2] {
			t.Error("piece 2 should have failed cleared (destination has it)")
		}
		if state.streamed[2] != true {
			t.Error("piece 2 should be marked streamed")
		}
		// Piece 3: destination doesn't have it, so failed stays as-is (not touched by resync)
	})

	t.Run("no-op when already in sync", func(t *testing.T) {
		monitor := newTestMonitor()
		hash := "abc123"
		numPieces := 5
		monitor.torrents[hash] = newTestState(numPieces)

		state := monitor.torrents[hash]
		state.markStreamed(0)
		state.markStreamed(2)

		writtenOnCold := []bool{true, false, true, false, false}
		reset := monitor.ResyncStreamed(hash, writtenOnCold)

		if reset != 0 {
			t.Errorf("expected 0 pieces reset, got %d", reset)
		}
	})

	t.Run("handles mismatched sizes", func(t *testing.T) {
		monitor := newTestMonitor()
		hash := "abc123"
		numPieces := 5
		monitor.torrents[hash] = newTestState(numPieces)

		state := monitor.torrents[hash]
		for i := range state.streamed {
			state.markStreamed(i)
		}

		// Destination reports fewer pieces than tracker has
		writtenOnCold := []bool{true, true, true}
		reset := monitor.ResyncStreamed(hash, writtenOnCold)

		// Pieces 3 and 4 should be reset (out of range = destination doesn't have)
		if reset != 2 {
			t.Errorf("expected 2 pieces reset, got %d", reset)
		}
	})

	t.Run("returns 0 for untracked torrent", func(t *testing.T) {
		monitor := newTestMonitor()
		reset := monitor.ResyncStreamed("nonexistent", []bool{true})
		if reset != 0 {
			t.Errorf("expected 0 for untracked torrent, got %d", reset)
		}
	})
}

func TestDeselectedPieceMask(t *testing.T) {
	t.Run("nil when all files selected", func(t *testing.T) {
		files := []*pb.FileInfo{
			{Path: "a.mp3", Size: 1000, Offset: 0, Selected: true},
			{Path: "b.mp3", Size: 1000, Offset: 1000, Selected: true},
		}
		mask := DeselectedPieceMask(files, 4, 500, 2000)
		if mask != nil {
			t.Error("expected nil when all files selected")
		}
	})

	t.Run("marks interior pieces in deselected files", func(t *testing.T) {
		// 3 files, 500 bytes each, piece size 100 → 15 pieces total
		// File 0 (selected):   offset 0-499   → pieces 0-4
		// File 1 (deselected): offset 500-999  → pieces 5-9
		// File 2 (selected):   offset 1000-1499 → pieces 10-14
		files := []*pb.FileInfo{
			{Path: "a.mp3", Size: 500, Offset: 0, Selected: true},
			{Path: "b.mp3", Size: 500, Offset: 500, Selected: false},
			{Path: "c.mp3", Size: 500, Offset: 1000, Selected: true},
		}
		mask := DeselectedPieceMask(files, 15, 100, 1500)
		if mask == nil {
			t.Fatal("expected non-nil mask")
		}
		if len(mask) != 15 {
			t.Fatalf("expected 15 entries, got %d", len(mask))
		}

		// Pieces 0-4: selected file only → false
		for i := range 5 {
			if mask[i] {
				t.Errorf("piece %d should NOT be deselected (in selected file)", i)
			}
		}
		// Pieces 5-9: deselected file only → true
		for i := 5; i < 10; i++ {
			if !mask[i] {
				t.Errorf("piece %d should be deselected (in deselected file)", i)
			}
		}
		// Pieces 10-14: selected file only → false
		for i := 10; i < 15; i++ {
			if mask[i] {
				t.Errorf("piece %d should NOT be deselected (in selected file)", i)
			}
		}
	})

	t.Run("boundary pieces are not deselected", func(t *testing.T) {
		// 2 files, piece size spans both
		// File 0 (selected):   offset 0-299   → ends mid-piece 0 (piece 0-499)
		// File 1 (deselected): offset 300-599
		files := []*pb.FileInfo{
			{Path: "a.mp3", Size: 300, Offset: 0, Selected: true},
			{Path: "b.mp3", Size: 300, Offset: 300, Selected: false},
		}
		// Piece 0: 0-499 → overlaps both files → NOT deselected (selected file overlaps)
		// Piece 1: 500-599 → only in deselected file → deselected
		mask := DeselectedPieceMask(files, 2, 500, 600)
		if mask == nil {
			t.Fatal("expected non-nil mask")
		}
		if mask[0] {
			t.Error("boundary piece 0 should NOT be deselected")
		}
		// Piece 1: offset 500-599, file 1 is offset 300-599 → overlaps
		// Since file 1 is deselected and no selected file overlaps piece 1
		if !mask[1] {
			t.Error("piece 1 should be deselected (only deselected file)")
		}
	})

	t.Run("nil for zero pieces", func(t *testing.T) {
		files := []*pb.FileInfo{
			{Path: "a.mp3", Size: 100, Offset: 0, Selected: false},
		}
		mask := DeselectedPieceMask(files, 0, 100, 100)
		if mask != nil {
			t.Error("expected nil for zero pieces")
		}
	})

	t.Run("nil for zero piece size", func(t *testing.T) {
		files := []*pb.FileInfo{
			{Path: "a.mp3", Size: 100, Offset: 0, Selected: false},
		}
		mask := DeselectedPieceMask(files, 10, 0, 100)
		if mask != nil {
			t.Error("expected nil for zero piece size")
		}
	})

	t.Run("all files deselected marks all pieces", func(t *testing.T) {
		files := []*pb.FileInfo{
			{Path: "a.mp3", Size: 500, Offset: 0, Selected: false},
			{Path: "b.mp3", Size: 500, Offset: 500, Selected: false},
		}
		mask := DeselectedPieceMask(files, 10, 100, 1000)
		if mask == nil {
			t.Fatal("expected non-nil mask")
		}
		for i, d := range mask {
			if !d {
				t.Errorf("piece %d should be deselected (all files deselected)", i)
			}
		}
	})
}

// deselectedPieceMaskFullScan is the pre-narrowing implementation, kept as an
// oracle: it visits every file for every piece, so it cannot drop the first or
// last overlapping file the way a mis-stated binary search can.
func deselectedPieceMaskFullScan(files []*pb.FileInfo, numPieces int32, pieceSize, totalSize int64) []bool {
	mask := make([]bool, numPieces)
	for pieceIdx := range numPieces {
		pieceStart := int64(pieceIdx) * pieceSize
		pieceEnd := min(pieceStart+pieceSize, totalSize)
		allDeselected := true
		for _, f := range files {
			fEnd := f.GetOffset() + f.GetSize()
			if f.GetOffset() < pieceEnd && fEnd > pieceStart && f.GetSelected() {
				allDeselected = false
				break
			}
		}
		mask[pieceIdx] = allDeselected
	}
	return mask
}

// mixedSelectionLayout builds an offset-sorted, contiguous file list mixing the
// shapes that distinguish a correct narrowing from a subtly wrong one: files
// smaller than a piece, files that end exactly on a piece boundary, files
// spanning several pieces, and zero-length files wedged at file boundaries
// (which end exactly where the next file starts).
func mixedSelectionLayout(pieceSize int64) ([]*pb.FileInfo, int32, int64) {
	sizes := []int64{
		pieceSize / 3, 0, pieceSize, pieceSize * 3, 1, 0,
		pieceSize*2 + 1, pieceSize - 1, 0, pieceSize / 2,
	}
	var files []*pb.FileInfo
	var offset int64
	for i := range 40 {
		size := sizes[i%len(sizes)]
		files = append(files, &pb.FileInfo{
			Path:     fmt.Sprintf("f%02d.bin", i),
			Size:     size,
			Offset:   offset,
			Selected: i%3 != 0,
		})
		offset += size
	}
	return files, int32((offset + pieceSize - 1) / pieceSize), offset
}

func TestDeselectedPieceMask_MatchesFullScan(t *testing.T) {
	for _, pieceSize := range []int64{16, 100, 4096} {
		files, numPieces, totalSize := mixedSelectionLayout(pieceSize)

		got := DeselectedPieceMask(files, numPieces, pieceSize, totalSize)
		want := deselectedPieceMaskFullScan(files, numPieces, pieceSize, totalSize)
		if !slices.Equal(got, want) {
			t.Fatalf("pieceSize %d: narrowed mask differs from full scan\ngot  %v\nwant %v",
				pieceSize, got, want)
		}
		// Guard against a layout that makes the comparison vacuous.
		if !slices.Contains(want, true) || !slices.Contains(want, false) {
			t.Fatalf("pieceSize %d: layout produced a uniform mask, test proves nothing", pieceSize)
		}
	}
}

func BenchmarkDeselectedPieceMask(b *testing.B) {
	const pieceSize = 4096
	files, numPieces, totalSize := mixedSelectionLayout(pieceSize)
	b.ResetTimer()
	for b.Loop() {
		DeselectedPieceMask(files, numPieces, pieceSize, totalSize)
	}
}

func TestPieceMonitor_RemovalNotification_Integration(t *testing.T) {
	t.Run("removal notification blocks until received", func(t *testing.T) {
		monitor := newTestMonitor()

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
		monitor := newTestMonitor()

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

// stallProbeSource is a minimal PieceSource whose pieces are all downloaded on
// the source but which never yields any data - the stall shape.
type stallProbeSource struct{ numPieces int }

func (s *stallProbeSource) GetPieceStates(context.Context, string) ([]PieceState, error) {
	states := make([]PieceState, s.numPieces)
	for i := range states {
		states[i] = PieceStateDownloaded
	}
	return states, nil
}

func (s *stallProbeSource) GetPieceHashes(context.Context, string) ([]string, error) {
	return make([]string, s.numPieces), nil
}

func (s *stallProbeSource) GetTorrentMetadata(context.Context, string) (*TorrentMetadata, error) {
	return &TorrentMetadata{InitTorrentRequest: &pb.InitTorrentRequest{
		TorrentHash: "h1",
		NumPieces:   int32(s.numPieces),
		PieceSize:   1,
		TotalSize:   int64(s.numPieces),
	}}, nil
}

func (s *stallProbeSource) ReadPiece(context.Context, *pb.Piece) ([]byte, error) {
	return nil, errors.New("unreadable")
}

// TestGetProgress_ReportsTheStallSignal guards the stalled case.
//
// lastAdvance only moves when a piece transitions to streamed, so a torrent
// whose source data cannot be read never sets it. Left at the zero time, the
// orchestrator could not distinguish "never advanced" from "no information" -
// and torrents that never advance at all are exactly what stall detection
// exists to quarantine. An earlier version of this feature excluded them.
func TestGetProgress_ReportsTheStallSignal(t *testing.T) {
	t.Parallel()

	const numPieces = 4
	monitor := newTestMonitor()
	monitor.source = &stallProbeSource{numPieces: numPieces}

	before := time.Now()
	if err := monitor.startTracking(context.Background(), "h1", nil); err != nil {
		t.Fatalf("startTracking: %v", err)
	}

	progress, err := monitor.GetProgress("h1")
	if err != nil {
		t.Fatalf("GetProgress: %v", err)
	}

	if progress.LastAdvance.IsZero() {
		t.Error("a torrent that has never streamed a piece must still carry a start time")
	}
	if progress.LastAdvance.Before(before) {
		t.Error("lastAdvance should be stamped when tracking begins")
	}
	if progress.Streamed != 0 {
		t.Errorf("Streamed = %d, want 0", progress.Streamed)
	}
	if progress.Available != numPieces {
		t.Errorf("Available = %d, want %d: every piece is downloaded on the source and unstreamed",
			progress.Available, numPieces)
	}
}

// fanoutProbeSource records the concurrency pollActiveTorrents achieves and
// returns a piece-state pattern derived from the requested hash, so a poll that
// applied one torrent's states to another's state is detectable.
//
// Each call parks until releaseTarget calls are simultaneously in flight. A
// serial poll can never reach that, so a watchdog started on the first call
// releases everyone after fanoutProbeTimeout - long enough that the fan-out
// wins the race on any machine, short enough that a regression fails rather
// than hangs.
type fanoutProbeSource struct {
	numPieces     int
	releaseTarget int64

	inFlight atomic.Int64
	maxSeen  atomic.Int64

	releaseOnce sync.Once
	release     chan struct{}
	armOnce     sync.Once

	callsMu sync.Mutex
	calls   map[string]int
}

const fanoutProbeTimeout = 2 * time.Second

func newFanoutProbeSource(numPieces int, releaseTarget int64) *fanoutProbeSource {
	return &fanoutProbeSource{
		numPieces:     numPieces,
		releaseTarget: releaseTarget,
		release:       make(chan struct{}),
		calls:         make(map[string]int),
	}
}

func (s *fanoutProbeSource) releaseAll() {
	s.releaseOnce.Do(func() { close(s.release) })
}

// downloadedPiece maps a hash to the single piece index it reports as
// downloaded: "t03" -> 3. Distinct per torrent so a cross-wired result is a
// mismatch rather than an indistinguishable duplicate.
func (s *fanoutProbeSource) downloadedPiece(hash string) int {
	idx := 0
	for _, c := range hash[1:] {
		idx = idx*10 + int(c-'0')
	}
	return idx
}

func (s *fanoutProbeSource) GetPieceStates(_ context.Context, hash string) ([]PieceState, error) {
	s.armOnce.Do(func() { time.AfterFunc(fanoutProbeTimeout, s.releaseAll) })

	n := s.inFlight.Add(1)
	for {
		seen := s.maxSeen.Load()
		if n <= seen || s.maxSeen.CompareAndSwap(seen, n) {
			break
		}
	}
	if n >= s.releaseTarget {
		s.releaseAll()
	}
	<-s.release
	s.inFlight.Add(-1)

	s.callsMu.Lock()
	s.calls[hash]++
	s.callsMu.Unlock()

	states := make([]PieceState, s.numPieces)
	states[s.downloadedPiece(hash)] = PieceStateDownloaded
	return states, nil
}

func (s *fanoutProbeSource) GetPieceHashes(context.Context, string) ([]string, error) {
	return make([]string, s.numPieces), nil
}

func (s *fanoutProbeSource) GetTorrentMetadata(context.Context, string) (*TorrentMetadata, error) {
	return nil, errors.New("not used")
}

func (s *fanoutProbeSource) ReadPiece(context.Context, *pb.Piece) ([]byte, error) {
	return nil, errors.New("not used")
}

// TestPollActiveTorrents_FansOutWithoutCrossWiring pins both halves of the
// concurrent poll: the per-torrent piece-state fetches overlap (a serial loop
// only ever reaches an in-flight count of 1), and every torrent's states are
// applied to its own torrentState.
func TestPollActiveTorrents_FansOutWithoutCrossWiring(t *testing.T) {
	t.Parallel()

	const (
		numTorrents = 20
		numPieces   = 32
	)

	src := newFanoutProbeSource(numPieces, pollPieceStatesConcurrency)
	monitor := newTestMonitor()
	monitor.source = src

	hashes := make([]string, numTorrents)
	for i := range numTorrents {
		hash := fmt.Sprintf("t%02d", i)
		hashes[i] = hash
		state := newTestState(numPieces)
		state.meta.TorrentHash = hash
		state.meta.PieceSize = 1
		state.meta.TotalSize = numPieces
		monitor.torrents[hash] = state
	}

	monitor.pollActiveTorrents(context.Background())

	if got := src.maxSeen.Load(); got < pollPieceStatesConcurrency {
		t.Errorf("max concurrent GetPieceStates = %d, want %d: piece-state polls did not overlap",
			got, pollPieceStatesConcurrency)
	}

	src.callsMu.Lock()
	calls := len(src.calls)
	src.callsMu.Unlock()
	if calls != numTorrents {
		t.Errorf("polled %d distinct torrents, want %d", calls, numTorrents)
	}

	for _, hash := range hashes {
		state := monitor.torrents[hash]
		want := src.downloadedPiece(hash)
		for i, ps := range state.lastStates {
			downloaded := ps == PieceStateDownloaded
			if downloaded != (i == want) {
				t.Fatalf("%s: piece %d downloaded=%v, want downloaded only at %d "+
					"(states applied to the wrong torrent)", hash, i, downloaded, want)
			}
		}
	}

	// Each torrent queues exactly its own downloaded piece.
	queued := make(map[string]int32)
	for range numTorrents {
		select {
		case piece := <-monitor.Completed():
			queued[piece.GetTorrentHash()] = piece.GetIndex()
		default:
			t.Fatalf("expected %d queued pieces, got %d", numTorrents, len(queued))
		}
	}
	for _, hash := range hashes {
		idx, ok := queued[hash]
		if !ok {
			t.Errorf("%s queued no piece", hash)
			continue
		}
		if int(idx) != src.downloadedPiece(hash) {
			t.Errorf("%s queued piece %d, want %d", hash, idx, src.downloadedPiece(hash))
		}
	}
}

// newBacklogState builds a torrentState whose pieces are all downloaded and
// none streamed, i.e. the state of a torrent whose data is already on disk when
// the sync starts.
func newBacklogState(numPieces int) (*torrentState, []PieceState) {
	st := newTestState(numPieces)
	st.meta.InitTorrentRequest.TorrentHash = "backlog"
	st.meta.InitTorrentRequest.PieceSize = 1 << 20
	st.meta.InitTorrentRequest.TotalSize = int64(numPieces) << 20
	st.hashes = make([]string, numPieces)

	current := make([]PieceState, numPieces)
	for i := range current {
		current[i] = PieceStateDownloaded
	}
	return st, current
}

func TestQueueCompletedPieces_StopsWhenTheQueueIsFull(t *testing.T) {
	ctx := context.Background()

	t.Run("queues every eligible piece while there is room", func(t *testing.T) {
		monitor := newTestMonitor()
		st, current := newBacklogState(6)
		current[1] = PieceStateNotDownloaded
		st.markStreamed(3)

		if got := monitor.queueCompletedPieces(ctx, st, current); got != 4 {
			t.Fatalf("queued %d pieces, want 4", got)
		}
		var indices []int32
		for len(monitor.completed) > 0 {
			indices = append(indices, (<-monitor.completed).GetIndex())
		}
		if !slices.Equal(indices, []int32{0, 2, 4, 5}) {
			t.Errorf("queued indices %v, want [0 2 4 5]", indices)
		}
	})

	t.Run("fills the queue to capacity and stops there", func(t *testing.T) {
		monitor := newTestMonitor()
		monitor.completed = make(chan *pb.Piece, 3)
		st, current := newBacklogState(500)

		if got := monitor.queueCompletedPieces(ctx, st, current); got != 3 {
			t.Fatalf("queued %d pieces, want 3", got)
		}
		var indices []int32
		for len(monitor.completed) > 0 {
			indices = append(indices, (<-monitor.completed).GetIndex())
		}
		if !slices.Equal(indices, []int32{0, 1, 2}) {
			t.Errorf("queued indices %v, want [0 1 2]", indices)
		}
	})

	// Re-offering is the only retry mechanism the scan has: it tracks streamed,
	// not queued, so a piece a sender already dequeued but whose ack has not
	// landed is offered again on the next tick. That is what makes the sender's
	// in-flight skip (see TestSendPiecePool_SkipsPieceAlreadyInFlight) load
	// bearing rather than defensive - without it those offers become a second
	// disk read and a second copy over the link.
	t.Run("re-offers a dequeued piece whose ack has not landed", func(t *testing.T) {
		monitor := newTestMonitor()
		st, current := newBacklogState(4)

		if got := monitor.queueCompletedPieces(ctx, st, current); got != 4 {
			t.Fatalf("queued %d pieces, want 4", got)
		}
		// A sender takes piece 0 off the queue and is still waiting on its ack.
		if idx := (<-monitor.completed).GetIndex(); idx != 0 {
			t.Fatalf("dequeued piece %d, want 0", idx)
		}

		// Every un-streamed piece is offered again: the three still sitting in
		// the queue and the one on the wire.
		if got := monitor.queueCompletedPieces(ctx, st, current); got != 4 {
			t.Fatalf("re-queued %d pieces on the next tick, want 4", got)
		}
		var queued []int32
		for len(monitor.completed) > 0 {
			queued = append(queued, (<-monitor.completed).GetIndex())
		}
		if !slices.Equal(queued, []int32{1, 2, 3, 0, 1, 2, 3}) {
			t.Errorf("queue contents %v, want [1 2 3 0 1 2 3] (every un-streamed piece offered twice)", queued)
		}
	})

	// The scan is what holds the torrent's write lock, which every send and ack
	// contends on, so a full queue has to cost O(1) rather than one discarded
	// *pb.Piece per remaining index.
	t.Run("an already-full queue costs constant work", func(t *testing.T) {
		const numPieces = 2000
		monitor := newTestMonitor()
		monitor.completed = make(chan *pb.Piece, 1)
		monitor.completed <- &pb.Piece{}
		monitor.queueFullLogNano.Store(time.Now().UnixNano()) // suppress the rate-limited warn
		st, current := newBacklogState(numPieces)

		allocs := testing.AllocsPerRun(20, func() {
			if got := monitor.queueCompletedPieces(ctx, st, current); got != 0 {
				t.Fatalf("queued %d pieces into a full queue, want 0", got)
			}
		})
		if allocs > 20 {
			t.Errorf("full-queue scan of %d pieces made %.0f allocs, want <= 20 "+
				"(scan does not stop at the full queue)", numPieces, allocs)
		}
	})
}

// countingStateSource records every GetPieceStates call and reports every piece
// downloaded except the indices in missing.
type countingStateSource struct {
	numPieces int
	missing   map[int]bool

	calls atomic.Int64
}

func (s *countingStateSource) GetPieceStates(context.Context, string) ([]PieceState, error) {
	s.calls.Add(1)
	states := make([]PieceState, s.numPieces)
	for i := range states {
		if s.missing[i] {
			states[i] = PieceStateNotDownloaded
			continue
		}
		states[i] = PieceStateDownloaded
	}
	return states, nil
}

func (s *countingStateSource) GetPieceHashes(context.Context, string) ([]string, error) {
	return make([]string, s.numPieces), nil
}

func (s *countingStateSource) GetTorrentMetadata(context.Context, string) (*TorrentMetadata, error) {
	return &TorrentMetadata{InitTorrentRequest: &pb.InitTorrentRequest{
		TorrentHash: "h1",
		NumPieces:   int32(s.numPieces),
		PieceSize:   1,
		TotalSize:   int64(s.numPieces),
	}}, nil
}

func (s *countingStateSource) ReadPiece(context.Context, *pb.Piece) ([]byte, error) {
	return nil, errors.New("not used")
}

// drainCompleted empties the completed channel and returns how many pieces it held.
func drainCompleted(monitor *PieceMonitor) int {
	n := 0
	for {
		select {
		case <-monitor.completed:
			n++
		default:
			return n
		}
	}
}

// TestPollTorrentPieces_ReusesStatesTheSourceCannotChange pins the refetch gate.
//
// The scan must still run every tick - it is what re-offers pieces the queue had
// no room for - but once the source holds every piece the array it scans is
// constant, and refetching it costs a round-trip plus two N-element allocations
// per torrent per tick for the whole transfer and the finalization wait after it.
func TestPollTorrentPieces_ReusesStatesTheSourceCannotChange(t *testing.T) {
	t.Parallel()

	const numPieces = 8

	setup := func(t *testing.T, missing map[int]bool) (*PieceMonitor, *countingStateSource) {
		t.Helper()
		source := &countingStateSource{numPieces: numPieces, missing: missing}
		monitor := newTestMonitor()
		monitor.source = source
		if err := monitor.startTracking(context.Background(), "h1", nil); err != nil {
			t.Fatalf("startTracking: %v", err)
		}
		if got := source.calls.Load(); got != 1 {
			t.Fatalf("GetPieceStates calls after startTracking = %d, want 1", got)
		}
		return monitor, source
	}

	t.Run("a source that holds every piece is asked once", func(t *testing.T) {
		t.Parallel()
		monitor, source := setup(t, nil)

		for tick := range 3 {
			if got := drainCompleted(monitor); got != numPieces {
				t.Fatalf("tick %d queued %d pieces, want %d: the scan must run from the cache too",
					tick, got, numPieces)
			}
			if err := monitor.pollTorrentPieces(context.Background(), "h1"); err != nil {
				t.Fatalf("pollTorrentPieces: %v", err)
			}
		}

		if got := source.calls.Load(); got != 1 {
			t.Errorf("GetPieceStates calls = %d, want 1: a constant piece-state array was refetched", got)
		}
	})

	t.Run("a piece the source is still missing forces a refetch", func(t *testing.T) {
		t.Parallel()
		monitor, source := setup(t, map[int]bool{numPieces - 1: true})

		for range 2 {
			if err := monitor.pollTorrentPieces(context.Background(), "h1"); err != nil {
				t.Fatalf("pollTorrentPieces: %v", err)
			}
		}

		if got := source.calls.Load(); got != 3 {
			t.Errorf("GetPieceStates calls = %d, want 3: an incomplete source must be re-asked every tick", got)
		}
	})

	t.Run("the cache expires so a lost source is noticed", func(t *testing.T) {
		t.Parallel()
		monitor, source := setup(t, nil)

		if err := monitor.pollTorrentPieces(context.Background(), "h1"); err != nil {
			t.Fatalf("pollTorrentPieces: %v", err)
		}
		if got := source.calls.Load(); got != 1 {
			t.Fatalf("GetPieceStates calls = %d, want 1 before the cache expires", got)
		}

		state, ok := monitor.torrents["h1"]
		if !ok {
			t.Fatal("torrent not tracked")
		}
		state.mu.Lock()
		state.statesFetchedAt = state.statesFetchedAt.Add(-sourceCompleteRefetchInterval)
		state.mu.Unlock()

		if err := monitor.pollTorrentPieces(context.Background(), "h1"); err != nil {
			t.Fatalf("pollTorrentPieces: %v", err)
		}
		if got := source.calls.Load(); got != 2 {
			t.Errorf("GetPieceStates calls = %d, want 2: the cache must expire", got)
		}
	})
}

// streamAll queues and acks every piece so the scan cursor reaches the end of
// the torrent, which is where a resync has to be able to rewind it from.
func streamAll(t *testing.T, monitor *PieceMonitor, st *torrentState, current []PieceState) {
	t.Helper()
	ctx := context.Background()
	hash := st.meta.GetTorrentHash()
	monitor.torrents[hash] = st

	if got := monitor.queueCompletedPieces(ctx, st, current); got != len(current) {
		t.Fatalf("initial scan queued %d pieces, want %d", got, len(current))
	}
	for len(monitor.completed) > 0 {
		monitor.MarkStreamed(hash, int((<-monitor.completed).GetIndex()))
	}
	if got := monitor.queueCompletedPieces(ctx, st, current); got != 0 {
		t.Fatalf("scan after full streaming queued %d pieces, want 0", got)
	}
	if st.firstUnstreamedScanIdx != len(current) {
		t.Fatalf("cursor at %d after full streaming, want %d", st.firstUnstreamedScanIdx, len(current))
	}
}

// queuedIndices drains the completed channel and returns what the scan offered.
func queuedIndices(monitor *PieceMonitor) []int32 {
	var indices []int32
	for len(monitor.completed) > 0 {
		indices = append(indices, (<-monitor.completed).GetIndex())
	}
	return indices
}

// A resync is how the destination's verification-failure recovery reaches the
// source: it clears the written bits of the corrupted pieces, answers
// FINALIZE_ERROR_INCOMPLETE, and the source un-marks them so the next poll
// re-offers them. The scan cursor only moves forward on its own, so without a
// rewind the re-offer never happens and the torrent stalls until the retry
// guard quarantines it as sync-failed.
func TestResyncStreamed_ReOffersPiecesBelowTheScanCursor(t *testing.T) {
	ctx := context.Background()

	t.Run("a piece the destination lost is offered again", func(t *testing.T) {
		monitor := newTestMonitor()
		st, current := newBacklogState(10)
		streamAll(t, monitor, st, current)

		destHas := make([]bool, len(current))
		for i := range destHas {
			destHas[i] = true
		}
		destHas[3] = false

		if reset := monitor.ResyncStreamed(st.meta.GetTorrentHash(), destHas); reset != 1 {
			t.Fatalf("resync reset %d pieces, want 1", reset)
		}
		monitor.queueCompletedPieces(ctx, st, current)
		if got := queuedIndices(monitor); !slices.Equal(got, []int32{3}) {
			t.Errorf("re-offered %v, want [3]", got)
		}
	})

	t.Run("the cursor rewinds to the lowest lost piece", func(t *testing.T) {
		monitor := newTestMonitor()
		st, current := newBacklogState(10)
		streamAll(t, monitor, st, current)

		destHas := make([]bool, len(current))
		for i := range destHas {
			destHas[i] = true
		}
		destHas[2], destHas[7] = false, false

		if reset := monitor.ResyncStreamed(st.meta.GetTorrentHash(), destHas); reset != 2 {
			t.Fatalf("resync reset %d pieces, want 2", reset)
		}
		if st.firstUnstreamedScanIdx != 2 {
			t.Errorf("cursor at %d, want 2 (the lowest un-marked piece)", st.firstUnstreamedScanIdx)
		}
		monitor.queueCompletedPieces(ctx, st, current)
		if got := queuedIndices(monitor); !slices.Equal(got, []int32{2, 7}) {
			t.Errorf("re-offered %v, want [2 7]", got)
		}
	})

	// The cursor is what keeps the per-tick scan off the streamed prefix, so a
	// resync that un-marks nothing must leave it where it was rather than
	// rewinding defensively.
	t.Run("a resync that loses nothing leaves the cursor alone", func(t *testing.T) {
		monitor := newTestMonitor()
		st, current := newBacklogState(10)
		streamAll(t, monitor, st, current)

		destHas := make([]bool, len(current))
		for i := range destHas {
			destHas[i] = true
		}

		if reset := monitor.ResyncStreamed(st.meta.GetTorrentHash(), destHas); reset != 0 {
			t.Fatalf("resync reset %d pieces, want 0", reset)
		}
		if st.firstUnstreamedScanIdx != len(current) {
			t.Errorf("cursor at %d, want %d", st.firstUnstreamedScanIdx, len(current))
		}
		monitor.queueCompletedPieces(ctx, st, current)
		if got := queuedIndices(monitor); got != nil {
			t.Errorf("re-offered %v, want nothing", got)
		}
	})
}

// progressByScan recomputes the three progress counts the way GetProgress did
// before torrentState maintained them incrementally: one pass over the
// per-piece slices. It is the oracle the counters must agree with, and shares
// no code with them.
func progressByScan(s *torrentState) (int, int, int) {
	var streamed, failed, available int
	for i := range s.streamed {
		if s.streamed[i] {
			streamed++
			continue
		}
		if s.failed[i] {
			failed++
		}
		if i < len(s.lastStates) && s.lastStates[i] == PieceStateDownloaded {
			available++
		}
	}
	return streamed, failed, available
}

// assertCountsMatchScan fails if any derived count has drifted from the slices.
func assertCountsMatchScan(t *testing.T, step string, s *torrentState) {
	t.Helper()
	streamed, failed, available := progressByScan(s)
	if s.streamedCount != streamed {
		t.Errorf("%s: streamedCount %d, scan says %d", step, s.streamedCount, streamed)
	}
	if s.failedCount != failed {
		t.Errorf("%s: failedCount %d, scan says %d", step, s.failedCount, failed)
	}
	if s.availableCount != available {
		t.Errorf("%s: availableCount %d, scan says %d", step, s.availableCount, available)
	}
}

// pieceStatesFromSeed builds a deterministic downloaded/not-downloaded pattern.
func pieceStatesFromSeed(numPieces, seed int) []PieceState {
	states := make([]PieceState, numPieces)
	for i := range states {
		states[i] = PieceStateNotDownloaded
		if (i*seed+seed)%3 != 0 {
			states[i] = PieceStateDownloaded
		}
	}
	return states
}

// TestTorrentState_CountsTrackTheSlices drives every mutator through a
// deterministic sequence that interleaves marking, un-marking, failing and
// replacing the source's piece-state array, checking after each step that the
// incrementally maintained counts still equal a full rescan.
func TestTorrentState_CountsTrackTheSlices(t *testing.T) {
	const numPieces = 24
	now := time.Now()

	state := newTestState(numPieces)
	state.setPieceStates(pieceStatesFromSeed(numPieces, 1), now)
	assertCountsMatchScan(t, "initial states", state)

	// A failure on an un-streamed piece, then the same piece streaming: the
	// failure must be counted and then given back.
	state.markFailed(4)
	assertCountsMatchScan(t, "failed 4", state)
	state.markStreamed(4)
	assertCountsMatchScan(t, "streamed 4 (was failed)", state)

	// Failing an already-streamed piece is ignored, so nothing is owed twice.
	state.markFailed(4)
	assertCountsMatchScan(t, "failed 4 again while streamed", state)

	// A spread of marks, including repeats and pieces the source does not hold.
	for _, i := range []int{0, 1, 2, 2, 7, 9, 12, 12, 20, 23} {
		state.markStreamed(i)
		assertCountsMatchScan(t, fmt.Sprintf("streamed %d", i), state)
	}

	for _, i := range []int{3, 5, 11, 18} {
		state.markFailed(i)
		assertCountsMatchScan(t, fmt.Sprintf("failed %d", i), state)
	}

	// Un-marking must return pieces to available when the source still has them,
	// including pieces that were never streamed (a no-op for the counts).
	for _, i := range []int{2, 2, 9, 15, 23} {
		state.unmarkStreamed(i)
		assertCountsMatchScan(t, fmt.Sprintf("unmarked %d", i), state)
	}

	// A refetch that changes which pieces the source holds; availableCount is
	// the one count that cannot be carried over.
	for seed := 2; seed <= 4; seed++ {
		state.setPieceStates(pieceStatesFromSeed(numPieces, seed), now)
		assertCountsMatchScan(t, fmt.Sprintf("refetched states seed %d", seed), state)
		state.markStreamed(seed)
		state.unmarkStreamed(seed + 1)
		assertCountsMatchScan(t, fmt.Sprintf("marks after refetch seed %d", seed), state)
	}
}

// TestGetProgress_MatchesAFullScanThroughThePublicAPI runs the same agreement
// check through the exported entry points a live transfer uses, so a mutator
// reached only from one of them cannot drift unnoticed.
func TestGetProgress_MatchesAFullScanThroughThePublicAPI(t *testing.T) {
	const numPieces = 16
	hash := "abc123"

	monitor := newTestMonitor()
	state := newTestState(numPieces)
	state.setPieceStates(pieceStatesFromSeed(numPieces, 1), time.Now())
	monitor.torrents[hash] = state

	check := func(step string) {
		t.Helper()
		progress, err := monitor.GetProgress(hash)
		if err != nil {
			t.Fatalf("%s: GetProgress: %v", step, err)
		}
		streamed, failed, available := progressByScan(state)
		if progress.Streamed != streamed || progress.Failed != failed || progress.Available != available {
			t.Errorf("%s: progress (%d streamed, %d failed, %d available), scan says (%d, %d, %d)",
				step, progress.Streamed, progress.Failed, progress.Available, streamed, failed, available)
		}
		if want := streamed == numPieces; progress.Complete != want {
			t.Errorf("%s: Complete %v, want %v", step, progress.Complete, want)
		}
	}

	check("fresh")

	monitor.MarkFailed(hash, 5)
	monitor.MarkFailed(hash, 6)
	check("after MarkFailed")

	monitor.MarkStreamed(hash, 5)
	check("after MarkStreamed of a failed piece")

	written := make([]bool, numPieces)
	for _, i := range []int{0, 1, 2, 3, 6} {
		written[i] = true
	}
	monitor.MarkStreamedBatch(hash, written)
	check("after MarkStreamedBatch")

	destHas := make([]bool, numPieces)
	for i := range destHas {
		destHas[i] = i%2 == 0
	}
	monitor.ResyncStreamed(hash, destHas)
	check("after ResyncStreamed")

	// Every piece streamed: the completion flag the maindata poll reads on every
	// tick must come out of the same counter.
	all := make([]bool, numPieces)
	for i := range all {
		all[i] = true
	}
	monitor.MarkStreamedBatch(hash, all)
	check("after marking every piece")
}
