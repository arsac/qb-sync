package destination

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bits-and-blooms/bitset"

	"github.com/arsac/qb-sync/internal/utils"
	pb "github.com/arsac/qb-sync/proto"
)

func TestComputeFilePieceRanges(t *testing.T) {
	t.Parallel()

	t.Run("multi-file torrent", func(t *testing.T) {
		t.Parallel()
		// 3 files, pieceLength=100, totalSize=250
		// File 0: offset=0, size=80   -> pieces 0..0 (1 piece)
		// File 1: offset=80, size=120  -> pieces 0..1 (2 pieces) — spans piece boundary at 100
		// File 2: offset=200, size=50  -> pieces 2..2 (1 piece)
		files := []*serverFileInfo{
			{offset: 0, size: 80, selected: true},
			{offset: 80, size: 120, selected: true},
			{offset: 200, size: 50, selected: true},
		}
		meta := torrentMeta{pieceLength: 100, totalSize: 250, files: files}
		meta.computeFilePieceRanges()

		assertFileRange(t, files[0], 0, 0, 1)
		assertFileRange(t, files[1], 0, 1, 2)
		assertFileRange(t, files[2], 2, 2, 1)
	})

	t.Run("zero-size file skipped", func(t *testing.T) {
		t.Parallel()
		files := []*serverFileInfo{
			{offset: 0, size: 100, selected: true},
			{offset: 100, size: 0, selected: true},
			{offset: 100, size: 50, selected: true},
		}
		meta := torrentMeta{pieceLength: 100, totalSize: 150, files: files}
		meta.computeFilePieceRanges()

		assertFileRange(t, files[0], 0, 0, 1)
		if files[1].piecesTotal != 0 {
			t.Errorf("zero-size file: piecesTotal = %d, want 0", files[1].piecesTotal)
		}
		assertFileRange(t, files[2], 1, 1, 1)
	})

	t.Run("single-piece torrent", func(t *testing.T) {
		t.Parallel()
		files := []*serverFileInfo{
			{offset: 0, size: 50, selected: true},
		}
		meta := torrentMeta{pieceLength: 100, totalSize: 50, files: files}
		meta.computeFilePieceRanges()

		assertFileRange(t, files[0], 0, 0, 1)
	})

	t.Run("boundary piece spans two files", func(t *testing.T) {
		t.Parallel()
		// pieceLength=100, totalSize=200
		// File 0: offset=0, size=50   -> piece 0
		// File 1: offset=50, size=150 -> pieces 0..1
		files := []*serverFileInfo{
			{offset: 0, size: 50, selected: true},
			{offset: 50, size: 150, selected: true},
		}
		meta := torrentMeta{pieceLength: 100, totalSize: 200, files: files}
		meta.computeFilePieceRanges()

		assertFileRange(t, files[0], 0, 0, 1)
		assertFileRange(t, files[1], 0, 1, 2)
	})

	t.Run("last piece clamped to max", func(t *testing.T) {
		t.Parallel()
		// File extends beyond total size conceptually, but lastPiece is clamped
		// totalSize=150, pieceLength=100 => maxPiece=1
		files := []*serverFileInfo{
			{offset: 0, size: 150, selected: true},
		}
		meta := torrentMeta{pieceLength: 100, totalSize: 150, files: files}
		meta.computeFilePieceRanges()

		assertFileRange(t, files[0], 0, 1, 2)
	})

	t.Run("zero pieceLength is no-op", func(t *testing.T) {
		t.Parallel()
		files := []*serverFileInfo{
			{offset: 0, size: 100, selected: true},
		}
		meta := torrentMeta{pieceLength: 0, totalSize: 100, files: files}
		meta.computeFilePieceRanges()

		if files[0].piecesTotal != 0 {
			t.Errorf("piecesTotal = %d, want 0 for zero pieceLength", files[0].piecesTotal)
		}
	})
}

func TestInitFilePieceCounts(t *testing.T) {
	t.Parallel()

	t.Run("partial resume", func(t *testing.T) {
		t.Parallel()
		// 2 files spanning 3 pieces
		// File 0: pieces 0..1
		// File 1: pieces 1..2
		// Written: [true, false, true]
		files := []*serverFileInfo{
			{offset: 0, size: 150, firstPiece: 0, lastPiece: 1, piecesTotal: 2, selected: true},
			{offset: 150, size: 150, firstPiece: 1, lastPiece: 2, piecesTotal: 2, selected: true},
		}
		written := boolSliceToBitSet([]bool{true, false, true})
		meta := torrentMeta{files: files}
		meta.initFilePieceCounts(written)

		if files[0].piecesWritten != 1 {
			t.Errorf("file 0: piecesWritten = %d, want 1", files[0].piecesWritten)
		}
		if files[1].piecesWritten != 1 {
			t.Errorf("file 1: piecesWritten = %d, want 1", files[1].piecesWritten)
		}
	})

	t.Run("all written", func(t *testing.T) {
		t.Parallel()
		files := []*serverFileInfo{
			{offset: 0, size: 200, firstPiece: 0, lastPiece: 1, piecesTotal: 2, selected: true},
		}
		written := boolSliceToBitSet([]bool{true, true})
		meta := torrentMeta{files: files}
		meta.initFilePieceCounts(written)

		if files[0].piecesWritten != 2 {
			t.Errorf("piecesWritten = %d, want 2", files[0].piecesWritten)
		}
	})

	t.Run("early-finalized files skipped", func(t *testing.T) {
		t.Parallel()
		files := []*serverFileInfo{
			{offset: 0, size: 100, firstPiece: 0, lastPiece: 0, piecesTotal: 1, earlyFinalized: true, selected: true},
			{offset: 100, size: 100, firstPiece: 1, lastPiece: 1, piecesTotal: 1, selected: true},
		}
		written := boolSliceToBitSet([]bool{true, true})
		meta := torrentMeta{files: files}
		meta.initFilePieceCounts(written)

		if files[0].piecesWritten != 0 {
			t.Errorf("early-finalized file: piecesWritten = %d, want 0", files[0].piecesWritten)
		}
		if files[1].piecesWritten != 1 {
			t.Errorf("non-finalized file: piecesWritten = %d, want 1", files[1].piecesWritten)
		}
	})

	t.Run("zero-size files skipped", func(t *testing.T) {
		t.Parallel()
		files := []*serverFileInfo{
			{offset: 0, size: 0, selected: true},
			{offset: 0, size: 100, firstPiece: 0, lastPiece: 0, piecesTotal: 1, selected: true},
		}
		written := boolSliceToBitSet([]bool{true})
		meta := torrentMeta{files: files}
		meta.initFilePieceCounts(written)

		if files[0].piecesWritten != 0 {
			t.Errorf("zero-size file: piecesWritten = %d, want 0", files[0].piecesWritten)
		}
		if files[1].piecesWritten != 1 {
			t.Errorf("normal file: piecesWritten = %d, want 1", files[1].piecesWritten)
		}
	})
}

func TestCheckFileCompletions(t *testing.T) {
	t.Parallel()

	t.Run("file completion triggers rename", func(t *testing.T) {
		t.Parallel()
		s, tmpDir := newTestDestServer(t)

		// Create a .partial file
		partialPath := filepath.Join(tmpDir, "testfile.txt.partial")
		if err := os.WriteFile(partialPath, []byte("hello"), 0o644); err != nil {
			t.Fatal(err)
		}

		fi := &serverFileInfo{
			path:        partialPath,
			size:        5,
			offset:      0,
			firstPiece:  0,
			lastPiece:   0,
			piecesTotal: 1,
			selected:    true,
			// piecesWritten will be incremented by checkFileCompletions
			piecesWritten: 0,
		}
		state := &serverTorrentState{
			torrentMeta: torrentMeta{files: []*serverFileInfo{fi}},
		}

		state.mu.Lock()
		s.checkFileCompletions("test-hash", state, 0)
		state.mu.Unlock()
		waitEarlyFinalize(t, state)

		if !fi.earlyFinalized {
			t.Error("file should be marked as earlyFinalized")
		}

		finalPath := filepath.Join(tmpDir, "testfile.txt")
		if fi.path != finalPath {
			t.Errorf("path = %s, want %s", fi.path, finalPath)
		}

		// Verify the file was actually renamed on disk
		if _, err := os.Stat(finalPath); err != nil {
			t.Errorf("final file should exist: %v", err)
		}
		if _, err := os.Stat(partialPath); !os.IsNotExist(err) {
			t.Error("partial file should no longer exist")
		}
	})

	t.Run("boundary piece completes multiple files", func(t *testing.T) {
		t.Parallel()
		s, tmpDir := newTestDestServer(t)

		// Two files that share piece 1
		partial1 := filepath.Join(tmpDir, "file1.txt.partial")
		partial2 := filepath.Join(tmpDir, "file2.txt.partial")
		if err := os.WriteFile(partial1, []byte("aaa"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(partial2, []byte("bbb"), 0o644); err != nil {
			t.Fatal(err)
		}

		// pieceLength=10, totalSize=20
		// File 0: offset=0, size=5  -> piece 0 only
		// File 1: offset=5, size=15 -> pieces 0..1
		// File 0 already has piece 0 written (piecesWritten=0, will become 1, total=1 -> complete)
		// File 1 has piecesWritten=1 for piece 1; now piece 0 completes it
		files := []*serverFileInfo{
			{
				path: partial1, size: 5, offset: 0,
				firstPiece: 0, lastPiece: 0, piecesTotal: 1, piecesWritten: 0, selected: true,
			},
			{
				path: partial2, size: 15, offset: 5,
				firstPiece: 0, lastPiece: 1, piecesTotal: 2, piecesWritten: 1, selected: true,
			},
		}
		state := &serverTorrentState{torrentMeta: torrentMeta{files: files}}

		state.mu.Lock()
		s.checkFileCompletions("test-hash", state, 0)
		state.mu.Unlock()
		waitEarlyFinalize(t, state)

		if !files[0].earlyFinalized {
			t.Error("file 0 should be early-finalized")
		}
		if !files[1].earlyFinalized {
			t.Error("file 1 should be early-finalized")
		}
	})

	t.Run("incomplete file not finalized", func(t *testing.T) {
		t.Parallel()
		s, tmpDir := newTestDestServer(t)

		partialPath := filepath.Join(tmpDir, "incomplete.txt.partial")
		if err := os.WriteFile(partialPath, []byte("data"), 0o644); err != nil {
			t.Fatal(err)
		}

		fi := &serverFileInfo{
			path: partialPath, size: 4, offset: 0,
			firstPiece: 0, lastPiece: 1, piecesTotal: 2, piecesWritten: 0, selected: true,
		}
		state := &serverTorrentState{torrentMeta: torrentMeta{files: []*serverFileInfo{fi}}}

		state.mu.Lock()
		s.checkFileCompletions("test-hash", state, 0)
		state.mu.Unlock()
		waitEarlyFinalize(t, state)

		if fi.earlyFinalized {
			t.Error("file should NOT be early-finalized with only 1/2 pieces")
		}
		if fi.piecesWritten != 1 {
			t.Errorf("piecesWritten = %d, want 1", fi.piecesWritten)
		}
	})

	t.Run("hardlinked files skipped", func(t *testing.T) {
		t.Parallel()
		s, _ := newTestDestServer(t)

		fi := &serverFileInfo{
			size: 100, offset: 0,
			firstPiece: 0, lastPiece: 0, piecesTotal: 1,
			hardlink: hardlinkInfo{state: hlStateComplete}, selected: true,
		}
		state := &serverTorrentState{torrentMeta: torrentMeta{files: []*serverFileInfo{fi}}}

		state.mu.Lock()
		s.checkFileCompletions("test-hash", state, 0)
		state.mu.Unlock()
		waitEarlyFinalize(t, state)

		if fi.piecesWritten != 0 {
			t.Error("hardlinked file should not have piecesWritten incremented")
		}
	})

	t.Run("already early-finalized files skipped", func(t *testing.T) {
		t.Parallel()
		s, _ := newTestDestServer(t)

		fi := &serverFileInfo{
			size: 100, offset: 0,
			firstPiece: 0, lastPiece: 0, piecesTotal: 1,
			earlyFinalized: true, selected: true,
		}
		state := &serverTorrentState{torrentMeta: torrentMeta{files: []*serverFileInfo{fi}}}

		state.mu.Lock()
		s.checkFileCompletions("test-hash", state, 0)
		state.mu.Unlock()
		waitEarlyFinalize(t, state)

		if fi.piecesWritten != 0 {
			t.Error("already-finalized file should not have piecesWritten incremented")
		}
	})

	t.Run("out-of-range piece does not increment", func(t *testing.T) {
		t.Parallel()
		s, _ := newTestDestServer(t)

		fi := &serverFileInfo{
			size: 100, offset: 100,
			firstPiece: 1, lastPiece: 1, piecesTotal: 1, selected: true,
		}
		state := &serverTorrentState{torrentMeta: torrentMeta{files: []*serverFileInfo{fi}}}

		state.mu.Lock()
		s.checkFileCompletions("test-hash", state, 0) // piece 0 doesn't overlap
		state.mu.Unlock()
		waitEarlyFinalize(t, state)

		if fi.piecesWritten != 0 {
			t.Error("out-of-range piece should not increment piecesWritten")
		}
	})

	t.Run("sync error defers to finalization", func(t *testing.T) {
		t.Parallel()
		s, tmpDir := newTestDestServer(t)

		// Create file then remove it to cause sync to fail (file handle will be nil,
		// so closeFileHandle returns nil, but we can test by making the rename fail)
		partialPath := filepath.Join(tmpDir, "nosuchdir", "bad.txt.partial")

		fi := &serverFileInfo{
			path: partialPath, size: 10, offset: 0,
			firstPiece: 0, lastPiece: 0, piecesTotal: 1, piecesWritten: 0, selected: true,
		}
		state := &serverTorrentState{torrentMeta: torrentMeta{files: []*serverFileInfo{fi}}}

		state.mu.Lock()
		s.checkFileCompletions("test-hash", state, 0)
		state.mu.Unlock()
		waitEarlyFinalize(t, state)

		// rename should fail (parent dir doesn't exist), so earlyFinalized should be false
		if fi.earlyFinalized {
			t.Error("file should NOT be early-finalized when rename fails")
		}
	})
}

func TestFinalizeFiles_SkipsEarlyFinalizedFiles(t *testing.T) {
	t.Parallel()

	s, tmpDir := newTestDestServer(t)

	// File 0: already early-finalized (at final path, file handle nil)
	finalPath := filepath.Join(tmpDir, "done.txt")
	if err := os.WriteFile(finalPath, []byte("done"), 0o644); err != nil {
		t.Fatal(err)
	}

	// File 1: still .partial, needs normal finalization
	partialPath := filepath.Join(tmpDir, "pending.txt.partial")
	if err := os.WriteFile(partialPath, []byte("pending"), 0o644); err != nil {
		t.Fatal(err)
	}

	state := &serverTorrentState{
		torrentMeta: torrentMeta{
			pieceLength: 100,
			totalSize:   200,
			files: []*serverFileInfo{
				{
					path:           finalPath,
					size:           4,
					offset:         0,
					earlyFinalized: true,
					selected:       true,
					// file is nil (closed during early finalization)
				},
				{
					path:     partialPath,
					size:     7,
					offset:   4,
					selected: true,
				},
			},
		},
		written: boolSliceToBitSet([]bool{true, true}),
	}

	ctx := context.Background()
	if err := s.finalizeFiles(ctx, "test-hash", state); err != nil {
		t.Fatalf("finalizeFiles failed: %v", err)
	}

	// File 0 should still be at final path
	if _, err := os.Stat(finalPath); err != nil {
		t.Errorf("early-finalized file should still exist at final path: %v", err)
	}

	// File 1 should be renamed
	pendingFinal := filepath.Join(tmpDir, "pending.txt")
	if _, err := os.Stat(pendingFinal); err != nil {
		t.Errorf("pending file should be renamed to final path: %v", err)
	}
}

func TestWritePiece_EarlyFinalizesCompletedFile(t *testing.T) {
	t.Parallel()

	s, tmpDir := newTestDestServer(t)

	hash := "write-piece-early-test"

	// Single file, single piece
	partialPath := filepath.Join(tmpDir, "single.bin.partial")
	pieceData := []byte("hello world!")
	pieceHash := utils.ComputeSHA1(pieceData)

	state := &serverTorrentState{
		torrentMeta: torrentMeta{
			pieceHashes: []string{pieceHash},
			pieceLength: int64(len(pieceData)),
			totalSize:   int64(len(pieceData)),
			files: []*serverFileInfo{
				{
					path:        partialPath,
					size:        int64(len(pieceData)),
					offset:      0,
					firstPiece:  0,
					lastPiece:   0,
					piecesTotal: 1,
					selected:    true,
				},
			},
		},
		written:   bitset.New(1),
		statePath: filepath.Join(tmpDir, ".state"),
	}

	s.store.mu.Lock()
	s.store.entries[hash] = state
	s.store.mu.Unlock()

	ctx := context.Background()
	result := s.writePiece(ctx, &pb.WritePieceRequest{
		TorrentHash: hash,
		PieceIndex:  0,
		Offset:      0,
		Data:        pieceData,
	})
	if !result.success {
		t.Fatalf("writePiece failed: %s", result.errMsg)
	}
	waitEarlyFinalize(t, state)

	// Verify the file was early-finalized
	fi := state.files[0]
	if !fi.earlyFinalized {
		t.Error("file should be early-finalized after its only piece was written")
	}

	finalPath := filepath.Join(tmpDir, "single.bin")
	if !strings.HasSuffix(fi.path, "single.bin") || strings.HasSuffix(fi.path, partialSuffix) {
		t.Errorf("path should be final, got %s", fi.path)
	}

	// Verify file exists at final path with correct content
	data, readErr := os.ReadFile(finalPath)
	if readErr != nil {
		t.Fatalf("cannot read final file: %v", readErr)
	}
	if string(data) != string(pieceData) {
		t.Errorf("file content = %q, want %q", string(data), string(pieceData))
	}
}

func TestCheckFileCompletions_VerifyFailure(t *testing.T) {
	t.Parallel()

	s, tmpDir := newTestDestServer(t)

	hash := "verify-failure-test"
	correctData := []byte("correct piece data!")
	pieceHash := utils.ComputeSHA1(correctData)

	// Write corrupted data so read-back verification fails against the correct hash.
	corruptedData := []byte("XXrrect piece data!")
	partialPath := filepath.Join(tmpDir, "verify.bin.partial")
	if err := os.WriteFile(partialPath, corruptedData, 0o644); err != nil {
		t.Fatal(err)
	}

	fi := &serverFileInfo{
		path:          partialPath,
		size:          int64(len(correctData)),
		offset:        0,
		firstPiece:    0,
		lastPiece:     0,
		piecesTotal:   1,
		piecesWritten: 0,
		selected:      true,
	}
	state := &serverTorrentState{
		torrentMeta: torrentMeta{
			pieceHashes: []string{pieceHash},
			pieceLength: int64(len(correctData)),
			totalSize:   int64(len(correctData)),
			files:       []*serverFileInfo{fi},
		},
		written:   boolSliceToBitSet([]bool{true}),
		statePath: filepath.Join(tmpDir, ".state"),
	}

	state.mu.Lock()
	s.checkFileCompletions(hash, state, 0)
	state.mu.Unlock()
	waitEarlyFinalize(t, state)

	// File should NOT be early-finalized (verification failed).
	if fi.earlyFinalized {
		t.Error("file should NOT be early-finalized when verification fails")
	}

	// File should still be at .partial path.
	if fi.path != partialPath {
		t.Errorf("path = %s, want %s", fi.path, partialPath)
	}

	// Corrupted piece should be marked unwritten.
	if state.written.Test(0) {
		t.Error("corrupted piece should be marked as unwritten")
	}
	if state.written.Count() != 0 {
		t.Errorf("writtenCount = %d, want 0", state.written.Count())
	}
	if fi.piecesWritten != 0 {
		t.Errorf("piecesWritten = %d, want 0", fi.piecesWritten)
	}
}

func TestCheckFileCompletions_VerifySkipsBoundaryPieces(t *testing.T) {
	t.Parallel()

	s, tmpDir := newTestDestServer(t)

	hash := "verify-boundary-test"

	// Two files sharing piece 0 (boundary piece).
	// File 0: offset=0, size=5 — piece 0 starts at 0, ends at 10 → boundary.
	// File 1: offset=5, size=5 — piece 0 starts at 0, ends at 10 → boundary.
	// Boundary pieces can't be verified from a single file, so verification
	// should skip them and the file should still early-finalize.

	partial1 := filepath.Join(tmpDir, "f1.bin.partial")
	partial2 := filepath.Join(tmpDir, "f2.bin.partial")
	data1 := []byte("hello")
	data2 := []byte("world")
	if err := os.WriteFile(partial1, data1, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(partial2, data2, 0o644); err != nil {
		t.Fatal(err)
	}

	pieceHash := utils.ComputeSHA1(append(data1, data2...))

	files := []*serverFileInfo{
		{
			path: partial1, size: 5, offset: 0,
			firstPiece: 0, lastPiece: 0, piecesTotal: 1, piecesWritten: 0, selected: true,
		},
		{
			path: partial2, size: 5, offset: 5,
			firstPiece: 0, lastPiece: 0, piecesTotal: 1, piecesWritten: 0, selected: true,
		},
	}
	state := &serverTorrentState{
		torrentMeta: torrentMeta{
			pieceHashes: []string{pieceHash},
			pieceLength: 10,
			totalSize:   10,
			files:       files,
		},
		written: boolSliceToBitSet([]bool{true}),
	}

	state.mu.Lock()
	s.checkFileCompletions(hash, state, 0)
	state.mu.Unlock()
	waitEarlyFinalize(t, state)

	// Both files should be early-finalized (boundary piece was skipped, not failed).
	if !files[0].earlyFinalized {
		t.Error("file 0 should be early-finalized (boundary piece skipped)")
	}
	if !files[1].earlyFinalized {
		t.Error("file 1 should be early-finalized (boundary piece skipped)")
	}
}

func TestCheckFileCompletions_VerifyPartialCorruption(t *testing.T) {
	t.Parallel()

	s, tmpDir := newTestDestServer(t)

	hash := "verify-partial-corrupt"

	// 3 interior pieces in a single file, middle piece corrupted.
	// pieceLength=10, totalSize=30, file covers all bytes.
	piece0 := []byte("0000000000")
	piece1 := []byte("1111111111")
	piece2 := []byte("2222222222")
	hash0 := utils.ComputeSHA1(piece0)
	hash1 := utils.ComputeSHA1(piece1)
	hash2 := utils.ComputeSHA1(piece2)

	// Write file with piece 1 corrupted.
	corrupted1 := []byte("XXXXXXXXXX")
	fileData := append(append(piece0, corrupted1...), piece2...)

	partialPath := filepath.Join(tmpDir, "multi.bin.partial")
	if err := os.WriteFile(partialPath, fileData, 0o644); err != nil {
		t.Fatal(err)
	}

	fi := &serverFileInfo{
		path: partialPath, size: 30, offset: 0,
		firstPiece: 0, lastPiece: 2, piecesTotal: 3, piecesWritten: 2, selected: true,
	}
	state := &serverTorrentState{
		torrentMeta: torrentMeta{
			pieceHashes: []string{hash0, hash1, hash2},
			pieceLength: 10,
			totalSize:   30,
			files:       []*serverFileInfo{fi},
		},
		written:   boolSliceToBitSet([]bool{true, true, true}),
		statePath: filepath.Join(tmpDir, ".state"),
	}

	state.mu.Lock()
	// Piece 2 is the "completing" piece.
	s.checkFileCompletions(hash, state, 2)
	state.mu.Unlock()
	waitEarlyFinalize(t, state)

	// File should NOT be early-finalized.
	if fi.earlyFinalized {
		t.Error("file should NOT be early-finalized with corrupted piece")
	}

	// Only piece 1 should be marked unwritten; pieces 0 and 2 stay written.
	if !state.written.Test(0) {
		t.Error("piece 0 should remain written (verified OK)")
	}
	if state.written.Test(1) {
		t.Error("piece 1 should be marked unwritten (corrupted)")
	}
	if !state.written.Test(2) {
		t.Error("piece 2 should remain written (verified OK)")
	}
	if state.written.Count() != 2 {
		t.Errorf("writtenCount = %d, want 2", state.written.Count())
	}
	if fi.piecesWritten != 2 {
		t.Errorf("piecesWritten = %d, want 2", fi.piecesWritten)
	}
}

func TestCheckFileCompletions_VerifyNoPieceHashes(t *testing.T) {
	t.Parallel()

	s, tmpDir := newTestDestServer(t)

	hash := "verify-nohash-test"

	// No piece hashes → verifyFilePieces returns nil → rename proceeds.
	partialPath := filepath.Join(tmpDir, "nohash.bin.partial")
	if err := os.WriteFile(partialPath, []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}

	fi := &serverFileInfo{
		path: partialPath, size: 4, offset: 0,
		firstPiece: 0, lastPiece: 0, piecesTotal: 1, piecesWritten: 0, selected: true,
	}
	state := &serverTorrentState{
		torrentMeta: torrentMeta{
			pieceHashes: nil, // No hashes available.
			pieceLength: 4,
			totalSize:   4,
			files:       []*serverFileInfo{fi},
		},
		written: boolSliceToBitSet([]bool{true}),
	}

	state.mu.Lock()
	s.checkFileCompletions(hash, state, 0)
	state.mu.Unlock()
	waitEarlyFinalize(t, state)

	if !fi.earlyFinalized {
		t.Error("file should be early-finalized when no piece hashes are available")
	}
}

// TestCheckFileCompletions_DefersIOToBackground pins that completing a file no
// longer costs the write path its fsync, read-back verify and rename. Those are
// proportional to the file's size on NFS and used to run before the piece was
// acked, which stalls a stream worker and can outlast the source's in-flight
// piece timeout.
//
// The assertions run while the test still holds state.mu, which the background
// pass needs for its bookkeeping, so "not renamed yet" is a fact rather than a
// race: pre-change, checkFileCompletions returned with the rename already done.
func TestCheckFileCompletions_DefersIOToBackground(t *testing.T) {
	t.Parallel()
	s, tmpDir := newTestDestServer(t)

	pieceData := []byte("payload")
	partialPath := filepath.Join(tmpDir, "movie.bin"+partialSuffix)
	if err := os.WriteFile(partialPath, pieceData, 0o644); err != nil {
		t.Fatal(err)
	}

	fi := &serverFileInfo{
		path: partialPath, size: int64(len(pieceData)), offset: 0,
		firstPiece: 0, lastPiece: 0, piecesTotal: 1, selected: true,
	}
	state := &serverTorrentState{
		torrentMeta: torrentMeta{
			pieceHashes: []string{utils.ComputeSHA1(pieceData)},
			pieceLength: int64(len(pieceData)),
			totalSize:   int64(len(pieceData)),
			files:       []*serverFileInfo{fi},
		},
		written: boolSliceToBitSet([]bool{true}),
	}

	state.mu.Lock()
	s.checkFileCompletions("bg-hash", state, 0)
	if state.earlyFinalizing != 1 {
		t.Errorf("earlyFinalizing = %d, want 1 background pass registered", state.earlyFinalizing)
	}
	if fi.path != partialPath {
		t.Errorf("file renamed on the write path: %s", fi.path)
	}
	if fi.file != nil {
		t.Error("write handle should be handed to the background pass")
	}
	state.mu.Unlock()

	waitEarlyFinalize(t, state)

	finalPath := filepath.Join(tmpDir, "movie.bin")
	if fi.path != finalPath {
		t.Errorf("path = %s, want %s", fi.path, finalPath)
	}
	if _, err := os.Stat(finalPath); err != nil {
		t.Errorf("final file should exist: %v", err)
	}
}

// TestFinalizeTorrent_DefersWhileEarlyFinalizing pins the guard that keeps
// FinalizeTorrent from racing a background early finalization, which owns its
// file's handle, path and written bits until it lands. BUSY is retried by the
// source without penalty, so deferring costs one poll interval.
func TestFinalizeTorrent_DefersWhileEarlyFinalizing(t *testing.T) {
	t.Parallel()
	s, tmpDir := newTestDestServer(t)

	const hash = "busy-hash"
	partialPath := filepath.Join(tmpDir, "movie.bin"+partialSuffix)
	if err := os.WriteFile(partialPath, []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}
	state := &serverTorrentState{
		torrentMeta: torrentMeta{
			pieceLength: 7,
			totalSize:   7,
			files: []*serverFileInfo{{
				path: partialPath, size: 7, offset: 0,
				firstPiece: 0, lastPiece: 0, piecesTotal: 1, piecesWritten: 1, selected: true,
			}},
		},
		written:         boolSliceToBitSet([]bool{true}),
		earlyFinalizing: 1,
	}
	s.store.mu.Lock()
	s.store.entries[hash] = state
	s.store.mu.Unlock()

	resp, err := s.FinalizeTorrent(context.Background(), &pb.FinalizeTorrentRequest{TorrentHash: hash})
	if err != nil {
		t.Fatalf("FinalizeTorrent error: %v", err)
	}
	if resp.GetSuccess() {
		t.Fatal("FinalizeTorrent should defer while an early finalization is in flight")
	}
	if resp.GetErrorCode() != pb.FinalizeErrorCode_FINALIZE_ERROR_BUSY {
		t.Errorf("errorCode = %v, want BUSY so the source retries without penalty", resp.GetErrorCode())
	}

	// Nothing may have been claimed or moved: a later call has to be able to
	// run the whole finalization.
	state.mu.Lock()
	active := state.finalization.active
	state.mu.Unlock()
	if active {
		t.Error("finalization must stay inactive after deferring")
	}
	if _, statErr := os.Stat(partialPath); statErr != nil {
		t.Errorf(".partial file should be untouched: %v", statErr)
	}
}

func assertFileRange(t *testing.T, fi *serverFileInfo, firstPiece, lastPiece, piecesTotal int) {
	t.Helper()
	if fi.firstPiece != firstPiece {
		t.Errorf("firstPiece = %d, want %d", fi.firstPiece, firstPiece)
	}
	if fi.lastPiece != lastPiece {
		t.Errorf("lastPiece = %d, want %d", fi.lastPiece, lastPiece)
	}
	if fi.piecesTotal != piecesTotal {
		t.Errorf("piecesTotal = %d, want %d", fi.piecesTotal, piecesTotal)
	}
}

// TestVerifyFilePieces_ScatteredCorruption pins the early-finalize read-back
// verify against the failure mode parallelizing it introduces: workers racing
// on the shared failed slice, or a worker's own fd reading at the wrong
// offset. Corruption is scattered so more than one worker must report, and the
// same expectation must hold at every worker count.
func TestVerifyFilePieces_ScatteredCorruption(t *testing.T) {
	t.Parallel()

	const (
		pieceLength = 16
		numPieces   = 10
		fileOffset  = 8 // makes piece 0 a boundary piece that must be skipped
	)
	totalSize := int64(pieceLength * numPieces)

	full := make([]byte, totalSize)
	for i := range full {
		full[i] = byte(i)
	}
	hashes := make([]string, numPieces)
	for p := range numPieces {
		hashes[p] = utils.ComputeSHA1(full[p*pieceLength : (p+1)*pieceLength])
	}

	want := []int{2, 5, 9}
	onDisk := slices.Clone(full)
	for _, p := range want {
		onDisk[p*pieceLength] ^= 0xff
	}

	path := filepath.Join(t.TempDir(), "scattered.bin.partial")
	if err := os.WriteFile(path, onDisk[fileOffset:], 0o644); err != nil {
		t.Fatal(err)
	}

	fi := &serverFileInfo{
		path: path, offset: fileOffset, size: totalSize - fileOffset,
		firstPiece: 0, lastPiece: numPieces - 1, selected: true,
	}
	state := &serverTorrentState{torrentMeta: torrentMeta{
		pieceHashes: hashes,
		pieceLength: pieceLength,
		totalSize:   totalSize,
		files:       []*serverFileInfo{fi},
	}}

	for _, workers := range []int{1, 3, 16} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			t.Parallel()

			s, _ := newTestDestServer(t)
			s.config.VerifyConcurrency = workers

			fh, openErr := os.Open(path)
			if openErr != nil {
				t.Fatal(openErr)
			}
			defer fh.Close()

			if got, _ := s.verifyFilePieces(t.Context(), state, fi, fh); !slices.Equal(got, want) {
				t.Errorf("shared handle: failed pieces = %v, want %v", got, want)
			}
			// nil handle exercises the self-open path for every worker.
			if got, _ := s.verifyFilePieces(t.Context(), state, fi, nil); !slices.Equal(got, want) {
				t.Errorf("self-opened: failed pieces = %v, want %v", got, want)
			}

			// A cancelled pass must fail closed: callers read "not in the
			// failed set" as proof a piece was read back and matched, so it
			// reports every piece it never got to rather than shrinking its
			// coverage silently.
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			got, interrupted := s.verifyFilePieces(ctx, state, fi, fh)
			if !interrupted {
				t.Error("cancelled pass reported interrupted = false")
			}
			if allInterior := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}; !slices.Equal(got, allInterior) {
				t.Errorf("cancelled: failed pieces = %v, want %v", got, allInterior)
			}
		})
	}
}

// TestPreVerifyCompleteFiles pins the init-time read-back pass to exactly the
// files whose data is already on disk and immutable: pre-existing/hardlinked
// (hlStateComplete) selected files. Everything else - a streamed .partial, a
// pending hardlink whose file doesn't exist yet, an unselected file - must be
// left for verifyFinalizedPieces, as must boundary pieces and any piece that
// fails to read back. The written bitmap must come out untouched: these files
// are skipForWriteData, so clearing a bit here would ask the source to
// re-stream data writePieceData would drop.
func TestPreVerifyCompleteFiles(t *testing.T) {
	t.Parallel()

	const (
		pieceLength = 16
		numPieces   = 11
	)
	totalSize := int64(pieceLength * numPieces)

	full := make([]byte, totalSize)
	for i := range full {
		full[i] = byte(i * 7)
	}
	hashes := make([]string, numPieces)
	for p := range numPieces {
		hashes[p] = utils.ComputeSHA1(full[p*pieceLength : (p+1)*pieceLength])
	}

	dir := t.TempDir()
	// write drops file content at [offset, offset+size) onto disk, optionally
	// flipping a byte so one piece fails its read-back.
	write := func(name string, offset, size int64, corruptAt int64) string {
		path := filepath.Join(dir, name)
		content := slices.Clone(full[offset : offset+size])
		if corruptAt >= 0 {
			content[corruptAt-offset] ^= 0xff
		}
		if err := os.WriteFile(path, content, 0o644); err != nil {
			t.Fatal(err)
		}
		return path
	}

	complete := hardlinkInfo{state: hlStateComplete}
	files := []*serverFileInfo{
		// Candidate, clean: pieces 0-2 interior.
		{path: write("a.bin", 0, 48, -1), offset: 0, size: 48, selected: true, hardlink: complete},
		// Candidate with piece 4 corrupted; piece 5 spans into the next file.
		{path: write("b.bin", 48, 40, 4*pieceLength), offset: 48, size: 40, selected: true, hardlink: complete},
		// Streamed .partial: readable and correct, but not a candidate.
		{path: write("c.bin.partial", 88, 24, -1), offset: 88, size: 24, selected: true},
		// Pending hardlink: file does not exist yet.
		{path: filepath.Join(dir, "d.bin"), offset: 112, size: 32, selected: true,
			hardlink: hardlinkInfo{state: hlStatePending}},
		// Unselected: no data on disk at all.
		{path: filepath.Join(dir, "e.bin"), offset: 144, size: 32},
	}

	meta := torrentMeta{
		pieceHashes: hashes,
		pieceLength: pieceLength,
		totalSize:   totalSize,
		files:       files,
	}
	meta.computeFilePieceRanges()

	written := bitset.New(numPieces)
	written.FlipRange(0, numPieces)
	state := &serverTorrentState{
		torrentMeta: meta,
		written:     written,
		verified:    bitset.New(numPieces),
	}

	s, _ := newTestDestServer(t)
	s.preVerifyCompleteFiles(t.Context(), "hash", state)

	var got []int
	for p := range numPieces {
		if state.verified.Test(uint(p)) {
			got = append(got, p)
		}
	}
	if want := []int{0, 1, 2, 3}; !slices.Equal(got, want) {
		t.Errorf("verified pieces = %v, want %v", got, want)
	}
	if state.written.Count() != numPieces {
		t.Errorf("written pieces = %d, want %d (pass must not clear bits)", state.written.Count(), numPieces)
	}
}

// TestStopPreVerify_WaitsForPassToExit pins the ordering finalization depends
// on: once stopPreVerify returns, the pre-verification pass has stopped
// touching state.verified, so the finalize read-back queue it is about to build
// is a stable snapshot. A stopPreVerify that only cancelled would return while
// the pass was still running.
func TestStopPreVerify_WaitsForPassToExit(t *testing.T) {
	t.Parallel()

	t.Run("no pass registered", func(t *testing.T) {
		t.Parallel()
		(&serverTorrentState{}).stopPreVerify() // must not block or panic
	})

	t.Run("waits then clears", func(t *testing.T) {
		t.Parallel()

		state := &serverTorrentState{}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		var exited atomic.Bool

		state.mu.Lock()
		state.preVerifyCancel = cancel
		state.preVerifyDone = done
		state.mu.Unlock()

		go func() {
			defer close(done)
			<-ctx.Done()
			// Unwinding a real pass (draining its worker queue, taking
			// state.mu for the last file) is not instantaneous. Stand in for
			// that so a stopPreVerify that skipped the join fails here rather
			// than passing on a lucky schedule.
			time.Sleep(50 * time.Millisecond)
			exited.Store(true)
		}()

		state.stopPreVerify()

		if !exited.Load() {
			t.Error("stopPreVerify returned before the pass exited")
		}

		state.mu.Lock()
		cancelLeft, doneLeft := state.preVerifyCancel, state.preVerifyDone
		state.mu.Unlock()
		if cancelLeft != nil || doneLeft != nil {
			t.Error("stopPreVerify left the pass registered")
		}

		state.stopPreVerify() // idempotent: must not block on the closed handle
	})
}

// TestStartPreVerify_StoppablePass pins that the pass started at init is the one
// finalization can stop, and that stopping it keeps the pass's core invariant:
// a bit in state.verified means that piece was read back off disk and matched.
// Cancellation must never widen the set - a corrupt file's pieces stay unmarked
// however far the pass got.
func TestStartPreVerify_StoppablePass(t *testing.T) {
	t.Parallel()

	const (
		pieceLength = 16
		numPieces   = 32
		perFile     = int64(pieceLength * 2)
	)
	totalSize := int64(pieceLength * numPieces)

	full := make([]byte, totalSize)
	for i := range full {
		full[i] = byte(i * 11)
	}
	hashes := make([]string, numPieces)
	for p := range numPieces {
		hashes[p] = utils.ComputeSHA1(full[p*pieceLength : (p+1)*pieceLength])
	}

	dir := t.TempDir()
	// Every file is a two-piece hlStateComplete candidate. The last one is
	// corrupt, so whichever files the pass reaches, its pieces must stay out of
	// state.verified.
	const corruptFile = numPieces/2 - 1
	files := make([]*serverFileInfo, 0, numPieces/2)
	for i := range numPieces / 2 {
		offset := int64(i) * perFile
		content := slices.Clone(full[offset : offset+perFile])
		if i == corruptFile {
			content[0] ^= 0xff
		}
		path := filepath.Join(dir, fmt.Sprintf("f%02d.bin", i))
		if err := os.WriteFile(path, content, 0o644); err != nil {
			t.Fatal(err)
		}
		files = append(files, &serverFileInfo{
			path: path, offset: offset, size: perFile, selected: true,
			hardlink: hardlinkInfo{state: hlStateComplete},
		})
	}

	meta := torrentMeta{
		pieceHashes: hashes,
		pieceLength: pieceLength,
		totalSize:   totalSize,
		files:       files,
	}
	meta.computeFilePieceRanges()

	written := bitset.New(numPieces)
	written.FlipRange(0, numPieces)
	state := &serverTorrentState{
		torrentMeta: meta,
		written:     written,
		verified:    bitset.New(numPieces),
	}

	s, _ := newTestDestServer(t)
	s.startPreVerify("hash", state)

	state.mu.Lock()
	registered := state.preVerifyCancel != nil && state.preVerifyDone != nil
	state.mu.Unlock()
	if !registered {
		t.Fatal("startPreVerify did not register a stoppable pass")
	}

	state.stopPreVerify()

	// Post-stop the pass is gone, so verified can be read without the lock.
	for p := range numPieces {
		if !state.verified.Test(uint(p)) {
			continue
		}
		if p/2 == corruptFile {
			t.Errorf("piece %d of the corrupt file was marked verified", p)
			continue
		}
		got, readErr := os.ReadFile(files[p/2].path)
		if readErr != nil {
			t.Fatal(readErr)
		}
		at := int64(p)*pieceLength - files[p/2].offset
		if utils.ComputeSHA1(got[at:at+pieceLength]) != hashes[p] {
			t.Errorf("piece %d marked verified but does not match on disk", p)
		}
	}
	if state.written.Count() != numPieces {
		t.Errorf("written pieces = %d, want %d (pass must not clear bits)", state.written.Count(), numPieces)
	}
}

// TestStartPreVerify_NoCandidates pins that a torrent with nothing already on
// disk registers no pass at all, so stopPreVerify stays free on the finalize
// path for the common fresh-transfer case.
func TestStartPreVerify_NoCandidates(t *testing.T) {
	t.Parallel()

	state := &serverTorrentState{
		torrentMeta: torrentMeta{
			pieceHashes: []string{"a"},
			pieceLength: 16,
			totalSize:   16,
			files:       []*serverFileInfo{{path: "x", offset: 0, size: 16, selected: true}},
		},
		written:  bitset.New(1),
		verified: bitset.New(1),
	}

	s, _ := newTestDestServer(t)
	s.startPreVerify("hash", state)

	state.mu.Lock()
	defer state.mu.Unlock()
	if state.preVerifyCancel != nil || state.preVerifyDone != nil {
		t.Error("startPreVerify registered a pass with no candidate files")
	}
}
