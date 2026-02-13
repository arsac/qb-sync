package destination

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

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
		computeFilePieceRanges(files, 100, 250)

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
		computeFilePieceRanges(files, 100, 150)

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
		computeFilePieceRanges(files, 100, 50)

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
		computeFilePieceRanges(files, 100, 200)

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
		computeFilePieceRanges(files, 100, 150)

		assertFileRange(t, files[0], 0, 1, 2)
	})

	t.Run("zero pieceLength is no-op", func(t *testing.T) {
		t.Parallel()
		files := []*serverFileInfo{
			{offset: 0, size: 100, selected: true},
		}
		computeFilePieceRanges(files, 0, 100)

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
		written := []bool{true, false, true}
		initFilePieceCounts(files, written)

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
		written := []bool{true, true}
		initFilePieceCounts(files, written)

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
		written := []bool{true, true}
		initFilePieceCounts(files, written)

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
		written := []bool{true}
		initFilePieceCounts(files, written)

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
		s, tmpDir := newTestColdServer(t)

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
			files: []*serverFileInfo{fi},
		}

		ctx := context.Background()
		state.mu.Lock()
		s.checkFileCompletions(ctx, "test-hash", state, 0)
		state.mu.Unlock()

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
		s, tmpDir := newTestColdServer(t)

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
		state := &serverTorrentState{files: files}

		ctx := context.Background()
		state.mu.Lock()
		s.checkFileCompletions(ctx, "test-hash", state, 0)
		state.mu.Unlock()

		if !files[0].earlyFinalized {
			t.Error("file 0 should be early-finalized")
		}
		if !files[1].earlyFinalized {
			t.Error("file 1 should be early-finalized")
		}
	})

	t.Run("incomplete file not finalized", func(t *testing.T) {
		t.Parallel()
		s, tmpDir := newTestColdServer(t)

		partialPath := filepath.Join(tmpDir, "incomplete.txt.partial")
		if err := os.WriteFile(partialPath, []byte("data"), 0o644); err != nil {
			t.Fatal(err)
		}

		fi := &serverFileInfo{
			path: partialPath, size: 4, offset: 0,
			firstPiece: 0, lastPiece: 1, piecesTotal: 2, piecesWritten: 0, selected: true,
		}
		state := &serverTorrentState{files: []*serverFileInfo{fi}}

		ctx := context.Background()
		state.mu.Lock()
		s.checkFileCompletions(ctx, "test-hash", state, 0)
		state.mu.Unlock()

		if fi.earlyFinalized {
			t.Error("file should NOT be early-finalized with only 1/2 pieces")
		}
		if fi.piecesWritten != 1 {
			t.Errorf("piecesWritten = %d, want 1", fi.piecesWritten)
		}
	})

	t.Run("hardlinked files skipped", func(t *testing.T) {
		t.Parallel()
		s, _ := newTestColdServer(t)

		fi := &serverFileInfo{
			size: 100, offset: 0,
			firstPiece: 0, lastPiece: 0, piecesTotal: 1,
			hlState: hlStateComplete, selected: true,
		}
		state := &serverTorrentState{files: []*serverFileInfo{fi}}

		ctx := context.Background()
		state.mu.Lock()
		s.checkFileCompletions(ctx, "test-hash", state, 0)
		state.mu.Unlock()

		if fi.piecesWritten != 0 {
			t.Error("hardlinked file should not have piecesWritten incremented")
		}
	})

	t.Run("already early-finalized files skipped", func(t *testing.T) {
		t.Parallel()
		s, _ := newTestColdServer(t)

		fi := &serverFileInfo{
			size: 100, offset: 0,
			firstPiece: 0, lastPiece: 0, piecesTotal: 1,
			earlyFinalized: true, selected: true,
		}
		state := &serverTorrentState{files: []*serverFileInfo{fi}}

		ctx := context.Background()
		state.mu.Lock()
		s.checkFileCompletions(ctx, "test-hash", state, 0)
		state.mu.Unlock()

		if fi.piecesWritten != 0 {
			t.Error("already-finalized file should not have piecesWritten incremented")
		}
	})

	t.Run("out-of-range piece does not increment", func(t *testing.T) {
		t.Parallel()
		s, _ := newTestColdServer(t)

		fi := &serverFileInfo{
			size: 100, offset: 100,
			firstPiece: 1, lastPiece: 1, piecesTotal: 1, selected: true,
		}
		state := &serverTorrentState{files: []*serverFileInfo{fi}}

		ctx := context.Background()
		state.mu.Lock()
		s.checkFileCompletions(ctx, "test-hash", state, 0) // piece 0 doesn't overlap
		state.mu.Unlock()

		if fi.piecesWritten != 0 {
			t.Error("out-of-range piece should not increment piecesWritten")
		}
	})

	t.Run("sync error defers to finalization", func(t *testing.T) {
		t.Parallel()
		s, tmpDir := newTestColdServer(t)

		// Create file then remove it to cause sync to fail (file handle will be nil,
		// so closeFileHandle returns nil, but we can test by making the rename fail)
		partialPath := filepath.Join(tmpDir, "nosuchdir", "bad.txt.partial")

		fi := &serverFileInfo{
			path: partialPath, size: 10, offset: 0,
			firstPiece: 0, lastPiece: 0, piecesTotal: 1, piecesWritten: 0, selected: true,
		}
		state := &serverTorrentState{files: []*serverFileInfo{fi}}

		ctx := context.Background()
		state.mu.Lock()
		s.checkFileCompletions(ctx, "test-hash", state, 0)
		state.mu.Unlock()

		// rename should fail (parent dir doesn't exist), so earlyFinalized should be false
		if fi.earlyFinalized {
			t.Error("file should NOT be early-finalized when rename fails")
		}
	})
}

func TestFinalizeFiles_SkipsEarlyFinalizedFiles(t *testing.T) {
	t.Parallel()

	s, tmpDir := newTestColdServer(t)

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
		written:      []bool{true, true},
		writtenCount: 2,
		pieceLength:  100,
		totalSize:    200,
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

	s, tmpDir := newTestColdServer(t)

	hash := "write-piece-early-test"

	// Single file, single piece
	partialPath := filepath.Join(tmpDir, "single.bin.partial")
	pieceData := []byte("hello world!")
	pieceHash := utils.ComputeSHA1(pieceData)

	state := &serverTorrentState{
		written:      []bool{false},
		writtenCount: 0,
		pieceHashes:  []string{pieceHash},
		pieceLength:  int64(len(pieceData)),
		totalSize:    int64(len(pieceData)),
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
		statePath: filepath.Join(tmpDir, ".state"),
	}

	s.mu.Lock()
	s.torrents[hash] = state
	s.mu.Unlock()

	ctx := context.Background()
	result := s.writePiece(ctx, &pb.WritePieceRequest{
		TorrentHash: hash,
		PieceIndex:  0,
		Offset:      0,
		Data:        pieceData,
		PieceHash:   pieceHash,
	})
	if !result.success {
		t.Fatalf("writePiece failed: %s", result.errMsg)
	}

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

	s, tmpDir := newTestColdServer(t)

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
		written:      []bool{true},
		writtenCount: 1,
		pieceHashes:  []string{pieceHash},
		pieceLength:  int64(len(correctData)),
		totalSize:    int64(len(correctData)),
		files:        []*serverFileInfo{fi},
		statePath:    filepath.Join(tmpDir, ".state"),
	}

	ctx := context.Background()
	state.mu.Lock()
	s.checkFileCompletions(ctx, hash, state, 0)
	state.mu.Unlock()

	// File should NOT be early-finalized (verification failed).
	if fi.earlyFinalized {
		t.Error("file should NOT be early-finalized when verification fails")
	}

	// File should still be at .partial path.
	if fi.path != partialPath {
		t.Errorf("path = %s, want %s", fi.path, partialPath)
	}

	// Corrupted piece should be marked unwritten.
	if state.written[0] {
		t.Error("corrupted piece should be marked as unwritten")
	}
	if state.writtenCount != 0 {
		t.Errorf("writtenCount = %d, want 0", state.writtenCount)
	}
	if fi.piecesWritten != 0 {
		t.Errorf("piecesWritten = %d, want 0", fi.piecesWritten)
	}
}

func TestCheckFileCompletions_VerifySkipsBoundaryPieces(t *testing.T) {
	t.Parallel()

	s, tmpDir := newTestColdServer(t)

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
		written:      []bool{true},
		writtenCount: 1,
		pieceHashes:  []string{pieceHash},
		pieceLength:  10,
		totalSize:    10,
		files:        files,
	}

	ctx := context.Background()
	state.mu.Lock()
	s.checkFileCompletions(ctx, hash, state, 0)
	state.mu.Unlock()

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

	s, tmpDir := newTestColdServer(t)

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
		written:      []bool{true, true, true},
		writtenCount: 3,
		pieceHashes:  []string{hash0, hash1, hash2},
		pieceLength:  10,
		totalSize:    30,
		files:        []*serverFileInfo{fi},
		statePath:    filepath.Join(tmpDir, ".state"),
	}

	ctx := context.Background()
	state.mu.Lock()
	// Piece 2 is the "completing" piece.
	s.checkFileCompletions(ctx, hash, state, 2)
	state.mu.Unlock()

	// File should NOT be early-finalized.
	if fi.earlyFinalized {
		t.Error("file should NOT be early-finalized with corrupted piece")
	}

	// Only piece 1 should be marked unwritten; pieces 0 and 2 stay written.
	if !state.written[0] {
		t.Error("piece 0 should remain written (verified OK)")
	}
	if state.written[1] {
		t.Error("piece 1 should be marked unwritten (corrupted)")
	}
	if !state.written[2] {
		t.Error("piece 2 should remain written (verified OK)")
	}
	if state.writtenCount != 2 {
		t.Errorf("writtenCount = %d, want 2", state.writtenCount)
	}
	if fi.piecesWritten != 2 {
		t.Errorf("piecesWritten = %d, want 2", fi.piecesWritten)
	}
}

func TestCheckFileCompletions_VerifyNoPieceHashes(t *testing.T) {
	t.Parallel()

	s, tmpDir := newTestColdServer(t)

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
		written:      []bool{true},
		writtenCount: 1,
		pieceHashes:  nil, // No hashes available.
		pieceLength:  4,
		totalSize:    4,
		files:        []*serverFileInfo{fi},
	}

	ctx := context.Background()
	state.mu.Lock()
	s.checkFileCompletions(ctx, hash, state, 0)
	state.mu.Unlock()

	if !fi.earlyFinalized {
		t.Error("file should be early-finalized when no piece hashes are available")
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
