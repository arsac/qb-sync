package destination

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/bits-and-blooms/bitset"

	"github.com/arsac/qb-sync/internal/utils"
	pb "github.com/arsac/qb-sync/proto"
)

func TestClassifyPiece(t *testing.T) {
	t.Parallel()

	t.Run("aligned files", func(t *testing.T) {
		t.Parallel()
		// pieceLength=100, totalSize=300
		// File 0: offset=0,   size=100 (selected)   -> piece 0
		// File 1: offset=100, size=100 (unselected)  -> piece 1
		// File 2: offset=200, size=100 (selected)    -> piece 2
		state := &serverTorrentState{
			torrentMeta: torrentMeta{
				pieceLength: 100,
				totalSize:   300,
				files: []*serverFileInfo{
					{offset: 0, size: 100, selected: true},
					{offset: 100, size: 100, selected: false},
					{offset: 200, size: 100, selected: true},
				},
			},
		}

		tests := []struct {
			pieceIdx int
			want     pieceClass
		}{
			{0, pieceFullySelected},
			{1, pieceNoSelectedOverlap},
			{2, pieceFullySelected},
		}
		for _, tt := range tests {
			got := state.classifyPiece(tt.pieceIdx)
			if got != tt.want {
				t.Errorf("piece %d: got %d, want %d", tt.pieceIdx, got, tt.want)
			}
		}
	})

	t.Run("boundary piece spanning selected and unselected", func(t *testing.T) {
		t.Parallel()
		// pieceLength=100, totalSize=200
		// File 0: offset=0,  size=80  (selected)   -> piece 0
		// File 1: offset=80, size=120 (unselected)  -> pieces 0..1
		state := &serverTorrentState{
			torrentMeta: torrentMeta{
				pieceLength: 100,
				totalSize:   200,
				files: []*serverFileInfo{
					{offset: 0, size: 80, selected: true},
					{offset: 80, size: 120, selected: false},
				},
			},
		}

		if got := state.classifyPiece(0); got != pieceBoundary {
			t.Errorf("piece 0: got %d, want pieceBoundary (%d)", got, pieceBoundary)
		}
		if got := state.classifyPiece(1); got != pieceNoSelectedOverlap {
			t.Errorf("piece 1: got %d, want pieceNoSelectedOverlap (%d)", got, pieceNoSelectedOverlap)
		}
	})

	t.Run("all selected", func(t *testing.T) {
		t.Parallel()
		state := &serverTorrentState{
			torrentMeta: torrentMeta{
				pieceLength: 100,
				totalSize:   200,
				files: []*serverFileInfo{
					{offset: 0, size: 100, selected: true},
					{offset: 100, size: 100, selected: true},
				},
			},
		}
		if got := state.classifyPiece(0); got != pieceFullySelected {
			t.Errorf("piece 0: got %d, want pieceFullySelected (%d)", got, pieceFullySelected)
		}
	})
}

func TestCountSelectedPiecesTotal(t *testing.T) {
	t.Parallel()

	// pieceLength=100, totalSize=300
	// File 0: offset=0,   size=100 (selected)   -> piece 0
	// File 1: offset=100, size=100 (unselected)  -> piece 1
	// File 2: offset=200, size=100 (selected)    -> piece 2
	state := &serverTorrentState{
		torrentMeta: torrentMeta{
			pieceLength: 100,
			totalSize:   300,
			files: []*serverFileInfo{
				{offset: 0, size: 100, selected: true},
				{offset: 100, size: 100, selected: false},
				{offset: 200, size: 100, selected: true},
			},
		},
		written: bitset.New(3),
	}

	got := state.countSelectedPiecesTotal()
	if got != 2 {
		t.Errorf("countSelectedPiecesTotal = %d, want 2", got)
	}
}

// --- calculatePiecesCovered tests with unselected files ---

func TestCalculatePiecesCovered_UnselectedFiles(t *testing.T) {
	t.Parallel()

	t.Run("unselected files mark pieces as covered", func(t *testing.T) {
		t.Parallel()
		// pieceLength=100, totalSize=300, 3 pieces
		// File 0: selected, hlStateNone  -> piece 0 NOT covered
		// File 1: unselected             -> piece 1 covered
		// File 2: selected, hlStateNone  -> piece 2 NOT covered
		meta := torrentMeta{
			pieceLength: 100,
			totalSize:   300,
			files: []*serverFileInfo{
				{offset: 0, size: 100, selected: true, hardlink: hardlinkInfo{state: hlStateNone}},
				{offset: 100, size: 100, selected: false},
				{offset: 200, size: 100, selected: true, hardlink: hardlinkInfo{state: hlStateNone}},
			},
		}
		covered := meta.calculatePiecesCovered()
		if covered[0] {
			t.Error("piece 0 should NOT be covered (selected, hlStateNone)")
		}
		if !covered[1] {
			t.Error("piece 1 SHOULD be covered (only overlaps unselected file)")
		}
		if covered[2] {
			t.Error("piece 2 should NOT be covered (selected, hlStateNone)")
		}
	})

	t.Run("boundary piece covered when all overlapping files unselected or hardlinked", func(t *testing.T) {
		t.Parallel()
		// pieceLength=100, totalSize=200
		// File 0: offset=0,  size=50, hlStateComplete, selected=true   -> piece 0
		// File 1: offset=50, size=150, unselected                      -> pieces 0..1
		meta := torrentMeta{
			pieceLength: 100,
			totalSize:   200,
			files: []*serverFileInfo{
				{offset: 0, size: 50, hardlink: hardlinkInfo{state: hlStateComplete}, selected: true},
				{offset: 50, size: 150, selected: false},
			},
		}
		covered := meta.calculatePiecesCovered()
		if !covered[0] {
			t.Error("piece 0 should be covered (hlStateComplete + unselected)")
		}
		if !covered[1] {
			t.Error("piece 1 should be covered (only overlaps unselected file)")
		}
	})

	t.Run("boundary piece NOT covered when any selected non-hardlinked file overlaps", func(t *testing.T) {
		t.Parallel()
		// pieceLength=100, totalSize=200
		// File 0: offset=0,  size=50, unselected        -> piece 0
		// File 1: offset=50, size=150, selected, none   -> pieces 0..1
		meta := torrentMeta{
			pieceLength: 100,
			totalSize:   200,
			files: []*serverFileInfo{
				{offset: 0, size: 50, selected: false},
				{offset: 50, size: 150, selected: true, hardlink: hardlinkInfo{state: hlStateNone}},
			},
		}
		covered := meta.calculatePiecesCovered()
		if covered[0] {
			t.Error("piece 0 should NOT be covered (selected file 1 overlaps)")
		}
		if covered[1] {
			t.Error("piece 1 should NOT be covered (selected file 1 overlaps)")
		}
	})
}

// --- writePieceData tests with unselected files ---

func TestWritePieceData_SkipsUnselectedFiles(t *testing.T) {
	t.Parallel()
	_, tmpDir := newTestDestServer(t)

	// File 0: selected, gets data written
	selectedPath := filepath.Join(tmpDir, "selected.bin.partial")
	// File 1: unselected, skipped (no .partial file)

	state := &serverTorrentState{
		torrentMeta: torrentMeta{
			pieceLength: 200,
			totalSize:   200,
			files: []*serverFileInfo{
				{path: selectedPath, size: 100, offset: 0, selected: true},
				{path: filepath.Join(tmpDir, "unselected.bin"), size: 100, offset: 100, selected: false},
			},
		},
	}

	// Create .partial for selected file
	if err := os.WriteFile(selectedPath, make([]byte, 100), 0o644); err != nil {
		t.Fatal(err)
	}

	// Write piece data spanning both files (200 bytes at offset 0)
	data := make([]byte, 200)
	for i := range data {
		data[i] = byte(i % 256)
	}

	if err := state.writePieceData(0, data); err != nil {
		t.Fatalf("writePieceData error: %v", err)
	}

	// Verify selected file got data
	content, err := os.ReadFile(selectedPath)
	if err != nil {
		t.Fatalf("reading selected file: %v", err)
	}
	if len(content) != 100 {
		t.Errorf("selected file size = %d, want 100", len(content))
	}
	for i := range 100 {
		if content[i] != byte(i%256) {
			t.Errorf("selected file byte %d = %d, want %d", i, content[i], byte(i%256))
			break
		}
	}

	// Verify unselected file was NOT created
	if _, statErr := os.Stat(state.files[1].path); !os.IsNotExist(statErr) {
		t.Error("unselected file should not exist on disk")
	}
}

// --- Persistence tests ---

func TestSaveLoadSelectedFile(t *testing.T) {
	t.Parallel()

	t.Run("roundtrip", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()

		files := []*serverFileInfo{
			{selected: true},
			{selected: false},
			{selected: true},
			{selected: false},
		}

		if err := saveSelectedFile(tmpDir, files); err != nil {
			t.Fatalf("saveSelectedFile: %v", err)
		}

		loaded := loadSelectedFile(tmpDir, 4)
		if loaded == nil {
			t.Fatal("loadSelectedFile returned nil")
		}

		expected := []bool{true, false, true, false}
		for i, want := range expected {
			if loaded[i] != want {
				t.Errorf("file %d: got %v, want %v", i, loaded[i], want)
			}
		}
	})

	t.Run("missing file returns nil", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		loaded := loadSelectedFile(tmpDir, 3)
		if loaded != nil {
			t.Error("expected nil for missing file")
		}
	})

	t.Run("file shorter than numFiles pads with false", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()

		// Save 2 files, then load expecting 4
		files := []*serverFileInfo{
			{selected: true},
			{selected: false},
		}
		if err := saveSelectedFile(tmpDir, files); err != nil {
			t.Fatal(err)
		}

		loaded := loadSelectedFile(tmpDir, 4)
		if loaded == nil {
			t.Fatal("loadSelectedFile returned nil")
		}
		if !loaded[0] || loaded[1] || loaded[2] || loaded[3] {
			t.Errorf("loaded = %v, want [true false false false]", loaded)
		}
	})
}

// --- setupFile tests for unselected files ---

func TestSetupFile_UnselectedFile(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	logger := testLogger(t)

	t.Run("unselected file gets no .partial and no directory creation", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		s := &Server{
			config: ServerConfig{BasePath: tmpDir},
			logger: logger,
			store:  newTorrentStore(tmpDir, logger),
		}

		fileInfo, result, err := s.setupFile(ctx, "hash1", &pb.FileInfo{
			Path:     "deep/nested/dir/file.bin",
			Size:     1024,
			Offset:   0,
			Selected: false,
		}, 0, "")

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Should have final path (no .partial suffix)
		expectedPath := filepath.Join(tmpDir, "deep/nested/dir/file.bin")
		if fileInfo.path != expectedPath {
			t.Errorf("path = %q, want %q", fileInfo.path, expectedPath)
		}
		if fileInfo.selected {
			t.Error("expected selected=false")
		}
		// No directory should have been created
		if _, dirErr := os.Stat(filepath.Join(tmpDir, "deep/nested/dir")); !os.IsNotExist(dirErr) {
			t.Error("directory should NOT be created for unselected files")
		}
		// Result should have no special flags
		if result.GetPreExisting() || result.GetHardlinked() || result.GetPending() {
			t.Error("unselected file should have no special flags in result")
		}
	})
}

// --- FinalizeTorrent with partial selection ---

func TestFinalizeTorrent_PartialSelection(t *testing.T) {
	t.Parallel()
	s, tmpDir := newTestDestServer(t)
	// InodeRegistry already initialized by newTorrentStore

	hash := "partial-select-finalize"

	// 3 pieces, 3 files. File 1 (piece 1) is unselected.
	// Only pieces 0 and 2 need to be written.
	pieceData0 := []byte("piece-zero-data!") // 16 bytes
	pieceData2 := []byte("piece-two--data!") // 16 bytes
	pieceHash0 := utils.ComputeSHA1(pieceData0)
	pieceHash2 := utils.ComputeSHA1(pieceData2)

	pieceLength := int64(16)
	totalSize := int64(48) // 3 pieces

	partialFile0 := filepath.Join(tmpDir, "file0.bin.partial")
	partialFile2 := filepath.Join(tmpDir, "file2.bin.partial")

	state := &serverTorrentState{
		torrentMeta: torrentMeta{
			pieceHashes: []string{pieceHash0, "", pieceHash2},
			pieceLength: pieceLength,
			totalSize:   totalSize,
			files: []*serverFileInfo{
				{path: partialFile0, size: 16, offset: 0, selected: true},
				{path: filepath.Join(tmpDir, "file1.bin"), size: 16, offset: 16, selected: false, earlyFinalized: true},
				{path: partialFile2, size: 16, offset: 32, selected: true},
			},
		},
		written:   bitset.New(3),
		statePath: filepath.Join(tmpDir, ".state"),
	}

	// Register state
	s.store.mu.Lock()
	s.store.entries[hash] = state
	s.store.mu.Unlock()

	ctx := context.Background()

	// Write piece 0
	result := s.writePiece(ctx, &pb.WritePieceRequest{
		TorrentHash: hash,
		PieceIndex:  0,
		Offset:      0,
		Size:        16,
		Data:        pieceData0,
		PieceHash:   pieceHash0,
	})
	if !result.success {
		t.Fatalf("writePiece 0 failed: %s", result.errMsg)
	}

	// Write piece 2
	result = s.writePiece(ctx, &pb.WritePieceRequest{
		TorrentHash: hash,
		PieceIndex:  2,
		Offset:      32,
		Size:        16,
		Data:        pieceData2,
		PieceHash:   pieceHash2,
	})
	if !result.success {
		t.Fatalf("writePiece 2 failed: %s", result.errMsg)
	}

	// Verify written count is 2 (not 3)
	state.mu.Lock()
	wc := state.written.Count()
	state.mu.Unlock()
	if wc != 2 {
		t.Errorf("writtenCount = %d, want 2", wc)
	}

	// FinalizeTorrent should succeed with only selected pieces written
	fResp, fErr := s.FinalizeTorrent(ctx, &pb.FinalizeTorrentRequest{
		TorrentHash: hash,
	})
	if fErr != nil {
		t.Fatalf("FinalizeTorrent error: %v", fErr)
	}
	if !fResp.GetSuccess() {
		t.Fatalf("FinalizeTorrent failed: %s (code: %v)", fResp.GetError(), fResp.GetErrorCode())
	}

	// Wait for the background finalization goroutine to complete before
	// checking results and allowing TempDir cleanup.
	state.mu.Lock()
	done := state.finalization.done
	state.mu.Unlock()
	if done != nil {
		<-done
	}

	// Unselected file (file1.bin) should NOT exist on disk
	if _, statErr := os.Stat(filepath.Join(tmpDir, "file1.bin")); !os.IsNotExist(statErr) {
		t.Error("unselected file1.bin should not exist on disk")
	}
}

// --- Stale piece clearing ---

func TestClearStalePieces(t *testing.T) {
	t.Parallel()
	s, tmpDir := newTestDestServer(t)

	existingFile := filepath.Join(tmpDir, "exists.bin")
	writeTestFile(t, existingFile, []byte("data"))

	t.Run("clears only missing file pieces", func(t *testing.T) {
		t.Parallel()
		files := []*serverFileInfo{
			{path: existingFile, selected: true, firstPiece: 0, lastPiece: 2},
			{path: filepath.Join(tmpDir, "missing.bin"), selected: true, firstPiece: 3, lastPiece: 5},
		}
		written := boolSliceToBitSet([]bool{true, true, true, true, true, true})

		s.clearStalePieces(context.Background(), "test", written, files)

		// Pieces 0-2 (existing file) should be preserved
		for i := range uint(3) {
			if !written.Test(i) {
				t.Errorf("piece %d should be preserved (file exists)", i)
			}
		}
		// Pieces 3-5 (missing file) should be cleared
		for i := uint(3); i < 6; i++ {
			if written.Test(i) {
				t.Errorf("piece %d should be cleared (file missing)", i)
			}
		}
	})

	t.Run("skips unselected and hardlinked files", func(t *testing.T) {
		t.Parallel()
		files := []*serverFileInfo{
			{path: filepath.Join(tmpDir, "missing1.bin"), selected: false, firstPiece: 0, lastPiece: 1},
			{path: filepath.Join(tmpDir, "missing2.bin"), selected: true, firstPiece: 2, lastPiece: 3,
				hardlink: hardlinkInfo{state: hlStatePending}},
		}
		written := boolSliceToBitSet([]bool{true, true, true, true})

		s.clearStalePieces(context.Background(), "test", written, files)

		// All pieces should be preserved — unselected and pending-hardlink files are skipped
		for i := range uint(4) {
			if !written.Test(i) {
				t.Errorf("piece %d should be preserved", i)
			}
		}
	})
}

// --- InitTorrent state cleaning ---

func TestInitTorrent_StaleMetadata_NukedBeforeInit(t *testing.T) {
	t.Parallel()
	s, tmpDir := newTestDestServer(t)
	// InodeRegistry already initialized by newTorrentStore

	hash := "stale-init-test"

	// Pre-create a stale metadata directory with no version file and a
	// bogus .state that claims all pieces are written.
	metaDir := filepath.Join(tmpDir, metaDirName, hash)
	if err := os.MkdirAll(metaDir, 0o755); err != nil {
		t.Fatal(err)
	}
	staleState := []byte{1, 1, 1} // 3 pieces "written"
	if err := os.WriteFile(filepath.Join(metaDir, ".state"), staleState, 0o644); err != nil {
		t.Fatal(err)
	}

	// InitTorrent should nuke the stale directory and start fresh.
	resp, err := s.InitTorrent(context.Background(), &pb.InitTorrentRequest{
		TorrentHash: hash,
		Name:        "test-torrent",
		NumPieces:   3,
		PieceSize:   100,
		TotalSize:   300,
		Files: []*pb.FileInfo{
			{Path: "file0.bin", Size: 100, Offset: 0, Selected: true},
			{Path: "file1.bin", Size: 100, Offset: 100, Selected: true},
			{Path: "file2.bin", Size: 100, Offset: 200, Selected: true},
		},
	})
	if err != nil {
		t.Fatalf("InitTorrent error: %v", err)
	}
	if !resp.GetSuccess() {
		t.Fatalf("InitTorrent failed: %s", resp.GetError())
	}

	// Stale .state was nuked — all 3 pieces should be needed (not pre-written).
	if resp.GetPiecesNeededCount() != 3 {
		t.Errorf("expected 3 pieces needed (stale state nuked), got %d", resp.GetPiecesNeededCount())
	}

	// .meta file should exist now.
	metaPath := filepath.Join(metaDir, metaFileName)
	if _, statErr := os.Stat(metaPath); statErr != nil {
		t.Fatalf(".meta file missing after init: %v", statErr)
	}
}

// --- InitTorrent with partial selection ---

func TestInitTorrent_PartialSelection_PiecesCovered(t *testing.T) {
	t.Parallel()
	s, _ := newTestDestServer(t)

	// 3 files, 3 pieces. File 1 is unselected -> piece 1 should be "covered" (not needed)
	resp, err := s.InitTorrent(context.Background(), &pb.InitTorrentRequest{
		TorrentHash: "partial-select-init",
		Name:        "test-torrent",
		NumPieces:   3,
		PieceSize:   100,
		TotalSize:   300,
		Files: []*pb.FileInfo{
			{Path: "file0.bin", Size: 100, Offset: 0, Selected: true},
			{Path: "file1.bin", Size: 100, Offset: 100, Selected: false},
			{Path: "file2.bin", Size: 100, Offset: 200, Selected: true},
		},
	})
	if err != nil {
		t.Fatalf("InitTorrent error: %v", err)
	}
	if !resp.GetSuccess() {
		t.Fatalf("InitTorrent failed: %s", resp.GetError())
	}

	// Piece 0: needed (selected file)
	// Piece 1: NOT needed (unselected file)
	// Piece 2: needed (selected file)
	if resp.GetPiecesNeededCount() != 2 {
		t.Errorf("expected 2 pieces needed, got %d", resp.GetPiecesNeededCount())
	}
	if resp.GetPiecesHaveCount() != 1 {
		t.Errorf("expected 1 piece have (covered by unselected), got %d", resp.GetPiecesHaveCount())
	}

	pn := resp.GetPiecesNeeded()
	if !pn[0] {
		t.Error("piece 0 should be needed (selected file)")
	}
	if pn[1] {
		t.Error("piece 1 should NOT be needed (unselected file)")
	}
	if !pn[2] {
		t.Error("piece 2 should be needed (selected file)")
	}
}
