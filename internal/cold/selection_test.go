package cold

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/arsac/qb-sync/internal/utils"
	pb "github.com/arsac/qb-sync/proto"
)

// --- Helper tests ---

func TestAnyProtoFileSelected(t *testing.T) {
	t.Parallel()

	t.Run("no files selected returns false", func(t *testing.T) {
		t.Parallel()
		files := []*pb.FileInfo{
			{Path: "a.txt", Selected: false},
			{Path: "b.txt", Selected: false},
		}
		if anyProtoFileSelected(files) {
			t.Error("expected false when no file is selected")
		}
	})

	t.Run("one file selected returns true", func(t *testing.T) {
		t.Parallel()
		files := []*pb.FileInfo{
			{Path: "a.txt", Selected: false},
			{Path: "b.txt", Selected: true},
		}
		if !anyProtoFileSelected(files) {
			t.Error("expected true when one file is selected")
		}
	})

	t.Run("empty list returns false", func(t *testing.T) {
		t.Parallel()
		if anyProtoFileSelected(nil) {
			t.Error("expected false for nil list")
		}
	})
}

func TestPieceOverlapsSelectedFile(t *testing.T) {
	t.Parallel()

	// Layout: pieceLength=100, totalSize=300
	// File 0: offset=0,   size=100 (selected)   -> piece 0
	// File 1: offset=100, size=100 (unselected)  -> piece 1
	// File 2: offset=200, size=100 (selected)    -> piece 2
	files := []*serverFileInfo{
		{offset: 0, size: 100, selected: true},
		{offset: 100, size: 100, selected: false},
		{offset: 200, size: 100, selected: true},
	}

	tests := []struct {
		pieceIdx int
		want     bool
	}{
		{0, true},  // overlaps selected file 0
		{1, false}, // overlaps only unselected file 1
		{2, true},  // overlaps selected file 2
	}

	for _, tt := range tests {
		got := pieceOverlapsSelectedFile(files, tt.pieceIdx, 100, 300)
		if got != tt.want {
			t.Errorf("piece %d: got %v, want %v", tt.pieceIdx, got, tt.want)
		}
	}
}

func TestPieceEntirelyInSelectedFiles(t *testing.T) {
	t.Parallel()

	// pieceLength=100, totalSize=200
	// File 0: offset=0,  size=80  (selected)   -> piece 0 only
	// File 1: offset=80, size=120 (unselected) -> pieces 0..1
	files := []*serverFileInfo{
		{offset: 0, size: 80, selected: true},
		{offset: 80, size: 120, selected: false},
	}

	// Piece 0 spans file 0 (selected) and file 1 (unselected) -> boundary
	if pieceEntirelyInSelectedFiles(files, 0, 100, 200) {
		t.Error("piece 0 should NOT be entirely in selected files (boundary)")
	}

	// Piece 1 only overlaps file 1 (unselected) -> not entirely selected
	if pieceEntirelyInSelectedFiles(files, 1, 100, 200) {
		t.Error("piece 1 should NOT be entirely in selected files")
	}

	// All-selected case
	allSelected := []*serverFileInfo{
		{offset: 0, size: 100, selected: true},
		{offset: 100, size: 100, selected: true},
	}
	if !pieceEntirelyInSelectedFiles(allSelected, 0, 100, 200) {
		t.Error("piece 0 should be entirely in selected files when all selected")
	}
}

func TestCountSelectedPiecesTotal(t *testing.T) {
	t.Parallel()

	// pieceLength=100, totalSize=300
	// File 0: offset=0,   size=100 (selected)   -> piece 0
	// File 1: offset=100, size=100 (unselected)  -> piece 1
	// File 2: offset=200, size=100 (selected)    -> piece 2
	state := &serverTorrentState{
		written:     []bool{false, false, false},
		pieceLength: 100,
		totalSize:   300,
		files: []*serverFileInfo{
			{offset: 0, size: 100, selected: true},
			{offset: 100, size: 100, selected: false},
			{offset: 200, size: 100, selected: true},
		},
	}

	got := countSelectedPiecesTotal(state)
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
		files := []*serverFileInfo{
			{offset: 0, size: 100, selected: true, hlState: hlStateNone},
			{offset: 100, size: 100, selected: false},
			{offset: 200, size: 100, selected: true, hlState: hlStateNone},
		}
		covered := calculatePiecesCovered(files, 3, 100, 300)
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
		files := []*serverFileInfo{
			{offset: 0, size: 50, hlState: hlStateComplete, selected: true},
			{offset: 50, size: 150, selected: false},
		}
		covered := calculatePiecesCovered(files, 2, 100, 200)
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
		files := []*serverFileInfo{
			{offset: 0, size: 50, selected: false},
			{offset: 50, size: 150, selected: true, hlState: hlStateNone},
		}
		covered := calculatePiecesCovered(files, 2, 100, 200)
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
	s, tmpDir := newTestColdServer(t)

	// File 0: selected, gets data written
	selectedPath := filepath.Join(tmpDir, "selected.bin.partial")
	// File 1: unselected, skipped (no .partial file)

	state := &serverTorrentState{
		pieceLength: 200,
		totalSize:   200,
		files: []*serverFileInfo{
			{path: selectedPath, size: 100, offset: 0, selected: true},
			{path: filepath.Join(tmpDir, "unselected.bin"), size: 100, offset: 100, selected: false},
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

	if err := s.writePieceData(state, 0, data); err != nil {
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
	unselectedPath := filepath.Join(tmpDir, "unselected.bin")
	if _, statErr := os.Stat(unselectedPath); !os.IsNotExist(statErr) {
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

// --- Backward compatibility tests ---

func TestInitTorrent_BackwardCompat_AllFilesSelected(t *testing.T) {
	t.Parallel()
	s, tmpDir := newTestColdServer(t)
	s.inodes = NewInodeRegistry(tmpDir, testLogger(t))

	// Send files without Selected field (all false = legacy hot)
	resp, err := s.InitTorrent(context.Background(), &pb.InitTorrentRequest{
		TorrentHash: "backcompat-test",
		Name:        "test-torrent",
		NumPieces:   2,
		PieceSize:   512,
		TotalSize:   1024,
		Files: []*pb.FileInfo{
			{Path: "file1.bin", Size: 512, Offset: 0},   // Selected not set (false)
			{Path: "file2.bin", Size: 512, Offset: 512}, // Selected not set (false)
		},
	})
	if err != nil {
		t.Fatalf("InitTorrent error: %v", err)
	}
	if !resp.GetSuccess() {
		t.Fatalf("InitTorrent failed: %s", resp.GetError())
	}

	// All pieces should be needed (backward compat treats all as selected)
	if resp.GetPiecesNeededCount() != 2 {
		t.Errorf("expected 2 pieces needed (backward compat), got %d", resp.GetPiecesNeededCount())
	}

	// Verify state files are marked selected
	s.mu.RLock()
	state := s.torrents["backcompat-test"]
	s.mu.RUnlock()

	state.mu.Lock()
	for i, fi := range state.files {
		if !fi.selected {
			t.Errorf("file %d should be selected (backward compat)", i)
		}
	}
	state.mu.Unlock()
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
			inodes: NewInodeRegistry(tmpDir, logger),
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
	s, tmpDir := newTestColdServer(t)
	s.inodes = NewInodeRegistry(tmpDir, testLogger(t))

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
		written:      []bool{false, false, false},
		writtenCount: 0,
		pieceHashes:  []string{pieceHash0, "", pieceHash2},
		pieceLength:  pieceLength,
		totalSize:    totalSize,
		files: []*serverFileInfo{
			{path: partialFile0, size: 16, offset: 0, selected: true},
			{path: filepath.Join(tmpDir, "file1.bin"), size: 16, offset: 16, selected: false, earlyFinalized: true},
			{path: partialFile2, size: 16, offset: 32, selected: true},
		},
		statePath: filepath.Join(tmpDir, ".state"),
	}

	// Register state
	s.mu.Lock()
	s.torrents[hash] = state
	s.mu.Unlock()

	ctx := context.Background()

	// Write piece 0
	resp, err := s.WritePiece(ctx, &pb.WritePieceRequest{
		TorrentHash: hash,
		PieceIndex:  0,
		Offset:      0,
		Size:        16,
		Data:        pieceData0,
		PieceHash:   pieceHash0,
	})
	if err != nil || !resp.GetSuccess() {
		t.Fatalf("WritePiece 0 failed: err=%v resp=%v", err, resp)
	}

	// Write piece 2
	resp, err = s.WritePiece(ctx, &pb.WritePieceRequest{
		TorrentHash: hash,
		PieceIndex:  2,
		Offset:      32,
		Size:        16,
		Data:        pieceData2,
		PieceHash:   pieceHash2,
	})
	if err != nil || !resp.GetSuccess() {
		t.Fatalf("WritePiece 2 failed: err=%v resp=%v", err, resp)
	}

	// Verify written count is 2 (not 3)
	state.mu.Lock()
	wc := state.writtenCount
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

	// Unselected file (file1.bin) should NOT exist on disk
	if _, statErr := os.Stat(filepath.Join(tmpDir, "file1.bin")); !os.IsNotExist(statErr) {
		t.Error("unselected file1.bin should not exist on disk")
	}
}

// --- Recovery with .selected file ---

func TestRecoverTorrentState_LoadsSelection(t *testing.T) {
	t.Parallel()
	s, tmpDir := newTestColdServer(t)

	hash := "recover-selection"
	metaDir := filepath.Join(tmpDir, metaDirName, hash)

	// Create torrent metadata
	createTestTorrentFileWithPaths(t, tmpDir, hash, []string{"file1.bin", "file2.bin"})

	// Create state file (2 pieces, both written)
	statePath := filepath.Join(metaDir, ".state")
	if err := os.WriteFile(statePath, []byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, 0o644); err != nil {
		t.Fatal(err)
	}

	// Create .selected file: file1 selected, file2 unselected
	if err := os.WriteFile(filepath.Join(metaDir, selectedFileName), []byte{1, 0}, 0o644); err != nil {
		t.Fatal(err)
	}

	// Create the selected file on disk
	writeTestFile(t, filepath.Join(tmpDir, "test", "file1.bin"), make([]byte, 1024))

	ctx := context.Background()
	state, err := s.recoverTorrentState(ctx, hash)
	if err != nil {
		t.Fatalf("recoverTorrentState: %v", err)
	}

	if len(state.files) != 2 {
		t.Fatalf("expected 2 files, got %d", len(state.files))
	}

	if !state.files[0].selected {
		t.Error("file 0 should be selected")
	}
	if state.files[1].selected {
		t.Error("file 1 should be unselected")
	}

	// Unselected file should be marked as earlyFinalized
	if !state.files[1].earlyFinalized {
		t.Error("unselected file should be marked earlyFinalized")
	}
}

func TestRecoverTorrentState_NoSelectedFile_DefaultsAllSelected(t *testing.T) {
	t.Parallel()
	s, tmpDir := newTestColdServer(t)

	hash := "recover-no-selection"
	metaDir := filepath.Join(tmpDir, metaDirName, hash)

	// Create torrent metadata (no .selected file)
	createTestTorrentFileWithPaths(t, tmpDir, hash, []string{"file1.bin"})
	statePath := filepath.Join(metaDir, ".state")
	if err := os.WriteFile(statePath, []byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, 0o644); err != nil {
		t.Fatal(err)
	}

	// Create the file on disk
	writeTestFile(t, filepath.Join(tmpDir, "test", "file1.bin"), make([]byte, 1024))

	ctx := context.Background()
	state, err := s.recoverTorrentState(ctx, hash)
	if err != nil {
		t.Fatalf("recoverTorrentState: %v", err)
	}

	if !state.files[0].selected {
		t.Error("file should default to selected when .selected file is missing")
	}
}

// --- InitTorrent with partial selection ---

func TestInitTorrent_PartialSelection_PiecesCovered(t *testing.T) {
	t.Parallel()
	s, tmpDir := newTestColdServer(t)
	s.inodes = NewInodeRegistry(tmpDir, testLogger(t))

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
