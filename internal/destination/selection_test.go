package destination

import (
	"bytes"
	"context"
	"fmt"
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

// classifyPieceScan is the pre-binary-search full scan, kept as the oracle for
// TestClassifyPiece_MatchesFullScan.
func classifyPieceScan(m *torrentMeta, pieceIdx int) pieceClass {
	pieceStart := int64(pieceIdx) * m.pieceLength
	pieceEnd := min(pieceStart+m.pieceLength, m.totalSize)

	hasSelected, hasUnselected := false, false
	for _, f := range m.files {
		if f.offset >= pieceEnd || f.offset+f.size <= pieceStart {
			continue
		}
		if f.selected {
			hasSelected = true
		} else {
			hasUnselected = true
		}
	}
	switch {
	case hasSelected && hasUnselected:
		return pieceBoundary
	case !hasSelected:
		return pieceNoSelectedOverlap
	default:
		return pieceFullySelected
	}
}

// coveredScan is the pre-binary-search full scan for calculatePiecesCovered.
func coveredScan(m *torrentMeta, pieceIdx int) bool {
	pieceStart := int64(pieceIdx) * m.pieceLength
	pieceEnd := min(pieceStart+m.pieceLength, m.totalSize)
	for _, f := range m.files {
		if f.offset < pieceEnd && f.offset+f.size > pieceStart && !f.skipForWriteData() {
			return false
		}
	}
	return true
}

// mixedLayoutMeta builds a contiguous multi-file torrent whose file sizes cross
// piece boundaries in every direction: sub-piece files, exact multiples, and
// files spanning many pieces, with zero-length files wedged between.
func mixedLayoutMeta(fileCount int) torrentMeta {
	const pieceLength int64 = 1 << 14
	sizes := []int64{pieceLength / 3, pieceLength, pieceLength*2 + 7, 0, pieceLength/2 + 1, pieceLength * 5}

	var offset int64
	files := make([]*serverFileInfo, 0, fileCount)
	for i := range fileCount {
		size := sizes[i%len(sizes)]
		hlState := hlStateNone
		if i%7 == 0 {
			hlState = hlStateComplete
		}
		files = append(files, &serverFileInfo{
			offset:   offset,
			size:     size,
			selected: i%4 != 0,
			hardlink: hardlinkInfo{state: hlState},
		})
		offset += size
	}
	return torrentMeta{pieceLength: pieceLength, totalSize: offset, files: files}
}

// TestClassifyPiece_MatchesFullScan pins the binary-search narrowing in
// classifyPiece and calculatePiecesCovered against the full scan they replaced.
// An off-by-one in the search bound or the break condition would drop the first
// or last overlapping file, silently mis-classifying boundary pieces.
func TestClassifyPiece_MatchesFullScan(t *testing.T) {
	t.Parallel()

	meta := mixedLayoutMeta(64)
	covered := meta.calculatePiecesCovered()
	for p := range int(meta.numPieces()) {
		if got, want := meta.classifyPiece(p), classifyPieceScan(&meta, p); got != want {
			t.Fatalf("classifyPiece(%d) = %d, want %d", p, got, want)
		}
		if got, want := covered[p], coveredScan(&meta, p); got != want {
			t.Fatalf("calculatePiecesCovered()[%d] = %v, want %v", p, got, want)
		}
	}
}

func BenchmarkClassifyPiece(b *testing.B) {
	meta := mixedLayoutMeta(400)
	numPieces := int(meta.numPieces())

	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		if meta.classifyPiece(i%numPieces) == pieceNoSelectedOverlap {
			continue
		}
	}
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

// TestWritePieceData_UnselectedFileStillConsumesItsShare pins that a file which
// takes no data does not shift the bytes behind it. The walk apportions every
// overlapping file's range and writeAt drops the ones that decline, so a
// declining file in the MIDDLE of a piece is the case where treating "skipped"
// as "contributed nothing" would silently write the wrong bytes to the next
// file. The existing coverage only ever declines the last file of a piece.
func TestWritePieceData_UnselectedFileStillConsumesItsShare(t *testing.T) {
	t.Parallel()
	_, tmpDir := newTestDestServer(t)

	head := filepath.Join(tmpDir, "head.bin.partial")
	tail := filepath.Join(tmpDir, "tail.bin.partial")
	state := &serverTorrentState{
		torrentMeta: torrentMeta{
			pieceLength: 150,
			totalSize:   150,
			files: []*serverFileInfo{
				{path: head, size: 50, offset: 0, selected: true},
				{path: filepath.Join(tmpDir, "middle.bin"), size: 50, offset: 50, selected: false},
				{path: tail, size: 50, offset: 100, selected: true},
			},
		},
	}
	for _, path := range []string{head, tail} {
		if err := os.WriteFile(path, make([]byte, 50), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	data := make([]byte, 150)
	for i := range data {
		data[i] = byte(i)
	}
	if err := state.writePieceData(0, data); err != nil {
		t.Fatalf("writePieceData: %v", err)
	}

	for _, tc := range []struct {
		path string
		want []byte
	}{
		{head, data[0:50]},
		{tail, data[100:150]},
	} {
		content, err := os.ReadFile(tc.path)
		if err != nil {
			t.Fatalf("reading %s: %v", tc.path, err)
		}
		if !bytes.Equal(content, tc.want) {
			t.Errorf("%s = %v, want %v", filepath.Base(tc.path), content[:8], tc.want[:8])
		}
	}
}

// TestWritePieceData_LeavesHardlinkedFilesUntouched pins that a file whose data
// arrived by hardlink takes no piece data. The hardlink shares its inode with
// another torrent's file, so a write lands in that torrent's data too - and the
// source streams these pieces whenever a boundary piece also covers a file this
// torrent really is writing.
func TestWritePieceData_LeavesHardlinkedFilesUntouched(t *testing.T) {
	t.Parallel()
	_, tmpDir := newTestDestServer(t)

	original := []byte("data owned by the source torrent")
	streamedPath := filepath.Join(tmpDir, "streamed.bin.partial")
	if err := os.WriteFile(streamedPath, make([]byte, len(original)), 0o644); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name    string
		hlState hardlinkState
	}{
		{"complete", hlStateComplete},
		{"pending", hlStatePending},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			linkedPath := filepath.Join(tmpDir, tc.name+".bin")
			if err := os.WriteFile(linkedPath, original, 0o644); err != nil {
				t.Fatal(err)
			}

			size := int64(len(original))
			state := &serverTorrentState{
				torrentMeta: torrentMeta{
					pieceLength: 2 * size,
					totalSize:   2 * size,
					files: []*serverFileInfo{
						{path: streamedPath, size: size, offset: 0, selected: true},
						{
							path: linkedPath, size: size, offset: size, selected: true,
							hardlink: hardlinkInfo{state: tc.hlState},
						},
					},
				},
			}

			data := bytes.Repeat([]byte{0xAA}, int(2*size))
			if err := state.writePieceData(0, data); err != nil {
				t.Fatalf("writePieceData: %v", err)
			}

			got, err := os.ReadFile(linkedPath)
			if err != nil {
				t.Fatalf("reading hardlinked file: %v", err)
			}
			if !bytes.Equal(got, original) {
				t.Errorf("hardlinked file = %q, want it untouched (%q)", got, original)
			}
		})
	}
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

// TestSetupFiles_ResultsAlignWithRequestOrder pins the invariant the parallel
// fan-out in setupFiles could break: files[i] and results[i] must describe
// reqFiles[i]. Every file gets a distinct on-disk shape (pre-existing, resumed
// .partial, fresh, unselected) so a mis-indexed slot is visible rather than
// hidden behind identical outcomes. Run under -race, the shared parent
// directory also exercises concurrent MkdirAll of the same path.
func TestSetupFiles_ResultsAlignWithRequestOrder(t *testing.T) {
	t.Parallel()
	s, tmpDir := newTestDestServer(t)

	const numFiles = 64
	reqFiles := make([]*pb.FileInfo, numFiles)
	for i := range numFiles {
		rel := filepath.Join("pack", fmt.Sprintf("f%02d.bin", i))
		size := int64(100 + i)
		reqFiles[i] = &pb.FileInfo{
			Path:     rel,
			Size:     size,
			Offset:   int64(i) * 1000,
			Selected: i%4 != 3,
		}
		abs := filepath.Join(tmpDir, rel)
		switch i % 4 {
		case 0: // pre-existing at the final path
			writeTestFile(t, abs, make([]byte, size))
		case 1: // resumed .partial from an interrupted sync
			writeTestFile(t, abs+partialSuffix, make([]byte, size))
		}
	}

	files, results, err := s.setupFiles(context.Background(), "hash1", reqFiles, "")
	if err != nil {
		t.Fatalf("setupFiles: %v", err)
	}
	if len(files) != numFiles || len(results) != numFiles {
		t.Fatalf("got %d files / %d results, want %d each", len(files), len(results), numFiles)
	}

	for i, f := range reqFiles {
		fi := files[i]
		if fi == nil {
			t.Fatalf("file %d: nil entry", i)
		}
		if fi.size != f.GetSize() || fi.offset != f.GetOffset() || fi.selected != f.GetSelected() {
			t.Errorf("file %d: got size=%d offset=%d selected=%v, want %d/%d/%v",
				i, fi.size, fi.offset, fi.selected, f.GetSize(), f.GetOffset(), f.GetSelected())
		}
		if results[i].GetFileIndex() != int32(i) {
			t.Errorf("file %d: result carries fileIndex %d", i, results[i].GetFileIndex())
		}

		final := filepath.Join(tmpDir, f.GetPath())
		wantPath, wantPreExisting := final+partialSuffix, false
		switch i % 4 {
		case 0:
			wantPath, wantPreExisting = final, true
		case 3: // unselected: final path, no .partial
			wantPath = final
		}
		if fi.path != wantPath {
			t.Errorf("file %d: path = %q, want %q", i, fi.path, wantPath)
		}
		if results[i].GetPreExisting() != wantPreExisting {
			t.Errorf("file %d: preExisting = %v, want %v", i, results[i].GetPreExisting(), wantPreExisting)
		}
	}
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

	// FinalizeTorrent defers with BUSY while an early finalization is still
	// reading a completed file back, so let those land first.
	waitEarlyFinalize(t, state)

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
