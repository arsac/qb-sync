package destination

import (
	"os"
	"path/filepath"
	"testing"
)

// TestWritePieceData_BinarySearchBoundaries pins the [sort.Search] file-skip
// optimization in writePieceData. A wrong predicate (e.g. >= instead of >)
// would silently skip the first overlapping file and corrupt data placement —
// these cases cross every boundary the search must get right.
func TestWritePieceData_BinarySearchBoundaries(t *testing.T) {
	t.Parallel()

	// Layout: three files, with a zero-length file wedged between the first
	// two. Offsets: f0 [0,100), zero [100,100), f1 [100,250), f2 [250,300).
	newState := func(t *testing.T) *serverTorrentState {
		t.Helper()
		tmpDir := t.TempDir()
		state := &serverTorrentState{
			torrentMeta: torrentMeta{
				pieceLength: 100,
				totalSize:   300,
				files: []*serverFileInfo{
					{path: filepath.Join(tmpDir, "f0.partial"), size: 100, offset: 0, selected: true},
					{path: filepath.Join(tmpDir, "zero.partial"), size: 0, offset: 100, selected: true},
					{path: filepath.Join(tmpDir, "f1.partial"), size: 150, offset: 100, selected: true},
					{path: filepath.Join(tmpDir, "f2.partial"), size: 50, offset: 250, selected: true},
				},
			},
		}
		for _, fi := range state.files {
			if fi.size == 0 {
				continue
			}
			if err := os.WriteFile(fi.path, make([]byte, fi.size), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		return state
	}

	pattern := func(n int, seed byte) []byte {
		data := make([]byte, n)
		for i := range data {
			data[i] = seed + byte(i%200)
		}
		return data
	}

	assertRange := func(t *testing.T, path string, fileOff, n int, want []byte) {
		t.Helper()
		content, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reading %s: %v", path, err)
		}
		for i := range n {
			if content[fileOff+i] != want[i] {
				t.Fatalf("%s byte %d = %d, want %d", filepath.Base(path), fileOff+i, content[fileOff+i], want[i])
			}
		}
	}

	t.Run("piece at offset zero lands in the first file", func(t *testing.T) {
		t.Parallel()
		state := newState(t)

		data := pattern(100, 1)
		if err := state.writePieceData(0, data); err != nil {
			t.Fatalf("writePieceData: %v", err)
		}
		assertRange(t, state.files[0].path, 0, 100, data)
	})

	t.Run("piece starting mid-file spans into the next file", func(t *testing.T) {
		t.Parallel()
		state := newState(t)

		// Offset 50: second half of f0 + first half of f1 (past the
		// zero-length file, which the search must step over).
		data := pattern(100, 7)
		if err := state.writePieceData(50, data); err != nil {
			t.Fatalf("writePieceData: %v", err)
		}
		assertRange(t, state.files[0].path, 50, 50, data[:50])
		assertRange(t, state.files[2].path, 0, 50, data[50:])
	})

	t.Run("piece exactly at a file boundary skips the zero-length file", func(t *testing.T) {
		t.Parallel()
		state := newState(t)

		// Offset 100 == end of f0 == offset of both zero and f1. The search
		// must select f1 (zero-length file can hold no data).
		data := pattern(100, 13)
		if err := state.writePieceData(100, data); err != nil {
			t.Fatalf("writePieceData: %v", err)
		}
		assertRange(t, state.files[2].path, 0, 100, data)
	})

	t.Run("last piece lands in the last file", func(t *testing.T) {
		t.Parallel()
		state := newState(t)

		// Offset 200: second half of f1 + all of f2.
		data := pattern(100, 23)
		if err := state.writePieceData(200, data); err != nil {
			t.Fatalf("writePieceData: %v", err)
		}
		assertRange(t, state.files[2].path, 100, 50, data[:50])
		assertRange(t, state.files[3].path, 0, 50, data[50:])
	})
}
