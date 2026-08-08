package destination

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestWriteAt_OpenFileWritesWithoutExclusiveLock pins that a write to a file
// that already has a handle never reaches for fileMu's exclusive lock. It is
// the property the stream workers' concurrency rests on: a pending Lock blocks
// new RLock acquisitions, so one write taking it forces every other worker to
// wait out the in-flight NFS round-trip before starting its own.
//
// A held RLock stands in for that in-flight write. The exclusive-lock shape
// blocks on it forever; the read-lock shape completes straight through.
func TestWriteAt_OpenFileWritesWithoutExclusiveLock(t *testing.T) {
	t.Parallel()

	fi := &serverFileInfo{
		path:     filepath.Join(t.TempDir(), "f.partial"),
		size:     64,
		selected: true,
	}
	if err := fi.openForWrite(); err != nil {
		t.Fatalf("openForWrite: %v", err)
	}

	fi.fileMu.RLock()
	defer fi.fileMu.RUnlock()

	done := make(chan error, 1)
	go func() { done <- fi.writeAt([]byte("concurrent"), 0) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("writeAt: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("writeAt blocked while another write held fileMu.RLock: " +
			"it is taking the exclusive lock, which serializes the stream workers")
	}

	content, err := os.ReadFile(fi.path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(content[:len("concurrent")]) != "concurrent" {
		t.Fatalf("file holds %q, want the concurrent write", content[:len("concurrent")])
	}
}

// TestWriteAt_ConcurrentFirstWritesAllLand covers the other side of the split:
// several workers racing on a file nobody has opened yet must all end up
// writing, with exactly one of them creating and pre-allocating the handle.
func TestWriteAt_ConcurrentFirstWritesAllLand(t *testing.T) {
	t.Parallel()

	const workers = 8
	fi := &serverFileInfo{
		path:     filepath.Join(t.TempDir(), "f.partial"),
		size:     workers,
		selected: true,
	}

	var wg sync.WaitGroup
	errs := make([]error, workers)
	start := make(chan struct{})
	for i := range workers {
		wg.Go(func() {
			<-start
			errs[i] = fi.writeAt([]byte{byte('a' + i)}, int64(i))
		})
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("worker %d: %v", i, err)
		}
	}

	content, err := os.ReadFile(fi.path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if len(content) != workers {
		t.Fatalf("file is %d bytes, want %d (pre-allocation lost)", len(content), workers)
	}
	for i := range workers {
		if content[i] != byte('a'+i) {
			t.Fatalf("byte %d = %q, want %q", i, content[i], byte('a'+i))
		}
	}
}

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
