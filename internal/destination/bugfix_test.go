package destination

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bits-and-blooms/bitset"
)

// ---------- Bug A: Inode reuse — size guard on hardlink ----------

func TestTryHardlinkFromRegistered_SizeMismatchEvicts(t *testing.T) {
	t.Parallel()
	s, tmpDir := newTestDestServer(t)
	ctx := context.Background()

	// Create a "registered" source file at 100 bytes.
	sourceDir := filepath.Join(tmpDir, "source")
	if err := os.MkdirAll(sourceDir, 0o755); err != nil {
		t.Fatal(err)
	}
	sourceFile := filepath.Join(sourceDir, "old.mkv")
	if err := os.WriteFile(sourceFile, make([]byte, 100), 0o644); err != nil {
		t.Fatal(err)
	}

	relSource, _ := filepath.Rel(tmpDir, sourceFile)
	inode := Inode(9999)
	s.store.Inodes().Register(inode, relSource)

	targetPath := filepath.Join(tmpDir, "target", "new.mkv")
	if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
		t.Fatal(err)
	}

	t.Run("matching size creates hardlink", func(t *testing.T) {
		t.Parallel()
		s2, tmpDir2 := newTestDestServer(t)

		srcDir2 := filepath.Join(tmpDir2, "src")
		_ = os.MkdirAll(srcDir2, 0o755)
		srcFile2 := filepath.Join(srcDir2, "file.bin")
		_ = os.WriteFile(srcFile2, make([]byte, 200), 0o644)

		relSrc2, _ := filepath.Rel(tmpDir2, srcFile2)
		s2.store.Inodes().Register(Inode(1234), relSrc2)

		tgt2 := filepath.Join(tmpDir2, "tgt", "file.bin")
		_ = os.MkdirAll(filepath.Dir(tgt2), 0o755)

		outcome, ok := s2.tryHardlinkFromRegistered(ctx, "h1", "file.bin", tgt2, Inode(1234), 200)
		if !ok {
			t.Fatal("expected ok=true for matching size")
		}
		if outcome.state != hlStateComplete {
			t.Fatalf("expected hlStateComplete, got %d", outcome.state)
		}
		// Target file should exist as hardlink
		if _, err := os.Stat(tgt2); err != nil {
			t.Fatalf("hardlink target should exist: %v", err)
		}
	})

	t.Run("mismatched size evicts and returns not-found", func(t *testing.T) {
		// Source file is 100 bytes, but expected size is 500.
		outcome, ok := s.tryHardlinkFromRegistered(ctx, "hash1", "new.mkv", targetPath, inode, 500)
		if ok {
			t.Fatalf("expected ok=false for size mismatch, got outcome=%+v", outcome)
		}

		// Inode should be evicted from registry.
		if _, found := s.store.Inodes().GetRegistered(inode); found {
			t.Fatal("stale inode should have been evicted")
		}
	})

	t.Run("missing source file evicts and returns not-found", func(t *testing.T) {
		missingInode := Inode(8888)
		s.store.Inodes().Register(missingInode, "nonexistent/file.mkv")

		outcome, ok := s.tryHardlinkFromRegistered(ctx, "hash2", "f.mkv", targetPath, missingInode, 100)
		if ok {
			t.Fatalf("expected ok=false for missing source, got outcome=%+v", outcome)
		}
		if _, found := s.store.Inodes().GetRegistered(missingInode); found {
			t.Fatal("stale inode should have been evicted")
		}
	})
}

func TestInodeRegistry_Evict(t *testing.T) {
	t.Parallel()
	logger := testLogger(t)
	r := NewInodeRegistry(t.TempDir(), logger)

	r.Register(Inode(1), "path/a")
	r.Register(Inode(2), "path/b")

	r.Evict(Inode(1))

	if _, found := r.GetRegistered(Inode(1)); found {
		t.Fatal("inode 1 should be evicted")
	}
	if _, found := r.GetRegistered(Inode(2)); !found {
		t.Fatal("inode 2 should still exist")
	}
	if r.Len() != 1 {
		t.Fatalf("expected 1 registered inode, got %d", r.Len())
	}
}

// ---------- Bug B: RegisterInProgress return value ----------

func TestResolveHardlink_RaceReturnsPending(t *testing.T) {
	t.Parallel()
	s, tmpDir := newTestDestServer(t)
	ctx := context.Background()

	inode := Inode(5555)
	targetA := filepath.Join(tmpDir, "torrentA", "file.mkv")
	targetB := filepath.Join(tmpDir, "torrentB", "file.mkv")
	_ = os.MkdirAll(filepath.Dir(targetA), 0o755)
	_ = os.MkdirAll(filepath.Dir(targetB), 0o755)

	// Torrent A registers first.
	outcomeA := s.resolveHardlink(ctx, "hashA", "file.mkv", targetA, inode, 100)
	if outcomeA.state != hlStateInProgress {
		t.Fatalf("torrent A should be in-progress, got %d", outcomeA.state)
	}

	// Torrent B tries to register same inode — should become pending, not in-progress.
	outcomeB := s.resolveHardlink(ctx, "hashB", "file.mkv", targetB, inode, 100)
	if outcomeB.state != hlStatePending {
		t.Fatalf("torrent B should be pending (lost race), got %d", outcomeB.state)
	}
	if outcomeB.doneCh == nil {
		t.Fatal("pending outcome should have a doneCh")
	}
}

func TestRegisterInodeInProgress_ReturnsFalseOnRace(t *testing.T) {
	t.Parallel()
	s, tmpDir := newTestDestServer(t)
	ctx := context.Background()

	inode := Inode(7777)
	target := filepath.Join(tmpDir, "t", "f.mkv")

	first := s.registerInodeInProgress(ctx, "h1", "f.mkv", target, inode)
	if !first {
		t.Fatal("first registration should succeed")
	}

	second := s.registerInodeInProgress(ctx, "h2", "f.mkv", target, inode)
	if second {
		t.Fatal("second registration should fail (race lost)")
	}
}

// ---------- Bug C: Abort in-progress inodes on verification failure ----------

// TestRecoverVerificationFailure_ClearsInProgressPieces verifies that
// recoverVerificationFailure clears failed pieces from the written bitmap.
// The companion AbortInProgress call (for unblocking pending torrents) happens
// in runBackgroundFinalization and is tested via TestAbortInProgress_ClosesDoneCh.
func TestRecoverVerificationFailure_ClearsInProgressPieces(t *testing.T) {
	t.Parallel()
	s, tmpDir := newTestDestServer(t)
	ctx := context.Background()

	inode := Inode(3333)
	s.store.Inodes().RegisterInProgress(inode, "hashX", "sub/file.mkv")

	filePath := filepath.Join(tmpDir, "sub", "file.mkv")
	_ = os.MkdirAll(filepath.Dir(filePath), 0o755)
	_ = os.WriteFile(filePath, make([]byte, 200), 0o644)

	state := &serverTorrentState{
		torrentMeta: torrentMeta{
			pieceLength: 100,
			totalSize:   200,
			files: []*serverFileInfo{
				{
					path:     filePath,
					offset:   0,
					size:     200,
					selected: true,
					hardlink: hardlinkInfo{
						sourceInode: inode,
						state:       hlStateInProgress,
					},
					firstPiece:  0,
					lastPiece:   1,
					piecesTotal: 2,
				},
			},
		},
		written:   bitset.New(2).Set(0).Set(1),
		statePath: filepath.Join(tmpDir, ".state"),
	}

	s.recoverVerificationFailure(ctx, "hashX", state, []int{0})

	// Failed piece 0 should be cleared from the bitmap.
	if state.written.Test(0) {
		t.Fatal("piece 0 should be cleared from written bitmap after recovery")
	}
	// Piece 1 was not failed, so it should remain.
	if !state.written.Test(1) {
		t.Fatal("piece 1 should still be set in written bitmap")
	}
}

func TestAbortInProgress_ClosesDoneCh(t *testing.T) {
	t.Parallel()
	logger := testLogger(t)
	r := NewInodeRegistry(t.TempDir(), logger)
	ctx := context.Background()

	inode := Inode(4444)
	r.RegisterInProgress(inode, "torrentA", "sub/file.mkv")

	// Get the doneCh before abort.
	_, doneCh, _, found := r.GetInProgress(inode)
	if !found {
		t.Fatal("should be in-progress")
	}

	r.AbortInProgress(ctx, inode, "torrentA")

	// doneCh should be closed.
	select {
	case <-doneCh:
		// OK — channel was closed.
	default:
		t.Fatal("doneCh should be closed after AbortInProgress")
	}

	// Entry should be removed.
	if _, _, _, stillFound := r.GetInProgress(inode); stillFound {
		t.Fatal("in-progress entry should be removed after abort")
	}
}

// ---------- Bug D: renamePartialFile updates fi.path ----------

func TestFinalizeFiles_UpdatesPathAfterRename(t *testing.T) {
	t.Parallel()
	s, tmpDir := newTestDestServer(t)
	ctx := context.Background()

	// Create a .partial file.
	dir := filepath.Join(tmpDir, "torrent")
	_ = os.MkdirAll(dir, 0o755)
	partialPath := filepath.Join(dir, "file.mkv.partial")
	_ = os.WriteFile(partialPath, make([]byte, 100), 0o644)

	state := &serverTorrentState{
		torrentMeta: torrentMeta{
			pieceLength: 100,
			totalSize:   100,
			files: []*serverFileInfo{
				{
					path:     partialPath,
					offset:   0,
					size:     100,
					selected: true,
				},
			},
		},
		written:   bitset.New(1).Set(0),
		statePath: filepath.Join(tmpDir, ".state"),
	}

	if err := s.finalizeFiles(ctx, "testHash", state); err != nil {
		t.Fatalf("finalizeFiles: %v", err)
	}

	fi := state.files[0]
	expectedPath := filepath.Join(dir, "file.mkv")
	if fi.path != expectedPath {
		t.Fatalf("fi.path should be updated to %q, got %q", expectedPath, fi.path)
	}
	if strings.HasSuffix(fi.path, partialSuffix) {
		t.Fatal("fi.path should not have .partial suffix after finalization")
	}
}

// ---------- Bug E: updateStateAfterRelocate returns error ----------

func TestUpdateStateAfterRelocate_UpdatesPaths(t *testing.T) {
	t.Parallel()

	basePath := "/base"
	state := &serverTorrentState{
		torrentMeta: torrentMeta{
			files: []*serverFileInfo{
				{path: "/base/old/torrent/file1.mkv.partial"},
				{path: "/base/old/torrent/file2.mkv"},
			},
		},
		saveSubPath: "old",
	}

	err := updateStateAfterRelocate(state, basePath, "old", "new")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := []string{
		"/base/new/torrent/file1.mkv.partial",
		"/base/new/torrent/file2.mkv",
	}
	for i, fi := range state.files {
		if fi.path != expected[i] {
			t.Errorf("file %d: path = %q, want %q", i, fi.path, expected[i])
		}
	}
	if state.saveSubPath != "new" {
		t.Errorf("saveSubPath = %q, want %q", state.saveSubPath, "new")
	}
}

// ---------- Bug F: recoverAffectedFile breaks hardlinks ----------

func TestRecoverAffectedFile_BreaksHardlink(t *testing.T) {
	t.Parallel()
	s, tmpDir := newTestDestServer(t)
	ctx := context.Background()

	// Create a hardlinked file on disk.
	dir := filepath.Join(tmpDir, "torrent")
	_ = os.MkdirAll(dir, 0o755)
	filePath := filepath.Join(dir, "file.mkv")
	_ = os.WriteFile(filePath, make([]byte, 200), 0o644)

	written := bitset.New(2).Set(0).Set(1)

	fi := &serverFileInfo{
		path:     filePath,
		offset:   0,
		size:     200,
		selected: true,
		hardlink: hardlinkInfo{
			state:       hlStateComplete,
			sourceInode: 12345,
		},
		firstPiece:    0,
		lastPiece:     1,
		piecesTotal:   2,
		piecesWritten: 2,
	}

	state := &serverTorrentState{
		torrentMeta: torrentMeta{
			pieceLength: 100,
			totalSize:   200,
			files:       []*serverFileInfo{fi},
		},
		written: written,
	}

	// Simulate piece 0 failing verification.
	s.recoverAffectedFile(ctx, "hashTest", state, fi, []int{0})

	// Hardlink state should be reset.
	if fi.hardlink.state != hlStateNone {
		t.Fatalf("hardlink state should be hlStateNone, got %d", fi.hardlink.state)
	}

	// Path should end with .partial for re-streaming.
	if !strings.HasSuffix(fi.path, partialSuffix) {
		t.Fatalf("path should end with %s, got %q", partialSuffix, fi.path)
	}

	// earlyFinalized should be cleared.
	if fi.earlyFinalized {
		t.Fatal("earlyFinalized should be false")
	}

	// Original file should be removed from disk.
	if _, err := os.Stat(filePath); !os.IsNotExist(err) {
		t.Fatal("hardlinked file should be deleted from disk")
	}
}

func TestRecoverAffectedFile_NormalFile(t *testing.T) {
	t.Parallel()
	s, tmpDir := newTestDestServer(t)
	ctx := context.Background()

	// Create a finalized (non-hardlinked) file.
	dir := filepath.Join(tmpDir, "torrent")
	_ = os.MkdirAll(dir, 0o755)
	filePath := filepath.Join(dir, "file.mkv")
	_ = os.WriteFile(filePath, make([]byte, 200), 0o644)

	written := bitset.New(2).Set(0).Set(1)

	fi := &serverFileInfo{
		path:          filePath,
		offset:        0,
		size:          200,
		selected:      true,
		firstPiece:    0,
		lastPiece:     1,
		piecesTotal:   2,
		piecesWritten: 2,
	}

	state := &serverTorrentState{
		torrentMeta: torrentMeta{
			pieceLength: 100,
			totalSize:   200,
			files:       []*serverFileInfo{fi},
		},
		written: written,
	}

	s.recoverAffectedFile(ctx, "hashTest", state, fi, []int{0})

	// File should be renamed to .partial.
	expectedPartial := filePath + partialSuffix
	if fi.path != expectedPartial {
		t.Fatalf("path should be %q, got %q", expectedPartial, fi.path)
	}

	// File should exist at .partial path.
	if _, err := os.Stat(expectedPartial); err != nil {
		t.Fatalf("partial file should exist: %v", err)
	}
}

func TestRecoverAffectedFile_SkipsUnaffected(t *testing.T) {
	t.Parallel()
	s, _ := newTestDestServer(t)
	ctx := context.Background()

	fi := &serverFileInfo{
		path:          "/some/path/file.mkv",
		offset:        0,
		size:          100,
		selected:      true,
		firstPiece:    0,
		lastPiece:     0,
		piecesTotal:   1,
		piecesWritten: 1,
	}

	state := &serverTorrentState{
		torrentMeta: torrentMeta{
			pieceLength: 100,
			totalSize:   100,
			files:       []*serverFileInfo{fi},
		},
		written: bitset.New(1).Set(0),
	}

	// Piece 5 doesn't overlap this file (firstPiece=0, lastPiece=0).
	s.recoverAffectedFile(ctx, "h", state, fi, []int{5})

	// Path should be unchanged.
	if fi.path != "/some/path/file.mkv" {
		t.Fatalf("path should be unchanged, got %q", fi.path)
	}
}

// ---------- Bug B: Race test with concurrent goroutines ----------

func TestResolveHardlink_ConcurrentRace(t *testing.T) {
	t.Parallel()
	s, tmpDir := newTestDestServer(t)
	ctx := context.Background()

	inode := Inode(6666)
	const numGoroutines = 10

	var wg sync.WaitGroup
	var inProgressCount, pendingCount int
	var mu sync.Mutex

	for i := range numGoroutines {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			target := filepath.Join(tmpDir, "t", fmt.Sprintf("f_%d.mkv", idx))
			_ = os.MkdirAll(filepath.Dir(target), 0o755)

			outcome := s.resolveHardlink(ctx, fmt.Sprintf("hash%d", idx), "f.mkv", target, inode, 100)

			mu.Lock()
			defer mu.Unlock()
			switch outcome.state {
			case hlStateInProgress:
				inProgressCount++
			case hlStatePending:
				pendingCount++
			case hlStateNone, hlStateComplete:
				// Unexpected in this test — no assertion needed.
			}
		}(i)
	}
	wg.Wait()

	// Exactly one goroutine should win the race (in-progress).
	// All others should be pending.
	if inProgressCount != 1 {
		t.Fatalf("expected exactly 1 in-progress, got %d", inProgressCount)
	}
	if pendingCount != numGoroutines-1 {
		t.Fatalf("expected %d pending, got %d", numGoroutines-1, pendingCount)
	}
}

// ---------- Bug 3: Path collision guard ----------

func TestInitNewTorrent_PathCollisionDetected(t *testing.T) {
	t.Parallel()
	s, tmpDir := newTestDestServer(t)

	// Register a path as owned by torrent A.
	targetPath := filepath.Join(tmpDir, "movies", "film.mkv")
	s.store.mu.Lock()
	s.store.filePaths[targetPath] = "hashA"
	s.store.mu.Unlock()

	files := []*serverFileInfo{
		{path: targetPath + partialSuffix, size: 100, selected: true},
	}

	s.store.mu.Lock()
	collisionErr := checkPathCollisions(s.store.filePaths, "hashB", files)
	s.store.mu.Unlock()

	if collisionErr == nil {
		t.Fatal("expected collision error for path owned by another torrent")
	}
	if !strings.Contains(collisionErr.Error(), "hashA") {
		t.Fatalf("error should mention owning hash, got: %v", collisionErr)
	}
}

func TestInitNewTorrent_SameTorrentNoCollision(t *testing.T) {
	t.Parallel()
	s, tmpDir := newTestDestServer(t)

	targetPath := filepath.Join(tmpDir, "movies", "film.mkv")
	s.store.mu.Lock()
	s.store.filePaths[targetPath] = "hashA"
	s.store.mu.Unlock()

	files := []*serverFileInfo{
		{path: targetPath + partialSuffix, size: 100, selected: true},
	}

	// Same hash re-initializing should not collide with itself.
	s.store.mu.Lock()
	collisionErr := checkPathCollisions(s.store.filePaths, "hashA", files)
	s.store.mu.Unlock()

	if collisionErr != nil {
		t.Fatalf("same torrent should not collide with itself: %v", collisionErr)
	}
}

func TestFilePathRegistration_Lifecycle(t *testing.T) {
	t.Parallel()
	s, tmpDir := newTestDestServer(t)

	files := []*serverFileInfo{
		{path: filepath.Join(tmpDir, "a.mkv") + partialSuffix, size: 100, selected: true},
		{path: filepath.Join(tmpDir, "b.mkv"), size: 200, selected: true},
		{path: filepath.Join(tmpDir, "c.mkv"), size: 300, selected: false}, // unselected -- not tracked
	}

	s.store.mu.Lock()
	registerFilePaths(s.store.filePaths, "hash1", files)
	s.store.mu.Unlock()

	// Selected files should be registered.
	s.store.mu.RLock()
	if s.store.filePaths[filepath.Join(tmpDir, "a.mkv")] != "hash1" {
		t.Fatal("a.mkv should be registered")
	}
	if s.store.filePaths[filepath.Join(tmpDir, "b.mkv")] != "hash1" {
		t.Fatal("b.mkv should be registered")
	}
	if _, exists := s.store.filePaths[filepath.Join(tmpDir, "c.mkv")]; exists {
		t.Fatal("unselected c.mkv should not be registered")
	}
	s.store.mu.RUnlock()

	// Unregister and verify cleanup.
	s.store.mu.Lock()
	unregisterFilePaths(s.store.filePaths, "hash1", files)
	s.store.mu.Unlock()

	s.store.mu.RLock()
	if len(s.store.filePaths) != 0 {
		t.Fatalf("expected empty filePaths after unregister, got %d", len(s.store.filePaths))
	}
	s.store.mu.RUnlock()
}

// ---------- Bug 4: writeAt holds RLock during write ----------

func TestWriteAt_ProtectedByFileMu(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	filePath := filepath.Join(tmpDir, "test.bin")
	fi := &serverFileInfo{
		path: filePath,
		size: 100,
	}

	// Write some data.
	data := []byte("hello world")
	if err := fi.writeAt(data, 0); err != nil {
		t.Fatalf("writeAt: %v", err)
	}

	// Verify the data was written.
	content, readErr := os.ReadFile(filePath)
	if readErr != nil {
		t.Fatalf("ReadFile: %v", readErr)
	}
	if string(content[:len(data)]) != "hello world" {
		t.Fatalf("expected 'hello world', got %q", content[:len(data)])
	}

	// Close the file via fileMu.Lock (simulating closeFileHandle).
	fi.fileMu.Lock()
	if fi.file != nil {
		_ = fi.file.Close()
		fi.file = nil
	}
	fi.fileMu.Unlock()

	// writeAt after close should return an error (file re-opened).
	// The file exists on disk so openForWrite succeeds, then writeAt writes.
	if err := fi.writeAt([]byte("test"), 0); err != nil {
		t.Fatalf("writeAt after reopen should succeed: %v", err)
	}
}

func TestCloseFileHandle_WaitsForInFlightWrites(t *testing.T) {
	t.Parallel()
	s, tmpDir := newTestDestServer(t)
	ctx := context.Background()

	filePath := filepath.Join(tmpDir, "test.bin")
	fi := &serverFileInfo{
		path: filePath,
		size: 1024,
	}

	// Open the file.
	if err := fi.openForWrite(); err != nil {
		t.Fatalf("openForWrite: %v", err)
	}

	// Simulate an in-flight write by holding RLock.
	fi.fileMu.RLock()

	closed := make(chan error, 1)
	go func() {
		closed <- s.closeFileHandle(ctx, "hash", fi)
	}()

	// closeFileHandle should block because we hold RLock.
	// Use a short timer — if it returns, the lock isn't working.
	select {
	case <-closed:
		t.Fatal("closeFileHandle should block while RLock is held")
	case <-time.After(50 * time.Millisecond):
		// Expected: still blocked.
	}

	// Release the read lock — closeFileHandle should proceed.
	fi.fileMu.RUnlock()

	if err := <-closed; err != nil {
		t.Fatalf("closeFileHandle: %v", err)
	}

	if fi.file != nil {
		t.Fatal("file handle should be nil after close")
	}
}

// ---------- Bug 5: save_sub_path empty transition ----------

func TestFinalizeTorrent_RelocatesWhenSubPathBecomesEmpty(t *testing.T) {
	t.Parallel()
	s, tmpDir := newTestDestServer(t)
	ctx := context.Background()

	// Set up a torrent with saveSubPath = "movies".
	oldDir := filepath.Join(tmpDir, "movies", "torrent")
	_ = os.MkdirAll(oldDir, 0o755)
	partialPath := filepath.Join(oldDir, "file.mkv"+partialSuffix)
	_ = os.WriteFile(partialPath, make([]byte, 100), 0o644)

	hash := "relocateTest"
	state := &serverTorrentState{
		torrentMeta: torrentMeta{
			pieceLength: 100,
			totalSize:   100,
			files: []*serverFileInfo{
				{path: partialPath, offset: 0, size: 100, selected: true},
			},
		},
		written:     bitset.New(1).Set(0),
		saveSubPath: "movies",
	}
	s.store.mu.Lock()
	s.store.entries[hash] = state
	s.store.mu.Unlock()

	// Relocate from "movies" to "" (root) with explicit flag.
	relocErr := s.relocateForSubPathChange(ctx, hash, state, "")
	if relocErr != nil {
		t.Fatalf("relocateForSubPathChange: %v", relocErr)
	}

	// File should be at root now.
	state.mu.Lock()
	newPath := state.files[0].path
	newSubPath := state.saveSubPath
	state.mu.Unlock()

	if newSubPath != "" {
		t.Fatalf("saveSubPath should be empty, got %q", newSubPath)
	}

	expectedPath := filepath.Join(tmpDir, "torrent", "file.mkv"+partialSuffix)
	if newPath != expectedPath {
		t.Fatalf("file path should be %q, got %q", expectedPath, newPath)
	}
}
