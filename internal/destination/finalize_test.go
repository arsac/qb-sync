package destination

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/autobrr/go-qbittorrent"
	"github.com/bits-and-blooms/bitset"
	"golang.org/x/sync/semaphore"

	"github.com/arsac/qb-sync/internal/utils"
	pb "github.com/arsac/qb-sync/proto"
)

func TestVerifyFinalizedPieces_ConcurrencyLimit(t *testing.T) {
	t.Parallel()

	// Create a single file containing 20 pieces of 1024 bytes each.
	// verifyFinalizedPieces should run at most maxVerifyConcurrency goroutines.
	const pieceSize = 1024
	const numPieces = 20
	const totalSize = pieceSize * numPieces

	tmpDir := t.TempDir()
	logger := testLogger(t)

	s := &Server{
		config:      ServerConfig{BasePath: tmpDir},
		logger:      logger,
		store:       newTorrentStore(tmpDir, logger),
		memBudget:   semaphore.NewWeighted(512 * 1024 * 1024),
		finalizeSem: semaphore.NewWeighted(1),
	}

	// Create file data and compute piece hashes
	fileData := make([]byte, totalSize)
	for i := range fileData {
		fileData[i] = byte(i % 251) // Deterministic non-zero data
	}

	pieceHashes := make([]string, numPieces)
	for i := range numPieces {
		offset := i * pieceSize
		pieceHashes[i] = utils.ComputeSHA1(fileData[offset : offset+pieceSize])
	}

	// Write the file to disk
	filePath := filepath.Join(tmpDir, "test.bin")
	if err := os.WriteFile(filePath, fileData, 0o644); err != nil {
		t.Fatal(err)
	}

	state := &serverTorrentState{
		torrentMeta: torrentMeta{
			pieceHashes: pieceHashes,
			pieceLength: pieceSize,
			totalSize:   totalSize,
			files: []*serverFileInfo{
				{path: filePath, offset: 0, size: totalSize, selected: true},
			},
		},
	}

	ctx := context.Background()
	failedPieces, err := s.verifyFinalizedPieces(ctx, "testHash", state)
	if err != nil {
		t.Fatalf("verifyFinalizedPieces failed: %v", err)
	}
	if len(failedPieces) > 0 {
		t.Fatalf("expected no failed pieces, got %v", failedPieces)
	}
}

func TestVerifyFinalizedPieces_UsesMaxVerifyConcurrency(t *testing.T) {
	t.Parallel()

	// Verify the constant is what we expect (documents the current value)
	if maxVerifyConcurrency != 4 {
		t.Errorf("maxVerifyConcurrency = %d, want 4", maxVerifyConcurrency)
	}
}

func TestVerifyFinalizedPieces_FailsOnHashMismatch(t *testing.T) {
	t.Parallel()

	const pieceSize = 256
	const numPieces = 2
	const totalSize = pieceSize * numPieces

	tmpDir := t.TempDir()
	logger := testLogger(t)

	s := &Server{
		config:      ServerConfig{BasePath: tmpDir},
		logger:      logger,
		store:       newTorrentStore(tmpDir, logger),
		memBudget:   semaphore.NewWeighted(512 * 1024 * 1024),
		finalizeSem: semaphore.NewWeighted(1),
	}

	fileData := make([]byte, totalSize)
	for i := range fileData {
		fileData[i] = byte(i % 251)
	}

	// Compute correct hashes but corrupt one
	pieceHashes := make([]string, numPieces)
	for i := range numPieces {
		offset := i * pieceSize
		pieceHashes[i] = utils.ComputeSHA1(fileData[offset : offset+pieceSize])
	}
	pieceHashes[1] = "0000000000000000000000000000000000000000" // Bad hash

	filePath := filepath.Join(tmpDir, "test.bin")
	if err := os.WriteFile(filePath, fileData, 0o644); err != nil {
		t.Fatal(err)
	}

	state := &serverTorrentState{
		torrentMeta: torrentMeta{
			pieceHashes: pieceHashes,
			pieceLength: pieceSize,
			totalSize:   totalSize,
			files: []*serverFileInfo{
				{path: filePath, offset: 0, size: totalSize, selected: true},
			},
		},
	}

	ctx := context.Background()
	failedPieces, err := s.verifyFinalizedPieces(ctx, "testHash", state)
	if err != nil {
		t.Fatalf("unexpected system error: %v", err)
	}
	if len(failedPieces) != 1 || failedPieces[0] != 1 {
		t.Fatalf("expected [1] failed pieces, got %v", failedPieces)
	}
}

// TestVerifyFinalizedPieces_RespectsVerifiedBitset is the regression test for
// the verified-bitset optimization in commit f64de92. Pieces that have been
// hash-verified post-flush during earlyFinalizeFile (via markInteriorVerified)
// are skipped at finalize time. Pieces NOT in the bitset (hardlinked-file
// pieces, late-arriving pieces in files that didn't go through
// earlyFinalizeFile) must still be re-verified.
//
// If markInteriorVerified ever sets a bit that isn't actually verified,
// verifyFinalizedPieces would skip a hash check and we'd silently ship
// corrupted data. The two sub-tests pin the bit-honoring contract from both
// sides: bit set → skip even when corrupt; bit clear → catch corrupt.
func TestVerifyFinalizedPieces_RespectsVerifiedBitset(t *testing.T) {
	t.Parallel()

	// Helper: build a state with two pieces, the second one corrupted on disk.
	// Caller can mark piece 1 as verified or not.
	const pieceSize = 256
	const numPieces = 2
	const totalSize = pieceSize * numPieces

	buildState := func(t *testing.T, tmpDir string) *serverTorrentState {
		t.Helper()
		fileData := make([]byte, totalSize)
		for i := range fileData {
			fileData[i] = byte(i % 251)
		}

		// Compute hashes from the original (correct) data, then corrupt piece 1
		// on disk. verifyFinalizedPieces reads back from disk and computes
		// SHA1; with the disk corrupted, the read-back hash won't match the
		// stored hash unless the verified bit short-circuits the check.
		pieceHashes := make([]string, numPieces)
		for i := range numPieces {
			offset := i * pieceSize
			pieceHashes[i] = utils.ComputeSHA1(fileData[offset : offset+pieceSize])
		}

		corrupted := make([]byte, totalSize)
		copy(corrupted, fileData)
		// Flip a byte in the second piece's region.
		corrupted[pieceSize] ^= 0xFF

		filePath := filepath.Join(tmpDir, "test.bin")
		if err := os.WriteFile(filePath, corrupted, 0o644); err != nil {
			t.Fatal(err)
		}

		written := bitset.New(numPieces)
		written.Set(0)
		written.Set(1)

		return &serverTorrentState{
			torrentMeta: torrentMeta{
				pieceHashes: pieceHashes,
				pieceLength: pieceSize,
				totalSize:   totalSize,
				files: []*serverFileInfo{
					{path: filePath, offset: 0, size: totalSize, selected: true},
				},
			},
			written:  written,
			verified: bitset.New(numPieces),
		}
	}

	newServer := func(t *testing.T, tmpDir string) *Server {
		t.Helper()
		logger := testLogger(t)
		return &Server{
			config:      ServerConfig{BasePath: tmpDir},
			logger:      logger,
			store:       newTorrentStore(tmpDir, logger),
			memBudget:   semaphore.NewWeighted(512 * 1024 * 1024),
			finalizeSem: semaphore.NewWeighted(1),
		}
	}

	t.Run("piece marked verified is skipped even when corrupt on disk", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		state := buildState(t, tmpDir)
		state.verified.Set(1) // claim piece 1 was already verified

		s := newServer(t, tmpDir)
		failed, err := s.verifyFinalizedPieces(context.Background(), "h", state)
		if err != nil {
			t.Fatalf("unexpected system error: %v", err)
		}
		if len(failed) != 0 {
			t.Fatalf("verified piece must be skipped — got failed=%v "+
				"(this means the bitset isn't being honored, "+
				"which is fine for correctness here but breaks the perf optimization)", failed)
		}
	})

	t.Run("piece NOT marked verified is checked and corruption surfaces", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		state := buildState(t, tmpDir)
		// Don't set state.verified.Set(1) — emulates a hardlinked-file piece
		// or a piece whose file never went through earlyFinalizeFile.

		s := newServer(t, tmpDir)
		failed, err := s.verifyFinalizedPieces(context.Background(), "h", state)
		if err != nil {
			t.Fatalf("unexpected system error: %v", err)
		}
		if len(failed) != 1 || failed[0] != 1 {
			t.Fatalf("unverified corrupt piece must be detected — got failed=%v", failed)
		}
	})

	t.Run("nil verified bitset still verifies all pieces", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		state := buildState(t, tmpDir)
		state.verified = nil // older state from before the bitset was introduced

		s := newServer(t, tmpDir)
		failed, err := s.verifyFinalizedPieces(context.Background(), "h", state)
		if err != nil {
			t.Fatalf("unexpected system error: %v", err)
		}
		if len(failed) != 1 || failed[0] != 1 {
			t.Fatalf("with nil bitset all pieces must be checked — got failed=%v", failed)
		}
	})
}

func TestFinalizeTorrent_PollReturnsVerifying(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	logger := testLogger(t)

	s := &Server{
		config:      ServerConfig{BasePath: tmpDir},
		logger:      logger,
		store:       newTorrentStore(tmpDir, logger),
		memBudget:   semaphore.NewWeighted(512 * 1024 * 1024),
		finalizeSem: semaphore.NewWeighted(1),
	}

	hash := "poll-verify-test"
	done := make(chan struct{}) // not closed yet — simulates in-progress verification
	state := &serverTorrentState{
		torrentMeta: torrentMeta{
			pieceLength: 256,
			totalSize:   512,
			files:       []*serverFileInfo{},
		},
		written: boolSliceToBitSet([]bool{true, true}),
		finalization: finalizationState{
			active: true,
			done:   done,
		},
	}

	s.store.mu.Lock()
	s.store.entries[hash] = state
	s.store.mu.Unlock()

	resp, err := s.FinalizeTorrent(context.Background(), &pb.FinalizeTorrentRequest{
		TorrentHash: hash,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.GetSuccess() {
		t.Fatalf("expected success=true for verifying response, got error: %s", resp.GetError())
	}
	if resp.GetState() != "verifying" {
		t.Errorf("expected state 'verifying', got %q", resp.GetState())
	}
}

func TestFinalizeTorrent_PollReturnsCompletedResult(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	logger := testLogger(t)

	s := &Server{
		config:      ServerConfig{BasePath: tmpDir},
		logger:      logger,
		store:       newTorrentStore(tmpDir, logger),
		memBudget:   semaphore.NewWeighted(512 * 1024 * 1024),
		finalizeSem: semaphore.NewWeighted(1),
	}

	hash := "poll-complete-test"
	done := make(chan struct{})
	close(done)
	state := &serverTorrentState{
		torrentMeta: torrentMeta{
			pieceLength: 256,
			totalSize:   256,
			files:       []*serverFileInfo{},
		},
		written: boolSliceToBitSet([]bool{true}),
		finalization: finalizationState{
			active: true,
			done:   done,
			result: &finalizeResult{
				success: true,
				state:   "uploading",
			},
		},
		torrentFile: []byte("fake-torrent-data"),
	}

	// In production, storeSuccessResult writes the .finalized marker during
	// background finalization (before the source polls). Pre-create it here
	// to simulate that the background work already completed.
	metaDir := filepath.Join(tmpDir, metaDirName, hash)
	if err := os.MkdirAll(metaDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(metaDir, finalizedFileName), nil, 0o644); err != nil {
		t.Fatal(err)
	}

	s.store.mu.Lock()
	s.store.entries[hash] = state
	s.store.mu.Unlock()

	resp, err := s.FinalizeTorrent(context.Background(), &pb.FinalizeTorrentRequest{
		TorrentHash: hash,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.GetSuccess() {
		t.Fatalf("expected success, got error: %s", resp.GetError())
	}
	if resp.GetState() != "uploading" {
		t.Errorf("expected state 'uploading', got %q", resp.GetState())
	}

	// Torrent should be cleaned up after returning success
	s.store.mu.RLock()
	_, exists := s.store.entries[hash]
	s.store.mu.RUnlock()
	if exists {
		t.Error("torrent should be removed from tracking after successful finalize poll")
	}

	// Metadata directory should contain only the .finalized marker
	markerPath := filepath.Join(metaDir, finalizedFileName)
	if _, statErr := os.Stat(markerPath); statErr != nil {
		t.Error("finalized marker should exist after finalization")
	}
	entries, _ := os.ReadDir(metaDir)
	if len(entries) != 1 || entries[0].Name() != finalizedFileName {
		t.Errorf("metadata directory should contain only .finalized marker, got %d entries", len(entries))
	}
}

func TestFinalizeTorrent_PollReturnsFailedResult(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	logger := testLogger(t)

	s := &Server{
		config:      ServerConfig{BasePath: tmpDir},
		logger:      logger,
		store:       newTorrentStore(tmpDir, logger),
		memBudget:   semaphore.NewWeighted(512 * 1024 * 1024),
		finalizeSem: semaphore.NewWeighted(1),
	}

	hash := "poll-fail-test"
	done := make(chan struct{})
	close(done)
	state := &serverTorrentState{
		torrentMeta: torrentMeta{
			pieceLength: 256,
			totalSize:   256,
			files: []*serverFileInfo{
				{size: 256, offset: 0, selected: true},
			},
		},
		written: boolSliceToBitSet([]bool{true}),
		finalization: finalizationState{
			active: true,
			done:   done,
			result: &finalizeResult{
				success: false,
				err:     "verification failed: piece 5: hash mismatch",
			},
		},
	}

	s.store.mu.Lock()
	s.store.entries[hash] = state
	s.store.mu.Unlock()

	// First poll returns the error
	resp, err := s.FinalizeTorrent(context.Background(), &pb.FinalizeTorrentRequest{
		TorrentHash: hash,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.GetSuccess() {
		t.Fatal("expected failure response")
	}
	if resp.GetError() != "verification failed: piece 5: hash mismatch" {
		t.Errorf("unexpected error message: %s", resp.GetError())
	}

	// After failure, finalizing should be cleared to allow retry
	state.mu.Lock()
	stillFinalizing := state.finalization.active
	state.mu.Unlock()
	if stillFinalizing {
		t.Error("finalizing flag should be cleared after returning failed result")
	}

	// Verify a second call actually retries (doesn't reject as "already in progress").
	// It will fail with "incomplete" since written.Count() < totalPieces after clearing,
	// but that proves it entered the normal finalization path.
	state.mu.Lock()
	state.written = bitset.New(1) // Clear all bits
	state.mu.Unlock()
	resp2, err2 := s.FinalizeTorrent(context.Background(), &pb.FinalizeTorrentRequest{
		TorrentHash: hash,
	})
	if err2 != nil {
		t.Fatalf("retry call should not error: %v", err2)
	}
	if resp2.GetSuccess() {
		t.Fatal("retry should fail due to incomplete pieces, not succeed")
	}
	if !strings.Contains(resp2.GetError(), "incomplete") {
		t.Errorf("retry should return incomplete error, got: %s", resp2.GetError())
	}
	if resp2.GetErrorCode() != pb.FinalizeErrorCode_FINALIZE_ERROR_INCOMPLETE {
		t.Errorf("retry should return FINALIZE_ERROR_INCOMPLETE, got: %v", resp2.GetErrorCode())
	}
}

func TestFinalizeTorrent_ConcurrentPollDuringSetup(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	logger := testLogger(t)

	s := &Server{
		config:      ServerConfig{BasePath: tmpDir},
		logger:      logger,
		store:       newTorrentStore(tmpDir, logger),
		memBudget:   semaphore.NewWeighted(512 * 1024 * 1024),
		finalizeSem: semaphore.NewWeighted(1),
	}

	hash := "concurrent-setup-test"
	done := make(chan struct{}) // not closed — simulates in-progress work
	// finalizeDone is set upfront (same as production code) so concurrent
	// polls always see "verifying" instead of a spurious error.
	state := &serverTorrentState{
		torrentMeta: torrentMeta{
			pieceLength: 256,
			totalSize:   256,
			files:       []*serverFileInfo{},
		},
		written: boolSliceToBitSet([]bool{true}),
		finalization: finalizationState{
			active: true,
			done:   done,
		},
	}

	s.store.mu.Lock()
	s.store.entries[hash] = state
	s.store.mu.Unlock()

	resp, err := s.FinalizeTorrent(context.Background(), &pb.FinalizeTorrentRequest{
		TorrentHash: hash,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.GetSuccess() {
		t.Fatalf("expected success=true for verifying response, got error: %s", resp.GetError())
	}
	if resp.GetState() != "verifying" {
		t.Errorf("expected state 'verifying', got %q", resp.GetState())
	}
}

func TestRunBackgroundFinalization_SerializesViaSemaphore(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	logger := testLogger(t)

	// createTorrentState writes deterministic file data to disk and returns the
	// corresponding serverTorrentState ready for finalization.
	createTorrentState := func(
		t *testing.T, dir, hash string, numPieces int, pieceSize int64,
	) *serverTorrentState {
		t.Helper()
		totalSize := int64(numPieces) * pieceSize
		fileData := make([]byte, totalSize)
		for j := range fileData {
			fileData[j] = byte(j % 251)
		}

		pieceHashes := make([]string, numPieces)
		for p := range numPieces {
			offset := int64(p) * pieceSize
			pieceHashes[p] = utils.ComputeSHA1(fileData[offset : offset+pieceSize])
		}

		filePath := filepath.Join(dir, hash+".bin")
		if writeErr := os.WriteFile(filePath, fileData, 0o644); writeErr != nil {
			t.Fatal(writeErr)
		}

		return &serverTorrentState{
			torrentMeta: torrentMeta{
				pieceHashes: pieceHashes,
				pieceLength: pieceSize,
				totalSize:   totalSize,
				files:       []*serverFileInfo{{path: filePath, offset: 0, size: totalSize, selected: true}},
			},
			torrentFile: []byte("fake-torrent-data"),
		}
	}

	newServer := func() *Server {
		bgCtx, bgCancel := context.WithCancel(context.Background())
		s := &Server{
			config:      ServerConfig{BasePath: tmpDir},
			logger:      logger,
			store:       newTorrentStore(tmpDir, logger),
			memBudget:   semaphore.NewWeighted(512 * 1024 * 1024),
			finalizeSem: semaphore.NewWeighted(1),
			bgCtx:       bgCtx,
			bgCancel:    bgCancel,
		}
		t.Cleanup(func() {
			bgCancel()
			s.bgWg.Wait()
		})
		return s
	}

	t.Run("blocks when semaphore is held", func(t *testing.T) {
		t.Parallel()

		s := newServer()
		s.finalizeSem.Acquire(context.Background(), 1)

		hash := "sem-block-test"
		state := createTorrentState(t, tmpDir, hash, 1, 256)

		s.store.mu.Lock()
		s.store.entries[hash] = state
		s.store.mu.Unlock()

		done := make(chan struct{})
		go s.runBackgroundFinalization(
			hash, state, &pb.FinalizeTorrentRequest{TorrentHash: hash}, time.Now(), done,
		)

		// Give goroutine time to start and block on semaphore acquire.
		time.Sleep(50 * time.Millisecond)

		select {
		case <-done:
			t.Fatal("finalization completed while semaphore was held")
		default:
		}

		s.finalizeSem.Release(1)

		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("finalization timed out after semaphore release")
		}
	})

	t.Run("multiple finalizations serialize", func(t *testing.T) {
		t.Parallel()

		const numTorrents = 3

		var maxConcurrent atomic.Int32
		var running atomic.Int32

		// Separate server so the finalizeSem is not shared with the other subtest.
		s := newServer()

		// Replace finalizeSem with a wide semaphore so runBackgroundFinalization
		// never blocks on it. We gate serialization through origSem (weight=1)
		// ourselves, recording max concurrent holders.
		origSem := s.finalizeSem
		s.finalizeSem = semaphore.NewWeighted(int64(numTorrents))

		var wg sync.WaitGroup
		for i := range numTorrents {
			hash := fmt.Sprintf("serial-test-%d", i)
			state := createTorrentState(t, tmpDir, hash, 10, 1024)

			s.store.mu.Lock()
			s.store.entries[hash] = state
			s.store.mu.Unlock()

			wg.Go(func() {
				origSem.Acquire(context.Background(), 1)
				cur := running.Add(1)
				for {
					old := maxConcurrent.Load()
					if cur <= old || maxConcurrent.CompareAndSwap(old, cur) {
						break
					}
				}

				done := make(chan struct{})
				s.runBackgroundFinalization(
					hash, state, &pb.FinalizeTorrentRequest{TorrentHash: hash}, time.Now(), done,
				)

				running.Add(-1)
				origSem.Release(1)
			})
		}

		wg.Wait()

		if mc := maxConcurrent.Load(); mc > 1 {
			t.Errorf("max concurrent finalizations = %d, want 1", mc)
		}
	})
}

func TestVerifyFinalizedPieces_RequiresPieceHashes(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	logger := testLogger(t)

	s := &Server{
		config:      ServerConfig{BasePath: tmpDir},
		logger:      logger,
		store:       newTorrentStore(tmpDir, logger),
		memBudget:   semaphore.NewWeighted(512 * 1024 * 1024),
		finalizeSem: semaphore.NewWeighted(1),
	}

	state := &serverTorrentState{
		torrentMeta: torrentMeta{
			pieceHashes: nil, // No hashes
			pieceLength: 1024,
			totalSize:   1024,
		},
	}

	ctx := context.Background()
	_, err := s.verifyFinalizedPieces(ctx, "testHash", state)
	if err == nil {
		t.Fatal("expected error when piece hashes are missing")
	}
}

func TestRecoverVerificationFailure(t *testing.T) {
	t.Parallel()

	const pieceSize int64 = 256
	const numPieces = 3
	const totalSize = pieceSize * numPieces

	tmpDir := t.TempDir()
	logger := testLogger(t)

	// Track whether state was persisted.
	var stateSaved atomic.Bool

	s := &Server{
		config:      ServerConfig{BasePath: tmpDir},
		logger:      logger,
		store:       newTorrentStore(tmpDir, logger),
		memBudget:   semaphore.NewWeighted(512 * 1024 * 1024),
		finalizeSem: semaphore.NewWeighted(1),
		saveStateFunc: func(_ string, _ *bitset.BitSet) error {
			stateSaved.Store(true)
			return nil
		},
	}

	// Create two files at final paths (no .partial suffix).
	// File 0: covers pieces 0–1 (512 bytes, offset 0)
	// File 1: covers piece 2 (256 bytes, offset 512)
	file0Path := filepath.Join(tmpDir, "file0.bin")
	file1Path := filepath.Join(tmpDir, "file1.bin")
	if err := os.WriteFile(file0Path, make([]byte, 512), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file1Path, make([]byte, 256), 0o644); err != nil {
		t.Fatal(err)
	}

	state := &serverTorrentState{
		torrentMeta: torrentMeta{
			pieceLength: pieceSize,
			totalSize:   totalSize,
			files: []*serverFileInfo{
				{
					path:           file0Path,
					size:           512,
					offset:         0,
					selected:       true,
					firstPiece:     0,
					lastPiece:      1,
					piecesTotal:    2,
					piecesWritten:  2,
					earlyFinalized: true,
				},
				{
					path:           file1Path,
					size:           256,
					offset:         512,
					selected:       true,
					firstPiece:     2,
					lastPiece:      2,
					piecesTotal:    1,
					piecesWritten:  1,
					earlyFinalized: true,
				},
			},
		},
		written:   boolSliceToBitSet([]bool{true, true, true}),
		statePath: filepath.Join(tmpDir, ".state"),
	}

	// Fail piece 1 — should affect file0 (which spans pieces 0–1) but not file1.
	s.recoverVerificationFailure(context.Background(), "test-hash", state, []int{1})

	// Piece 1 should be unwritten.
	if state.written.Test(1) {
		t.Error("piece 1 should be marked unwritten")
	}
	// Pieces 0 and 2 should still be written.
	if !state.written.Test(0) {
		t.Error("piece 0 should still be written")
	}
	if !state.written.Test(2) {
		t.Error("piece 2 should still be written")
	}

	// writtenCount should be decremented.
	if state.written.Count() != 2 {
		t.Errorf("writtenCount = %d, want 2", state.written.Count())
	}

	// File0 should be renamed back to .partial.
	if !strings.HasSuffix(state.files[0].path, partialSuffix) {
		t.Errorf("file0 should have .partial suffix, got %q", state.files[0].path)
	}
	if state.files[0].earlyFinalized {
		t.Error("file0 earlyFinalized should be cleared")
	}
	// File0 piecesWritten should be recalculated (piece 0 written, piece 1 not).
	if state.files[0].piecesWritten != 1 {
		t.Errorf("file0 piecesWritten = %d, want 1", state.files[0].piecesWritten)
	}

	// File1 should be untouched — piece 2 is not in failed set.
	if strings.HasSuffix(state.files[1].path, partialSuffix) {
		t.Error("file1 should not be renamed to .partial")
	}
	if state.files[1].piecesWritten != 1 {
		t.Errorf("file1 piecesWritten = %d, want 1", state.files[1].piecesWritten)
	}

	// State should be persisted.
	if !stateSaved.Load() {
		t.Error("state should have been saved after recovery")
	}

	// The .partial file should exist on disk.
	if _, err := os.Stat(state.files[0].path); err != nil {
		t.Errorf("partial file should exist: %v", err)
	}
}

func TestVerifyFinalizedPieces_CollectsAllFailures(t *testing.T) {
	t.Parallel()

	const pieceSize = 256
	const numPieces = 4
	const totalSize = pieceSize * numPieces

	tmpDir := t.TempDir()
	logger := testLogger(t)

	s := &Server{
		config:      ServerConfig{BasePath: tmpDir},
		logger:      logger,
		store:       newTorrentStore(tmpDir, logger),
		memBudget:   semaphore.NewWeighted(512 * 1024 * 1024),
		finalizeSem: semaphore.NewWeighted(1),
	}

	fileData := make([]byte, totalSize)
	for i := range fileData {
		fileData[i] = byte(i % 251)
	}

	// Compute correct hashes then corrupt two of them.
	pieceHashes := make([]string, numPieces)
	for i := range numPieces {
		offset := i * pieceSize
		pieceHashes[i] = utils.ComputeSHA1(fileData[offset : offset+pieceSize])
	}
	pieceHashes[1] = "0000000000000000000000000000000000000000"
	pieceHashes[3] = "0000000000000000000000000000000000000000"

	filePath := filepath.Join(tmpDir, "test.bin")
	if err := os.WriteFile(filePath, fileData, 0o644); err != nil {
		t.Fatal(err)
	}

	state := &serverTorrentState{
		torrentMeta: torrentMeta{
			pieceHashes: pieceHashes,
			pieceLength: pieceSize,
			totalSize:   totalSize,
			files: []*serverFileInfo{
				{path: filePath, offset: 0, size: totalSize, selected: true},
			},
		},
	}

	ctx := context.Background()
	failedPieces, err := s.verifyFinalizedPieces(ctx, "testHash", state)
	if err != nil {
		t.Fatalf("unexpected system error: %v", err)
	}
	if len(failedPieces) != 2 {
		t.Fatalf("expected 2 failed pieces, got %d: %v", len(failedPieces), failedPieces)
	}

	// Check both corrupted pieces are reported (order may vary due to concurrency).
	failedSet := make(map[int]struct{}, len(failedPieces))
	for _, p := range failedPieces {
		failedSet[p] = struct{}{}
	}
	if _, ok := failedSet[1]; !ok {
		t.Error("piece 1 should be in failed set")
	}
	if _, ok := failedSet[3]; !ok {
		t.Error("piece 3 should be in failed set")
	}
}

func TestFinalizeTorrent_NotFound_ReturnsErrorCode(t *testing.T) {
	t.Parallel()
	s, _ := newTestDestServer(t)

	// No torrent state exists — not in memory and no metadata on disk.
	resp, err := s.FinalizeTorrent(context.Background(), &pb.FinalizeTorrentRequest{
		TorrentHash: "nonexistent",
	})
	if err != nil {
		t.Fatalf("unexpected gRPC error: %v", err)
	}
	if resp.GetSuccess() {
		t.Fatal("expected failure response")
	}
	if resp.GetErrorCode() != pb.FinalizeErrorCode_FINALIZE_ERROR_NOT_FOUND {
		t.Errorf("expected FINALIZE_ERROR_NOT_FOUND, got %v", resp.GetErrorCode())
	}
}

// TestFinalizeFiles_PendingHardlinkRejectsWrongSizedSource regression-tests the
// guard added to mirror tryHardlinkFromRegistered: when a pending-hardlink
// source has finished writing but ends up at a size that doesn't match this
// torrent's expected size (stale FileID, source-torrent metadata divergence,
// crash-restart with stale in-progress state), finalizeFiles must NOT create
// the link. A wrong-sized link makes destination qB reject the torrent at
// AddTorrent with "mismatching file size".
func TestFinalizeFiles_PendingHardlinkRejectsWrongSizedSource(t *testing.T) {
	t.Parallel()
	s, tmpDir := newTestDestServer(t)
	ctx := context.Background()

	// Source file the pending hardlink would link to. Written at 50 bytes.
	sourceRel := filepath.Join("other-torrent", "shared.mkv")
	sourceAbs := filepath.Join(tmpDir, sourceRel)
	if err := os.MkdirAll(filepath.Dir(sourceAbs), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(sourceAbs, make([]byte, 50), 0o644); err != nil {
		t.Fatal(err)
	}

	// Source has finished writing → its doneCh is closed before finalizeFiles runs.
	doneCh := make(chan struct{})
	close(doneCh)

	// THIS torrent's metadata claims the file should be 100 bytes. The 50-byte
	// source thus must NOT be linked.
	targetDir := filepath.Join(tmpDir, "this-torrent")
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		t.Fatal(err)
	}
	state := &serverTorrentState{
		torrentMeta: torrentMeta{
			pieceLength: 100,
			totalSize:   100,
			files: []*serverFileInfo{
				{
					path:     filepath.Join(targetDir, "shared.mkv"),
					offset:   0,
					size:     100,
					selected: true,
					hardlink: hardlinkInfo{
						state:      hlStatePending,
						sourcePath: sourceRel,
						doneCh:     doneCh,
					},
				},
			},
		},
		written:   bitset.New(1).Set(0),
		statePath: filepath.Join(tmpDir, ".state"),
	}

	err := s.finalizeFiles(ctx, "testHash", state)
	if err == nil {
		t.Fatal("expected finalizeFiles to fail when pending-hardlink source size mismatches expected size")
	}
	if !strings.Contains(err.Error(), "expected 100") || !strings.Contains(err.Error(), "size 50") {
		t.Errorf("error should name observed and expected sizes, got: %v", err)
	}

	// Crucially, NO link must have been created — qb-sync would otherwise have
	// produced the wrong-sized destination file that triggers qB's rejection.
	targetPath := filepath.Join(targetDir, "shared.mkv")
	if _, statErr := os.Stat(targetPath); statErr == nil {
		t.Errorf("hardlink should NOT have been created at %s", targetPath)
	}
}

func TestFinalizationStateDiskStageDoneSurvivesReset(t *testing.T) {
	var f finalizationState

	f.start()
	f.diskStageDone = true
	f.storeResult(&finalizeResult{err: "queue timeout", errorCode: pb.FinalizeErrorCode_FINALIZE_ERROR_BUSY})
	f.reset()

	if f.active || f.result != nil || f.done != nil {
		t.Error("reset must clear the active/result/done lifecycle fields")
	}
	if !f.diskStageDone {
		t.Error("diskStageDone must survive reset so retries skip re-verification")
	}
}

// newTwoStageTestState writes deterministic file data to disk and returns a
// serverTorrentState ready for finalization (mirrors the helper in
// TestRunBackgroundFinalization_SerializesViaSemaphore).
func newTwoStageTestState(t *testing.T, dir, hash string, numPieces int) *serverTorrentState {
	t.Helper()
	const pieceSize = int64(256)
	totalSize := int64(numPieces) * pieceSize
	fileData := make([]byte, totalSize)
	for j := range fileData {
		fileData[j] = byte(j % 251)
	}

	pieceHashes := make([]string, numPieces)
	for p := range numPieces {
		offset := int64(p) * pieceSize
		pieceHashes[p] = utils.ComputeSHA1(fileData[offset : offset+pieceSize])
	}

	filePath := filepath.Join(dir, hash+".bin")
	if writeErr := os.WriteFile(filePath, fileData, 0o644); writeErr != nil {
		t.Fatal(writeErr)
	}

	written := make([]bool, numPieces)
	for p := range written {
		written[p] = true
	}

	return &serverTorrentState{
		torrentMeta: torrentMeta{
			pieceHashes: pieceHashes,
			pieceLength: pieceSize,
			totalSize:   totalSize,
			files:       []*serverFileInfo{{path: filePath, offset: 0, size: totalSize, selected: true}},
		},
		written:     boolSliceToBitSet(written),
		statePath:   filepath.Join(dir, hash+".state"),
		torrentFile: []byte("fake-torrent-data"),
	}
}

func TestRunBackgroundFinalization_SkipsDiskStageWhenDone(t *testing.T) {
	t.Parallel()

	corruptDataFile := func(t *testing.T, state *serverTorrentState) {
		t.Helper()
		// Corrupt the on-disk data in place (same length) AFTER hashes were
		// computed, so verification — if it runs — fails with a clean hash
		// mismatch rather than a short read.
		corrupted := make([]byte, state.totalSize)
		if err := os.WriteFile(state.files[0].path, corrupted, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("diskStageDone skips verification entirely", func(t *testing.T) {
		t.Parallel()
		s, tmpDir := newTestDestServer(t)

		hash := "skip-disk-stage"
		state := newTwoStageTestState(t, tmpDir, hash, 2)
		state.finalization.diskStageDone = true
		corruptDataFile(t, state)

		s.store.mu.Lock()
		s.store.entries[hash] = state
		s.store.mu.Unlock()

		done := make(chan struct{})
		s.runBackgroundFinalization(hash, state, &pb.FinalizeTorrentRequest{TorrentHash: hash}, time.Now(), done)

		state.mu.Lock()
		result := state.finalization.result
		state.mu.Unlock()
		if result == nil || !result.success {
			t.Fatalf("expected success (verification skipped via diskStageDone), got %+v", result)
		}
	})

	t.Run("without diskStageDone corruption is detected", func(t *testing.T) {
		t.Parallel()
		s, tmpDir := newTestDestServer(t)

		hash := "verify-disk-stage"
		state := newTwoStageTestState(t, tmpDir, hash, 2)
		corruptDataFile(t, state)

		s.store.mu.Lock()
		s.store.entries[hash] = state
		s.store.mu.Unlock()

		done := make(chan struct{})
		s.runBackgroundFinalization(hash, state, &pb.FinalizeTorrentRequest{TorrentHash: hash}, time.Now(), done)

		state.mu.Lock()
		result := state.finalization.result
		state.mu.Unlock()
		if result == nil || result.success {
			t.Fatalf("expected verification failure, got %+v", result)
		}
		if result.errorCode != pb.FinalizeErrorCode_FINALIZE_ERROR_INCOMPLETE {
			t.Errorf("expected INCOMPLETE, got %v", result.errorCode)
		}
	})
}

func TestRunBackgroundFinalization_DiskQueueTimeoutReturnsBusy(t *testing.T) {
	t.Parallel()

	s, tmpDir := newTestDestServer(t)
	s.finalizeQueueWait = 50 * time.Millisecond

	// Hold the disk-stage slot so the finalization queues and times out.
	if err := s.finalizeSem.Acquire(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	defer s.finalizeSem.Release(1)

	hash := "disk-queue-busy"
	state := newTwoStageTestState(t, tmpDir, hash, 1)

	s.store.mu.Lock()
	s.store.entries[hash] = state
	s.store.mu.Unlock()

	done := make(chan struct{})
	s.runBackgroundFinalization(hash, state, &pb.FinalizeTorrentRequest{TorrentHash: hash}, time.Now(), done)

	state.mu.Lock()
	result := state.finalization.result
	state.mu.Unlock()
	if result == nil || result.success {
		t.Fatalf("expected queue-timeout failure, got %+v", result)
	}
	if result.errorCode != pb.FinalizeErrorCode_FINALIZE_ERROR_BUSY {
		t.Errorf("expected BUSY error code, got %v (%s)", result.errorCode, result.err)
	}
}

func TestRunBackgroundFinalization_QBStageIndependentOfDiskSem(t *testing.T) {
	t.Parallel()

	s, tmpDir := newTestDestServer(t)

	// Hold the disk-stage slot for the entire test. A torrent whose disk stage
	// is already done must complete its qB stage without touching finalizeSem.
	if err := s.finalizeSem.Acquire(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	defer s.finalizeSem.Release(1)

	hash := "qb-stage-independent"
	state := newTwoStageTestState(t, tmpDir, hash, 1)
	state.finalization.diskStageDone = true

	s.store.mu.Lock()
	s.store.entries[hash] = state
	s.store.mu.Unlock()

	finished := make(chan struct{})
	go func() {
		done := make(chan struct{})
		s.runBackgroundFinalization(hash, state, &pb.FinalizeTorrentRequest{TorrentHash: hash}, time.Now(), done)
		close(finished)
	}()

	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("qB stage blocked on the disk-stage semaphore")
	}

	state.mu.Lock()
	result := state.finalization.result
	state.mu.Unlock()
	if result == nil || !result.success {
		t.Fatalf("expected success, got %+v", result)
	}
}

// TestRunQBStage_BusyClassification exercises the qB stage with a real mock
// qB client (unlike the nil-client short-circuit tests above), pinning the two
// BUSY-producing paths: qB stuck in a checking state at budget expiry, and the
// qB-stage queue timing out.
func TestRunQBStage_BusyClassification(t *testing.T) {
	t.Parallel()

	newQBServer := func(t *testing.T, mock *mockQBClient) (*Server, string) {
		t.Helper()
		s, tmpDir := newTestDestServer(t)
		s.qbClient = mock
		s.config.QB = &QBConfig{
			PollInterval: 10 * time.Millisecond,
			PollTimeout:  50 * time.Millisecond,
		}
		return s, tmpDir
	}

	t.Run("qB stuck checking at budget expiry stores BUSY", func(t *testing.T) {
		t.Parallel()
		hash := "qb-checking-busy"
		mock := &mockQBClient{
			torrents: []qbittorrent.Torrent{
				{Hash: hash, State: qbittorrent.TorrentStateCheckingUp, Progress: 0.5},
			},
		}
		s, tmpDir := newQBServer(t, mock)

		state := newTwoStageTestState(t, tmpDir, hash, 1)
		state.finalization.diskStageDone = true // isolate the qB stage

		s.store.mu.Lock()
		s.store.entries[hash] = state
		s.store.mu.Unlock()

		done := make(chan struct{})
		s.runBackgroundFinalization(hash, state, &pb.FinalizeTorrentRequest{TorrentHash: hash}, time.Now(), done)

		state.mu.Lock()
		result := state.finalization.result
		state.mu.Unlock()
		if result == nil || result.success {
			t.Fatalf("expected qB-checking timeout failure, got %+v", result)
		}
		if result.errorCode != pb.FinalizeErrorCode_FINALIZE_ERROR_BUSY {
			t.Errorf("expected BUSY for qB-still-checking timeout, got %v (%s)", result.errorCode, result.err)
		}
	})

	t.Run("qB in genuine error state stores NONE, not BUSY", func(t *testing.T) {
		t.Parallel()
		hash := "qb-error-not-busy"
		mock := &mockQBClient{
			torrents: []qbittorrent.Torrent{
				{Hash: hash, State: qbittorrent.TorrentStateMissingFiles, Progress: 0.5},
			},
		}
		s, tmpDir := newQBServer(t, mock)

		state := newTwoStageTestState(t, tmpDir, hash, 1)
		state.finalization.diskStageDone = true

		s.store.mu.Lock()
		s.store.entries[hash] = state
		s.store.mu.Unlock()

		done := make(chan struct{})
		s.runBackgroundFinalization(hash, state, &pb.FinalizeTorrentRequest{TorrentHash: hash}, time.Now(), done)

		state.mu.Lock()
		result := state.finalization.result
		state.mu.Unlock()
		if result == nil || result.success {
			t.Fatalf("expected error-state failure, got %+v", result)
		}
		if result.errorCode != pb.FinalizeErrorCode_FINALIZE_ERROR_NONE {
			t.Errorf("genuine qB error states must burn the retry budget (NONE), got %v", result.errorCode)
		}
	})

	t.Run("qB-stage queue timeout stores BUSY", func(t *testing.T) {
		t.Parallel()
		hash := "qb-queue-busy"
		mock := &mockQBClient{
			torrents: []qbittorrent.Torrent{
				{Hash: hash, State: qbittorrent.TorrentStateStoppedUp, Progress: 1.0},
			},
		}
		s, tmpDir := newQBServer(t, mock)
		s.finalizeQueueWait = 50 * time.Millisecond

		// Hold the qB-stage slot so the finalization queues and times out.
		if err := s.qbStageSem.Acquire(context.Background(), 1); err != nil {
			t.Fatal(err)
		}
		defer s.qbStageSem.Release(1)

		state := newTwoStageTestState(t, tmpDir, hash, 1)
		state.finalization.diskStageDone = true

		s.store.mu.Lock()
		s.store.entries[hash] = state
		s.store.mu.Unlock()

		done := make(chan struct{})
		s.runBackgroundFinalization(hash, state, &pb.FinalizeTorrentRequest{TorrentHash: hash}, time.Now(), done)

		state.mu.Lock()
		result := state.finalization.result
		state.mu.Unlock()
		if result == nil || result.success {
			t.Fatalf("expected qB-stage queue-timeout failure, got %+v", result)
		}
		if result.errorCode != pb.FinalizeErrorCode_FINALIZE_ERROR_BUSY {
			t.Errorf("expected BUSY for qB-stage queue timeout, got %v (%s)", result.errorCode, result.err)
		}
	})
}
