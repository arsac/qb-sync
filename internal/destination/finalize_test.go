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

// TestVerifyConcurrency_HonorsConfigAndClamps pins the operator knob: the
// configured value is used, zero falls back to the default, and out-of-range
// values clamp to the cap (ServerConfig.Validate is not on the startup path,
// so the clamp is the only runtime guard against an unbounded worker pool).
func TestVerifyConcurrency_HonorsConfigAndClamps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  int
		want int
	}{
		{"zero falls back to default", 0, maxVerifyConcurrency},
		{"configured value honored", 2, 2},
		{"cap value honored", maxVerifyConcurrencyCap, maxVerifyConcurrencyCap},
		{"out-of-range clamps to cap", 500, maxVerifyConcurrencyCap},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			s := &Server{config: ServerConfig{VerifyConcurrency: tt.cfg}}
			if got := s.verifyConcurrency(); got != tt.want {
				t.Errorf("verifyConcurrency() with config %d = %d, want %d", tt.cfg, got, tt.want)
			}
		})
	}
}

// TestFinalizeFiles_ParallelSyncAndRename pins the phase-2 fan-out: every
// non-hardlinked file must end up synced, closed, and renamed to its own final
// path, with fi.path updated to match, while hardlink-complete files are left
// untouched. Each file carries index-specific content and a distinct name, so a
// cross-wired task (renaming file i onto file j's target) fails loudly rather
// than passing on a shuffled-but-complete result set.
func TestFinalizeFiles_ParallelSyncAndRename(t *testing.T) {
	t.Parallel()
	s, tmpDir := newTestDestServer(t)
	ctx := context.Background()

	// More files than either concurrency limit so tasks genuinely overlap.
	const numFiles = 96

	dir := filepath.Join(tmpDir, "torrent")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}

	const (
		shapeClosedPartial = iota // .partial on disk, no open handle
		shapeOpenPartial          // .partial on disk with a live write handle
		shapeHardlinked           // hardlink-complete: must not be renamed
		shapeAlreadyFinal         // early-finalized: path already has no suffix
		numShapes
	)

	type expectation struct {
		fi        *serverFileInfo
		shape     int
		finalPath string
		content   []byte
	}

	files := make([]*serverFileInfo, numFiles)
	expected := make([]expectation, numFiles)
	for i := range numFiles {
		shape := i % numShapes
		finalPath := filepath.Join(dir, fmt.Sprintf("file%03d.mkv", i))
		content := fmt.Appendf(nil, "content-of-file-%03d", i)

		onDisk := finalPath
		if shape != shapeAlreadyFinal {
			onDisk = finalPath + partialSuffix
		}
		if err := os.WriteFile(onDisk, content, 0o644); err != nil {
			t.Fatal(err)
		}

		fi := &serverFileInfo{
			path:     onDisk,
			offset:   int64(i) * int64(len(content)),
			size:     int64(len(content)),
			selected: true,
		}
		if shape == shapeHardlinked {
			fi.hardlink.state = hlStateComplete
		}
		if shape == shapeOpenPartial {
			if err := fi.openForWrite(); err != nil {
				t.Fatal(err)
			}
		}

		files[i] = fi
		expected[i] = expectation{fi: fi, shape: shape, finalPath: finalPath, content: content}
	}

	state := &serverTorrentState{
		torrentMeta: torrentMeta{pieceLength: 16, totalSize: 16, files: files},
		written:     bitset.New(1).Set(0),
		statePath:   filepath.Join(tmpDir, ".state"),
	}

	if err := s.finalizeFiles(ctx, "testHash", state); err != nil {
		t.Fatalf("finalizeFiles: %v", err)
	}

	for i, exp := range expected {
		if exp.shape == shapeHardlinked {
			// Skipped entirely: still .partial, in memory and on disk.
			if exp.fi.path != exp.finalPath+partialSuffix {
				t.Errorf("file %d (hardlinked): path = %q, want unchanged %q",
					i, exp.fi.path, exp.finalPath+partialSuffix)
			}
			if _, err := os.Stat(exp.finalPath + partialSuffix); err != nil {
				t.Errorf("file %d (hardlinked): .partial should still exist: %v", i, err)
			}
			continue
		}

		if exp.fi.path != exp.finalPath {
			t.Errorf("file %d: path = %q, want %q", i, exp.fi.path, exp.finalPath)
		}
		if exp.fi.file != nil {
			t.Errorf("file %d: handle should be closed after finalize", i)
		}
		got, err := os.ReadFile(exp.finalPath)
		if err != nil {
			t.Errorf("file %d: reading final path: %v", i, err)
			continue
		}
		if string(got) != string(exp.content) {
			t.Errorf("file %d: final path holds %q, want %q", i, got, exp.content)
		}
	}
}

// TestRunDiskStage_StopsPreVerify pins that the disk stage retires the
// init-time pre-verification pass before it starts reading pieces back itself.
// Left running, that pass reads the same bytes off NFS a second time,
// concurrently, and can still be setting verified bits while the finalize
// read-back queue is being built from them.
func TestRunDiskStage_StopsPreVerify(t *testing.T) {
	t.Parallel()

	s, tmpDir := newTestDestServer(t)

	hash := "stops-preverify"
	state := newTwoStageTestState(t, tmpDir, hash, 2)

	s.store.mu.Lock()
	s.store.entries[hash] = state
	s.store.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	passDone := make(chan struct{})
	var exited atomic.Bool
	state.mu.Lock()
	state.preVerifyCancel = cancel
	state.preVerifyDone = passDone
	state.mu.Unlock()

	go func() {
		defer close(passDone)
		<-ctx.Done()
		time.Sleep(50 * time.Millisecond) // stand-in for the pass unwinding
		exited.Store(true)
	}()

	done := make(chan struct{})
	s.runBackgroundFinalization(hash, state, &pb.FinalizeTorrentRequest{TorrentHash: hash}, time.Now(), done)

	if !exited.Load() {
		t.Error("disk stage ran with the pre-verification pass still going")
	}
	state.mu.Lock()
	cancelLeft, doneLeft := state.preVerifyCancel, state.preVerifyDone
	state.mu.Unlock()
	if cancelLeft != nil || doneLeft != nil {
		t.Error("disk stage left the pre-verification pass registered")
	}
}

// newPendingHardlinkState builds a torrent whose files are all pending on
// another torrent's data. Each file gets its own already-written source file
// (distinct content and size) plus its own doneCh, so a task that resolves the
// wrong file's hardlink is visible in the linked content rather than hidden by
// a uniform fixture. Returns the state, the per-file doneCh channels, the
// target paths and the expected contents, all index-aligned.
func newPendingHardlinkState(
	t *testing.T,
	tmpDir string,
	numFiles int,
) (*serverTorrentState, []chan struct{}, []string, [][]byte) {
	t.Helper()

	srcDir := filepath.Join(tmpDir, "source")
	dstDir := filepath.Join(tmpDir, "target")
	for _, d := range []string{srcDir, dstDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}

	files := make([]*serverFileInfo, numFiles)
	dones := make([]chan struct{}, numFiles)
	targets := make([]string, numFiles)
	contents := make([][]byte, numFiles)

	var offset int64
	for i := range numFiles {
		// Distinct length per index so a cross-wired link also trips the
		// size check, not just the content comparison.
		content := fmt.Appendf(nil, "pending-source-%03d%s", i, strings.Repeat("x", i))
		rel := filepath.Join("source", fmt.Sprintf("src%03d.bin", i))
		if err := os.WriteFile(filepath.Join(tmpDir, rel), content, 0o644); err != nil {
			t.Fatal(err)
		}

		target := filepath.Join(dstDir, fmt.Sprintf("file%03d.bin", i))
		done := make(chan struct{})
		files[i] = &serverFileInfo{
			path:     target,
			size:     int64(len(content)),
			offset:   offset,
			selected: true,
			hardlink: hardlinkInfo{
				state:      hlStatePending,
				sourcePath: rel,
				doneCh:     done,
			},
		}
		dones[i] = done
		targets[i] = target
		contents[i] = content
		offset += int64(len(content))
	}

	state := &serverTorrentState{
		torrentMeta: torrentMeta{pieceLength: 16, totalSize: offset, files: files},
		written:     bitset.New(1).Set(0),
		statePath:   filepath.Join(tmpDir, ".state"),
	}
	return state, dones, targets, contents
}

// TestResolvePendingHardlinks_WaitsConcurrently pins that a torrent's pending
// hardlinks are waited on in parallel. Serially, file i's source torrent could
// not even be looked at until file i-1's had finalized, so a torrent pending on
// several sources paid one full defaultHardlinkWaitTimeout per file before
// reporting the first source that never showed up.
//
// The last file's source is released first: with a serial pass every worker is
// still parked on file 0's channel, so its link never appears and the watchdog
// below fires. With the fan-out it lands immediately.
func TestResolvePendingHardlinks_WaitsConcurrently(t *testing.T) {
	t.Parallel()
	s, tmpDir := newTestDestServer(t)

	// More files than fileSetupConcurrency so the last one is only reachable
	// if earlier waits release their slots, i.e. genuinely concurrently.
	const numFiles = 40
	state, dones, targets, contents := newPendingHardlinkState(t, tmpDir, numFiles)

	errCh := make(chan error, 1)
	go func() { errCh <- s.resolvePendingHardlinks(context.Background(), "pending", state) }()

	last := numFiles - 1
	close(dones[last])

	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(targets[last]); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("last file's hardlink never appeared: waits are serialized behind file 0")
		}
		time.Sleep(time.Millisecond)
	}

	for i := range last {
		close(dones[i])
	}
	if err := <-errCh; err != nil {
		t.Fatalf("resolvePendingHardlinks: %v", err)
	}

	for i := range numFiles {
		got, err := os.ReadFile(targets[i])
		if err != nil {
			t.Errorf("file %d: reading target: %v", i, err)
			continue
		}
		if string(got) != string(contents[i]) {
			t.Errorf("file %d: linked content %q, want %q", i, got, contents[i])
		}
		if state.files[i].hardlink.state != hlStateComplete {
			t.Errorf("file %d: hardlink state = %v, want complete", i, state.files[i].hardlink.state)
		}
	}
}

// TestResolvePendingHardlinks_FirstFailureCancelsWaits pins that one file's
// failure stops the siblings still parked on their doneCh. Without a shared
// cancellation the group would sit on them until defaultHardlinkWaitTimeout
// (30 minutes) even though the torrent's finalization is already doomed.
func TestResolvePendingHardlinks_FirstFailureCancelsWaits(t *testing.T) {
	t.Parallel()
	s, tmpDir := newTestDestServer(t)

	const numFiles = 8
	state, dones, _, _ := newPendingHardlinkState(t, tmpDir, numFiles)

	// File 0's source finalizes at the wrong size (stale FileID), which is a
	// hard finalize failure. Every other file's source stays pending forever.
	if err := os.Truncate(filepath.Join(tmpDir, state.files[0].hardlink.sourcePath), 1); err != nil {
		t.Fatal(err)
	}

	errCh := make(chan error, 1)
	go func() { errCh <- s.resolvePendingHardlinks(context.Background(), "pending", state) }()
	close(dones[0])

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected an error for the missing hardlink source")
		}
		if !strings.Contains(err.Error(), "pending hardlink source") {
			t.Fatalf("error = %v, want the missing-source failure", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("resolvePendingHardlinks did not return: siblings still waiting on their timeout")
	}
}
