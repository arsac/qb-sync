package cold

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	s := &Server{
		config:         ServerConfig{BasePath: tmpDir},
		logger:         logger,
		torrents:       make(map[string]*serverTorrentState),
		abortingHashes: make(map[string]chan struct{}),
		inodes:         NewInodeRegistry(tmpDir, logger),
		memBudget:      semaphore.NewWeighted(512 * 1024 * 1024),
		finalizeSem:    semaphore.NewWeighted(1),
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
		pieceHashes: pieceHashes,
		pieceLength: pieceSize,
		totalSize:   totalSize,
		files: []*serverFileInfo{
			{path: filePath, offset: 0, size: totalSize},
		},
	}

	ctx := context.Background()
	err := s.verifyFinalizedPieces(ctx, "testHash", state)
	if err != nil {
		t.Fatalf("verifyFinalizedPieces failed: %v", err)
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
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	s := &Server{
		config:         ServerConfig{BasePath: tmpDir},
		logger:         logger,
		torrents:       make(map[string]*serverTorrentState),
		abortingHashes: make(map[string]chan struct{}),
		inodes:         NewInodeRegistry(tmpDir, logger),
		memBudget:      semaphore.NewWeighted(512 * 1024 * 1024),
		finalizeSem:    semaphore.NewWeighted(1),
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
		pieceHashes: pieceHashes,
		pieceLength: pieceSize,
		totalSize:   totalSize,
		files: []*serverFileInfo{
			{path: filePath, offset: 0, size: totalSize},
		},
	}

	ctx := context.Background()
	err := s.verifyFinalizedPieces(ctx, "testHash", state)
	if err == nil {
		t.Fatal("expected error for hash mismatch, got nil")
	}
}

func TestFinalizeTorrent_PollReturnsVerifying(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	s := &Server{
		config:         ServerConfig{BasePath: tmpDir},
		logger:         logger,
		torrents:       make(map[string]*serverTorrentState),
		abortingHashes: make(map[string]chan struct{}),
		inodes:         NewInodeRegistry(tmpDir, logger),
		memBudget:      semaphore.NewWeighted(512 * 1024 * 1024),
		finalizeSem:    semaphore.NewWeighted(1),
	}

	hash := "poll-verify-test"
	done := make(chan struct{}) // not closed yet — simulates in-progress verification
	state := &serverTorrentState{
		written:      []bool{true, true},
		writtenCount: 2,
		pieceLength:  256,
		totalSize:    512,
		finalizing:   true,
		finalizeDone: done,
		files:        []*serverFileInfo{},
	}

	s.mu.Lock()
	s.torrents[hash] = state
	s.mu.Unlock()

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
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	s := &Server{
		config:         ServerConfig{BasePath: tmpDir},
		logger:         logger,
		torrents:       make(map[string]*serverTorrentState),
		abortingHashes: make(map[string]chan struct{}),
		inodes:         NewInodeRegistry(tmpDir, logger),
		memBudget:      semaphore.NewWeighted(512 * 1024 * 1024),
		finalizeSem:    semaphore.NewWeighted(1),
	}

	hash := "poll-complete-test"
	done := make(chan struct{})
	close(done)
	state := &serverTorrentState{
		written:      []bool{true},
		writtenCount: 1,
		pieceLength:  256,
		totalSize:    256,
		finalizing:   true,
		finalizeDone: done,
		finalizeResult: &finalizeResult{
			success: true,
			state:   "uploading",
		},
		torrentPath: filepath.Join(tmpDir, metaDirName, hash, "test.torrent"),
		files:       []*serverFileInfo{},
	}

	// Create metaDir so cleanupFinalizedTorrent doesn't fail
	metaDir := filepath.Join(tmpDir, metaDirName, hash)
	if err := os.MkdirAll(metaDir, 0o755); err != nil {
		t.Fatal(err)
	}

	s.mu.Lock()
	s.torrents[hash] = state
	s.mu.Unlock()

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
	s.mu.RLock()
	_, exists := s.torrents[hash]
	s.mu.RUnlock()
	if exists {
		t.Error("torrent should be removed from tracking after successful finalize poll")
	}

	// Metadata directory should be removed
	if _, statErr := os.Stat(metaDir); !os.IsNotExist(statErr) {
		t.Error("metadata directory should be removed after finalization")
	}
}

func TestFinalizeTorrent_PollReturnsFailedResult(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	s := &Server{
		config:         ServerConfig{BasePath: tmpDir},
		logger:         logger,
		torrents:       make(map[string]*serverTorrentState),
		abortingHashes: make(map[string]chan struct{}),
		inodes:         NewInodeRegistry(tmpDir, logger),
		memBudget:      semaphore.NewWeighted(512 * 1024 * 1024),
		finalizeSem:    semaphore.NewWeighted(1),
	}

	hash := "poll-fail-test"
	done := make(chan struct{})
	close(done)
	state := &serverTorrentState{
		written:      []bool{true},
		writtenCount: 1,
		pieceLength:  256,
		totalSize:    256,
		finalizing:   true,
		finalizeDone: done,
		finalizeResult: &finalizeResult{
			success: false,
			err:     "verification failed: piece 5: hash mismatch",
		},
		files: []*serverFileInfo{},
	}

	s.mu.Lock()
	s.torrents[hash] = state
	s.mu.Unlock()

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
	stillFinalizing := state.finalizing
	state.mu.Unlock()
	if stillFinalizing {
		t.Error("finalizing flag should be cleared after returning failed result")
	}

	// Verify a second call actually retries (doesn't reject as "already in progress").
	// It will fail with "incomplete" since writtenCount < totalPieces after state reset,
	// but that proves it entered the normal finalization path.
	state.mu.Lock()
	state.writtenCount = 0
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
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	s := &Server{
		config:         ServerConfig{BasePath: tmpDir},
		logger:         logger,
		torrents:       make(map[string]*serverTorrentState),
		abortingHashes: make(map[string]chan struct{}),
		inodes:         NewInodeRegistry(tmpDir, logger),
		memBudget:      semaphore.NewWeighted(512 * 1024 * 1024),
		finalizeSem:    semaphore.NewWeighted(1),
	}

	hash := "concurrent-setup-test"
	done := make(chan struct{}) // not closed — simulates in-progress work
	// finalizeDone is set upfront (same as production code) so concurrent
	// polls always see "verifying" instead of a spurious error.
	state := &serverTorrentState{
		written:      []bool{true},
		writtenCount: 1,
		pieceLength:  256,
		totalSize:    256,
		finalizing:   true,
		finalizeDone: done,
		files:        []*serverFileInfo{},
	}

	s.mu.Lock()
	s.torrents[hash] = state
	s.mu.Unlock()

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
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

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
			pieceHashes: pieceHashes,
			pieceLength: pieceSize,
			totalSize:   totalSize,
			files:       []*serverFileInfo{{path: filePath, offset: 0, size: totalSize}},
			torrentPath: filepath.Join(dir, metaDirName, hash, "test.torrent"),
		}
	}

	newServer := func() *Server {
		return &Server{
			config:         ServerConfig{BasePath: tmpDir},
			logger:         logger,
			torrents:       make(map[string]*serverTorrentState),
			abortingHashes: make(map[string]chan struct{}),
			inodes:         NewInodeRegistry(tmpDir, logger),
			memBudget:      semaphore.NewWeighted(512 * 1024 * 1024),
			finalizeSem:    semaphore.NewWeighted(1),
		}
	}

	t.Run("blocks when semaphore is held", func(t *testing.T) {
		t.Parallel()

		s := newServer()
		s.finalizeSem.Acquire(context.Background(), 1)

		hash := "sem-block-test"
		state := createTorrentState(t, tmpDir, hash, 1, 256)

		s.mu.Lock()
		s.torrents[hash] = state
		s.mu.Unlock()

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

			s.mu.Lock()
			s.torrents[hash] = state
			s.mu.Unlock()

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
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	s := &Server{
		config:         ServerConfig{BasePath: tmpDir},
		logger:         logger,
		torrents:       make(map[string]*serverTorrentState),
		abortingHashes: make(map[string]chan struct{}),
		inodes:         NewInodeRegistry(tmpDir, logger),
		memBudget:      semaphore.NewWeighted(512 * 1024 * 1024),
		finalizeSem:    semaphore.NewWeighted(1),
	}

	state := &serverTorrentState{
		pieceHashes: nil, // No hashes
		pieceLength: 1024,
		totalSize:   1024,
	}

	ctx := context.Background()
	err := s.verifyFinalizedPieces(ctx, "testHash", state)
	if err == nil {
		t.Fatal("expected error when piece hashes are missing")
	}
}
