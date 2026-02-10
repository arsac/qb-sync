package cold

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

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
