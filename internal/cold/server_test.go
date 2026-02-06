package cold

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/arsac/qb-sync/proto"
)

func TestServerConfig_GetSavePath(t *testing.T) {
	tests := []struct {
		name     string
		config   ServerConfig
		expected string
	}{
		{
			name:     "returns SavePath when set",
			config:   ServerConfig{BasePath: "/data/cold", SavePath: "/downloads"},
			expected: "/downloads",
		},
		{
			name:     "falls back to BasePath when SavePath empty",
			config:   ServerConfig{BasePath: "/data/cold"},
			expected: "/data/cold",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.config.GetSavePath()
			if got != tt.expected {
				t.Errorf("GetSavePath() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestServerConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  ServerConfig
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid config",
			config: ServerConfig{
				BasePath:   "/tmp/test",
				ListenAddr: ":50051",
			},
			wantErr: false,
		},
		{
			name: "missing base path",
			config: ServerConfig{
				ListenAddr: ":50051",
			},
			wantErr: true,
			errMsg:  "base path is required",
		},
		{
			name: "missing listen address",
			config: ServerConfig{
				BasePath: "/tmp/test",
			},
			wantErr: true,
			errMsg:  "listen address is required",
		},
		{
			name: "orphan timeout too small",
			config: ServerConfig{
				BasePath:      "/tmp/test",
				ListenAddr:    ":50051",
				OrphanTimeout: 30 * time.Minute, // Less than minOrphanTimeout (1h)
			},
			wantErr: true,
			errMsg:  "orphan timeout must be at least",
		},
		{
			name: "orphan timeout at minimum",
			config: ServerConfig{
				BasePath:      "/tmp/test",
				ListenAddr:    ":50051",
				OrphanTimeout: minOrphanTimeout,
			},
			wantErr: false,
		},
		{
			name: "orphan timeout zero uses default",
			config: ServerConfig{
				BasePath:      "/tmp/test",
				ListenAddr:    ":50051",
				OrphanTimeout: 0, // Zero means use default
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr && tt.errMsg != "" && err != nil {
				if !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("Validate() error = %v, want error containing %q", err, tt.errMsg)
				}
			}
		})
	}
}

func TestIsOrphanedTorrent(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	t.Run("tracked torrent is not orphaned", func(t *testing.T) {
		tmpDir := t.TempDir()
		s := &Server{
			config:         ServerConfig{BasePath: tmpDir},
			logger:         logger,
			torrents:       make(map[string]*serverTorrentState),
			abortingHashes: make(map[string]chan struct{}),
		}

		hash := "abc123"
		s.torrents[hash] = &serverTorrentState{}

		// Create metadata even though it shouldn't be checked
		createTestMetadata(t, tmpDir, hash, time.Now().Add(-48*time.Hour))

		if s.isOrphanedTorrent(ctx, hash, 24*time.Hour) {
			t.Error("tracked torrent should not be orphaned")
		}
	})

	t.Run("untracked torrent with recent state file is not orphaned", func(t *testing.T) {
		tmpDir := t.TempDir()
		s := &Server{
			config:         ServerConfig{BasePath: tmpDir},
			logger:         logger,
			torrents:       make(map[string]*serverTorrentState),
			abortingHashes: make(map[string]chan struct{}),
		}

		hash := "abc123"
		createTestStateFile(t, tmpDir, hash, time.Now().Add(-1*time.Hour))

		if s.isOrphanedTorrent(ctx, hash, 24*time.Hour) {
			t.Error("torrent with recent state file should not be orphaned")
		}
	})

	t.Run("untracked torrent with old state file is orphaned", func(t *testing.T) {
		tmpDir := t.TempDir()
		s := &Server{
			config:         ServerConfig{BasePath: tmpDir},
			logger:         logger,
			torrents:       make(map[string]*serverTorrentState),
			abortingHashes: make(map[string]chan struct{}),
		}

		hash := "abc123"
		createTestStateFile(t, tmpDir, hash, time.Now().Add(-48*time.Hour))

		if !s.isOrphanedTorrent(ctx, hash, 24*time.Hour) {
			t.Error("torrent with old state file should be orphaned")
		}
	})

	t.Run("falls back to files.json when state file missing", func(t *testing.T) {
		tmpDir := t.TempDir()
		s := &Server{
			config:         ServerConfig{BasePath: tmpDir},
			logger:         logger,
			torrents:       make(map[string]*serverTorrentState),
			abortingHashes: make(map[string]chan struct{}),
		}

		hash := "abc123"
		// Create only files.json (no .state file)
		createTestFilesInfo(t, tmpDir, hash, time.Now().Add(-48*time.Hour))

		if !s.isOrphanedTorrent(ctx, hash, 24*time.Hour) {
			t.Error("torrent with old files.json should be orphaned")
		}
	})

	t.Run("no metadata returns not orphaned", func(t *testing.T) {
		tmpDir := t.TempDir()
		s := &Server{
			config:         ServerConfig{BasePath: tmpDir},
			logger:         logger,
			torrents:       make(map[string]*serverTorrentState),
			abortingHashes: make(map[string]chan struct{}),
		}

		hash := "nonexistent"

		if s.isOrphanedTorrent(ctx, hash, 24*time.Hour) {
			t.Error("torrent with no metadata should not be marked orphaned")
		}
	})
}

//nolint:gocognit // Test functions have inherent complexity from setup and assertions
func TestCleanupOrphan(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	t.Run("skips cleanup if torrent becomes tracked", func(t *testing.T) {
		tmpDir := t.TempDir()
		s := &Server{
			config:         ServerConfig{BasePath: tmpDir},
			logger:         logger,
			torrents:       make(map[string]*serverTorrentState),
			abortingHashes: make(map[string]chan struct{}),
		}

		hash := "abc123"
		metaDir := filepath.Join(tmpDir, metaDirName, hash)
		partialFile := filepath.Join(tmpDir, "test.txt.partial")

		// Create metadata and partial file
		createTestFilesInfoWithPaths(t, tmpDir, hash, []string{partialFile})
		if err := os.WriteFile(partialFile, []byte("test"), 0o644); err != nil {
			t.Fatal(err)
		}

		// Add to tracked torrents (simulating race with InitTorrent)
		s.torrents[hash] = &serverTorrentState{}

		s.cleanupOrphan(ctx, hash)

		// Files should still exist
		if _, err := os.Stat(partialFile); os.IsNotExist(err) {
			t.Error("partial file should not be deleted when torrent is tracked")
		}
		if _, err := os.Stat(metaDir); os.IsNotExist(err) {
			t.Error("meta directory should not be deleted when torrent is tracked")
		}
	})

	t.Run("deletes partial files and metadata", func(t *testing.T) {
		tmpDir := t.TempDir()
		s := &Server{
			config:         ServerConfig{BasePath: tmpDir},
			logger:         logger,
			torrents:       make(map[string]*serverTorrentState),
			abortingHashes: make(map[string]chan struct{}),
		}

		hash := "abc123"
		partialFile := filepath.Join(tmpDir, "data", "test.txt.partial")

		// Create directory structure
		if err := os.MkdirAll(filepath.Dir(partialFile), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(partialFile, []byte("test data"), 0o644); err != nil {
			t.Fatal(err)
		}

		createTestFilesInfoWithPaths(t, tmpDir, hash, []string{partialFile})

		s.cleanupOrphan(ctx, hash)

		// Partial file should be deleted
		if _, err := os.Stat(partialFile); !os.IsNotExist(err) {
			t.Error("partial file should be deleted")
		}

		// Meta directory should be deleted
		metaDir := filepath.Join(tmpDir, metaDirName, hash)
		if _, err := os.Stat(metaDir); !os.IsNotExist(err) {
			t.Error("meta directory should be deleted")
		}
	})

	t.Run("also deletes non-partial version of files", func(t *testing.T) {
		tmpDir := t.TempDir()
		s := &Server{
			config:         ServerConfig{BasePath: tmpDir},
			logger:         logger,
			torrents:       make(map[string]*serverTorrentState),
			abortingHashes: make(map[string]chan struct{}),
		}

		hash := "abc123"
		partialFile := filepath.Join(tmpDir, "data", "test.txt.partial")
		finalFile := filepath.Join(tmpDir, "data", "test.txt")

		// Create both partial and final versions
		if err := os.MkdirAll(filepath.Dir(partialFile), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(partialFile, []byte("partial"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(finalFile, []byte("final"), 0o644); err != nil {
			t.Fatal(err)
		}

		createTestFilesInfoWithPaths(t, tmpDir, hash, []string{partialFile})

		s.cleanupOrphan(ctx, hash)

		// Both files should be deleted
		if _, err := os.Stat(partialFile); !os.IsNotExist(err) {
			t.Error("partial file should be deleted")
		}
		if _, err := os.Stat(finalFile); !os.IsNotExist(err) {
			t.Error("final file should be deleted")
		}
	})

	t.Run("cleans up metadata directory even when files.json is unreadable", func(t *testing.T) {
		tmpDir := t.TempDir()
		s := &Server{
			config:         ServerConfig{BasePath: tmpDir},
			logger:         logger,
			torrents:       make(map[string]*serverTorrentState),
			abortingHashes: make(map[string]chan struct{}),
		}

		hash := "abc123"
		metaDir := filepath.Join(tmpDir, metaDirName, hash)

		// Create meta dir with invalid files.json
		if err := os.MkdirAll(metaDir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(metaDir, filesInfoFileName), []byte("invalid json"), 0o644); err != nil {
			t.Fatal(err)
		}

		s.cleanupOrphan(ctx, hash)

		// Meta directory should be deleted to prevent unbounded growth.
		// Partial files can't be located without valid files.json but are
		// identifiable by .partial suffix for manual cleanup.
		if _, err := os.Stat(metaDir); !os.IsNotExist(err) {
			t.Error("meta directory should be deleted even when files.json is unreadable")
		}
	})
}

func TestCleanupOrphanedTorrents(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	t.Run("scans meta directory and cleans orphans", func(t *testing.T) {
		tmpDir := t.TempDir()
		s := &Server{
			config: ServerConfig{
				BasePath:      tmpDir,
				OrphanTimeout: 1 * time.Hour,
			},
			logger:         logger,
			torrents:       make(map[string]*serverTorrentState),
			abortingHashes: make(map[string]chan struct{}),
		}

		// Create an orphaned torrent (old metadata)
		orphanHash := "orphan123"
		orphanPartial := filepath.Join(tmpDir, "orphan.partial")
		if err := os.MkdirAll(filepath.Dir(orphanPartial), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(orphanPartial, []byte("orphan data"), 0o644); err != nil {
			t.Fatal(err)
		}
		createTestFilesInfoWithPaths(t, tmpDir, orphanHash, []string{orphanPartial})
		// Backdate the metadata
		metaDir := filepath.Join(tmpDir, metaDirName, orphanHash)
		setModTime(t, filepath.Join(metaDir, filesInfoFileName), time.Now().Add(-2*time.Hour))

		// Create a fresh torrent (recent metadata)
		freshHash := "fresh456"
		freshPartial := filepath.Join(tmpDir, "fresh.partial")
		if err := os.WriteFile(freshPartial, []byte("fresh data"), 0o644); err != nil {
			t.Fatal(err)
		}
		createTestFilesInfoWithPaths(t, tmpDir, freshHash, []string{freshPartial})

		s.cleanupOrphanedTorrents(ctx)

		// Orphaned torrent should be cleaned up
		if _, err := os.Stat(orphanPartial); !os.IsNotExist(err) {
			t.Error("orphan partial file should be deleted")
		}
		if _, err := os.Stat(filepath.Join(tmpDir, metaDirName, orphanHash)); !os.IsNotExist(err) {
			t.Error("orphan meta directory should be deleted")
		}

		// Fresh torrent should remain
		if _, err := os.Stat(freshPartial); os.IsNotExist(err) {
			t.Error("fresh partial file should not be deleted")
		}
		if _, err := os.Stat(filepath.Join(tmpDir, metaDirName, freshHash)); os.IsNotExist(err) {
			t.Error("fresh meta directory should not be deleted")
		}
	})

	t.Run("handles missing meta directory", func(t *testing.T) {
		tmpDir := t.TempDir()
		s := &Server{
			config:         ServerConfig{BasePath: tmpDir},
			logger:         logger,
			torrents:       make(map[string]*serverTorrentState),
			abortingHashes: make(map[string]chan struct{}),
		}

		// Should not panic when meta directory doesn't exist
		s.cleanupOrphanedTorrents(ctx)
	})

	t.Run("skips non-directory entries", func(t *testing.T) {
		tmpDir := t.TempDir()
		s := &Server{
			config: ServerConfig{
				BasePath:      tmpDir,
				OrphanTimeout: 1 * time.Hour,
			},
			logger:   logger,
			torrents: make(map[string]*serverTorrentState),
		}

		// Create meta directory with a regular file
		metaDir := filepath.Join(tmpDir, metaDirName)
		if err := os.MkdirAll(metaDir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(metaDir, "somefile.txt"), []byte("data"), 0o644); err != nil {
			t.Fatal(err)
		}

		// Should not panic and file should remain
		s.cleanupOrphanedTorrents(ctx)

		if _, err := os.Stat(filepath.Join(metaDir, "somefile.txt")); os.IsNotExist(err) {
			t.Error("regular file in meta directory should not be deleted")
		}
	})
}

//nolint:gocognit,gocyclo,cyclop // Test functions have inherent complexity from setup and assertions
func TestAbortTorrent(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	t.Run("returns success for non-existent torrent", func(t *testing.T) {
		tmpDir := t.TempDir()
		s := &Server{
			config:         ServerConfig{BasePath: tmpDir},
			logger:         logger,
			torrents:       make(map[string]*serverTorrentState),
			abortingHashes: make(map[string]chan struct{}),
		}

		resp, err := s.AbortTorrent(ctx, &pb.AbortTorrentRequest{
			TorrentHash: "nonexistent",
			DeleteFiles: true,
		})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !resp.GetSuccess() {
			t.Error("expected success for non-existent torrent")
		}
		if resp.GetFilesDeleted() != 0 {
			t.Errorf("expected 0 files deleted, got %d", resp.GetFilesDeleted())
		}
	})

	t.Run("removes torrent from tracking", func(t *testing.T) {
		tmpDir := t.TempDir()
		s := &Server{
			config:         ServerConfig{BasePath: tmpDir},
			logger:         logger,
			torrents:       make(map[string]*serverTorrentState),
			abortingHashes: make(map[string]chan struct{}),
		}

		hash := "abc123"
		s.torrents[hash] = &serverTorrentState{
			files: []*serverFileInfo{},
		}

		_, err := s.AbortTorrent(ctx, &pb.AbortTorrentRequest{
			TorrentHash: hash,
			DeleteFiles: false,
		})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		s.mu.RLock()
		_, exists := s.torrents[hash]
		s.mu.RUnlock()

		if exists {
			t.Error("torrent should be removed from tracking")
		}
	})

	t.Run("deletes files when deleteFiles is true", func(t *testing.T) {
		tmpDir := t.TempDir()
		s := &Server{
			config:         ServerConfig{BasePath: tmpDir},
			logger:         logger,
			torrents:       make(map[string]*serverTorrentState),
			abortingHashes: make(map[string]chan struct{}),
		}

		hash := "abc123"
		partialFile := filepath.Join(tmpDir, "data", "test.partial")
		stateFile := filepath.Join(tmpDir, metaDirName, hash, ".state")
		torrentFile := filepath.Join(tmpDir, metaDirName, hash, "test.torrent")

		// Create files
		if err := os.MkdirAll(filepath.Dir(partialFile), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Dir(stateFile), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(partialFile, []byte("data"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(stateFile, []byte{1, 0, 1}, 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(torrentFile, []byte("torrent"), 0o644); err != nil {
			t.Fatal(err)
		}

		s.torrents[hash] = &serverTorrentState{
			files: []*serverFileInfo{
				{path: partialFile, size: 4},
			},
			statePath:   stateFile,
			torrentPath: torrentFile,
		}

		resp, err := s.AbortTorrent(ctx, &pb.AbortTorrentRequest{
			TorrentHash: hash,
			DeleteFiles: true,
		})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !resp.GetSuccess() {
			t.Errorf("expected success, got error: %s", resp.GetError())
		}
		if resp.GetFilesDeleted() != 1 {
			t.Errorf("expected 1 file deleted, got %d", resp.GetFilesDeleted())
		}

		// Verify files are deleted
		if _, statErr := os.Stat(partialFile); !os.IsNotExist(statErr) {
			t.Error("partial file should be deleted")
		}
		if _, statErr := os.Stat(stateFile); !os.IsNotExist(statErr) {
			t.Error("state file should be deleted")
		}
		if _, statErr := os.Stat(torrentFile); !os.IsNotExist(statErr) {
			t.Error("torrent file should be deleted")
		}
	})

	t.Run("does not delete files when deleteFiles is false", func(t *testing.T) {
		tmpDir := t.TempDir()
		s := &Server{
			config:         ServerConfig{BasePath: tmpDir},
			logger:         logger,
			torrents:       make(map[string]*serverTorrentState),
			abortingHashes: make(map[string]chan struct{}),
		}

		hash := "abc123"
		partialFile := filepath.Join(tmpDir, "data", "test.partial")

		// Create file
		if err := os.MkdirAll(filepath.Dir(partialFile), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(partialFile, []byte("data"), 0o644); err != nil {
			t.Fatal(err)
		}

		s.torrents[hash] = &serverTorrentState{
			files: []*serverFileInfo{
				{path: partialFile, size: 4},
			},
		}

		resp, err := s.AbortTorrent(ctx, &pb.AbortTorrentRequest{
			TorrentHash: hash,
			DeleteFiles: false,
		})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !resp.GetSuccess() {
			t.Error("expected success")
		}
		if resp.GetFilesDeleted() != 0 {
			t.Errorf("expected 0 files deleted, got %d", resp.GetFilesDeleted())
		}

		// File should still exist
		if _, statErr := os.Stat(partialFile); os.IsNotExist(statErr) {
			t.Error("partial file should not be deleted")
		}
	})

	t.Run("closes open file handles before deletion", func(t *testing.T) {
		tmpDir := t.TempDir()
		s := &Server{
			config:         ServerConfig{BasePath: tmpDir},
			logger:         logger,
			torrents:       make(map[string]*serverTorrentState),
			abortingHashes: make(map[string]chan struct{}),
		}

		hash := "abc123"
		partialFile := filepath.Join(tmpDir, "data", "test.partial")

		// Create and open file
		if err := os.MkdirAll(filepath.Dir(partialFile), 0o755); err != nil {
			t.Fatal(err)
		}
		f, err := os.Create(partialFile)
		if err != nil {
			t.Fatal(err)
		}

		s.torrents[hash] = &serverTorrentState{
			files: []*serverFileInfo{
				{path: partialFile, size: 0, file: f},
			},
		}

		resp, err := s.AbortTorrent(ctx, &pb.AbortTorrentRequest{
			TorrentHash: hash,
			DeleteFiles: true,
		})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !resp.GetSuccess() {
			t.Errorf("expected success, got error: %s", resp.GetError())
		}

		// File should be deleted (would fail if still open on Windows)
		if _, statErr := os.Stat(partialFile); !os.IsNotExist(statErr) {
			t.Error("partial file should be deleted")
		}
	})

	t.Run("handles concurrent abort requests", func(t *testing.T) {
		tmpDir := t.TempDir()
		s := &Server{
			config:         ServerConfig{BasePath: tmpDir},
			logger:         logger,
			torrents:       make(map[string]*serverTorrentState),
			abortingHashes: make(map[string]chan struct{}),
		}

		hash := "abc123"
		s.torrents[hash] = &serverTorrentState{
			files: []*serverFileInfo{},
		}

		var wg sync.WaitGroup
		results := make(chan *pb.AbortTorrentResponse, 10)

		for range 10 {
			wg.Go(func() {
				resp, err := s.AbortTorrent(ctx, &pb.AbortTorrentRequest{
					TorrentHash: hash,
					DeleteFiles: false,
				})
				if err != nil {
					t.Errorf("unexpected error: %v", err)
					return
				}
				results <- resp
			})
		}

		wg.Wait()
		close(results)

		// All should succeed
		for resp := range results {
			if !resp.GetSuccess() {
				t.Error("expected all abort requests to succeed")
			}
		}

		// Torrent should be removed
		s.mu.RLock()
		_, exists := s.torrents[hash]
		s.mu.RUnlock()

		if exists {
			t.Error("torrent should be removed after concurrent aborts")
		}
	})

	t.Run("InitTorrent waits for AbortTorrent to complete", func(t *testing.T) {
		tmpDir := t.TempDir()
		s := &Server{
			config:         ServerConfig{BasePath: tmpDir},
			logger:         logger,
			torrents:       make(map[string]*serverTorrentState),
			abortingHashes: make(map[string]chan struct{}),
		}

		hash := "racetest123"
		partialPath := filepath.Join(tmpDir, "test.mp4.partial")

		// Create a partial file that AbortTorrent will delete
		if err := os.WriteFile(partialPath, []byte("test data"), 0o644); err != nil {
			t.Fatalf("failed to create partial file: %v", err)
		}

		s.torrents[hash] = &serverTorrentState{
			files: []*serverFileInfo{
				{path: partialPath, size: 9},
			},
		}

		// Track timing to verify wait behavior
		var abortFinished, initFinished time.Time
		var initErr error
		abortStartedCh := make(chan struct{})

		var wg sync.WaitGroup

		// Start AbortTorrent first
		wg.Go(func() {
			// Manually register the abort before signaling and sleeping.
			// This simulates the behavior where AbortTorrent takes a long time.
			s.mu.Lock()
			abortCh := make(chan struct{})
			s.abortingHashes[hash] = abortCh
			state := s.torrents[hash]
			delete(s.torrents, hash)
			s.mu.Unlock()

			close(abortStartedCh) // Signal that abort has registered

			// Simulate slow cleanup
			time.Sleep(50 * time.Millisecond)

			// Clean up files
			state.mu.Lock()
			for _, fi := range state.files {
				_ = os.Remove(fi.path)
			}
			state.mu.Unlock()

			// Clean up abort tracking
			s.mu.Lock()
			delete(s.abortingHashes, hash)
			s.mu.Unlock()
			close(abortCh)

			abortFinished = time.Now()
		})

		// Wait for AbortTorrent to register before starting InitTorrent
		<-abortStartedCh

		// Start InitTorrent - it should wait for AbortTorrent to complete
		wg.Go(func() {
			_, initErr = s.InitTorrent(ctx, &pb.InitTorrentRequest{
				TorrentHash: hash,
				Name:        "test",
				NumPieces:   1,
				PieceSize:   1024,
				TotalSize:   9,
				Files: []*pb.FileInfo{
					{Path: "test.mp4", Size: 9, Offset: 0},
				},
			})
			initFinished = time.Now()
		})

		wg.Wait()

		if initErr != nil {
			t.Fatalf("InitTorrent failed: %v", initErr)
		}

		// InitTorrent should have finished after AbortTorrent finished
		// (because it waited for abort to complete)
		if initFinished.Before(abortFinished) {
			t.Errorf("InitTorrent finished before AbortTorrent (abortFinished: %v, initFinished: %v)",
				abortFinished, initFinished)
		}

		// The torrent should now be tracked (InitTorrent succeeded after abort)
		s.mu.RLock()
		_, exists := s.torrents[hash]
		s.mu.RUnlock()

		if !exists {
			t.Error("torrent should be tracked after InitTorrent")
		}
	})
}

// Helper functions

func createTestMetadata(t *testing.T, basePath, hash string, modTime time.Time) {
	t.Helper()
	createTestStateFile(t, basePath, hash, modTime)
	createTestFilesInfo(t, basePath, hash, modTime)
}

func createTestStateFile(t *testing.T, basePath, hash string, modTime time.Time) {
	t.Helper()
	metaDir := filepath.Join(basePath, metaDirName, hash)
	if err := os.MkdirAll(metaDir, 0o755); err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(metaDir, ".state")
	if err := os.WriteFile(statePath, []byte{1, 0, 1}, 0o644); err != nil {
		t.Fatal(err)
	}
	setModTime(t, statePath, modTime)
}

func createTestFilesInfo(t *testing.T, basePath, hash string, modTime time.Time) {
	t.Helper()
	createTestFilesInfoWithPaths(t, basePath, hash, []string{})
	filesPath := filepath.Join(basePath, metaDirName, hash, filesInfoFileName)
	setModTime(t, filesPath, modTime)
}

func createTestFilesInfoWithPaths(t *testing.T, basePath, hash string, filePaths []string) {
	t.Helper()
	metaDir := filepath.Join(basePath, metaDirName, hash)
	if err := os.MkdirAll(metaDir, 0o755); err != nil {
		t.Fatal(err)
	}

	files := make([]persistedFileInfo, len(filePaths))
	for i, path := range filePaths {
		files[i] = persistedFileInfo{
			Path:   path,
			Size:   1024,
			Offset: int64(i) * 1024,
		}
	}

	info := persistedTorrentInfo{
		Name:        "test",
		NumPieces:   10,
		PieceLength: 1024,
		TotalSize:   10240,
		Files:       files,
	}

	data, marshalErr := json.Marshal(info)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}

	filesPath := filepath.Join(metaDir, filesInfoFileName)
	if writeErr := os.WriteFile(filesPath, data, 0o644); writeErr != nil {
		t.Fatal(writeErr)
	}
}

func setModTime(t *testing.T, path string, modTime time.Time) {
	t.Helper()
	if err := os.Chtimes(path, modTime, modTime); err != nil {
		t.Fatal(err)
	}
}
