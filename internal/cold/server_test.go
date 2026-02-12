package cold

import (
	"bytes"
	"context"
	"crypto/sha1"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/autobrr/go-qbittorrent"

	pb "github.com/arsac/qb-sync/proto"
)

func TestNewServer_MemBudget(t *testing.T) {
	t.Parallel()
	logger := testLogger(t)

	t.Run("uses default when MaxStreamBufferBytes is zero", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		s := NewServer(ServerConfig{
			BasePath:   tmpDir,
			ListenAddr: ":50051",
		}, logger)

		// Default is 512 MB. We should be able to acquire that amount.
		budget := int64(defaultMaxStreamBufferMB) * 1024 * 1024
		if !s.memBudget.TryAcquire(budget) {
			t.Fatal("should be able to acquire full default budget")
		}
		// Should not be able to acquire any more
		if s.memBudget.TryAcquire(1) {
			t.Fatal("should not be able to acquire beyond budget")
		}
		s.memBudget.Release(budget)
	})

	t.Run("uses custom MaxStreamBufferBytes", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		customBytes := int64(64 * 1024 * 1024) // 64 MB
		s := NewServer(ServerConfig{
			BasePath:             tmpDir,
			ListenAddr:           ":50051",
			MaxStreamBufferBytes: customBytes,
		}, logger)

		if !s.memBudget.TryAcquire(customBytes) {
			t.Fatal("should be able to acquire full custom budget")
		}
		if s.memBudget.TryAcquire(1) {
			t.Fatal("should not be able to acquire beyond custom budget")
		}
		s.memBudget.Release(customBytes)
	})

	t.Run("uses default when MaxStreamBufferBytes is negative", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		s := NewServer(ServerConfig{
			BasePath:             tmpDir,
			ListenAddr:           ":50051",
			MaxStreamBufferBytes: -1,
		}, logger)

		budget := int64(defaultMaxStreamBufferMB) * 1024 * 1024
		if !s.memBudget.TryAcquire(budget) {
			t.Fatal("should fall back to default budget when negative")
		}
		s.memBudget.Release(budget)
	})
}

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
	logger := testLogger(t)

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

	t.Run("falls back to torrent file when state file missing", func(t *testing.T) {
		tmpDir := t.TempDir()
		s := &Server{
			config:         ServerConfig{BasePath: tmpDir},
			logger:         logger,
			torrents:       make(map[string]*serverTorrentState),
			abortingHashes: make(map[string]chan struct{}),
		}

		hash := "abc123"
		// Create only .torrent file (no .state file)
		createTestTorrentFile(t, tmpDir, hash, time.Now().Add(-48*time.Hour))

		if !s.isOrphanedTorrent(ctx, hash, 24*time.Hour) {
			t.Error("torrent with old .torrent file should be orphaned")
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

func TestCleanupOrphan(t *testing.T) {
	ctx := context.Background()
	logger := testLogger(t)

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
		partialFile := filepath.Join(tmpDir, "test", "test.txt.partial")

		// Create metadata and partial file
		createTestTorrentFileWithPaths(t, tmpDir, hash, []string{"test.txt"})
		if err := os.MkdirAll(filepath.Dir(partialFile), 0o755); err != nil {
			t.Fatal(err)
		}
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
		partialFile := filepath.Join(tmpDir, "test", "data", "test.txt.partial")

		// Create directory structure
		if err := os.MkdirAll(filepath.Dir(partialFile), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(partialFile, []byte("test data"), 0o644); err != nil {
			t.Fatal(err)
		}

		createTestTorrentFileWithPaths(t, tmpDir, hash, []string{"data/test.txt"})

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
		partialFile := filepath.Join(tmpDir, "test", "data", "test.txt.partial")
		finalFile := filepath.Join(tmpDir, "test", "data", "test.txt")

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

		createTestTorrentFileWithPaths(t, tmpDir, hash, []string{"data/test.txt"})

		s.cleanupOrphan(ctx, hash)

		// Both files should be deleted
		if _, err := os.Stat(partialFile); !os.IsNotExist(err) {
			t.Error("partial file should be deleted")
		}
		if _, err := os.Stat(finalFile); !os.IsNotExist(err) {
			t.Error("final file should be deleted")
		}
	})

	t.Run("cleans up metadata directory even when torrent file is missing", func(t *testing.T) {
		tmpDir := t.TempDir()
		s := &Server{
			config:         ServerConfig{BasePath: tmpDir},
			logger:         logger,
			torrents:       make(map[string]*serverTorrentState),
			abortingHashes: make(map[string]chan struct{}),
		}

		hash := "abc123"
		metaDir := filepath.Join(tmpDir, metaDirName, hash)

		// Create meta dir without a .torrent file
		if err := os.MkdirAll(metaDir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(metaDir, ".state"), []byte{1, 0}, 0o644); err != nil {
			t.Fatal(err)
		}

		s.cleanupOrphan(ctx, hash)

		// Meta directory should be deleted to prevent unbounded growth.
		// Partial files can't be located without a valid .torrent file but are
		// identifiable by .partial suffix for manual cleanup.
		if _, err := os.Stat(metaDir); !os.IsNotExist(err) {
			t.Error("meta directory should be deleted even when torrent file is missing")
		}
	})
}

func TestCleanupOrphanedTorrents(t *testing.T) {
	ctx := context.Background()
	logger := testLogger(t)

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
		orphanPartial := filepath.Join(tmpDir, "test", "orphan.partial")
		if err := os.MkdirAll(filepath.Dir(orphanPartial), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(orphanPartial, []byte("orphan data"), 0o644); err != nil {
			t.Fatal(err)
		}
		createTestTorrentFileWithPaths(t, tmpDir, orphanHash, []string{"orphan"})
		// Backdate the torrent file to make it appear orphaned
		metaDir := filepath.Join(tmpDir, metaDirName, orphanHash)
		torrentPath, _ := findTorrentFile(metaDir)
		setModTime(t, torrentPath, time.Now().Add(-2*time.Hour))

		// Create a fresh torrent (recent metadata)
		freshHash := "fresh456"
		freshPartial := filepath.Join(tmpDir, "test", "fresh.partial")
		if err := os.WriteFile(freshPartial, []byte("fresh data"), 0o644); err != nil {
			t.Fatal(err)
		}
		createTestTorrentFileWithPaths(t, tmpDir, freshHash, []string{"fresh"})

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

func newAbortTestServer(t *testing.T) *Server {
	t.Helper()
	logger := testLogger(t)
	return &Server{
		config:         ServerConfig{BasePath: t.TempDir()},
		logger:         logger,
		torrents:       make(map[string]*serverTorrentState),
		abortingHashes: make(map[string]chan struct{}),
	}
}

func TestAbortTorrent_NonExistent(t *testing.T) {
	s := newAbortTestServer(t)

	resp, err := s.AbortTorrent(context.Background(), &pb.AbortTorrentRequest{
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
}

func TestAbortTorrent_RemovesFromTracking(t *testing.T) {
	s := newAbortTestServer(t)
	hash := "abc123"
	s.torrents[hash] = &serverTorrentState{files: []*serverFileInfo{}}

	_, err := s.AbortTorrent(context.Background(), &pb.AbortTorrentRequest{
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
}

func TestAbortTorrent_DeletesFiles(t *testing.T) {
	s := newAbortTestServer(t)
	hash := "abc123"
	partialFile := filepath.Join(s.config.BasePath, "data", "test.partial")
	stateFile := filepath.Join(s.config.BasePath, metaDirName, hash, ".state")
	torrentFile := filepath.Join(s.config.BasePath, metaDirName, hash, "test.torrent")

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
		files:       []*serverFileInfo{{path: partialFile, size: 4, selected: true}},
		statePath:   stateFile,
		torrentPath: torrentFile,
	}

	resp, err := s.AbortTorrent(context.Background(), &pb.AbortTorrentRequest{
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

	if _, statErr := os.Stat(partialFile); !os.IsNotExist(statErr) {
		t.Error("partial file should be deleted")
	}
	if _, statErr := os.Stat(stateFile); !os.IsNotExist(statErr) {
		t.Error("state file should be deleted")
	}
	if _, statErr := os.Stat(torrentFile); !os.IsNotExist(statErr) {
		t.Error("torrent file should be deleted")
	}
}

func TestAbortTorrent_PreservesFiles(t *testing.T) {
	s := newAbortTestServer(t)
	hash := "abc123"
	partialFile := filepath.Join(s.config.BasePath, "data", "test.partial")

	if err := os.MkdirAll(filepath.Dir(partialFile), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(partialFile, []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}

	s.torrents[hash] = &serverTorrentState{
		files: []*serverFileInfo{{path: partialFile, size: 4, selected: true}},
	}

	resp, err := s.AbortTorrent(context.Background(), &pb.AbortTorrentRequest{
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

	if _, statErr := os.Stat(partialFile); os.IsNotExist(statErr) {
		t.Error("partial file should not be deleted")
	}
}

func TestAbortTorrent_ClosesFileHandles(t *testing.T) {
	s := newAbortTestServer(t)
	hash := "abc123"
	partialFile := filepath.Join(s.config.BasePath, "data", "test.partial")

	if err := os.MkdirAll(filepath.Dir(partialFile), 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(partialFile)
	if err != nil {
		t.Fatal(err)
	}

	s.torrents[hash] = &serverTorrentState{
		files: []*serverFileInfo{{path: partialFile, size: 0, file: f, selected: true}},
	}

	resp, abortErr := s.AbortTorrent(context.Background(), &pb.AbortTorrentRequest{
		TorrentHash: hash,
		DeleteFiles: true,
	})
	if abortErr != nil {
		t.Fatalf("unexpected error: %v", abortErr)
	}
	if !resp.GetSuccess() {
		t.Errorf("expected success, got error: %s", resp.GetError())
	}

	if _, statErr := os.Stat(partialFile); !os.IsNotExist(statErr) {
		t.Error("partial file should be deleted")
	}
}

func TestAbortTorrent_ConcurrentRequests(t *testing.T) {
	s := newAbortTestServer(t)
	ctx := context.Background()
	hash := "abc123"
	s.torrents[hash] = &serverTorrentState{files: []*serverFileInfo{}}

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

	for resp := range results {
		if !resp.GetSuccess() {
			t.Error("expected all abort requests to succeed")
		}
	}

	s.mu.RLock()
	_, exists := s.torrents[hash]
	s.mu.RUnlock()

	if exists {
		t.Error("torrent should be removed after concurrent aborts")
	}
}

func TestAbortTorrent_InitWaitsForAbort(t *testing.T) {
	s := newAbortTestServer(t)
	ctx := context.Background()
	hash := "racetest123"
	partialPath := filepath.Join(s.config.BasePath, "test.mp4.partial")

	if err := os.WriteFile(partialPath, []byte("test data"), 0o644); err != nil {
		t.Fatalf("failed to create partial file: %v", err)
	}

	s.torrents[hash] = &serverTorrentState{
		files: []*serverFileInfo{{path: partialPath, size: 9, selected: true}},
	}

	var abortFinished, initFinished time.Time
	var initErr error
	abortStartedCh := make(chan struct{})

	var wg sync.WaitGroup

	wg.Go(func() {
		s.mu.Lock()
		abortCh := make(chan struct{})
		s.abortingHashes[hash] = abortCh
		state := s.torrents[hash]
		delete(s.torrents, hash)
		s.mu.Unlock()

		close(abortStartedCh)

		time.Sleep(50 * time.Millisecond)

		state.mu.Lock()
		for _, fi := range state.files {
			_ = os.Remove(fi.path)
		}
		state.mu.Unlock()

		s.mu.Lock()
		delete(s.abortingHashes, hash)
		s.mu.Unlock()
		close(abortCh)

		abortFinished = time.Now()
	})

	<-abortStartedCh

	wg.Go(func() {
		_, initErr = s.InitTorrent(ctx, &pb.InitTorrentRequest{
			TorrentHash: hash,
			Name:        "test",
			NumPieces:   1,
			PieceSize:   1024,
			TotalSize:   9,
			Files: []*pb.FileInfo{
				{Path: "test.mp4", Size: 9, Offset: 0, Selected: true},
			},
		})
		initFinished = time.Now()
	})

	wg.Wait()

	if initErr != nil {
		t.Fatalf("InitTorrent failed: %v", initErr)
	}

	if initFinished.Before(abortFinished) {
		t.Errorf(
			"InitTorrent finished before AbortTorrent (abortFinished: %v, initFinished: %v)",
			abortFinished, initFinished,
		)
	}

	s.mu.RLock()
	_, exists := s.torrents[hash]
	s.mu.RUnlock()

	if !exists {
		t.Error("torrent should be tracked after InitTorrent")
	}
}

func TestSetupFile_PreExisting(t *testing.T) {
	ctx := context.Background()
	logger := testLogger(t)

	t.Run("detects pre-existing file with correct size", func(t *testing.T) {
		tmpDir := t.TempDir()
		s := &Server{
			config: ServerConfig{BasePath: tmpDir},
			logger: logger,
			inodes: NewInodeRegistry(tmpDir, logger),
		}

		// Create a file at the target path with the expected size
		filePath := "data/test.mp4"
		targetPath := filepath.Join(tmpDir, filePath)
		if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
			t.Fatal(err)
		}
		fileData := make([]byte, 1024)
		if err := os.WriteFile(targetPath, fileData, 0o644); err != nil {
			t.Fatal(err)
		}

		fileInfo, result, err := s.setupFile(ctx, "abc123", &pb.FileInfo{
			Path:     filePath,
			Size:     1024,
			Offset:   0,
			Inode:    12345,
			Selected: true,
		}, 0, "")

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if fileInfo.hlState != hlStateComplete {
			t.Errorf("expected hlStateComplete, got %v", fileInfo.hlState)
		}
		if fileInfo.path != targetPath {
			t.Errorf("expected path %q, got %q", targetPath, fileInfo.path)
		}
		if !result.GetPreExisting() {
			t.Error("expected PreExisting to be true")
		}
		if result.GetHardlinked() {
			t.Error("expected Hardlinked to be false for pre-existing files")
		}
	})

	t.Run("falls through when file exists with wrong size", func(t *testing.T) {
		tmpDir := t.TempDir()
		s := &Server{
			config: ServerConfig{BasePath: tmpDir},
			logger: logger,
			inodes: NewInodeRegistry(tmpDir, logger),
		}

		// Create a file with the wrong size
		filePath := "data/test.mp4"
		targetPath := filepath.Join(tmpDir, filePath)
		if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(targetPath, []byte("short"), 0o644); err != nil {
			t.Fatal(err)
		}

		fileInfo, result, err := s.setupFile(ctx, "abc123", &pb.FileInfo{
			Path:     filePath,
			Size:     1024,
			Offset:   0,
			Selected: true,
		}, 0, "")

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if fileInfo.hlState == hlStateComplete {
			t.Error("should not be hlStateComplete when size doesn't match")
		}
		if result.GetPreExisting() {
			t.Error("expected PreExisting to be false when size doesn't match")
		}
		if !strings.HasSuffix(fileInfo.path, ".partial") {
			t.Errorf("expected .partial path, got %q", fileInfo.path)
		}
	})

	t.Run("falls through when file does not exist", func(t *testing.T) {
		tmpDir := t.TempDir()
		s := &Server{
			config: ServerConfig{BasePath: tmpDir},
			logger: logger,
			inodes: NewInodeRegistry(tmpDir, logger),
		}

		fileInfo, result, err := s.setupFile(ctx, "abc123", &pb.FileInfo{
			Path:     "data/test.mp4",
			Size:     1024,
			Offset:   0,
			Selected: true,
		}, 0, "")

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if fileInfo.hlState == hlStateComplete {
			t.Error("should not be hlStateComplete when file doesn't exist")
		}
		if result.GetPreExisting() {
			t.Error("expected PreExisting to be false when file doesn't exist")
		}
		if !strings.HasSuffix(fileInfo.path, ".partial") {
			t.Errorf("expected .partial path, got %q", fileInfo.path)
		}
	})
}

func TestInitTorrent_PreExistingFiles(t *testing.T) {
	ctx := context.Background()
	logger := testLogger(t)

	t.Run("all files pre-existing yields zero pieces needed", func(t *testing.T) {
		tmpDir := t.TempDir()
		s := &Server{
			config:         ServerConfig{BasePath: tmpDir},
			logger:         logger,
			torrents:       make(map[string]*serverTorrentState),
			abortingHashes: make(map[string]chan struct{}),
			inodes:         NewInodeRegistry(tmpDir, logger),
		}

		// Pre-create both files at their final paths with correct sizes
		file1Path := filepath.Join(tmpDir, "data/file1.bin")
		file2Path := filepath.Join(tmpDir, "data/file2.bin")
		if err := os.MkdirAll(filepath.Join(tmpDir, "data"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(file1Path, make([]byte, 512), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(file2Path, make([]byte, 512), 0o644); err != nil {
			t.Fatal(err)
		}

		resp, err := s.InitTorrent(ctx, &pb.InitTorrentRequest{
			TorrentHash: "preexist1",
			Name:        "test-torrent",
			NumPieces:   2,
			PieceSize:   512,
			TotalSize:   1024,
			Files: []*pb.FileInfo{
				{Path: "data/file1.bin", Size: 512, Offset: 0, Selected: true},
				{Path: "data/file2.bin", Size: 512, Offset: 512, Selected: true},
			},
		})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !resp.GetSuccess() {
			t.Fatalf("expected success, got error: %s", resp.GetError())
		}
		if resp.GetPiecesNeededCount() != 0 {
			t.Errorf("expected 0 pieces needed, got %d", resp.GetPiecesNeededCount())
		}
		if resp.GetPiecesHaveCount() != 2 {
			t.Errorf("expected 2 pieces have, got %d", resp.GetPiecesHaveCount())
		}

		// Verify hardlink results report pre-existing
		for i, hr := range resp.GetHardlinkResults() {
			if !hr.GetPreExisting() {
				t.Errorf("file %d: expected PreExisting=true", i)
			}
		}
	})

	t.Run("mix of pre-existing and missing files", func(t *testing.T) {
		tmpDir := t.TempDir()
		s := &Server{
			config:         ServerConfig{BasePath: tmpDir},
			logger:         logger,
			torrents:       make(map[string]*serverTorrentState),
			abortingHashes: make(map[string]chan struct{}),
			inodes:         NewInodeRegistry(tmpDir, logger),
		}

		// Only pre-create the first file
		if err := os.MkdirAll(filepath.Join(tmpDir, "data"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(tmpDir, "data/file1.bin"), make([]byte, 512), 0o644); err != nil {
			t.Fatal(err)
		}
		// data/file2.bin intentionally not created

		resp, err := s.InitTorrent(ctx, &pb.InitTorrentRequest{
			TorrentHash: "preexist2",
			Name:        "test-torrent",
			NumPieces:   2,
			PieceSize:   512,
			TotalSize:   1024,
			Files: []*pb.FileInfo{
				{Path: "data/file1.bin", Size: 512, Offset: 0, Selected: true},
				{Path: "data/file2.bin", Size: 512, Offset: 512, Selected: true},
			},
		})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !resp.GetSuccess() {
			t.Fatalf("expected success, got error: %s", resp.GetError())
		}
		if resp.GetPiecesNeededCount() != 1 {
			t.Errorf("expected 1 piece needed, got %d", resp.GetPiecesNeededCount())
		}
		if resp.GetPiecesHaveCount() != 1 {
			t.Errorf("expected 1 piece have, got %d", resp.GetPiecesHaveCount())
		}

		results := resp.GetHardlinkResults()
		if !results[0].GetPreExisting() {
			t.Error("file 0: expected PreExisting=true")
		}
		if results[1].GetPreExisting() {
			t.Error("file 1: expected PreExisting=false")
		}
	})

	t.Run("wrong size file is not treated as pre-existing", func(t *testing.T) {
		tmpDir := t.TempDir()
		s := &Server{
			config:         ServerConfig{BasePath: tmpDir},
			logger:         logger,
			torrents:       make(map[string]*serverTorrentState),
			abortingHashes: make(map[string]chan struct{}),
			inodes:         NewInodeRegistry(tmpDir, logger),
		}

		// Create file with wrong size
		if err := os.MkdirAll(filepath.Join(tmpDir, "data"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(tmpDir, "data/file1.bin"), []byte("short"), 0o644); err != nil {
			t.Fatal(err)
		}

		resp, err := s.InitTorrent(ctx, &pb.InitTorrentRequest{
			TorrentHash: "preexist3",
			Name:        "test-torrent",
			NumPieces:   1,
			PieceSize:   1024,
			TotalSize:   1024,
			Files: []*pb.FileInfo{
				{Path: "data/file1.bin", Size: 1024, Offset: 0, Selected: true},
			},
		})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !resp.GetSuccess() {
			t.Fatalf("expected success, got error: %s", resp.GetError())
		}
		if resp.GetPiecesNeededCount() != 1 {
			t.Errorf("expected 1 piece needed (wrong size should not match), got %d", resp.GetPiecesNeededCount())
		}
		if resp.GetHardlinkResults()[0].GetPreExisting() {
			t.Error("wrong-size file should not be marked pre-existing")
		}
	})
}

func TestCountHardlinkResults_PreExisting(t *testing.T) {
	results := []*pb.HardlinkResult{
		{Hardlinked: true, SourcePath: "/some/path"},
		{PreExisting: true},
		{Pending: true},
		{PreExisting: true},
	}

	hardlinked, pending, preExisting := countHardlinkResults(results)

	if hardlinked != 1 {
		t.Errorf("expected 1 hardlinked, got %d", hardlinked)
	}
	if pending != 1 {
		t.Errorf("expected 1 pending, got %d", pending)
	}
	if preExisting != 2 {
		t.Errorf("expected 2 preExisting, got %d", preExisting)
	}
}

// Helper functions

func createTestMetadata(t *testing.T, basePath, hash string, modTime time.Time) {
	t.Helper()
	createTestStateFile(t, basePath, hash, modTime)
	createTestTorrentFile(t, basePath, hash, modTime)
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

// createTestTorrentFile creates a .torrent file in the metaDir with the given modTime.
// If no file paths are provided, uses a single dummy file.
func createTestTorrentFile(t *testing.T, basePath, hash string, modTime time.Time) {
	t.Helper()
	createTestTorrentFileWithPaths(t, basePath, hash, nil)
	torrentPath, err := findTorrentFile(filepath.Join(basePath, metaDirName, hash))
	if err != nil {
		t.Fatal(err)
	}
	setModTime(t, torrentPath, modTime)
}

// createTestTorrentFileWithPaths creates a .torrent file in the metaDir.
// relPaths are torrent-relative paths (e.g., "data/test.txt"). If empty, uses a dummy file.
func createTestTorrentFileWithPaths(t *testing.T, basePath, hash string, relPaths []string) {
	t.Helper()
	metaDir := filepath.Join(basePath, metaDirName, hash)
	if err := os.MkdirAll(metaDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Build pieces (10 fake piece hashes)
	numPieces := 10
	var piecesBuf bytes.Buffer
	for range numPieces {
		h := sha1.Sum([]byte("fake-piece"))
		piecesBuf.Write(h[:])
	}

	bt := bencodeTorrent{
		Info: bencodeInfo{
			Name:        "test",
			PieceLength: 1024,
			Pieces:      piecesBuf.String(),
		},
	}
	if len(relPaths) == 0 {
		bt.Info.Length = int64(numPieces) * 1024
	} else {
		files := make([]bencodeFile, len(relPaths))
		for i, p := range relPaths {
			files[i] = bencodeFile{
				Length: 1024,
				Path:   strings.Split(p, "/"),
			}
		}
		bt.Info.Files = files
	}

	torrentPath := filepath.Join(metaDir, "test.torrent")
	if err := os.WriteFile(torrentPath, encodeTorrent(t, bt), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(metaDir, versionFileName), []byte(metaVersion), 0o644); err != nil {
		t.Fatal(err)
	}
}

func setModTime(t *testing.T, path string, modTime time.Time) {
	t.Helper()
	if err := os.Chtimes(path, modTime, modTime); err != nil {
		t.Fatal(err)
	}
}

// writeTestFile creates a file at path with the given content, creating parent directories as needed.
func writeTestFile(t *testing.T, path string, content []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatal(err)
	}
}

// assertFileExists fails the test if the file does not exist.
func assertFileExists(t *testing.T, path, msg string) {
	t.Helper()
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Errorf("%s: %s", msg, path)
	}
}

// assertFileNotExists fails the test if the file exists.
func assertFileNotExists(t *testing.T, path, msg string) {
	t.Helper()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("%s: %s", msg, path)
	}
}

// newRelocateInitRequest builds an InitTorrentRequest for relocation tests with a single file.
func newRelocateInitRequest(hash, subPath string) *pb.InitTorrentRequest {
	return &pb.InitTorrentRequest{
		TorrentHash: hash,
		Name:        "test-torrent",
		NumPieces:   1,
		PieceSize:   1024,
		TotalSize:   512,
		SaveSubPath: subPath,
		Files: []*pb.FileInfo{
			{Path: "data/file.bin", Size: 512, Offset: 0, Selected: true},
		},
	}
}

func TestRelocateFiles(t *testing.T) {
	t.Parallel()
	logger := testLogger(t)
	ctx := context.Background()

	newServer := func(t *testing.T) (*Server, string) {
		t.Helper()
		tmpDir := t.TempDir()
		s := &Server{
			config: ServerConfig{BasePath: tmpDir},
			logger: logger,
		}
		return s, tmpDir
	}

	t.Run("moves partial files to new sub-path", func(t *testing.T) {
		t.Parallel()
		s, tmpDir := newServer(t)

		partialPath := filepath.Join(tmpDir, "data", "file.mkv.partial")
		writeTestFile(t, partialPath, []byte("partial data"))

		moved, err := s.relocateFiles(ctx, "hash1", []string{"data/file.mkv"}, "", "movies")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if moved != 1 {
			t.Errorf("expected moved=1, got %d", moved)
		}

		newPath := filepath.Join(tmpDir, "movies", "data", "file.mkv.partial")
		assertFileExists(t, newPath, "file should exist at new path")
		assertFileNotExists(t, partialPath, "file should not exist at old path")
	})

	t.Run("moves finalized files to new sub-path", func(t *testing.T) {
		t.Parallel()
		s, tmpDir := newServer(t)

		finalPath := filepath.Join(tmpDir, "data", "file.mkv")
		writeTestFile(t, finalPath, []byte("final data"))

		moved, err := s.relocateFiles(ctx, "hash2", []string{"data/file.mkv"}, "", "movies")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if moved != 1 {
			t.Errorf("expected moved=1, got %d", moved)
		}

		assertFileExists(t, filepath.Join(tmpDir, "movies", "data", "file.mkv"), "file should exist at new path")
		assertFileNotExists(t, finalPath, "file should not exist at old path")
	})

	t.Run("moves both partial and finalized", func(t *testing.T) {
		t.Parallel()
		s, tmpDir := newServer(t)

		writeTestFile(t, filepath.Join(tmpDir, "data", "file.mkv.partial"), []byte("partial"))
		writeTestFile(t, filepath.Join(tmpDir, "data", "file.mkv"), []byte("final"))

		moved, err := s.relocateFiles(ctx, "hash3", []string{"data/file.mkv"}, "", "movies")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if moved != 2 {
			t.Errorf("expected moved=2, got %d", moved)
		}

		newDir := filepath.Join(tmpDir, "movies", "data")
		assertFileExists(t, filepath.Join(newDir, "file.mkv.partial"), "partial file should exist at new path")
		assertFileExists(t, filepath.Join(newDir, "file.mkv"), "finalized file should exist at new path")
	})

	t.Run("skips missing files", func(t *testing.T) {
		t.Parallel()
		s, _ := newServer(t)

		moved, err := s.relocateFiles(ctx, "hash4", []string{"data/nonexistent.mkv"}, "", "movies")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if moved != 0 {
			t.Errorf("expected moved=0, got %d", moved)
		}
	})

	t.Run("skips when target exists", func(t *testing.T) {
		t.Parallel()
		s, tmpDir := newServer(t)

		oldPath := filepath.Join(tmpDir, "data", "file.mkv")
		writeTestFile(t, oldPath, []byte("old"))

		newPath := filepath.Join(tmpDir, "movies", "data", "file.mkv")
		writeTestFile(t, newPath, []byte("already here"))

		moved, err := s.relocateFiles(ctx, "hash5", []string{"data/file.mkv"}, "", "movies")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if moved != 0 {
			t.Errorf("expected moved=0, got %d", moved)
		}

		assertFileExists(t, oldPath, "old file should still exist when target already present")
	})

	t.Run("noop when sub-paths equal", func(t *testing.T) {
		t.Parallel()
		s, _ := newServer(t)

		moved, err := s.relocateFiles(ctx, "hash6", []string{"data/file.mkv"}, "movies", "movies")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if moved != 0 {
			t.Errorf("expected moved=0, got %d", moved)
		}
	})
}

func TestUpdateStateAfterRelocate(t *testing.T) {
	t.Parallel()

	t.Run("updates file paths and saveSubPath", func(t *testing.T) {
		t.Parallel()
		basePath := "/data/cold"
		state := &serverTorrentState{
			saveSubPath: "",
			files: []*serverFileInfo{
				{path: filepath.Join(basePath, "data", "file.bin.partial"), size: 1024, selected: true},
				{path: filepath.Join(basePath, "data", "file2.bin"), size: 2048, selected: true},
			},
		}

		updateStateAfterRelocate(state, basePath, "", "movies")

		if state.saveSubPath != "movies" {
			t.Errorf("expected saveSubPath=movies, got %q", state.saveSubPath)
		}

		wantPaths := []string{
			filepath.Join(basePath, "movies", "data", "file.bin.partial"),
			filepath.Join(basePath, "movies", "data", "file2.bin"),
		}
		for i, want := range wantPaths {
			if state.files[i].path != want {
				t.Errorf("file[%d] path = %q, want %q", i, state.files[i].path, want)
			}
		}
	})
}

func TestInitTorrent_RelocatesOnSubPathChange(t *testing.T) {
	ctx := context.Background()
	logger := testLogger(t)

	// initAndRestart calls InitTorrent, then removes in-memory state to simulate a cold restart.
	initAndRestart := func(t *testing.T, s *Server, req *pb.InitTorrentRequest) {
		t.Helper()
		resp, err := s.InitTorrent(ctx, req)
		if err != nil {
			t.Fatalf("InitTorrent failed: %v", err)
		}
		if !resp.GetSuccess() {
			t.Fatalf("InitTorrent not successful: %s", resp.GetError())
		}
		s.mu.Lock()
		delete(s.torrents, req.GetTorrentHash())
		s.mu.Unlock()
	}

	t.Run("relocates files from empty to category sub-path", func(t *testing.T) {
		tmpDir := t.TempDir()
		s := &Server{
			config:         ServerConfig{BasePath: tmpDir},
			logger:         logger,
			torrents:       make(map[string]*serverTorrentState),
			abortingHashes: make(map[string]chan struct{}),
			inodes:         NewInodeRegistry(tmpDir, logger),
		}

		hash := "relocinit1"

		// Init with empty sub-path, then simulate cold restart
		initAndRestart(t, s, newRelocateInitRequest(hash, ""))

		// Simulate streamed data (files are lazily opened on WritePiece)
		oldPartialPath := filepath.Join(tmpDir, "data", "file.bin.partial")
		writeTestFile(t, oldPartialPath, make([]byte, 512))

		// Re-init with new sub-path "movies"
		resp, err := s.InitTorrent(ctx, newRelocateInitRequest(hash, "movies"))
		if err != nil {
			t.Fatalf("second InitTorrent failed: %v", err)
		}
		if !resp.GetSuccess() {
			t.Fatalf("second InitTorrent not successful: %s", resp.GetError())
		}

		newPartialPath := filepath.Join(tmpDir, "movies", "data", "file.bin.partial")
		assertFileExists(t, newPartialPath, "partial file should exist at new sub-path after re-init")
		assertFileNotExists(t, oldPartialPath, "partial file should not exist at old path after relocation")

		// Verify persisted sub-path was updated
		metaDir := filepath.Join(tmpDir, metaDirName, hash)
		subPath := loadSubPathFile(metaDir)
		if subPath != "movies" {
			t.Errorf("persisted subPath = %q, want %q", subPath, "movies")
		}
	})

	t.Run("no relocation when sub-path unchanged", func(t *testing.T) {
		tmpDir := t.TempDir()
		s := &Server{
			config:         ServerConfig{BasePath: tmpDir},
			logger:         logger,
			torrents:       make(map[string]*serverTorrentState),
			abortingHashes: make(map[string]chan struct{}),
			inodes:         NewInodeRegistry(tmpDir, logger),
		}

		hash := "relocinit2"

		// Init with sub-path "movies", then simulate cold restart
		initAndRestart(t, s, newRelocateInitRequest(hash, "movies"))

		// Simulate streamed data
		partialPath := filepath.Join(tmpDir, "movies", "data", "file.bin.partial")
		writeTestFile(t, partialPath, make([]byte, 512))

		// Re-init with same sub-path
		resp, err := s.InitTorrent(ctx, newRelocateInitRequest(hash, "movies"))
		if err != nil {
			t.Fatalf("second InitTorrent failed: %v", err)
		}
		if !resp.GetSuccess() {
			t.Fatalf("second InitTorrent not successful: %s", resp.GetError())
		}

		assertFileExists(t, partialPath, "partial file should still exist at same path")
	})
}

func TestFinalizeTorrent_RelocatesOnSubPathChange(t *testing.T) {
	ctx := context.Background()
	logger := testLogger(t)

	// newIncompleteState builds a serverTorrentState with incomplete pieces so that
	// FinalizeTorrent aborts after the relocation check without triggering full finalization.
	newIncompleteState := func(filePath, subPath string) *serverTorrentState {
		return &serverTorrentState{
			written:      []bool{true, false},
			writtenCount: 1,
			pieceLength:  512,
			totalSize:    1024,
			saveSubPath:  subPath,
			files: []*serverFileInfo{
				{path: filePath, size: 1024, offset: 0, selected: true},
			},
		}
	}

	t.Run("relocates files before finalization", func(t *testing.T) {
		tmpDir := t.TempDir()
		s := &Server{
			config:         ServerConfig{BasePath: tmpDir},
			logger:         logger,
			torrents:       make(map[string]*serverTorrentState),
			abortingHashes: make(map[string]chan struct{}),
			inodes:         NewInodeRegistry(tmpDir, logger),
		}

		hash := "relocfin1"
		oldFilePath := filepath.Join(tmpDir, "data", "file.bin.partial")
		writeTestFile(t, oldFilePath, []byte("file content"))

		state := newIncompleteState(oldFilePath, "")
		s.mu.Lock()
		s.torrents[hash] = state
		s.mu.Unlock()

		resp, err := s.FinalizeTorrent(ctx, &pb.FinalizeTorrentRequest{
			TorrentHash: hash,
			SaveSubPath: "movies",
		})
		if err != nil {
			t.Fatalf("FinalizeTorrent RPC error: %v", err)
		}
		if resp.GetSuccess() {
			t.Fatal("expected failure due to incomplete pieces")
		}

		// Verify state was updated by relocation
		state.mu.Lock()
		currentSubPath := state.saveSubPath
		currentFilePath := state.files[0].path
		state.mu.Unlock()

		if currentSubPath != "movies" {
			t.Errorf("state.saveSubPath = %q, want %q", currentSubPath, "movies")
		}

		expectedNewPath := filepath.Join(tmpDir, "movies", "data", "file.bin.partial")
		if currentFilePath != expectedNewPath {
			t.Errorf("state.files[0].path = %q, want %q", currentFilePath, expectedNewPath)
		}

		assertFileExists(t, expectedNewPath, "file should exist at new path after relocation")
		assertFileNotExists(t, oldFilePath, "file should not exist at old path after relocation")
	})

	t.Run("no relocation when request sub-path is empty", func(t *testing.T) {
		tmpDir := t.TempDir()
		s := &Server{
			config:         ServerConfig{BasePath: tmpDir},
			logger:         logger,
			torrents:       make(map[string]*serverTorrentState),
			abortingHashes: make(map[string]chan struct{}),
			inodes:         NewInodeRegistry(tmpDir, logger),
		}

		hash := "relocfin2"
		filePath := filepath.Join(tmpDir, "movies", "data", "file.bin.partial")
		writeTestFile(t, filePath, []byte("file content"))

		state := newIncompleteState(filePath, "movies")
		s.mu.Lock()
		s.torrents[hash] = state
		s.mu.Unlock()

		resp, err := s.FinalizeTorrent(ctx, &pb.FinalizeTorrentRequest{
			TorrentHash: hash,
			SaveSubPath: "",
		})
		if err != nil {
			t.Fatalf("FinalizeTorrent RPC error: %v", err)
		}
		if resp.GetSuccess() {
			t.Fatal("expected failure due to incomplete pieces")
		}

		// State should be unchanged
		state.mu.Lock()
		currentSubPath := state.saveSubPath
		currentFilePath := state.files[0].path
		state.mu.Unlock()

		if currentSubPath != "movies" {
			t.Errorf("state.saveSubPath = %q, want %q (should be unchanged)", currentSubPath, "movies")
		}
		if currentFilePath != filePath {
			t.Errorf("state.files[0].path = %q, want %q (should be unchanged)", currentFilePath, filePath)
		}

		assertFileExists(t, filePath, "file should still exist at original path")
	})
}

func TestInitTorrentResync(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	logger := testLogger(t)

	t.Run("resync deletes qB entry and re-initializes", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		mock := &mockQBClient{
			torrents: []qbittorrent.Torrent{
				{
					Hash:     "resync1",
					State:    "stalledUP",
					Progress: 1.0,
				},
			},
		}
		s := &Server{
			config:         ServerConfig{BasePath: tmpDir},
			logger:         logger,
			torrents:       make(map[string]*serverTorrentState),
			abortingHashes: make(map[string]chan struct{}),
			inodes:         NewInodeRegistry(tmpDir, logger),
			qbClient:       mock,
		}

		resp, err := s.InitTorrent(ctx, &pb.InitTorrentRequest{
			TorrentHash: "resync1",
			Name:        "test-resync",
			NumPieces:   2,
			PieceSize:   512,
			TotalSize:   1024,
			Resync:      true,
			Files: []*pb.FileInfo{
				{Path: "data/file1.bin", Size: 512, Offset: 0, Selected: true},
				{Path: "data/file2.bin", Size: 512, Offset: 512, Selected: true},
			},
		})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !resp.GetSuccess() {
			t.Fatalf("expected success, got error: %s", resp.GetError())
		}

		// Should have deleted the stale qB entry (deleteFiles=false)
		if !mock.deleteCalled {
			t.Error("DeleteTorrentsCtx should have been called for re-sync")
		}
		if len(mock.deleteHashes) != 1 || mock.deleteHashes[0] != "resync1" {
			t.Errorf("expected delete hash [resync1], got %v", mock.deleteHashes)
		}
		if mock.deleteDeleteFiles {
			t.Error("deleteFiles should be false for re-sync")
		}

		// Should return READY status (re-initialized), not COMPLETE
		if resp.GetStatus() != pb.TorrentSyncStatus_SYNC_STATUS_READY {
			t.Errorf("expected READY status after re-sync, got %v", resp.GetStatus())
		}

		// Should be tracked in server state
		s.mu.RLock()
		_, exists := s.torrents["resync1"]
		s.mu.RUnlock()
		if !exists {
			t.Error("torrent should be tracked after re-sync init")
		}
	})

	t.Run("without resync returns COMPLETE", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		mock := &mockQBClient{
			torrents: []qbittorrent.Torrent{
				{
					Hash:     "noresync1",
					State:    "stalledUP",
					Progress: 1.0,
				},
			},
		}
		s := &Server{
			config:         ServerConfig{BasePath: tmpDir},
			logger:         logger,
			torrents:       make(map[string]*serverTorrentState),
			abortingHashes: make(map[string]chan struct{}),
			inodes:         NewInodeRegistry(tmpDir, logger),
			qbClient:       mock,
		}

		resp, err := s.InitTorrent(ctx, &pb.InitTorrentRequest{
			TorrentHash: "noresync1",
			Name:        "test-no-resync",
			NumPieces:   2,
			PieceSize:   512,
			TotalSize:   1024,
			Resync:      false,
			Files: []*pb.FileInfo{
				{Path: "data/file1.bin", Size: 512, Offset: 0, Selected: true},
				{Path: "data/file2.bin", Size: 512, Offset: 512, Selected: true},
			},
		})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !resp.GetSuccess() {
			t.Fatalf("expected success, got error: %s", resp.GetError())
		}

		// Should return COMPLETE without deleting
		if resp.GetStatus() != pb.TorrentSyncStatus_SYNC_STATUS_COMPLETE {
			t.Errorf("expected COMPLETE status, got %v", resp.GetStatus())
		}
		if mock.deleteCalled {
			t.Error("DeleteTorrentsCtx should NOT have been called without resync")
		}

		// Should NOT be tracked (still complete)
		s.mu.RLock()
		_, exists := s.torrents["noresync1"]
		s.mu.RUnlock()
		if exists {
			t.Error("torrent should NOT be tracked when returning COMPLETE")
		}
	})

	t.Run("resync without files returns COMPLETE", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		mock := &mockQBClient{
			torrents: []qbittorrent.Torrent{
				{
					Hash:     "noresync2",
					State:    "stalledUP",
					Progress: 1.0,
				},
			},
		}
		s := &Server{
			config:         ServerConfig{BasePath: tmpDir},
			logger:         logger,
			torrents:       make(map[string]*serverTorrentState),
			abortingHashes: make(map[string]chan struct{}),
			inodes:         NewInodeRegistry(tmpDir, logger),
			qbClient:       mock,
		}

		// Resync=true but no files — the guard is `len(req.GetFiles()) > 0 && req.GetResync()`
		resp, err := s.InitTorrent(ctx, &pb.InitTorrentRequest{
			TorrentHash: "noresync2",
			Name:        "test-resync-no-files",
			NumPieces:   1,
			PieceSize:   1024,
			TotalSize:   1024,
			Resync:      true,
			// No Files — minimal request
		})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !resp.GetSuccess() {
			t.Fatalf("expected success, got error: %s", resp.GetError())
		}
		if resp.GetStatus() != pb.TorrentSyncStatus_SYNC_STATUS_COMPLETE {
			t.Errorf("expected COMPLETE (resync without files should be no-op), got %v", resp.GetStatus())
		}
		if mock.deleteCalled {
			t.Error("DeleteTorrentsCtx should NOT be called when files are empty")
		}
	})
}

func TestInitTorrentResync_PartialRecovery(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	logger := testLogger(t)

	t.Run("detects pre-existing partial files", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		mock := &mockQBClient{
			torrents: []qbittorrent.Torrent{
				{Hash: "resyncpartial", State: "stalledUP", Progress: 1.0},
			},
		}
		s := &Server{
			config:         ServerConfig{BasePath: tmpDir},
			logger:         logger,
			torrents:       make(map[string]*serverTorrentState),
			abortingHashes: make(map[string]chan struct{}),
			inodes:         NewInodeRegistry(tmpDir, logger),
			qbClient:       mock,
		}

		// Pre-create file1 at final path (previously synced) and
		// file2 as .partial (interrupted previous resync attempt).
		writeTestFile(t, filepath.Join(tmpDir, "data", "file1.bin"), make([]byte, 512))
		// .partial pre-allocated to full size (simulates openFile + Truncate from previous attempt)
		writeTestFile(t, filepath.Join(tmpDir, "data", "file2.bin.partial"), make([]byte, 512))

		resp, err := s.InitTorrent(ctx, &pb.InitTorrentRequest{
			TorrentHash: "resyncpartial",
			Name:        "test-resync-partial",
			NumPieces:   2,
			PieceSize:   512,
			TotalSize:   1024,
			Resync:      true,
			Files: []*pb.FileInfo{
				{Path: "data/file1.bin", Size: 512, Offset: 0, Selected: true},
				{Path: "data/file2.bin", Size: 512, Offset: 512, Selected: true},
			},
		})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !resp.GetSuccess() {
			t.Fatalf("expected success, got error: %s", resp.GetError())
		}
		if resp.GetStatus() != pb.TorrentSyncStatus_SYNC_STATUS_READY {
			t.Errorf("expected READY, got %v", resp.GetStatus())
		}

		// file1 at final path → pre-existing (hlStateComplete), pieces covered
		results := resp.GetHardlinkResults()
		if !results[0].GetPreExisting() {
			t.Error("file 0: expected PreExisting=true (final path)")
		}
		// file2 .partial detected → NOT treated as hardlinked, but no error.
		// Its pieces are not covered (no .state file), so still needed.
		if results[1].GetPreExisting() {
			t.Error("file 1: .partial should NOT be marked as PreExisting")
		}
		if results[1].GetHardlinked() {
			t.Error("file 1: .partial should NOT be marked as Hardlinked")
		}

		// file1 covered → 1 piece have; file2 partial → 1 piece needed
		if resp.GetPiecesHaveCount() != 1 {
			t.Errorf("expected 1 piece have, got %d", resp.GetPiecesHaveCount())
		}
		if resp.GetPiecesNeededCount() != 1 {
			t.Errorf("expected 1 piece needed, got %d", resp.GetPiecesNeededCount())
		}

		// Verify that .partial was reused — the file should still be at the .partial path.
		s.mu.RLock()
		state := s.torrents["resyncpartial"]
		s.mu.RUnlock()
		if state == nil {
			t.Fatal("torrent state should exist")
		}
		if !strings.HasSuffix(state.files[1].path, ".partial") {
			t.Errorf("file 1 path should end with .partial, got %s", state.files[1].path)
		}
	})

	t.Run("persists initial state for pre-existing pieces", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		mock := &mockQBClient{
			torrents: []qbittorrent.Torrent{
				{Hash: "resyncstate", State: "stalledUP", Progress: 1.0},
			},
		}

		var savedPath string
		var savedWritten []bool
		s := &Server{
			config:         ServerConfig{BasePath: tmpDir},
			logger:         logger,
			torrents:       make(map[string]*serverTorrentState),
			abortingHashes: make(map[string]chan struct{}),
			inodes:         NewInodeRegistry(tmpDir, logger),
			qbClient:       mock,
			saveStateFunc: func(path string, written []bool) error {
				savedPath = path
				savedWritten = append([]bool(nil), written...)
				return nil
			},
		}

		// Pre-create file1 at final path (1 piece pre-existing)
		writeTestFile(t, filepath.Join(tmpDir, "data", "file1.bin"), make([]byte, 512))

		resp, err := s.InitTorrent(ctx, &pb.InitTorrentRequest{
			TorrentHash: "resyncstate",
			Name:        "test-resync-state",
			NumPieces:   2,
			PieceSize:   512,
			TotalSize:   1024,
			Resync:      true,
			Files: []*pb.FileInfo{
				{Path: "data/file1.bin", Size: 512, Offset: 0, Selected: true},
				{Path: "data/file2.bin", Size: 512, Offset: 512, Selected: true},
			},
		})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !resp.GetSuccess() {
			t.Fatalf("expected success, got error: %s", resp.GetError())
		}

		// Initial state should have been saved immediately (pre-existing piece detected)
		if savedPath == "" {
			t.Fatal("doSaveState should have been called for initial state persistence")
		}
		if len(savedWritten) != 2 {
			t.Fatalf("expected 2 pieces in saved state, got %d", len(savedWritten))
		}
		// Piece 0 (file1, pre-existing) should be marked as written
		if !savedWritten[0] {
			t.Error("piece 0 should be written (pre-existing file)")
		}
		// Piece 1 (file2, missing) should not be written
		if savedWritten[1] {
			t.Error("piece 1 should NOT be written (file2 not on disk)")
		}
	})
}
