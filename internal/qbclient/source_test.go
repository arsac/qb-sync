package qbclient

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/autobrr/go-qbittorrent"

	"github.com/arsac/qb-sync/internal/utils"
	pb "github.com/arsac/qb-sync/proto"
)

func TestResolveContentDir(t *testing.T) {
	tests := []struct {
		name              string
		dataPath          string
		qbDefaultSavePath string
		torrentSavePath   string
		want              string
	}{
		{
			name:              "no category (save_path equals default)",
			dataPath:          "/data",
			qbDefaultSavePath: "/downloads",
			torrentSavePath:   "/downloads",
			want:              "/data",
		},
		{
			name:              "ATM with category subdirectory",
			dataPath:          "/data",
			qbDefaultSavePath: "/downloads",
			torrentSavePath:   "/downloads/movies",
			want:              "/data/movies",
		},
		{
			name:              "ATM with nested category path",
			dataPath:          "/data",
			qbDefaultSavePath: "/downloads",
			torrentSavePath:   "/downloads/media/movies",
			want:              "/data/media/movies",
		},
		{
			name:              "save_path outside default falls back to dataPath",
			dataPath:          "/data",
			qbDefaultSavePath: "/downloads",
			torrentSavePath:   "/media/movies",
			want:              "/data",
		},
		{
			name:              "same paths (no Docker)",
			dataPath:          "/data/torrents",
			qbDefaultSavePath: "/data/torrents",
			torrentSavePath:   "/data/torrents/movies",
			want:              "/data/torrents/movies",
		},
		{
			name:              "same paths no category",
			dataPath:          "/data/torrents",
			qbDefaultSavePath: "/data/torrents",
			torrentSavePath:   "/data/torrents",
			want:              "/data/torrents",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Source{
				dataPath:          tt.dataPath,
				qbDefaultSavePath: tt.qbDefaultSavePath,
			}
			got := s.ResolveContentDir(tt.torrentSavePath)
			if got != tt.want {
				t.Errorf("ResolveContentDir(%q) = %q, want %q", tt.torrentSavePath, got, tt.want)
			}
		})
	}
}

func TestResolveReadDirs(t *testing.T) {
	tests := []struct {
		name              string
		dataPath          string
		qbDefaultSavePath string
		torrent           qbittorrent.Torrent
		wantPrimary       string
		wantFallback      string
	}{
		{
			name:              "completed torrent: primary=save, fallback=download",
			dataPath:          "/data",
			qbDefaultSavePath: "/downloads",
			torrent: qbittorrent.Torrent{
				SavePath:     "/downloads/movies",
				DownloadPath: "/downloads/incomplete",
				Progress:     1.0,
			},
			wantPrimary:  "/data/movies",
			wantFallback: "/data/incomplete",
		},
		{
			name:              "downloading: primary=download, fallback=save",
			dataPath:          "/data",
			qbDefaultSavePath: "/downloads",
			torrent: qbittorrent.Torrent{
				SavePath:     "/downloads/movies",
				DownloadPath: "/downloads/incomplete",
				Progress:     0.5,
			},
			wantPrimary:  "/data/incomplete",
			wantFallback: "/data/movies",
		},
		{
			name:              "no DownloadPath: primary=save, no fallback",
			dataPath:          "/data",
			qbDefaultSavePath: "/downloads",
			torrent: qbittorrent.Torrent{
				SavePath: "/downloads/movies",
				Progress: 0.3,
			},
			wantPrimary:  "/data/movies",
			wantFallback: "",
		},
		{
			name:              "DownloadPath outside save root: primary=save, no fallback",
			dataPath:          "/data",
			qbDefaultSavePath: "/downloads",
			torrent: qbittorrent.Torrent{
				SavePath:     "/downloads/movies",
				DownloadPath: "/other-mount/incomplete",
				Progress:     0.5,
			},
			wantPrimary:  "/data/movies",
			wantFallback: "",
		},
		{
			name:              "zero progress with DownloadPath: primary=download, fallback=save",
			dataPath:          "/data",
			qbDefaultSavePath: "/downloads",
			torrent: qbittorrent.Torrent{
				SavePath:     "/downloads/movies",
				DownloadPath: "/downloads/incomplete",
				Progress:     0,
			},
			wantPrimary:  "/data/incomplete",
			wantFallback: "/data/movies",
		},
		{
			name:              "DownloadPath same as SavePath: primary=save, no fallback",
			dataPath:          "/data",
			qbDefaultSavePath: "/downloads",
			torrent: qbittorrent.Torrent{
				SavePath:     "/downloads/movies",
				DownloadPath: "/downloads/movies",
				Progress:     0.5,
			},
			wantPrimary:  "/data/movies",
			wantFallback: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Source{
				dataPath:          tt.dataPath,
				qbDefaultSavePath: tt.qbDefaultSavePath,
			}
			primary, fallback := s.resolveReadDirs(tt.torrent)
			if primary != tt.wantPrimary {
				t.Errorf("primary = %q, want %q", primary, tt.wantPrimary)
			}
			if fallback != tt.wantFallback {
				t.Errorf("fallback = %q, want %q", fallback, tt.wantFallback)
			}
		})
	}
}

func TestReadPiece_ENOENTFallback(t *testing.T) {
	content := []byte("piece data here!")
	files := []*pb.FileInfo{
		{Path: "file.bin", Size: int64(len(content)), Offset: 0},
	}
	makePiece := func(hash string) *pb.Piece {
		return &pb.Piece{TorrentHash: hash, Offset: 0, Size: int64(len(content))}
	}

	t.Run("succeeds on first try from primary dir", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "file.bin"), content, 0o644); err != nil {
			t.Fatal(err)
		}

		s := &Source{}
		s.fileCache.Store("h1", &cachedMeta{files: files, contentDir: dir})

		data, err := s.ReadPiece(context.Background(), makePiece("h1"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if string(data) != string(content) {
			t.Errorf("got %q, want %q", string(data), string(content))
		}
	})

	t.Run("ENOENT on primary falls back to fallbackDir", func(t *testing.T) {
		staleDir := t.TempDir() // empty — file no longer here
		correctDir := t.TempDir()
		if err := os.WriteFile(filepath.Join(correctDir, "file.bin"), content, 0o644); err != nil {
			t.Fatal(err)
		}

		s := &Source{}
		s.fileCache.Store("h2", &cachedMeta{
			files:       files,
			contentDir:  staleDir,
			fallbackDir: correctDir,
		})

		data, err := s.ReadPiece(context.Background(), makePiece("h2"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if string(data) != string(content) {
			t.Errorf("got %q, want %q", string(data), string(content))
		}

		// Cache should be updated: primary promoted to correctDir, no fallback
		raw, ok := s.fileCache.Load("h2")
		if !ok {
			t.Fatal("cache entry should still exist")
		}
		cached := raw.(*cachedMeta)
		if cached.contentDir != correctDir {
			t.Errorf("cache contentDir = %q, want %q", cached.contentDir, correctDir)
		}
		if cached.fallbackDir != "" {
			t.Errorf("cache fallbackDir = %q, want empty", cached.fallbackDir)
		}
	})

	t.Run("readPieceMultiFile returns ENOENT for missing file", func(t *testing.T) {
		// Verifies the ENOENT detection that ReadPiece relies on for its
		// evict-and-retry path. Full ReadPiece integration (evict + refetch)
		// requires a real qBittorrent client, so we test the building block.
		emptyDir := t.TempDir()

		s := &Source{}
		_, err := s.readPieceMultiFile(emptyDir, files, 0, int64(len(content)))
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("expected ENOENT, got %v", err)
		}
	})

	t.Run("cache eviction removes entry", func(t *testing.T) {
		// Verifies the cache eviction mechanics that ReadPiece uses when
		// both primary and fallback dirs fail with ENOENT.
		s := &Source{}
		s.fileCache.Store("h4", &cachedMeta{
			files:       files,
			contentDir:  "/nonexistent/primary",
			fallbackDir: "/nonexistent/fallback",
		})

		s.fileCache.Delete("h4")
		if _, ok := s.fileCache.Load("h4"); ok {
			t.Error("cache entry should have been evicted")
		}
	})
}

func TestReadChunkFromFile_Basic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.bin")

	content := []byte("hello world, this is test data!")
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatal(err)
	}

	// Read from middle
	data, err := utils.ReadChunkFromFile(path, 6, 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != "world" {
		t.Errorf("got %q, want %q", string(data), "world")
	}

	// Read from start
	data, err = utils.ReadChunkFromFile(path, 0, 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != "hello" {
		t.Errorf("got %q, want %q", string(data), "hello")
	}

	// Read past EOF returns available data
	data, err = utils.ReadChunkFromFile(path, int64(len(content)-3), 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != "ta!" {
		t.Errorf("got %q, want %q", string(data), "ta!")
	}
}

func TestReadPieceMultiFile_SingleFile(t *testing.T) {
	// Regression test: single-file torrents must work through readPieceMultiFile.
	// The old code had a shortcut that tried to open the base directory as a file.
	dir := t.TempDir()
	filePath := filepath.Join(dir, "movie.mkv")

	content := []byte("0123456789abcdef")
	if err := os.WriteFile(filePath, content, 0o644); err != nil {
		t.Fatal(err)
	}

	s := &Source{}
	files := []*pb.FileInfo{
		{Path: "movie.mkv", Size: int64(len(content)), Offset: 0},
	}

	// Read a "piece" from the middle
	data, err := s.readPieceMultiFile(dir, files, 4, 8)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != "456789ab" {
		t.Errorf("got %q, want %q", string(data), "456789ab")
	}
}

func TestReadPieceMultiFile_MultipleFiles(t *testing.T) {
	dir := t.TempDir()

	// Create two files
	file1 := filepath.Join(dir, "part1.bin")
	file2 := filepath.Join(dir, "part2.bin")

	if err := os.WriteFile(file1, []byte("AAAAAAAAAA"), 0o644); err != nil { // 10 bytes
		t.Fatal(err)
	}
	if err := os.WriteFile(file2, []byte("BBBBBBBBBB"), 0o644); err != nil { // 10 bytes
		t.Fatal(err)
	}

	s := &Source{}
	files := []*pb.FileInfo{
		{Path: "part1.bin", Size: 10, Offset: 0},
		{Path: "part2.bin", Size: 10, Offset: 10},
	}

	// Read entirely from second file (offset 12, size 4)
	data, err := s.readPieceMultiFile(dir, files, 12, 4)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != "BBBB" {
		t.Errorf("got %q, want %q", string(data), "BBBB")
	}
}

func TestReadPieceMultiFile_PieceSpansFiles(t *testing.T) {
	dir := t.TempDir()

	file1 := filepath.Join(dir, "a.bin")
	file2 := filepath.Join(dir, "b.bin")

	if err := os.WriteFile(file1, []byte("AAAA"), 0o644); err != nil { // 4 bytes
		t.Fatal(err)
	}
	if err := os.WriteFile(file2, []byte("BBBB"), 0o644); err != nil { // 4 bytes
		t.Fatal(err)
	}

	s := &Source{}
	files := []*pb.FileInfo{
		{Path: "a.bin", Size: 4, Offset: 0},
		{Path: "b.bin", Size: 4, Offset: 4},
	}

	// Read a piece that spans both files: last 2 bytes of a.bin + first 2 bytes of b.bin
	data, err := s.readPieceMultiFile(dir, files, 2, 4)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != "AABB" {
		t.Errorf("got %q, want %q", string(data), "AABB")
	}
}
