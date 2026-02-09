package qbclient

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
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
		qbTempPath        string
		tempDataPath      string
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
		{
			name:              "temp path resolves via qbTempPath (sibling dirs)",
			dataPath:          "/mnt/data/torrents",
			qbDefaultSavePath: "/data/torrents",
			qbTempPath:        "/data/incomplete",
			tempDataPath:      "/mnt/data/incomplete",
			torrentSavePath:   "/data/incomplete/movies",
			want:              "/mnt/data/incomplete/movies",
		},
		{
			name:              "temp path exact match",
			dataPath:          "/mnt/data/torrents",
			qbDefaultSavePath: "/data/torrents",
			qbTempPath:        "/data/incomplete",
			tempDataPath:      "/mnt/data/incomplete",
			torrentSavePath:   "/data/incomplete",
			want:              "/mnt/data/incomplete",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Source{
				dataPath:          tt.dataPath,
				qbDefaultSavePath: tt.qbDefaultSavePath,
				qbTempPath:        tt.qbTempPath,
				tempDataPath:      tt.tempDataPath,
			}
			got := s.ResolveContentDir(tt.torrentSavePath)
			if got != tt.want {
				t.Errorf("ResolveContentDir(%q) = %q, want %q", tt.torrentSavePath, got, tt.want)
			}
		})
	}
}

func TestResolveReadDir(t *testing.T) {
	tests := []struct {
		name              string
		dataPath          string
		qbDefaultSavePath string
		qbTempPath        string
		tempDataPath      string
		torrent           qbittorrent.Torrent
		want              string
	}{
		{
			name:              "completed torrent: ContentPath in save dir",
			dataPath:          "/data",
			qbDefaultSavePath: "/downloads",
			torrent: qbittorrent.Torrent{
				SavePath:    "/downloads/movies",
				ContentPath: "/downloads/movies/MyMovie",
				Progress:    1.0,
			},
			want: "/data/movies",
		},
		{
			name:              "downloading: ContentPath in download dir",
			dataPath:          "/data",
			qbDefaultSavePath: "/downloads",
			torrent: qbittorrent.Torrent{
				SavePath:    "/downloads/movies",
				ContentPath: "/downloads/incomplete/MyMovie",
				Progress:    0.5,
			},
			want: "/data/incomplete",
		},
		{
			name:              "no DownloadPath: resolves via ContentPath",
			dataPath:          "/data",
			qbDefaultSavePath: "/downloads",
			torrent: qbittorrent.Torrent{
				SavePath:    "/downloads/movies",
				ContentPath: "/downloads/movies/MyMovie",
				Progress:    0.3,
			},
			want: "/data/movies",
		},
		{
			name:              "zero progress: ContentPath in download dir",
			dataPath:          "/data",
			qbDefaultSavePath: "/downloads",
			torrent: qbittorrent.Torrent{
				SavePath:    "/downloads/movies",
				ContentPath: "/downloads/incomplete/MyMovie",
				Progress:    0,
			},
			want: "/data/incomplete",
		},
		{
			name:              "temp path: ContentPath resolves via qbTempPath",
			dataPath:          "/mnt/data/torrents",
			qbDefaultSavePath: "/data/torrents",
			qbTempPath:        "/data/incomplete",
			tempDataPath:      "/mnt/data/incomplete",
			torrent: qbittorrent.Torrent{
				SavePath:    "/data/torrents/movies",
				ContentPath: "/data/incomplete/MyMovie",
				Progress:    0.5,
			},
			want: "/mnt/data/incomplete",
		},
		{
			name:              "ContentPath reflects temp dir with category subdir",
			dataPath:          "/mnt/data/torrents",
			qbDefaultSavePath: "/data/torrents",
			qbTempPath:        "/data/incomplete",
			tempDataPath:      "/mnt/data/incomplete",
			torrent: qbittorrent.Torrent{
				SavePath:    "/data/torrents/movies",
				ContentPath: "/data/incomplete/movies/MyMovie",
				Progress:    0.3,
			},
			want: "/mnt/data/incomplete/movies",
		},
		{
			name:              "ContentPath matches SavePath after completion",
			dataPath:          "/data",
			qbDefaultSavePath: "/downloads",
			qbTempPath:        "/temp/incomplete",
			tempDataPath:      "/data/../temp/incomplete",
			torrent: qbittorrent.Torrent{
				SavePath:    "/downloads/movies",
				ContentPath: "/downloads/movies/MyMovie",
				Progress:    1.0,
			},
			want: "/data/movies",
		},
		{
			name:              "empty ContentPath falls back to save dir",
			dataPath:          "/data",
			qbDefaultSavePath: "/downloads",
			torrent: qbittorrent.Torrent{
				SavePath: "/downloads/movies",
				Progress: 0.5,
			},
			want: "/data/movies",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Source{
				dataPath:          tt.dataPath,
				qbDefaultSavePath: tt.qbDefaultSavePath,
				qbTempPath:        tt.qbTempPath,
				tempDataPath:      tt.tempDataPath,
			}
			got := s.resolveReadDir(tt.torrent)
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestReadPiece_ENOENTRetry(t *testing.T) {
	content := []byte("piece data here!")
	files := []*pb.FileInfo{
		{Path: "file.bin", Size: int64(len(content)), Offset: 0},
	}
	makePiece := func(hash string) *pb.Piece {
		return &pb.Piece{TorrentHash: hash, Offset: 0, Size: int64(len(content))}
	}

	t.Run("succeeds on first try", func(t *testing.T) {
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

	t.Run("readPieceMultiFile returns ENOENT for missing file", func(t *testing.T) {
		// Verifies the ENOENT detection that ReadPiece relies on for its
		// evict-and-retry path. Full ReadPiece integration (evict + refetch)
		// requires a real qBittorrent client, so we test the building block.
		emptyDir := t.TempDir()

		s := &Source{}
		_, err := s.readPieceMultiFile("h3", emptyDir, files, 0, int64(len(content)))
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("expected ENOENT, got %v", err)
		}
	})

	t.Run("cache eviction removes entry", func(t *testing.T) {
		// Verifies the cache eviction mechanics that ReadPiece uses when
		// ENOENT triggers a re-query.
		s := &Source{}
		s.fileCache.Store("h4", &cachedMeta{
			files:      files,
			contentDir: "/nonexistent/primary",
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
	data, err := s.readPieceMultiFile("testhash", dir, files, 4, 8)
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
	data, err := s.readPieceMultiFile("testhash", dir, files, 12, 4)
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
	data, err := s.readPieceMultiFile("testhash", dir, files, 2, 4)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != "AABB" {
		t.Errorf("got %q, want %q", string(data), "AABB")
	}
}

func TestFileHandleCache_Get(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.bin")
	if err := os.WriteFile(path, []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}

	var c fileHandleCache

	t.Run("opens and caches a handle", func(t *testing.T) {
		f, err := c.get("h1", path)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		buf := make([]byte, 5)
		n, _ := f.ReadAt(buf, 0)
		if string(buf[:n]) != "hello" {
			t.Errorf("got %q, want %q", string(buf[:n]), "hello")
		}
	})

	t.Run("returns cached handle on second call", func(t *testing.T) {
		f1, _ := c.get("h1", path)
		f2, _ := c.get("h1", path)
		// Same fd should be returned (pointer equality).
		if f1 != f2 {
			t.Error("expected same *os.File pointer for cached handle")
		}
	})

	t.Run("concurrent get deduplicates handles", func(t *testing.T) {
		path2 := filepath.Join(dir, "data2.bin")
		if err := os.WriteFile(path2, []byte("world"), 0o644); err != nil {
			t.Fatal(err)
		}

		var c2 fileHandleCache
		var wg sync.WaitGroup
		results := make([]*os.File, 10)

		for i := range results {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				f, err := c2.get("h2", path2)
				if err != nil {
					t.Errorf("goroutine %d: unexpected error: %v", idx, err)
					return
				}
				results[idx] = f
			}(i)
		}
		wg.Wait()

		// All goroutines should get the same handle.
		for i := 1; i < len(results); i++ {
			if results[i] != results[0] {
				t.Errorf("goroutine %d got different handle than goroutine 0", i)
			}
		}
	})

	t.Run("returns error for nonexistent file", func(t *testing.T) {
		var c2 fileHandleCache
		_, err := c2.get("h3", filepath.Join(dir, "nonexistent"))
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("expected ENOENT, got %v", err)
		}
	})
}

func TestFileHandleCache_Evict(t *testing.T) {
	dir := t.TempDir()
	path1 := filepath.Join(dir, "a.bin")
	path2 := filepath.Join(dir, "b.bin")
	if err := os.WriteFile(path1, []byte("aaa"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path2, []byte("bbb"), 0o644); err != nil {
		t.Fatal(err)
	}

	var c fileHandleCache
	f1, _ := c.get("h1", path1)
	f2, _ := c.get("h1", path2)

	c.evict("h1")

	// Handles should be closed — ReadAt on closed file returns an error.
	buf := make([]byte, 1)
	if _, err := f1.ReadAt(buf, 0); err == nil {
		t.Error("expected error reading from closed handle f1")
	}
	if _, err := f2.ReadAt(buf, 0); err == nil {
		t.Error("expected error reading from closed handle f2")
	}

	// Fresh get should return a new (working) handle.
	f3, err := c.get("h1", path1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	n, _ := f3.ReadAt(buf, 0)
	if string(buf[:n]) != "a" {
		t.Errorf("got %q, want %q", string(buf[:n]), "a")
	}
}

func TestFileHandleCache_EvictPath(t *testing.T) {
	dir := t.TempDir()
	path1 := filepath.Join(dir, "a.bin")
	path2 := filepath.Join(dir, "b.bin")
	if err := os.WriteFile(path1, []byte("aaa"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path2, []byte("bbb"), 0o644); err != nil {
		t.Fatal(err)
	}

	var c fileHandleCache
	f1, _ := c.get("h1", path1)
	f2, _ := c.get("h1", path2)

	c.evictPath("h1", path1)

	// path1 handle should be closed.
	buf := make([]byte, 1)
	if _, err := f1.ReadAt(buf, 0); err == nil {
		t.Error("expected error reading from evicted handle")
	}

	// path2 handle should still work.
	n, err := f2.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(buf[:n]) != "b" {
		t.Errorf("got %q, want %q", string(buf[:n]), "b")
	}
}

func TestReadChunkCached_StaleHandleRetry(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.bin")
	if err := os.WriteFile(path, []byte("original"), 0o644); err != nil {
		t.Fatal(err)
	}

	s := &Source{}

	// Prime the cache with a handle to "original".
	data, err := s.readChunkCached("h1", path, 0, 8)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != "original" {
		t.Fatalf("got %q, want %q", string(data), "original")
	}

	// Replace the file (simulates torrent client rewriting).
	// On Linux, deleting and recreating the file makes the old fd stale for new data,
	// but ReadAt on the old fd still works (reads old content via inode).
	// Instead, we test the cache evictPath codepath directly by evicting and verifying
	// the new handle sees fresh content.
	if err := os.WriteFile(path, []byte("replaced"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Evict and re-read to prove the retry path works.
	s.handles.evictPath("h1", path)
	data, err = s.readChunkCached("h1", path, 0, 8)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != "replaced" {
		t.Errorf("got %q, want %q", string(data), "replaced")
	}
}

func TestEvictCache_ClosesHandles(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.bin")
	if err := os.WriteFile(path, []byte("test"), 0o644); err != nil {
		t.Fatal(err)
	}

	s := &Source{}
	s.fileCache.Store("h1", &cachedMeta{
		files:      []*pb.FileInfo{{Path: "data.bin", Size: 4, Offset: 0}},
		contentDir: dir,
	})

	// Open a handle through the cache.
	f, _ := s.handles.get("h1", path)

	s.EvictCache("h1")

	// fileCache should be empty.
	if _, ok := s.fileCache.Load("h1"); ok {
		t.Error("fileCache should have been evicted")
	}

	// Handle should be closed.
	buf := make([]byte, 1)
	if _, err := f.ReadAt(buf, 0); err == nil {
		t.Error("expected error reading from closed handle")
	}
}
