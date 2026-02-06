package qbclient

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/arsac/qb-sync/internal/utils"
	pb "github.com/arsac/qb-sync/proto"
)

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
