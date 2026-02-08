package utils

import (
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"
)

// inodesUnsupportedErr is returned when the platform does not support inodes.
var inodesUnsupportedErr = errors.New("inodes not supported on this platform")

// FileRegion describes a contiguous region within a file for multi-file piece reading.
type FileRegion struct {
	Path   string // Absolute path to the file
	Offset int64  // Byte offset where this file's data starts in the torrent
	Size   int64  // Total size of this file
}

// ReadPieceFromFiles reads piece data that may span multiple files.
// Files must be ordered by offset.
func ReadPieceFromFiles(files []FileRegion, pieceOffset, pieceSize int64) ([]byte, error) {
	data := make([]byte, 0, pieceSize)
	remaining := pieceSize
	currentOffset := pieceOffset

	for _, f := range files {
		if remaining <= 0 {
			break
		}

		fileEnd := f.Offset + f.Size
		if fileEnd <= currentOffset {
			continue
		}

		fileReadOffset := max(currentOffset-f.Offset, 0)
		availableInFile := f.Size - fileReadOffset
		toRead := min(remaining, availableInFile)

		chunk, err := ReadChunkFromFile(f.Path, fileReadOffset, toRead)
		if err != nil {
			return nil, fmt.Errorf("reading from %s at offset %d: %w", f.Path, fileReadOffset, err)
		}

		data = append(data, chunk...)
		remaining -= int64(len(chunk))
		currentOffset += int64(len(chunk))
	}

	return data, nil
}

// ReadChunkFromFile reads a chunk of data from a file at a specific offset.
func ReadChunkFromFile(path string, offset, size int64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	data := make([]byte, size)
	n, err := file.ReadAt(data, offset)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}

	return data[:n], nil
}

func AreHardlinked(path1, path2 string) (bool, error) {
	info1, err := os.Stat(path1)
	if err != nil {
		return false, err
	}

	info2, err := os.Stat(path2)
	if err != nil {
		return false, err
	}

	return os.SameFile(info1, info2), nil
}

// GetInode returns the inode number for a file.
func GetInode(path string) (uint64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}

	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, inodesUnsupportedErr
	}

	return stat.Ino, nil
}

func FileExistsWithSize(path string, expectedSize int64) (bool, error) {
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return info.Size() == expectedSize, nil
}
