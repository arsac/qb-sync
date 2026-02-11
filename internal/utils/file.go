package utils

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

// errInodesUnsupported is returned when the platform does not support inodes.
var errInodesUnsupported = errors.New("inodes not supported on this platform")

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

// AreHardlinked reports whether two paths refer to the same underlying file (i.e., share an inode).
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
		return 0, errInodesUnsupported
	}

	return stat.Ino, nil
}

// AtomicWriteFile writes data to a file atomically using write-to-temp + fsync + rename.
// This prevents corruption from crashes or NFS connection drops mid-write.
func AtomicWriteFile(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return fmt.Errorf("creating temp file: %w", err)
	}
	tmpPath := tmp.Name()

	// Clean up temp file on any failure path
	success := false
	defer func() {
		if !success {
			_ = tmp.Close()
			_ = os.Remove(tmpPath)
		}
	}()

	if _, writeErr := tmp.Write(data); writeErr != nil {
		return fmt.Errorf("writing temp file: %w", writeErr)
	}
	if syncErr := tmp.Sync(); syncErr != nil {
		return fmt.Errorf("syncing temp file: %w", syncErr)
	}
	if closeErr := tmp.Close(); closeErr != nil {
		return fmt.Errorf("closing temp file: %w", closeErr)
	}
	if chmodErr := os.Chmod(tmpPath, perm); chmodErr != nil {
		return fmt.Errorf("setting permissions: %w", chmodErr)
	}
	if renameErr := os.Rename(tmpPath, path); renameErr != nil {
		return fmt.Errorf("renaming temp file: %w", renameErr)
	}

	success = true
	return nil
}
