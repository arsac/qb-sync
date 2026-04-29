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
// Files must be ordered by offset. Allocates one buffer of pieceSize bytes
// and reads each file's contribution into the appropriate slice — no per-chunk
// allocation or append/copy.
func ReadPieceFromFiles(files []FileRegion, pieceOffset, pieceSize int64) ([]byte, error) {
	buf := make([]byte, pieceSize)
	written := int64(0)
	currentOffset := pieceOffset

	for _, f := range files {
		if written >= pieceSize {
			break
		}

		fileEnd := f.Offset + f.Size
		if fileEnd <= currentOffset {
			continue
		}

		fileReadOffset := max(currentOffset-f.Offset, 0)
		availableInFile := f.Size - fileReadOffset
		toRead := min(pieceSize-written, availableInFile)

		if err := readChunkInto(f.Path, fileReadOffset, buf[written:written+toRead]); err != nil {
			return nil, fmt.Errorf("reading from %s at offset %d: %w", f.Path, fileReadOffset, err)
		}

		written += toRead
		currentOffset += toRead
	}

	if written < pieceSize {
		return nil, fmt.Errorf("short read: got %d bytes, want %d", written, pieceSize)
	}
	return buf, nil
}

// ReadChunkFromFile reads a chunk of data from a file at a specific offset.
func ReadChunkFromFile(path string, offset, size int64) ([]byte, error) {
	data := make([]byte, size)
	if err := readChunkInto(path, offset, data); err != nil {
		return nil, err
	}
	return data, nil
}

// readChunkInto reads len(buf) bytes from path starting at offset directly
// into buf. Returns an error on short read or any error other than [io.EOF]
// at the end of the read.
func readChunkInto(path string, offset int64, buf []byte) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	n, err := file.ReadAt(buf, offset)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if n < len(buf) {
		return fmt.Errorf("short read from %s at offset %d: got %d bytes, want %d", path, offset, n, len(buf))
	}
	return nil
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

// GetFileID returns the device and inode numbers for a file.
// statDev wraps the platform-specific Stat_t.Dev conversion so the wire-format
// device ID is stable: Linux Stat_t.Dev is already uint64, Darwin's int32 is
// zero-extended (not sign-extended) into uint64.
func GetFileID(path string) (uint64, uint64, error) {
	info, statErr := os.Stat(path)
	if statErr != nil {
		return 0, 0, statErr
	}

	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0, errInodesUnsupported
	}

	return statDev(stat), stat.Ino, nil
}

// AtomicWriteFile writes data to a file atomically using write-to-temp + fsync + chmod + rename.
// fsync ensures data is durable before the rename publishes the new content; this is the
// safe choice for source-of-truth files (.meta, .finalized markers, completion cache).
func AtomicWriteFile(path string, data []byte, perm os.FileMode) error {
	return writeTempThenRename(path, data, func(tmp *os.File) error {
		if syncErr := tmp.Sync(); syncErr != nil {
			return fmt.Errorf("syncing temp file: %w", syncErr)
		}
		if closeErr := tmp.Close(); closeErr != nil {
			return fmt.Errorf("closing temp file: %w", closeErr)
		}
		if chmodErr := os.Chmod(tmp.Name(), perm); chmodErr != nil {
			return fmt.Errorf("setting permissions: %w", chmodErr)
		}
		return nil
	})
}

// AtomicWriteFileNoSync writes data atomically without fsync or chmod. Use for
// regenerable checkpoint files (e.g., .state bitsets) where a torn write at
// crash time is acceptable because recovery code re-derives the missing data.
// Saves ~2 NFS round-trips per call (the fsync commit and the setattr).
func AtomicWriteFileNoSync(path string, data []byte) error {
	return writeTempThenRename(path, data, func(tmp *os.File) error {
		if closeErr := tmp.Close(); closeErr != nil {
			return fmt.Errorf("closing temp file: %w", closeErr)
		}
		return nil
	})
}

// writeTempThenRename creates a temp file in path's directory, writes data,
// runs finalize (which must close the file and may sync/chmod), then renames
// over path. On any failure before the rename, the temp file is closed and
// removed. finalize takes ownership of closing tmp.
func writeTempThenRename(path string, data []byte, finalize func(tmp *os.File) error) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return fmt.Errorf("creating temp file: %w", err)
	}
	tmpPath := tmp.Name()

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
	if finalizeErr := finalize(tmp); finalizeErr != nil {
		return finalizeErr
	}
	if renameErr := os.Rename(tmpPath, path); renameErr != nil {
		return fmt.Errorf("renaming temp file: %w", renameErr)
	}

	success = true
	return nil
}
