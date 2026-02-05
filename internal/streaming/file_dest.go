package streaming

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/arsac/qb-sync/internal/utils"
	pb "github.com/arsac/qb-sync/proto"
)

const (
	defaultWrittenSliceSize = 10000
	dirPermissions          = 0o750
	filePermissions         = 0o600
)

// FileDestination implements PieceDestination writing to local files.
// This can be used for local testing or when destination is mounted via NFS.
type FileDestination struct {
	basePath string
	files    map[string]*destinationFile
	mu       sync.Mutex
}

type destinationFile struct {
	file    *os.File
	written []bool // which pieces have been written
	mu      sync.Mutex
}

// NewFileDestination creates a destination that writes to local files.
func NewFileDestination(basePath string) *FileDestination {
	return &FileDestination{
		basePath: basePath,
		files:    make(map[string]*destinationFile),
	}
}

// InitTorrent initializes a torrent on the destination (no-op for file dest).
func (d *FileDestination) InitTorrent(_ context.Context, req *pb.InitTorrentRequest) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	hash := req.GetTorrentHash()
	if _, ok := d.files[hash]; ok {
		return nil // Already initialized
	}

	// Pre-create the file entry with proper piece count
	df, err := d.createFile(hash)
	if err != nil {
		return err
	}

	numPieces := int(req.GetNumPieces())
	if numPieces > 0 {
		df.written = make([]bool, numPieces)
	}

	return nil
}

// WritePiece writes a piece to the destination file.
func (d *FileDestination) WritePiece(_ context.Context, req *pb.WritePieceRequest) error {
	data := req.GetData()
	pieceHash := req.GetPieceHash()

	// Verify piece hash if available (SHA1 is mandated by BitTorrent protocol)
	if err := utils.VerifyPieceHash(data, pieceHash); err != nil {
		return fmt.Errorf("piece %d: %w", req.GetPieceIndex(), err)
	}

	df, err := d.getOrCreateFile(req.GetTorrentHash())
	if err != nil {
		return err
	}

	df.mu.Lock()
	defer df.mu.Unlock()

	// Write at correct offset
	_, err = df.file.WriteAt(data, req.GetOffset())
	if err != nil {
		return fmt.Errorf("writing piece: %w", err)
	}

	// Mark as written
	pieceIndex := int(req.GetPieceIndex())
	if pieceIndex < len(df.written) {
		df.written[pieceIndex] = true
	}

	return nil
}

// GetWrittenPieces returns which pieces have been successfully written.
func (d *FileDestination) GetWrittenPieces(_ context.Context, hash string) ([]bool, error) {
	d.mu.Lock()
	df, ok := d.files[hash]
	d.mu.Unlock()

	if !ok {
		return nil, nil
	}

	df.mu.Lock()
	defer df.mu.Unlock()

	result := make([]bool, len(df.written))
	copy(result, df.written)
	return result, nil
}

// Close closes all open files.
func (d *FileDestination) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	var lastErr error
	for _, df := range d.files {
		if err := df.file.Close(); err != nil {
			lastErr = err
		}
	}
	d.files = make(map[string]*destinationFile)
	return lastErr
}

func (d *FileDestination) getOrCreateFile(hash string) (*destinationFile, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if df, ok := d.files[hash]; ok {
		return df, nil
	}

	return d.createFile(hash)
}

func (d *FileDestination) createFile(hash string) (*destinationFile, error) {
	// Create destination file
	destPath := filepath.Join(d.basePath, hash+".partial")

	// Ensure directory exists
	if err := os.MkdirAll(d.basePath, dirPermissions); err != nil {
		return nil, fmt.Errorf("creating destination directory: %w", err)
	}

	file, err := os.OpenFile(destPath, os.O_RDWR|os.O_CREATE, filePermissions)
	if err != nil {
		return nil, fmt.Errorf("opening destination file: %w", err)
	}

	df := &destinationFile{
		file:    file,
		written: make([]bool, defaultWrittenSliceSize),
	}

	d.files[hash] = df
	return df, nil
}
