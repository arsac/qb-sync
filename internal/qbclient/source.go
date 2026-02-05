package qbclient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/autobrr/go-qbittorrent"

	"github.com/arsac/qb-sync/internal/streaming"
	"github.com/arsac/qb-sync/internal/utils"
	pb "github.com/arsac/qb-sync/proto"
)

// Source implements streaming.PieceSource using qBittorrent API.
type Source struct {
	client      *ResilientClient
	contentPath string // Base path where torrent content is stored
}

// NewSource creates a new qBittorrent piece source with resilient client.
func NewSource(client *ResilientClient, contentPath string) *Source {
	return &Source{
		client:      client,
		contentPath: contentPath,
	}
}

// GetPieceStates returns the current state of all pieces for a torrent.
// Returns ErrTorrentNotFound if the torrent no longer exists (e.g., was deleted).
// Uses resilient client with automatic retry for transient errors.
func (s *Source) GetPieceStates(ctx context.Context, hash string) ([]streaming.PieceState, error) {
	states, err := s.client.GetTorrentPieceStatesCtx(ctx, hash)
	if err != nil {
		// Detect 404 errors which indicate the torrent was deleted.
		// The go-qbittorrent library returns "unexpected status: 404" for deleted torrents.
		// Note: 404 errors are NOT retried by ResilientClient (non-retriable).
		if strings.Contains(err.Error(), "404") {
			return nil, fmt.Errorf("getting piece states: %w", streaming.ErrTorrentNotFound)
		}
		return nil, fmt.Errorf("getting piece states: %w", err)
	}

	result := make([]streaming.PieceState, len(states))
	for i, state := range states {
		result[i] = streaming.PieceState(state)
	}
	return result, nil
}

// GetPieceHashes returns the expected SHA1 hash for each piece.
// Uses resilient client with automatic retry for transient errors.
func (s *Source) GetPieceHashes(ctx context.Context, hash string) ([]string, error) {
	hashes, err := s.client.GetTorrentPieceHashesCtx(ctx, hash)
	if err != nil {
		return nil, fmt.Errorf("getting piece hashes: %w", err)
	}
	return hashes, nil
}

// GetTorrentMetadata returns metadata needed for streaming.
// Uses resilient client with automatic retry for transient errors.
func (s *Source) GetTorrentMetadata(ctx context.Context, hash string) (*streaming.TorrentMetadata, error) {
	// Get torrent properties (with retry)
	props, err := s.client.GetTorrentPropertiesCtx(ctx, hash)
	if err != nil {
		return nil, fmt.Errorf("getting torrent properties: %w", err)
	}

	// Get torrent info from list (with retry)
	torrents, err := s.client.GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
		Hashes: []string{hash},
	})
	if err != nil {
		return nil, fmt.Errorf("getting torrent info: %w", err)
	}
	if len(torrents) == 0 {
		return nil, fmt.Errorf("torrent not found: %s", hash)
	}
	torrent := torrents[0]

	// Get files (with retry)
	qbFiles, err := s.client.GetFilesInformationCtx(ctx, hash)
	if err != nil {
		return nil, fmt.Errorf("getting torrent files: %w", err)
	}

	// Sort files by Index to ensure correct offset calculation.
	// qBittorrent may return files in any order, but torrent piece data
	// is laid out according to the file order in the .torrent metadata.
	// Note: Using sort.Slice because TorrentFiles has anonymous struct elements.
	sortedQBFiles := make(qbittorrent.TorrentFiles, len(*qbFiles))
	copy(sortedQBFiles, *qbFiles)
	sort.Slice(sortedQBFiles, func(i, j int) bool {
		return sortedQBFiles[i].Index < sortedQBFiles[j].Index
	})

	files := make([]*pb.FileInfo, len(sortedQBFiles))
	var offset int64
	for i, f := range sortedQBFiles {
		files[i] = &pb.FileInfo{
			Path:   f.Name,
			Size:   f.Size,
			Offset: offset,
		}

		// Get inode for hardlink detection
		// Use contentPath (host path) instead of torrent.ContentPath (container path)
		// f.Name is relative to save_path, which maps to contentPath on the host
		filePath := filepath.Join(s.contentPath, f.Name)
		if inode, inodeErr := utils.GetInode(filePath); inodeErr == nil {
			files[i].Inode = inode
		}

		offset += f.Size
	}

	pieceSize := int64(props.PieceSize)
	numPieces := props.PiecesNum
	if numPieces == 0 && pieceSize > 0 {
		numPieces = int((torrent.Size + pieceSize - 1) / pieceSize)
	}

	// Validate piece count fits in int32 (protobuf field type)
	if numPieces > math.MaxInt32 {
		return nil, fmt.Errorf("piece count %d exceeds maximum supported value", numPieces)
	}

	// Export the raw .torrent file (with retry)
	torrentFile, exportErr := s.client.ExportTorrentCtx(ctx, hash)
	if exportErr != nil {
		return nil, fmt.Errorf("exporting torrent: %w", exportErr)
	}

	// Get piece hashes for verification (with retry)
	pieceHashes, hashErr := s.client.GetTorrentPieceHashesCtx(ctx, hash)
	if hashErr != nil {
		return nil, fmt.Errorf("getting piece hashes: %w", hashErr)
	}

	return &streaming.TorrentMetadata{
		InitTorrentRequest: &pb.InitTorrentRequest{
			TorrentHash: hash,
			Name:        torrent.Name,
			PieceSize:   pieceSize,
			TotalSize:   torrent.Size,
			NumPieces:   int32(numPieces),
			Files:       files,
			TorrentFile: torrentFile,
			PieceHashes: pieceHashes,
		},
		// Use contentPath (host path) instead of torrent.ContentPath (container path).
		// File paths in files[] are relative to this directory.
		ContentDir: s.contentPath,
	}, nil
}

// ReadPiece reads a piece's data from disk.
func (s *Source) ReadPiece(ctx context.Context, piece *pb.Piece) ([]byte, error) {
	// Get torrent metadata to find content path
	meta, err := s.GetTorrentMetadata(ctx, piece.GetTorrentHash())
	if err != nil {
		return nil, err
	}

	// For single-file torrents, ContentDir is the file itself
	// For multi-file torrents, ContentDir is the directory
	contentPath := meta.ContentDir
	files := meta.GetFiles()

	// If it's a single file torrent (one file that matches total size)
	if len(files) == 1 && files[0].GetSize() == meta.GetTotalSize() {
		return s.readPieceFromFile(contentPath, piece.GetOffset(), piece.GetSize())
	}

	// Multi-file torrent: piece may span multiple files
	return s.readPieceMultiFile(contentPath, files, piece.GetOffset(), piece.GetSize())
}

func (s *Source) readPieceFromFile(path string, offset, size int64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening file: %w", err)
	}
	defer file.Close()

	data := make([]byte, size)
	n, err := file.ReadAt(data, offset)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("reading piece: %w", err)
	}

	return data[:n], nil
}

func (s *Source) readPieceMultiFile(
	basePath string,
	files []*pb.FileInfo,
	offset, size int64,
) ([]byte, error) {
	data := make([]byte, 0, size)
	remaining := size
	currentOffset := offset

	for _, file := range files {
		fileEnd := file.GetOffset() + file.GetSize()

		// Skip files before our offset
		if fileEnd <= currentOffset {
			continue
		}

		// Stop if we've read everything
		if remaining <= 0 {
			break
		}

		// Calculate read position within this file
		fileReadOffset := max(currentOffset-file.GetOffset(), 0)

		// Calculate how much to read from this file
		availableInFile := file.GetSize() - fileReadOffset
		toRead := min(remaining, availableInFile)

		// Read from file
		filePath := filepath.Join(basePath, file.GetPath())
		chunk, err := s.readPieceFromFile(filePath, fileReadOffset, toRead)
		if err != nil {
			return nil, fmt.Errorf("reading from %s: %w", file.GetPath(), err)
		}

		data = append(data, chunk...)
		remaining -= int64(len(chunk))
		currentOffset += int64(len(chunk))
	}

	return data, nil
}
