// Package streaming provides the core streaming infrastructure for transferring
// torrent pieces from hot (source) to cold (destination). It includes piece
// monitoring, gRPC client/server communication, adaptive congestion control,
// and connection pooling for high-throughput bidirectional streaming.
package streaming

import (
	"context"
	"errors"

	pb "github.com/arsac/qb-sync/proto"
)

// ErrTorrentNotFound indicates the torrent no longer exists in qBittorrent.
// This typically means the torrent was deleted by the user.
var ErrTorrentNotFound = errors.New("torrent not found in qBittorrent")

// PieceState represents the download state of a piece.
type PieceState int

const (
	PieceStateNotDownloaded PieceState = 0
	PieceStateDownloading   PieceState = 1
	PieceStateDownloaded    PieceState = 2
)

// TorrentMetadata contains torrent info with source-side context.
// Embeds the proto InitTorrentRequest for wire-format compatibility,
// plus local path info needed by the source.
type TorrentMetadata struct {
	*pb.InitTorrentRequest

	ContentDir string // Read directory (source-side only)
}

// PieceSource provides piece metadata and content from the source.
type PieceSource interface {
	// GetPieceStates returns the current state of all pieces for a torrent.
	GetPieceStates(ctx context.Context, hash string) ([]PieceState, error)

	// GetPieceHashes returns the expected SHA1 hash for each piece.
	GetPieceHashes(ctx context.Context, hash string) ([]string, error)

	// GetTorrentMetadata returns metadata needed for streaming.
	GetTorrentMetadata(ctx context.Context, hash string) (*TorrentMetadata, error)

	// ReadPiece reads a piece's data from disk.
	ReadPiece(ctx context.Context, piece *pb.Piece) ([]byte, error)
}

// PieceDestination receives streamed pieces at the destination.
type PieceDestination interface {
	// InitTorrent initializes a torrent on the destination.
	// Must be called before WritePiece for a given torrent.
	// Returns sync status and pieces_needed for resume.
	InitTorrent(ctx context.Context, req *pb.InitTorrentRequest) (*InitTorrentResult, error)

	// WritePiece writes a piece to the destination.
	WritePiece(ctx context.Context, req *pb.WritePieceRequest) error

	// Close closes the destination.
	Close() error
}

// HardlinkDestination extends PieceDestination with hardlink support.
// Used to avoid re-transferring files that are hardlinked on the source.
type HardlinkDestination interface {
	PieceDestination

	// GetFileByInode checks if a file with the given inode exists on the destination.
	// Returns the path if found, allowing the caller to create a hardlink instead of transferring.
	GetFileByInode(ctx context.Context, inode uint64) (path string, found bool, err error)

	// CreateHardlink creates a hardlink from an existing file to a new path.
	// This operation should be idempotent.
	CreateHardlink(ctx context.Context, sourcePath, targetPath string) error

	// RegisterFile registers a completed file's inode for future hardlink lookups.
	RegisterFile(ctx context.Context, inode uint64, path string, size int64) error
}

// StreamProgress tracks streaming progress for a torrent.
type StreamProgress struct {
	TorrentHash  string
	TotalPieces  int
	Streamed     int
	Failed       int
	InFlight     int
	BytesSent    int64
	BytesPerSec  float64
	Complete     bool
	LastError    error
	LastPieceIdx int
}
