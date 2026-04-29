// Package streaming provides the core streaming infrastructure for transferring
// torrent pieces from source to destination. It includes piece
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
	// Returns sync status and pieces_needed for resume.
	InitTorrent(ctx context.Context, req *pb.InitTorrentRequest) (*InitTorrentResult, error)

	// Close closes the destination.
	Close() error
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
