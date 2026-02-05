package cold

import (
	"os"
	"sync"

	pb "github.com/arsac/qb-sync/proto"
)

// inProgressInode tracks a file that is currently being written by a torrent.
// Other torrents with the same source inode can wait on doneCh and then hardlink.
type inProgressInode struct {
	targetPath  string        // Relative path to final file (without .partial)
	doneCh      chan struct{} // Closed when file is finalized
	torrentHash string        // Torrent that's writing this file
	closeOnce   sync.Once     // Ensures doneCh is closed exactly once
}

// close safely closes doneCh exactly once, preventing double-close panics.
func (i *inProgressInode) close() {
	i.closeOnce.Do(func() {
		close(i.doneCh)
	})
}

// serverTorrentState holds the state for a torrent being received.
type serverTorrentState struct {
	info             *pb.InitTorrentRequest
	written          []bool
	pieceHashes      []string          // SHA1 hashes per piece for verification
	files            []*serverFileInfo // Files in this torrent
	torrentPath      string            // Path to stored .torrent file
	statePath        string            // Path to written pieces state file
	dirty            bool              // Whether state needs to be flushed
	piecesSinceFlush int               // Pieces written since last flush (for count-based trigger)
	finalizing       bool              // True during FinalizeTorrent to prevent concurrent writes
	mu               sync.Mutex

	// Cached for re-initialization
	hardlinkResults []*pb.HardlinkResult
	piecesCovered   []bool
}

// serverFileInfo holds information about a file in a torrent.
type serverFileInfo struct {
	path   string   // Full path on disk
	size   int64    // File size
	offset int64    // Offset within torrent's total data
	file   *os.File // Open file handle (lazy opened)

	// Hardlink tracking
	hardlinked      bool          // Already hardlinked (from registered inode)
	sourceInode     uint64        // Source inode for registration
	pendingHardlink bool          // Waiting for another torrent's file
	hardlinkSource  string        // Relative path to hardlink from (when ready)
	hardlinkDoneCh  chan struct{} // Wait on this before hardlinking
}

// persistedFileInfo stores file information for recovery after restart.
type persistedFileInfo struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	Offset int64  `json:"offset"`
}

// persistedTorrentInfo stores torrent metadata for recovery after restart.
type persistedTorrentInfo struct {
	Name        string              `json:"name"`
	NumPieces   int                 `json:"numPieces"`
	PieceLength int64               `json:"pieceLength"`
	TotalSize   int64               `json:"totalSize"`
	Files       []persistedFileInfo `json:"files"`
}

// streamWork represents a piece to be processed by a worker.
type streamWork struct {
	req *pb.WritePieceRequest
}

// torrentRef is a reference to a torrent state for safe iteration.
// Used when collecting torrents under s.mu to process under individual state.mu.
type torrentRef struct {
	hash  string
	state *serverTorrentState
}
