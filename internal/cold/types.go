package cold

import (
	"os"
	"sync"

	pb "github.com/arsac/qb-sync/proto"
)

// Inode represents a filesystem inode number.
type Inode uint64

// hardlinkState represents the state of hardlink resolution for a file.
// States are ordered by typical progression: None -> InProgress/Pending -> Complete.
type hardlinkState int

const (
	hlStateNone       hardlinkState = iota // No hardlink resolution has occurred
	hlStateInProgress                      // This torrent is the first writer for this inode
	hlStatePending                         // Waiting for another torrent to finish writing
	hlStateComplete                        // Successfully hardlinked from a registered inode
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
	writtenCount     int               // Number of true entries in written (maintained for O(1) checks)
	pieceHashes      []string          // SHA1 hashes per piece for verification
	pieceLength      int64             // Size of each piece (last piece may be smaller)
	totalSize        int64             // Total size of all files combined
	files            []*serverFileInfo // Files in this torrent
	torrentPath      string            // Path to stored .torrent file
	statePath        string            // Path to written pieces state file
	saveSubPath      string            // Relative sub-path prefix (e.g., "movies" from category)
	dirty            bool              // Whether state needs to be flushed
	piecesSinceFlush int               // Pieces written since last flush (for count-based trigger)
	finalizing       bool              // True during FinalizeTorrent to prevent concurrent writes
	initializing     bool              // True while disk I/O is in progress during InitTorrent
	mu               sync.Mutex

	// Cached for re-initialization (hardlink info for logging)
	hardlinkResults []*pb.HardlinkResult
}

// serverFileInfo holds information about a file in a torrent.
type serverFileInfo struct {
	path   string   // Full path on disk
	size   int64    // File size
	offset int64    // Offset within torrent's total data
	file   *os.File // Open file handle (lazy opened)

	// Hardlink tracking
	hlState        hardlinkState // Current state in the hardlink state machine
	sourceInode    Inode         // Source inode for registration
	hardlinkSource string        // Relative path to hardlink from (when ready)
	hardlinkDoneCh chan struct{} // Wait on this before hardlinking (used when hlState == hlStatePending)
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
	PieceHashes []string            `json:"pieceHashes"`    // SHA1 hashes per piece for post-restart verification
	SaveSubPath string              `json:"saveSubPath,omitempty"` // Relative sub-path prefix (e.g., "movies")
}

// torrentRef is a reference to a torrent state for safe iteration.
// Used when collecting torrents under s.mu to process under individual state.mu.
type torrentRef struct {
	hash  string
	state *serverTorrentState
}
