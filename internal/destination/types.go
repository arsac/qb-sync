package destination

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
	finalizeDone     chan struct{}     // Closed when background finalization completes
	finalizeResult   *finalizeResult   // Result of background finalization (nil = not started)
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

	// File selection (priority > 0 on source side)
	selected bool // True if file is selected for download; unselected files have no .partial

	// Per-file piece tracking (computed during init, not persisted)
	firstPiece     int  // First piece index overlapping this file
	lastPiece      int  // Last piece index overlapping this file (inclusive)
	piecesTotal    int  // Total number of overlapping pieces
	piecesWritten  int  // Count of overlapping pieces already written
	earlyFinalized bool // True after sync+close+rename before torrent finalization
}

// skipForWriteData reports whether this file should be skipped during piece write.
// True for files that are hardlinked, pending hardlink, or unselected.
func (fi *serverFileInfo) skipForWriteData() bool {
	return fi.hlState == hlStateComplete || fi.hlState == hlStatePending || !fi.selected
}

// overlaps reports whether the given piece index falls within this file's piece range.
func (fi *serverFileInfo) overlaps(pieceIdx int) bool {
	return pieceIdx >= fi.firstPiece && pieceIdx <= fi.lastPiece
}

// recalcPiecesWritten recomputes piecesWritten from the torrent's written bitmap.
func (fi *serverFileInfo) recalcPiecesWritten(written []bool) {
	fi.piecesWritten = 0
	for p := fi.firstPiece; p <= fi.lastPiece; p++ {
		if written[p] {
			fi.piecesWritten++
		}
	}
}

// startFinalization marks the torrent as finalizing and returns the done channel.
// Caller must hold state.mu.
func (s *serverTorrentState) startFinalization() chan struct{} {
	done := make(chan struct{})
	s.finalizing = true
	s.finalizeDone = done
	return done
}

// resetFinalization clears all finalization state.
// Caller must hold state.mu.
func (s *serverTorrentState) resetFinalization() {
	s.finalizing = false
	s.finalizeResult = nil
	s.finalizeDone = nil
}

// finalizeResult stores the outcome of a background finalization.
type finalizeResult struct {
	success   bool
	state     string               // qBittorrent state on success
	err       string               // Error message on failure
	errorCode pb.FinalizeErrorCode // Structured error code for retry decisions
}

// torrentRef is a reference to a torrent state for safe iteration.
// Used when collecting torrents under s.mu to process under individual state.mu.
type torrentRef struct {
	hash  string
	state *serverTorrentState
}
