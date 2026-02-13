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

// torrentMeta holds piece geometry and hash data that is immutable after initialization.
// All fields are set once during initNewTorrent (or recoverTorrentState) and never modified.
// Safe to read without holding state.mu.
//
// Note: the files slice reference is immutable (never appended/removed), but individual
// serverFileInfo fields have varying mutability:
//
// Immutable per-file (set during init, never modified):
//
//	offset, size, selected, firstPiece, lastPiece, piecesTotal,
//	hl.sourceInode, hl.sourcePath, hl.doneCh
//
// Mutable per-file (require state.mu):
//
//	file, path, earlyFinalized, piecesWritten,
//	hl.state (transitions through hardlink state machine during finalization)
type torrentMeta struct {
	pieceHashes []string          // SHA1 hashes per piece for verification
	pieceLength int64             // Size of each piece (last piece may be smaller)
	totalSize   int64             // Total size of all files combined
	files       []*serverFileInfo // Files in this torrent (slice immutable, elements partly mutable)
}

// serverTorrentState holds the state for a torrent being received.
type serverTorrentState struct {
	torrentMeta // Immutable metadata (safe to read without mu)

	// Immutable after init (set once during initNewTorrent or recoverTorrentState):
	info      *pb.InitTorrentRequest // Original init request; nil when recovered from disk
	statePath string                 // Path to written pieces state file

	// Mutable state (require state.mu):
	torrentPath      string // Path to stored .torrent file; may be set late during resumeTorrent
	saveSubPath      string // Relative sub-path prefix; may change if category relocates at finalize
	written          []bool
	writtenCount     int    // Number of true entries in written (maintained for O(1) checks)
	dirty            bool   // Whether state needs to be flushed
	piecesSinceFlush int    // Pieces written since last flush (for count-based trigger)
	flushGen         uint64 // Monotonic counter incremented on every successful state flush
	initializing     bool   // True while disk I/O is in progress during InitTorrent
	mu               sync.Mutex

	// Finalization lifecycle (state machine: inactive -> active -> result stored).
	// Require state.mu. Use start()/reset()/storeResult() methods.
	finalization finalizationState

	// Cached for re-initialization (hardlink info for logging)
	hardlinkResults []*pb.HardlinkResult
}

// finalizationState tracks the background finalization lifecycle.
// States: inactive (active=false) -> active (active=true, done open) -> complete (result set, done closed).
type finalizationState struct {
	active bool            // True during FinalizeTorrent to prevent concurrent writes
	done   chan struct{}   // Closed when background finalization completes
	result *finalizeResult // Result of background finalization (nil = not started)
}

// start marks finalization as active and returns the done channel.
// Caller must hold state.mu.
func (f *finalizationState) start() chan struct{} {
	done := make(chan struct{})
	f.active = true
	f.done = done
	return done
}

// reset clears all finalization state back to inactive.
// Caller must hold state.mu.
func (f *finalizationState) reset() {
	f.active = false
	f.result = nil
	f.done = nil
}

// storeResult records the outcome of background finalization.
// Caller must hold state.mu.
func (f *finalizationState) storeResult(r *finalizeResult) {
	f.result = r
}

// finalizeResult stores the outcome of a background finalization.
type finalizeResult struct {
	success   bool
	state     string               // qBittorrent state on success
	err       string               // Error message on failure
	errorCode pb.FinalizeErrorCode // Structured error code for retry decisions
}

// hardlinkInfo tracks the hardlink resolution state for a single file.
// State machine: None -> InProgress (first writer) or Pending (wait for another) -> Complete.
//
// sourceInode, sourcePath, and doneCh are immutable after init.
// state is mutable and requires the parent serverTorrentState.mu.
type hardlinkInfo struct {
	state       hardlinkState // Current state in the hardlink state machine
	sourceInode Inode         // Source inode for registration (immutable after init)
	sourcePath  string        // Relative path to hardlink from (immutable after init)
	doneCh      chan struct{} // Wait on this before hardlinking (immutable after init)
}

// serverFileInfo holds information about a file in a torrent.
type serverFileInfo struct {
	path   string   // Full path on disk
	size   int64    // File size
	offset int64    // Offset within torrent's total data
	file   *os.File // Open file handle (lazy opened)

	// Hardlink tracking (state machine for cross-torrent file dedup)
	hl hardlinkInfo

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
	return fi.hl.state == hlStateComplete || fi.hl.state == hlStatePending || !fi.selected
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

// torrentRef is a reference to a torrent state for safe iteration.
// Used when collecting torrents under s.mu to process under individual state.mu.
type torrentRef struct {
	hash  string
	state *serverTorrentState
}
