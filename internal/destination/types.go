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
//	hardlink.sourceInode, hardlink.sourcePath, hardlink.doneCh
//
// Mutable per-file (require state.mu):
//
//	file, path, earlyFinalized, piecesWritten,
//	hardlink.state (transitions through hardlink state machine during finalization)
type torrentMeta struct {
	pieceHashes []string          // SHA1 hashes per piece for verification
	pieceLength int64             // Size of each piece (last piece may be smaller)
	totalSize   int64             // Total size of all files combined
	files       []*serverFileInfo // Files in this torrent (slice immutable, elements partly mutable)
}

// numPieces returns the number of pieces derived from piece geometry.
func (m *torrentMeta) numPieces() int32 {
	if m.pieceLength <= 0 {
		return 0
	}
	return int32((m.totalSize + m.pieceLength - 1) / m.pieceLength)
}

// computeFilePieceRanges sets firstPiece, lastPiece, and piecesTotal on each file
// based on its offset/size and the torrent's piece geometry.
func (m *torrentMeta) computeFilePieceRanges() {
	if m.pieceLength <= 0 {
		return
	}
	maxPiece := int((m.totalSize - 1) / m.pieceLength)
	for _, f := range m.files {
		if f.size <= 0 {
			continue
		}
		f.firstPiece = int(f.offset / m.pieceLength)
		f.lastPiece = min(int((f.offset+f.size-1)/m.pieceLength), maxPiece)
		f.piecesTotal = f.lastPiece - f.firstPiece + 1
	}
}

// initFilePieceCounts initializes piecesWritten on each file from the existing written bitmap.
func (m *torrentMeta) initFilePieceCounts(written []bool) {
	for _, f := range m.files {
		if f.earlyFinalized || f.size <= 0 {
			continue
		}
		f.recalcPiecesWritten(written)
	}
}

// calculatePiecesCovered determines which pieces are fully covered by hardlinked, pending,
// or unselected files (none of these need data streamed from source).
func (m *torrentMeta) calculatePiecesCovered() []bool {
	numPieces := m.numPieces()
	piecesCovered := make([]bool, numPieces)
	for pieceIdx := range numPieces {
		pieceStart := int64(pieceIdx) * m.pieceLength
		pieceEnd := min(pieceStart+m.pieceLength, m.totalSize)

		// Piece is covered if every overlapping file is hardlinked, pending, or unselected.
		covered := true
		for _, f := range m.files {
			if f.offset < pieceEnd && f.offset+f.size > pieceStart && !f.skipForWriteData() {
				covered = false
				break
			}
		}
		piecesCovered[pieceIdx] = covered
	}
	return piecesCovered
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
	// All fields are mutable and require state.mu.
	// Use start()/reset()/storeResult() methods for transitions.
	finalization finalizationState

	// Cached for re-initialization (hardlink info for logging)
	hardlinkResults []*pb.HardlinkResult
}

// finalizationState tracks the background finalization lifecycle.
// All fields are mutable and require the parent serverTorrentState.mu:
//   - active: set during FinalizeTorrent, cleared on completion or failure
//   - done: created when active becomes true, closed when background work ends
//   - result: nil until background goroutine stores success or failure
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
// Use applyOutcome() during init and markComplete() during finalization.
type hardlinkInfo struct {
	state       hardlinkState // Current state in the hardlink state machine
	sourceInode Inode         // Source inode for registration (immutable after init)
	sourcePath  string        // Relative path to hardlink from (immutable after init)
	doneCh      chan struct{} // Wait on this before hardlinking (immutable after init)
}

// applyOutcome sets the hardlink state from an init-time resolution outcome.
// Called during setupFiles when a file's hardlink is resolved.
func (h *hardlinkInfo) applyOutcome(outcome hardlinkOutcome) {
	h.state = outcome.state
	if outcome.state == hlStatePending {
		h.sourcePath = outcome.sourcePath
		h.doneCh = outcome.doneCh
	}
}

// markComplete transitions the hardlink state to Complete.
// Called during finalization after a pending hardlink is resolved.
// Caller must hold state.mu.
func (h *hardlinkInfo) markComplete() {
	h.state = hlStateComplete
}

// serverFileInfo holds information about a file in a torrent.
//
// Immutable fields (set during init, safe to read without state.mu):
//
//	offset, size, selected, firstPiece, lastPiece, piecesTotal,
//	hardlink.sourceInode, hardlink.sourcePath, hardlink.doneCh
//
// Mutable fields (require state.mu):
//
//	path, file, earlyFinalized, piecesWritten, hardlink.state
type serverFileInfo struct {
	path   string   // Full path on disk (mutable: renamed during early finalize)
	size   int64    // File size
	offset int64    // Offset within torrent's total data
	file   *os.File // Open file handle (lazy opened, mutable)

	// Hardlink tracking (state machine for cross-torrent file dedup)
	hardlink hardlinkInfo

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
// True for unselected files, or files that are hardlinked/pending hardlink.
func (f *serverFileInfo) skipForWriteData() bool {
	return !f.selected || f.hardlink.state == hlStateComplete || f.hardlink.state == hlStatePending
}

// overlaps reports whether the given piece index falls within this file's piece range.
func (f *serverFileInfo) overlaps(pieceIdx int) bool {
	return pieceIdx >= f.firstPiece && pieceIdx <= f.lastPiece
}

// recalcPiecesWritten recomputes piecesWritten from the torrent's written bitmap.
func (f *serverFileInfo) recalcPiecesWritten(written []bool) {
	f.piecesWritten = 0
	for p := f.firstPiece; p <= f.lastPiece; p++ {
		if written[p] {
			f.piecesWritten++
		}
	}
}

// torrentRef is a reference to a torrent state for safe iteration.
// Used when collecting torrents under s.mu to process under individual state.mu.
type torrentRef struct {
	hash  string
	state *serverTorrentState
}
