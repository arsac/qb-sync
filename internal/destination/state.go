package destination

import (
	"fmt"
	"os"
	"sort"

	"github.com/bits-and-blooms/bitset"

	pb "github.com/arsac/qb-sync/proto"
)

// openForWrite lazily opens the file for writing, creating and pre-allocating it if needed.
// Protected by fileMu so it can be called outside state.mu for concurrent disk I/O.
//
// Declines to open an early-finalized file: its data is complete, verified and
// renamed into place, so opening f.path would recreate the .partial the rename
// consumed. writeAt turns that into a dropped write.
func (f *serverFileInfo) openForWrite() error {
	f.fileMu.Lock()
	defer f.fileMu.Unlock()

	if f.file != nil || f.earlyFinalized {
		return nil
	}

	file, err := os.OpenFile(f.path, os.O_RDWR|os.O_CREATE, serverFilePermissions)
	if err != nil {
		return err
	}

	// Pre-allocate to expected size
	if truncErr := file.Truncate(f.size); truncErr != nil {
		_ = file.Close()
		return truncErr
	}

	f.file = file
	return nil
}

// writeAt ensures the file is open and writes data at the given offset.
// Holds fileMu.RLock during the write so closeFileHandle and takeWriteHandle
// (which acquire the exclusive lock) block until all in-flight writes complete.
func (f *serverFileInfo) writeAt(data []byte, offset int64) error {
	// Ensure file is open (uses exclusive lock internally for creation).
	if openErr := f.openForWrite(); openErr != nil {
		return openErr
	}

	// Read-lock for the actual write - concurrent writes to different
	// offsets are safe (pwrite), while Close waits for all to drain.
	f.fileMu.RLock()
	defer f.fileMu.RUnlock()

	if f.file == nil {
		if f.earlyFinalized {
			// Every piece overlapping this file was written and read-back
			// verified before it was renamed, so the only writes that reach
			// here are duplicates the source re-sent after a stale ack.
			return nil
		}
		return fmt.Errorf("file closed during write: %s", f.path)
	}

	_, writeErr := f.file.WriteAt(data, offset)
	return writeErr
}

// takeWriteHandle closes a completed file to further writes and hands its write
// handle to an early finalization. Blocks until in-flight writeAt calls drain,
// so the returned handle is owned exclusively by the caller and safe to sync,
// read back and close.
//
// Caller must hold state.mu: path, file and earlyFinalized are documented as
// state.mu-guarded but the write path reads them under fileMu alone, so every
// writer holds both locks and each reader may hold either.
func (f *serverFileInfo) takeWriteHandle() *os.File {
	f.fileMu.Lock()
	defer f.fileMu.Unlock()

	fh := f.file
	f.file = nil
	f.earlyFinalized = true
	return fh
}

// readmitWrites re-admits writes to a file whose early finalization did not
// stick, either because it deferred the file back to finalizeFiles or because
// finalize-time verification failed and its pieces will be re-streamed.
// Caller must hold state.mu.
func (f *serverFileInfo) readmitWrites() {
	f.fileMu.Lock()
	defer f.fileMu.Unlock()

	f.earlyFinalized = false
}

// setPath records the file's new location after a rename.
// Caller must hold state.mu.
func (f *serverFileInfo) setPath(path string) {
	f.fileMu.Lock()
	defer f.fileMu.Unlock()

	f.path = path
}

// writePieceData writes piece data to the correct file(s) based on offset.
// A piece may span multiple files in a multi-file torrent.
// Skips files that are hardlinked, pending hardlink, or unselected.
func (s *serverTorrentState) writePieceData(offset int64, data []byte) error {
	remaining := data
	currentOffset := offset

	for _, fi := range s.files[s.firstFileEndingAfter(currentOffset):] {
		if len(remaining) == 0 {
			break
		}

		fileEnd := fi.offset + fi.size

		if fileEnd <= currentOffset {
			continue
		}

		fileWriteOffset := max(currentOffset-fi.offset, 0)
		availableInFile := fi.size - fileWriteOffset
		toProcess := min(int64(len(remaining)), availableInFile)

		if fi.skipForWriteData() {
			remaining = remaining[toProcess:]
			currentOffset += toProcess
			continue
		}

		// No per-piece fsync: data integrity is guaranteed by verifyFilePieces
		// (early finalization) and verifyFinalizedPieces (full finalization),
		// which read back and SHA1-verify pieces before rename.
		// Per-piece fsync would severely degrade write throughput on NFS/spinning disks.
		if writeErr := fi.writeAt(remaining[:toProcess], fileWriteOffset); writeErr != nil {
			return fmt.Errorf("writing to %s: %w", fi.path, writeErr)
		}

		remaining = remaining[toProcess:]
		currentOffset += toProcess
	}

	return nil
}

// firstFileEndingAfter returns the index of the first file whose data extends
// past offset, so callers can iterate only the files a byte range actually
// touches instead of scanning the whole torrent.
//
// Valid because files are constructed sorted by offset and contiguous
// (qbclient/source.go assigns offsets as a running sum), which makes each
// file's end offset monotonically non-decreasing. For many-file torrents
// (season packs, archives) this turns per-piece work from O(F) into O(log F).
func (m *torrentMeta) firstFileEndingAfter(offset int64) int {
	return sort.Search(len(m.files), func(i int) bool {
		return m.files[i].offset+m.files[i].size > offset
	})
}

// buildReadyResponse creates a successful READY response with piece information.
func (s *serverTorrentState) buildReadyResponse() *pb.InitTorrentResponse {
	piecesNeeded, needCount, haveCount := calculatePiecesNeeded(s.written)
	return &pb.InitTorrentResponse{
		Success:           true,
		Status:            pb.TorrentSyncStatus_SYNC_STATUS_READY,
		PiecesNeeded:      piecesNeeded,
		HardlinkResults:   s.hardlinkResults,
		PiecesNeededCount: needCount,
		PiecesHaveCount:   haveCount,
	}
}

// countSelectedFiles returns the number of selected files.
func (m *torrentMeta) countSelectedFiles() int {
	count := 0
	for _, f := range m.files {
		if f.selected {
			count++
		}
	}
	return count
}

// countSelectedPiecesTotal returns the number of pieces that overlap at least one selected file.
func (s *serverTorrentState) countSelectedPiecesTotal() int {
	count := 0
	for i := range int(s.written.Len()) {
		if s.classifyPiece(i) != pieceNoSelectedOverlap {
			count++
		}
	}
	return count
}

// pieceClass classifies how a piece relates to the file selection.
type pieceClass int

const (
	// pieceNoSelectedOverlap means no selected file overlaps this piece.
	pieceNoSelectedOverlap pieceClass = iota
	// pieceFullySelected means all overlapping files are selected.
	pieceFullySelected
	// pieceBoundary means the piece overlaps both selected and unselected files.
	pieceBoundary
)

// classifyPiece determines a piece's relationship to the file selection in a single
// pass with early exit on boundary detection.
func (m *torrentMeta) classifyPiece(pieceIdx int) pieceClass {
	pieceStart := int64(pieceIdx) * m.pieceLength
	pieceEnd := min(pieceStart+m.pieceLength, m.totalSize)

	hasSelected := false
	hasUnselected := false

	for _, f := range m.files[m.firstFileEndingAfter(pieceStart):] {
		if f.offset >= pieceEnd {
			break
		}
		if f.selected {
			hasSelected = true
		} else {
			hasUnselected = true
		}
		if hasSelected && hasUnselected {
			return pieceBoundary
		}
	}

	if !hasSelected {
		return pieceNoSelectedOverlap
	}
	return pieceFullySelected
}

// calculatePiecesNeeded converts written state to pieces_needed (inverse).
// pieces_needed[i] = true means the piece needs to be streamed.
func calculatePiecesNeeded(written *bitset.BitSet) ([]bool, int32, int32) {
	n := int(written.Len())
	piecesNeeded := make([]bool, n)
	var needCount, haveCount int32
	for i := range n {
		if written.Test(uint(i)) {
			haveCount++
		} else {
			piecesNeeded[i] = true
			needCount++
		}
	}
	return piecesNeeded, needCount, haveCount
}

// countHardlinkResults counts hardlinked, pending, and pre-existing files from results.
func countHardlinkResults(results []*pb.HardlinkResult) (int, int, int) {
	hardlinked, pending, preExisting := 0, 0, 0
	for _, r := range results {
		if r.GetPreExisting() {
			preExisting++
		} else if r.GetHardlinked() {
			hardlinked++
		}
		if r.GetPending() {
			pending++
		}
	}
	return hardlinked, pending, preExisting
}
