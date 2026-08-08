package destination

import (
	"fmt"
	"os"

	"github.com/bits-and-blooms/bitset"

	"github.com/arsac/qb-sync/internal/utils"
	pb "github.com/arsac/qb-sync/proto"
)

// declinesWrites reports whether piece data aimed at this file must be dropped
// rather than written. Caller must hold fileMu.
//
// Three reasons, all of which mean the same thing to the write path - there is
// no .partial for this file to receive bytes:
//
//   - unselected, so no .partial was ever created;
//   - the data is supplied by a hardlink to another torrent's file, which
//     writing to would corrupt that torrent;
//   - early-finalized, so its bytes are complete, verified and renamed into
//     place, and opening f.path would recreate the .partial the rename consumed.
//
// A file that has an open write handle can never start declining: hardlink
// resolution only moves Pending to Complete (both already declining) and
// selection is immutable, so the only new decliner is early finalization, which
// takes the handle under the same lock. That is what lets writeIfOpen treat a
// non-nil handle as permission to write without re-checking here.
func (f *serverFileInfo) declinesWrites() bool {
	return f.earlyFinalized || f.skipForWriteData()
}

// openForWrite lazily opens the file for writing, creating and pre-allocating it if needed.
// Protected by fileMu so it can be called outside state.mu for concurrent disk I/O.
func (f *serverFileInfo) openForWrite() error {
	f.fileMu.Lock()
	defer f.fileMu.Unlock()

	if f.file != nil || f.declinesWrites() {
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
//
// The already-open case must not touch the exclusive lock. A pending Lock also
// blocks new RLock acquisitions, so routing every write through openForWrite
// made each of the stream workers wait out the previous worker's NFS round-trip
// before its own could start - concurrent writes to one file serialized
// completely, whatever the worker count.
func (f *serverFileInfo) writeAt(data []byte, offset int64) error {
	if wrote, err := f.writeIfOpen(data, offset); wrote {
		return err
	}

	if openErr := f.openForWrite(); openErr != nil {
		return openErr
	}

	if wrote, err := f.writeIfOpen(data, offset); wrote {
		return err
	}
	return f.noHandleReason()
}

// writeIfOpen writes under the read lock when the file already has a handle, so
// concurrent writes to different offsets overlap (pwrite) while takeWriteHandle
// and closeFileHandle still wait for them all to drain. Reports false, without
// writing, when the file has no handle for openForWrite to be given a turn.
func (f *serverFileInfo) writeIfOpen(data []byte, offset int64) (bool, error) {
	f.fileMu.RLock()
	defer f.fileMu.RUnlock()

	if f.file == nil {
		return false, nil
	}
	if _, writeErr := f.file.WriteAt(data, offset); writeErr != nil {
		return true, fmt.Errorf("writing to %s: %w", f.path, writeErr)
	}
	return true, nil
}

// noHandleReason explains why a file still has no write handle after
// openForWrite returned successfully.
func (f *serverFileInfo) noHandleReason() error {
	f.fileMu.RLock()
	defer f.fileMu.RUnlock()

	if f.declinesWrites() {
		// Either the file's bytes come from somewhere other than this stream,
		// or every piece overlapping it was written and read-back verified
		// before it was renamed - so the only writes that reach here are ones
		// the file was never going to accept.
		return nil
	}
	return fmt.Errorf("file closed during write: %s", f.path)
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

// setHardlinkState advances the file's hardlink state machine.
// Caller must hold state.mu.
func (f *serverFileInfo) setHardlinkState(hlState hardlinkState) {
	f.fileMu.Lock()
	defer f.fileMu.Unlock()

	f.hardlink.state = hlState
}

// writePieceData writes piece data to the correct file(s) based on offset.
// A piece may span multiple files in a multi-file torrent. Files that take no
// piece data (unselected, hardlinked, early-finalized) drop their share inside
// writeAt, which is where fileMu makes that decision race-free.
//
// No per-piece fsync: data integrity is guaranteed by verifyFilePieces (early
// finalization) and verifyFinalizedPieces (full finalization), which read back
// and SHA1-verify pieces before rename. Per-piece fsync would severely degrade
// write throughput on NFS/spinning disks.
func (s *serverTorrentState) writePieceData(offset int64, data []byte) error {
	return utils.WalkPieceRegions(s.files, fileSpan, offset, data,
		func(_ int, fi *serverFileInfo, fileWriteOffset int64, region []byte) error {
			return fi.writeAt(region, fileWriteOffset)
		})
}

// firstFileEndingAfter narrows a byte range to the files it actually touches.
// See [utils.FirstFileEndingAfter] for the invariant this relies on.
func (m *torrentMeta) firstFileEndingAfter(offset int64) int {
	return utils.FirstFileEndingAfter(m.files, offset, fileEnd)
}

// fileSpan reports a file's span in the torrent's byte space, for
// [utils.WalkPieceRegions].
func fileSpan(f *serverFileInfo) utils.Span { return utils.Span{Offset: f.offset, Size: f.size} }

// fileEnd reports a file's exclusive end offset, for [utils.FirstFileEndingAfter].
func fileEnd(f *serverFileInfo) int64 { return fileSpan(f).End() }

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
