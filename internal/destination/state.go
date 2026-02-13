package destination

import (
	"fmt"
	"os"

	"github.com/arsac/qb-sync/internal/utils"
	pb "github.com/arsac/qb-sync/proto"
)

// --- serverFileInfo domain methods ---

// openForWrite lazily opens the file for writing, creating and pre-allocating it if needed.
func (fi *serverFileInfo) openForWrite() (*os.File, error) {
	if fi.file != nil {
		return fi.file, nil
	}

	file, err := os.OpenFile(fi.path, os.O_RDWR|os.O_CREATE, serverFilePermissions)
	if err != nil {
		return nil, err
	}

	// Pre-allocate to expected size
	if truncErr := file.Truncate(fi.size); truncErr != nil {
		_ = file.Close()
		return nil, truncErr
	}

	fi.file = file
	return file, nil
}

// --- serverTorrentState domain methods ---

// verifyPieceHash checks the piece data against expected hash.
// Returns empty string if valid, error message if invalid.
func (s *serverTorrentState) verifyPieceHash(pieceIndex int32, data []byte, reqHash string) string {
	// Prefer pre-stored hash from InitTorrent, fall back to request hash
	expectedHash := reqHash
	if int(pieceIndex) < len(s.pieceHashes) && s.pieceHashes[pieceIndex] != "" {
		expectedHash = s.pieceHashes[pieceIndex]
	}

	if err := utils.VerifyPieceHash(data, expectedHash); err != nil {
		return err.Error()
	}
	return ""
}

// writePieceData writes piece data to the correct file(s) based on offset.
// A piece may span multiple files in a multi-file torrent.
// Skips files that are hardlinked, pending hardlink, or unselected.
func (s *serverTorrentState) writePieceData(offset int64, data []byte) error {
	remaining := data
	currentOffset := offset

	for _, fi := range s.files {
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

		file, openErr := fi.openForWrite()
		if openErr != nil {
			return fmt.Errorf("opening %s: %w", fi.path, openErr)
		}
		// No per-piece fsync: data integrity is guaranteed by verifyFilePieces
		// (early finalization) and verifyFinalizedPieces (full finalization),
		// which read back and SHA1-verify pieces before rename.
		// Per-piece fsync would severely degrade write throughput on NFS/spinning disks.
		if _, writeErr := file.WriteAt(remaining[:toProcess], fileWriteOffset); writeErr != nil {
			return fmt.Errorf("writing to %s: %w", fi.path, writeErr)
		}

		remaining = remaining[toProcess:]
		currentOffset += toProcess
	}

	return nil
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
func (s *serverTorrentState) countSelectedFiles() int {
	count := 0
	for _, f := range s.files {
		if f.selected {
			count++
		}
	}
	return count
}

// countSelectedPiecesTotal returns the number of pieces that overlap at least one selected file.
// Caller must hold state.mu.
func (s *serverTorrentState) countSelectedPiecesTotal() int {
	count := 0
	for i := range s.written {
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
func (s *serverTorrentState) classifyPiece(pieceIdx int) pieceClass {
	pieceStart := int64(pieceIdx) * s.pieceLength
	pieceEnd := min(pieceStart+s.pieceLength, s.totalSize)

	hasSelected := false
	hasUnselected := false

	for _, fi := range s.files {
		if fi.offset >= pieceEnd || fi.offset+fi.size <= pieceStart {
			continue
		}
		if fi.selected {
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

// --- Free functions (operate on domain types before state construction) ---

// calculatePiecesNeeded converts written state to pieces_needed (inverse).
// pieces_needed[i] = true means the piece needs to be streamed.
func calculatePiecesNeeded(written []bool) ([]bool, int32, int32) {
	piecesNeeded := make([]bool, len(written))
	var needCount, haveCount int32
	for i, w := range written {
		if w {
			haveCount++
		} else {
			piecesNeeded[i] = true
			needCount++
		}
	}
	return piecesNeeded, needCount, haveCount
}

// calculatePiecesCovered determines which pieces are fully covered by hardlinked, pending,
// or unselected files (none of these need data streamed from source).
func calculatePiecesCovered(files []*serverFileInfo, numPieces int32, pieceSize, totalSize int64) []bool {
	piecesCovered := make([]bool, numPieces)
	for pieceIdx := range numPieces {
		pieceStart := int64(pieceIdx) * pieceSize
		pieceEnd := min(pieceStart+pieceSize, totalSize)

		// Piece is covered if every overlapping file is hardlinked, pending, or unselected
		covered := true
		for _, f := range files {
			if f.offset < pieceEnd && f.offset+f.size > pieceStart && !f.skipForWriteData() {
				covered = false
				break
			}
		}
		piecesCovered[pieceIdx] = covered
	}
	return piecesCovered
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

// countWritten counts the number of written pieces.
func countWritten(written []bool) int {
	count := 0
	for _, w := range written {
		if w {
			count++
		}
	}
	return count
}

// computeFilePieceRanges sets firstPiece, lastPiece, and piecesTotal on each file
// based on its offset/size and the torrent's piece geometry.
func computeFilePieceRanges(files []*serverFileInfo, pieceLength, totalSize int64) {
	if pieceLength <= 0 {
		return
	}
	maxPiece := int((totalSize - 1) / pieceLength)
	for _, fi := range files {
		if fi.size <= 0 {
			continue
		}
		fi.firstPiece = int(fi.offset / pieceLength)
		fi.lastPiece = min(int((fi.offset+fi.size-1)/pieceLength), maxPiece)
		fi.piecesTotal = fi.lastPiece - fi.firstPiece + 1
	}
}

// initFilePieceCounts initializes piecesWritten on each file from the existing written bitmap.
func initFilePieceCounts(files []*serverFileInfo, written []bool) {
	for _, fi := range files {
		if fi.earlyFinalized || fi.size <= 0 {
			continue
		}
		fi.recalcPiecesWritten(written)
	}
}
