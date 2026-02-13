package destination

import (
	"fmt"
	"os"

	"github.com/arsac/qb-sync/internal/utils"
	pb "github.com/arsac/qb-sync/proto"
)

// openForWrite lazily opens the file for writing, creating and pre-allocating it if needed.
func (f *serverFileInfo) openForWrite() (*os.File, error) {
	if f.file != nil {
		return f.file, nil
	}

	file, err := os.OpenFile(f.path, os.O_RDWR|os.O_CREATE, serverFilePermissions)
	if err != nil {
		return nil, err
	}

	// Pre-allocate to expected size
	if truncErr := file.Truncate(f.size); truncErr != nil {
		_ = file.Close()
		return nil, truncErr
	}

	f.file = file
	return file, nil
}

// verifyPieceHash checks the piece data against expected hash.
// Returns empty string if valid, error message if invalid.
func (m *torrentMeta) verifyPieceHash(pieceIndex int32, data []byte, reqHash string) string {
	// Prefer pre-stored hash from InitTorrent, fall back to request hash
	expectedHash := reqHash
	if int(pieceIndex) < len(m.pieceHashes) && m.pieceHashes[pieceIndex] != "" {
		expectedHash = m.pieceHashes[pieceIndex]
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
func (m *torrentMeta) classifyPiece(pieceIdx int) pieceClass {
	pieceStart := int64(pieceIdx) * m.pieceLength
	pieceEnd := min(pieceStart+m.pieceLength, m.totalSize)

	hasSelected := false
	hasUnselected := false

	for _, f := range m.files {
		if f.offset >= pieceEnd || f.offset+f.size <= pieceStart {
			continue
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
