package cold

import (
	pb "github.com/arsac/qb-sync/proto"
)

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
// or unselected files (none of these need data streamed from hot).
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
	for i, w := range written {
		if !w {
			continue
		}
		for _, fi := range files {
			if fi.earlyFinalized || fi.size <= 0 {
				continue
			}
			if i >= fi.firstPiece && i <= fi.lastPiece {
				fi.piecesWritten++
			}
		}
	}
}

// countSelectedFiles returns the number of selected files.
func countSelectedFiles(files []*serverFileInfo) int {
	count := 0
	for _, f := range files {
		if f.selected {
			count++
		}
	}
	return count
}

// countSelectedPiecesTotal returns the number of pieces that overlap at least one selected file.
// Caller must hold state.mu.
func countSelectedPiecesTotal(state *serverTorrentState) int {
	count := 0
	for i := range state.written {
		if pieceOverlapsSelectedFile(state.files, i, state.pieceLength, state.totalSize) {
			count++
		}
	}
	return count
}

// pieceOverlapsSelectedFile returns true if the piece overlaps at least one selected file.
func pieceOverlapsSelectedFile(files []*serverFileInfo, pieceIdx int, pieceLength, totalSize int64) bool {
	pieceStart := int64(pieceIdx) * pieceLength
	pieceEnd := min(pieceStart+pieceLength, totalSize)

	for _, fi := range files {
		if fi.offset < pieceEnd && fi.offset+fi.size > pieceStart && fi.selected {
			return true
		}
	}
	return false
}

// pieceEntirelyInSelectedFiles returns true if the piece's byte range only overlaps
// selected files. Used to decide whether post-finalization verification can read back
// the piece (unselected files have no data on disk).
func pieceEntirelyInSelectedFiles(files []*serverFileInfo, pieceIdx int, pieceLength, totalSize int64) bool {
	pieceStart := int64(pieceIdx) * pieceLength
	pieceEnd := min(pieceStart+pieceLength, totalSize)

	for _, fi := range files {
		if fi.offset < pieceEnd && fi.offset+fi.size > pieceStart && !fi.selected {
			return false
		}
	}
	return true
}
