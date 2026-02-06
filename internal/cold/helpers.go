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

// calculatePiecesCovered determines which pieces are fully covered by hardlinked or pending files.
func calculatePiecesCovered(files []*serverFileInfo, numPieces int32, pieceSize, totalSize int64) []bool {
	piecesCovered := make([]bool, numPieces)
	for pieceIdx := range numPieces {
		pieceStart := int64(pieceIdx) * pieceSize
		pieceEnd := min(pieceStart+pieceSize, totalSize)

		// Piece is covered if all overlapping files are hardlinked or pending
		covered := true
		for _, f := range files {
			fileStart := f.offset
			fileEnd := f.offset + f.size

			// Check if file overlaps with piece
			if fileStart < pieceEnd && fileEnd > pieceStart {
				if f.hlState != hlStateComplete && f.hlState != hlStatePending {
					covered = false
					break
				}
			}
		}
		piecesCovered[pieceIdx] = covered
	}
	return piecesCovered
}

// buildPersistedInfo creates a persistedTorrentInfo from file info.
func buildPersistedInfo(
	name string,
	numPieces int32,
	pieceSize, totalSize int64,
	files []*serverFileInfo,
	pieceHashes []string,
) *persistedTorrentInfo {
	persistedFiles := make([]persistedFileInfo, len(files))
	for i, f := range files {
		persistedFiles[i] = persistedFileInfo{
			Path:   f.path,
			Size:   f.size,
			Offset: f.offset,
		}
	}
	return &persistedTorrentInfo{
		Name:        name,
		NumPieces:   int(numPieces),
		PieceLength: pieceSize,
		TotalSize:   totalSize,
		Files:       persistedFiles,
		PieceHashes: pieceHashes,
	}
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
