package destination

// The piece ledger: every mutation of a torrent's written and verified bitmaps
// goes through the methods in this file, together with the per-file
// written-piece counts those bitmaps feed. A verified bit vouches for written
// bytes currently on disk, and a file's count is its slice of the written
// bitmap, so the three drift apart the moment one is updated on its own.
//
// All methods require state.mu held; the ledger adds no locking of its own.
// checkFileCompletions nudges a file's count incrementally on the write path
// for the same reason - it holds state.mu and pairs the increment with the
// bitmap bit markPieceWritten just set. clearStalePieces trims a bitmap at
// construction time, before the state - and the verified set it would need to
// keep in step - exists, which is why it lives outside the ledger.

// markPieceWritten records a written piece and schedules a state flush.
// Out-of-range indices are ignored.
func (s *serverTorrentState) markPieceWritten(pieceIndex int32) {
	if pieceIndex < 0 || uint(pieceIndex) >= s.written.Len() {
		return
	}

	s.written.Set(uint(pieceIndex))
	s.dirty = true
	s.piecesSinceFlush++
}

// markPieceVerified records that a piece's bytes were read back from disk and
// hashed correctly after the flush, so finalization can skip its read-back.
// No-op when the state tracks no verified set.
func (s *serverTorrentState) markPieceVerified(p int) {
	if s.verified == nil {
		return
	}
	s.verified.Set(uint(p))
}

// revokePieces marks pieces unwritten so the source re-streams them, and marks
// the state dirty for the next flush. Their verified bits go with them: a bit
// that survived would vouch for the revoked bytes and let the re-streamed
// replacement skip its post-flush read-back.
func (s *serverTorrentState) revokePieces(pieces []int) {
	for _, p := range pieces {
		s.revokePiece(uint(p))
	}
	s.dirty = true
}

// revokeFileRange revokes every piece a file's bytes backed - the accounting
// half of deleting the file. Not just its failed pieces: the file is gone
// whole, and a piece left marked written would have the source re-stream only
// the failures into an empty replacement.
func (s *serverTorrentState) revokeFileRange(fi *serverFileInfo) {
	for p := fi.firstPiece; p <= fi.lastPiece; p++ {
		s.revokePiece(uint(p))
	}
	s.dirty = true
}

func (s *serverTorrentState) revokePiece(p uint) {
	s.written.Clear(p)
	if s.verified != nil {
		s.verified.Clear(p)
	}
}

// recountFilePieces re-derives every file's written-piece count from the
// written bitmap. Call it once, after every revocation has landed: a boundary
// piece belongs to two files, so a count taken mid-recovery is invalidated by
// the next file's revocation.
func (s *serverTorrentState) recountFilePieces() {
	for _, fi := range s.files {
		if fi.size > 0 {
			fi.recalcPiecesWritten(s.written)
		}
	}
}
