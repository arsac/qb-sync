package destination

import (
	"testing"

	"github.com/bits-and-blooms/bitset"
)

// newLedgerState builds a 3-piece torrent whose two files share boundary piece
// 1: file A backs pieces 0-1, file B backs pieces 1-2. The shared piece is
// what makes the coupled updates in piece_ledger.go worth owning in one place.
func newLedgerState() (*serverTorrentState, *serverFileInfo, *serverFileInfo) {
	fileA := &serverFileInfo{
		size: 150, selected: true,
		firstPiece: 0, lastPiece: 1, piecesTotal: 2,
	}
	fileB := &serverFileInfo{
		offset: 150, size: 150, selected: true,
		firstPiece: 1, lastPiece: 2, piecesTotal: 2,
	}
	state := &serverTorrentState{
		torrentMeta: torrentMeta{
			pieceLength: 100,
			totalSize:   300,
			files:       []*serverFileInfo{fileA, fileB},
		},
		written:  bitset.New(3),
		verified: bitset.New(3),
	}
	return state, fileA, fileB
}

func TestMarkPieceWritten(t *testing.T) {
	t.Parallel()
	state, _, _ := newLedgerState()

	state.markPieceWritten(1)

	if !state.written.Test(1) {
		t.Error("piece 1 should be written")
	}
	if !state.dirty {
		t.Error("a written piece must schedule a state flush")
	}
	if state.piecesSinceFlush != 1 {
		t.Errorf("piecesSinceFlush = %d, want 1", state.piecesSinceFlush)
	}

	// Out-of-range indices are ignored, not panics or stray bits.
	state.markPieceWritten(-1)
	state.markPieceWritten(3)
	if state.written.Count() != 1 {
		t.Errorf("written count = %d after out-of-range marks, want 1", state.written.Count())
	}
}

func TestMarkPieceVerified_NilVerifiedSet(t *testing.T) {
	t.Parallel()
	state, _, _ := newLedgerState()
	state.verified = nil

	state.markPieceVerified(0) // must not panic
}

// TestRevokePieces pins that revoking a piece revokes its verified bit with
// it. A verified bit that outlives the bytes it vouched for lets the
// re-streamed replacement skip its post-flush read-back.
func TestRevokePieces(t *testing.T) {
	t.Parallel()
	state, _, _ := newLedgerState()
	state.written.Set(0).Set(1).Set(2)
	state.verified.Set(0).Set(1)

	state.revokePieces([]int{0, 1})

	for p := range uint(2) {
		if state.written.Test(p) {
			t.Errorf("piece %d should be unwritten after revocation", p)
		}
		if state.verified.Test(p) {
			t.Errorf("piece %d kept its verified bit past its revocation", p)
		}
	}
	if !state.written.Test(2) {
		t.Error("piece 2 was not revoked and must stay written")
	}
	if !state.dirty {
		t.Error("a revocation must schedule a state flush")
	}
}

// TestRevokeFileRange pins the accounting half of deleting a file: every piece
// in its range is revoked, written and verified alike, not just the failures.
func TestRevokeFileRange(t *testing.T) {
	t.Parallel()
	state, _, fileB := newLedgerState()
	state.written.Set(0).Set(1).Set(2)
	state.verified.Set(0).Set(1).Set(2)

	state.revokeFileRange(fileB)

	for p := uint(1); p <= 2; p++ {
		if state.written.Test(p) || state.verified.Test(p) {
			t.Errorf("piece %d belongs to the deleted file and must be fully revoked", p)
		}
	}
	if !state.written.Test(0) || !state.verified.Test(0) {
		t.Error("piece 0 is outside the deleted file and must keep both bits")
	}
}

func TestRevokeFileRange_NilVerifiedSet(t *testing.T) {
	t.Parallel()
	state, _, fileB := newLedgerState()
	state.verified = nil
	state.written.Set(1).Set(2)

	state.revokeFileRange(fileB) // must not panic

	if state.written.Test(1) || state.written.Test(2) {
		t.Error("written bits must be revoked even when no verified set is tracked")
	}
}

// TestRecountFilePieces pins why the recount is a single pass over all files
// after every revocation has landed: boundary piece 1 belongs to both files,
// so a count taken for file A before file B's revocation would still include
// it.
func TestRecountFilePieces(t *testing.T) {
	t.Parallel()
	state, fileA, fileB := newLedgerState()
	state.written.Set(0).Set(1).Set(2)
	fileA.piecesWritten, fileB.piecesWritten = 2, 2

	state.revokeFileRange(fileB)
	state.recountFilePieces()

	if fileA.piecesWritten != 1 {
		t.Errorf("file A count = %d, want 1: the shared boundary piece went with file B", fileA.piecesWritten)
	}
	if fileB.piecesWritten != 0 {
		t.Errorf("file B count = %d, want 0 after its range was revoked", fileB.piecesWritten)
	}
}
