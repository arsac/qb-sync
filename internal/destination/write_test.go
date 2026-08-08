package destination

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/bits-and-blooms/bitset"

	"github.com/arsac/qb-sync/internal/utils"
	pb "github.com/arsac/qb-sync/proto"
)

// TestVerifyIncomingPiece_GatesOnSelectionAndHash pins the pre-write hash gate.
// Its skip condition is not an optimization: a piece straddling a deselected
// file arrives with that file's region zero-filled, so it cannot match the
// torrent's hash and rejecting it would strand every boundary piece of a
// partially-selected torrent.
func TestVerifyIncomingPiece_GatesOnSelectionAndHash(t *testing.T) {
	t.Parallel()

	good := []byte("piece-zero-data!") // 16 bytes
	bad := []byte("not-what-we-want")  // 16 bytes, same length

	// One selected 32-byte file: both pieces are fully selected.
	fullySelected := func(hashes []string) *serverTorrentState {
		return &serverTorrentState{
			torrentMeta: torrentMeta{
				pieceHashes: hashes,
				pieceLength: 16,
				totalSize:   32,
				files: []*serverFileInfo{
					{path: "whole.bin", size: 32, offset: 0, selected: true},
				},
			},
		}
	}

	// A selected 8-byte file followed by an unselected 24-byte one, so piece 0
	// straddles the selection boundary.
	boundary := &serverTorrentState{
		torrentMeta: torrentMeta{
			pieceHashes: []string{utils.ComputeSHA1(good), ""},
			pieceLength: 16,
			totalSize:   32,
			files: []*serverFileInfo{
				{path: "head.bin", size: 8, offset: 0, selected: true},
				{path: "tail.bin", size: 24, offset: 8, selected: false},
			},
		},
	}

	tests := []struct {
		name       string
		state      *serverTorrentState
		pieceIndex int32
		data       []byte
		wantErr    bool
	}{
		{
			name:       "fully selected piece matching its hash",
			state:      fullySelected([]string{utils.ComputeSHA1(good), ""}),
			pieceIndex: 0,
			data:       good,
		},
		{
			name:       "fully selected piece failing its hash",
			state:      fullySelected([]string{utils.ComputeSHA1(good), ""}),
			pieceIndex: 0,
			data:       bad,
			wantErr:    true,
		},
		{
			name:       "boundary piece is not hash checked",
			state:      boundary,
			pieceIndex: 0,
			data:       bad,
		},
		{
			name:       "piece with no recorded hash",
			state:      fullySelected([]string{"", ""}),
			pieceIndex: 0,
			data:       bad,
		},
		{
			name:       "piece index past the hash list",
			state:      fullySelected([]string{utils.ComputeSHA1(good)}),
			pieceIndex: 1,
			data:       bad,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := verifyIncomingPiece(tc.state, tc.pieceIndex, tc.data)
			if (err != nil) != tc.wantErr {
				t.Fatalf("verifyIncomingPiece error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

// TestAdmitPiece_SkipsWorkThePieceDoesNotNeed pins the pre-write gate's two
// short-circuits. Neither is load-bearing (commitPiece re-tests both under the
// lock it records with), so the assertion is that they answer at all, and that
// an ordinary piece is let through to the hash check and the write.
func TestAdmitPiece_SkipsWorkThePieceDoesNotNeed(t *testing.T) {
	t.Parallel()

	newState := func() *serverTorrentState {
		return &serverTorrentState{
			torrentMeta: torrentMeta{pieceLength: 16, totalSize: 32},
			written:     bitset.New(2),
		}
	}

	t.Run("a piece already on disk needs no work", func(t *testing.T) {
		t.Parallel()
		state := newState()
		state.written.Set(0)

		res, done := admitPiece(state, 0)
		if !done || !res.success {
			t.Fatalf("admitPiece = (%+v, %v), want a successful short-circuit", res, done)
		}
	})

	t.Run("a piece arriving during finalization is refused", func(t *testing.T) {
		t.Parallel()
		state := newState()
		state.finalization.active = true

		res, done := admitPiece(state, 0)
		if !done || res.success {
			t.Fatalf("admitPiece = (%+v, %v), want a failing short-circuit", res, done)
		}
		if res.errorCode != pb.PieceErrorCode_PIECE_ERROR_FINALIZING {
			t.Errorf("errorCode = %v, want PIECE_ERROR_FINALIZING", res.errorCode)
		}
	})

	t.Run("an ordinary piece is let through", func(t *testing.T) {
		t.Parallel()
		if _, done := admitPiece(newState(), 0); done {
			t.Fatal("admitPiece short-circuited a piece that still needs writing")
		}
	})
}

// stateLockProbeHandler stands in for any reporting the write path does after
// it records a piece: handling the progress record takes the torrent's state
// lock, so it can only complete once commitPiece has released it. Logging under
// state.mu self-deadlocks instead, which the caller's watchdog reports.
type stateLockProbeHandler struct {
	state *serverTorrentState
	seen  chan struct{}
}

func (h *stateLockProbeHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *stateLockProbeHandler) Handle(_ context.Context, r slog.Record) error {
	if r.Message != "write progress" {
		return nil
	}
	h.state.mu.Lock()
	guarded := h.state.written.Count()
	h.state.mu.Unlock()
	if guarded == 0 {
		return nil
	}
	select {
	case h.seen <- struct{}{}:
	default:
	}
	return nil
}

func (h *stateLockProbeHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h *stateLockProbeHandler) WithGroup(string) slog.Handler { return h }

// TestWritePiece_ReportsOutsideTheStateLock pins that the per-piece reporting -
// the progress line and the three Prometheus operations beside it - runs after
// the state lock is released. Every stream worker takes state.mu once per
// piece, so work that reads no torrent state has no business inside it.
func TestWritePiece_ReportsOutsideTheStateLock(t *testing.T) {
	t.Parallel()

	s, tmpDir := newTestDestServer(t)
	hash := "report-outside-lock"
	pieceData := []byte("one whole piece of torrent data")

	state := &serverTorrentState{
		torrentMeta: torrentMeta{
			pieceHashes: []string{utils.ComputeSHA1(pieceData)},
			pieceLength: int64(len(pieceData)),
			totalSize:   int64(len(pieceData)),
			files: []*serverFileInfo{{
				path:        filepath.Join(tmpDir, "only.bin"+partialSuffix),
				size:        int64(len(pieceData)),
				offset:      0,
				firstPiece:  0,
				lastPiece:   0,
				piecesTotal: 1,
				selected:    true,
			}},
		},
		written:   bitset.New(1),
		statePath: filepath.Join(tmpDir, hash+".state"),
	}
	s.store.mu.Lock()
	s.store.entries[hash] = state
	s.store.mu.Unlock()

	probe := &stateLockProbeHandler{state: state, seen: make(chan struct{}, 1)}
	s.logger = slog.New(probe)

	// The piece completes the torrent's only file, so the progress line fires
	// on the count == total branch.
	done := make(chan writeResult, 1)
	go func() {
		done <- s.writePiece(context.Background(), &pb.WritePieceRequest{
			TorrentHash: hash,
			PieceIndex:  0,
			Offset:      0,
			Size:        int64(len(pieceData)),
			Data:        pieceData,
		})
	}()

	select {
	case res := <-done:
		if !res.success {
			t.Fatalf("writePiece failed: %s", res.errMsg)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("writePiece blocked: the progress log ran while state.mu was still held")
	}

	select {
	case <-probe.seen:
	default:
		t.Fatal("progress line never fired; the test no longer exercises the reporting path")
	}

	waitEarlyFinalize(t, state)
}
