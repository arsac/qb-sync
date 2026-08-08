package destination

import (
	"context"
	"fmt"
	"os"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/arsac/qb-sync/internal/metrics"
	"github.com/arsac/qb-sync/internal/utils"
	pb "github.com/arsac/qb-sync/proto"
)

// writeResult captures the outcome of writing a single piece.
type writeResult struct {
	success   bool
	errMsg    string
	errorCode pb.PieceErrorCode
}

// writePieceOK builds a success result.
func writePieceOK() writeResult {
	return writeResult{success: true}
}

// writePieceError builds a failure result with the given message and error code.
func writePieceError(msg string, code pb.PieceErrorCode) writeResult {
	return writeResult{
		errMsg:    msg,
		errorCode: code,
	}
}

// markPieceWritten updates state tracking after a piece is written.
// Caller must hold state.mu.
func markPieceWritten(state *serverTorrentState, pieceIndex int32) {
	if pieceIndex < 0 || uint(pieceIndex) >= state.written.Len() {
		return
	}

	state.written.Set(uint(pieceIndex))
	state.dirty = true
	state.piecesSinceFlush++
}

// writePiece receives and writes a single piece.
func (s *Server) writePiece(ctx context.Context, req *pb.WritePieceRequest) writeResult {
	if s.config.DryRun {
		return writePieceOK()
	}

	torrentHash := req.GetTorrentHash()

	state, exists := s.store.Get(torrentHash)
	if !exists {
		return writePieceError("torrent not initialized", pb.PieceErrorCode_PIECE_ERROR_NOT_INITIALIZED)
	}

	pieceIndex := req.GetPieceIndex()
	if pieceIndex < 0 {
		return writePieceError("negative piece index", pb.PieceErrorCode_PIECE_ERROR_IO)
	}
	data := req.GetData()

	// Early check with lock (optimization to skip hash verification in common cases).
	// This is NOT the correctness check - see double-check below after hash verification.
	state.mu.Lock()
	alreadyWritten := uint(pieceIndex) < state.written.Len() && state.written.Test(uint(pieceIndex))
	isFinalizing := state.finalization.active
	state.mu.Unlock()

	if alreadyWritten {
		return writePieceOK()
	}

	// Early rejection during finalization (optimization to skip expensive hash verification)
	if isFinalizing {
		return writePieceError("torrent is being finalized", pb.PieceErrorCode_PIECE_ERROR_FINALIZING)
	}

	// Verify piece hash outside lock (pieceHashes is immutable after init).
	// This is CPU-intensive so we don't hold the lock during verification.
	// Skip verification for boundary pieces overlapping deselected files:
	// source zero-fills the deselected region (file doesn't exist on disk),
	// changing the hash. writePieceData skips deselected files, so only
	// the selected file data is actually written.
	writeStart := time.Now()
	if state.classifyPiece(int(pieceIndex)) == pieceFullySelected &&
		int(pieceIndex) < len(state.pieceHashes) && state.pieceHashes[pieceIndex] != "" {
		if hashErr := utils.VerifyPieceHash(data, state.pieceHashes[pieceIndex]); hashErr != nil {
			metrics.PieceWriteDuration.Observe(time.Since(writeStart).Seconds())
			return writePieceError(hashErr.Error(), pb.PieceErrorCode_PIECE_ERROR_HASH_MISMATCH)
		}
	}

	// Disk I/O outside state.mu: writePieceData only touches immutable metadata
	// (files slice, offsets) and file handles that are safe to use without the lock
	// because the early-written check above ensures no concurrent writer for the
	// same piece, and finalization check prevents races with file rename.
	if writeErr := state.writePieceData(req.GetOffset(), data); writeErr != nil {
		metrics.PieceWriteErrorsTotal.WithLabelValues(metrics.ModeDestination).Inc()
		return writePieceError(fmt.Sprintf("write failed: %v", writeErr), pb.PieceErrorCode_PIECE_ERROR_IO)
	}

	state.mu.Lock()
	defer state.mu.Unlock()

	// CORRECTNESS CHECK: Re-verify finalizing flag under lock.
	// Even if finalization started between the early check and now, this prevents the write.
	if state.finalization.active {
		return writePieceError("torrent is being finalized", pb.PieceErrorCode_PIECE_ERROR_FINALIZING)
	}

	// Re-check under lock: a concurrent writer may have marked this piece.
	if uint(pieceIndex) < state.written.Len() && state.written.Test(uint(pieceIndex)) {
		return writePieceOK()
	}

	markPieceWritten(state, pieceIndex)
	s.checkFileCompletions(ctx, torrentHash, state, pieceIndex)
	metrics.PieceWriteDuration.Observe(time.Since(writeStart).Seconds())

	metrics.PiecesReceivedTotal.Inc()
	metrics.BytesReceivedTotal.Add(float64(len(data)))

	count := state.written.Count()
	if count%50 == 0 || count == state.written.Len() {
		s.logger.DebugContext(ctx, "write progress",
			"hash", torrentHash,
			"progress", fmt.Sprintf("%d/%d", int(count), int(state.written.Len())),
		)
	}

	return writePieceOK()
}

// verifyFilePieces reads back interior pieces from a synced .partial file and
// verifies their hashes. Returns indices of pieces that failed verification,
// ascending, and whether ctx cancellation cut the pass short. Boundary pieces
// (spanning adjacent files) are skipped - they are deferred to
// verifyFinalizedPieces.
//
// If fh is non-nil, reads go through it directly (saves NFS open round-trips
// per piece). If fh is nil, opens fi.path for the duration of the verify pass.
//
// Safe to call without state.mu: all accessed fields (pieceHashes, pieceLength,
// totalSize, fi geometry) are immutable after initialization.
func (s *Server) verifyFilePieces(
	ctx context.Context,
	state *serverTorrentState,
	fi *serverFileInfo,
	fh *os.File,
) ([]int, bool) {
	if len(state.pieceHashes) == 0 {
		return nil, false
	}

	var pieces []int
	forEachInteriorPiece(state, fi, func(p int) {
		pieces = append(pieces, p)
	})
	if len(pieces) == 0 {
		return nil, false
	}

	if fh == nil {
		f, openErr := os.Open(fi.path)
		if openErr != nil {
			// Can't read anything - treat every interior piece as failed so
			// the caller re-streams them. Boundary pieces are deferred to
			// verifyFinalizedPieces regardless.
			return pieces, false
		}
		defer f.Close()
		fh = f
	}

	return s.verifyPiecesParallel(ctx, state, fi, fh, pieces), ctx.Err() != nil
}

// verifyPiecesParallel read-back-verifies pieces with the same worker-pool
// shape verifyFinalizedPieces uses. A serial pass leaves an NFS export with
// one outstanding read at a time and hashes on a single core, so a completed
// multi-GB file stalls its stream worker for the whole read-back while the
// remaining workers keep writing - overlapping reads and SHA1 across workers
// removes that stall from the transfer's critical path.
//
// Each worker owns a pieceLength buffer (never escapes, VerifyPieceHash only
// hashes it) and, past the first, its own read fd: the per-file open cost is
// paid once, not per piece, and mirrors the per-goroutine FdCache rule that
// verify workers never share a handle.
//
// Cancelling ctx stops the reads but the pass still drains the queue, reporting
// every piece it never got to as failed. Callers act on "not in the failed set"
// as proof a piece was read back and matched, so an interrupted pass has to fail
// closed rather than silently shrink its coverage.
func (s *Server) verifyPiecesParallel(
	ctx context.Context,
	state *serverTorrentState,
	fi *serverFileInfo,
	fh *os.File,
	pieces []int,
) []int {
	workers := min(s.verifyConcurrency(), len(pieces))

	var (
		next     atomic.Int64
		failedMu sync.Mutex
		failed   []int
		wg       sync.WaitGroup
	)

	for w := range workers {
		wg.Go(func() {
			rf := fh
			if w > 0 {
				if own, openErr := os.Open(fi.path); openErr == nil {
					defer own.Close()
					rf = own
				}
			}
			buf := make([]byte, state.pieceLength)
			for {
				i := int(next.Add(1)) - 1
				if i >= len(pieces) {
					return
				}
				p := pieces[i]
				if ctx.Err() == nil && verifyOneFilePiece(state, fi, rf, buf, p) {
					continue
				}
				failedMu.Lock()
				failed = append(failed, p)
				failedMu.Unlock()
			}
		})
	}
	wg.Wait()

	sort.Ints(failed)
	return failed
}

// verifyOneFilePiece reads interior piece p of fi through rf into buf and
// reports whether its hash matches. Pieces with no known hash pass trivially.
func verifyOneFilePiece(state *serverTorrentState, fi *serverFileInfo, rf *os.File, buf []byte, p int) bool {
	if state.pieceHashes[p] == "" {
		return true
	}

	pieceStart := int64(p) * state.pieceLength
	pieceEnd := min(pieceStart+state.pieceLength, state.totalSize)
	pieceSize := pieceEnd - pieceStart

	pieceBuf := buf[:pieceSize]
	n, readErr := rf.ReadAt(pieceBuf, pieceStart-fi.offset)
	if readErr != nil || int64(n) != pieceSize {
		return false
	}

	return utils.VerifyPieceHash(pieceBuf, state.pieceHashes[p]) == nil
}

// markInteriorVerified marks every interior piece of fi as verified post-flush
// so verifyFinalizedPieces can skip them, except the pieces in failed (ascending,
// as returned by verifyFilePieces). Boundary pieces span adjacent files and remain
// unverified - they'll be checked at finalize. Caller must hold state.mu.
func markInteriorVerified(state *serverTorrentState, fi *serverFileInfo, failed []int) {
	if state.verified == nil {
		return
	}
	forEachInteriorPiece(state, fi, func(p int) {
		if _, bad := slices.BinarySearch(failed, p); !bad {
			state.verified.Set(uint(p))
		}
	})
}

// preVerifyCandidates returns the files whose data is already complete on disk
// at init: files found pre-existing at their final path (a prior session
// early-finalized them) and files hardlinked from another torrent. Both reach
// FinalizeTorrent with nothing in state.verified, so verifyFinalizedPieces
// re-reads and re-hashes every byte of them.
//
// Restricted to hlStateComplete because that is also the one file state whose
// fi.path is never rewritten afterwards - neither earlyFinalizeFile nor
// renamePartialFiles touches these - so the pass can read the path without
// holding state.mu for its whole duration.
func preVerifyCandidates(state *serverTorrentState) []*serverFileInfo {
	var out []*serverFileInfo
	for _, fi := range state.files {
		if fi.selected && fi.size > 0 && fi.hardlink.state == hlStateComplete {
			out = append(out, fi)
		}
	}
	return out
}

// preVerifyCompleteFiles read-back-verifies files that were already complete on
// disk when the torrent initialized, marking their interior pieces so
// verifyFinalizedPieces can skip them. Resuming an interrupted transfer
// otherwise re-reads and re-hashes everything a prior session already wrote,
// and a cross-seeded torrent re-reads every hardlinked file - both entirely
// inside the finalize stall the source is blocked on. Reading concurrently with
// the transfer trades NFS read bandwidth (which the network-bound stream is not
// using) for that stall.
//
// Purely additive: bits are only ever set, only for pieces that read back and
// hashed correctly. A piece that fails here is left for verifyFinalizedPieces,
// which is also the only path that may act on the failure - these files are
// skipForWriteData, so clearing their written bits here would ask the source to
// re-stream data writePieceData would then drop.
func (s *Server) preVerifyCompleteFiles(ctx context.Context, hash string, state *serverTorrentState) {
	if state.verified == nil {
		return
	}
	files := preVerifyCandidates(state)
	start := time.Now()
	pieces := 0

	for _, fi := range files {
		if ctx.Err() != nil {
			return
		}
		failed, _ := s.verifyFilePieces(ctx, state, fi, nil)

		state.mu.Lock()
		before := state.verified.Count()
		markInteriorVerified(state, fi, failed)
		pieces += int(state.verified.Count() - before)
		state.mu.Unlock()
	}

	s.logger.InfoContext(ctx, "pre-verified pieces already on disk at init",
		"hash", hash,
		"files", len(files),
		"pieces", pieces,
		"duration", time.Since(start).Round(time.Millisecond),
	)
}

// forEachInteriorPiece invokes fn for each piece fully contained within fi
// (i.e., not spanning into an adjacent file).
func forEachInteriorPiece(state *serverTorrentState, fi *serverFileInfo, fn func(p int)) {
	for p := fi.firstPiece; p <= fi.lastPiece; p++ {
		pieceStart := int64(p) * state.pieceLength
		pieceEnd := min(pieceStart+state.pieceLength, state.totalSize)
		if pieceStart < fi.offset || pieceEnd > fi.offset+fi.size {
			continue
		}
		fn(p)
	}
}

// checkFileCompletions checks if the just-written piece completes any file's
// piece coverage. If so, immediately syncs, closes, verifies interior pieces,
// and renames that file from .partial to its final path. Caller must hold state.mu.
// Note: earlyFinalizeFile temporarily releases state.mu during I/O operations.
func (s *Server) checkFileCompletions(
	ctx context.Context,
	hash string,
	state *serverTorrentState,
	pieceIndex int32,
) {
	idx := int(pieceIndex)
	// Files are sorted by offset, so firstPiece is monotonically non-decreasing.
	// Binary-search past every file whose lastPiece is below idx — turns the
	// per-piece scan from O(F) into O(log F) for many-file torrents. Any piece
	// overlaps at most a small contiguous range of files; iteration stops as
	// soon as we encounter a file whose firstPiece exceeds idx.
	startIdx := sort.Search(len(state.files), func(i int) bool {
		return state.files[i].lastPiece >= idx
	})
	for i := startIdx; i < len(state.files); i++ {
		fi := state.files[i]
		if fi.firstPiece > idx {
			break
		}
		if fi.earlyFinalized || fi.size <= 0 || fi.skipForWriteData() {
			continue
		}
		fi.piecesWritten++
		if fi.piecesWritten < fi.piecesTotal {
			continue
		}
		s.earlyFinalizeFile(ctx, hash, state, fi, i)
	}
}

// earlyFinalizeFile syncs, verifies, and renames a completed .partial file.
// On verification failure, marks failed pieces as unwritten for re-streaming.
//
// Caller must hold state.mu. This method temporarily releases state.mu during
// fsync, close, and piece verification to avoid blocking concurrent WritePiece
// calls. It is safe because all pieces overlapping this file are already marked
// written, so no concurrent WritePiece will access fi.file.
func (s *Server) earlyFinalizeFile(
	ctx context.Context,
	hash string,
	state *serverTorrentState,
	fi *serverFileInfo,
	fileIndex int,
) {
	// Snapshot the file handle and prevent concurrent access.
	fh := fi.file
	fi.file = nil
	fi.earlyFinalized = true // Block re-entry from concurrent checkFileCompletions

	// Release lock for I/O: fsync, verify (via the same fd), then close.
	// Verifying through the still-open fd skips re-opening the file per piece,
	// which on NFS saves two round-trips (LOOKUP + OPEN) per piece.
	state.mu.Unlock()

	failedPieces, syncCloseErr := s.syncVerifyClose(ctx, hash, state, fi, fh)

	state.mu.Lock()

	// If FinalizeTorrent started while we released the lock, bail out.
	// Background verification (verifyFinalizedPieces) will catch any corruption,
	// and modifying state.written here could double-decrement with
	// recoverVerificationFailure which doesn't guard against already-false entries.
	if state.finalization.active {
		s.logger.InfoContext(ctx, "finalization started during early finalize I/O, deferring",
			"hash", hash, "file", fi.path)
		return
	}

	if syncCloseErr != nil {
		fi.earlyFinalized = false
		// Reopen the file so finalizeFiles() can retry the sync.
		// The original handle was closed by syncAndCloseHandle even on error.
		if reopenErr := fi.openForWrite(); reopenErr != nil {
			s.logger.ErrorContext(ctx, "failed to reopen file after sync failure",
				"hash", hash, "file", fi.path, "error", reopenErr)
		}
		s.logger.WarnContext(ctx, "early finalization sync failed, deferring to finalizeFiles",
			"hash", hash, "file", fi.path, "error", syncCloseErr)
		return
	}

	if len(failedPieces) > 0 {
		fi.earlyFinalized = false
		for _, p := range failedPieces {
			state.written.Clear(uint(p))
			fi.piecesWritten--
		}
		state.dirty = true
		if state.statePath != "" {
			if saveErr := s.saveState(state.statePath, state.written); saveErr != nil {
				metrics.StateSaveErrorsTotal.WithLabelValues(metrics.ModeDestination).Inc()
				s.logger.ErrorContext(ctx, "failed to persist state after verify failure",
					"hash", hash, "file", fi.path, "error", saveErr)
			} else {
				state.dirty = false
				state.flushGen++
			}
		}
		metrics.EarlyFinalizeVerifyFailuresTotal.Inc()
		s.logger.WarnContext(ctx, "early verify failed, pieces will be re-streamed",
			"hash", hash, "file", fi.path, "failedPieces", len(failedPieces))
		return // File stays as .partial
	}

	if err := s.renamePartialFile(ctx, hash, fi); err != nil {
		fi.earlyFinalized = false
		// Sync succeeded, rename didn't. fi.file is nil (closed by
		// syncVerifyClose). Reopen so a concurrent WritePiece doesn't
		// race with finalizeFiles' retry on a nil handle, and so the
		// state machine matches the syncCloseErr path above.
		if reopenErr := fi.openForWrite(); reopenErr != nil {
			s.logger.ErrorContext(ctx, "failed to reopen file after rename failure",
				"hash", hash, "file", fi.path, "error", reopenErr)
		}
		s.logger.WarnContext(ctx, "early finalization rename failed, deferring",
			"hash", hash, "file", fi.path, "error", err)
		return
	}

	markInteriorVerified(state, fi, nil)

	fi.path = targetPath(fi)
	metrics.FilesEarlyFinalizedTotal.Inc()
	s.logger.InfoContext(ctx, "file early-finalized",
		"hash", hash, "file", fi.path, "fileIndex", fileIndex)
}

// syncVerifyClose syncs the supplied handle (if any), verifies the file's
// interior pieces, then closes the handle. Always attempts close even on
// sync error. Returns the first sync/close error and the list of failed
// piece indices (only meaningful when the returned error is nil).
//
// When fh is non-nil, the verify reads use it directly, which saves NFS
// LOOKUP+OPEN round-trips per piece compared to verifyFilePieces opening
// fresh. When fh is nil (e.g., test setup that bypasses openForWrite),
// verifyFilePieces opens fi.path itself.
func (s *Server) syncVerifyClose(
	ctx context.Context,
	hash string,
	state *serverTorrentState,
	fi *serverFileInfo,
	fh *os.File,
) ([]int, error) {
	var firstErr error
	var failedPieces []int

	if fh != nil {
		if syncErr := fh.Sync(); syncErr != nil {
			metrics.FileSyncErrorsTotal.WithLabelValues(metrics.ModeDestination).Inc()
			s.logger.WarnContext(ctx, "failed to sync file",
				"hash", hash, "path", fi.path, "error", syncErr)
			firstErr = fmt.Errorf("syncing %s: %w", fi.path, syncErr)
		}
	}

	if firstErr == nil {
		var interrupted bool
		failedPieces, interrupted = s.verifyFilePieces(ctx, state, fi, fh)
		if interrupted {
			// An interrupted pass reports the pieces it never read as failed,
			// which is the right answer for a caller that only adds skips but
			// the wrong one here: clearing them would re-stream a file that is
			// almost certainly intact. Defer the whole file to finalizeFiles.
			failedPieces = nil
			firstErr = fmt.Errorf("verifying %s: %w", fi.path, ctx.Err())
		}
	}

	if fh != nil {
		if closeErr := fh.Close(); closeErr != nil {
			metrics.FileSyncErrorsTotal.WithLabelValues(metrics.ModeDestination).Inc()
			s.logger.WarnContext(ctx, "failed to close file",
				"hash", hash, "path", fi.path, "error", closeErr)
			if firstErr == nil {
				firstErr = fmt.Errorf("closing %s: %w", fi.path, closeErr)
			}
		}
	}

	return failedPieces, firstErr
}
