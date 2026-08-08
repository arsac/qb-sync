package destination

import (
	"context"
	"fmt"
	"os"
	"slices"
	"sort"
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

	// Disk I/O outside state.mu: writePieceData reads immutable metadata (files
	// slice, offsets) directly, and reaches every mutable field it needs - path,
	// handle, early-finalized and hardlink state - through writeAt, which holds
	// fileMu. That covers the overlaps state.mu would otherwise be needed for: a
	// duplicate the source re-sent can still be here when another worker's write
	// completes the file and hands it to an early finalization that renames it,
	// or when finalization resolves a neighbouring file's pending hardlink.
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
	s.checkFileCompletions(torrentHash, state, pieceIndex)
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

// verifyFilePieces reads back interior pieces from a file and verifies their
// hashes through the shared verifyPieceSet worker pool. Returns indices of
// pieces that failed verification, ascending, and whether ctx cancellation cut
// the pass short. Boundary pieces (spanning adjacent files) are skipped - they
// are deferred to verifyFinalizedPieces.
//
// Interior pieces lie entirely within fi, so a single region describes every
// read the pass makes. Each worker opens its own handle on it via the pool's
// per-goroutine FdCache; a file that can't be opened at all fails every piece,
// which is what makes the caller re-stream it.
//
// Safe to call without state.mu: all accessed fields (pieceHashes, pieceLength,
// totalSize, fi geometry) are immutable after initialization.
func (s *Server) verifyFilePieces(
	ctx context.Context,
	hash string,
	state *serverTorrentState,
	fi *serverFileInfo,
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

	regions := []utils.FileRegion{{Path: fi.path, Offset: fi.offset, Size: fi.size}}
	return s.verifyPieceSet(ctx, hash, state, regions, pieces, nil), ctx.Err() != nil
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

	done := 0
	for _, fi := range files {
		if ctx.Err() != nil {
			break
		}
		failed, _ := s.verifyFilePieces(ctx, hash, state, fi)

		state.mu.Lock()
		before := state.verified.Count()
		markInteriorVerified(state, fi, failed)
		pieces += int(state.verified.Count() - before)
		state.mu.Unlock()
		done++
	}

	s.logger.InfoContext(ctx, "pre-verified pieces already on disk at init",
		"hash", hash,
		"files", done,
		"candidates", len(files),
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
// piece coverage. If so, it hands that file's sync, verify and rename to a
// background early finalization. Caller must hold state.mu.
func (s *Server) checkFileCompletions(
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
		s.startEarlyFinalize(hash, state, fi, i)
	}
}

// startEarlyFinalize hands a completed file's fsync, read-back verification and
// rename to a background goroutine.
//
// That work is NFS-bound and proportional to the file's size, so running it
// inline held the ack for the piece that completed the file: on a multi-GB file
// it outlasts the source's 60s in-flight piece timeout, which marks the piece
// stale, halves the stream's congestion window and re-sends data the
// destination already has. It also parked one of the destination's stream
// workers and that piece's memory budget for the whole read-back. Both are
// released immediately now.
//
// Caller must hold state.mu. takeWriteHandle drains in-flight writes and closes
// the file to further ones before handing over its handle, so no concurrent
// WritePiece can be writing through the handle this goroutine is about to sync
// and close, or reopen the .partial it is about to rename away. The in-flight
// count is raised before the lock is dropped so FinalizeTorrent cannot start
// underneath the goroutine.
func (s *Server) startEarlyFinalize(
	hash string,
	state *serverTorrentState,
	fi *serverFileInfo,
	fileIndex int,
) {
	if s.bgCtx.Err() != nil {
		// Shutting down: bgWg may already be past its Wait. Leave the file
		// untouched for finalizeFiles to sync and rename.
		return
	}

	fh := fi.takeWriteHandle()

	state.earlyFinalizing++
	s.bgWg.Go(func() {
		defer state.finishEarlyFinalize()
		s.earlyFinalizeFile(s.bgCtx, hash, state, fi, fileIndex, fh)
	})
}

// earlyFinalizeFile syncs, verifies, and renames a completed .partial file.
// On verification failure, marks failed pieces as unwritten for re-streaming.
//
// Runs without state.mu and takes it only for the bookkeeping at the end. Safe
// because all pieces overlapping this file are already marked written, so no
// concurrent WritePiece will touch fi, and FinalizeTorrent defers while the
// early finalization is counted in flight.
//
// fh is the write handle snapshotted by startEarlyFinalize, which this
// goroutine owns: syncVerifyClose syncs it, verifies the file's contents and
// closes it.
func (s *Server) earlyFinalizeFile(
	ctx context.Context,
	hash string,
	state *serverTorrentState,
	fi *serverFileInfo,
	fileIndex int,
	fh *os.File,
) {
	// Bound concurrent read-back verifications, each of which holds up to
	// verifyConcurrency piece buffers and that many outstanding NFS reads. A
	// failed acquire means shutdown, and dropping through is right: the
	// cancelled ctx makes syncVerifyClose close the handle and report the
	// verify as interrupted, which defers the file to finalizeFiles.
	if acqErr := s.earlyFinalizeSem.Acquire(ctx, 1); acqErr == nil {
		defer s.earlyFinalizeSem.Release(1)
	}

	failedPieces, syncCloseErr := s.syncVerifyClose(ctx, hash, state, fi, fh)

	state.mu.Lock()
	defer state.mu.Unlock()

	if syncCloseErr != nil {
		fi.readmitWrites()
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
		fi.readmitWrites()
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
		fi.readmitWrites()
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

	fi.setPath(targetPath(fi))
	metrics.FilesEarlyFinalizedTotal.Inc()
	s.logger.InfoContext(ctx, "file early-finalized",
		"hash", hash, "file", fi.path, "fileIndex", fileIndex)
}

// syncVerifyClose syncs the supplied handle (if any), verifies the file's
// interior pieces, then closes the handle. Always attempts close even on
// sync error. Returns the first sync/close error and the list of failed
// piece indices (only meaningful when the returned error is nil).
//
// The sync must land before the verify: the read-back opens its own handles,
// so it sees the file's contents on the server, not this handle's dirty pages.
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
		failedPieces, interrupted = s.verifyFilePieces(ctx, hash, state, fi)
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
