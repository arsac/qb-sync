package destination

import (
	"context"
	"fmt"
	"strings"
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
func (s *Server) markPieceWritten(ctx context.Context, hash string, state *serverTorrentState, pieceIndex int32) {
	if int(pieceIndex) >= len(state.written) {
		return
	}

	state.written[pieceIndex] = true
	state.writtenCount++
	state.dirty = true
	state.piecesSinceFlush++

	flushCount := s.config.StateFlushCount
	if flushCount == 0 {
		flushCount = defaultStateFlushCount
	}

	shouldFlush := state.piecesSinceFlush >= flushCount || state.writtenCount == len(state.written)

	if shouldFlush && state.statePath != "" {
		if saveErr := s.saveState(state.statePath, state.written); saveErr != nil {
			metrics.StateSaveErrorsTotal.WithLabelValues(metrics.ModeDestination).Inc()
			s.logger.WarnContext(ctx, "failed to save state", "hash", hash, "error", saveErr)
		} else {
			state.dirty = false
			state.piecesSinceFlush = 0
		}
	}
}

// writePiece receives and writes a single piece.
func (s *Server) writePiece(ctx context.Context, req *pb.WritePieceRequest) writeResult {
	if s.config.DryRun {
		return writePieceOK()
	}

	torrentHash := req.GetTorrentHash()

	s.mu.RLock()
	state, exists := s.torrents[torrentHash]
	initializing := exists && state.initializing
	s.mu.RUnlock()

	if !exists || initializing {
		return writePieceError("torrent not initialized", pb.PieceErrorCode_PIECE_ERROR_NOT_INITIALIZED)
	}

	pieceIndex := req.GetPieceIndex()
	data := req.GetData()

	// Early check with lock (optimization to skip hash verification in common cases).
	// This is NOT the correctness check - see double-check below after hash verification.
	state.mu.Lock()
	alreadyWritten := int(pieceIndex) < len(state.written) && state.written[pieceIndex]
	isFinalizing := state.finalizing
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
	if state.classifyPiece(int(pieceIndex)) == pieceFullySelected {
		if hashErr := state.verifyPieceHash(pieceIndex, data, req.GetPieceHash()); hashErr != "" {
			metrics.PieceWriteDuration.Observe(time.Since(writeStart).Seconds())
			return writePieceError(hashErr, pb.PieceErrorCode_PIECE_ERROR_HASH_MISMATCH)
		}
	}

	state.mu.Lock()
	defer state.mu.Unlock()

	// CORRECTNESS CHECK: Re-verify finalizing flag under lock.
	// Even if finalization started between the early check and now, this prevents the write.
	if state.finalizing {
		return writePieceError("torrent is being finalized", pb.PieceErrorCode_PIECE_ERROR_FINALIZING)
	}

	if int(pieceIndex) < len(state.written) && state.written[pieceIndex] {
		return writePieceOK()
	}

	if writeErr := state.writePieceData(req.GetOffset(), data); writeErr != nil {
		metrics.PieceWriteErrorsTotal.WithLabelValues(metrics.ModeDestination).Inc()
		return writePieceError(fmt.Sprintf("write failed: %v", writeErr), pb.PieceErrorCode_PIECE_ERROR_IO)
	}

	s.markPieceWritten(ctx, torrentHash, state, pieceIndex)
	s.checkFileCompletions(ctx, torrentHash, state, pieceIndex)
	metrics.PieceWriteDuration.Observe(time.Since(writeStart).Seconds())

	metrics.PiecesReceivedTotal.Inc()
	metrics.BytesReceivedTotal.Add(float64(len(data)))

	if state.writtenCount%50 == 0 || state.writtenCount == len(state.written) {
		s.logger.InfoContext(ctx, "write progress",
			"hash", torrentHash,
			"progress", fmt.Sprintf("%d/%d", state.writtenCount, len(state.written)),
		)
	}

	return writePieceOK()
}

// verifyFilePieces reads back interior pieces from a synced .partial file and
// verifies their hashes. Returns indices of pieces that failed verification.
// Boundary pieces (spanning adjacent files) are skipped — they are deferred to
// verifyFinalizedPieces. Caller must hold state.mu.
func (s *Server) verifyFilePieces(
	state *serverTorrentState,
	fi *serverFileInfo,
) []int {
	if len(state.pieceHashes) == 0 {
		return nil
	}

	var failed []int
	for p := fi.firstPiece; p <= fi.lastPiece; p++ {
		if state.pieceHashes[p] == "" {
			continue
		}

		pieceStart := int64(p) * state.pieceLength
		pieceEnd := min(pieceStart+state.pieceLength, state.totalSize)

		// Skip boundary pieces — they span adjacent files and can't be
		// fully read from this file alone. Deferred to verifyFinalizedPieces.
		if pieceStart < fi.offset || pieceEnd > fi.offset+fi.size {
			continue
		}

		pieceSize := pieceEnd - pieceStart
		fileOffset := pieceStart - fi.offset

		data, readErr := utils.ReadChunkFromFile(fi.path, fileOffset, pieceSize)
		if readErr != nil {
			failed = append(failed, p)
			continue
		}

		if err := utils.VerifyPieceHash(data, state.pieceHashes[p]); err != nil {
			failed = append(failed, p)
		}
	}

	return failed
}

// checkFileCompletions checks if the just-written piece completes any file's
// piece coverage. If so, immediately syncs, closes, verifies interior pieces,
// and renames that file from .partial to its final path. Caller must hold state.mu.
func (s *Server) checkFileCompletions(
	ctx context.Context,
	hash string,
	state *serverTorrentState,
	pieceIndex int32,
) {
	idx := int(pieceIndex)
	for i, fi := range state.files {
		if fi.earlyFinalized || fi.size <= 0 {
			continue
		}
		if fi.hlState == hlStateComplete || fi.hlState == hlStatePending {
			continue
		}
		if !fi.overlaps(idx) {
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
// Caller must hold state.mu.
func (s *Server) earlyFinalizeFile(
	ctx context.Context,
	hash string,
	state *serverTorrentState,
	fi *serverFileInfo,
	fileIndex int,
) {
	if err := s.closeFileHandle(ctx, hash, fi); err != nil {
		s.logger.WarnContext(ctx, "early finalization sync failed, deferring",
			"hash", hash, "file", fi.path, "error", err)
		return // finalizeFiles() will retry
	}
	// Verify interior pieces by reading back from disk after sync.
	if failedPieces := s.verifyFilePieces(state, fi); len(failedPieces) > 0 {
		for _, p := range failedPieces {
			state.written[p] = false
			state.writtenCount--
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
			}
		}
		metrics.EarlyFinalizeVerifyFailuresTotal.Inc()
		s.logger.WarnContext(ctx, "early verify failed, pieces will be re-streamed",
			"hash", hash, "file", fi.path, "failedPieces", len(failedPieces))
		return // File stays as .partial
	}
	if err := s.renamePartialFile(ctx, hash, fi); err != nil {
		s.logger.WarnContext(ctx, "early finalization rename failed, deferring",
			"hash", hash, "file", fi.path, "error", err)
		return
	}
	fi.path = strings.TrimSuffix(fi.path, partialSuffix)
	fi.earlyFinalized = true
	metrics.FilesEarlyFinalizedTotal.Inc()
	s.logger.InfoContext(ctx, "file early-finalized",
		"hash", hash, "file", fi.path, "fileIndex", fileIndex)
}
