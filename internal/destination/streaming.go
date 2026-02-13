package destination

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/arsac/qb-sync/internal/metrics"
	pb "github.com/arsac/qb-sync/proto"
)

// StreamPiecesBidi provides full-duplex streaming for maximum throughput.
// Uses a worker pool to process pieces concurrently while maintaining
// reliable acknowledgment delivery.
func (s *Server) StreamPiecesBidi(stream pb.QBSyncService_StreamPiecesBidiServer) error {
	ctx := stream.Context()

	numWorkers := s.config.StreamWorkers
	if numWorkers <= 0 {
		numWorkers = defaultStreamWorkers
	}

	// Bound work channel to numWorkers: each queued item holds full piece data
	// (up to 16 MB), so a larger buffer risks unbounded memory growth across
	// concurrent streams. When full, streamReceiver blocks and gRPC HTTP/2 flow
	// control provides backpressure to the sender.
	workCh := make(chan *pb.WritePieceRequest, numWorkers)
	ackCh := make(chan *pb.PieceAck, ackQueueSize)
	errCh := make(chan error, 1)

	s.logger.InfoContext(ctx, "bidi stream started", "workers", numWorkers)

	var wg sync.WaitGroup

	for range numWorkers {
		wg.Go(func() {
			s.streamWorker(ctx, workCh, ackCh)
		})
	}

	// Ack sender runs separately from worker WaitGroup to avoid deadlock
	ackDone := make(chan struct{})
	go func() {
		defer close(ackDone)
		s.streamAckSender(ctx, stream, ackCh, errCh)
	}()

	piecesReceived, recvErr := s.streamReceiver(ctx, stream, workCh)
	close(workCh)
	wg.Wait()

	// Release budget for any pieces left in workCh that workers didn't process
	// (e.g., all workers exited early via ctx.Done at the ack send).
	for req := range workCh {
		if dataLen := int64(len(req.GetData())); dataLen > 0 {
			s.memBudget.Release(dataLen)
		}
	}

	close(ackCh)
	<-ackDone
	metrics.DestWorkerQueueDepth.Set(0)
	metrics.DestWorkersBusy.Set(0)
	select {
	case sendErr := <-errCh:
		if sendErr != nil {
			return sendErr
		}
	default:
	}

	s.logger.InfoContext(ctx, "bidi stream ended", "piecesReceived", piecesReceived)

	return recvErr
}

// streamReceiver reads pieces from the stream and dispatches them to workers.
// Returns the number of pieces received and any error.
func (s *Server) streamReceiver(
	ctx context.Context,
	stream pb.QBSyncService_StreamPiecesBidiServer,
	workCh chan<- *pb.WritePieceRequest,
) (int64, error) {
	var count int64
	for {
		req, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			return count, nil // Client finished sending
		}
		if recvErr != nil {
			return count, recvErr
		}

		// Acquire memory budget for piece data before queuing.
		// When this blocks, we stop calling stream.Recv(), which triggers
		// gRPC HTTP/2 flow control back to source — proper backpressure.
		dataLen := int64(len(req.GetData()))
		if dataLen > 0 {
			if acqErr := s.memBudget.Acquire(ctx, dataLen); acqErr != nil {
				return count, acqErr
			}
		}

		select {
		case workCh <- req:
			count++
			metrics.DestWorkerQueueDepth.Set(float64(len(workCh)))
		case <-ctx.Done():
			if dataLen > 0 {
				s.memBudget.Release(dataLen)
			}
			return count, ctx.Err()
		}
	}
}

// streamWorker processes pieces from the work channel.
func (s *Server) streamWorker(
	ctx context.Context,
	workCh <-chan *pb.WritePieceRequest,
	ackCh chan<- *pb.PieceAck,
) {
	for req := range workCh {
		metrics.DestWorkersBusy.Inc()
		result := s.writePiece(ctx, req)
		metrics.DestWorkersBusy.Dec()

		// Release memory budget now that piece data has been written to disk.
		if dataLen := int64(len(req.GetData())); dataLen > 0 {
			s.memBudget.Release(dataLen)
		}

		ack := &pb.PieceAck{
			TorrentHash: req.GetTorrentHash(),
			PieceIndex:  req.GetPieceIndex(),
			Success:     result.success,
			Error:       result.errMsg,
			ErrorCode:   result.errorCode,
		}

		select {
		case ackCh <- ack:
		case <-ctx.Done():
			return
		}
	}
}

// streamAckSender sends acknowledgments back over the stream.
func (s *Server) streamAckSender(
	_ context.Context,
	stream pb.QBSyncService_StreamPiecesBidiServer,
	ackCh <-chan *pb.PieceAck,
	errCh chan<- error,
) {
	for ack := range ackCh {
		if sendErr := stream.Send(ack); sendErr != nil {
			select {
			case errCh <- sendErr:
			default:
			}
			// Drain remaining acks to let workers finish
			//nolint:revive // intentionally empty - just draining channel
			for range ackCh {
			}
			return
		}
	}
}
