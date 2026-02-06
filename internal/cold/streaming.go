package cold

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

	workCh := make(chan *pb.WritePieceRequest, streamWorkQueueSize)
	ackCh := make(chan *pb.PieceAck, streamWorkQueueSize)
	errCh := make(chan error, 1)

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

	recvErr := s.streamReceiver(ctx, stream, workCh)
	close(workCh)
	wg.Wait()
	close(ackCh)
	<-ackDone
	metrics.ColdWorkerQueueDepth.Set(0)
	metrics.ColdWorkersBusy.Set(0)
	select {
	case sendErr := <-errCh:
		if sendErr != nil {
			return sendErr
		}
	default:
	}

	return recvErr
}

// streamReceiver reads pieces from the stream and dispatches them to workers.
func (s *Server) streamReceiver(
	ctx context.Context,
	stream pb.QBSyncService_StreamPiecesBidiServer,
	workCh chan<- *pb.WritePieceRequest,
) error {
	for {
		req, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			return nil // Client finished sending
		}
		if recvErr != nil {
			return recvErr
		}

		select {
		case workCh <- req:
			metrics.ColdWorkerQueueDepth.Set(float64(len(workCh)))
		case <-ctx.Done():
			return ctx.Err()
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
		metrics.ColdWorkersBusy.Inc()
		resp, writeErr := s.WritePiece(ctx, req)
		metrics.ColdWorkersBusy.Dec()
		ack := &pb.PieceAck{
			TorrentHash: req.GetTorrentHash(),
			PieceIndex:  req.GetPieceIndex(),
		}

		switch {
		case writeErr != nil:
			ack.Success = false
			ack.Error = writeErr.Error()
			ack.ErrorCode = pb.PieceErrorCode_PIECE_ERROR_IO
		case !resp.GetSuccess():
			ack.Success = false
			ack.Error = resp.GetError()
			ack.ErrorCode = resp.GetErrorCode()
		default:
			ack.Success = true
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
