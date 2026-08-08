package streaming

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"github.com/arsac/qb-sync/internal/metrics"
	pb "github.com/arsac/qb-sync/proto"
)

// errSendClosed reports that CloseSend already half-closed the stream, so no
// further pieces can be sent over it.
var errSendClosed = errors.New("stream send closed")

// ackSink consumes everything a stream's receive loop produces. StreamPool is
// the production implementation: it routes acks and errors straight into its
// aggregated channels, so an ack crosses one channel between gRPC and the ack
// processor rather than being relayed by a second per-stream goroutine.
type ackSink interface {
	// deliverAck hands one ack to the sink, blocking until it is accepted or
	// ctx (the stream's own context) is cancelled. Returning false stops the
	// receive loop and so tears the stream down; the sink records why, since
	// only it knows whether the consumer went away or simply stopped keeping
	// up.
	deliverAck(ctx context.Context, ack *pb.PieceAck) bool

	// streamEnded reports that the receive loop exited. err is the stream
	// error, or nil for a clean EOF or a cancelled stream context - a stream
	// that ends without an error still ends, which the sink must notice.
	streamEnded(err error)
}

// PieceStream manages a bidirectional streaming connection for piece transfer.
// This is a thin wrapper around the gRPC stream - in-flight tracking is handled
// by AdaptiveWindow in BidiQueue for congestion control.
//
// Each PieceStream owns a cancellable context derived from the parent. When the
// receive loop detects stream death, it cancels the context to unblock any Send()
// stuck on HTTP/2 flow control.
//
// sendMu keeps exactly one goroutine inside stream.Send at a time, which is
// gRPC's requirement for a stream's send side.
type PieceStream struct {
	connIdx int // Index of the gRPC connection this stream uses

	ctx    context.Context
	cancel context.CancelFunc // Cancels stream context; unblocks stuck Send()
	stream pb.QBSyncService_StreamPiecesBidiClient
	logger *slog.Logger

	done chan struct{} // Closed when receive goroutine exits

	// sendMu serializes stream.Send and stream.CloseSend. sendClosed is set
	// under it so a Send that was waiting its turn is refused rather than
	// reaching a half-closed stream.
	sendMu     sync.Mutex
	sendClosed bool

	closeSendOnce sync.Once // Protects CloseSend from multiple calls
	closeSendErr  error     // Result of the first CloseSend call

	// Test-overridable timeout. Zero means use the package-level const.
	sendTimeoutOverride time.Duration
}

func (ps *PieceStream) effectiveSendTimeout() time.Duration {
	if ps.sendTimeoutOverride > 0 {
		return ps.sendTimeoutOverride
	}
	return sendTimeout
}

// receiveAcks reads acknowledgments from the stream and hands each one to sink.
// Exits when the context is cancelled, the sink stops accepting, or the stream
// ends. On exit it reports the outcome to the sink and then cancels the stream
// context to unblock any Send() stuck on HTTP/2 flow control - this is the
// primary mechanism that breaks the deadlock when the receiver stops consuming
// data, and the only way stream death is detected at all, which is why a sink
// that cannot keep up must stop the loop rather than block it.
//
// The sink is notified before ps.done closes, so anything that waits on Done()
// sees the reason the stream ended already published.
func (ps *PieceStream) receiveAcks(sink ackSink) {
	var exitErr error

	defer close(ps.done)
	defer ps.cancel() // Unblock stuck Send() by cancelling the stream context
	defer func() { sink.streamEnded(exitErr) }()
	defer func() {
		if r := recover(); r != nil {
			exitErr = fmt.Errorf("panic in receiveAcks: %v", r)
			ps.logger.Error("panic in receiveAcks",
				"panic", r,
				"stack", string(debug.Stack()),
			)
		}
	}()

	for {
		// Check for context cancellation before blocking on Recv
		select {
		case <-ps.ctx.Done():
			metrics.ReceiveAcksExitTotal.WithLabelValues(metrics.ReasonContextCancel).Inc()
			return
		default:
		}

		ack, err := ps.stream.Recv()
		switch {
		case errors.Is(err, io.EOF):
			metrics.ReceiveAcksExitTotal.WithLabelValues(metrics.ReasonEOF).Inc()
			return
		case err != nil && ps.ctx.Err() != nil:
			metrics.ReceiveAcksExitTotal.WithLabelValues(metrics.ReasonContextCancel).Inc()
			return
		case err != nil:
			metrics.ReceiveAcksExitTotal.WithLabelValues(metrics.ReasonStreamError).Inc()
			exitErr = err
			return
		}

		if !sink.deliverAck(ps.ctx, ack) {
			return // Sink recorded why; defer ps.cancel() unblocks stuck Send()
		}
	}
}

// Send sends a piece over the stream. Safe for concurrent use: sendMu admits one
// caller at a time, so the stream sees a single sender as gRPC requires.
//
// A send that doesn't return within the timeout cancels the stream context,
// which makes gRPC reset the stream and unblocks a Send parked on HTTP/2 flow
// control. That is the independent safety net for the case where receiveAcks is
// itself stuck and so cannot detect stream death and cancel on its own; it also
// releases any caller queued behind this one, since the next to acquire sendMu
// sees the cancelled context.
func (ps *PieceStream) Send(req *pb.WritePieceRequest) error {
	// Fast path: stream already cancelled.
	if err := ps.ctx.Err(); err != nil {
		return err
	}

	ps.sendMu.Lock()
	defer ps.sendMu.Unlock()

	if ps.sendClosed {
		return errSendClosed
	}
	if err := ps.ctx.Err(); err != nil {
		return err
	}

	timer := time.AfterFunc(ps.effectiveSendTimeout(), ps.cancelOnSendTimeout)
	defer timer.Stop()

	return ps.stream.Send(req)
}

// cancelOnSendTimeout tears down a stream whose Send has not returned in time.
func (ps *PieceStream) cancelOnSendTimeout() {
	metrics.SendTimeoutTotal.Inc()
	ps.cancel()
}

// Done returns a channel that's closed when the stream ends.
func (ps *PieceStream) Done() <-chan struct{} {
	return ps.done
}

// CloseSend signals that no more pieces will be sent. Taking sendMu waits out
// any in-progress send and refuses every later one, so stream.CloseSend() cannot
// interleave with a Send. Safe for concurrent and repeated calls via [sync.Once].
// Does not cancel the stream context, so receiveAcks continues to drain acks
// after the send side closes.
func (ps *PieceStream) CloseSend() error {
	ps.closeSendOnce.Do(func() {
		ps.sendMu.Lock()
		defer ps.sendMu.Unlock()

		ps.sendClosed = true
		ps.closeSendErr = ps.stream.CloseSend()
	})
	return ps.closeSendErr
}

// Close closes the stream and waits for all goroutines to exit.
// This should be called when the stream is no longer needed to ensure clean shutdown.
func (ps *PieceStream) Close() {
	// Cancel stream context first to unblock a Send stuck in stream.Send() due
	// to HTTP/2 flow control, so CloseSend doesn't wait on sendMu indefinitely.
	ps.cancel()

	_ = ps.CloseSend()

	// Wait for receiver goroutine to exit (it will exit when stream ends or errors)
	<-ps.done
}
