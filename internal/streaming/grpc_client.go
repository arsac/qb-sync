package streaming

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	pb "github.com/arsac/qb-sync/proto"
)

const (
	// gRPC keepalive parameters.
	keepaliveTime    = 30 * time.Second // Send pings every 30 seconds if no activity
	keepaliveTimeout = 10 * time.Second // Wait 10 seconds for ping ack
)

// successResponse is implemented by gRPC response types that have Success/Error fields.
type successResponse interface {
	GetSuccess() bool
	GetError() string
}

// checkRPCResponse validates a gRPC response with success/error fields.
func checkRPCResponse(resp successResponse, operation string) error {
	if !resp.GetSuccess() {
		return fmt.Errorf("%s failed: %s", operation, resp.GetError())
	}
	return nil
}

var _ PieceDestination = (*GRPCDestination)(nil)
var _ HardlinkDestination = (*GRPCDestination)(nil)

// GRPCDestination sends pieces to a remote gRPC server.
// It provides PieceDestination-like functionality plus additional features
// for hardlink deduplication, streaming, and torrent lifecycle management.
type GRPCDestination struct {
	conn        *grpc.ClientConn
	client      pb.QBSyncServiceClient
	initResults map[string]*InitTorrentResult // Cached init results
	initGroup   singleflight.Group            // Deduplicates concurrent InitTorrent calls
	mu          sync.RWMutex
}

// NewGRPCDestination creates a new gRPC destination client.
func NewGRPCDestination(addr string) (*GRPCDestination, error) {
	// Configure keepalive for better connection management
	kaParams := keepalive.ClientParameters{
		Time:                keepaliveTime,
		Timeout:             keepaliveTimeout,
		PermitWithoutStream: true, // Send pings even without active streams
	}

	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(kaParams),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}

	return &GRPCDestination{
		conn:        conn,
		client:      pb.NewQBSyncServiceClient(conn),
		initResults: make(map[string]*InitTorrentResult),
	}, nil
}

// ValidateConnection checks that the cold server is reachable.
// This should be called before starting the main loop to fail fast.
func (d *GRPCDestination) ValidateConnection(ctx context.Context) error {
	// Use GetFileByInode with inode 0 as a health check.
	// This validates the gRPC connection is working and the server is responsive.
	// Inode 0 is invalid so it will always return not found, but that's fine for validation.
	_, err := d.client.GetFileByInode(ctx, &pb.GetFileByInodeRequest{
		Inode: 0,
	})
	if err != nil {
		return fmt.Errorf("cold server not reachable: %w", err)
	}
	return nil
}

// InitTorrentResult contains the result of InitTorrent including sync status and hardlink information.
type InitTorrentResult struct {
	Status            pb.TorrentSyncStatus
	PiecesNeeded      []bool
	PiecesNeededCount int32
	PiecesHaveCount   int32
	HardlinkResults   []*pb.HardlinkResult
}

// initTorrentResultFromProto converts a proto response to InitTorrentResult.
func initTorrentResultFromProto(resp *pb.InitTorrentResponse) *InitTorrentResult {
	return &InitTorrentResult{
		Status:            resp.GetStatus(),
		PiecesNeeded:      resp.GetPiecesNeeded(),
		PiecesNeededCount: resp.GetPiecesNeededCount(),
		PiecesHaveCount:   resp.GetPiecesHaveCount(),
		HardlinkResults:   resp.GetHardlinkResults(),
	}
}

// InitTorrent initializes a torrent on the remote server.
// Returns sync status, pieces needed for streaming, and hardlink results.
// Results are cached and returned on subsequent calls for the same torrent.
// Uses singleflight to deduplicate concurrent requests for the same torrent.
func (d *GRPCDestination) InitTorrent(ctx context.Context, req *pb.InitTorrentRequest) (*InitTorrentResult, error) {
	hash := req.GetTorrentHash()

	// Check cache first (fast path)
	d.mu.RLock()
	if result, ok := d.initResults[hash]; ok {
		d.mu.RUnlock()
		return result, nil
	}
	d.mu.RUnlock()

	// Use singleflight to deduplicate concurrent requests for same hash.
	// This avoids holding the lock during RPC which could cause deadlocks.
	v, err, _ := d.initGroup.Do(hash, func() (any, error) {
		// Double-check cache inside singleflight
		d.mu.RLock()
		if result, ok := d.initResults[hash]; ok {
			d.mu.RUnlock()
			return result, nil
		}
		d.mu.RUnlock()

		resp, rpcErr := d.client.InitTorrent(ctx, req)
		if rpcErr != nil {
			return nil, fmt.Errorf("init torrent RPC failed: %w", rpcErr)
		}

		if err := checkRPCResponse(resp, "init torrent"); err != nil {
			return nil, err
		}

		result := initTorrentResultFromProto(resp)

		// Cache the result
		d.mu.Lock()
		d.initResults[hash] = result
		d.mu.Unlock()

		return result, nil
	})

	if err != nil {
		return nil, err
	}
	result, ok := v.(*InitTorrentResult)
	if !ok {
		return nil, fmt.Errorf("unexpected type from singleflight: %T", v)
	}
	return result, nil
}

// IsInitialized returns whether a torrent has been initialized.
func (d *GRPCDestination) IsInitialized(hash string) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	_, ok := d.initResults[hash]
	return ok
}

// CheckTorrentStatus queries the cold server for a torrent's current sync status.
// Unlike InitTorrent, this does NOT cache results and does NOT require full torrent info.
// Use this for status checking (is torrent complete/verifying/ready on cold?).
// For full initialization with file tracking and hardlink detection, use InitTorrent.
//
// Note: This sends a minimal request with just the hash. Cold server will check:
// 1. qBittorrent status (returns COMPLETE/VERIFYING if found).
// 2. Existing tracking state (returns READY with pieces_needed if found).
// 3. If neither, returns READY with empty state (caller should do full InitTorrent).
func (d *GRPCDestination) CheckTorrentStatus(ctx context.Context, hash string) (*InitTorrentResult, error) {
	// Create minimal request with just the hash
	req := &pb.InitTorrentRequest{
		TorrentHash: hash,
	}

	resp, err := d.client.InitTorrent(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("check status RPC failed: %w", err)
	}

	if respErr := checkRPCResponse(resp, "check status"); respErr != nil {
		return nil, respErr
	}

	// Return result WITHOUT caching - BidiQueue should still do full InitTorrent
	return initTorrentResultFromProto(resp), nil
}

// WritePiece sends a piece to the remote server (unary, for simple cases).
func (d *GRPCDestination) WritePiece(ctx context.Context, req *pb.WritePieceRequest) error {
	resp, err := d.client.WritePiece(ctx, req)
	if err != nil {
		return fmt.Errorf("write piece RPC failed: %w", err)
	}
	return checkRPCResponse(resp, "write piece")
}

// OpenStream opens a bidirectional stream for high-throughput piece transfer.
func (d *GRPCDestination) OpenStream(ctx context.Context, logger *slog.Logger) (*PieceStream, error) {
	stream, err := d.client.StreamPiecesBidi(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to open stream: %w", err)
	}

	ps := &PieceStream{
		ctx:      ctx,
		stream:   stream,
		logger:   logger,
		acks:     make(chan *pb.PieceAck, DefaultAckChannelSize),
		ackReady: make(chan struct{}, 1), // Signal when acks are processed
		done:     make(chan struct{}),
		errors:   make(chan error, 1),
	}

	// Start goroutine to receive acks
	go ps.receiveAcks()

	return ps, nil
}

// GetFileByInode checks if a file with the given inode exists on the receiver.
func (d *GRPCDestination) GetFileByInode(ctx context.Context, inode uint64) (string, bool, error) {
	resp, err := d.client.GetFileByInode(ctx, &pb.GetFileByInodeRequest{
		Inode: inode,
	})
	if err != nil {
		return "", false, fmt.Errorf("get file by inode RPC failed: %w", err)
	}

	return resp.GetPath(), resp.GetFound(), nil
}

// CreateHardlink creates a hardlink on the receiver from source to target path.
func (d *GRPCDestination) CreateHardlink(ctx context.Context, sourcePath, targetPath string) error {
	resp, err := d.client.CreateHardlink(ctx, &pb.CreateHardlinkRequest{
		SourcePath: sourcePath,
		TargetPath: targetPath,
	})
	if err != nil {
		return fmt.Errorf("create hardlink RPC failed: %w", err)
	}
	return checkRPCResponse(resp, "create hardlink")
}

// RegisterFile registers a completed file for hardlink tracking on the receiver.
func (d *GRPCDestination) RegisterFile(ctx context.Context, inode uint64, path string, size int64) error {
	resp, err := d.client.RegisterFile(ctx, &pb.RegisterFileRequest{
		Inode: inode,
		Path:  path,
		Size:  size,
	})
	if err != nil {
		return fmt.Errorf("register file RPC failed: %w", err)
	}
	return checkRPCResponse(resp, "register file")
}

// FinalizeTorrent requests the cold server to finalize a torrent:
// rename .partial files, add to qBittorrent, verify, and confirm.
// On success, clears the cached init result to prevent memory leaks.
func (d *GRPCDestination) FinalizeTorrent(
	ctx context.Context,
	hash, savePath, category, tags string,
) error {
	resp, err := d.client.FinalizeTorrent(ctx, &pb.FinalizeTorrentRequest{
		TorrentHash: hash,
		SavePath:    savePath,
		Category:    category,
		Tags:        tags,
	})
	if err != nil {
		return fmt.Errorf("finalize torrent RPC failed: %w", err)
	}
	if respErr := checkRPCResponse(resp, "finalize"); respErr != nil {
		return respErr
	}

	// Clean up cached init result to prevent memory leak
	d.ClearInitResult(hash)
	return nil
}

// AbortTorrent requests the cold server to abort an in-progress torrent
// and optionally delete partial files. Called when a torrent is removed
// from hot before streaming completes.
func (d *GRPCDestination) AbortTorrent(ctx context.Context, hash string, deleteFiles bool) (int32, error) {
	// Always clear init results - the server removes tracking regardless of deletion success,
	// and if RPC fails the torrent needs re-initialization anyway
	defer d.ClearInitResult(hash)

	resp, err := d.client.AbortTorrent(ctx, &pb.AbortTorrentRequest{
		TorrentHash: hash,
		DeleteFiles: deleteFiles,
	})
	if err != nil {
		return 0, fmt.Errorf("abort torrent RPC failed: %w", err)
	}
	if respErr := checkRPCResponse(resp, "abort"); respErr != nil {
		return resp.GetFilesDeleted(), respErr
	}

	return resp.GetFilesDeleted(), nil
}

// StartTorrent resumes a stopped torrent on the cold server.
// Called during disk pressure cleanup after hot stops seeding,
// to ensure cold takes over before hot deletes.
func (d *GRPCDestination) StartTorrent(ctx context.Context, hash string) error {
	resp, err := d.client.StartTorrent(ctx, &pb.StartTorrentRequest{
		TorrentHash: hash,
	})
	if err != nil {
		return fmt.Errorf("start torrent RPC failed: %w", err)
	}
	return checkRPCResponse(resp, "start torrent")
}

// ClearInitResult removes a cached init result for a torrent hash.
// Use this when a piece ack indicates the torrent is not initialized on cold,
// so the next send triggers re-initialization.
func (d *GRPCDestination) ClearInitResult(hash string) {
	d.mu.Lock()
	delete(d.initResults, hash)
	d.mu.Unlock()
}

// Close closes the gRPC connection.
func (d *GRPCDestination) Close() error {
	return d.conn.Close()
}

// CleanupStaleEntries removes cached init results for torrents that are no longer active.
// Call this periodically with the set of active torrent hashes to prevent memory leaks
// for transfers that were interrupted without proper finalization or abort.
func (d *GRPCDestination) CleanupStaleEntries(activeHashes map[string]struct{}) int {
	d.mu.Lock()
	defer d.mu.Unlock()

	removed := 0
	for hash := range d.initResults {
		if _, active := activeHashes[hash]; !active {
			delete(d.initResults, hash)
			removed++
		}
	}
	return removed
}

// grpcStatusProvider is implemented by errors that wrap a gRPC status
// (e.g., fmt.Errorf("...: %w", statusErr)). status.FromError only checks
// the outermost error, so we fall back to errors.As for wrapped errors.
type grpcStatusProvider interface {
	GRPCStatus() *status.Status
}

// IsTransientError returns true if the error is a transient gRPC error that may
// succeed on retry (e.g., network issues, server overload).
func IsTransientError(err error) bool {
	s, ok := status.FromError(err)
	if !ok {
		// Fall back to unwrapping wrapped gRPC errors
		var provider grpcStatusProvider
		if errors.As(err, &provider) {
			s = provider.GRPCStatus()
		} else {
			return false
		}
	}
	//nolint:exhaustive // Only specific transient codes are relevant
	switch s.Code() {
	case codes.Unavailable, codes.ResourceExhausted, codes.Aborted, codes.DeadlineExceeded:
		return true
	default:
		return false
	}
}

// GRPCErrorCode extracts the gRPC status code from an error, if present.
// Returns codes.Unknown if the error is not a gRPC status error.
func GRPCErrorCode(err error) codes.Code {
	if s, ok := status.FromError(err); ok {
		return s.Code()
	}
	// Fall back to unwrapping wrapped gRPC errors
	var provider grpcStatusProvider
	if errors.As(err, &provider) {
		return provider.GRPCStatus().Code()
	}
	return codes.Unknown
}

// PieceStream manages a bidirectional streaming connection for piece transfer.
// This is a thin wrapper around the gRPC stream - in-flight tracking is handled
// by AdaptiveWindow in BidiQueue for congestion control.
type PieceStream struct {
	ctx    context.Context
	stream pb.QBSyncService_StreamPiecesBidiClient
	logger *slog.Logger

	acks     chan *pb.PieceAck // Incoming acknowledgments
	ackReady chan struct{}     // Signals when acks have been processed (for backpressure)
	done     chan struct{}     // Closed when receive goroutine exits
	errors   chan error        // Stream errors

	sendMu sync.Mutex // Protects concurrent Send calls (gRPC streams are not send-safe)
}

// pieceKey creates a unique key for tracking in-flight pieces.
func pieceKey(hash string, index int32) string {
	return fmt.Sprintf("%s:%d", hash, index)
}

// receiveAcks reads acknowledgments from the stream.
// Exits when context is cancelled or stream ends.
func (ps *PieceStream) receiveAcks() { //nolint:gocognit // complexity from panic recovery
	defer close(ps.done)
	defer func() {
		if r := recover(); r != nil {
			ps.logger.Error("panic in receiveAcks",
				"panic", r,
				"stack", string(debug.Stack()),
			)
			select {
			case ps.errors <- fmt.Errorf("panic in receiveAcks: %v", r):
			default:
			}
		}
	}()

	for {
		// Check for context cancellation before blocking on Recv
		select {
		case <-ps.ctx.Done():
			return
		default:
		}

		ack, err := ps.stream.Recv()
		if errors.Is(err, io.EOF) {
			return
		}
		if err != nil {
			// Check if this is due to context cancellation
			if ps.ctx.Err() != nil {
				return
			}
			select {
			case ps.errors <- err:
			default:
			}
			return
		}

		// Signal that an ack was processed (for backpressure relief)
		select {
		case ps.ackReady <- struct{}{}:
		default:
		}

		// Send ack to consumer - block if channel full to prevent dropped acks
		select {
		case ps.acks <- ack:
		case <-ps.ctx.Done():
			return
		}
	}
}

// Send sends a piece over the stream.
// This method is safe for concurrent use.
func (ps *PieceStream) Send(req *pb.WritePieceRequest) error {
	ps.sendMu.Lock()
	defer ps.sendMu.Unlock()
	return ps.stream.Send(req)
}

// Acks returns the channel of incoming acknowledgments.
func (ps *PieceStream) Acks() <-chan *pb.PieceAck {
	return ps.acks
}

// AckReady returns a channel that signals when acks have been processed.
// Use this for efficient backpressure instead of polling.
func (ps *PieceStream) AckReady() <-chan struct{} {
	return ps.ackReady
}

// Errors returns the channel for stream errors.
func (ps *PieceStream) Errors() <-chan error {
	return ps.errors
}

// Done returns a channel that's closed when the stream ends.
func (ps *PieceStream) Done() <-chan struct{} {
	return ps.done
}

// CloseSend signals that no more pieces will be sent.
// The stream remains open for receiving acks.
func (ps *PieceStream) CloseSend() error {
	return ps.stream.CloseSend()
}

// Close closes the stream and waits for the receiver goroutine to exit.
// This should be called when the stream is no longer needed to ensure clean shutdown.
func (ps *PieceStream) Close() {
	// CloseSend signals the server we're done sending
	_ = ps.stream.CloseSend()

	// Wait for receiver goroutine to exit (it will exit when stream ends or errors)
	<-ps.done
}

// ParsePieceKey extracts the torrent hash and piece index from a piece key.
func ParsePieceKey(key string) (string, int32, bool) {
	// Format is "hash:index"
	lastColon := strings.LastIndexByte(key, ':')
	if lastColon == -1 {
		return "", 0, false
	}

	idx, err := strconv.ParseInt(key[lastColon+1:], 10, 32)
	if err != nil {
		return "", 0, false
	}

	return key[:lastColon], int32(idx), true
}
