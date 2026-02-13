package streaming

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"

	pb "github.com/arsac/qb-sync/proto"
)

const (
	// gRPC keepalive parameters.
	keepaliveTime    = 30 * time.Second // Send pings every 30 seconds if no activity
	keepaliveTimeout = 10 * time.Second // Wait 10 seconds for ping ack

	// maxGRPCMessageSize is the maximum gRPC message size for piece transfers.
	// Torrent pieces are commonly 1–16 MB; the default gRPC limit of 4 MB is too small.
	maxGRPCMessageSize = 32 * 1024 * 1024 // 32 MB

	// HTTP/2 flow control window sizes. The default 64 KB initial window requires
	// 262 RTTs to transfer a 16 MB piece, capping first-piece throughput at ~61 MB/s
	// on a 1 ms LAN. Larger windows allow the sender to push data without waiting
	// for WINDOW_UPDATE frames, closing the gap with rsync on fast links.
	initialStreamWindowSize = 16 * 1024 * 1024 // 16 MB per-stream flow control window
	initialConnWindowSize   = 64 * 1024 * 1024 // 64 MB connection-level flow control window

	// finalizeConnTimeout is how long FinalizeTorrent waits for the gRPC
	// connection to become READY before giving up. This prevents fail-fast
	// behavior on unary RPCs when the destination server was recently restarted.
	finalizeConnTimeout = 20 * time.Second

	// maxReconnectBackoff caps gRPC's exponential reconnection backoff.
	// The default (120s) causes long gaps between reconnect attempts, which
	// can exceed finalizeConnTimeout and prevent recovery after a server restart.
	maxReconnectBackoff = 5 * time.Second

	// ackWriteTimeout is how long receiveAcks waits to write an ack to the channel
	// before treating the stream as stuck. If forwardAcks is slow draining the channel,
	// this prevents receiveAcks from blocking forever and never calling Recv() again
	// (which is the only way to detect stream death and trigger ps.cancel()).
	ackWriteTimeout = 30 * time.Second

	// sendTimeout is how long Send waits for the gRPC stream.Send to complete.
	// gRPC Send() blocks on HTTP/2 flow control when the receiver stops consuming.
	// Since Send doesn't accept a context, the caller-side timer cancels the stream
	// context if the sendLoop's stream.Send doesn't return in time.
	sendTimeout = 30 * time.Second

	// gRPC connect backoff parameters.
	backoffMultiplier = 1.6 // Exponential backoff multiplier between reconnect attempts
	backoffJitter     = 0.2 // Randomization factor to prevent thundering herd
)

var (
	// ErrFinalizeVerifying is returned by FinalizeTorrent when the destination server is
	// still verifying pieces in the background. The caller should retry later
	// without counting this as a failure.
	ErrFinalizeVerifying = errors.New("finalization in progress: destination server is verifying pieces")

	// ErrFinalizeIncomplete is returned by FinalizeTorrent when destination reports that
	// not all pieces are written. This typically happens after a destination restart where
	// the persisted state is stale. The caller should re-sync with destination to discover
	// which pieces are actually missing and re-stream them.
	ErrFinalizeIncomplete = errors.New("finalization failed: incomplete pieces on destination")
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

var _ HardlinkDestination = (*GRPCDestination)(nil)

// GRPCDestination sends pieces to a remote gRPC server.
// It provides PieceDestination-like functionality plus additional features
// for hardlink deduplication, streaming, and torrent lifecycle management.
//
// Multiple TCP connections can be opened to the same server to avoid the
// single-TCP-flow bandwidth ceiling imposed by HTTP/2 multiplexing.
// Streaming RPCs are distributed across connections via round-robin;
// unary RPCs always use the first connection.
type GRPCDestination struct {
	addr     string            // Server address for dynamic connection creation
	opts     []grpc.DialOption // Stored dial options for AddConnection
	minConns int               // Minimum connection count
	maxConns int               // Maximum connection count

	conns       []*grpc.ClientConn
	clients     []pb.QBSyncServiceClient
	streamIdx   atomic.Uint32                 // Lock-free round-robin for OpenStream
	initResults map[string]*InitTorrentResult // Cached init results
	initGroup   singleflight.Group            // Deduplicates concurrent InitTorrent calls
	mu          sync.RWMutex
	closeOnce   sync.Once
	closeErr    error
}

// NewGRPCDestination creates a new gRPC destination client with minConns
// initial TCP connections, scalable up to maxConns. Each connection gets its
// own kernel CUBIC window, avoiding the single-TCP-flow bandwidth ceiling.
// Streaming RPCs are distributed across connections via round-robin.
func NewGRPCDestination(addr string, minConns, maxConns int) (*GRPCDestination, error) {
	if minConns <= 0 {
		minConns = 1
	}
	if maxConns < minConns {
		maxConns = minConns
	}

	kaParams := keepalive.ClientParameters{
		Time:                keepaliveTime,
		Timeout:             keepaliveTimeout,
		PermitWithoutStream: true,
	}

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(kaParams),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  1 * time.Second,
				Multiplier: backoffMultiplier,
				Jitter:     backoffJitter,
				MaxDelay:   maxReconnectBackoff,
			},
		}),
		grpc.WithInitialWindowSize(initialStreamWindowSize),
		grpc.WithInitialConnWindowSize(initialConnWindowSize),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxGRPCMessageSize),
			grpc.MaxCallSendMsgSize(maxGRPCMessageSize),
		),
	}

	d := &GRPCDestination{
		addr:        addr,
		opts:        opts,
		minConns:    minConns,
		maxConns:    maxConns,
		conns:       make([]*grpc.ClientConn, 0, maxConns),
		clients:     make([]pb.QBSyncServiceClient, 0, maxConns),
		initResults: make(map[string]*InitTorrentResult),
	}

	for i := range minConns {
		conn, err := grpc.NewClient(addr, opts...)
		if err != nil {
			// Clean up already-opened connections
			for _, c := range d.conns {
				_ = c.Close()
			}
			return nil, fmt.Errorf("failed to connect (conn %d): %w", i, err)
		}
		d.conns = append(d.conns, conn)
		d.clients = append(d.clients, pb.NewQBSyncServiceClient(conn))
	}

	return d, nil
}

// client returns the primary client for unary RPCs (always the first connection).
func (d *GRPCDestination) client() pb.QBSyncServiceClient {
	return d.clients[0]
}

// streamConnIdx returns the next connection index for streaming RPCs (round-robin across all connections).
func (d *GRPCDestination) streamConnIdx() int {
	d.mu.RLock()
	n := len(d.clients)
	d.mu.RUnlock()

	if n == 1 {
		return 0
	}
	idx := d.streamIdx.Add(1) - 1
	return int(idx % uint32(n))
}

// ValidateConnection checks that the destination server is reachable on all
// connections using the standard gRPC health check protocol (grpc.health.v1.Health).
func (d *GRPCDestination) ValidateConnection(ctx context.Context) error {
	d.mu.RLock()
	conns := make([]*grpc.ClientConn, len(d.conns))
	copy(conns, d.conns)
	d.mu.RUnlock()

	for i, conn := range conns {
		healthClient := healthpb.NewHealthClient(conn)
		_, err := healthClient.Check(ctx, &healthpb.HealthCheckRequest{})
		if err != nil {
			return fmt.Errorf("destination server not reachable (conn %d): %w", i, err)
		}
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

		resp, rpcErr := d.client().InitTorrent(ctx, req)
		if rpcErr != nil {
			return nil, fmt.Errorf("init torrent RPC failed: %w", rpcErr)
		}

		if err := checkRPCResponse(resp, "init torrent"); err != nil {
			return nil, err
		}

		result := initTorrentResultFromProto(resp)

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

// CheckTorrentStatus queries the destination server for a torrent's current sync status.
// Unlike InitTorrent, this does NOT cache results and does NOT require full torrent info.
// Use this for status checking (is torrent complete/verifying/ready on destination?).
// For full initialization with file tracking and hardlink detection, use InitTorrent.
//
// Note: This sends a minimal request with just the hash. Destination server will check:
// 1. qBittorrent status (returns COMPLETE/VERIFYING if found).
// 2. Existing tracking state (returns READY with pieces_needed if found).
// 3. If neither, returns READY with empty state (caller should do full InitTorrent).
func (d *GRPCDestination) CheckTorrentStatus(ctx context.Context, hash string) (*InitTorrentResult, error) {
	req := &pb.InitTorrentRequest{
		TorrentHash: hash,
	}

	resp, err := d.client().InitTorrent(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("check status RPC failed: %w", err)
	}

	if respErr := checkRPCResponse(resp, "check status"); respErr != nil {
		return nil, respErr
	}

	// Return result WITHOUT caching - BidiQueue should still do full InitTorrent
	return initTorrentResultFromProto(resp), nil
}

// OpenStream opens a bidirectional stream for high-throughput piece transfer.
// Each stream gets its own cancellable context so a stuck Send() can be
// unblocked without tearing down the entire pool.
func (d *GRPCDestination) OpenStream(ctx context.Context, logger *slog.Logger) (*PieceStream, error) {
	connIdx := d.streamConnIdx()

	d.mu.RLock()
	client := d.clients[connIdx]
	d.mu.RUnlock()
	streamCtx, streamCancel := context.WithCancel(ctx)

	stream, err := client.StreamPiecesBidi(streamCtx)
	if err != nil {
		streamCancel()
		return nil, fmt.Errorf("failed to open stream: %w", err)
	}

	ps := &PieceStream{
		connIdx:  connIdx,
		ctx:      streamCtx,
		cancel:   streamCancel,
		stream:   stream,
		logger:   logger,
		acks:     make(chan *pb.PieceAck, DefaultAckChannelSize),
		ackReady: make(chan struct{}, 1), // Signal when acks are processed
		done:     make(chan struct{}),
		errors:   make(chan error, 1),
		sendCh:   make(chan *sendRequest), // unbuffered: natural backpressure
		stopSend: make(chan struct{}),
		sendDone: make(chan struct{}),
	}

	go ps.receiveAcks()
	go ps.sendLoop()

	return ps, nil
}

// GetFileByInode checks if a file with the given inode exists on the receiver.
func (d *GRPCDestination) GetFileByInode(ctx context.Context, inode uint64) (string, bool, error) {
	resp, err := d.client().GetFileByInode(ctx, &pb.GetFileByInodeRequest{
		Inode: inode,
	})
	if err != nil {
		return "", false, fmt.Errorf("get file by inode RPC failed: %w", err)
	}

	return resp.GetPath(), resp.GetFound(), nil
}

// CreateHardlink creates a hardlink on the receiver from source to target path.
func (d *GRPCDestination) CreateHardlink(ctx context.Context, sourcePath, targetPath string) error {
	resp, err := d.client().CreateHardlink(ctx, &pb.CreateHardlinkRequest{
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
	resp, err := d.client().RegisterFile(ctx, &pb.RegisterFileRequest{
		Inode: inode,
		Path:  path,
		Size:  size,
	})
	if err != nil {
		return fmt.Errorf("register file RPC failed: %w", err)
	}
	return checkRPCResponse(resp, "register file")
}

// FinalizeTorrent requests the destination server to finalize a torrent:
// rename .partial files, add to qBittorrent, verify, and confirm.
// On success, clears the cached init result to prevent memory leaks.
//
// Returns ErrFinalizeVerifying if the destination server is still verifying pieces
// in the background. The caller should retry later without penalty.
func (d *GRPCDestination) FinalizeTorrent(
	ctx context.Context,
	hash, savePath, category, tags, saveSubPath string,
) error {
	// Use WaitForReady so the RPC waits for the connection to recover
	// after a destination server restart instead of failing fast with "connection refused".
	// The per-call timeout prevents blocking the orchestrator loop indefinitely.
	callCtx, cancel := context.WithTimeout(ctx, finalizeConnTimeout)
	defer cancel()

	resp, err := d.client().FinalizeTorrent(callCtx, &pb.FinalizeTorrentRequest{
		TorrentHash: hash,
		SavePath:    savePath,
		Category:    category,
		Tags:        tags,
		SaveSubPath: saveSubPath,
	}, grpc.WaitForReady(true))
	if err != nil {
		return fmt.Errorf("finalize torrent RPC failed: %w", err)
	}
	if respErr := checkRPCResponse(resp, "finalize"); respErr != nil {
		// Detect incomplete-pieces error from destination — this means destination's written
		// state diverged from source's streamed state (typically after destination restart
		// where flushed state was stale). Return a sentinel so the orchestrator
		// can re-sync instead of endlessly retrying.
		if resp.GetErrorCode() == pb.FinalizeErrorCode_FINALIZE_ERROR_INCOMPLETE {
			return fmt.Errorf("%w: %s", ErrFinalizeIncomplete, resp.GetError())
		}
		return respErr
	}

	// Destination returns state="verifying" when background verification is still running.
	// Return a sentinel error so the orchestrator can retry without penalty.
	if resp.GetState() == "verifying" {
		return ErrFinalizeVerifying
	}

	d.ClearInitResult(hash)
	return nil
}

// AbortTorrent requests the destination server to abort an in-progress torrent
// and optionally delete partial files. Called when a torrent is removed
// from source before streaming completes.
func (d *GRPCDestination) AbortTorrent(ctx context.Context, hash string, deleteFiles bool) (int32, error) {
	// Always clear init results - the server removes tracking regardless of deletion success,
	// and if RPC fails the torrent needs re-initialization anyway
	defer d.ClearInitResult(hash)

	resp, err := d.client().AbortTorrent(ctx, &pb.AbortTorrentRequest{
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

// StartTorrent resumes a stopped torrent on the destination server.
// Called during disk pressure cleanup after source stops seeding,
// to ensure destination takes over before source deletes.
func (d *GRPCDestination) StartTorrent(ctx context.Context, hash, tag string) error {
	resp, err := d.client().StartTorrent(ctx, &pb.StartTorrentRequest{
		TorrentHash: hash,
		Tag:         tag,
	})
	if err != nil {
		return fmt.Errorf("start torrent RPC failed: %w", err)
	}
	return checkRPCResponse(resp, "start torrent")
}

// ClearInitResult removes a cached init result for a torrent hash.
// Use this when a piece ack indicates the torrent is not initialized on destination,
// so the next send triggers re-initialization.
func (d *GRPCDestination) ClearInitResult(hash string) {
	d.mu.Lock()
	delete(d.initResults, hash)
	d.mu.Unlock()
}

// ClearInitCache removes all cached init results.
// Call this when the gRPC connection resets (e.g. destination server restart) so
// torrents get re-initialized on the new server instance.
func (d *GRPCDestination) ClearInitCache() {
	d.mu.Lock()
	clear(d.initResults)
	d.mu.Unlock()
}

// AddConnection creates a new TCP connection and appends it to the pool.
// Returns error if already at max connections.
func (d *GRPCDestination) AddConnection() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if len(d.conns) >= d.maxConns {
		return errors.New("at maximum connection count")
	}

	conn, err := grpc.NewClient(d.addr, d.opts...)
	if err != nil {
		return fmt.Errorf("failed to create connection: %w", err)
	}

	d.conns = append(d.conns, conn)
	d.clients = append(d.clients, pb.NewQBSyncServiceClient(conn))
	return nil
}

// RemoveConnection removes the connection at expectedIdx. The caller must
// drain all streams on that connection first. expectedIdx must equal the
// current last index (len-1); if a connection was added concurrently the
// index won't match and the call returns an error, preventing removal of
// the wrong connection.
// Returns error if at minimum connection count or index mismatch.
func (d *GRPCDestination) RemoveConnection(expectedIdx int) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if len(d.conns) <= d.minConns {
		return errors.New("at minimum connection count")
	}

	lastIdx := len(d.conns) - 1
	if lastIdx != expectedIdx {
		return fmt.Errorf("connection index mismatch: expected %d, current last %d", expectedIdx, lastIdx)
	}

	conn := d.conns[lastIdx]
	d.conns = d.conns[:lastIdx]
	d.clients = d.clients[:lastIdx]

	return conn.Close()
}

// ConnectionCount returns the current number of TCP connections.
func (d *GRPCDestination) ConnectionCount() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return len(d.conns)
}

// MinConnections returns the minimum connection count.
func (d *GRPCDestination) MinConnections() int {
	return d.minConns
}

// MaxConnections returns the maximum connection count.
func (d *GRPCDestination) MaxConnections() int {
	return d.maxConns
}

// Close closes all gRPC connections. Safe for repeated calls.
func (d *GRPCDestination) Close() error {
	d.closeOnce.Do(func() {
		d.mu.Lock()
		conns := make([]*grpc.ClientConn, len(d.conns))
		copy(conns, d.conns)
		d.mu.Unlock()

		var errs []error
		for _, conn := range conns {
			if err := conn.Close(); err != nil {
				errs = append(errs, err)
			}
		}
		d.closeErr = errors.Join(errs...)
	})
	return d.closeErr
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
