// Package cold implements the cold (destination) server for the qb-sync system.
// It receives torrent pieces over gRPC, writes them to disk, and manages the
// full torrent lifecycle including initialization, piece verification,
// hardlink deduplication, finalization, and qBittorrent integration.
package cold

import (
	"context"
	"log/slog"
	"net"
	"sync"

	"github.com/autobrr/go-qbittorrent"

	"github.com/arsac/qb-sync/internal/health"
	pb "github.com/arsac/qb-sync/proto"

	"google.golang.org/grpc"
)

// Server receives pieces over gRPC and writes them to disk.
//
// Lock ordering (to prevent deadlocks):
//  1. s.mu - server-level lock for torrents map and abortingHashes
//  2. state.mu - per-torrent lock for torrent state
//  3. s.inodeMu - lock for inode-to-path mapping
//  4. s.inodeProgressMu - lock for in-progress inode tracking
//
// Always acquire locks in the order above. Release s.mu before acquiring
// state.mu when possible to reduce contention. The inode locks (3, 4) may
// be acquired independently when s.mu and state.mu are not held.
type Server struct {
	pb.UnimplementedQBSyncServiceServer

	config   ServerConfig
	logger   *slog.Logger
	server   *grpc.Server
	torrents map[string]*serverTorrentState
	mu       sync.RWMutex // Protects torrents and abortingHashes

	// Abort tracking: prevents race between AbortTorrent and InitTorrent.
	// When a hash is being aborted, the channel is present and open.
	// InitTorrent waits for the channel to close before proceeding.
	// AbortTorrent closes the channel when cleanup is complete.
	abortingHashes map[string]chan struct{}

	// Hardlink tracking: source inode -> path on receiver
	inodeToPath map[uint64]string
	inodeMu     sync.RWMutex // Protects inodeToPath

	// In-progress inode tracking for parallel torrent coordination.
	// When a torrent starts writing a file with a given source inode,
	// it registers here. Other torrents with the same inode can wait
	// for the file to be finalized and then create a hardlink.
	inodeInProgress map[uint64]*inProgressInode
	inodeProgressMu sync.RWMutex // Protects inodeInProgress

	// qBittorrent client for adding verified torrents (cold server only)
	qbClient *qbittorrent.Client

	// Health server for K8s probes
	healthServer *health.Server
}

// NewServer creates a new gRPC piece receiver server.
func NewServer(config ServerConfig, logger *slog.Logger) *Server {
	s := &Server{
		config:          config,
		logger:          logger,
		torrents:        make(map[string]*serverTorrentState),
		abortingHashes:  make(map[string]chan struct{}),
		inodeToPath:     make(map[uint64]string),
		inodeInProgress: make(map[uint64]*inProgressInode),
	}

	// Initialize qBittorrent client if configured
	if config.ColdQB != nil && config.ColdQB.URL != "" {
		s.qbClient = qbittorrent.NewClient(qbittorrent.Config{
			Host:     config.ColdQB.URL,
			Username: config.ColdQB.Username,
			Password: config.ColdQB.Password,
		})
	}

	// Load persisted inode tracking
	if loadErr := s.loadInodeMap(); loadErr != nil {
		logger.Warn("failed to load inode map, starting fresh", "error", loadErr)
	}

	return s
}

// collectTorrents returns a snapshot of all torrents for safe iteration.
// The caller should acquire s.mu.RLock() or s.mu.Lock() before calling.
// Returns a slice that can be iterated after releasing s.mu.
func (s *Server) collectTorrents() []torrentRef {
	refs := make([]torrentRef, 0, len(s.torrents))
	for hash, state := range s.torrents {
		refs = append(refs, torrentRef{hash: hash, state: state})
	}
	return refs
}

// SetHealthServer sets the health server for registering health checks.
func (s *Server) SetHealthServer(hs *health.Server) {
	s.healthServer = hs
}

// Run starts the gRPC server and blocks until context is cancelled.
func (s *Server) Run(ctx context.Context) error {
	lc := net.ListenConfig{}
	listener, err := lc.Listen(ctx, "tcp", s.config.ListenAddr)
	if err != nil {
		return err
	}

	s.server = grpc.NewServer()
	pb.RegisterQBSyncServiceServer(s.server, s)

	// Start background state flusher
	go s.runStateFlusher(ctx)

	// Start background orphan cleanup
	go s.runOrphanCleaner(ctx)

	// Start background inode cleanup
	go s.runInodeCleaner(ctx)

	// Register health checks if health server is configured
	if s.healthServer != nil {
		if s.qbClient != nil {
			s.healthServer.RegisterCheck("qbittorrent", health.QBHealthCheck(s.qbClient.LoginCtx))
		}
		s.healthServer.SetReady(true)
	}

	s.logger.InfoContext(ctx, "starting gRPC server", "addr", s.config.ListenAddr)

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.server.Serve(listener)
	}()

	select {
	case <-ctx.Done():
		s.logger.InfoContext(ctx, "shutting down gRPC server")
		s.server.GracefulStop()
		s.cleanup()
		return ctx.Err()
	case serveErr := <-errCh:
		return serveErr
	}
}

// cleanup closes all file handles and saves state before shutdown.
func (s *Server) cleanup() {
	// Collect all torrent references and clear the map atomically
	s.mu.Lock()
	torrents := s.collectTorrents()
	s.torrents = make(map[string]*serverTorrentState)
	s.mu.Unlock()

	// Process each torrent individually with only state.mu held
	for _, t := range torrents {
		t.state.mu.Lock()
		// Save dirty state before closing
		if t.state.dirty && t.state.statePath != "" {
			if saveErr := s.saveState(t.state.statePath, t.state.written); saveErr != nil {
				s.logger.Warn("failed to save state on cleanup",
					"hash", t.hash,
					"error", saveErr,
				)
			}
		}
		// Close all open file handles
		for _, fi := range t.state.files {
			if fi.file != nil {
				if closeErr := fi.file.Close(); closeErr != nil {
					s.logger.Warn("failed to close file on cleanup",
						"hash", t.hash,
						"path", fi.path,
						"error", closeErr,
					)
				}
			}
		}
		t.state.mu.Unlock()
	}

	// Persist inode tracking before shutdown
	if saveErr := s.saveInodeMap(); saveErr != nil {
		s.logger.Warn("failed to save inode map on cleanup", "error", saveErr)
	}
}
