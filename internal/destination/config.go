package destination

import (
	"errors"
	"fmt"
	"time"
)

const (
	serverDirPermissions  = 0o755
	serverFilePermissions = 0o644

	// Default state persistence settings (hybrid time + count approach).
	defaultStateFlushInterval = 30 * time.Second // Time-based: flush dirty state every 30s
	defaultStateFlushCount    = 100              // Count-based: flush after N pieces as safety net

	// Metadata directory and file names for recovery after restart.
	metaDirName      = ".qbsync"
	subPathFileName  = ".subpath"
	selectedFileName = ".selected"
	versionFileName  = ".version"

	// metaVersion is bumped when the metadata format changes.
	// If a metadata directory has a missing or different version,
	// it is nuked and re-initialized from scratch on next InitTorrent.
	metaVersion = "1"

	// Concurrent streaming settings.
	defaultStreamWorkers = 8   // Number of concurrent piece writers (tuned for NFS/ZFS)
	ackQueueSize         = 100 // Buffer size for outbound acks (small messages, larger buffer is fine)

	// Default polling settings for waitForTorrentReady.
	defaultQBPollInterval = 2 * time.Second
	defaultQBPollTimeout  = 5 * time.Minute

	// stopTorrentTimeout is how long to wait when stopping a torrent after adding to qBittorrent.
	// Uses a detached context because the gRPC caller may cancel before the stop completes.
	stopTorrentTimeout = 10 * time.Second

	// Default orphan cleanup settings.
	defaultOrphanCleanupInterval = 1 * time.Hour  // How often to scan for orphans
	defaultOrphanTimeout         = 24 * time.Hour // Consider torrent orphaned after this inactive period
	minOrphanTimeout             = 1 * time.Hour  // Minimum orphan timeout to prevent accidental cleanup

	// Default inode cleanup settings.
	defaultInodeCleanupInterval = 6 * time.Hour // How often to check for stale inode entries

	// Default hardlink wait timeout.
	defaultHardlinkWaitTimeout = 30 * time.Minute // Max time to wait for pending hardlink source

	// Partial file suffix.
	partialSuffix = ".partial"

	// Memory management.
	bytesPerMB               = 1024 * 1024
	defaultMaxStreamBufferMB = 512 // Default global memory budget for buffered piece data
	maxVerifyConcurrency     = 4   // Limit concurrent piece reads during finalization to cap transient memory

	// verifyIdleTimeout is how long verification can go without verifying a piece
	// before it is considered stalled. Resets on each successfully verified piece.
	verifyIdleTimeout = 60 * time.Second

	// verifyIdleCheckDivisor divides verifyIdleTimeout to determine the watchdog tick interval.
	// A value of 2 means the watchdog checks at half the timeout interval.
	verifyIdleCheckDivisor = 2

	// finalizeQueueTimeout is how long a finalization can wait for its turn
	// in the semaphore queue. Generous because many torrents may finish at once,
	// and each finalization can take up to backgroundFinalizeTimeout to complete.
	finalizeQueueTimeout = 2 * time.Hour

	// backgroundFinalizeTimeout is the upper-bound timeout for the actual
	// finalization work (verification + inode registration + qBittorrent).
	// Starts after the semaphore is acquired, so queue wait doesn't count.
	backgroundFinalizeTimeout = 30 * time.Minute
)

// QBConfig holds qBittorrent configuration for the destination server.
type QBConfig struct {
	URL          string        // qBittorrent WebUI URL
	Username     string        // qBittorrent username
	Password     string        // qBittorrent password
	PollInterval time.Duration // Poll interval for torrent verification (default: 2s)
	PollTimeout  time.Duration // Poll timeout for torrent verification (default: 5m)
}

// ServerConfig configures the gRPC piece receiver server.
type ServerConfig struct {
	ListenAddr         string        // Address to listen on (e.g., ":50051")
	BasePath           string        // Base path for writing torrent data
	SavePath           string        // Path as destination qBittorrent sees it (container mount, e.g., "/downloads"). Defaults to BasePath.
	StateFlushInterval time.Duration // How often to flush dirty state (0 = use default)
	StateFlushCount    int           // Flush after this many pieces written (0 = use default)
	StreamWorkers      int           // Number of concurrent piece writers (0 = use default)

	// Orphan cleanup settings - clean up partial files when source disconnects unexpectedly.
	OrphanCleanupInterval time.Duration // How often to scan for orphans (0 = use default 1h)
	OrphanTimeout         time.Duration // Consider torrent orphaned after this inactive period (0 = use default 24h)

	// Inode cleanup settings - remove stale entries from inode-to-path map when files are deleted.
	InodeCleanupInterval time.Duration // How often to check for stale inodes (0 = use default 6h)

	// QB holds qBittorrent config for auto-adding verified torrents.
	// If nil, FinalizeTorrent only verifies pieces (no qB integration).
	QB *QBConfig

	// SyncedTag is applied to torrents after successful finalization (for visibility).
	// Empty string disables tagging.
	SyncedTag string

	// MaxStreamBufferBytes is the global memory budget for buffered piece data
	// across all streams (0 = use default).
	MaxStreamBufferBytes int64

	// DryRun prevents modifications (no writes, no qB changes).
	DryRun bool
}

// GetSavePath returns the save path for destination qBittorrent.
// Falls back to BasePath when SavePath is not explicitly set.
func (c *ServerConfig) GetSavePath() string {
	if c.SavePath != "" {
		return c.SavePath
	}
	return c.BasePath
}

// Validate validates the server configuration.
func (c *ServerConfig) Validate() error {
	if c.BasePath == "" {
		return errors.New("base path is required")
	}
	if c.ListenAddr == "" {
		return errors.New("listen address is required")
	}
	// Validate orphan timeout is not too aggressive
	if c.OrphanTimeout > 0 && c.OrphanTimeout < minOrphanTimeout {
		return fmt.Errorf("orphan timeout must be at least %v to prevent accidental cleanup", minOrphanTimeout)
	}
	return nil
}
