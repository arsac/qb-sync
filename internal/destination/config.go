package destination

import (
	"errors"
	"fmt"
	"time"

	"github.com/arsac/qb-sync/internal/arr"
)

const (
	serverDirPermissions  = 0o755
	serverFilePermissions = 0o644

	// Default state persistence interval for the background flusher.
	defaultStateFlushInterval = 30 * time.Second

	// Metadata directory and file names for recovery after restart.
	metaDirName       = ".qbsync"
	finalizedFileName = ".finalized"
	stateFileName     = ".state"

	// Concurrent streaming settings.
	defaultStreamWorkers = 8   // Number of concurrent piece writers (tuned for NFS/ZFS)
	workChBufferMultiple = 2   // Work channel is N*workers; memory bounded by memBudget semaphore
	ackQueueSize         = 100 // Buffer size for outbound acks (small messages, larger buffer is fine)

	// Default polling settings for waitForTorrentReady. The timeout is sized
	// from the torrent's totalSize since qBittorrent's recheck of large
	// torrents can take well over the historical 5-minute fixed budget on
	// spinning rust or NFS — see computePollTimeout.
	defaultQBPollInterval     = 2 * time.Second
	defaultQBPollTimeoutBase  = 10 * time.Minute // floor for any torrent, regardless of size
	defaultQBPollTimeoutPerGB = 1 * time.Minute  // added per GB of torrent data
	defaultQBPollTimeoutMax   = 6 * time.Hour    // hard cap to prevent unbounded waits

	// stopTorrentTimeout is how long to wait when stopping a torrent after adding to qBittorrent.
	// Uses a detached context because the gRPC caller may cancel before the stop completes.
	stopTorrentTimeout = 10 * time.Second

	// Settings for verifying deselected file priorities actually persisted on
	// destination qB after AddTorrent. qB silently drops priority changes on
	// freshly-added stopped torrents occasionally — verification + retry catches
	// that and keeps trying until the torrent has settled enough for the change
	// to stick.
	priorityVerifyInterval          = 500 * time.Millisecond
	priorityVerifyMaxInterval       = 5 * time.Second
	priorityVerifyTimeout           = 30 * time.Second
	priorityVerifyBackoffMultiplier = 2 // exponential backoff base

	// Default orphan cleanup settings.
	defaultOrphanCleanupInterval = 1 * time.Hour  // How often to scan for orphans
	defaultOrphanTimeout         = 24 * time.Hour // Consider torrent orphaned after this inactive period
	minOrphanTimeout             = 1 * time.Hour  // Minimum orphan timeout to prevent accidental cleanup

	// Default inode cleanup settings.
	defaultInodeCleanupInterval = 6 * time.Hour // How often to check for stale inode entries
	inodeRebuildWorkers         = 32            // Concurrent workers for startup inode rebuild from .meta (sized to hide NFS RTT)

	// Concurrent workers for the per-file metadata probing at torrent init
	// (setupFiles, clearStalePieces). Each file costs several NFS round-trips
	// before a single piece can stream, and they are pure latency, so size this
	// to hide RTT the same way inodeRebuildWorkers does.
	fileSetupConcurrency = 32

	// Default hardlink wait timeout.
	defaultHardlinkWaitTimeout = 30 * time.Minute // Max time to wait for pending hardlink source

	// Partial file suffix.
	partialSuffix = ".partial"

	// metaFileName is the protobuf-encoded PersistedTorrentMeta file name.
	metaFileName = ".meta"

	// Memory management.
	defaultMaxStreamBufferMB = 512 // Default global memory budget for buffered piece data
	maxVerifyConcurrency     = 4   // Default concurrent piece reads per verify pass (caps transient memory)
	// maxVerifyConcurrencyCap bounds the --verify-concurrency knob. Each worker
	// holds up to one max-size (32 MB) piece buffer: 16 workers = 512 MB
	// transient worst case, matching the default stream memory budget. Keep in
	// sync with the bound in internal/config DestinationConfig.Validate;
	// verifyConcurrency() clamps defensively in case the two ever drift.
	maxVerifyConcurrencyCap  = 16
	parentDirSyncConcurrency = 8 // Concurrent fsyncs of unique parent dirs during finalize (each is a separate NFS commit RTT)
	// verifyChunkPieces caps the run of consecutive pieces a verify worker
	// claims per turn - see verifyChunkSize for why runs beat one-at-a-time.
	verifyChunkPieces = 32
	// defaultPreVerifyConcurrency bounds init-time pre-verification passes across
	// all torrents. Each pass reads and rehashes whole files, so without a cap a
	// burst of InitTorrent calls fans out to one full-file read per torrent and
	// saturates the NFS mount everything else on the server shares. Multiplies
	// with VerifyConcurrency for the total in-flight reads; tune that side if the
	// product needs adjusting.
	defaultPreVerifyConcurrency = 4

	// verifyIdleTimeout is how long verification can go without verifying a piece
	// before it is considered stalled. Resets on each successfully verified piece.
	verifyIdleTimeout = 60 * time.Second

	// verifyIdleCheckDivisor divides verifyIdleTimeout to determine the watchdog tick interval.
	// A value of 2 means the watchdog checks at half the timeout interval.
	verifyIdleCheckDivisor = 2

	// finalizeQueueTimeout is how long a finalization can wait for its turn
	// in a stage-semaphore queue. Generous because many torrents may finish at
	// once, and each stage occupant can hold its slot for a long time.
	finalizeQueueTimeout = 2 * time.Hour

	// diskStageTimeoutBase / PerGB / Max bound the disk-bound finalization
	// stage (parent-dir sync + piece verification + inode registration).
	// Starts after the disk-stage semaphore is acquired, so queue wait
	// doesn't count. Complementary to verifyIdleTimeout: the watchdog
	// catches a stalled verifier; this cap catches slow-but-continuous work.
	//
	// Verification reads back every piece, so wall-clock cost scales linearly
	// with data volume — same shape as qB's recheck timeout. A flat 30 min
	// caused 200 GB+ torrents on slow NFS to be quarantined as sync-failed
	// despite valid data; the GB-based scale matches actual workload.
	diskStageTimeoutBase  = 10 * time.Minute
	diskStageTimeoutPerGB = 30 * time.Second
	diskStageTimeoutMax   = 6 * time.Hour

	// defaultQBStageTimeoutMargin pads the qB-stage budget beyond the two
	// waitForTorrentReady poll budgets (initial + post-recheck). Covers
	// AddTorrent itself, login retries, and the partial-selection
	// priority-verify loop (priorityVerifyTimeout) — none of which are part
	// of the poll budget.
	defaultQBStageTimeoutMargin = 5 * time.Minute

	// maxQBFinalizeConcurrency caps the qB-stage semaphore weight. Above this,
	// concurrent qB rechecks compete for disk I/O and the API burst rate can
	// trip the qbclient circuit breaker (5 failures / 30s).
	// Keep in sync with the bound in internal/config DestinationConfig.Validate
	// (the startup-time check); GetQBFinalizeConcurrency clamps defensively in
	// case the two ever drift.
	maxQBFinalizeConcurrency = 8
)

// QBConfig holds qBittorrent configuration for the destination server.
type QBConfig struct {
	URL                    string        // qBittorrent WebUI URL
	Username               string        // qBittorrent username
	Password               string        // qBittorrent password
	PollInterval           time.Duration // Poll interval for torrent verification (default: 2s)
	PollTimeout            time.Duration // Poll timeout for torrent verification (default: 5m)
	PriorityVerifyInterval time.Duration // Initial backoff between filePrio retries on partial-selection adds (default: 500ms)
	PriorityVerifyTimeout  time.Duration // Total budget for filePrio verify-and-retry (default: 30s)
}

// ServerConfig configures the gRPC piece receiver server.
type ServerConfig struct {
	ListenAddr         string        // Address to listen on (e.g., ":50051")
	BasePath           string        // Base path for writing torrent data
	SavePath           string        // Path as destination qBittorrent sees it (container mount, e.g., "/downloads"). Defaults to BasePath.
	StateFlushInterval time.Duration // How often to flush dirty state (0 = use default)
	StreamWorkers      int           // Number of concurrent piece writers (0 = use default)
	VerifyConcurrency  int           // Concurrent piece-read goroutines per read-back verify pass - init pre-verify, early finalization, and full finalization alike (0 = use default 4). Raise on healthy storage to speed verification; lower if your NFS server can't handle the burst.

	// Arr configures the Sonarr/Radarr instances consulted by
	// CheckArrRejections. Zero instances disable the filter, and the server then
	// reports filter_enabled=false so the source stops asking.
	Arr arr.Config

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

	// QBFinalizeConcurrency is how many torrents may concurrently occupy the
	// qBittorrent integration stage (add + recheck wait). 0 = default 1.
	// Values >1 increase destination qB API and disk load; raising it is only
	// recommended on SSD-backed storage. Capped at maxQBFinalizeConcurrency.
	QBFinalizeConcurrency int

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

// GetQBFinalizeConcurrency returns the qB-stage semaphore weight, defaulting
// to 1 and clamping to maxQBFinalizeConcurrency. The clamp is defensive:
// ServerConfig.Validate is not on the startup path (internal/config validates
// there), so an out-of-range value must not reach the semaphore.
func (c *ServerConfig) GetQBFinalizeConcurrency() int {
	if c.QBFinalizeConcurrency <= 0 {
		return 1
	}
	return min(c.QBFinalizeConcurrency, maxQBFinalizeConcurrency)
}

// streamWorkers returns the configured number of concurrent piece writers,
// falling back to the default when unset.
func (c *ServerConfig) streamWorkers() int {
	if c.StreamWorkers <= 0 {
		return defaultStreamWorkers
	}
	return c.StreamWorkers
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
	if c.QBFinalizeConcurrency < 0 || c.QBFinalizeConcurrency > maxQBFinalizeConcurrency {
		return fmt.Errorf("qb finalize concurrency must be between 0 and %d (0 = default 1)", maxQBFinalizeConcurrency)
	}
	if c.VerifyConcurrency < 0 || c.VerifyConcurrency > maxVerifyConcurrencyCap {
		return fmt.Errorf("verify concurrency must be between 0 and %d (0 = default %d)",
			maxVerifyConcurrencyCap, maxVerifyConcurrency)
	}
	return nil
}
