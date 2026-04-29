package destination

import (
	"context"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"sync"

	"github.com/arsac/qb-sync/internal/metrics"
)

// InodeRegistry manages inode-to-path mappings for hardlink deduplication.
// It tracks both completed files (registered) and files being written (in-progress).
type InodeRegistry struct {
	// Registered inodes: source file ID -> relative path on receiver
	registered   map[FileID]string
	registeredMu sync.RWMutex

	// In-progress inode tracking for parallel torrent coordination.
	// When a torrent starts writing a file with a given source inode,
	// it registers here. Other torrents with the same inode can wait
	// for the file to be finalized and then create a hardlink.
	inProgress   map[FileID]*inProgressInode
	inProgressMu sync.RWMutex

	basePath string
	logger   *slog.Logger
}

// NewInodeRegistry creates a new InodeRegistry.
func NewInodeRegistry(basePath string, logger *slog.Logger) *InodeRegistry {
	return &InodeRegistry{
		registered: make(map[FileID]string),
		inProgress: make(map[FileID]*inProgressInode),
		basePath:   basePath,
		logger:     logger,
	}
}

// GetRegistered looks up a registered file ID and returns its path if found.
func (r *InodeRegistry) GetRegistered(fileID FileID) (string, bool) {
	r.registeredMu.RLock()
	path, found := r.registered[fileID]
	r.registeredMu.RUnlock()
	return path, found
}

// Register adds a file ID-to-path mapping.
func (r *InodeRegistry) Register(fileID FileID, relPath string) {
	r.registeredMu.Lock()
	r.registered[fileID] = relPath
	metrics.InodeRegistrySize.Set(float64(len(r.registered)))
	r.registeredMu.Unlock()
}

// Evict removes a registered file ID mapping. Used to purge stale entries
// when a source inode has been recycled (e.g., file deleted and inode reused
// by the filesystem for a different file).
func (r *InodeRegistry) Evict(fileID FileID) {
	r.registeredMu.Lock()
	delete(r.registered, fileID)
	metrics.InodeRegistrySize.Set(float64(len(r.registered)))
	r.registeredMu.Unlock()
}

// GetInProgress looks up an in-progress file ID.
// Returns targetPath, doneCh, torrentHash, and found.
func (r *InodeRegistry) GetInProgress(fileID FileID) (string, chan struct{}, string, bool) {
	r.inProgressMu.RLock()
	inProg, ok := r.inProgress[fileID]
	r.inProgressMu.RUnlock()

	if !ok {
		return "", nil, "", false
	}

	return inProg.targetPath, inProg.doneCh, inProg.torrentHash, true
}

// RegisterInProgress registers a torrent as the first writer for a file ID.
// Returns true if registration succeeded, false if already registered.
func (r *InodeRegistry) RegisterInProgress(fileID FileID, torrentHash, relTargetPath string) bool {
	r.inProgressMu.Lock()
	defer r.inProgressMu.Unlock()

	if _, exists := r.inProgress[fileID]; exists {
		return false
	}

	r.inProgress[fileID] = &inProgressInode{
		targetPath:  relTargetPath,
		doneCh:      make(chan struct{}),
		torrentHash: torrentHash,
	}
	return true
}

// CompleteInProgress marks an in-progress file ID as complete, signaling waiters.
// Returns true if the file ID was found and completed.
func (r *InodeRegistry) CompleteInProgress(fileID FileID, expectedHash string) bool {
	r.inProgressMu.Lock()
	defer r.inProgressMu.Unlock()

	inProg, ok := r.inProgress[fileID]
	if !ok {
		return false
	}

	if inProg.torrentHash == expectedHash {
		inProg.close()
		delete(r.inProgress, fileID)
		return true
	}
	return false
}

// AbortInProgress aborts an in-progress file ID, signaling waiters.
// Only aborts if the file ID belongs to the specified torrent.
// Safe to call with nil receiver or zero file ID (no-op).
func (r *InodeRegistry) AbortInProgress(ctx context.Context, fileID FileID, torrentHash string) {
	if r == nil || fileID.IsZero() {
		return
	}
	r.inProgressMu.Lock()
	defer r.inProgressMu.Unlock()

	if inProg, ok := r.inProgress[fileID]; ok {
		if inProg.torrentHash == torrentHash {
			inProg.close()
			delete(r.inProgress, fileID)
			r.logger.DebugContext(ctx, "cleaned up in-progress inode on abort",
				"hash", torrentHash,
				"fileID", fileID,
			)
		}
	}
}

// CleanupStale removes entries where the file no longer exists on disk.
func (r *InodeRegistry) CleanupStale(ctx context.Context) {
	// Collect all entries to check (avoid holding lock during file stat operations)
	r.registeredMu.RLock()
	entries := make(map[FileID]string, len(r.registered))
	maps.Copy(entries, r.registered)
	r.registeredMu.RUnlock()

	if len(entries) == 0 {
		return
	}

	// Check each entry
	var staleFileIDs []FileID
	for fileID, relPath := range entries {
		fullPath := filepath.Join(r.basePath, relPath)
		if _, err := os.Stat(fullPath); os.IsNotExist(err) {
			staleFileIDs = append(staleFileIDs, fileID)
		}
	}

	if len(staleFileIDs) == 0 {
		return
	}

	// Remove stale entries
	r.registeredMu.Lock()
	for _, fileID := range staleFileIDs {
		// Double-check the path hasn't changed since we read it
		if currentPath, ok := r.registered[fileID]; ok && currentPath == entries[fileID] {
			delete(r.registered, fileID)
		}
	}
	remaining := len(r.registered)
	metrics.InodeRegistrySize.Set(float64(remaining))
	r.registeredMu.Unlock()

	r.logger.InfoContext(ctx, "cleaned up stale inode entries",
		"removed", len(staleFileIDs),
		"remaining", remaining,
	)
}

// Len returns the number of registered inodes.
func (r *InodeRegistry) Len() int {
	r.registeredMu.RLock()
	defer r.registeredMu.RUnlock()
	return len(r.registered)
}
