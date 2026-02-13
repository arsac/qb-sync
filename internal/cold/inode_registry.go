package cold

import (
	"context"
	"encoding/json"
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
	// Registered inodes: source inode -> relative path on receiver
	registered   map[Inode]string
	registeredMu sync.RWMutex

	// In-progress inode tracking for parallel torrent coordination.
	// When a torrent starts writing a file with a given source inode,
	// it registers here. Other torrents with the same inode can wait
	// for the file to be finalized and then create a hardlink.
	inProgress   map[Inode]*inProgressInode
	inProgressMu sync.RWMutex

	basePath string
	logger   *slog.Logger
}

// NewInodeRegistry creates a new InodeRegistry.
func NewInodeRegistry(basePath string, logger *slog.Logger) *InodeRegistry {
	return &InodeRegistry{
		registered: make(map[Inode]string),
		inProgress: make(map[Inode]*inProgressInode),
		basePath:   basePath,
		logger:     logger,
	}
}

// GetRegistered looks up a registered inode and returns its path if found.
func (r *InodeRegistry) GetRegistered(inode Inode) (string, bool) {
	r.registeredMu.RLock()
	path, found := r.registered[inode]
	r.registeredMu.RUnlock()
	return path, found
}

// Register adds an inode-to-path mapping.
func (r *InodeRegistry) Register(inode Inode, relPath string) {
	r.registeredMu.Lock()
	r.registered[inode] = relPath
	metrics.InodeRegistrySize.Set(float64(len(r.registered)))
	r.registeredMu.Unlock()
}

// GetInProgress looks up an in-progress inode.
// Returns targetPath, doneCh, torrentHash, and found.
func (r *InodeRegistry) GetInProgress(inode Inode) (string, chan struct{}, string, bool) {
	r.inProgressMu.RLock()
	inProg, ok := r.inProgress[inode]
	r.inProgressMu.RUnlock()

	if !ok {
		return "", nil, "", false
	}

	return inProg.targetPath, inProg.doneCh, inProg.torrentHash, true
}

// RegisterInProgress registers a torrent as the first writer for an inode.
// Returns true if registration succeeded, false if already registered.
func (r *InodeRegistry) RegisterInProgress(inode Inode, torrentHash, relTargetPath string) bool {
	r.inProgressMu.Lock()
	defer r.inProgressMu.Unlock()

	if _, exists := r.inProgress[inode]; exists {
		return false
	}

	r.inProgress[inode] = &inProgressInode{
		targetPath:  relTargetPath,
		doneCh:      make(chan struct{}),
		torrentHash: torrentHash,
	}
	return true
}

// CompleteInProgress marks an in-progress inode as complete, signaling waiters.
// Returns true if the inode was found and completed.
func (r *InodeRegistry) CompleteInProgress(inode Inode, expectedHash string) bool {
	r.inProgressMu.Lock()
	defer r.inProgressMu.Unlock()

	inProg, ok := r.inProgress[inode]
	if !ok {
		return false
	}

	if inProg.torrentHash == expectedHash {
		inProg.close()
		delete(r.inProgress, inode)
		return true
	}
	return false
}

// AbortInProgress aborts an in-progress inode, signaling waiters.
// Only aborts if the inode belongs to the specified torrent.
// Safe to call with nil receiver or zero inode (no-op).
func (r *InodeRegistry) AbortInProgress(ctx context.Context, inode Inode, torrentHash string) {
	if r == nil || inode == 0 {
		return
	}
	r.inProgressMu.Lock()
	defer r.inProgressMu.Unlock()

	if inProg, ok := r.inProgress[inode]; ok {
		if inProg.torrentHash == torrentHash {
			inProg.close()
			delete(r.inProgress, inode)
			r.logger.DebugContext(ctx, "cleaned up in-progress inode on abort",
				"hash", torrentHash,
				"inode", inode,
			)
		}
	}
}

// mapPath returns the path to the inode map persistence file.
func (r *InodeRegistry) mapPath() string {
	return filepath.Join(r.basePath, metaDirName, ".inode_map.json")
}

// Load loads the persisted inode-to-path mapping from disk.
func (r *InodeRegistry) Load() error {
	data, err := os.ReadFile(r.mapPath())
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var loaded map[Inode]string
	if unmarshalErr := json.Unmarshal(data, &loaded); unmarshalErr != nil {
		return unmarshalErr
	}

	r.registeredMu.Lock()
	r.registered = loaded
	metrics.InodeRegistrySize.Set(float64(len(r.registered)))
	r.registeredMu.Unlock()

	return nil
}

// Save persists the inode-to-path mapping to disk.
func (r *InodeRegistry) Save() error {
	r.registeredMu.RLock()
	data, err := json.Marshal(r.registered)
	r.registeredMu.RUnlock()

	if err != nil {
		return err
	}

	mapFile := r.mapPath()
	if mkdirErr := os.MkdirAll(filepath.Dir(mapFile), serverDirPermissions); mkdirErr != nil {
		return mkdirErr
	}

	return atomicWriteFile(mapFile, data)
}

// CleanupStale removes entries where the file no longer exists on disk.
func (r *InodeRegistry) CleanupStale(ctx context.Context) {
	// Collect all entries to check (avoid holding lock during file stat operations)
	r.registeredMu.RLock()
	entries := make(map[Inode]string, len(r.registered))
	maps.Copy(entries, r.registered)
	r.registeredMu.RUnlock()

	if len(entries) == 0 {
		return
	}

	// Check each entry
	var staleInodes []Inode
	for inode, relPath := range entries {
		fullPath := filepath.Join(r.basePath, relPath)
		if _, err := os.Stat(fullPath); os.IsNotExist(err) {
			staleInodes = append(staleInodes, inode)
		}
	}

	if len(staleInodes) == 0 {
		return
	}

	// Remove stale entries
	r.registeredMu.Lock()
	for _, inode := range staleInodes {
		// Double-check the path hasn't changed since we read it
		if currentPath, ok := r.registered[inode]; ok && currentPath == entries[inode] {
			delete(r.registered, inode)
		}
	}
	remaining := len(r.registered)
	metrics.InodeRegistrySize.Set(float64(remaining))
	r.registeredMu.Unlock()

	r.logger.InfoContext(ctx, "cleaned up stale inode entries",
		"removed", len(staleInodes),
		"remaining", remaining,
	)
}

// Len returns the number of registered inodes.
func (r *InodeRegistry) Len() int {
	r.registeredMu.RLock()
	defer r.registeredMu.RUnlock()
	return len(r.registered)
}
