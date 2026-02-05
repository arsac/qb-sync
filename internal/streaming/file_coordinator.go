package streaming

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"sync"

	"golang.org/x/sync/singleflight"
)

// FileCoordinator coordinates file transfers to avoid sending duplicate data
// for hardlinked files. It uses the singleflight pattern to ensure that when
// multiple goroutines need the same file (by inode), only one transfer occurs
// and others wait then create hardlinks.
type FileCoordinator struct {
	dest   HardlinkDestination
	logger *slog.Logger

	group singleflight.Group

	// Track completed files: inode -> receiver path
	completed map[uint64]string
	mu        sync.RWMutex
}

// NewFileCoordinator creates a new file coordinator wrapping a destination.
func NewFileCoordinator(dest HardlinkDestination, logger *slog.Logger) *FileCoordinator {
	return &FileCoordinator{
		dest:      dest,
		logger:    logger,
		completed: make(map[uint64]string),
	}
}

// FileResult contains the result of ensuring a file exists on the receiver.
type FileResult struct {
	Path        string // Path on receiver where file exists
	Hardlinked  bool   // True if file was hardlinked (not transferred)
	Transferred bool   // True if file was transferred by this call
	Shared      bool   // True if result was shared from another goroutine
}

// EnsureFile ensures a file with the given inode exists on the receiver.
// If the file was already transferred (same inode), it creates a hardlink.
// If another goroutine is currently transferring the same inode, it waits
// for completion then creates a hardlink.
// The transferFunc is called only if this is the first request for this inode.
func (c *FileCoordinator) EnsureFile(
	ctx context.Context,
	inode uint64,
	targetPath string,
	size int64,
	transferFunc func(ctx context.Context) error,
) (*FileResult, error) {
	// Fast path: check if already completed locally
	c.mu.RLock()
	existingPath, found := c.completed[inode]
	c.mu.RUnlock()

	if found {
		return c.createHardlinkResult(ctx, existingPath, targetPath)
	}

	// Check if file exists on receiver (from previous session)
	remotePath, remoteFound, err := c.dest.GetFileByInode(ctx, inode)
	if err != nil {
		return nil, fmt.Errorf("checking remote file: %w", err)
	}

	if remoteFound {
		// File exists on receiver, update local cache and hardlink
		c.mu.Lock()
		c.completed[inode] = remotePath
		c.mu.Unlock()

		return c.createHardlinkResult(ctx, remotePath, targetPath)
	}

	// Use singleflight to coordinate concurrent transfers of same inode
	key := strconv.FormatUint(inode, 10)

	// Use a detached context for the transfer function since singleflight
	// shares results across multiple callers. If the original caller's context
	// is cancelled, we still want the transfer to complete for other waiters.
	// The outer select handles context cancellation for the waiting caller.
	transferCtx := context.WithoutCancel(ctx)

	resultCh := c.group.DoChan(key, func() (any, error) {
		// Double-check after acquiring singleflight slot
		c.mu.RLock()
		if path, ok := c.completed[inode]; ok {
			c.mu.RUnlock()
			return path, nil
		}
		c.mu.RUnlock()

		// Perform the transfer with detached context
		if transferErr := transferFunc(transferCtx); transferErr != nil {
			return nil, transferErr
		}

		// Register the file on receiver
		if regErr := c.dest.RegisterFile(transferCtx, inode, targetPath, size); regErr != nil {
			c.logger.WarnContext(transferCtx, "failed to register file",
				"inode", inode,
				"path", targetPath,
				"error", regErr,
			)
			// Continue anyway - file was transferred successfully
		}

		// Update local cache
		c.mu.Lock()
		c.completed[inode] = targetPath
		c.mu.Unlock()

		return targetPath, nil
	})

	// Wait for result with context cancellation support
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case result := <-resultCh:
		if result.Err != nil {
			return nil, result.Err
		}

		resultPath, ok := result.Val.(string)
		if !ok {
			return nil, fmt.Errorf("unexpected result type: %T", result.Val)
		}

		// If this wasn't the goroutine that did the transfer, create hardlink
		if result.Shared {
			return c.createHardlinkResult(ctx, resultPath, targetPath)
		}

		return &FileResult{
			Path:        resultPath,
			Transferred: true,
			Shared:      false,
		}, nil
	}
}

// createHardlinkResult creates a hardlink and returns the result.
func (c *FileCoordinator) createHardlinkResult(
	ctx context.Context,
	sourcePath, targetPath string,
) (*FileResult, error) {
	// Skip if source and target are the same
	if sourcePath == targetPath {
		return &FileResult{
			Path:       targetPath,
			Hardlinked: false,
			Shared:     true,
		}, nil
	}

	if err := c.dest.CreateHardlink(ctx, sourcePath, targetPath); err != nil {
		return nil, fmt.Errorf("creating hardlink from %s to %s: %w", sourcePath, targetPath, err)
	}

	c.logger.InfoContext(ctx, "created hardlink instead of transfer",
		"source", sourcePath,
		"target", targetPath,
	)

	return &FileResult{
		Path:       targetPath,
		Hardlinked: true,
		Shared:     true,
	}, nil
}

// MarkComplete marks a file as completed without going through singleflight.
// Use this when a file was transferred through normal piece streaming.
func (c *FileCoordinator) MarkComplete(ctx context.Context, inode uint64, path string, size int64) {
	c.mu.Lock()
	c.completed[inode] = path
	c.mu.Unlock()

	// Also register on receiver for persistence across restarts
	if err := c.dest.RegisterFile(ctx, inode, path, size); err != nil {
		c.logger.WarnContext(ctx, "failed to register completed file",
			"inode", inode,
			"path", path,
			"error", err,
		)
	}
}

// IsComplete checks if a file with the given inode has been completed.
func (c *FileCoordinator) IsComplete(inode uint64) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	path, found := c.completed[inode]
	return path, found
}

// CheckRemote checks if a file exists on the receiver and caches the result.
func (c *FileCoordinator) CheckRemote(ctx context.Context, inode uint64) (string, bool, error) {
	// Check local cache first
	c.mu.RLock()
	if path, found := c.completed[inode]; found {
		c.mu.RUnlock()
		return path, true, nil
	}
	c.mu.RUnlock()

	// Check receiver
	path, found, err := c.dest.GetFileByInode(ctx, inode)
	if err != nil {
		return "", false, err
	}

	if found {
		c.mu.Lock()
		c.completed[inode] = path
		c.mu.Unlock()
	}

	return path, found, nil
}

// Destination returns the underlying destination for direct piece operations.
func (c *FileCoordinator) Destination() HardlinkDestination {
	return c.dest
}
