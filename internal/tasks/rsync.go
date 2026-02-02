package tasks

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/mailoarsac/qb-router/internal/config"
	"github.com/mailoarsac/qb-router/internal/utils"
)

const (
	batchInterval     = 15 * time.Second
	errChannelBufSize = 3
)

type RsyncTask struct {
	cfg         *config.Config
	logger      *slog.Logger
	watcher     *utils.Watcher
	initialDone chan struct{}
	pendingDirs sync.Map
}

func NewRsyncTask(cfg *config.Config, logger *slog.Logger) *RsyncTask {
	return &RsyncTask{
		cfg:         cfg,
		logger:      logger,
		initialDone: make(chan struct{}),
	}
}

func (t *RsyncTask) Run(ctx context.Context) error {
	watcher, watcherErr := utils.NewWatcher(t.logger, t.cfg.SrcPath)
	if watcherErr != nil {
		return watcherErr
	}
	t.watcher = watcher
	defer t.watcher.Close()

	errCh := make(chan error, errChannelBufSize)

	go func() {
		errCh <- t.initialSync(ctx)
	}()

	go func() {
		errCh <- t.watcher.Run(ctx)
	}()

	go func() {
		errCh <- t.processEvents(ctx)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case taskErr := <-errCh:
		return taskErr
	}
}

func (t *RsyncTask) initialSync(ctx context.Context) error {
	defer close(t.initialDone)

	t.logger.InfoContext(ctx, "starting initial sync", "src", t.cfg.SrcPath, "dest", t.cfg.DestPath)

	if t.cfg.DryRun {
		t.logger.InfoContext(ctx, "dry run: skipping initial sync")
		return nil
	}

	args := t.rsyncArgs()
	args = append(args, t.cfg.SrcPath+"/", t.cfg.DestPath+"/")

	if err := utils.Execute(ctx, t.logger, "rsync", args...); err != nil {
		t.logger.ErrorContext(ctx, "initial sync failed", "error", err)
		return err
	}

	t.logger.InfoContext(ctx, "initial sync completed")
	return nil
}

func (t *RsyncTask) processEvents(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.initialDone:
	}

	ticker := time.NewTicker(batchInterval)
	defer ticker.Stop()

	var batch []string

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case path := <-t.watcher.Events:
			dir := filepath.Dir(path)
			if _, loaded := t.pendingDirs.LoadOrStore(dir, struct{}{}); !loaded {
				batch = append(batch, dir)
			}

		case <-ticker.C:
			if len(batch) == 0 {
				continue
			}

			dirs := batch
			batch = nil

			for _, dir := range dirs {
				t.pendingDirs.Delete(dir)
			}

			if err := t.syncDirs(ctx, dirs); err != nil {
				t.logger.ErrorContext(ctx, "sync failed", "error", err)
			}
		}
	}
}

func (t *RsyncTask) syncDirs(ctx context.Context, dirs []string) error {
	t.logger.InfoContext(ctx, "syncing directories", "count", len(dirs))

	if t.cfg.DryRun {
		for _, dir := range dirs {
			t.logger.InfoContext(ctx, "dry run: would sync", "dir", dir)
		}
		return nil
	}

	tmpFile, createErr := os.CreateTemp("", "rsync-files-*.txt")
	if createErr != nil {
		return createErr
	}
	defer os.Remove(tmpFile.Name())

	for _, dir := range dirs {
		if _, writeErr := tmpFile.WriteString(dir + "/\n"); writeErr != nil {
			_ = tmpFile.Close()
			return writeErr
		}
	}
	if closeErr := tmpFile.Close(); closeErr != nil {
		return closeErr
	}

	args := t.rsyncArgs()
	args = append(args, "--files-from="+tmpFile.Name())
	args = append(args, t.cfg.SrcPath+"/", t.cfg.DestPath+"/")

	return utils.Execute(ctx, t.logger, "rsync", args...)
}

func (t *RsyncTask) rsyncArgs() []string {
	return []string{
		"--hard-links",
		"--times",
		"--whole-file",
		"--inplace",
		"--partial",
		"--verbose",
		"--progress",
		"--one-file-system",
		"--recursive",
		"--perms",
		"--group",
		"--owner",
		"--devices",
		"--specials",
		"--acls",
		"--xattrs",
	}
}
