package utils

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/fsnotify/fsnotify"
)

const eventChannelBufSize = 1000

// Watcher watches a directory tree for file changes.
type Watcher struct {
	watcher *fsnotify.Watcher
	logger  *slog.Logger
	root    string
	Events  chan string
}

// NewWatcher creates a new Watcher for the given root directory.
func NewWatcher(logger *slog.Logger, root string) (*Watcher, error) {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	return &Watcher{
		watcher: w,
		logger:  logger,
		root:    root,
		Events:  make(chan string, eventChannelBufSize),
	}, nil
}

// AddRecursive adds all directories under path to the watcher.
func (w *Watcher) AddRecursive(ctx context.Context, path string) error {
	return filepath.WalkDir(path, func(p string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			if addErr := w.watcher.Add(p); addErr != nil {
				w.logger.WarnContext(ctx, "failed to watch directory", "path", p, "error", addErr)
			}
		}
		return nil
	})
}

// Run starts the watcher event loop.
func (w *Watcher) Run(ctx context.Context) error {
	if err := w.AddRecursive(ctx, w.root); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-w.watcher.Events:
			if !ok {
				return nil
			}
			w.handleEvent(ctx, event)
		case err, ok := <-w.watcher.Errors:
			if !ok {
				return nil
			}
			w.logger.ErrorContext(ctx, "watcher error", "error", err)
		}
	}
}

func (w *Watcher) handleEvent(ctx context.Context, event fsnotify.Event) {
	if event.Op&(fsnotify.Create|fsnotify.Write|fsnotify.Rename) == 0 {
		return
	}

	info, statErr := os.Stat(event.Name)
	if statErr != nil {
		return
	}

	if info.IsDir() && event.Op&fsnotify.Create != 0 {
		if addErr := w.AddRecursive(ctx, event.Name); addErr != nil {
			w.logger.WarnContext(ctx, "failed to watch new directory", "path", event.Name, "error", addErr)
		}
	}

	relPath, relErr := filepath.Rel(w.root, event.Name)
	if relErr != nil {
		relPath = event.Name
	}

	select {
	case w.Events <- relPath:
	default:
		w.logger.WarnContext(ctx, "event queue full, dropping event", "path", relPath)
	}
}

func (w *Watcher) Close() error {
	close(w.Events)
	return w.watcher.Close()
}
