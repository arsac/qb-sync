package tasks

import (
	"context"
	"log/slog"

	"golang.org/x/sync/errgroup"

	"github.com/mailoarsac/qb-router/internal/config"
)

type Runner struct {
	cfg    *config.Config
	logger *slog.Logger
}

func NewRunner(cfg *config.Config, logger *slog.Logger) *Runner {
	return &Runner{
		cfg:    cfg,
		logger: logger,
	}
}

func (r *Runner) Run(ctx context.Context) error {
	qbTask, err := NewQBTask(r.cfg, r.logger.With("task", "qb"))
	if err != nil {
		return err
	}

	rsyncTask := NewRsyncTask(r.cfg, r.logger.With("task", "rsync"))

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		r.logger.Info("starting qbittorrent task")
		return qbTask.Run(ctx)
	})

	g.Go(func() error {
		r.logger.Info("starting rsync task")
		return rsyncTask.Run(ctx)
	})

	return g.Wait()
}
