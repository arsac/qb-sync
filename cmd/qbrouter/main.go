package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/mailoarsac/qb-router/internal/config"
	"github.com/mailoarsac/qb-router/internal/logger"
	"github.com/mailoarsac/qb-router/internal/tasks"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	v := viper.New()

	rootCmd := &cobra.Command{
		Use:   "qbrouter",
		Short: "Sync and manage torrents between qBittorrent instances",
		RunE: func(_ *cobra.Command, _ []string) error {
			cfg, err := config.Load(v)
			if err != nil {
				return err
			}

			log := logger.New("main")
			log.Info("starting qbrouter",
				"src", cfg.SrcPath,
				"dest", cfg.DestPath,
				"srcURL", cfg.SrcURL,
				"destURL", cfg.DestURL,
				"dryRun", cfg.DryRun,
			)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			sigCh := make(chan os.Signal, 1)
			signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

			go func() {
				sig := <-sigCh
				log.Info("received signal, shutting down", "signal", sig)
				cancel()
			}()

			runner := tasks.NewRunner(cfg, log)
			return runner.Run(ctx)
		},
	}

	config.SetupFlags(rootCmd)
	if err := config.BindFlags(rootCmd, v); err != nil {
		return err
	}

	return rootCmd.Execute()
}
