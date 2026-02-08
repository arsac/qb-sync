package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/sync/errgroup"

	"github.com/arsac/qb-sync/internal/cold"
	"github.com/arsac/qb-sync/internal/config"
	"github.com/arsac/qb-sync/internal/health"
	"github.com/arsac/qb-sync/internal/hot"
	"github.com/arsac/qb-sync/internal/logger"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	rootCmd := &cobra.Command{
		Use:   "qbsync",
		Short: "Sync torrents between qBittorrent instances",
		Long: `qbsync synchronizes torrents between hot (source) and cold (destination) servers.

Run as hot server to stream pieces from source qBittorrent to cold server.
Run as cold server to receive pieces and manage destination qBittorrent.`,
	}

	hotCmd := &cobra.Command{
		Use:   "hot",
		Short: "Run as hot (source) server",
		Long:  "Stream pieces from local qBittorrent to cold server.",
		RunE:  runHot,
	}

	coldCmd := &cobra.Command{
		Use:   "cold",
		Short: "Run as cold (destination) server",
		Long:  "Receive pieces via gRPC, write to disk, add verified torrents to qBittorrent.",
		RunE:  runCold,
	}

	// Setup flags for each subcommand
	config.SetupHotFlags(hotCmd)
	config.SetupColdFlags(coldCmd)

	rootCmd.AddCommand(hotCmd, coldCmd)

	return rootCmd.Execute()
}

func runHot(cmd *cobra.Command, _ []string) error {
	v := viper.New()
	if err := config.BindHotFlags(cmd, v); err != nil {
		return err
	}

	cfg, err := config.LoadHot(v)
	if err != nil {
		return err
	}

	log := logger.New("hot", logger.ParseLevel(cfg.LogLevel))
	log.Info("starting hot server",
		"data", cfg.DataPath,
		"qbURL", cfg.QBURL,
		"coldAddr", cfg.ColdAddr,
		"healthAddr", cfg.HealthAddr,
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

	g, ctx := errgroup.WithContext(ctx)

	// Start health server if configured
	var healthServer *health.Server
	if cfg.HealthAddr != "" {
		healthServer = health.NewServer(health.Config{Addr: cfg.HealthAddr}, log.With("component", "health"))
		g.Go(func() error {
			return healthServer.Run(ctx)
		})
	}

	// Start the hot runner
	g.Go(func() error {
		runner := hot.NewRunner(cfg, log)
		if healthServer != nil {
			runner.SetHealthServer(healthServer)
		}
		return runner.Run(ctx)
	})

	return g.Wait()
}

func runCold(cmd *cobra.Command, _ []string) error {
	v := viper.New()
	if err := config.BindColdFlags(cmd, v); err != nil {
		return err
	}

	cfg, err := config.LoadCold(v)
	if err != nil {
		return err
	}

	log := logger.New("cold", logger.ParseLevel(cfg.LogLevel))
	log.Info("starting cold server",
		"listen", cfg.ListenAddr,
		"data", cfg.DataPath,
		"savePath", cfg.SavePath,
		"qbURL", cfg.QBURL,
		"healthAddr", cfg.HealthAddr,
		"streamWorkers", cfg.StreamWorkers,
		"maxStreamBufferMB", cfg.MaxStreamBufferMB,
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

	// Build server config
	serverCfg := cold.ServerConfig{
		ListenAddr:           cfg.ListenAddr,
		BasePath:             cfg.DataPath,
		SavePath:             cfg.SavePath,
		StreamWorkers:        cfg.StreamWorkers,
		MaxStreamBufferBytes: int64(cfg.MaxStreamBufferMB) * 1024 * 1024,
		SyncedTag:            cfg.SyncedTag,
		DryRun:               cfg.DryRun,
	}

	// Add qBittorrent config if provided
	if cfg.QBURL != "" {
		serverCfg.ColdQB = &cold.QBConfig{
			URL:          cfg.QBURL,
			Username:     cfg.QBUsername,
			Password:     cfg.QBPassword,
			PollInterval: cfg.PollInterval,
			PollTimeout:  cfg.PollTimeout,
		}
	}

	g, ctx := errgroup.WithContext(ctx)

	// Start health server if configured
	var healthServer *health.Server
	if cfg.HealthAddr != "" {
		healthServer = health.NewServer(health.Config{Addr: cfg.HealthAddr}, log.With("component", "health"))
		g.Go(func() error {
			return healthServer.Run(ctx)
		})
	}

	// Start the cold server
	g.Go(func() error {
		server := cold.NewServer(serverCfg, log)
		if healthServer != nil {
			server.SetHealthServer(healthServer)
		}
		return server.Run(ctx)
	})

	return g.Wait()
}
