package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/sync/errgroup"

	"github.com/arsac/qb-sync/internal/config"
	"github.com/arsac/qb-sync/internal/destination"
	"github.com/arsac/qb-sync/internal/health"
	"github.com/arsac/qb-sync/internal/logger"
	"github.com/arsac/qb-sync/internal/source"
)

const bytesPerMB = 1024 * 1024

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
		Long: `qbsync synchronizes torrents between source and destination servers.

Run as source server to stream pieces from source qBittorrent to destination server.
Run as destination server to receive pieces and manage destination qBittorrent.`,
	}

	sourceCmd := &cobra.Command{
		Use:   "source",
		Short: "Run as source server",
		Long:  "Stream pieces from local qBittorrent to destination server.",
		RunE:  runSource,
	}

	destinationCmd := &cobra.Command{
		Use:   "destination",
		Short: "Run as destination server",
		Long:  "Receive pieces via gRPC, write to disk, add verified torrents to qBittorrent.",
		RunE:  runDestination,
	}

	config.SetupSourceFlags(sourceCmd)
	config.SetupDestinationFlags(destinationCmd)

	rootCmd.AddCommand(sourceCmd, destinationCmd)

	return rootCmd.Execute()
}

// signalContext returns a context that is cancelled on SIGINT or SIGTERM.
func signalContext(log *slog.Logger) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Info("received signal, shutting down", "signal", sig)
		cancel()
	}()
	return ctx, cancel
}

// startHealthServer starts the health server in the errgroup if addr is non-empty.
func startHealthServer(
	ctx context.Context, g *errgroup.Group, addr string, log *slog.Logger,
) *health.Server {
	if addr == "" {
		return nil
	}
	hs := health.NewServer(health.Config{Addr: addr}, log.With("component", "health"))
	g.Go(func() error {
		return hs.Run(ctx)
	})
	return hs
}

func runSource(cmd *cobra.Command, _ []string) error {
	v := viper.New()
	if err := config.BindSourceFlags(cmd, v); err != nil {
		return err
	}

	cfg, err := config.LoadSource(v)
	if err != nil {
		return err
	}

	log := logger.New("source", logger.ParseLevel(cfg.LogLevel))
	log.Info("starting source server",
		"data", cfg.DataPath,
		"qbURL", cfg.QBURL,
		"destinationAddr", cfg.DestinationAddr,
		"healthAddr", cfg.HealthAddr,
		"dryRun", cfg.DryRun,
	)

	ctx, cancel := signalContext(log)
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)
	healthServer := startHealthServer(ctx, g, cfg.HealthAddr, log)

	g.Go(func() error {
		runner := source.NewRunner(cfg, log)
		if healthServer != nil {
			runner.SetHealthServer(healthServer)
		}
		return runner.Run(ctx)
	})

	return g.Wait()
}

func runDestination(cmd *cobra.Command, _ []string) error {
	v := viper.New()
	if err := config.BindDestinationFlags(cmd, v); err != nil {
		return err
	}

	cfg, err := config.LoadDestination(v)
	if err != nil {
		return err
	}

	log := logger.New("destination", logger.ParseLevel(cfg.LogLevel))
	log.Info("starting destination server",
		"listen", cfg.ListenAddr,
		"data", cfg.DataPath,
		"savePath", cfg.SavePath,
		"qbURL", cfg.QBURL,
		"healthAddr", cfg.HealthAddr,
		"streamWorkers", cfg.StreamWorkers,
		"maxStreamBufferMB", cfg.MaxStreamBufferMB,
		"dryRun", cfg.DryRun,
	)

	ctx, cancel := signalContext(log)
	defer cancel()

	serverCfg := destination.ServerConfig{
		ListenAddr:           cfg.ListenAddr,
		BasePath:             cfg.DataPath,
		SavePath:             cfg.SavePath,
		StreamWorkers:        cfg.StreamWorkers,
		MaxStreamBufferBytes: int64(cfg.MaxStreamBufferMB) * bytesPerMB,
		SyncedTag:            cfg.SyncedTag,
		DryRun:               cfg.DryRun,
	}

	if cfg.QBURL != "" {
		serverCfg.QB = &destination.QBConfig{
			URL:          cfg.QBURL,
			Username:     cfg.QBUsername,
			Password:     cfg.QBPassword,
			PollInterval: cfg.PollInterval,
			PollTimeout:  cfg.PollTimeout,
		}
	}

	g, ctx := errgroup.WithContext(ctx)
	healthServer := startHealthServer(ctx, g, cfg.HealthAddr, log)

	g.Go(func() error {
		server := destination.NewServer(serverCfg, log)
		if healthServer != nil {
			server.SetHealthServer(healthServer)
		}
		return server.Run(ctx)
	})

	return g.Wait()
}
