package cli

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/jgalley/usgmon/internal/config"
	"github.com/jgalley/usgmon/internal/daemon"
	"github.com/jgalley/usgmon/internal/storage"
	"github.com/spf13/cobra"
)

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Start the daemon",
	Long:  `Start the usgmon daemon. This is typically invoked by systemd.`,
	RunE:  runServe,
}

func runServe(cmd *cobra.Command, args []string) error {
	// Load configuration
	cfg, err := config.Load(cfgFile)
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	// Override log level from flag if specified
	if cmd.Flags().Changed("log-level") {
		cfg.Logging.Level = logLevel
	}

	logger := setupLogger(cfg.Logging.Level, cfg.Logging.Format)

	logger.Info("starting usgmon daemon",
		"config", cfgFile,
		"db", cfg.Database.Path,
		"workers", cfg.Scan.Workers,
		"paths", len(cfg.Paths),
	)

	// Initialize storage
	store, err := storage.NewSQLiteStorage(cfg.Database.Path)
	if err != nil {
		return fmt.Errorf("opening database: %w", err)
	}
	defer store.Close()

	ctx := context.Background()
	if err := store.Initialize(ctx); err != nil {
		return fmt.Errorf("initializing database: %w", err)
	}

	// Create daemon
	d := daemon.New(cfg, store, logger)

	// Setup signal handling
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		logger.Info("received signal, initiating graceful shutdown", "signal", sig)
		cancel()
	}()

	// Run daemon
	if err := d.Run(ctx); err != nil && err != context.Canceled {
		return fmt.Errorf("daemon error: %w", err)
	}

	logger.Info("daemon stopped")
	return nil
}
