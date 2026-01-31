package cli

import (
	"log/slog"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

var (
	cfgFile  string
	logLevel string
	rootCmd  *cobra.Command
)

// Execute runs the root command.
func Execute() error {
	return rootCmd.Execute()
}

func init() {
	rootCmd = &cobra.Command{
		Use:   "usgmon",
		Short: "Directory usage monitor daemon",
		Long: `usgmon is a daemon that periodically monitors disk usage of directories
at configurable depths and stores historical data in SQLite for trend analysis.`,
		SilenceUsage: true,
	}

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default: /etc/usgmon/usgmon.yaml)")
	rootCmd.PersistentFlags().StringVar(&logLevel, "log-level", "info", "log level (debug, info, warn, error)")

	rootCmd.AddCommand(serveCmd)
	rootCmd.AddCommand(scanCmd)
	rootCmd.AddCommand(queryCmd)
	rootCmd.AddCommand(topCmd)
	rootCmd.AddCommand(versionCmd)
}

// setupLogger creates a logger based on the configured level.
func setupLogger(level string, format string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn", "warning":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{Level: lvl}

	var handler slog.Handler
	if format == "json" {
		handler = slog.NewJSONHandler(os.Stderr, opts)
	} else {
		handler = slog.NewTextHandler(os.Stderr, opts)
	}

	return slog.New(handler)
}
