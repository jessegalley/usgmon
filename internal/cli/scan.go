package cli

import (
	"context"
	"fmt"
	"os"
	"sort"
	"text/tabwriter"
	"time"

	"github.com/jgalley/usgmon/internal/config"
	"github.com/jgalley/usgmon/internal/scanner"
	"github.com/jgalley/usgmon/internal/storage"
	"github.com/spf13/cobra"
)

var (
	scanDepth int
	scanStore bool
)

var scanCmd = &cobra.Command{
	Use:   "scan <path>",
	Short: "One-shot scan of a directory",
	Long: `Scan a directory and print its size. By default, the results are not stored.

Examples:
  usgmon scan /www/users/bob.com
  usgmon scan /www/users --depth 1
  usgmon scan /www/users --depth 1 --store`,
	Args: cobra.ExactArgs(1),
	RunE: runScan,
}

func init() {
	scanCmd.Flags().IntVar(&scanDepth, "depth", 0, "scan depth (0 = scan the path itself)")
	scanCmd.Flags().BoolVar(&scanStore, "store", false, "store results in database")
}

func runScan(cmd *cobra.Command, args []string) error {
	path := args[0]

	// Check if path exists
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("accessing path: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is not a directory", path)
	}

	logger := setupLogger(logLevel, "text")

	// Create scanner
	s := scanner.New(4, nil) // auto-detect strategy

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	var results []scanner.Result

	if scanDepth == 0 {
		// Scan single directory
		result, err := s.ScanSingle(ctx, path)
		if err != nil {
			return fmt.Errorf("scan failed: %w", err)
		}
		results = []scanner.Result{result}
	} else {
		// Scan at depth
		var err error
		results, err = s.ScanPath(ctx, path, scanDepth)
		if err != nil {
			return fmt.Errorf("scan failed: %w", err)
		}
	}

	// Sort results by path
	sort.Slice(results, func(i, j int) bool {
		return results[i].Path < results[j].Path
	})

	// Print results
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	for _, r := range results {
		if r.Error != nil {
			fmt.Fprintf(w, "%s\t(error: %v)\n", r.Path, r.Error)
		} else {
			fmt.Fprintf(w, "%s\t%s\n", r.Path, formatSize(r.SizeBytes))
		}
	}
	w.Flush()

	// Store results if requested
	if scanStore {
		cfg, err := config.Load(cfgFile)
		if err != nil {
			return fmt.Errorf("loading config: %w", err)
		}

		store, err := storage.NewSQLiteStorage(cfg.Database.Path)
		if err != nil {
			return fmt.Errorf("opening database: %w", err)
		}
		defer store.Close()

		if err := store.Initialize(ctx); err != nil {
			return fmt.Errorf("initializing database: %w", err)
		}

		scanID, err := store.StartScan(ctx, path)
		if err != nil {
			return fmt.Errorf("creating scan record: %w", err)
		}

		now := time.Now().UTC()
		records := make([]storage.UsageRecord, 0, len(results))
		for _, r := range results {
			if r.Error == nil {
				records = append(records, storage.UsageRecord{
					BasePath:   path,
					Directory:  r.Path,
					SizeBytes:  r.SizeBytes,
					RecordedAt: now,
					ScanID:     scanID,
				})
			}
		}

		if err := store.RecordUsageBatch(ctx, records); err != nil {
			return fmt.Errorf("storing results: %w", err)
		}

		if err := store.CompleteScan(ctx, scanID, len(records)); err != nil {
			return fmt.Errorf("completing scan: %w", err)
		}

		logger.Info("results stored", "count", len(records))
	}

	return nil
}

// formatSize formats bytes as human-readable size.
func formatSize(bytes int64) string {
	const (
		KiB = 1024
		MiB = KiB * 1024
		GiB = MiB * 1024
		TiB = GiB * 1024
	)

	switch {
	case bytes >= TiB:
		return fmt.Sprintf("%.2f TiB", float64(bytes)/float64(TiB))
	case bytes >= GiB:
		return fmt.Sprintf("%.2f GiB", float64(bytes)/float64(GiB))
	case bytes >= MiB:
		return fmt.Sprintf("%.2f MiB", float64(bytes)/float64(MiB))
	case bytes >= KiB:
		return fmt.Sprintf("%.2f KiB", float64(bytes)/float64(KiB))
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}
