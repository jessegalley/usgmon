package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/jgalley/usgmon/internal/config"
	"github.com/jgalley/usgmon/internal/storage"
	"github.com/spf13/cobra"
)

var (
	queryDays   int
	querySince  string
	queryFormat string
	queryLimit  int
)

var queryCmd = &cobra.Command{
	Use:   "query <path>",
	Short: "Query historical usage data",
	Long: `Query historical usage data for a directory.

Examples:
  usgmon query /www/users/bob.com
  usgmon query /www/users/bob.com --days 7
  usgmon query /www/users/bob.com --since "2026-01-01"
  usgmon query /www/users/bob.com --format json`,
	Args: cobra.ExactArgs(1),
	RunE: runQuery,
}

func init() {
	queryCmd.Flags().IntVar(&queryDays, "days", 0, "show records from the last N days")
	queryCmd.Flags().StringVar(&querySince, "since", "", "show records since date (YYYY-MM-DD)")
	queryCmd.Flags().StringVar(&queryFormat, "format", "text", "output format (text, json)")
	queryCmd.Flags().IntVar(&queryLimit, "limit", 100, "maximum number of records to show")
}

func runQuery(cmd *cobra.Command, args []string) error {
	path := args[0]

	cfg, err := config.Load(cfgFile)
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	store, err := storage.NewSQLiteStorage(cfg.Database.Path)
	if err != nil {
		return fmt.Errorf("opening database: %w", err)
	}
	defer store.Close()

	ctx := context.Background()
	if err := store.Initialize(ctx); err != nil {
		return fmt.Errorf("initializing database: %w", err)
	}

	opts := storage.QueryOptions{
		Directory: path,
		Limit:     queryLimit,
	}

	// Apply time filters
	if queryDays > 0 {
		since := time.Now().AddDate(0, 0, -queryDays)
		opts.Since = &since
	} else if querySince != "" {
		since, err := time.Parse("2006-01-02", querySince)
		if err != nil {
			return fmt.Errorf("invalid date format (use YYYY-MM-DD): %w", err)
		}
		opts.Since = &since
	}

	records, err := store.QueryUsage(ctx, opts)
	if err != nil {
		return fmt.Errorf("querying usage: %w", err)
	}

	if len(records) == 0 {
		fmt.Println("No records found")
		return nil
	}

	switch queryFormat {
	case "json":
		return outputJSON(records)
	default:
		return outputText(records)
	}
}

func outputText(records []storage.UsageRecord) error {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "TIMESTAMP\tSIZE\tCHANGE")
	fmt.Fprintln(w, "---------\t----\t------")

	for i, r := range records {
		change := "-"
		if i < len(records)-1 {
			prev := records[i+1]
			diff := r.SizeBytes - prev.SizeBytes
			if diff != 0 {
				sign := "+"
				if diff < 0 {
					sign = ""
				}
				change = fmt.Sprintf("%s%s", sign, formatSize(diff))
			}
		}
		fmt.Fprintf(w, "%s\t%s\t%s\n",
			r.RecordedAt.Local().Format("2006-01-02 15:04"),
			formatSize(r.SizeBytes),
			change,
		)
	}
	return w.Flush()
}

type jsonRecord struct {
	Timestamp  string `json:"timestamp"`
	SizeBytes  int64  `json:"size_bytes"`
	SizeHuman  string `json:"size_human"`
	ChangeFrom *int64 `json:"change_from,omitempty"`
}

func outputJSON(records []storage.UsageRecord) error {
	jsonRecords := make([]jsonRecord, len(records))
	for i, r := range records {
		jr := jsonRecord{
			Timestamp: r.RecordedAt.Format(time.RFC3339),
			SizeBytes: r.SizeBytes,
			SizeHuman: formatSize(r.SizeBytes),
		}
		if i < len(records)-1 {
			diff := r.SizeBytes - records[i+1].SizeBytes
			jr.ChangeFrom = &diff
		}
		jsonRecords[i] = jr
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(jsonRecords)
}
