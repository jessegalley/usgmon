package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/jgalley/usgmon/internal/config"
	"github.com/jgalley/usgmon/internal/storage"
	"github.com/spf13/cobra"
)

var (
	topDays      int
	topSince     string
	topUntil     string
	topDirection string
	topMinChange string
	topLimit     int
	topFormat    string
)

var topCmd = &cobra.Command{
	Use:   "top <base-path>",
	Short: "Find directories with largest usage changes",
	Long: `Find directories with the largest disk usage changes over a time interval.

Examples:
  usgmon top /www/users --days 7
  usgmon top /www/users --direction increase --limit 5
  usgmon top /www/users --min-change 1G --format json
  usgmon top /www/users --since "2026-01-01" --until "2026-01-31"`,
	Args: cobra.ExactArgs(1),
	RunE: runTop,
}

func init() {
	topCmd.Flags().IntVar(&topDays, "days", 7, "look back N days from now")
	topCmd.Flags().StringVar(&topSince, "since", "", "start of time range (YYYY-MM-DD)")
	topCmd.Flags().StringVar(&topUntil, "until", "", "end of time range (YYYY-MM-DD)")
	topCmd.Flags().StringVar(&topDirection, "direction", "both", "filter: \"increase\", \"decrease\", \"both\"")
	topCmd.Flags().StringVar(&topMinChange, "min-change", "0", "minimum change threshold (e.g., \"100M\", \"1G\")")
	topCmd.Flags().IntVar(&topLimit, "limit", 10, "maximum results")
	topCmd.Flags().StringVar(&topFormat, "format", "text", "output format (text, json)")
}

func runTop(cmd *cobra.Command, args []string) error {
	basePath := filepath.Clean(args[0])

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

	// Parse time range
	var since, until time.Time
	if topSince != "" {
		since, err = time.Parse("2006-01-02", topSince)
		if err != nil {
			return fmt.Errorf("invalid --since date format (use YYYY-MM-DD): %w", err)
		}
	} else {
		since = time.Now().AddDate(0, 0, -topDays)
	}

	if topUntil != "" {
		until, err = time.Parse("2006-01-02", topUntil)
		if err != nil {
			return fmt.Errorf("invalid --until date format (use YYYY-MM-DD): %w", err)
		}
		// Set to end of day
		until = until.Add(24*time.Hour - time.Second)
	} else {
		until = time.Now()
	}

	// Parse min-change
	minChangeBytes, err := parseSize(topMinChange)
	if err != nil {
		return fmt.Errorf("invalid --min-change value: %w", err)
	}

	// Validate direction
	if topDirection != "increase" && topDirection != "decrease" && topDirection != "both" {
		return fmt.Errorf("invalid --direction value: must be \"increase\", \"decrease\", or \"both\"")
	}

	opts := storage.TopChangerOptions{
		BasePath:       basePath,
		Since:          since,
		Until:          until,
		Direction:      topDirection,
		MinChangeBytes: minChangeBytes,
		Limit:          topLimit,
	}

	changes, err := store.GetTopChangers(ctx, opts)
	if err != nil {
		return fmt.Errorf("querying top changers: %w", err)
	}

	if len(changes) == 0 {
		fmt.Println("No changes found")
		return nil
	}

	switch topFormat {
	case "json":
		return outputTopJSON(changes)
	default:
		return outputTopText(changes)
	}
}

func outputTopText(changes []storage.DirectoryChange) error {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "DIRECTORY\tBEFORE\tAFTER\tCHANGE\t%")
	fmt.Fprintln(w, "---------\t------\t-----\t------\t-")

	for _, c := range changes {
		sign := "+"
		if c.ChangeBytes < 0 {
			sign = ""
		}
		percentStr := fmt.Sprintf("%+.0f%%", c.ChangePercent)
		fmt.Fprintf(w, "%s\t%s\t%s\t%s%s\t%s\n",
			c.Directory,
			formatSize(c.StartSize),
			formatSize(c.EndSize),
			sign, formatSize(c.ChangeBytes),
			percentStr,
		)
	}
	return w.Flush()
}

type topJSONRecord struct {
	Directory       string  `json:"directory"`
	BasePath        string  `json:"base_path"`
	StartSize       int64   `json:"start_size_bytes"`
	StartSizeHuman  string  `json:"start_size_human"`
	EndSize         int64   `json:"end_size_bytes"`
	EndSizeHuman    string  `json:"end_size_human"`
	StartTime       string  `json:"start_time"`
	EndTime         string  `json:"end_time"`
	ChangeBytes     int64   `json:"change_bytes"`
	ChangeHuman     string  `json:"change_human"`
	ChangePercent   float64 `json:"change_percent"`
}

func outputTopJSON(changes []storage.DirectoryChange) error {
	records := make([]topJSONRecord, len(changes))
	for i, c := range changes {
		records[i] = topJSONRecord{
			Directory:      c.Directory,
			BasePath:       c.BasePath,
			StartSize:      c.StartSize,
			StartSizeHuman: formatSize(c.StartSize),
			EndSize:        c.EndSize,
			EndSizeHuman:   formatSize(c.EndSize),
			StartTime:      c.StartTime.Format(time.RFC3339),
			EndTime:        c.EndTime.Format(time.RFC3339),
			ChangeBytes:    c.ChangeBytes,
			ChangeHuman:    formatSize(c.ChangeBytes),
			ChangePercent:  c.ChangePercent,
		}
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(records)
}

// parseSize parses a human-readable size string (e.g., "100M", "1G") into bytes.
func parseSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" || s == "0" {
		return 0, nil
	}

	// Find where the number ends and the suffix begins
	var numStr string
	var suffix string
	for i, c := range s {
		if c < '0' || c > '9' {
			if c != '.' {
				numStr = s[:i]
				suffix = strings.ToUpper(strings.TrimSpace(s[i:]))
				break
			}
		}
	}
	if numStr == "" {
		numStr = s
	}

	num, err := strconv.ParseFloat(numStr, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid number: %s", numStr)
	}

	const (
		KiB = 1024
		MiB = KiB * 1024
		GiB = MiB * 1024
		TiB = GiB * 1024
	)

	var multiplier float64 = 1
	switch suffix {
	case "K", "KB", "KIB":
		multiplier = KiB
	case "M", "MB", "MIB":
		multiplier = MiB
	case "G", "GB", "GIB":
		multiplier = GiB
	case "T", "TB", "TIB":
		multiplier = TiB
	case "":
		multiplier = 1
	default:
		return 0, fmt.Errorf("unknown size suffix: %s", suffix)
	}

	return int64(num * multiplier), nil
}
