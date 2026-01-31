package daemon

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/jgalley/usgmon/internal/config"
	"github.com/jgalley/usgmon/internal/scanner"
	"github.com/jgalley/usgmon/internal/storage"
)

// Daemon manages periodic directory scanning.
type Daemon struct {
	cfg     *config.Config
	storage storage.Storage
	scanner *scanner.Scanner
	logger  *slog.Logger

	mu       sync.Mutex
	running  bool
	stopCh   chan struct{}
	doneCh   chan struct{}
	scanners map[string]context.CancelFunc // active scans
}

// New creates a new Daemon instance.
func New(cfg *config.Config, store storage.Storage, logger *slog.Logger) *Daemon {
	return &Daemon{
		cfg:      cfg,
		storage:  store,
		scanner:  scanner.New(cfg.Scan.Workers, nil), // auto-detect strategy
		logger:   logger,
		scanners: make(map[string]context.CancelFunc),
	}
}

// Run starts the daemon and blocks until Stop is called or the context is cancelled.
func (d *Daemon) Run(ctx context.Context) error {
	d.mu.Lock()
	if d.running {
		d.mu.Unlock()
		return nil
	}
	d.running = true
	d.stopCh = make(chan struct{})
	d.doneCh = make(chan struct{})
	d.mu.Unlock()

	defer func() {
		d.mu.Lock()
		d.running = false
		close(d.doneCh)
		d.mu.Unlock()
	}()

	if len(d.cfg.Paths) == 0 {
		d.logger.Warn("no paths configured for monitoring")
		<-ctx.Done()
		return ctx.Err()
	}

	// Start a timer for each configured path
	var wg sync.WaitGroup
	pathCtx, pathCancel := context.WithCancel(ctx)
	defer pathCancel()

	for _, p := range d.cfg.Paths {
		wg.Add(1)
		go func(pathCfg config.PathConfig) {
			defer wg.Done()
			d.runPathScanner(pathCtx, pathCfg)
		}(p)
	}

	// Wait for shutdown signal
	select {
	case <-ctx.Done():
		d.logger.Info("context cancelled, shutting down")
	case <-d.stopCh:
		d.logger.Info("stop requested, shutting down")
	}

	// Cancel all path scanners and wait
	pathCancel()
	wg.Wait()

	// Wait for any in-progress scans to complete
	d.waitForScans()

	return nil
}

// Stop signals the daemon to stop gracefully.
func (d *Daemon) Stop() {
	d.mu.Lock()
	if d.running && d.stopCh != nil {
		close(d.stopCh)
	}
	d.mu.Unlock()
}

// Wait blocks until the daemon has fully stopped.
func (d *Daemon) Wait() {
	d.mu.Lock()
	doneCh := d.doneCh
	d.mu.Unlock()

	if doneCh != nil {
		<-doneCh
	}
}

// runPathScanner runs the scan loop for a single path configuration.
func (d *Daemon) runPathScanner(ctx context.Context, pathCfg config.PathConfig) {
	interval := pathCfg.EffectiveInterval(d.cfg.Scan.Interval)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	d.logger.Info("starting path scanner",
		"path", pathCfg.Path,
		"depth", pathCfg.Depth,
		"interval", interval,
		"follow_symlinks", pathCfg.FollowSymlinks,
	)

	// Run initial scan immediately
	d.runScan(ctx, pathCfg)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.runScan(ctx, pathCfg)
		}
	}
}

// runScan performs a single scan of the configured path.
func (d *Daemon) runScan(ctx context.Context, pathCfg config.PathConfig) {
	scanCtx, cancel := context.WithCancel(ctx)

	// Register this scan
	d.mu.Lock()
	d.scanners[pathCfg.Path] = cancel
	d.mu.Unlock()

	defer func() {
		d.mu.Lock()
		delete(d.scanners, pathCfg.Path)
		d.mu.Unlock()
		cancel()
	}()

	d.logger.Info("starting scan",
		"path", pathCfg.Path,
		"depth", pathCfg.Depth,
	)

	// Create scan record
	scanID, err := d.storage.StartScan(scanCtx, pathCfg.Path)
	if err != nil {
		d.logger.Error("failed to create scan record", "error", err)
		return
	}

	// Perform the scan
	opts := scanner.ScanOptions{
		FollowSymlinks: pathCfg.FollowSymlinks,
	}
	results, err := d.scanner.ScanPathWithOptions(scanCtx, pathCfg.Path, pathCfg.Depth, opts)
	if err != nil {
		d.logger.Error("scan failed", "path", pathCfg.Path, "error", err)
		if err := d.storage.FailScan(context.Background(), scanID, err.Error()); err != nil {
			d.logger.Error("failed to mark scan as failed", "error", err)
		}
		return
	}

	// Store results
	now := time.Now().UTC()
	records := make([]storage.UsageRecord, 0, len(results))
	for _, r := range results {
		if r.Error != nil {
			d.logger.Warn("scan error for directory",
				"directory", r.Path,
				"error", r.Error,
			)
			continue
		}
		records = append(records, storage.UsageRecord{
			BasePath:   pathCfg.Path,
			Directory:  r.Path,
			SizeBytes:  r.SizeBytes,
			RecordedAt: now,
			ScanID:     scanID,
		})
	}

	if err := d.storage.RecordUsageBatch(scanCtx, records); err != nil {
		d.logger.Error("failed to store results", "error", err)
		if err := d.storage.FailScan(context.Background(), scanID, err.Error()); err != nil {
			d.logger.Error("failed to mark scan as failed", "error", err)
		}
		return
	}

	if err := d.storage.CompleteScan(scanCtx, scanID, len(records)); err != nil {
		d.logger.Error("failed to complete scan", "error", err)
		return
	}

	d.logger.Info("scan completed",
		"path", pathCfg.Path,
		"directories", len(records),
		"strategy", d.scanner.Strategy(),
	)
}

// waitForScans waits for all in-progress scans to complete.
func (d *Daemon) waitForScans() {
	d.mu.Lock()
	count := len(d.scanners)
	d.mu.Unlock()

	if count == 0 {
		return
	}

	d.logger.Info("waiting for in-progress scans to complete", "count", count)

	// Poll until all scans complete (with timeout)
	timeout := time.After(30 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			d.logger.Warn("timeout waiting for scans, forcing shutdown")
			d.mu.Lock()
			for _, cancel := range d.scanners {
				cancel()
			}
			d.mu.Unlock()
			return
		case <-ticker.C:
			d.mu.Lock()
			count := len(d.scanners)
			d.mu.Unlock()
			if count == 0 {
				return
			}
		}
	}
}
