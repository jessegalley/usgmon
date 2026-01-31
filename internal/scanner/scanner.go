package scanner

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Result represents the result of scanning a single directory.
type Result struct {
	Path      string
	SizeBytes int64
	Error     error
	Duration  time.Duration
}

// Scanner orchestrates directory size scanning with a worker pool.
type Scanner struct {
	workers  int
	strategy Strategy
}

// New creates a new Scanner with the specified number of workers.
// If strategy is nil, it will be auto-detected per scan.
func New(workers int, strategy Strategy) *Scanner {
	if workers < 1 {
		workers = 1
	}
	return &Scanner{
		workers:  workers,
		strategy: strategy,
	}
}

// ScanPath scans all directories at the given depth under basePath.
// If depth is 0, it scans basePath itself.
func (s *Scanner) ScanPath(ctx context.Context, basePath string, depth int) ([]Result, error) {
	dirs, err := s.getDirectoriesAtDepth(basePath, depth)
	if err != nil {
		return nil, err
	}

	if len(dirs) == 0 {
		return nil, nil
	}

	// Determine strategy if not preset
	strategy := s.strategy
	if strategy == nil {
		strategy = DetectStrategy(basePath)
	}

	workCh := make(chan string, len(dirs))
	resultCh := make(chan Result, len(dirs))

	// Spawn worker pool
	var wg sync.WaitGroup
	for i := 0; i < s.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for dir := range workCh {
				start := time.Now()
				size, err := strategy.GetSize(ctx, dir)
				resultCh <- Result{
					Path:      dir,
					SizeBytes: size,
					Error:     err,
					Duration:  time.Since(start),
				}
			}
		}()
	}

	// Send work
	for _, dir := range dirs {
		select {
		case workCh <- dir:
		case <-ctx.Done():
			close(workCh)
			// Drain remaining results
			go func() { wg.Wait(); close(resultCh) }()
			var results []Result
			for r := range resultCh {
				results = append(results, r)
			}
			return results, ctx.Err()
		}
	}
	close(workCh)

	// Collect results
	go func() { wg.Wait(); close(resultCh) }()

	var results []Result
	for r := range resultCh {
		results = append(results, r)
	}

	return results, nil
}

// ScanSingle scans a single directory and returns its size.
func (s *Scanner) ScanSingle(ctx context.Context, path string) (Result, error) {
	strategy := s.strategy
	if strategy == nil {
		strategy = DetectStrategy(path)
	}

	start := time.Now()
	size, err := strategy.GetSize(ctx, path)
	return Result{
		Path:      path,
		SizeBytes: size,
		Error:     err,
		Duration:  time.Since(start),
	}, nil
}

// Strategy returns the scanner's strategy name.
func (s *Scanner) Strategy() string {
	if s.strategy != nil {
		return s.strategy.Name()
	}
	return "auto"
}

// getDirectoriesAtDepth returns all directories at exactly the specified depth.
// Depth 0 returns just the basePath itself (if it's a directory).
// Depth 1 returns immediate subdirectories, etc.
func (s *Scanner) getDirectoriesAtDepth(basePath string, depth int) ([]string, error) {
	info, err := os.Stat(basePath)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, nil
	}

	if depth == 0 {
		return []string{basePath}, nil
	}

	var dirs []string
	currentLevel := []string{basePath}

	for d := 0; d < depth; d++ {
		var nextLevel []string
		for _, dir := range currentLevel {
			entries, err := os.ReadDir(dir)
			if err != nil {
				// Skip directories we can't read
				continue
			}
			for _, entry := range entries {
				if entry.IsDir() && !isSymlink(entry) {
					nextLevel = append(nextLevel, filepath.Join(dir, entry.Name()))
				}
			}
		}
		currentLevel = nextLevel
	}

	dirs = currentLevel
	return dirs, nil
}

// isSymlink checks if a directory entry is a symbolic link.
func isSymlink(entry fs.DirEntry) bool {
	return entry.Type()&fs.ModeSymlink != 0
}
