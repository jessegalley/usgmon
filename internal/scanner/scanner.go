package scanner

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

// visitedSet tracks visited directories by device+inode pairs to prevent loops.
type visitedSet map[uint64]map[uint64]bool

// seen checks if a path has been visited, and marks it as visited if not.
// Returns true if the path was already visited.
func (v visitedSet) seen(path string) (bool, error) {
	var stat syscall.Stat_t
	if err := syscall.Stat(path, &stat); err != nil {
		return false, err
	}
	if v[stat.Dev] == nil {
		v[stat.Dev] = make(map[uint64]bool)
	}
	if v[stat.Dev][stat.Ino] {
		return true, nil
	}
	v[stat.Dev][stat.Ino] = true
	return false, nil
}

// ScanOptions holds options for scanning operations.
type ScanOptions struct {
	FollowSymlinks bool
}

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
	return s.ScanPathWithOptions(ctx, basePath, depth, ScanOptions{})
}

// ScanPathWithOptions scans all directories at the given depth under basePath with options.
// If depth is 0, it scans basePath itself.
func (s *Scanner) ScanPathWithOptions(ctx context.Context, basePath string, depth int, opts ScanOptions) ([]Result, error) {
	dirs, err := s.getDirectoriesAtDepth(basePath, depth, opts)
	if err != nil {
		return nil, err
	}

	if len(dirs) == 0 {
		return nil, nil
	}

	// Determine strategy if not preset
	strategy := s.strategy
	if strategy == nil {
		strategy = DetectStrategy(basePath, opts.FollowSymlinks)
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

// ScanPathStreaming scans directories and sends results to a channel as they complete.
// The channel is closed when scanning is done. Caller should check ctx.Err() after
// the channel closes to determine if the scan completed successfully or was cancelled.
//
// This implementation uses streaming enumeration: intermediate directory levels (0 to depth-1)
// are enumerated synchronously (typically small), then level N directories are streamed
// directly to workers as they're discovered. This allows workers to start processing
// immediately rather than waiting for all directories to be enumerated first.
func (s *Scanner) ScanPathStreaming(ctx context.Context, basePath string, depth int, opts ScanOptions) (<-chan Result, error) {
	// Validate basePath upfront
	info, err := os.Stat(basePath)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		resultCh := make(chan Result)
		close(resultCh)
		return resultCh, nil
	}

	// Determine strategy
	strategy := s.strategy
	if strategy == nil {
		strategy = DetectStrategy(basePath, opts.FollowSymlinks)
	}

	// Bounded channels - no pre-sizing to len(dirs)
	dirCh := make(chan string, s.workers*4)
	resultCh := make(chan Result, s.workers*2)

	// Start enumerator goroutine FIRST
	go func() {
		s.streamDirectoriesAtDepth(ctx, basePath, depth, opts, dirCh)
	}()

	// Start workers immediately - they begin as soon as dirs arrive
	go func() {
		defer close(resultCh)
		var wg sync.WaitGroup
		for i := 0; i < s.workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for dir := range dirCh {
					start := time.Now()
					size, err := strategy.GetSize(ctx, dir)
					select {
					case resultCh <- Result{
						Path:      dir,
						SizeBytes: size,
						Error:     err,
						Duration:  time.Since(start),
					}:
					case <-ctx.Done():
						return
					}
				}
			}()
		}
		wg.Wait()
	}()

	return resultCh, nil
}

// ScanSingle scans a single directory and returns its size.
func (s *Scanner) ScanSingle(ctx context.Context, path string) (Result, error) {
	return s.ScanSingleWithOptions(ctx, path, ScanOptions{})
}

// ScanSingleWithOptions scans a single directory and returns its size with options.
func (s *Scanner) ScanSingleWithOptions(ctx context.Context, path string, opts ScanOptions) (Result, error) {
	strategy := s.strategy
	if strategy == nil {
		strategy = DetectStrategy(path, opts.FollowSymlinks)
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
func (s *Scanner) getDirectoriesAtDepth(basePath string, depth int, opts ScanOptions) ([]string, error) {
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

	visited := make(visitedSet)
	// Mark the base path as visited
	if _, err := visited.seen(basePath); err != nil {
		return nil, err
	}

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
				entryPath := filepath.Join(dir, entry.Name())

				if isSymlink(entry) {
					if !opts.FollowSymlinks {
						continue
					}
					// Follow symlink and check if it points to a directory
					targetInfo, err := os.Stat(entryPath)
					if err != nil {
						// Broken symlink or permission error
						continue
					}
					if !targetInfo.IsDir() {
						continue
					}
					// Check for loops
					alreadySeen, err := visited.seen(entryPath)
					if err != nil || alreadySeen {
						continue
					}
					nextLevel = append(nextLevel, entryPath)
				} else if entry.IsDir() {
					// Check for loops (even for non-symlinks, in case of bind mounts)
					alreadySeen, err := visited.seen(entryPath)
					if err != nil || alreadySeen {
						continue
					}
					nextLevel = append(nextLevel, entryPath)
				}
			}
		}
		currentLevel = nextLevel
	}

	return currentLevel, nil
}

// streamDirectoriesAtDepth enumerates directories at the specified depth and streams them
// to dirCh as they're discovered. Levels 0 to depth-1 are enumerated synchronously (small),
// then level N directories are streamed directly to the channel.
// The channel is closed when enumeration completes or context is cancelled.
func (s *Scanner) streamDirectoriesAtDepth(ctx context.Context, basePath string, depth int, opts ScanOptions, dirCh chan<- string) {
	defer close(dirCh)

	// Handle depth 0: just send basePath
	if depth == 0 {
		select {
		case dirCh <- basePath:
		case <-ctx.Done():
		}
		return
	}

	visited := make(visitedSet)
	// Mark the base path as visited
	if _, err := visited.seen(basePath); err != nil {
		return
	}

	// Enumerate levels 0 to depth-1 synchronously (these are typically small)
	currentLevel := []string{basePath}
	for d := 0; d < depth-1; d++ {
		var nextLevel []string
		for _, dir := range currentLevel {
			select {
			case <-ctx.Done():
				return
			default:
			}

			entries, err := os.ReadDir(dir)
			if err != nil {
				// Skip directories we can't read
				continue
			}
			for _, entry := range entries {
				entryPath := filepath.Join(dir, entry.Name())

				if isSymlink(entry) {
					if !opts.FollowSymlinks {
						continue
					}
					// Follow symlink and check if it points to a directory
					targetInfo, err := os.Stat(entryPath)
					if err != nil {
						continue
					}
					if !targetInfo.IsDir() {
						continue
					}
					alreadySeen, err := visited.seen(entryPath)
					if err != nil || alreadySeen {
						continue
					}
					nextLevel = append(nextLevel, entryPath)
				} else if entry.IsDir() {
					alreadySeen, err := visited.seen(entryPath)
					if err != nil || alreadySeen {
						continue
					}
					nextLevel = append(nextLevel, entryPath)
				}
			}
		}
		currentLevel = nextLevel
	}

	// Stream the final level (level N) directly to the channel as directories are discovered
	for _, dir := range currentLevel {
		select {
		case <-ctx.Done():
			return
		default:
		}

		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, entry := range entries {
			entryPath := filepath.Join(dir, entry.Name())

			var shouldSend bool

			if isSymlink(entry) {
				if !opts.FollowSymlinks {
					continue
				}
				targetInfo, err := os.Stat(entryPath)
				if err != nil {
					continue
				}
				if !targetInfo.IsDir() {
					continue
				}
				alreadySeen, err := visited.seen(entryPath)
				if err != nil || alreadySeen {
					continue
				}
				shouldSend = true
			} else if entry.IsDir() {
				alreadySeen, err := visited.seen(entryPath)
				if err != nil || alreadySeen {
					continue
				}
				shouldSend = true
			}

			if shouldSend {
				select {
				case dirCh <- entryPath:
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

// isSymlink checks if a directory entry is a symbolic link.
func isSymlink(entry fs.DirEntry) bool {
	return entry.Type()&fs.ModeSymlink != 0
}
