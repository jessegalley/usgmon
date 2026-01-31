package scanner

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
)

// WalkStrategy uses filepath.WalkDir to calculate directory size.
type WalkStrategy struct {
	followSymlinks bool
}

// Name returns the strategy name.
func (s *WalkStrategy) Name() string {
	return "walk"
}

// GetSize traverses the directory tree and sums file sizes.
func (s *WalkStrategy) GetSize(ctx context.Context, path string) (int64, error) {
	if !s.followSymlinks {
		return s.walkNoFollow(ctx, path)
	}
	return s.walkFollowSymlinks(ctx, path)
}

// walkNoFollow uses the standard filepath.WalkDir which doesn't follow symlinks.
func (s *WalkStrategy) walkNoFollow(ctx context.Context, path string) (int64, error) {
	var totalSize int64

	err := filepath.WalkDir(path, func(p string, d fs.DirEntry, err error) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err != nil {
			return nil
		}

		if !d.IsDir() {
			info, err := d.Info()
			if err != nil {
				return nil
			}
			totalSize += info.Size()
		}

		return nil
	})

	if err != nil {
		return 0, err
	}

	return totalSize, nil
}

// walkFollowSymlinks implements a custom walk that follows symlinks with loop detection.
func (s *WalkStrategy) walkFollowSymlinks(ctx context.Context, path string) (int64, error) {
	visited := make(map[uint64]map[uint64]bool)
	var totalSize int64

	var walk func(string) error
	walk = func(dir string) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Check if we've visited this directory (by device+inode)
		var stat syscall.Stat_t
		if err := syscall.Stat(dir, &stat); err != nil {
			return nil // Skip directories we can't stat
		}
		if visited[stat.Dev] == nil {
			visited[stat.Dev] = make(map[uint64]bool)
		}
		if visited[stat.Dev][stat.Ino] {
			return nil // Already visited, skip to prevent loop
		}
		visited[stat.Dev][stat.Ino] = true

		entries, err := os.ReadDir(dir)
		if err != nil {
			return nil // Skip directories we can't read
		}

		for _, entry := range entries {
			entryPath := filepath.Join(dir, entry.Name())

			// Use os.Stat to follow symlinks
			info, err := os.Stat(entryPath)
			if err != nil {
				continue // Skip entries we can't stat
			}

			if info.IsDir() {
				if err := walk(entryPath); err != nil {
					return err
				}
			} else {
				totalSize += info.Size()
			}
		}

		return nil
	}

	if err := walk(path); err != nil {
		return 0, err
	}

	return totalSize, nil
}
