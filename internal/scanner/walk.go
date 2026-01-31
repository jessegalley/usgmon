package scanner

import (
	"context"
	"io/fs"
	"path/filepath"
)

// WalkStrategy uses filepath.WalkDir to calculate directory size.
type WalkStrategy struct{}

// Name returns the strategy name.
func (s *WalkStrategy) Name() string {
	return "walk"
}

// GetSize traverses the directory tree and sums file sizes.
// Note: This resolves the path first (in case it's a symlink to a directory),
// then walks without following symlinks inside. This allows calculating size of
// symlinked directories at target depth without traversing broken or circular
// symlinks inside them.
func (s *WalkStrategy) GetSize(ctx context.Context, path string) (int64, error) {
	// Resolve the path in case it's a symlink to a directory
	resolvedPath, err := filepath.EvalSymlinks(path)
	if err != nil {
		// If we can't resolve, try the original path
		resolvedPath = path
	}
	return s.walkNoFollow(ctx, resolvedPath)
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

