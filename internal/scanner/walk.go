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
func (s *WalkStrategy) GetSize(ctx context.Context, path string) (int64, error) {
	var totalSize int64

	err := filepath.WalkDir(path, func(p string, d fs.DirEntry, err error) error {
		// Check for context cancellation periodically
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err != nil {
			// Skip files we can't access
			return nil
		}

		if !d.IsDir() {
			info, err := d.Info()
			if err != nil {
				// Skip files we can't stat
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
