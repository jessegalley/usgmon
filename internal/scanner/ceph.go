package scanner

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"

	"golang.org/x/sys/unix"
)

// CephStrategy reads directory size from CephFS xattr.
type CephStrategy struct{}

// Name returns the strategy name.
func (s *CephStrategy) Name() string {
	return "ceph"
}

// GetSize reads the ceph.dir.rbytes xattr to get directory size.
// Note: This always resolves the path first (in case it's a symlink to a directory),
// allowing size calculation for symlinked directories at target depth.
func (s *CephStrategy) GetSize(ctx context.Context, path string) (int64, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	// Resolve symlinks - the target directory at depth N may be a symlink
	resolvedPath, err := filepath.EvalSymlinks(path)
	if err != nil {
		// If we can't resolve, try the original path
		resolvedPath = path
	}

	buf := make([]byte, 64)
	sz, err := unix.Getxattr(resolvedPath, "ceph.dir.rbytes", buf)
	if err != nil {
		return 0, fmt.Errorf("reading ceph.dir.rbytes xattr: %w", err)
	}

	size, err := strconv.ParseInt(string(buf[:sz]), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parsing xattr value %q: %w", string(buf[:sz]), err)
	}

	return size, nil
}
