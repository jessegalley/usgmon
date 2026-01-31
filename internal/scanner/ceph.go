package scanner

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"

	"golang.org/x/sys/unix"
)

// CephStrategy reads directory size from CephFS xattr.
type CephStrategy struct {
	followSymlinks bool
}

// Name returns the strategy name.
func (s *CephStrategy) Name() string {
	return "ceph"
}

// GetSize reads the ceph.dir.rbytes xattr to get directory size.
func (s *CephStrategy) GetSize(ctx context.Context, path string) (int64, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	// Resolve symlinks first if configured
	resolvedPath := path
	if s.followSymlinks {
		var err error
		resolvedPath, err = filepath.EvalSymlinks(path)
		if err != nil {
			return 0, fmt.Errorf("resolving symlink: %w", err)
		}
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
