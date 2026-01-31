package scanner

import (
	"context"
	"fmt"
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
func (s *CephStrategy) GetSize(ctx context.Context, path string) (int64, error) {
	// Check for context cancellation
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	buf := make([]byte, 64)
	sz, err := unix.Getxattr(path, "ceph.dir.rbytes", buf)
	if err != nil {
		return 0, fmt.Errorf("reading ceph.dir.rbytes xattr: %w", err)
	}

	size, err := strconv.ParseInt(string(buf[:sz]), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parsing xattr value %q: %w", string(buf[:sz]), err)
	}

	return size, nil
}
