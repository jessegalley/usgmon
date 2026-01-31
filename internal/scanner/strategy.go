package scanner

import (
	"context"
	"os/exec"
	"syscall"
)

// Strategy defines the interface for directory size calculation methods.
type Strategy interface {
	// Name returns the strategy name for logging.
	Name() string

	// GetSize returns the total size in bytes of the given directory.
	GetSize(ctx context.Context, path string) (int64, error)
}

// CephFSMagic is the filesystem magic number for CephFS.
const CephFSMagic = 0x00c36400

// DetectStrategy returns the best available strategy for the given path.
func DetectStrategy(path string, followSymlinks bool) Strategy {
	if isCephFS(path) {
		return &CephStrategy{followSymlinks: followSymlinks}
	}

	if duPath, err := exec.LookPath("du"); err == nil {
		return &DuStrategy{duPath: duPath, followSymlinks: followSymlinks}
	}

	return &WalkStrategy{followSymlinks: followSymlinks}
}

// isCephFS checks if the path is on a CephFS filesystem.
func isCephFS(path string) bool {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return false
	}
	return stat.Type == CephFSMagic
}
