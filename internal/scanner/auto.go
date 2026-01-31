package scanner

import (
	"context"
	"os/exec"
	"path/filepath"
)

// AutoStrategy detects the best strategy per-directory.
// This handles cases where symlinks cross filesystem boundaries
// (e.g., base path on ext4 but symlink target on CephFS).
type AutoStrategy struct {
	duPath string
	hasDu  bool
}

// NewAutoStrategy creates an AutoStrategy that will detect per-directory.
func NewAutoStrategy() *AutoStrategy {
	duPath, err := exec.LookPath("du")
	return &AutoStrategy{
		duPath: duPath,
		hasDu:  err == nil,
	}
}

// Name returns the strategy name.
func (s *AutoStrategy) Name() string {
	return "auto"
}

// StrategyFor returns the appropriate strategy for a specific path.
// This resolves symlinks and checks the actual filesystem type.
func (s *AutoStrategy) StrategyFor(path string) Strategy {
	// Resolve symlinks first to check the actual filesystem
	resolvedPath, err := filepath.EvalSymlinks(path)
	if err != nil {
		resolvedPath = path
	}

	// Check if this specific directory is on CephFS
	if isCephFS(resolvedPath) {
		return &CephStrategy{}
	}

	// Fall back to du or walk
	if s.hasDu {
		return &DuStrategy{duPath: s.duPath}
	}

	return &WalkStrategy{}
}

// GetSize detects the filesystem type for this specific path and uses
// the appropriate strategy.
func (s *AutoStrategy) GetSize(ctx context.Context, path string) (int64, error) {
	return s.StrategyFor(path).GetSize(ctx, path)
}
