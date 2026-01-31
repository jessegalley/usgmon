package scanner

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

// DuStrategy uses the du command to calculate directory size.
type DuStrategy struct {
	duPath string
}

// Name returns the strategy name.
func (s *DuStrategy) Name() string {
	return "du"
}

// GetSize executes du -sb to get directory size.
// Note: du without -L follows the argument symlink (if path is a symlink) but does
// not follow symlinks inside the directory. This is the desired behavior - we want
// to calculate size of symlinked directories at target depth, but not traverse
// broken or circular symlinks inside them.
func (s *DuStrategy) GetSize(ctx context.Context, path string) (int64, error) {
	args := []string{"-sb", path}
	cmd := exec.CommandContext(ctx, s.duPath, args...)
	output, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return 0, fmt.Errorf("du failed: %s", string(exitErr.Stderr))
		}
		return 0, fmt.Errorf("executing du: %w", err)
	}

	// Output format: "12345\t/path/to/dir\n"
	fields := strings.Fields(string(output))
	if len(fields) < 1 {
		return 0, fmt.Errorf("unexpected du output: %q", string(output))
	}

	size, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parsing du output %q: %w", fields[0], err)
	}

	return size, nil
}
