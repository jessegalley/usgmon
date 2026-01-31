package storage

import (
	"context"
	"time"
)

// UsageRecord represents a single disk usage measurement.
type UsageRecord struct {
	ID         int64
	BasePath   string
	Directory  string
	SizeBytes  int64
	RecordedAt time.Time
	ScanID     string
}

// Scan represents a scan operation.
type Scan struct {
	ScanID             string
	BasePath           string
	StartedAt          time.Time
	CompletedAt        *time.Time
	DirectoriesScanned int
	Status             string
}

// QueryOptions specifies filters for querying usage records.
type QueryOptions struct {
	Directory string
	BasePath  string
	Since     *time.Time
	Until     *time.Time
	Limit     int
}

// Storage defines the interface for persisting usage data.
type Storage interface {
	// Initialize prepares the storage (creates tables, etc.).
	Initialize(ctx context.Context) error

	// Close releases any resources held by the storage.
	Close() error

	// StartScan creates a new scan record and returns its ID.
	StartScan(ctx context.Context, basePath string) (string, error)

	// CompleteScan marks a scan as completed.
	CompleteScan(ctx context.Context, scanID string, directoriesScanned int) error

	// FailScan marks a scan as failed.
	FailScan(ctx context.Context, scanID string, reason string) error

	// RecordUsage stores a usage measurement.
	RecordUsage(ctx context.Context, record UsageRecord) error

	// RecordUsageBatch stores multiple usage measurements efficiently.
	RecordUsageBatch(ctx context.Context, records []UsageRecord) error

	// QueryUsage retrieves usage records matching the given options.
	QueryUsage(ctx context.Context, opts QueryOptions) ([]UsageRecord, error)

	// GetLatestUsage retrieves the most recent usage record for a directory.
	GetLatestUsage(ctx context.Context, directory string) (*UsageRecord, error)
}
