package storage

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

// SQLiteStorage implements Storage using SQLite.
type SQLiteStorage struct {
	db *sql.DB
}

// NewSQLiteStorage creates a new SQLite storage instance.
func NewSQLiteStorage(dbPath string) (*SQLiteStorage, error) {
	// Ensure parent directory exists
	if err := os.MkdirAll(filepath.Dir(dbPath), 0755); err != nil {
		return nil, fmt.Errorf("creating database directory: %w", err)
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}

	// Enable WAL mode for better concurrent access
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("enabling WAL mode: %w", err)
	}

	// Enable foreign keys
	if _, err := db.Exec("PRAGMA foreign_keys=ON"); err != nil {
		db.Close()
		return nil, fmt.Errorf("enabling foreign keys: %w", err)
	}

	return &SQLiteStorage{db: db}, nil
}

// Initialize creates the database schema.
func (s *SQLiteStorage) Initialize(ctx context.Context) error {
	schema := `
		CREATE TABLE IF NOT EXISTS scans (
			scan_id TEXT PRIMARY KEY,
			base_path TEXT NOT NULL,
			started_at DATETIME NOT NULL,
			completed_at DATETIME,
			directories_scanned INTEGER DEFAULT 0,
			status TEXT DEFAULT 'running'
		);

		CREATE TABLE IF NOT EXISTS usage_records (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			base_path TEXT NOT NULL,
			directory TEXT NOT NULL,
			size_bytes INTEGER NOT NULL,
			recorded_at DATETIME NOT NULL,
			scan_id TEXT NOT NULL,
			FOREIGN KEY (scan_id) REFERENCES scans(scan_id)
		);

		CREATE INDEX IF NOT EXISTS idx_usage_dir_time ON usage_records(directory, recorded_at);
		CREATE INDEX IF NOT EXISTS idx_usage_base_path ON usage_records(base_path);
		CREATE INDEX IF NOT EXISTS idx_usage_scan_id ON usage_records(scan_id);
	`

	_, err := s.db.ExecContext(ctx, schema)
	if err != nil {
		return fmt.Errorf("creating schema: %w", err)
	}

	return nil
}

// Close closes the database connection.
func (s *SQLiteStorage) Close() error {
	return s.db.Close()
}

// StartScan creates a new scan record.
func (s *SQLiteStorage) StartScan(ctx context.Context, basePath string) (string, error) {
	scanID := uuid.New().String()
	now := time.Now().UTC()

	_, err := s.db.ExecContext(ctx,
		`INSERT INTO scans (scan_id, base_path, started_at, status) VALUES (?, ?, ?, 'running')`,
		scanID, basePath, now,
	)
	if err != nil {
		return "", fmt.Errorf("inserting scan record: %w", err)
	}

	return scanID, nil
}

// CompleteScan marks a scan as completed.
func (s *SQLiteStorage) CompleteScan(ctx context.Context, scanID string, directoriesScanned int) error {
	now := time.Now().UTC()

	_, err := s.db.ExecContext(ctx,
		`UPDATE scans SET completed_at = ?, directories_scanned = ?, status = 'completed' WHERE scan_id = ?`,
		now, directoriesScanned, scanID,
	)
	if err != nil {
		return fmt.Errorf("completing scan: %w", err)
	}

	return nil
}

// FailScan marks a scan as failed.
func (s *SQLiteStorage) FailScan(ctx context.Context, scanID string, reason string) error {
	now := time.Now().UTC()

	_, err := s.db.ExecContext(ctx,
		`UPDATE scans SET completed_at = ?, status = ? WHERE scan_id = ?`,
		now, "failed: "+reason, scanID,
	)
	if err != nil {
		return fmt.Errorf("failing scan: %w", err)
	}

	return nil
}

// RecordUsage stores a single usage measurement.
func (s *SQLiteStorage) RecordUsage(ctx context.Context, record UsageRecord) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO usage_records (base_path, directory, size_bytes, recorded_at, scan_id)
		 VALUES (?, ?, ?, ?, ?)`,
		record.BasePath, record.Directory, record.SizeBytes, record.RecordedAt, record.ScanID,
	)
	if err != nil {
		return fmt.Errorf("inserting usage record: %w", err)
	}

	return nil
}

// RecordUsageBatch stores multiple usage measurements in a single transaction.
func (s *SQLiteStorage) RecordUsageBatch(ctx context.Context, records []UsageRecord) error {
	if len(records) == 0 {
		return nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("starting transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO usage_records (base_path, directory, size_bytes, recorded_at, scan_id)
		 VALUES (?, ?, ?, ?, ?)`,
	)
	if err != nil {
		return fmt.Errorf("preparing statement: %w", err)
	}
	defer stmt.Close()

	for _, record := range records {
		_, err := stmt.ExecContext(ctx,
			record.BasePath, record.Directory, record.SizeBytes, record.RecordedAt, record.ScanID,
		)
		if err != nil {
			return fmt.Errorf("inserting record for %s: %w", record.Directory, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	return nil
}

// QueryUsage retrieves usage records matching the given options.
func (s *SQLiteStorage) QueryUsage(ctx context.Context, opts QueryOptions) ([]UsageRecord, error) {
	query := `SELECT id, base_path, directory, size_bytes, recorded_at, scan_id
		      FROM usage_records WHERE 1=1`
	args := []interface{}{}

	if opts.Directory != "" {
		query += " AND directory = ?"
		args = append(args, opts.Directory)
	}

	if opts.BasePath != "" {
		query += " AND base_path = ?"
		args = append(args, opts.BasePath)
	}

	if opts.Since != nil {
		query += " AND recorded_at >= ?"
		args = append(args, *opts.Since)
	}

	if opts.Until != nil {
		query += " AND recorded_at <= ?"
		args = append(args, *opts.Until)
	}

	query += " ORDER BY recorded_at DESC"

	if opts.Limit > 0 {
		query += " LIMIT ?"
		args = append(args, opts.Limit)
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("querying usage: %w", err)
	}
	defer rows.Close()

	var records []UsageRecord
	for rows.Next() {
		var r UsageRecord
		if err := rows.Scan(&r.ID, &r.BasePath, &r.Directory, &r.SizeBytes, &r.RecordedAt, &r.ScanID); err != nil {
			return nil, fmt.Errorf("scanning row: %w", err)
		}
		records = append(records, r)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating rows: %w", err)
	}

	return records, nil
}

// GetLatestUsage retrieves the most recent usage record for a directory.
func (s *SQLiteStorage) GetLatestUsage(ctx context.Context, directory string) (*UsageRecord, error) {
	var r UsageRecord
	err := s.db.QueryRowContext(ctx,
		`SELECT id, base_path, directory, size_bytes, recorded_at, scan_id
		 FROM usage_records
		 WHERE directory = ?
		 ORDER BY recorded_at DESC
		 LIMIT 1`,
		directory,
	).Scan(&r.ID, &r.BasePath, &r.Directory, &r.SizeBytes, &r.RecordedAt, &r.ScanID)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("querying latest usage: %w", err)
	}

	return &r, nil
}
