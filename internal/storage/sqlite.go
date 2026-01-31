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
		CREATE INDEX IF NOT EXISTS idx_usage_base_path_time ON usage_records(base_path, recorded_at, directory, size_bytes);
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

// GetTopChangers finds directories with the largest usage changes over a time interval.
func (s *SQLiteStorage) GetTopChangers(ctx context.Context, opts TopChangerOptions) ([]DirectoryChange, error) {
	// Normalize base path: remove trailing slash for consistent comparison
	basePath := opts.BasePath
	if len(basePath) > 1 && basePath[len(basePath)-1] == '/' {
		basePath = basePath[:len(basePath)-1]
	}

	query := `
		WITH ranked AS (
			SELECT
				directory,
				base_path,
				size_bytes,
				recorded_at,
				ROW_NUMBER() OVER (PARTITION BY directory ORDER BY recorded_at ASC) AS rn_first,
				ROW_NUMBER() OVER (PARTITION BY directory ORDER BY recorded_at DESC) AS rn_last
			FROM usage_records
			WHERE (base_path = ? OR base_path = ? || '/')
			  AND recorded_at BETWEEN ? AND ?
		),
		changes AS (
			SELECT
				r1.directory,
				r1.base_path,
				r1.size_bytes AS start_size,
				r1.recorded_at AS start_time,
				r2.size_bytes AS end_size,
				r2.recorded_at AS end_time
			FROM ranked r1
			JOIN ranked r2 ON r1.directory = r2.directory
			WHERE r1.rn_first = 1 AND r2.rn_last = 1
		)
		SELECT
			directory, base_path, start_size, end_size, start_time, end_time,
			(end_size - start_size) AS change_bytes,
			CASE WHEN start_size > 0 THEN ROUND(100.0 * (end_size - start_size) / start_size, 2) ELSE 0 END AS change_percent
		FROM changes
		WHERE ABS(end_size - start_size) >= ?
		  AND (? = 'both' OR (? = 'increase' AND end_size > start_size) OR (? = 'decrease' AND end_size < start_size))
		ORDER BY ABS(end_size - start_size) DESC
		LIMIT ?;
	`

	rows, err := s.db.QueryContext(ctx, query,
		basePath,
		basePath,
		opts.Since.UTC(),
		opts.Until.UTC(),
		opts.MinChangeBytes,
		opts.Direction,
		opts.Direction,
		opts.Direction,
		opts.Limit,
	)
	if err != nil {
		return nil, fmt.Errorf("querying top changers: %w", err)
	}
	defer rows.Close()

	var results []DirectoryChange
	for rows.Next() {
		var dc DirectoryChange
		if err := rows.Scan(
			&dc.Directory,
			&dc.BasePath,
			&dc.StartSize,
			&dc.EndSize,
			&dc.StartTime,
			&dc.EndTime,
			&dc.ChangeBytes,
			&dc.ChangePercent,
		); err != nil {
			return nil, fmt.Errorf("scanning row: %w", err)
		}
		results = append(results, dc)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating rows: %w", err)
	}

	return results, nil
}
