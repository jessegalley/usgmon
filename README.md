# usgmon

A Go daemon that periodically monitors disk usage of directories at configurable depths and stores historical data in SQLite for trend analysis.

## Features

- Monitor filesystem paths to a specific depth
- Track total disk usage of each directory at that depth
- Store usage data with timestamps for historical analysis
- Support multiple monitored paths with different depths and intervals
- Query historical changes over time
- Worker pool for parallel size counting
- Multiple scanning strategies with automatic detection:
  - **CephFS**: Reads `ceph.dir.rbytes` xattr (instant, no traversal)
  - **du**: Executes `du -sb` command
  - **Walk**: Manual `filepath.WalkDir` fallback

## Installation

### From Source

Requires Go 1.22 or later.

```bash
git clone https://github.com/jgalley/usgmon.git
cd usgmon
make build
```

The binary will be at `bin/usgmon`.

### System Installation

```bash
sudo make install
```

This installs:
- Binary to `/usr/local/bin/usgmon`
- Example config to `/etc/usgmon/usgmon.yaml.example`
- Systemd unit to `/etc/systemd/system/usgmon.service`
- Data directory at `/var/lib/usgmon/`

## Usage

### One-Shot Scan

Scan a single directory:

```bash
usgmon scan /www/users/bob.com
# Output: /www/users/bob.com    1.2 GiB
```

Scan subdirectories at a specific depth:

```bash
usgmon scan /www/users --depth 1
# Output:
# /www/users/alice.com    523 MiB
# /www/users/bob.com      1.2 GiB
# /www/users/carol.com    89 MiB
```

Scan and store results to the database:

```bash
usgmon scan /www/users --depth 1 --store --config /etc/usgmon/usgmon.yaml
```

### Query Historical Data

View usage history for a directory:

```bash
usgmon query /www/users/bob.com
# Output:
# TIMESTAMP         SIZE       CHANGE
# ---------         ----       ------
# 2026-01-30 15:00  1.2 GiB    +50 MiB
# 2026-01-30 14:00  1.15 GiB   +10 MiB
# 2026-01-30 13:00  1.14 GiB   -
```

Filter by time range:

```bash
usgmon query /www/users/bob.com --days 7
usgmon query /www/users/bob.com --since "2026-01-01"
```

Output as JSON:

```bash
usgmon query /www/users/bob.com --format json
```

### Daemon Mode

Start the daemon (typically via systemd):

```bash
usgmon serve --config /etc/usgmon/usgmon.yaml
```

### Version

```bash
usgmon version
```

## Configuration

Create a configuration file at `/etc/usgmon/usgmon.yaml`:

```yaml
database:
  path: /var/lib/usgmon/usgmon.db

logging:
  level: info      # debug, info, warn, error
  format: text     # text or json

scan:
  interval: 1h     # Default scan interval
  workers: 4       # Worker pool size

paths:
  - path: /www/users
    depth: 1       # Scan /www/users/* directories
    interval: 30m  # Optional override

  - path: /home
    depth: 1
```

### Configuration Options

| Option | Description | Default |
|--------|-------------|---------|
| `database.path` | Path to SQLite database file | `/var/lib/usgmon/usgmon.db` |
| `logging.level` | Log level (debug, info, warn, error) | `info` |
| `logging.format` | Log format (text, json) | `text` |
| `scan.interval` | Default interval between scans | `1h` |
| `scan.workers` | Number of worker goroutines | `4` |
| `paths[].path` | Directory path to monitor | required |
| `paths[].depth` | Depth to scan (0 = path itself) | `0` |
| `paths[].interval` | Override scan interval for this path | inherits default |

## Systemd

Enable and start the service:

```bash
sudo systemctl daemon-reload
sudo systemctl enable usgmon
sudo systemctl start usgmon
```

View logs:

```bash
journalctl -u usgmon -f
```

## Scanning Strategies

usgmon automatically selects the best available strategy for each path:

1. **CephFS** - If the path is on a CephFS filesystem (detected via statfs), reads the `ceph.dir.rbytes` extended attribute for instant size retrieval without traversal.

2. **du** - If the `du` command is available, executes `du -sb` for efficient size calculation.

3. **Walk** - Falls back to `filepath.WalkDir` for manual traversal when neither of the above is available.

## Database Schema

usgmon uses SQLite with the following schema:

```sql
CREATE TABLE usage_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    base_path TEXT NOT NULL,
    directory TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    recorded_at DATETIME NOT NULL,
    scan_id TEXT NOT NULL
);

CREATE TABLE scans (
    scan_id TEXT PRIMARY KEY,
    base_path TEXT NOT NULL,
    started_at DATETIME NOT NULL,
    completed_at DATETIME,
    directories_scanned INTEGER DEFAULT 0,
    status TEXT DEFAULT 'running'
);
```

## Building

```bash
# Build for current platform
make build

# Build for multiple platforms
make build-all

# Run tests
make test

# Run linter
make lint

# Clean build artifacts
make clean
```

The build produces a static binary with `CGO_ENABLED=0` using the pure Go SQLite driver.

## Dependencies

- [github.com/spf13/cobra](https://github.com/spf13/cobra) - CLI framework
- [github.com/spf13/viper](https://github.com/spf13/viper) - Configuration management
- [modernc.org/sqlite](https://modernc.org/sqlite) - Pure Go SQLite driver
- [github.com/google/uuid](https://github.com/google/uuid) - UUID generation
- [golang.org/x/sys/unix](https://golang.org/x/sys) - System calls for xattr reading

## License

MIT
