package config

import (
	"fmt"
	"time"

	"github.com/spf13/viper"
)

// Config represents the complete application configuration.
type Config struct {
	Database DatabaseConfig `mapstructure:"database"`
	Logging  LoggingConfig  `mapstructure:"logging"`
	Scan     ScanConfig     `mapstructure:"scan"`
	Paths    []PathConfig   `mapstructure:"paths"`
}

// DatabaseConfig holds database-related settings.
type DatabaseConfig struct {
	Path string `mapstructure:"path"`
}

// LoggingConfig holds logging-related settings.
type LoggingConfig struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
}

// ScanConfig holds default scan settings.
type ScanConfig struct {
	Interval time.Duration `mapstructure:"interval"`
	Workers  int           `mapstructure:"workers"`
}

// PathConfig holds configuration for a monitored path.
type PathConfig struct {
	Path     string        `mapstructure:"path"`
	Depth    int           `mapstructure:"depth"`
	Interval time.Duration `mapstructure:"interval"`
}

// EffectiveInterval returns the interval for this path, falling back to the default.
func (p PathConfig) EffectiveInterval(defaultInterval time.Duration) time.Duration {
	if p.Interval > 0 {
		return p.Interval
	}
	return defaultInterval
}

// Load reads configuration from the specified file path.
func Load(configPath string) (*Config, error) {
	v := viper.New()

	// Set defaults
	v.SetDefault("database.path", "/var/lib/usgmon/usgmon.db")
	v.SetDefault("logging.level", "info")
	v.SetDefault("logging.format", "text")
	v.SetDefault("scan.interval", "1h")
	v.SetDefault("scan.workers", 4)

	if configPath != "" {
		v.SetConfigFile(configPath)
	} else {
		v.SetConfigName("usgmon")
		v.SetConfigType("yaml")
		v.AddConfigPath("/etc/usgmon")
		v.AddConfigPath("$HOME/.config/usgmon")
		v.AddConfigPath(".")
	}

	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("reading config: %w", err)
		}
		// Config file not found is OK if using defaults
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshaling config: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validating config: %w", err)
	}

	return &cfg, nil
}

// Validate checks the configuration for errors.
func (c *Config) Validate() error {
	if c.Database.Path == "" {
		return fmt.Errorf("database.path is required")
	}

	if c.Scan.Workers < 1 {
		return fmt.Errorf("scan.workers must be at least 1")
	}

	if c.Scan.Interval < time.Second {
		return fmt.Errorf("scan.interval must be at least 1s")
	}

	for i, p := range c.Paths {
		if p.Path == "" {
			return fmt.Errorf("paths[%d].path is required", i)
		}
		if p.Depth < 0 {
			return fmt.Errorf("paths[%d].depth must be non-negative", i)
		}
	}

	return nil
}

// Default returns a default configuration suitable for testing or initial setup.
func Default() *Config {
	return &Config{
		Database: DatabaseConfig{
			Path: "/var/lib/usgmon/usgmon.db",
		},
		Logging: LoggingConfig{
			Level:  "info",
			Format: "text",
		},
		Scan: ScanConfig{
			Interval: time.Hour,
			Workers:  4,
		},
		Paths: []PathConfig{},
	}
}
