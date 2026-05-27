package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// Config holds all persistent CLI settings.
type Config struct {
	// ServerURL is the base URL of the UI service, e.g. http://localhost:8081
	ServerURL string `json:"server_url"`
	// Token is the current Keycloak access token (JWT).
	Token string `json:"token"`
	// MapperThresholdMB is the amount of data (in MB) assigned per mapper.
	MapperThresholdMB int `json:"mapper_threshold_mb"`
}

// configPath returns the path to the config file.
func configPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("config: cannot find home directory: %w", err)
	}
	return filepath.Join(home, ".mapreduce", "config.json"), nil
}

// Load reads the config file. Returns an empty Config (not an error) if the
// file does not exist yet first-run scenario.
func Load() (*Config, error) {
	path, err := configPath()
	if err != nil {
		return nil, err
	}

	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return &Config{MapperThresholdMB: 10}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}

	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("config: parse %s: %w", path, err)
	}

	// Default to 10 if not set.
	if cfg.MapperThresholdMB <= 0 {
		cfg.MapperThresholdMB = 10
	}
	return &cfg, nil
}

// Save writes the config file, creating the directory if needed.
func (c *Config) Save() error {
	path, err := configPath()
	if err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("config: create dir: %w", err)
	}

	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return fmt.Errorf("config: marshal: %w", err)
	}

	// Write with restricted permissions the token is a credential.
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("config: write %s: %w", path, err)
	}
	return nil
}
