package config

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

// PluginType represents the type of plugin to build.
type PluginType string

const (
	PluginTypeMapper  PluginType = "mapper"
	PluginTypeReducer PluginType = "reducer"
)

// Config holds all runtime configuration for the builder.
type Config struct {
	// Build identification
	JobID      string     // JOB_ID (required)
	PluginType PluginType // PLUGIN_TYPE: "mapper" or "reducer" (required)

	// Source code location
	SourcePath string // SOURCE_PATH: MinIO object key to .go source file (required)

	// Output location
	OutputPath string // OUTPUT_PATH: MinIO object key for compiled plugin (required)

	// Manager callback
	ManagerURL string // MANAGER_URL (required)

	// MinIO configuration
	MinioEndpoint   string // MINIO_ENDPOINT (required)
	MinioAccessKey  string // MINIO_ACCESS_KEY (required)
	MinioSecretKey  string // MINIO_SECRET_KEY (required)
	MinioUseSSL     bool   // MINIO_USE_SSL, default false
	MinioBucketCode string // MINIO_BUCKET_CODE
}

// Load reads configuration from environment variables.
func Load() (*Config, error) {
	v := viper.New()
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	v.SetDefault("minio_use_ssl", false)
	v.SetDefault("minio_bucket_code", "code")

	cfg := &Config{
		JobID:           v.GetString("job_id"),
		PluginType:      PluginType(v.GetString("plugin_type")),
		SourcePath:      v.GetString("source_path"),
		OutputPath:      v.GetString("output_path"),
		ManagerURL:      v.GetString("manager_url"),
		MinioEndpoint:   v.GetString("minio_endpoint"),
		MinioAccessKey:  v.GetString("minio_access_key"),
		MinioSecretKey:  v.GetString("minio_secret_key"),
		MinioUseSSL:     v.GetBool("minio_use_ssl"),
		MinioBucketCode: v.GetString("minio_bucket_code"),
	}

	// Validate required fields
	required := []struct{ key, val string }{
		{"JOB_ID", cfg.JobID},
		{"PLUGIN_TYPE", string(cfg.PluginType)},
		{"SOURCE_PATH", cfg.SourcePath},
		{"OUTPUT_PATH", cfg.OutputPath},
		{"MANAGER_URL", cfg.ManagerURL},
		{"MINIO_ENDPOINT", cfg.MinioEndpoint},
		{"MINIO_ACCESS_KEY", cfg.MinioAccessKey},
		{"MINIO_SECRET_KEY", cfg.MinioSecretKey},
	}
	for _, r := range required {
		if r.val == "" {
			return nil, fmt.Errorf("config: required env var %s is not set", r.key)
		}
	}

	// Validate plugin type
	if cfg.PluginType != PluginTypeMapper && cfg.PluginType != PluginTypeReducer {
		return nil, fmt.Errorf("config: PLUGIN_TYPE must be 'mapper' or 'reducer', got %q", cfg.PluginType)
	}

	return cfg, nil
}
