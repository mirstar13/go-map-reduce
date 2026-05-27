package config

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

// Config holds all runtime configuration for the builder service.
type Config struct {
	Port string // BUILDER_PORT, default "8080"

	// MinIO configuration
	MinioEndpoint   string // MINIO_ENDPOINT (required)
	MinioAccessKey  string // MINIO_ACCESS_KEY (required)
	MinioSecretKey  string // MINIO_SECRET_KEY (required)
	MinioUseSSL     bool   // MINIO_USE_SSL, default false
	MinioBucketCode string // MINIO_BUCKET_CODE

	// Manager URL for reporting status
	ManagerURL string // MANAGER_URL (required)

	// Logging
	LogLevel string // LOG_LEVEL, default "info"
}

// Load reads configuration from environment variables.
func Load() (*Config, error) {
	v := viper.New()
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	v.SetDefault("builder_port", "8080")
	v.SetDefault("minio_use_ssl", false)
	v.SetDefault("minio_bucket_code", "code")
	v.SetDefault("log_level", "info")

	cfg := &Config{
		Port:            v.GetString("builder_port"),
		MinioEndpoint:   v.GetString("minio_endpoint"),
		MinioAccessKey:  v.GetString("minio_access_key"),
		MinioSecretKey:  v.GetString("minio_secret_key"),
		MinioUseSSL:     v.GetBool("minio_use_ssl"),
		MinioBucketCode: v.GetString("minio_bucket_code"),
		ManagerURL:      v.GetString("manager_url"),
		LogLevel:        v.GetString("log_level"),
	}

	// Validate required fields
	required := []struct{ key, val string }{
		{"MINIO_ENDPOINT", cfg.MinioEndpoint},
		{"MINIO_ACCESS_KEY", cfg.MinioAccessKey},
		{"MINIO_SECRET_KEY", cfg.MinioSecretKey},
		{"MANAGER_URL", cfg.ManagerURL},
	}
	for _, r := range required {
		if r.val == "" {
			return nil, fmt.Errorf("config: required env var %s is not set", r.key)
		}
	}

	return cfg, nil
}
