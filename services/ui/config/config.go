package config

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

// Config holds all runtime configuration for the UI service.
type Config struct {
	Port string // UI_PORT, default "8081"

	// Keycloak — public token endpoint (used for login proxy)
	KeycloakURL      string // KEYCLOAK_URL  (required)
	KeycloakRealm    string // KEYCLOAK_REALM
	KeycloakClientID string // KEYCLOAK_CLIENT_ID

	// Keycloak — admin credentials (used for user-management endpoints)
	KeycloakAdminUser     string // KEYCLOAK_ADMIN_USER
	KeycloakAdminPassword string // KEYCLOAK_ADMIN_PASSWORD  (required)

	// Manager service
	// ClusterIP service URL — used for read operations (list / get).
	ManagerAPIURL string // MANAGER_API_URL  (required)
	// Headless service hostname pattern for replica-targeted submissions.
	// Individual pods are addressed as manager-{i}.{ManagerHeadlessHost}:{ManagerPort}.
	ManagerHeadlessHost string // MANAGER_HEADLESS_HOST
	ManagerPort         string // MANAGER_PORT, default "8080"
	ManagerReplicas     int    // MANAGER_REPLICAS, default 2

	// MinIO — used for file uploads (input data, mapper / reducer code)
	MinioEndpoint     string // MINIO_ENDPOINT   (required)
	MinioBucketInput  string // MINIO_BUCKET_INPUT
	MinioBucketCode   string // MINIO_BUCKET_CODE
	MinioBucketOutput string // MINIO_BUCKET_OUTPUT
	MinioAccessKey    string // MINIO_ACCESS_KEY  (required)
	MinioSecretKey    string // MINIO_SECRET_KEY  (required)
	MinioUseSSL       bool   // MINIO_USE_SSL, default false

	// Observability
	LogLevel  string // LOG_LEVEL,  default "info"  — debug|info|warn|error
	LogFormat string // LOG_FORMAT, default "json"  — json|console
}

// Load reads configuration for the UI service.
// Returns an error if any required variable is missing or has an invalid value.
func Load() (*Config, error) {
	v := viper.New()

	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	v.SetConfigName(".env")
	v.SetConfigType("env")
	v.AddConfigPath(".")
	v.AddConfigPath("../..")
	_ = v.ReadInConfig()

	v.SetDefault("ui_port", "8081")
	v.SetDefault("keycloak_realm", "mapreduce")
	v.SetDefault("keycloak_client_id", "mapreduce-ui")
	v.SetDefault("keycloak_admin_user", "admin")
	v.SetDefault("manager_headless_host", "manager-headless.mapreduce.svc.cluster.local")
	v.SetDefault("manager_port", "8080")
	v.SetDefault("manager_replicas", 2)
	v.SetDefault("minio_bucket_input", "input")
	v.SetDefault("minio_bucket_code", "code")
	v.SetDefault("minio_bucket_output", "output")
	v.SetDefault("minio_use_ssl", false)
	v.SetDefault("log_level", "info")
	v.SetDefault("log_format", "json")

	cfg := &Config{
		Port:                  v.GetString("ui_port"),
		KeycloakURL:           v.GetString("keycloak_url"),
		KeycloakRealm:         v.GetString("keycloak_realm"),
		KeycloakClientID:      v.GetString("keycloak_client_id"),
		KeycloakAdminUser:     v.GetString("keycloak_admin_user"),
		KeycloakAdminPassword: v.GetString("keycloak_admin_password"),
		ManagerAPIURL:         v.GetString("manager_api_url"),
		ManagerHeadlessHost:   v.GetString("manager_headless_host"),
		ManagerPort:           v.GetString("manager_port"),
		ManagerReplicas:       v.GetInt("manager_replicas"),
		MinioEndpoint:         v.GetString("minio_endpoint"),
		MinioBucketInput:      v.GetString("minio_bucket_input"),
		MinioBucketCode:       v.GetString("minio_bucket_code"),
		MinioBucketOutput:     v.GetString("minio_bucket_output"),
		MinioAccessKey:        v.GetString("minio_access_key"),
		MinioSecretKey:        v.GetString("minio_secret_key"),
		MinioUseSSL:           v.GetBool("minio_use_ssl"),
		LogLevel:              v.GetString("log_level"),
		LogFormat:             v.GetString("log_format"),
	}

	required := []struct{ key, val string }{
		{"KEYCLOAK_URL", cfg.KeycloakURL},
		{"KEYCLOAK_ADMIN_PASSWORD", cfg.KeycloakAdminPassword},
		{"MANAGER_API_URL", cfg.ManagerAPIURL},
		{"MINIO_ENDPOINT", cfg.MinioEndpoint},
		{"MINIO_ACCESS_KEY", cfg.MinioAccessKey},
		{"MINIO_SECRET_KEY", cfg.MinioSecretKey},
	}
	for _, r := range required {
		if r.val == "" {
			return nil, fmt.Errorf("config: required env var %s is not set", r.key)
		}
	}

	if cfg.ManagerReplicas < 1 {
		return nil, fmt.Errorf("config: MANAGER_REPLICAS must be >= 1, got %d", cfg.ManagerReplicas)
	}

	return cfg, nil
}
