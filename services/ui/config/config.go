package config

import (
	"fmt"
	"os"
	"strconv"
)

// Config holds all configuration for the UI service.
// Values are loaded once at startup from environment variables.
type Config struct {
	// HTTP server
	Port string // UI_PORT, default "8081"

	// Keycloak — public token endpoint (used for login proxy)
	KeycloakURL      string // KEYCLOAK_URL
	KeycloakRealm    string // KEYCLOAK_REALM
	KeycloakClientID string // KEYCLOAK_CLIENT_ID

	// Keycloak — admin credentials (used for user-management endpoints)
	KeycloakAdminUser     string // KEYCLOAK_ADMIN_USER
	KeycloakAdminPassword string // KEYCLOAK_ADMIN_PASSWORD

	// Manager service
	// ClusterIP service URL — used for read operations (list / get).
	ManagerAPIURL string // MANAGER_API_URL
	// Headless service hostname pattern for replica-targeted submissions.
	// Individual pods are addressed as manager-{i}.{ManagerHeadlessHost}:{ManagerPort}
	ManagerHeadlessHost string // MANAGER_HEADLESS_HOST
	ManagerPort         string // MANAGER_PORT, default "8080"
	ManagerReplicas     int    // MANAGER_REPLICAS

	// MinIO — used for file uploads (input data, mapper/reducer code)
	MinioEndpoint     string // MINIO_ENDPOINT
	MinioBucketInput  string // MINIO_BUCKET_INPUT
	MinioBucketCode   string // MINIO_BUCKET_CODE (mapper / reducer scripts)
	MinioBucketOutput string // MINIO_BUCKET_OUTPUT
	MinioAccessKey    string // MINIO_ACCESS_KEY
	MinioSecretKey    string // MINIO_SECRET_KEY
	MinioUseSSL       bool   // MINIO_USE_SSL, default false
}

// Load reads configuration from the environment.
// Returns an error if any required variable is missing.
func Load() (*Config, error) {
	cfg := &Config{
		Port:             getenv("UI_PORT", "8081"),
		KeycloakURL:      getenv("KEYCLOAK_URL", ""),
		KeycloakRealm:    getenv("KEYCLOAK_REALM", "mapreduce"),
		KeycloakClientID: getenv("KEYCLOAK_CLIENT_ID", "mapreduce-ui"),

		KeycloakAdminUser:     getenv("KEYCLOAK_ADMIN_USER", "admin"),
		KeycloakAdminPassword: getenv("KEYCLOAK_ADMIN_PASSWORD", ""),

		ManagerAPIURL:       getenv("MANAGER_API_URL", ""),
		ManagerHeadlessHost: getenv("MANAGER_HEADLESS_HOST", "manager-headless.mapreduce.svc.cluster.local"),
		ManagerPort:         getenv("MANAGER_PORT", "8080"),

		MinioEndpoint:     getenv("MINIO_ENDPOINT", ""),
		MinioBucketInput:  getenv("MINIO_BUCKET_INPUT", "input"),
		MinioBucketCode:   getenv("MINIO_BUCKET_CODE", "code"),
		MinioBucketOutput: getenv("MINIO_BUCKET_OUTPUT", "output"),
		MinioAccessKey:    getenv("MINIO_ACCESS_KEY", ""),
		MinioSecretKey:    getenv("MINIO_SECRET_KEY", ""),
	}

	// MANAGER_REPLICAS
	replicasStr := getenv("MANAGER_REPLICAS", "2")
	replicas, err := strconv.Atoi(replicasStr)
	if err != nil || replicas < 1 {
		return nil, fmt.Errorf("config: MANAGER_REPLICAS must be a positive integer, got %q", replicasStr)
	}
	cfg.ManagerReplicas = replicas

	// MINIO_USE_SSL
	sslStr := getenv("MINIO_USE_SSL", "false")
	cfg.MinioUseSSL, _ = strconv.ParseBool(sslStr)

	// Validate required fields.
	required := map[string]string{
		"KEYCLOAK_URL":            cfg.KeycloakURL,
		"MANAGER_API_URL":         cfg.ManagerAPIURL,
		"MINIO_ENDPOINT":          cfg.MinioEndpoint,
		"MINIO_ACCESS_KEY":        cfg.MinioAccessKey,
		"MINIO_SECRET_KEY":        cfg.MinioSecretKey,
		"KEYCLOAK_ADMIN_PASSWORD": cfg.KeycloakAdminPassword,
	}
	for name, val := range required {
		if val == "" {
			return nil, fmt.Errorf("config: required environment variable %s is not set", name)
		}
	}

	return cfg, nil
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
