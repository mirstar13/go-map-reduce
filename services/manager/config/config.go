package config

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

// Config holds all runtime configuration for the Manager service.
type Config struct {
	Port string // MANAGER_PORT, default "8080"

	MyReplicaName string // MY_REPLICA_NAME, e.g. "manager-0"

	// Database
	PostgresDSN string // POSTGRES_DSN  (required)

	// MinIO object-storage
	MinioEndpoint     string // MINIO_ENDPOINT   (required)
	MinioBucketInput  string // MINIO_BUCKET_INPUT
	MinioBucketCode   string // MINIO_BUCKET_CODE
	MinioBucketJobs   string // MINIO_BUCKET_JOBS
	MinioBucketOutput string // MINIO_BUCKET_OUTPUT
	MinioAccessKey    string // MINIO_ACCESS_KEY  (required)
	MinioSecretKey    string // MINIO_SECRET_KEY  (required)
	MinioUseSSL       bool   // MINIO_USE_SSL, default false

	// Kubernetes worker dispatch
	WorkerImage     string // WORKER_IMAGE
	WorkerNamespace string // WORKER_NAMESPACE
	// ManagerURL is advertised to workers for POST /tasks/*/complete|fail callbacks.
	ManagerURL string // MANAGER_URL

	// Job execution limits
	TaskTimeoutSeconds int // TASK_TIMEOUT_SECONDS, default 300
	TaskMaxRetries     int // TASK_MAX_RETRIES, default 3

	// Observability
	LogLevel  string // LOG_LEVEL,  default "info"  — debug|info|warn|error
	LogFormat string // LOG_FORMAT, default "json"  — json|console
}

// Load reads configuration for the Manager service.
// Returns an error if any required variable is missing or has an invalid value.
func Load() (*Config, error) {
	v := viper.New()

	// AutomaticEnv makes v.GetString("manager_port") transparently check
	// the env var MANAGER_PORT (viper uppercases the key automatically).
	v.AutomaticEnv()
	// Allow compound keys like "minio.endpoint" to map to MINIO_ENDPOINT.
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	v.SetConfigName(".env")
	v.SetConfigType("env")
	v.AddConfigPath(".")     // look in the working directory first
	v.AddConfigPath("../..") // also project root when running from a sub-dir
	_ = v.ReadInConfig()     // silently ignore: file is optional

	v.SetDefault("manager_port", "8080")
	v.SetDefault("my_replica_name", "manager-0")
	v.SetDefault("minio_bucket_input", "input")
	v.SetDefault("minio_bucket_code", "code")
	v.SetDefault("minio_bucket_jobs", "jobs")
	v.SetDefault("minio_bucket_output", "output")
	v.SetDefault("minio_use_ssl", false)
	v.SetDefault("worker_image", "mirstar13/mapreduce-worker:latest")
	v.SetDefault("worker_namespace", "mapreduce")
	v.SetDefault("manager_url", "http://manager-api.mapreduce.svc.cluster.local:8080")
	v.SetDefault("task_timeout_seconds", 300)
	v.SetDefault("task_max_retries", 3)
	v.SetDefault("log_level", "info")
	v.SetDefault("log_format", "json")

	cfg := &Config{
		Port:               v.GetString("manager_port"),
		MyReplicaName:      v.GetString("my_replica_name"),
		PostgresDSN:        v.GetString("postgres_dsn"),
		MinioEndpoint:      v.GetString("minio_endpoint"),
		MinioBucketInput:   v.GetString("minio_bucket_input"),
		MinioBucketCode:    v.GetString("minio_bucket_code"),
		MinioBucketJobs:    v.GetString("minio_bucket_jobs"),
		MinioBucketOutput:  v.GetString("minio_bucket_output"),
		MinioAccessKey:     v.GetString("minio_access_key"),
		MinioSecretKey:     v.GetString("minio_secret_key"),
		MinioUseSSL:        v.GetBool("minio_use_ssl"),
		WorkerImage:        v.GetString("worker_image"),
		WorkerNamespace:    v.GetString("worker_namespace"),
		ManagerURL:         v.GetString("manager_url"),
		TaskTimeoutSeconds: v.GetInt("task_timeout_seconds"),
		TaskMaxRetries:     v.GetInt("task_max_retries"),
		LogLevel:           v.GetString("log_level"),
		LogFormat:          v.GetString("log_format"),
	}

	required := []struct{ key, val string }{
		{"POSTGRES_DSN", cfg.PostgresDSN},
		{"MINIO_ENDPOINT", cfg.MinioEndpoint},
		{"MINIO_ACCESS_KEY", cfg.MinioAccessKey},
		{"MINIO_SECRET_KEY", cfg.MinioSecretKey},
	}
	for _, r := range required {
		if r.val == "" {
			return nil, fmt.Errorf("config: required env var %s is not set", r.key)
		}
	}

	if cfg.TaskTimeoutSeconds < 1 {
		return nil, fmt.Errorf("config: TASK_TIMEOUT_SECONDS must be > 0, got %d", cfg.TaskTimeoutSeconds)
	}
	if cfg.TaskMaxRetries < 0 {
		return nil, fmt.Errorf("config: TASK_MAX_RETRIES must be >= 0, got %d", cfg.TaskMaxRetries)
	}

	return cfg, nil
}
