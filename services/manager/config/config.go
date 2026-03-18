package config

import (
	"fmt"
	"os"
	"strconv"
)

// Config holds all configuration for the Manager service.
type Config struct {
	Port string // MANAGER_PORT, default "8080"

	MyReplicaName string // MY_REPLICA_NAME, e.g. "manager-0"

	PostgresDSN string // POSTGRES_DSN

	// MinIO
	MinioEndpoint     string // MINIO_ENDPOINT
	MinioBucketInput  string // MINIO_BUCKET_INPUT
	MinioBucketCode   string // MINIO_BUCKET_CODE
	MinioBucketJobs   string // MINIO_BUCKET_JOBS
	MinioBucketOutput string // MINIO_BUCKET_OUTPUT
	MinioAccessKey    string // MINIO_ACCESS_KEY
	MinioSecretKey    string // MINIO_SECRET_KEY
	MinioUseSSL       bool   // MINIO_USE_SSL

	// Kubernetes
	WorkerImage     string // WORKER_IMAGE
	WorkerNamespace string // WORKER_NAMESPACE
	// URL the Manager advertises to workers so they can POST callbacks.
	// Should point to the ClusterIP service, e.g. http://manager-api.mapreduce.svc.cluster.local:8080
	ManagerURL string // MANAGER_URL

	// Job execution limits
	TaskTimeoutSeconds int // TASK_TIMEOUT_SECONDS, default 300
	TaskMaxRetries     int // TASK_MAX_RETRIES, default 3
}

// Load reads configuration from the environment.
func Load() (*Config, error) {
	cfg := &Config{
		Port:              getenv("MANAGER_PORT", "8080"),
		MyReplicaName:     getenv("MY_REPLICA_NAME", "manager-0"),
		PostgresDSN:       getenv("POSTGRES_DSN", ""),
		MinioEndpoint:     getenv("MINIO_ENDPOINT", ""),
		MinioBucketInput:  getenv("MINIO_BUCKET_INPUT", "input"),
		MinioBucketCode:   getenv("MINIO_BUCKET_CODE", "code"),
		MinioBucketJobs:   getenv("MINIO_BUCKET_JOBS", "jobs"),
		MinioBucketOutput: getenv("MINIO_BUCKET_OUTPUT", "output"),
		MinioAccessKey:    getenv("MINIO_ACCESS_KEY", ""),
		MinioSecretKey:    getenv("MINIO_SECRET_KEY", ""),
		WorkerImage:       getenv("WORKER_IMAGE", "mirstar13/mapreduce-worker:latest"),
		WorkerNamespace:   getenv("WORKER_NAMESPACE", "mapreduce"),
		ManagerURL:        getenv("MANAGER_URL", "http://manager-api.mapreduce.svc.cluster.local:8080"),
	}

	sslStr := getenv("MINIO_USE_SSL", "false")
	cfg.MinioUseSSL, _ = strconv.ParseBool(sslStr)

	var err error
	if cfg.TaskTimeoutSeconds, err = parseInt("TASK_TIMEOUT_SECONDS", "300"); err != nil {
		return nil, err
	}

	if cfg.TaskMaxRetries, err = parseInt("TASK_MAX_RETRIES", "3"); err != nil {
		return nil, err
	}

	required := map[string]string{
		"POSTGRES_DSN":     cfg.PostgresDSN,
		"MINIO_ENDPOINT":   cfg.MinioEndpoint,
		"MINIO_ACCESS_KEY": cfg.MinioAccessKey,
		"MINIO_SECRET_KEY": cfg.MinioSecretKey,
	}
	for name, val := range required {
		if val == "" {
			return nil, fmt.Errorf("config: required env var %s is not set", name)
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

func parseInt(key, fallback string) (int, error) {
	v := getenv(key, fallback)
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("config: %s must be a non-negative integer, got %q", key, v)
	}
	return n, nil
}
