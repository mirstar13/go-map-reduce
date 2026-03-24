package config

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/spf13/viper"
)

// TaskType represents the type of task (map or reduce).
type TaskType string

const (
	TaskTypeMap    TaskType = "map"
	TaskTypeReduce TaskType = "reduce"
)

// InputSpec describes the input file and byte range for a map task.
type InputSpec struct {
	File   string `json:"file"`
	Offset int64  `json:"offset"`
	Length int64  `json:"length"`
}

// InputLocation describes a single partition file from a map task.
type InputLocation struct {
	ReducerIndex int    `json:"reducer_index"`
	Path         string `json:"path"`
}

// Config holds all runtime configuration for a worker.
type Config struct {
	// Task identification
	TaskID    string   // TASK_ID (required)
	TaskType  TaskType // TASK_TYPE: "map" or "reduce" (required)
	JobID     string   // JOB_ID (required)
	TaskIndex int      // TASK_INDEX (required)

	// Manager callback
	ManagerURL string // MANAGER_URL (required)

	// Map task specific
	InputSpec   *InputSpec // INPUT_PATH as JSON (map only)
	MapperPath  string     // MAPPER_PATH (map only)
	NumReducers int        // NUM_REDUCERS (map only)

	// Reduce task specific
	ReducerPath    string          // REDUCER_PATH (reduce only)
	InputLocations []InputLocation // INPUT_LOCATIONS as JSON array (reduce only)

	// MinIO configuration
	MinioEndpoint     string // MINIO_ENDPOINT (required)
	MinioAccessKey    string // MINIO_ACCESS_KEY (required)
	MinioSecretKey    string // MINIO_SECRET_KEY (required)
	MinioUseSSL       bool   // MINIO_USE_SSL, default false
	MinioBucketInput  string // MINIO_BUCKET_INPUT
	MinioBucketCode   string // MINIO_BUCKET_CODE
	MinioBucketJobs   string // MINIO_BUCKET_JOBS
	MinioBucketOutput string // MINIO_BUCKET_OUTPUT
}

// Load reads configuration from environment variables.
func Load() (*Config, error) {
	v := viper.New()
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	v.SetDefault("minio_use_ssl", false)
	v.SetDefault("minio_bucket_input", "input")
	v.SetDefault("minio_bucket_code", "code")
	v.SetDefault("minio_bucket_jobs", "jobs")
	v.SetDefault("minio_bucket_output", "output")

	cfg := &Config{
		TaskID:            v.GetString("task_id"),
		TaskType:          TaskType(v.GetString("task_type")),
		JobID:             v.GetString("job_id"),
		ManagerURL:        v.GetString("manager_url"),
		MapperPath:        v.GetString("mapper_path"),
		ReducerPath:       v.GetString("reducer_path"),
		MinioEndpoint:     v.GetString("minio_endpoint"),
		MinioAccessKey:    v.GetString("minio_access_key"),
		MinioSecretKey:    v.GetString("minio_secret_key"),
		MinioUseSSL:       v.GetBool("minio_use_ssl"),
		MinioBucketInput:  v.GetString("minio_bucket_input"),
		MinioBucketCode:   v.GetString("minio_bucket_code"),
		MinioBucketJobs:   v.GetString("minio_bucket_jobs"),
		MinioBucketOutput: v.GetString("minio_bucket_output"),
	}

	// Parse TASK_INDEX
	taskIndexStr := v.GetString("task_index")
	if taskIndexStr != "" {
		idx, err := strconv.Atoi(taskIndexStr)
		if err != nil {
			return nil, fmt.Errorf("config: invalid TASK_INDEX %q: %w", taskIndexStr, err)
		}
		cfg.TaskIndex = idx
	}

	// Parse NUM_REDUCERS for map tasks
	numReducersStr := v.GetString("num_reducers")
	if numReducersStr != "" {
		n, err := strconv.Atoi(numReducersStr)
		if err != nil {
			return nil, fmt.Errorf("config: invalid NUM_REDUCERS %q: %w", numReducersStr, err)
		}
		cfg.NumReducers = n
	}

	// Parse INPUT_PATH JSON for map tasks
	inputPathJSON := v.GetString("input_path")
	if inputPathJSON != "" {
		var spec InputSpec
		if err := json.Unmarshal([]byte(inputPathJSON), &spec); err != nil {
			return nil, fmt.Errorf("config: invalid INPUT_PATH JSON: %w", err)
		}
		cfg.InputSpec = &spec
	}

	// Parse INPUT_LOCATIONS JSON for reduce tasks
	inputLocationsJSON := v.GetString("input_locations")
	if inputLocationsJSON != "" {
		var locs []InputLocation
		if err := json.Unmarshal([]byte(inputLocationsJSON), &locs); err != nil {
			return nil, fmt.Errorf("config: invalid INPUT_LOCATIONS JSON: %w", err)
		}
		cfg.InputLocations = locs
	}

	// Validate required fields
	required := []struct{ key, val string }{
		{"TASK_ID", cfg.TaskID},
		{"TASK_TYPE", string(cfg.TaskType)},
		{"JOB_ID", cfg.JobID},
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

	// Validate task type
	if cfg.TaskType != TaskTypeMap && cfg.TaskType != TaskTypeReduce {
		return nil, fmt.Errorf("config: TASK_TYPE must be 'map' or 'reduce', got %q", cfg.TaskType)
	}

	// Validate map-specific requirements
	if cfg.TaskType == TaskTypeMap {
		if cfg.InputSpec == nil {
			return nil, fmt.Errorf("config: INPUT_PATH is required for map tasks")
		}
		if cfg.MapperPath == "" {
			return nil, fmt.Errorf("config: MAPPER_PATH is required for map tasks")
		}
		if cfg.NumReducers < 1 {
			return nil, fmt.Errorf("config: NUM_REDUCERS must be >= 1 for map tasks")
		}
	}

	// Validate reduce-specific requirements
	if cfg.TaskType == TaskTypeReduce {
		if cfg.ReducerPath == "" {
			return nil, fmt.Errorf("config: REDUCER_PATH is required for reduce tasks")
		}
		if len(cfg.InputLocations) == 0 {
			return nil, fmt.Errorf("config: INPUT_LOCATIONS is required for reduce tasks")
		}
	}

	return cfg, nil
}
