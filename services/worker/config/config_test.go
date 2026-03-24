package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setEnv(t *testing.T, env map[string]string) {
	t.Helper()
	for k, v := range env {
		t.Setenv(k, v)
	}
}

func baseEnv() map[string]string {
	return map[string]string{
		"TASK_ID":          "550e8400-e29b-41d4-a716-446655440000",
		"TASK_TYPE":        "map",
		"JOB_ID":           "660e8400-e29b-41d4-a716-446655440000",
		"TASK_INDEX":       "0",
		"MANAGER_URL":      "http://manager:8080",
		"MINIO_ENDPOINT":   "minio:9000",
		"MINIO_ACCESS_KEY": "minioadmin",
		"MINIO_SECRET_KEY": "minioadmin",
	}
}

func TestLoad_MapTask_Success(t *testing.T) {
	env := baseEnv()
	env["INPUT_PATH"] = `{"file":"input/data.jsonl","offset":0,"length":1024}`
	env["MAPPER_PATH"] = "code/mapper.py"
	env["NUM_REDUCERS"] = "2"
	setEnv(t, env)

	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", cfg.TaskID)
	assert.Equal(t, TaskTypeMap, cfg.TaskType)
	assert.Equal(t, "660e8400-e29b-41d4-a716-446655440000", cfg.JobID)
	assert.Equal(t, 0, cfg.TaskIndex)
	assert.Equal(t, "http://manager:8080", cfg.ManagerURL)

	require.NotNil(t, cfg.InputSpec)
	assert.Equal(t, "input/data.jsonl", cfg.InputSpec.File)
	assert.Equal(t, int64(0), cfg.InputSpec.Offset)
	assert.Equal(t, int64(1024), cfg.InputSpec.Length)

	assert.Equal(t, "code/mapper.py", cfg.MapperPath)
	assert.Equal(t, 2, cfg.NumReducers)

	assert.Equal(t, "minio:9000", cfg.MinioEndpoint)
	assert.Equal(t, "minioadmin", cfg.MinioAccessKey)
}

func TestLoad_ReduceTask_Success(t *testing.T) {
	env := baseEnv()
	env["TASK_TYPE"] = "reduce"
	env["REDUCER_PATH"] = "code/reducer.py"
	env["INPUT_LOCATIONS"] = `[{"reducer_index":0,"path":"jobs/abc/map-0-reduce-0.txt"}]`
	setEnv(t, env)

	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, TaskTypeReduce, cfg.TaskType)
	assert.Equal(t, "code/reducer.py", cfg.ReducerPath)
	require.Len(t, cfg.InputLocations, 1)
	assert.Equal(t, 0, cfg.InputLocations[0].ReducerIndex)
	assert.Equal(t, "jobs/abc/map-0-reduce-0.txt", cfg.InputLocations[0].Path)
}

func TestLoad_MissingTaskID_ReturnsError(t *testing.T) {
	env := baseEnv()
	delete(env, "TASK_ID")
	setEnv(t, env)

	// Clear TASK_ID explicitly since t.Setenv won't clear what was set before
	os.Unsetenv("TASK_ID")

	_, err := Load()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "TASK_ID")
}

func TestLoad_InvalidTaskType_ReturnsError(t *testing.T) {
	env := baseEnv()
	env["TASK_TYPE"] = "invalid"
	setEnv(t, env)

	_, err := Load()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "TASK_TYPE")
}

func TestLoad_MapTask_MissingMapperPath_ReturnsError(t *testing.T) {
	env := baseEnv()
	env["INPUT_PATH"] = `{"file":"input/data.jsonl","offset":0,"length":1024}`
	env["NUM_REDUCERS"] = "2"
	// Missing MAPPER_PATH
	setEnv(t, env)

	_, err := Load()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "MAPPER_PATH")
}

func TestLoad_MapTask_MissingInputPath_ReturnsError(t *testing.T) {
	env := baseEnv()
	env["MAPPER_PATH"] = "code/mapper.py"
	env["NUM_REDUCERS"] = "2"
	// Missing INPUT_PATH
	setEnv(t, env)

	_, err := Load()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "INPUT_PATH")
}

func TestLoad_MapTask_InvalidInputPathJSON_ReturnsError(t *testing.T) {
	env := baseEnv()
	env["INPUT_PATH"] = `not json`
	env["MAPPER_PATH"] = "code/mapper.py"
	env["NUM_REDUCERS"] = "2"
	setEnv(t, env)

	_, err := Load()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "INPUT_PATH")
}

func TestLoad_ReduceTask_MissingReducerPath_ReturnsError(t *testing.T) {
	env := baseEnv()
	env["TASK_TYPE"] = "reduce"
	env["INPUT_LOCATIONS"] = `[{"reducer_index":0,"path":"jobs/abc/map-0-reduce-0.txt"}]`
	// Missing REDUCER_PATH
	setEnv(t, env)

	_, err := Load()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "REDUCER_PATH")
}

func TestLoad_ReduceTask_MissingInputLocations_ReturnsError(t *testing.T) {
	env := baseEnv()
	env["TASK_TYPE"] = "reduce"
	env["REDUCER_PATH"] = "code/reducer.py"
	// Missing INPUT_LOCATIONS
	setEnv(t, env)

	_, err := Load()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "INPUT_LOCATIONS")
}

func TestLoad_InvalidTaskIndex_ReturnsError(t *testing.T) {
	env := baseEnv()
	env["TASK_INDEX"] = "not-a-number"
	env["INPUT_PATH"] = `{"file":"input/data.jsonl","offset":0,"length":1024}`
	env["MAPPER_PATH"] = "code/mapper.py"
	env["NUM_REDUCERS"] = "2"
	setEnv(t, env)

	_, err := Load()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "TASK_INDEX")
}

func TestLoad_DefaultBuckets(t *testing.T) {
	env := baseEnv()
	env["INPUT_PATH"] = `{"file":"input/data.jsonl","offset":0,"length":1024}`
	env["MAPPER_PATH"] = "code/mapper.py"
	env["NUM_REDUCERS"] = "2"
	setEnv(t, env)

	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, "input", cfg.MinioBucketInput)
	assert.Equal(t, "code", cfg.MinioBucketCode)
	assert.Equal(t, "jobs", cfg.MinioBucketJobs)
	assert.Equal(t, "output", cfg.MinioBucketOutput)
	assert.False(t, cfg.MinioUseSSL)
}
