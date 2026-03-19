package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setEnv sets multiple env vars and returns a cleanup function that restores them.
func setEnv(t *testing.T, pairs map[string]string) {
	t.Helper()
	for k, v := range pairs {
		t.Setenv(k, v)
	}
}

// requiredVars is the minimum set of env vars needed for Load() to succeed.
var requiredVars = map[string]string{
	"POSTGRES_DSN":     "postgres://user:pass@localhost:5432/db",
	"MINIO_ENDPOINT":   "minio:9000",
	"MINIO_ACCESS_KEY": "minioadmin",
	"MINIO_SECRET_KEY": "minioadmin_secret",
}

func TestLoad_AllRequiredVarsSet_Succeeds(t *testing.T) {
	setEnv(t, requiredVars)
	cfg, err := Load()
	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Equal(t, requiredVars["POSTGRES_DSN"], cfg.PostgresDSN)
	assert.Equal(t, requiredVars["MINIO_ENDPOINT"], cfg.MinioEndpoint)
	assert.Equal(t, requiredVars["MINIO_ACCESS_KEY"], cfg.MinioAccessKey)
	assert.Equal(t, requiredVars["MINIO_SECRET_KEY"], cfg.MinioSecretKey)
}

func TestLoad_MissingPostgresDSN_ReturnsError(t *testing.T) {
	vars := map[string]string{
		"MINIO_ENDPOINT":   "minio:9000",
		"MINIO_ACCESS_KEY": "key",
		"MINIO_SECRET_KEY": "secret",
	}
	setEnv(t, vars)
	os.Unsetenv("POSTGRES_DSN")

	_, err := Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "POSTGRES_DSN")
}

func TestLoad_MissingMinioEndpoint_ReturnsError(t *testing.T) {
	vars := map[string]string{
		"POSTGRES_DSN":     "postgres://user:pass@localhost:5432/db",
		"MINIO_ACCESS_KEY": "key",
		"MINIO_SECRET_KEY": "secret",
	}
	setEnv(t, vars)
	os.Unsetenv("MINIO_ENDPOINT")

	_, err := Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MINIO_ENDPOINT")
}

func TestLoad_MissingMinioAccessKey_ReturnsError(t *testing.T) {
	vars := map[string]string{
		"POSTGRES_DSN":     "postgres://user:pass@localhost:5432/db",
		"MINIO_ENDPOINT":   "minio:9000",
		"MINIO_SECRET_KEY": "secret",
	}
	setEnv(t, vars)
	os.Unsetenv("MINIO_ACCESS_KEY")

	_, err := Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MINIO_ACCESS_KEY")
}

func TestLoad_MissingMinioSecretKey_ReturnsError(t *testing.T) {
	vars := map[string]string{
		"POSTGRES_DSN":     "postgres://user:pass@localhost:5432/db",
		"MINIO_ENDPOINT":   "minio:9000",
		"MINIO_ACCESS_KEY": "key",
	}
	setEnv(t, vars)
	os.Unsetenv("MINIO_SECRET_KEY")

	_, err := Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MINIO_SECRET_KEY")
}

func TestLoad_Defaults_AreApplied(t *testing.T) {
	setEnv(t, requiredVars)
	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, "8080", cfg.Port)
	assert.Equal(t, "manager-0", cfg.MyReplicaName)
	assert.Equal(t, "input", cfg.MinioBucketInput)
	assert.Equal(t, "code", cfg.MinioBucketCode)
	assert.Equal(t, "jobs", cfg.MinioBucketJobs)
	assert.Equal(t, "output", cfg.MinioBucketOutput)
	assert.False(t, cfg.MinioUseSSL)
	assert.Equal(t, 300, cfg.TaskTimeoutSeconds)
	assert.Equal(t, 3, cfg.TaskMaxRetries)
	assert.Equal(t, "info", cfg.LogLevel)
	assert.Equal(t, "json", cfg.LogFormat)
}

func TestLoad_CustomPort_Overrides(t *testing.T) {
	setEnv(t, requiredVars)
	t.Setenv("MANAGER_PORT", "9090")

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, "9090", cfg.Port)
}

func TestLoad_CustomReplicaName_Overrides(t *testing.T) {
	setEnv(t, requiredVars)
	t.Setenv("MY_REPLICA_NAME", "manager-1")

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, "manager-1", cfg.MyReplicaName)
}

func TestLoad_TaskTimeoutZero_ReturnsError(t *testing.T) {
	setEnv(t, requiredVars)
	t.Setenv("TASK_TIMEOUT_SECONDS", "0")

	_, err := Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TASK_TIMEOUT_SECONDS")
}

func TestLoad_TaskTimeoutNegative_ReturnsError(t *testing.T) {
	setEnv(t, requiredVars)
	t.Setenv("TASK_TIMEOUT_SECONDS", "-5")

	_, err := Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TASK_TIMEOUT_SECONDS")
}

func TestLoad_TaskMaxRetriesNegative_ReturnsError(t *testing.T) {
	setEnv(t, requiredVars)
	t.Setenv("TASK_MAX_RETRIES", "-1")

	_, err := Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TASK_MAX_RETRIES")
}

func TestLoad_TaskMaxRetriesZero_IsValid(t *testing.T) {
	// Zero retries means no retry — still a legal value.
	setEnv(t, requiredVars)
	t.Setenv("TASK_MAX_RETRIES", "0")

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, 0, cfg.TaskMaxRetries)
}

func TestLoad_MinioUseSSLTrue_ParsedCorrectly(t *testing.T) {
	setEnv(t, requiredVars)
	t.Setenv("MINIO_USE_SSL", "true")

	cfg, err := Load()
	require.NoError(t, err)
	assert.True(t, cfg.MinioUseSSL)
}

func TestLoad_WorkerImageOverride(t *testing.T) {
	setEnv(t, requiredVars)
	t.Setenv("WORKER_IMAGE", "myrepo/worker:v2")

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, "myrepo/worker:v2", cfg.WorkerImage)
}

func TestLoad_ManagerURLOverride(t *testing.T) {
	setEnv(t, requiredVars)
	t.Setenv("MANAGER_URL", "http://manager.internal:8080")

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, "http://manager.internal:8080", cfg.ManagerURL)
}
