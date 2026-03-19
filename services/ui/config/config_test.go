package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// requiredVars is the minimum set of env vars needed for Load() to succeed.
var requiredVars = map[string]string{
	"KEYCLOAK_URL":            "http://keycloak:8080",
	"KEYCLOAK_ADMIN_PASSWORD": "admin_secret",
	"MANAGER_API_URL":         "http://manager-api:8080",
	"MINIO_ENDPOINT":          "minio:9000",
	"MINIO_ACCESS_KEY":        "minioadmin",
	"MINIO_SECRET_KEY":        "minioadmin_secret",
}

func setEnv(t *testing.T, pairs map[string]string) {
	t.Helper()
	for k, v := range pairs {
		t.Setenv(k, v)
	}
}

func TestLoad_AllRequiredVarsSet_Succeeds(t *testing.T) {
	setEnv(t, requiredVars)
	cfg, err := Load()
	require.NoError(t, err)
	require.NotNil(t, cfg)

	assert.Equal(t, requiredVars["KEYCLOAK_URL"], cfg.KeycloakURL)
	assert.Equal(t, requiredVars["KEYCLOAK_ADMIN_PASSWORD"], cfg.KeycloakAdminPassword)
	assert.Equal(t, requiredVars["MANAGER_API_URL"], cfg.ManagerAPIURL)
	assert.Equal(t, requiredVars["MINIO_ENDPOINT"], cfg.MinioEndpoint)
	assert.Equal(t, requiredVars["MINIO_ACCESS_KEY"], cfg.MinioAccessKey)
	assert.Equal(t, requiredVars["MINIO_SECRET_KEY"], cfg.MinioSecretKey)
}

func TestLoad_MissingKeycloakURL_ReturnsError(t *testing.T) {
	setEnv(t, requiredVars)
	os.Unsetenv("KEYCLOAK_URL")

	_, err := Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "KEYCLOAK_URL")
}

func TestLoad_MissingKeycloakAdminPassword_ReturnsError(t *testing.T) {
	setEnv(t, requiredVars)
	os.Unsetenv("KEYCLOAK_ADMIN_PASSWORD")

	_, err := Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "KEYCLOAK_ADMIN_PASSWORD")
}

func TestLoad_MissingManagerAPIURL_ReturnsError(t *testing.T) {
	setEnv(t, requiredVars)
	os.Unsetenv("MANAGER_API_URL")

	_, err := Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MANAGER_API_URL")
}

func TestLoad_MissingMinioEndpoint_ReturnsError(t *testing.T) {
	setEnv(t, requiredVars)
	os.Unsetenv("MINIO_ENDPOINT")

	_, err := Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MINIO_ENDPOINT")
}

func TestLoad_MissingMinioAccessKey_ReturnsError(t *testing.T) {
	setEnv(t, requiredVars)
	os.Unsetenv("MINIO_ACCESS_KEY")

	_, err := Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MINIO_ACCESS_KEY")
}

func TestLoad_MissingMinioSecretKey_ReturnsError(t *testing.T) {
	setEnv(t, requiredVars)
	os.Unsetenv("MINIO_SECRET_KEY")

	_, err := Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MINIO_SECRET_KEY")
}

func TestLoad_Defaults_AreApplied(t *testing.T) {
	setEnv(t, requiredVars)

	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, "8081", cfg.Port)
	assert.Equal(t, "mapreduce", cfg.KeycloakRealm)
	assert.Equal(t, "mapreduce-ui", cfg.KeycloakClientID)
	assert.Equal(t, "admin", cfg.KeycloakAdminUser)
	assert.Equal(t, "manager-headless.mapreduce.svc.cluster.local", cfg.ManagerHeadlessHost)
	assert.Equal(t, "8080", cfg.ManagerPort)
	assert.Equal(t, 2, cfg.ManagerReplicas)
	assert.Equal(t, "input", cfg.MinioBucketInput)
	assert.Equal(t, "code", cfg.MinioBucketCode)
	assert.Equal(t, "output", cfg.MinioBucketOutput)
	assert.False(t, cfg.MinioUseSSL)
	assert.Equal(t, "info", cfg.LogLevel)
	assert.Equal(t, "json", cfg.LogFormat)
}

func TestLoad_CustomPort_Overrides(t *testing.T) {
	setEnv(t, requiredVars)
	t.Setenv("UI_PORT", "9090")

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, "9090", cfg.Port)
}

func TestLoad_CustomRealm_Overrides(t *testing.T) {
	setEnv(t, requiredVars)
	t.Setenv("KEYCLOAK_REALM", "myrealm")

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, "myrealm", cfg.KeycloakRealm)
}

func TestLoad_CustomManagerReplicas_Overrides(t *testing.T) {
	setEnv(t, requiredVars)
	t.Setenv("MANAGER_REPLICAS", "4")

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, 4, cfg.ManagerReplicas)
}

func TestLoad_MinioUseSSLTrue_ParsedCorrectly(t *testing.T) {
	setEnv(t, requiredVars)
	t.Setenv("MINIO_USE_SSL", "true")

	cfg, err := Load()
	require.NoError(t, err)
	assert.True(t, cfg.MinioUseSSL)
}

func TestLoad_CustomBucketNames_Override(t *testing.T) {
	setEnv(t, requiredVars)
	t.Setenv("MINIO_BUCKET_INPUT", "my-input")
	t.Setenv("MINIO_BUCKET_CODE", "my-code")
	t.Setenv("MINIO_BUCKET_OUTPUT", "my-output")

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, "my-input", cfg.MinioBucketInput)
	assert.Equal(t, "my-code", cfg.MinioBucketCode)
	assert.Equal(t, "my-output", cfg.MinioBucketOutput)
}

func TestLoad_ManagerReplicasZero_ReturnsError(t *testing.T) {
	setEnv(t, requiredVars)
	t.Setenv("MANAGER_REPLICAS", "0")

	_, err := Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MANAGER_REPLICAS")
}

func TestLoad_ManagerReplicasNegative_ReturnsError(t *testing.T) {
	setEnv(t, requiredVars)
	t.Setenv("MANAGER_REPLICAS", "-1")

	_, err := Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MANAGER_REPLICAS")
}

func TestLoad_ManagerReplicasOne_IsValid(t *testing.T) {
	// A single replica is the minimum valid value.
	setEnv(t, requiredVars)
	t.Setenv("MANAGER_REPLICAS", "1")

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, 1, cfg.ManagerReplicas)
}

func TestLoad_LogLevelOverride(t *testing.T) {
	setEnv(t, requiredVars)
	t.Setenv("LOG_LEVEL", "debug")

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, "debug", cfg.LogLevel)
}

func TestLoad_LogFormatConsole_Overrides(t *testing.T) {
	setEnv(t, requiredVars)
	t.Setenv("LOG_FORMAT", "console")

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, "console", cfg.LogFormat)
}

func TestLoad_ManagerHeadlessHostOverride(t *testing.T) {
	setEnv(t, requiredVars)
	t.Setenv("MANAGER_HEADLESS_HOST", "manager-headless.prod.svc.cluster.local")

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, "manager-headless.prod.svc.cluster.local", cfg.ManagerHeadlessHost)
}

func TestLoad_KeycloakAdminUserOverride(t *testing.T) {
	setEnv(t, requiredVars)
	t.Setenv("KEYCLOAK_ADMIN_USER", "superadmin")

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, "superadmin", cfg.KeycloakAdminUser)
}
