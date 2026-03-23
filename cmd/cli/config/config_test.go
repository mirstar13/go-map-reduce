package config

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// overrideHome redirects os.UserHomeDir() to a temp directory for the duration
// of the test. On Windows UserHomeDir() reads USERPROFILE (not HOME), so we
// set both env vars to be cross-platform.
func overrideHome(t *testing.T) string {
	t.Helper()
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)
	t.Setenv("USERPROFILE", tmp) // Windows: used by os.UserHomeDir
	t.Setenv("HOMEDRIVE", "")    // Windows: clear drive letter fallback
	t.Setenv("HOMEPATH", "")     // Windows: clear path fallback
	return tmp
}

// configFilePath returns where the config file should live under the given home.
func configFilePath(home string) string {
	return filepath.Join(home, ".mapreduce", "config.json")
}

func TestLoad_MissingFile_ReturnsEmptyConfig(t *testing.T) {
	// First-run scenario: no config file exists yet. Load must succeed and
	// return a zero-value Config rather than an error.
	overrideHome(t)

	cfg, err := Load()
	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Empty(t, cfg.ServerURL)
	assert.Empty(t, cfg.Token)
}

func TestLoad_ExistingFile_ReturnsStoredValues(t *testing.T) {
	home := overrideHome(t)

	// Pre-write a well-formed config file.
	dir := filepath.Join(home, ".mapreduce")
	require.NoError(t, os.MkdirAll(dir, 0o700))
	data := `{"server_url":"http://localhost:8081","token":"my.jwt.token"}`
	require.NoError(t, os.WriteFile(configFilePath(home), []byte(data), 0o600))

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, "http://localhost:8081", cfg.ServerURL)
	assert.Equal(t, "my.jwt.token", cfg.Token)
}

func TestLoad_CorruptFile_ReturnsError(t *testing.T) {
	home := overrideHome(t)

	dir := filepath.Join(home, ".mapreduce")
	require.NoError(t, os.MkdirAll(dir, 0o700))
	require.NoError(t, os.WriteFile(configFilePath(home), []byte("not json {{"), 0o600))

	_, err := Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse")
}

func TestSave_CreatesFileAndDirectory(t *testing.T) {
	// When ~/.mapreduce doesn't exist yet, Save must create it.
	home := overrideHome(t)

	cfg := &Config{
		ServerURL: "http://ui:8081",
		Token:     "tok.en.here",
	}
	require.NoError(t, cfg.Save())

	_, err := os.Stat(configFilePath(home))
	assert.NoError(t, err, "config.json must exist after Save")
}

func TestSave_RoundTrip(t *testing.T) {
	// Save then Load must produce the same values.
	overrideHome(t)

	original := &Config{
		ServerURL: "https://prod.example.com",
		Token:     "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.payload.sig",
	}
	require.NoError(t, original.Save())

	loaded, err := Load()
	require.NoError(t, err)
	assert.Equal(t, original.ServerURL, loaded.ServerURL)
	assert.Equal(t, original.Token, loaded.Token)
}

func TestSave_OverwritesExistingFile(t *testing.T) {
	// Saving twice must overwrite; Load must return the second version.
	overrideHome(t)

	first := &Config{ServerURL: "http://v1:8081", Token: "tok1"}
	require.NoError(t, first.Save())

	second := &Config{ServerURL: "http://v2:8081", Token: "tok2"}
	require.NoError(t, second.Save())

	loaded, err := Load()
	require.NoError(t, err)
	assert.Equal(t, "http://v2:8081", loaded.ServerURL)
	assert.Equal(t, "tok2", loaded.Token)
}

func TestSave_FilePermissions_AreRestrictive(t *testing.T) {
	// Unix-only: Windows does not enforce POSIX permission bits.
	if runtime.GOOS == "windows" {
		t.Skip("POSIX file permission bits are not enforced on Windows")
	}

	home := overrideHome(t)

	cfg := &Config{ServerURL: "http://ui:8081", Token: "secret"}
	require.NoError(t, cfg.Save())

	info, err := os.Stat(configFilePath(home))
	require.NoError(t, err)

	// Must be 0600: owner read/write only — the file holds a JWT credential.
	perm := info.Mode().Perm()
	assert.Equal(t, os.FileMode(0o600), perm,
		"config file must be 0600 — token is a credential")
}

func TestSave_EmptyConfig_RoundTrips(t *testing.T) {
	overrideHome(t)

	cfg := &Config{}
	require.NoError(t, cfg.Save())

	loaded, err := Load()
	require.NoError(t, err)
	assert.Empty(t, loaded.ServerURL)
	assert.Empty(t, loaded.Token)
}
