package test

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/joho/godotenv"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	_ = godotenv.Overload("test.env")
	os.Exit(m.Run())
}

func requireE2E(t *testing.T) (string, string, string) {
	t.Helper()
	if os.Getenv("E2E_TESTS") != "true" {
		t.Skip("skipping E2E tests; set E2E_TESTS=true to run")
	}
	uiURL := os.Getenv("UI_URL")
	username := os.Getenv("TEST_USERNAME")
	password := os.Getenv("TEST_PASSWORD")
	if uiURL == "" || username == "" || password == "" {
		t.Fatal("UI_URL, TEST_USERNAME, and TEST_PASSWORD must be set for E2E tests")
	}
	return uiURL, username, password
}

// compilePlugin compiles a Go source file into a Linux binary.
func compilePlugin(t *testing.T, name string, source string) string {
	t.Helper()
	tmpDir := t.TempDir()
	sourcePath := filepath.Join(tmpDir, name+".go")
	binaryPath := filepath.Join(tmpDir, name+".bin")

	err := os.WriteFile(sourcePath, []byte(source), 0644)
	require.NoError(t, err)

	cmd := exec.Command("go", "build", "-o", binaryPath, sourcePath)
	cmd.Env = append(os.Environ(),
		"GOOS=linux",
		"GOARCH=amd64",
		"CGO_ENABLED=0",
	)
	// Run build from project root so imports resolve
	cmd.Dir = ".." 

	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "compile %s failed: %s", name, string(out))

	return binaryPath
}
