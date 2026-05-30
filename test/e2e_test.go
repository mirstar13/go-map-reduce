package test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/mirstar13/go-map-reduce/cmd/cli/client"
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

func TestE2E_FullJobLifecycle(t *testing.T) {
	uiURL, username, password := requireE2E(t)

	// 1. Authenticate via the UI Gateway
	t.Logf("Authenticating as %s against %s", username, uiURL)
	c := client.NewInsecure(uiURL, "")
	var loginResp struct {
		AccessToken string `json:"access_token"`
	}
	err := c.PostNoAuth("/auth/login", map[string]string{
		"username": username,
		"password": password,
	}, &loginResp)
	require.NoError(t, err, "login should succeed")
	require.NotEmpty(t, loginResp.AccessToken, "should receive an access token")

	// Small sleep to avoid "token used before issued" error if cluster clock is slightly behind
	time.Sleep(2 * time.Second)

	// Set the token on the client for subsequent requests
	authClient := client.NewInsecure(uiURL, loginResp.AccessToken)

	// 2. Prepare test data and compile plugins
	t.Log("Compiling plugins...")
	inputContent := `hello world
hello
world world
`
	mapperSource := `package main
import (
	"strings"
	"github.com/hashicorp/go-plugin"
	mrplugin "github.com/mirstar13/go-map-reduce/pkg/plugin"
)
type MapperImpl struct{}
func (m *MapperImpl) Map(inputs []mrplugin.MapInput) ([]mrplugin.Record, error) {
	var records []mrplugin.Record
	for _, input := range inputs {
		words := strings.Fields(input.Value)
		for _, w := range words {
			records = append(records, mrplugin.Record{Key: w, Value: "1"})
		}
	}
	return records, nil
}
func main() {
	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: mrplugin.Handshake,
		Plugins: map[string]plugin.Plugin{
			"mapper": &mrplugin.MapperPlugin{Impl: &MapperImpl{}},
		},
	})
}
`
	reducerSource := `package main
import (
	"fmt"
	"github.com/hashicorp/go-plugin"
	mrplugin "github.com/mirstar13/go-map-reduce/pkg/plugin"
)
type ReducerImpl struct{}
func (r *ReducerImpl) Reduce(inputs []mrplugin.ReduceInput) ([]mrplugin.Record, error) {
	var records []mrplugin.Record
	for _, input := range inputs {
		records = append(records, mrplugin.Record{Key: input.Key, Value: fmt.Sprintf("%d", len(input.Values))})
	}
	return records, nil
}
func main() {
	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: mrplugin.Handshake,
		Plugins: map[string]plugin.Plugin{
			"reducer": &mrplugin.ReducerPlugin{Impl: &ReducerImpl{}},
		},
	})
}
`
	inputPath := filepath.Join(t.TempDir(), "input.txt")
	err = os.WriteFile(inputPath, []byte(inputContent), 0644)
	require.NoError(t, err)

	mapperBin := compilePlugin(t, "mapper", mapperSource)
	reducerBin := compilePlugin(t, "reducer", reducerSource)

	// 3. Upload files
	t.Log("Uploading files...")
	var inputUploadResp struct {
		Path string `json:"path"`
	}
	err = authClient.UploadFile("/files/input", inputPath, &inputUploadResp)
	require.NoError(t, err, "upload input data")
	require.NotEmpty(t, inputUploadResp.Path)

	var mapperUploadResp struct {
		Path string `json:"path"`
	}
	err = authClient.UploadFile("/files/code", mapperBin, &mapperUploadResp)
	require.NoError(t, err, "upload mapper binary")

	var reducerUploadResp struct {
		Path string `json:"path"`
	}
	err = authClient.UploadFile("/files/code", reducerBin, &reducerUploadResp)
	require.NoError(t, err, "upload reducer binary")

	// 4. Submit Job
	t.Log("Submitting job...")
	submitReq := map[string]any{
		"input_path":   inputUploadResp.Path,
		"mapper_path":  mapperUploadResp.Path,
		"reducer_path": reducerUploadResp.Path,
		"num_mappers":  1,
		"num_reducers": 1,
		"input_format": "text",
	}
	var jobResp struct {
		JobID string `json:"job_id"`
	}
	err = authClient.Post("/jobs", submitReq, &jobResp)
	require.NoError(t, err, "submit job")
	require.NotEmpty(t, jobResp.JobID)
	
	jobID := jobResp.JobID
	t.Logf("Job submitted successfully with ID: %s", jobID)

	// 5. Wait for execution
	t.Log("Waiting for job execution to complete (polling)...")
	maxRetries := 60 // 60 * 2s = 120 seconds max
	jobCompleted := false
	for i := 0; i < maxRetries; i++ {
		var getResp map[string]any
		err = authClient.Get("/jobs/"+jobID, &getResp)
		require.NoError(t, err, "get job status")
		
		status := getResp["status"].(string)
		t.Logf("Job status: %s", status)
		
		if status == "COMPLETED" {
			jobCompleted = true
			break
		}
		if status == "FAILED" || status == "CANCELLED" {
			t.Fatalf("job finished with unexpected status: %s. error: %v", status, getResp["error_message"])
		}
		time.Sleep(2 * time.Second)
	}

	require.True(t, jobCompleted, "job should complete within the timeout period")

	// 6. Get output locations
	t.Log("Job completed! Retrieving output paths...")
	var outputResp struct {
		OutputPaths []struct {
			TaskIndex  int `json:"task_index"`
			OutputPath struct {
				String string `json:"String"`
				Valid  bool   `json:"Valid"`
			} `json:"output_path"`
		} `json:"output_paths"`
	}
	err = authClient.Get(fmt.Sprintf("/jobs/%s/output", jobID), &outputResp)
	require.NoError(t, err, "get job outputs")
	require.Len(t, outputResp.OutputPaths, 1, "should have 1 output file")
	t.Logf("Job finished. Output path: %s", outputResp.OutputPaths[0].OutputPath.String)

	// 7. Cleanup
	t.Log("Cleaning up job...")
	err = authClient.Delete("/jobs/"+jobID, nil)
	require.NoError(t, err, "cleanup job should succeed")
}
