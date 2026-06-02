package test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/mirstar13/go-map-reduce/cmd/cli/client"
	"github.com/stretchr/testify/require"
)

// mapperMain is the wrapper code for mapper plugins.
const mapperMainTemplate = `package main
import (
	"github.com/hashicorp/go-plugin"
	mrplugin "github.com/mirstar13/go-map-reduce/pkg/plugin"
)
func main() {
	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: mrplugin.Handshake,
		Plugins: map[string]plugin.Plugin{
			"mapper": &mrplugin.MapperPlugin{Impl: Mapper},
		},
	})
}
`

// reducerMain is the wrapper code for reducer plugins.
const reducerMainTemplate = `package main
import (
	"github.com/hashicorp/go-plugin"
	mrplugin "github.com/mirstar13/go-map-reduce/pkg/plugin"
)
func main() {
	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: mrplugin.Handshake,
		Plugins: map[string]plugin.Plugin{
			"reducer": &mrplugin.ReducerPlugin{Impl: Reducer},
		},
	})
}
`

// compileExamplePlugin compiles an example Go source file into a Linux binary with the required main wrapper.
func compileExamplePlugin(t *testing.T, name string, sourcePath string, isMapper bool) string {
	t.Helper()
	tmpDir := t.TempDir()

	// Copy the original source
	srcContent, err := os.ReadFile(sourcePath)
	require.NoError(t, err)

	pluginSrcPath := filepath.Join(tmpDir, "plugin.go")
	err = os.WriteFile(pluginSrcPath, srcContent, 0644)
	require.NoError(t, err)

	// Write the main wrapper
	mainSrcPath := filepath.Join(tmpDir, "main.go")
	wrapper := reducerMainTemplate
	if isMapper {
		wrapper = mapperMainTemplate
	}
	err = os.WriteFile(mainSrcPath, []byte(wrapper), 0644)
	require.NoError(t, err)

	binaryPath := filepath.Join(tmpDir, name+".bin")

	// Run build from project root so imports resolve correctly
	// We pass both files to go build so they are in the same 'main' package
	cmd := exec.Command("go", "build", "-o", binaryPath, mainSrcPath, pluginSrcPath)
	cmd.Env = append(os.Environ(),
		"GOOS=linux",
		"GOARCH=amd64",
		"CGO_ENABLED=0",
	)
	cmd.Dir = ".."

	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "compile %s failed: %s", name, string(out))

	return binaryPath
}

func runExampleTest(t *testing.T, exampleName string, inputFileName string, inputFormat string) {
	uiURL, username, password := requireE2E(t)

	// 1. Authenticate
	c := client.NewInsecure(uiURL, "")
	var loginResp struct {
		AccessToken string `json:"access_token"`
	}
	err := c.PostNoAuth("/auth/login", map[string]string{
		"username": username,
		"password": password,
	}, &loginResp)
	require.NoError(t, err, "login should succeed")

	// Small sleep to avoid "token used before issued" error if cluster clock is slightly behind
	time.Sleep(5 * time.Second)

	authClient := client.NewInsecure(uiURL, loginResp.AccessToken)

	// 2. Compile plugins
	t.Logf("Compiling plugins for %s...", exampleName)
	exampleDir := filepath.Join("..", "examples", exampleName)
	mapperBin := compileExamplePlugin(t, exampleName+"-mapper", filepath.Join(exampleDir, "mapper.go"), true)
	reducerBin := compileExamplePlugin(t, exampleName+"-reducer", filepath.Join(exampleDir, "reducer.go"), false)

	// 3. Upload files
	t.Log("Uploading files...")
	var inputUploadResp struct {
		Path string `json:"path"`
	}
	err = authClient.UploadFile("/files/input", filepath.Join(exampleDir, inputFileName), &inputUploadResp)
	require.NoError(t, err, "upload input data")

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
		"num_mappers":  2,
		"num_reducers": 2,
		"input_format": inputFormat,
	}
	var jobResp struct {
		JobID string `json:"job_id"`
	}
	err = authClient.Post("/jobs", submitReq, &jobResp)
	require.NoError(t, err, "submit job")
	jobID := jobResp.JobID
	t.Logf("Job ID: %s", jobID)

	// 5. Wait for execution
	t.Log("Waiting for job execution to complete...")
	maxRetries := 60
	jobCompleted := false
	for i := 0; i < maxRetries; i++ {
		var getResp map[string]any
		err = authClient.Get("/jobs/"+jobID, &getResp)
		require.NoError(t, err)

		status := getResp["status"].(string)
		t.Logf("Job status: %s", status)

		if status == "COMPLETED" {
			jobCompleted = true
			break
		}
		if status == "FAILED" || status == "CANCELLED" {
			t.Fatalf("job finished with unexpected status: %s", status)
		}
		time.Sleep(2 * time.Second)
	}
	require.True(t, jobCompleted, "job should complete")

	// 6. Verify outputs exist
	var outputResp struct {
		OutputPaths []struct {
			TaskIndex int `json:"task_index"`
		} `json:"output_paths"`
	}
	err = authClient.Get(fmt.Sprintf("/jobs/%s/output", jobID), &outputResp)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(outputResp.OutputPaths), 1, "should have at least one output file")

	// 7. Cleanup
	t.Log("Cleaning up job...")
	err = authClient.Delete("/jobs/"+jobID, nil)
	require.NoError(t, err, "cleanup job should succeed")
}

func TestE2E_Example_WordCount(t *testing.T) {
	runExampleTest(t, "wordcount", "input.txt", "text")
}

func TestE2E_Example_InvertedIndex(t *testing.T) {
	runExampleTest(t, "inverted-index", "input.jsonl", "jsonl")
}
