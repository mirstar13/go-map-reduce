package test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mirstar13/go-map-reduce/cmd/cli/client"
	"github.com/stretchr/testify/require"
)

// TestGraph_PageRankIteration verifies a single iteration of the PageRank algorithm.
func TestGraph_PageRankIteration(t *testing.T) {
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
	require.NoError(t, err)
	authClient := client.NewInsecure(uiURL, loginResp.AccessToken)

	// 2. Prepare tiny graph (already preprocessed format)
	// A -> B, B -> A
	// Format: rank|distance|label|neighbors
	inputContent := "A\t1.0|INF|A|B\nB\t1.0|INF|B|A\n"
	inputPath := filepath.Join(t.TempDir(), "graph.txt")
	err = os.WriteFile(inputPath, []byte(inputContent), 0644)
	require.NoError(t, err)

	// 3. Compile PageRank plugins
	t.Log("Compiling PageRank plugins...")
	mapperBin := compileExamplePlugin(t, "pagerank-mapper", "../examples/graph/pagerank/mapper.go", true)
	reducerBin := compileExamplePlugin(t, "pagerank-reducer", "../examples/graph/pagerank/reducer.go", false)

	// 4. Upload files
	t.Log("Uploading files...")
	var inputUploadResp struct{ Path string `json:"path"` }
	err = authClient.UploadFile("/files/input", inputPath, &inputUploadResp)
	require.NoError(t, err)

	var mapperUploadResp struct{ Path string `json:"path"` }
	err = authClient.UploadFile("/files/code", mapperBin, &mapperUploadResp)
	require.NoError(t, err)

	var reducerUploadResp struct{ Path string `json:"path"` }
	err = authClient.UploadFile("/files/code", reducerBin, &reducerUploadResp)
	require.NoError(t, err)

	// 5. Submit Job
	t.Log("Submitting PageRank job...")
	submitReq := map[string]any{
		"input_path":   inputUploadResp.Path,
		"mapper_path":  mapperUploadResp.Path,
		"reducer_path": reducerUploadResp.Path,
		"num_mappers":  1,
		"num_reducers": 1,
		"input_format": "text",
	}
	var jobResp struct{ JobID string `json:"job_id"` }
	err = authClient.Post("/jobs", submitReq, &jobResp)
	require.NoError(t, err)
	jobID := jobResp.JobID
	t.Logf("Job submitted: %s", jobID)

	// 6. Poll for completion
	for i := 0; i < 60; i++ {
		var getResp map[string]any
		_ = authClient.Get("/jobs/"+jobID, &getResp)
		if getResp["status"] == "COMPLETED" {
			break
		}
		if i == 59 {
			t.Fatal("Job timed out")
		}
		time.Sleep(2 * time.Second)
	}

	// 7. Verify outputs exist
	var outputResp struct {
		OutputPaths []struct {
			TaskIndex int `json:"task_index"`
		} `json:"output_paths"`
	}
	err = authClient.Get(fmt.Sprintf("/jobs/%s/output", jobID), &outputResp)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(outputResp.OutputPaths), 1)

	// Cleanup
	_ = authClient.Delete("/jobs/"+jobID, nil)
}

// TestGraph_Preprocessor verifies the preprocessing step from raw edge list.
func TestGraph_Preprocessor(t *testing.T) {
	uiURL, username, password := requireE2E(t)

	c := client.NewInsecure(uiURL, "")
	var loginResp struct{ AccessToken string `json:"access_token"` }
	_ = c.PostNoAuth("/auth/login", map[string]string{"username": username, "password": password}, &loginResp)
	authClient := client.NewInsecure(uiURL, loginResp.AccessToken)

	// 1. Raw Edge List: 1 follows 2, 2 follows 3
	inputContent := "1 2\n2 3\n"
	inputPath := filepath.Join(t.TempDir(), "edges.txt")
	err := os.WriteFile(inputPath, []byte(inputContent), 0644)
	require.NoError(t, err)

	// 2. Compile Preprocessor plugins
	mapperBin := compileExamplePlugin(t, "prep-mapper", "../examples/graph/preprocessor/mapper.go", true)
	reducerBin := compileExamplePlugin(t, "prep-reducer", "../examples/graph/preprocessor/reducer.go", false)

	// 3. Upload and Submit
	var inputUploadResp struct{ Path string `json:"path"` }
	_ = authClient.UploadFile("/files/input", inputPath, &inputUploadResp)
	var mapperUploadResp struct{ Path string `json:"path"` }
	_ = authClient.UploadFile("/files/code", mapperBin, &mapperUploadResp)
	var reducerUploadResp struct{ Path string `json:"path"` }
	_ = authClient.UploadFile("/files/code", reducerBin, &reducerUploadResp)

	submitReq := map[string]any{
		"input_path":   inputUploadResp.Path,
		"mapper_path":  mapperUploadResp.Path,
		"reducer_path": reducerUploadResp.Path,
		"num_mappers":  1,
		"num_reducers": 1,
		"input_format": "text",
	}
	var jobResp struct{ JobID string `json:"job_id"` }
	err = authClient.Post("/jobs", submitReq, &jobResp)
	require.NoError(t, err)
	jobID := jobResp.JobID

	// 4. Poll for completion
	for i := 0; i < 60; i++ {
		var getResp map[string]any
		_ = authClient.Get("/jobs/"+jobID, &getResp)
		if getResp["status"] == "COMPLETED" {
			break
		}
		time.Sleep(2 * time.Second)
	}

	// 5. Cleanup
	_ = authClient.Delete("/jobs/"+jobID, nil)
}
