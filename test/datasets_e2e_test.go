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

// waitForJob polls the job status until it reaches a terminal state or times out.
func waitForJob(t *testing.T, authClient *client.Client, jobID string) string {
	t.Helper()
	maxRetries := 60
	for i := 0; i < maxRetries; i++ {
		var getResp map[string]any
		err := authClient.Get("/jobs/"+jobID, &getResp)
		require.NoError(t, err)
		
		status := getResp["status"].(string)
		t.Logf("Job %s status: %s", jobID, status)
		
		if status == "COMPLETED" {
			return "COMPLETED"
		}
		if status == "FAILED" || status == "CANCELLED" {
			t.Fatalf("job %s finished with unexpected status: %s", jobID, status)
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("job %s timed out", jobID)
	return "TIMEOUT"
}

func TestE2E_Dataset_StackOverflow_Reputation(t *testing.T) {
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
	authClient := client.NewInsecure(uiURL, loginResp.AccessToken)

	// 2. Prepare small interaction data
	// Format: source_id target_id timestamp
	inputContent := "1 10 1217567877\n2 10 1217567878\n3 20 1217567879\n"
	inputPath := filepath.Join(t.TempDir(), "interactions.txt")
	err = os.WriteFile(inputPath, []byte(inputContent), 0644)
	require.NoError(t, err)

	// 3. Compile plugins
	t.Log("Compiling Stack Overflow Reputation plugins...")
	mapperBin := compileExamplePlugin(t, "so-reputation-mapper", "../examples/sx-stackoverflow/mapper.go", true)
	reducerBin := compileExamplePlugin(t, "so-reputation-reducer", "../examples/sx-stackoverflow/reducer.go", false)

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
	t.Log("Submitting Reputation job...")
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

	// 6. Wait
	waitForJob(t, authClient, jobID)

	// 7. Cleanup
	_ = authClient.Delete("/jobs/"+jobID, nil)
}

func TestE2E_Dataset_GooglePlus_MultiFile(t *testing.T) {
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
	authClient := client.NewInsecure(uiURL, loginResp.AccessToken)

	// 2. Prepare multiple small edge files
	tmpDir := t.TempDir()
	file1 := filepath.Join(tmpDir, "file1.edges")
	os.WriteFile(file1, []byte("1 2\n2 3\n"), 0644)
	file2 := filepath.Join(tmpDir, "file2.edges")
	os.WriteFile(file2, []byte("3 4\n4 1\n"), 0644)

	// 3. Upload to a common prefix
	prefix := fmt.Sprintf("gplus-test-%d", time.Now().Unix())
	var upload1, upload2 struct{ Path string `json:"path"` }
	err = authClient.UploadFile("/files/input?prefix="+prefix, file1, &upload1)
	require.NoError(t, err)
	err = authClient.UploadFile("/files/input?prefix="+prefix, file2, &upload2)
	require.NoError(t, err)

	// 4. Compile plugins
	t.Log("Compiling Google+ Degree Distribution plugins...")
	mapperBin := compileExamplePlugin(t, "gplus-degree-mapper", "../examples/googleplus/mapper.go", true)
	reducerBin := compileExamplePlugin(t, "gplus-degree-reducer", "../examples/googleplus/reducer.go", false)

	// 5. Upload binaries
	var mapperUploadResp struct{ Path string `json:"path"` }
	err = authClient.UploadFile("/files/code", mapperBin, &mapperUploadResp)
	require.NoError(t, err)

	var reducerUploadResp struct{ Path string `json:"path"` }
	err = authClient.UploadFile("/files/code", reducerBin, &reducerUploadResp)
	require.NoError(t, err)

	// 6. Submit Job pointing to the PREFIX directory
	inputPath := filepath.ToSlash(filepath.Dir(upload1.Path))
	t.Logf("Submitting G+ job for directory: %s", inputPath)
	
	submitReq := map[string]any{
		"input_path":   inputPath,
		"mapper_path":  mapperUploadResp.Path,
		"reducer_path": reducerUploadResp.Path,
		"num_mappers":  2, // Should process both files
		"num_reducers": 1,
		"input_format": "text",
	}
	var jobResp struct{ JobID string `json:"job_id"` }
	err = authClient.Post("/jobs", submitReq, &jobResp)
	require.NoError(t, err)
	jobID := jobResp.JobID

	// 7. Wait
	waitForJob(t, authClient, jobID)

	// 8. Cleanup
	_ = authClient.Delete("/jobs/"+jobID, nil)
}
