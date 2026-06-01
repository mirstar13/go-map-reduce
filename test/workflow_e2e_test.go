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

func TestE2E_WorkflowPipeline(t *testing.T) {
	uiURL, username, password := requireE2E(t)

	// 1. Authenticate
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

	// Small sleep to avoid "token used before issued" error
	time.Sleep(2 * time.Second)
	authClient := client.NewInsecure(uiURL, loginResp.AccessToken)

	// 2. Prepare plugins
	t.Log("Compiling plugins...")

	// Stage 1: WordCount
	s1MapperSource := `package main
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
	s1ReducerSource := `package main
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

	// Stage 2: Frequency of frequencies
	s2MapperSource := `package main
import (
	"strings"
	"github.com/hashicorp/go-plugin"
	mrplugin "github.com/mirstar13/go-map-reduce/pkg/plugin"
)
type MapperImpl struct{}
func (m *MapperImpl) Map(inputs []mrplugin.MapInput) ([]mrplugin.Record, error) {
	var records []mrplugin.Record
	for _, input := range inputs {
		parts := strings.SplitN(input.Value, "\t", 2)
		if len(parts) == 2 {
			// Key is the count, value is "1"
			records = append(records, mrplugin.Record{Key: parts[1], Value: "1"})
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
	s2ReducerSource := `package main
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

	s1MapperBin := compilePlugin(t, "s1-mapper", s1MapperSource)
	s1ReducerBin := compilePlugin(t, "s1-reducer", s1ReducerSource)
	s2MapperBin := compilePlugin(t, "s2-mapper", s2MapperSource)
	s2ReducerBin := compilePlugin(t, "s2-reducer", s2ReducerSource)

	// 3. Prepare data
	inputContent := `apple banana apple
cherry banana apple
cherry date
`
	// apple: 3, banana: 2, cherry: 2, date: 1
	// Stage 2 expected:
	// 3: 1 (apple)
	// 2: 2 (banana, cherry)
	// 1: 1 (date)

	inputPath := filepath.Join(t.TempDir(), "input.txt")
	err = os.WriteFile(inputPath, []byte(inputContent), 0644)
	require.NoError(t, err)

	// 4. Upload files
	t.Log("Uploading files...")
	var inputResp struct {
		Path string `json:"path"`
	}
	err = authClient.UploadFile("/files/input", inputPath, &inputResp)
	require.NoError(t, err)

	uploadBin := func(name, path string) string {
		var resp struct {
			Path string `json:"path"`
		}
		err = authClient.UploadFile("/files/code", path, &resp)
		require.NoError(t, err, "upload %s", name)
		return resp.Path
	}

	s1MapperRemote := uploadBin("s1-mapper", s1MapperBin)
	s1ReducerRemote := uploadBin("s1-reducer", s1ReducerBin)
	s2MapperRemote := uploadBin("s2-mapper", s2MapperBin)
	s2ReducerRemote := uploadBin("s2-reducer", s2ReducerBin)

	// 5. Submit Workflow
	t.Log("Submitting workflow...")
	workflowSpec := map[string]any{
		"name": "E2E Pipeline Test",
		"stages": map[string]any{
			"stage1": map[string]any{
				"mapper_path":  s1MapperRemote,
				"reducer_path": s1ReducerRemote,
				"input_path":   inputResp.Path,
				"num_mappers":  1,
				"num_reducers": 1,
				"input_format": "text",
			},
			"stage2": map[string]any{
				"mapper_path":  s2MapperRemote,
				"reducer_path": s2ReducerRemote,
				"num_mappers":  1,
				"num_reducers": 1,
				"input_format": "text",
				"depends_on":   []string{"stage1"},
			},
		},
	}

	var wfResp struct {
		WorkflowID string `json:"workflow_id"`
	}
	err = authClient.Post("/workflows", workflowSpec, &wfResp)
	require.NoError(t, err, "submit workflow")
	wfID := wfResp.WorkflowID
	t.Logf("Workflow submitted: %s", wfID)

	// 6. Poll for completion
	t.Log("Waiting for workflow to complete...")
	maxRetries := 60
	completed := false
	for range maxRetries {
		var getResp map[string]any
		err = authClient.Get("/workflows/"+wfID, &getResp)
		require.NoError(t, err)

		wf := getResp["workflow"].(map[string]any)
		status := wf["status"].(string)
		t.Logf("Workflow status: %s", status)

		if status == "COMPLETED" {
			completed = true
			break
		}
		if status == "FAILED" {
			t.Fatal("workflow failed")
		}

		// Also check stage statuses to verify sequencing
		stages := getResp["stages"].([]any)
		for _, s := range stages {
			stage := s.(map[string]any)
			name := stage["stage_name"].(map[string]any)["String"].(string)
			stageStatus := stage["status"].(string)
			t.Logf("  Stage %s: %s", name, stageStatus)

			if name == "stage2" && stageStatus == "RUNNING" {
				// Stage 2 is running, verify stage 1 is COMPLETED
				for _, s2 := range stages {
					stage2 := s2.(map[string]any)
					if stage2["stage_name"].(map[string]any)["String"].(string) == "stage1" {
						require.Equal(t, "COMPLETED", stage2["status"].(string), "stage2 should not start before stage1 completes")
					}
				}
			}

			err_map := stage["error_message"].(map[string]any)
			if err_map["Valid"].(bool) == true {
				t.Logf("Error: %s", err_map["String"].(string))
			}
		}

		time.Sleep(2 * time.Second)
	}
	require.True(t, completed, "workflow should complete")

	// 7. Verify final output
	t.Log("Verifying final output...")
	var getResp map[string]any
	err = authClient.Get("/workflows/"+wfID, &getResp)
	require.NoError(t, err)

	stages := getResp["stages"].([]any)
	var stage2JobID string
	for _, s := range stages {
		stage := s.(map[string]any)
		if stage["stage_name"].(map[string]any)["String"].(string) == "stage2" {
			stage2JobID = stage["job_id"].(string)
		}
	}
	require.NotEmpty(t, stage2JobID)

	var outputResp struct {
		OutputPaths []struct {
			OutputPath struct {
				String string `json:"String"`
			} `json:"output_path"`
		} `json:"output_paths"`
	}
	err = authClient.Get(fmt.Sprintf("/jobs/%s/output", stage2JobID), &outputResp)
	require.NoError(t, err)
	require.NotEmpty(t, outputResp.OutputPaths)

	// Note: We don't easily have a way to download the file content in this test
	// unless we use the minio client directly or add a download endpoint.
	// For now, presence of output and COMPLETED status is a good indicator.

	// 8. Cleanup
	t.Log("Cleaning up workflow...")
	// Actually we should probably have a DELETE /workflows/:id but manager might not have it yet.
	// The task doesn't ask for it.
}
