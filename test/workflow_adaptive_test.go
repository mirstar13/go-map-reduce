package test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mirstar13/go-map-reduce/cmd/cli/client"
	"github.com/stretchr/testify/require"
)

func TestE2E_WorkflowAdaptive(t *testing.T) {
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

	s1MapperBin := compilePlugin(t, "adaptive-s1-mapper", s1MapperSource)
	s1ReducerBin := compilePlugin(t, "adaptive-s1-reducer", s1ReducerSource)

	// 3. Prepare data
	// Small input: 3 words -> max 3 output records in stage 1.
	inputContent := `apple banana cherry
`
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

	// 5. Submit Workflow
	t.Log("Submitting workflow...")
	workflowSpec := map[string]any{
		"name": "Adaptive Workflow E2E Test",
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
				"mapper_path":  s1MapperRemote,
				"reducer_path": s1ReducerRemote,
				"num_mappers":  1,
				"num_reducers": 1,
				"input_format": "text",
				"depends_on":   []string{"stage1"},
				"condition": map[string]any{
					"metric":   "output_records",
					"operator": ">",
					"value":    100, // Should fail since stage1 only has 3 words
				},
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
	stage2Skipped := false
	for range maxRetries {
		var getResp map[string]any
		err = authClient.Get("/workflows/"+wfID, &getResp)
		require.NoError(t, err)

		wf := getResp["workflow"].(map[string]any)
		status := wf["status"].(string)
		t.Logf("Workflow status: %s", status)

		if status == "COMPLETED" {
			completed = true

			stages := getResp["stages"].([]any)
			for _, s := range stages {
				stage := s.(map[string]any)
				name := stage["stage_name"].(map[string]any)["String"].(string)
				stageStatus := stage["status"].(string)
				if name == "stage2" && stageStatus == "SKIPPED" {
					stage2Skipped = true
				}
			}
			break
		}
		if status == "FAILED" {
			t.Fatal("workflow failed")
		}

		time.Sleep(2 * time.Second)
	}
	require.True(t, completed, "workflow should complete")
	require.True(t, stage2Skipped, "stage2 should be SKIPPED due to condition")
}
