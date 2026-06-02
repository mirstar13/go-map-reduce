package workflow

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/mirstar13/go-map-reduce/db"
)

func TestResolvePlaceholders(t *testing.T) {
	logger := zap.NewNop()

	t.Run("no placeholders", func(t *testing.T) {
		ctrl := NewController(nil, nil, logger)
		input := "/data/input.txt"
		jobsByStageName := map[string]db.Job{}
		resolved, err := ctrl.resolvePlaceholders(input, jobsByStageName)
		require.NoError(t, err)
		require.Equal(t, input, resolved)
	})

	t.Run("single placeholder", func(t *testing.T) {
		ctrl := NewController(nil, nil, logger)
		input := "${stage1.output_path}/part-*"
		jobsByStageName := map[string]db.Job{
			"stage1": {
				Status:     "COMPLETED",
				OutputPath: "/output/stage1",
			},
		}
		resolved, err := ctrl.resolvePlaceholders(input, jobsByStageName)
		require.NoError(t, err)
		require.Equal(t, "/output/stage1/part-*", resolved)
	})

	t.Run("multiple placeholders", func(t *testing.T) {
		ctrl := NewController(nil, nil, logger)
		input := "${stage1.output_path},${stage2.output_path}"
		jobsByStageName := map[string]db.Job{
			"stage1": {
				Status:     "COMPLETED",
				OutputPath: "/output/s1",
			},
			"stage2": {
				Status:     "COMPLETED",
				OutputPath: "/output/s2",
			},
		}
		resolved, err := ctrl.resolvePlaceholders(input, jobsByStageName)
		require.NoError(t, err)
		require.Equal(t, "/output/s1,/output/s2", resolved)
	})

	t.Run("stage not completed", func(t *testing.T) {
		ctrl := NewController(nil, nil, logger)
		input := "${stage1.output_path}"
		jobsByStageName := map[string]db.Job{
			"stage1": {
				Status: "RUNNING",
			},
		}
		_, err := ctrl.resolvePlaceholders(input, jobsByStageName)
		require.Error(t, err)
		require.Contains(t, err.Error(), "is not completed")
	})

	t.Run("stage not found", func(t *testing.T) {
		ctrl := NewController(nil, nil, logger)
		input := "${missing.output_path}"
		jobsByStageName := map[string]db.Job{}
		_, err := ctrl.resolvePlaceholders(input, jobsByStageName)
		require.Error(t, err)
	})
}
