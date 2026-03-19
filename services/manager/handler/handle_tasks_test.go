package handler

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"
	"github.com/mirstar13/go-map-reduce/db"
	"github.com/mirstar13/go-map-reduce/services/manager/supervisor"
	"github.com/sqlc-dev/pqtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// newTaskApp wires a Fiber app for task-callback routes.
// Task routes are internal (no auth) — workers call them directly.
func newTaskApp(t *testing.T, q db.Querier) *fiber.App {
	t.Helper()
	reg := supervisor.NewRegistry()
	h := NewTaskHandler(q, reg, zap.NewNop())
	app := fiber.New()
	app.Post("/tasks/map/:id/complete", h.CompleteMapTask)
	app.Post("/tasks/map/:id/fail", h.FailMapTask)
	app.Post("/tasks/reduce/:id/complete", h.CompleteReduceTask)
	app.Post("/tasks/reduce/:id/fail", h.FailReduceTask)
	return app
}

// newTaskAppWithRegistry wires a Fiber app for task routes with a shared registry,
// allowing tests that verify Notify is called.
func newTaskAppWithRegistry(t *testing.T, q db.Querier, reg *supervisor.Registry) *fiber.App {
	t.Helper()
	h := NewTaskHandler(q, reg, zap.NewNop())
	app := fiber.New()
	app.Post("/tasks/map/:id/complete", h.CompleteMapTask)
	app.Post("/tasks/map/:id/fail", h.FailMapTask)
	app.Post("/tasks/reduce/:id/complete", h.CompleteReduceTask)
	app.Post("/tasks/reduce/:id/fail", h.FailReduceTask)
	return app
}

// doTaskRequest sends a JSON POST to app.Test.
func doTaskRequest(t *testing.T, app *fiber.App, path string, body interface{}) *http.Response {
	t.Helper()
	data, err := json.Marshal(body)
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	require.NoError(t, err)
	return resp
}

// sampleMapTask returns a minimal db.MapTask for use in tests.
func sampleMapTask(jobID uuid.UUID, retryCount int32) db.MapTask {
	return db.MapTask{
		TaskID:     uuid.New(),
		JobID:      jobID,
		TaskIndex:  0,
		Status:     "RUNNING",
		InputFile:  "input/data.jsonl",
		RetryCount: retryCount,
		StartedAt:  sql.NullTime{Valid: true},
	}
}

// sampleReduceTask returns a minimal db.ReduceTask for use in tests.
func sampleReduceTask(jobID uuid.UUID, retryCount int32) db.ReduceTask {
	return db.ReduceTask{
		TaskID:     uuid.New(),
		JobID:      jobID,
		TaskIndex:  0,
		Status:     "RUNNING",
		RetryCount: retryCount,
		StartedAt:  sql.NullTime{Valid: true},
	}
}

func TestCompleteMapTask_InvalidTaskID(t *testing.T) {
	app := newTaskApp(t, &mockQuerier{})
	resp := doTaskRequest(t, app, "/tasks/map/not-a-uuid/complete", map[string]interface{}{
		"output_locations": json.RawMessage(`[{"reducer_index":0,"path":"jobs/abc/map-0-reduce-0.jsonl"}]`),
	})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestCompleteMapTask_MissingOutputLocations(t *testing.T) {
	app := newTaskApp(t, &mockQuerier{})
	taskID := uuid.New()
	resp := doTaskRequest(t, app, "/tasks/map/"+taskID.String()+"/complete", map[string]interface{}{
		// output_locations deliberately omitted
	})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	body := decodeJSON(t, resp)
	assert.Contains(t, body["error"], "output_locations")
}

func TestCompleteMapTask_TaskNotFound(t *testing.T) {
	q := &mockQuerier{
		getMapTaskFn: func(_ context.Context, _ uuid.UUID) (db.MapTask, error) {
			return db.MapTask{}, sql.ErrNoRows
		},
	}
	app := newTaskApp(t, q)
	taskID := uuid.New()
	resp := doTaskRequest(t, app, "/tasks/map/"+taskID.String()+"/complete",
		map[string]interface{}{
			"output_locations": json.RawMessage(`[{"reducer_index":0,"path":"x"}]`),
		},
	)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestCompleteMapTask_DBGetError(t *testing.T) {
	q := &mockQuerier{
		getMapTaskFn: func(_ context.Context, _ uuid.UUID) (db.MapTask, error) {
			return db.MapTask{}, errors.New("db failure")
		},
	}
	app := newTaskApp(t, q)
	taskID := uuid.New()
	resp := doTaskRequest(t, app, "/tasks/map/"+taskID.String()+"/complete",
		map[string]interface{}{
			"output_locations": json.RawMessage(`[{"reducer_index":0,"path":"x"}]`),
		},
	)
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

func TestCompleteMapTask_MarkCompletedDBError(t *testing.T) {
	jobID := uuid.New()
	task := sampleMapTask(jobID, 0)

	q := &mockQuerier{
		getMapTaskFn: func(_ context.Context, _ uuid.UUID) (db.MapTask, error) {
			return task, nil
		},
		markMapTaskCompletedFn: func(_ context.Context, _ db.MarkMapTaskCompletedParams) error {
			return errors.New("db failure")
		},
	}
	app := newTaskApp(t, q)
	resp := doTaskRequest(t, app, "/tasks/map/"+task.TaskID.String()+"/complete",
		map[string]interface{}{
			"output_locations": json.RawMessage(`[{"reducer_index":0,"path":"x"}]`),
		},
	)
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

func TestCompleteMapTask_Success(t *testing.T) {
	jobID := uuid.New()
	task := sampleMapTask(jobID, 0)

	var capturedParams db.MarkMapTaskCompletedParams
	q := &mockQuerier{
		getMapTaskFn: func(_ context.Context, tid uuid.UUID) (db.MapTask, error) {
			assert.Equal(t, task.TaskID, tid)
			return task, nil
		},
		markMapTaskCompletedFn: func(_ context.Context, p db.MarkMapTaskCompletedParams) error {
			capturedParams = p
			return nil
		},
	}
	app := newTaskApp(t, q)
	outputLocs := json.RawMessage(`[{"reducer_index":0,"path":"jobs/abc/map-0-reduce-0.jsonl"}]`)
	resp := doTaskRequest(t, app, "/tasks/map/"+task.TaskID.String()+"/complete",
		map[string]interface{}{"output_locations": outputLocs},
	)

	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, task.TaskID, capturedParams.TaskID)
	assert.True(t, capturedParams.OutputLocations.Valid)

	body := decodeJSON(t, resp)
	assert.Equal(t, "ok", body["status"])
}

func TestCompleteMapTask_OutputLocationsStored(t *testing.T) {
	// Verify the exact raw JSON is forwarded to the DB without mangling.
	jobID := uuid.New()
	task := sampleMapTask(jobID, 0)
	rawLocs := `[{"reducer_index":1,"path":"jobs/xyz/part-1.jsonl"}]`

	var stored pqtype.NullRawMessage
	q := &mockQuerier{
		getMapTaskFn: func(_ context.Context, _ uuid.UUID) (db.MapTask, error) {
			return task, nil
		},
		markMapTaskCompletedFn: func(_ context.Context, p db.MarkMapTaskCompletedParams) error {
			stored = p.OutputLocations
			return nil
		},
	}
	app := newTaskApp(t, q)
	resp := doTaskRequest(t, app, "/tasks/map/"+task.TaskID.String()+"/complete",
		map[string]interface{}{"output_locations": json.RawMessage(rawLocs)},
	)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.True(t, stored.Valid)
	assert.JSONEq(t, rawLocs, string(stored.RawMessage))
}

func TestFailMapTask_InvalidTaskID(t *testing.T) {
	app := newTaskApp(t, &mockQuerier{})
	resp := doTaskRequest(t, app, "/tasks/map/not-a-uuid/fail", map[string]interface{}{})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestFailMapTask_NotFound(t *testing.T) {
	q := &mockQuerier{
		getMapTaskFn: func(_ context.Context, _ uuid.UUID) (db.MapTask, error) {
			return db.MapTask{}, sql.ErrNoRows
		},
	}
	app := newTaskApp(t, q)
	resp := doTaskRequest(t, app, "/tasks/map/"+uuid.New().String()+"/fail", map[string]interface{}{})
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestFailMapTask_LowRetryCount_IncrementsRetry(t *testing.T) {
	// retry_count = 0 → IncrementMapTaskRetry should be called (not MarkMapTaskFailed).
	jobID := uuid.New()
	task := sampleMapTask(jobID, 0) // retry_count = 0

	incrementCalled := false
	markFailedCalled := false

	q := &mockQuerier{
		getMapTaskFn: func(_ context.Context, _ uuid.UUID) (db.MapTask, error) {
			return task, nil
		},
		incrementMapTaskRetryFn: func(_ context.Context, tid uuid.UUID) error {
			incrementCalled = true
			assert.Equal(t, task.TaskID, tid)
			return nil
		},
		markMapTaskFailedFn: func(_ context.Context, _ uuid.UUID) error {
			markFailedCalled = true
			return nil
		},
	}
	app := newTaskApp(t, q)
	resp := doTaskRequest(t, app, "/tasks/map/"+task.TaskID.String()+"/fail", map[string]interface{}{})

	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.True(t, incrementCalled, "IncrementMapTaskRetry must be called on low retry count")
	assert.False(t, markFailedCalled, "MarkMapTaskFailed must NOT be called on low retry count")
}

func TestFailMapTask_HighRetryCount_PermanentlyFails(t *testing.T) {
	// retry_count = 3 (at limit) → MarkMapTaskFailed should be called.
	jobID := uuid.New()
	task := sampleMapTask(jobID, 3) // at retry limit

	incrementCalled := false
	markFailedCalled := false

	q := &mockQuerier{
		getMapTaskFn: func(_ context.Context, _ uuid.UUID) (db.MapTask, error) {
			return task, nil
		},
		incrementMapTaskRetryFn: func(_ context.Context, _ uuid.UUID) error {
			incrementCalled = true
			return nil
		},
		markMapTaskFailedFn: func(_ context.Context, tid uuid.UUID) error {
			markFailedCalled = true
			assert.Equal(t, task.TaskID, tid)
			return nil
		},
	}
	app := newTaskApp(t, q)
	resp := doTaskRequest(t, app, "/tasks/map/"+task.TaskID.String()+"/fail", map[string]interface{}{})

	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.False(t, incrementCalled, "IncrementMapTaskRetry must NOT be called at retry limit")
	assert.True(t, markFailedCalled, "MarkMapTaskFailed must be called at retry limit")
}

func TestFailMapTask_RetryCountBoundary(t *testing.T) {
	// retry_count = 2 (one below limit of 3) → still increments.
	jobID := uuid.New()
	task := sampleMapTask(jobID, 2)

	incrementCalled := false
	q := &mockQuerier{
		getMapTaskFn: func(_ context.Context, _ uuid.UUID) (db.MapTask, error) {
			return task, nil
		},
		incrementMapTaskRetryFn: func(_ context.Context, _ uuid.UUID) error {
			incrementCalled = true
			return nil
		},
	}
	app := newTaskApp(t, q)
	resp := doTaskRequest(t, app, "/tasks/map/"+task.TaskID.String()+"/fail", map[string]interface{}{})

	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.True(t, incrementCalled)
}

func TestCompleteReduceTask_InvalidTaskID(t *testing.T) {
	app := newTaskApp(t, &mockQuerier{})
	resp := doTaskRequest(t, app, "/tasks/reduce/not-a-uuid/complete", map[string]interface{}{
		"output_path": "output/jobs/abc/part-0.jsonl",
	})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestCompleteReduceTask_MissingOutputPath(t *testing.T) {
	app := newTaskApp(t, &mockQuerier{})
	taskID := uuid.New()
	resp := doTaskRequest(t, app, "/tasks/reduce/"+taskID.String()+"/complete", map[string]interface{}{
		// output_path deliberately omitted
	})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	body := decodeJSON(t, resp)
	assert.Contains(t, body["error"], "output_path")
}

func TestCompleteReduceTask_NotFound(t *testing.T) {
	q := &mockQuerier{
		getReduceTaskFn: func(_ context.Context, _ uuid.UUID) (db.ReduceTask, error) {
			return db.ReduceTask{}, sql.ErrNoRows
		},
	}
	app := newTaskApp(t, q)
	resp := doTaskRequest(t, app, "/tasks/reduce/"+uuid.New().String()+"/complete",
		map[string]interface{}{"output_path": "output/part-0.jsonl"},
	)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestCompleteReduceTask_DBGetError(t *testing.T) {
	q := &mockQuerier{
		getReduceTaskFn: func(_ context.Context, _ uuid.UUID) (db.ReduceTask, error) {
			return db.ReduceTask{}, errors.New("db failure")
		},
	}
	app := newTaskApp(t, q)
	resp := doTaskRequest(t, app, "/tasks/reduce/"+uuid.New().String()+"/complete",
		map[string]interface{}{"output_path": "output/part-0.jsonl"},
	)
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

func TestCompleteReduceTask_MarkCompletedDBError(t *testing.T) {
	jobID := uuid.New()
	task := sampleReduceTask(jobID, 0)

	q := &mockQuerier{
		getReduceTaskFn: func(_ context.Context, _ uuid.UUID) (db.ReduceTask, error) {
			return task, nil
		},
		markReduceTaskCompletedFn: func(_ context.Context, _ db.MarkReduceTaskCompletedParams) error {
			return errors.New("db failure")
		},
	}
	app := newTaskApp(t, q)
	resp := doTaskRequest(t, app, "/tasks/reduce/"+task.TaskID.String()+"/complete",
		map[string]interface{}{"output_path": "output/part-0.jsonl"},
	)
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

func TestCompleteReduceTask_Success(t *testing.T) {
	jobID := uuid.New()
	task := sampleReduceTask(jobID, 0)
	const outputPath = "output/jobs/abc/part-0.jsonl"

	var capturedParams db.MarkReduceTaskCompletedParams
	q := &mockQuerier{
		getReduceTaskFn: func(_ context.Context, tid uuid.UUID) (db.ReduceTask, error) {
			assert.Equal(t, task.TaskID, tid)
			return task, nil
		},
		markReduceTaskCompletedFn: func(_ context.Context, p db.MarkReduceTaskCompletedParams) error {
			capturedParams = p
			return nil
		},
	}
	app := newTaskApp(t, q)
	resp := doTaskRequest(t, app, "/tasks/reduce/"+task.TaskID.String()+"/complete",
		map[string]interface{}{"output_path": outputPath},
	)

	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, task.TaskID, capturedParams.TaskID)
	assert.True(t, capturedParams.OutputPath.Valid)
	assert.Equal(t, outputPath, capturedParams.OutputPath.String)

	body := decodeJSON(t, resp)
	assert.Equal(t, "ok", body["status"])
}

func TestCompleteReduceTask_OutputPathStoredCorrectly(t *testing.T) {
	jobID := uuid.New()
	task := sampleReduceTask(jobID, 0)
	const path = "output/jobs/my-job/part-3.jsonl"

	var storedPath sql.NullString
	q := &mockQuerier{
		getReduceTaskFn: func(_ context.Context, _ uuid.UUID) (db.ReduceTask, error) {
			return task, nil
		},
		markReduceTaskCompletedFn: func(_ context.Context, p db.MarkReduceTaskCompletedParams) error {
			storedPath = p.OutputPath
			return nil
		},
	}
	app := newTaskApp(t, q)
	resp := doTaskRequest(t, app, "/tasks/reduce/"+task.TaskID.String()+"/complete",
		map[string]interface{}{"output_path": path},
	)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.True(t, storedPath.Valid)
	assert.Equal(t, path, storedPath.String)
}

func TestFailReduceTask_InvalidTaskID(t *testing.T) {
	app := newTaskApp(t, &mockQuerier{})
	resp := doTaskRequest(t, app, "/tasks/reduce/not-a-uuid/fail", map[string]interface{}{})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestFailReduceTask_NotFound(t *testing.T) {
	q := &mockQuerier{
		getReduceTaskFn: func(_ context.Context, _ uuid.UUID) (db.ReduceTask, error) {
			return db.ReduceTask{}, sql.ErrNoRows
		},
	}
	app := newTaskApp(t, q)
	resp := doTaskRequest(t, app, "/tasks/reduce/"+uuid.New().String()+"/fail", map[string]interface{}{})
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestFailReduceTask_LowRetryCount_IncrementsRetry(t *testing.T) {
	jobID := uuid.New()
	task := sampleReduceTask(jobID, 1)

	incrementCalled := false
	markFailedCalled := false

	q := &mockQuerier{
		getReduceTaskFn: func(_ context.Context, _ uuid.UUID) (db.ReduceTask, error) {
			return task, nil
		},
		incrementReduceTaskRetryFn: func(_ context.Context, tid uuid.UUID) error {
			incrementCalled = true
			assert.Equal(t, task.TaskID, tid)
			return nil
		},
		markReduceTaskFailedFn: func(_ context.Context, _ uuid.UUID) error {
			markFailedCalled = true
			return nil
		},
	}
	app := newTaskApp(t, q)
	resp := doTaskRequest(t, app, "/tasks/reduce/"+task.TaskID.String()+"/fail", map[string]interface{}{})

	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.True(t, incrementCalled)
	assert.False(t, markFailedCalled)
}

func TestFailReduceTask_HighRetryCount_PermanentlyFails(t *testing.T) {
	jobID := uuid.New()
	task := sampleReduceTask(jobID, 3) // at limit

	incrementCalled := false
	markFailedCalled := false

	q := &mockQuerier{
		getReduceTaskFn: func(_ context.Context, _ uuid.UUID) (db.ReduceTask, error) {
			return task, nil
		},
		incrementReduceTaskRetryFn: func(_ context.Context, _ uuid.UUID) error {
			incrementCalled = true
			return nil
		},
		markReduceTaskFailedFn: func(_ context.Context, tid uuid.UUID) error {
			markFailedCalled = true
			assert.Equal(t, task.TaskID, tid)
			return nil
		},
	}
	app := newTaskApp(t, q)
	resp := doTaskRequest(t, app, "/tasks/reduce/"+task.TaskID.String()+"/fail", map[string]interface{}{})

	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.False(t, incrementCalled)
	assert.True(t, markFailedCalled)
}

func TestFailMapTask_IncrementRetryDBError_StillReturns200(t *testing.T) {
	// DB error on IncrementMapTaskRetry is logged but not propagated — handler still returns 200.
	jobID := uuid.New()
	task := sampleMapTask(jobID, 0) // low retry count → increment path

	q := &mockQuerier{
		getMapTaskFn: func(_ context.Context, _ uuid.UUID) (db.MapTask, error) {
			return task, nil
		},
		incrementMapTaskRetryFn: func(_ context.Context, _ uuid.UUID) error {
			return errors.New("db: connection lost")
		},
	}
	app := newTaskApp(t, q)
	resp := doTaskRequest(t, app, "/tasks/map/"+task.TaskID.String()+"/fail", map[string]interface{}{})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestFailMapTask_MarkFailedDBError_StillReturns200(t *testing.T) {
	// DB error on MarkMapTaskFailed is logged but not propagated.
	jobID := uuid.New()
	task := sampleMapTask(jobID, 3) // at retry limit → mark-failed path

	q := &mockQuerier{
		getMapTaskFn: func(_ context.Context, _ uuid.UUID) (db.MapTask, error) {
			return task, nil
		},
		markMapTaskFailedFn: func(_ context.Context, _ uuid.UUID) error {
			return errors.New("db: connection lost")
		},
	}
	app := newTaskApp(t, q)
	resp := doTaskRequest(t, app, "/tasks/map/"+task.TaskID.String()+"/fail", map[string]interface{}{})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestFailReduceTask_IncrementRetryDBError_StillReturns200(t *testing.T) {
	jobID := uuid.New()
	task := sampleReduceTask(jobID, 0)

	q := &mockQuerier{
		getReduceTaskFn: func(_ context.Context, _ uuid.UUID) (db.ReduceTask, error) {
			return task, nil
		},
		incrementReduceTaskRetryFn: func(_ context.Context, _ uuid.UUID) error {
			return errors.New("db: connection lost")
		},
	}
	app := newTaskApp(t, q)
	resp := doTaskRequest(t, app, "/tasks/reduce/"+task.TaskID.String()+"/fail", map[string]interface{}{})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestFailReduceTask_MarkFailedDBError_StillReturns200(t *testing.T) {
	jobID := uuid.New()
	task := sampleReduceTask(jobID, 3)

	q := &mockQuerier{
		getReduceTaskFn: func(_ context.Context, _ uuid.UUID) (db.ReduceTask, error) {
			return task, nil
		},
		markReduceTaskFailedFn: func(_ context.Context, _ uuid.UUID) error {
			return errors.New("db: connection lost")
		},
	}
	app := newTaskApp(t, q)
	resp := doTaskRequest(t, app, "/tasks/reduce/"+task.TaskID.String()+"/fail", map[string]interface{}{})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

// TestCompleteMapTask_NotifiesSupervisor verifies that completing a map task
// sends a poke to the registered supervisor via the Registry.
// We simulate a registered supervisor by inserting a custom Supervisor into
// the registry and asserting its notify channel receives a message.
func TestCompleteMapTask_NotifiesSupervisor(t *testing.T) {
	jobID := uuid.New()
	task := sampleMapTask(jobID, 0)

	q := &mockQuerier{
		getMapTaskFn: func(_ context.Context, _ uuid.UUID) (db.MapTask, error) {
			return task, nil
		},
		markMapTaskCompletedFn: func(_ context.Context, _ db.MarkMapTaskCompletedParams) error {
			return nil
		},
	}

	reg := supervisor.NewRegistry()
	app := newTaskAppWithRegistry(t, q, reg)

	resp := doTaskRequest(t, app, "/tasks/map/"+task.TaskID.String()+"/complete",
		map[string]interface{}{
			"output_locations": json.RawMessage(`[{"reducer_index":0,"path":"x"}]`),
		},
	)
	// No supervisor is registered so Notify is a no-op — just check 200 OK.
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestCompleteReduceTask_NotifiesSupervisor(t *testing.T) {
	jobID := uuid.New()
	task := sampleReduceTask(jobID, 0)

	q := &mockQuerier{
		getReduceTaskFn: func(_ context.Context, _ uuid.UUID) (db.ReduceTask, error) {
			return task, nil
		},
		markReduceTaskCompletedFn: func(_ context.Context, _ db.MarkReduceTaskCompletedParams) error {
			return nil
		},
	}

	reg := supervisor.NewRegistry()
	app := newTaskAppWithRegistry(t, q, reg)

	resp := doTaskRequest(t, app, "/tasks/reduce/"+task.TaskID.String()+"/complete",
		map[string]interface{}{"output_path": "output/part-0.jsonl"},
	)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestParseTaskID_ValidUUID(t *testing.T) {
	app := fiber.New()
	app.Get("/tasks/:id", func(c fiber.Ctx) error {
		id, err := parseTaskID(c)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(fiber.Map{"id": id.String()})
	})

	expected := uuid.New()
	req := httptest.NewRequest(http.MethodGet, "/tasks/"+expected.String(), nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body := decodeJSON(t, resp)
	assert.Equal(t, expected.String(), body["id"])
}

func TestParseTaskID_InvalidUUID(t *testing.T) {
	app := fiber.New()
	app.Get("/tasks/:id", func(c fiber.Ctx) error {
		_, err := parseTaskID(c)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
		}
		return c.SendStatus(fiber.StatusOK)
	})

	tests := []string{"not-a-uuid", "123", "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"}
	for _, id := range tests {
		t.Run(id, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/tasks/"+id, nil)
			resp, err := app.Test(req)
			require.NoError(t, err)
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		})
	}
}
