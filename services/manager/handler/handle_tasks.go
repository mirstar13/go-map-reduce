package handler

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"
	"github.com/sqlc-dev/pqtype"
	"go.uber.org/zap"

	"github.com/mirstar13/go-map-reduce/db"
	"github.com/mirstar13/go-map-reduce/services/manager/supervisor"
)

// TaskHandler handles worker callback routes:
//
//	POST /tasks/:id/complete
//	POST /tasks/:id/fail
//
// Workers call these endpoints after finishing (or failing) a task.
// The handlers update the DB and notify the owning job's supervisor via the Registry.
type TaskHandler struct {
	queries  db.Querier
	registry *supervisor.Registry
	log      *zap.Logger
}

// NewTaskHandler creates a TaskHandler.
func NewTaskHandler(queries db.Querier, registry *supervisor.Registry, log *zap.Logger) *TaskHandler {
	return &TaskHandler{queries: queries, registry: registry, log: log}
}

// mapCompleteRequest is the body sent by a map worker on success.
// output_locations is a JSON array: [{"reducer_index":0,"path":"jobs/..."}, ...]
type mapCompleteRequest struct {
	OutputLocations json.RawMessage `json:"output_locations"`
}

// CompleteMapTask handles POST /tasks/map/:id/complete.
func (h *TaskHandler) CompleteMapTask(c fiber.Ctx) error {
	taskID, err := parseTaskID(c)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	var req mapCompleteRequest
	if err := c.Bind().JSON(&req); err != nil || len(req.OutputLocations) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "output_locations is required",
		})
	}

	task, err := h.queries.GetMapTask(c.Context(), taskID)
	if err != nil {
		if err == sql.ErrNoRows {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "task not found"})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "could not get task"})
	}

	if err := h.queries.MarkMapTaskCompleted(c.Context(), db.MarkMapTaskCompletedParams{
		TaskID: taskID,
		OutputLocations: pqtype.NullRawMessage{
			RawMessage: req.OutputLocations,
			Valid:      true,
		},
	}); err != nil {
		h.log.Error("mark map task completed", zap.String("task_id", taskID.String()), zap.Error(err))
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "could not update task"})
	}

	h.log.Info("map task completed",
		zap.String("task_id", taskID.String()),
		zap.String("job_id", task.JobID.String()),
	)

	h.registry.Notify(task.JobID)
	return c.Status(fiber.StatusOK).JSON(fiber.Map{"status": "ok"})
}

// FailMapTask handles POST /tasks/map/:id/fail.
func (h *TaskHandler) FailMapTask(c fiber.Ctx) error {
	taskID, err := parseTaskID(c)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	task, err := h.queries.GetMapTask(c.Context(), taskID)
	if err != nil {
		if err == sql.ErrNoRows {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "task not found"})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "could not get task"})
	}

	// Increment retry or permanently fail, depending on retry count.
	if task.RetryCount < 3 { // TaskMaxRetries will be checked by supervisor on re-dispatch
		if err := h.queries.IncrementMapTaskRetry(c.Context(), taskID); err != nil {
			h.log.Error("increment map task retry", zap.Error(err))
		}
	} else {
		if err := h.queries.MarkMapTaskFailed(c.Context(), taskID); err != nil {
			h.log.Error("mark map task failed", zap.Error(err))
		}
	}

	h.log.Warn("map task failed",
		zap.String("task_id", taskID.String()),
		zap.String("job_id", task.JobID.String()),
		zap.Int32("retry_count", task.RetryCount),
	)

	h.registry.Notify(task.JobID)
	return c.Status(fiber.StatusOK).JSON(fiber.Map{"status": "ok"})
}

// reduceCompleteRequest is the body sent by a reduce worker on success.
type reduceCompleteRequest struct {
	OutputPath string `json:"output_path"` // MinIO object key of the part file
}

// CompleteReduceTask handles POST /tasks/reduce/:id/complete.
func (h *TaskHandler) CompleteReduceTask(c fiber.Ctx) error {
	taskID, err := parseTaskID(c)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	var req reduceCompleteRequest
	if err := c.Bind().JSON(&req); err != nil || req.OutputPath == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "output_path is required",
		})
	}

	task, err := h.queries.GetReduceTask(c.Context(), taskID)
	if err != nil {
		if err == sql.ErrNoRows {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "task not found"})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "could not get task"})
	}

	if err := h.queries.MarkReduceTaskCompleted(c.Context(), db.MarkReduceTaskCompletedParams{
		TaskID:     taskID,
		OutputPath: sql.NullString{String: req.OutputPath, Valid: true},
	}); err != nil {
		h.log.Error("mark reduce task completed", zap.Error(err))
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "could not update task"})
	}

	h.log.Info("reduce task completed",
		zap.String("task_id", taskID.String()),
		zap.String("job_id", task.JobID.String()),
		zap.String("output_path", req.OutputPath),
	)

	h.registry.Notify(task.JobID)
	return c.Status(fiber.StatusOK).JSON(fiber.Map{"status": "ok"})
}

// FailReduceTask handles POST /tasks/reduce/:id/fail.
func (h *TaskHandler) FailReduceTask(c fiber.Ctx) error {
	taskID, err := parseTaskID(c)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	task, err := h.queries.GetReduceTask(c.Context(), taskID)
	if err != nil {
		if err == sql.ErrNoRows {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "task not found"})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "could not get task"})
	}

	if task.RetryCount < 3 {
		if err := h.queries.IncrementReduceTaskRetry(c.Context(), taskID); err != nil {
			h.log.Error("increment reduce task retry", zap.Error(err))
		}
	} else {
		if err := h.queries.MarkReduceTaskFailed(c.Context(), taskID); err != nil {
			h.log.Error("mark reduce task failed", zap.Error(err))
		}
	}

	h.log.Warn("reduce task failed",
		zap.String("task_id", taskID.String()),
		zap.String("job_id", task.JobID.String()),
		zap.Int32("retry_count", task.RetryCount),
	)

	h.registry.Notify(task.JobID)
	return c.Status(fiber.StatusOK).JSON(fiber.Map{"status": "ok"})
}

func parseTaskID(c fiber.Ctx) (uuid.UUID, error) {
	raw := c.Params("id")
	id, err := uuid.Parse(raw)
	if err != nil {
		return uuid.Nil, fmt.Errorf("invalid task id %q", raw)
	}
	return id, nil
}
