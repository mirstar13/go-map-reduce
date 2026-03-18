package handler

import (
	"strings"

	"github.com/gofiber/fiber/v3"
	"go.uber.org/zap"

	"github.com/mirstar13/go-map-reduce/pkg/middleware/auth"
	"github.com/mirstar13/go-map-reduce/services/ui/client"
)

// JobHandler handles job-related routes.
// All requests are forwarded to the Manager service; the UI service does not
// access the database directly.
type JobHandler struct {
	manager *client.ManagerClient
	log     *zap.Logger
}

// NewJobHandler creates a new JobHandler.
func NewJobHandler(manager *client.ManagerClient, log *zap.Logger) *JobHandler {
	return &JobHandler{manager: manager, log: log}
}

// ListJobs godoc
//
//	GET /jobs
//	Requires: user or admin role.
//
// Returns the list of jobs owned by the calling user.
// Admins receive all jobs.
func (h *JobHandler) ListJobs(c fiber.Ctx) error {
	id := auth.GetIdentity(c)
	raw, status, err := h.manager.ListJobs(c.Context(), id.Subject, id.Email, rolesHeader(id))
	if err != nil {
		h.log.Error("list jobs: manager error", zap.Error(err))
		return c.Status(fiber.StatusBadGateway).JSON(fiber.Map{"error": "upstream error"})
	}
	return c.Status(status).Send(raw)
}

// GetJob godoc
//
//	GET /jobs/:id
//	Requires: user or admin role.
//
// Returns a single job. The Manager enforces that regular users can only see
// their own jobs.
func (h *JobHandler) GetJob(c fiber.Ctx) error {
	jobID := c.Params("id")
	if jobID == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "job id is required"})
	}

	id := auth.GetIdentity(c)
	raw, status, err := h.manager.GetJob(c.Context(), jobID, id.Subject, id.Email, rolesHeader(id))
	if err != nil {
		h.log.Error("get job: manager error", zap.String("job_id", jobID), zap.Error(err))
		return c.Status(fiber.StatusBadGateway).JSON(fiber.Map{"error": "upstream error"})
	}
	return c.Status(status).Send(raw)
}

// SubmitJob godoc
//
//	POST /jobs
//	Requires: user or admin role.
//
// Expected JSON body forwarded verbatim to the Manager:
//
//	{
//	  "mapper_path":   "code/<uuid>-mapper.py",   // object key in MinIO code bucket
//	  "reducer_path":  "code/<uuid>-reducer.py",  // object key in MinIO code bucket
//	  "input_path":    "input/<uuid>-data.jsonl", // object key in MinIO input bucket
//	  "num_mappers":   4,
//	  "num_reducers":  2,
//	  "input_format":  "jsonl"                    // "jsonl" | "text"
//	}
//
// The UI service hashes the caller's user ID to route to a specific Manager
// StatefulSet replica, ensuring consistent ownership tracking.
func (h *JobHandler) SubmitJob(c fiber.Ctx) error {
	id := auth.GetIdentity(c)

	body := c.Body()
	if len(body) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "request body is required"})
	}

	raw, status, err := h.manager.SubmitJob(c.Context(), id.Subject, id.Email, rolesHeader(id), body)
	if err != nil {
		h.log.Error("submit job: manager error", zap.String("user", id.Subject), zap.Error(err))
		return c.Status(fiber.StatusBadGateway).JSON(fiber.Map{"error": "upstream error"})
	}

	h.log.Info("job submitted", zap.String("user", id.Subject), zap.Int("manager_status", status))
	return c.Status(status).Send(raw)
}

// CancelJob godoc
//
//	DELETE /jobs/:id
//	Requires: user or admin role.
//
// Asks the Manager to cancel a job. The Manager enforces ownership: regular
// users can only cancel their own jobs.
func (h *JobHandler) CancelJob(c fiber.Ctx) error {
	jobID := c.Params("id")
	if jobID == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "job id is required"})
	}

	id := auth.GetIdentity(c)
	raw, status, err := h.manager.CancelJob(c.Context(), jobID, id.Subject, id.Email, rolesHeader(id))
	if err != nil {
		h.log.Error("cancel job: manager error", zap.String("job_id", jobID), zap.Error(err))
		return c.Status(fiber.StatusBadGateway).JSON(fiber.Map{"error": "upstream error"})
	}

	h.log.Info("job cancel requested", zap.String("job_id", jobID), zap.String("user", id.Subject))
	return c.Status(status).Send(raw)
}

// GetJobOutput godoc
//
//	GET /jobs/:id/output
//	Requires: user or admin role.
//
// Retrieves the output file paths / presigned download URLs for a completed job.
func (h *JobHandler) GetJobOutput(c fiber.Ctx) error {
	jobID := c.Params("id")
	if jobID == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "job id is required"})
	}

	id := auth.GetIdentity(c)
	raw, status, err := h.manager.GetJobOutput(c.Context(), jobID, id.Subject, id.Email, rolesHeader(id))
	if err != nil {
		h.log.Error("get job output: manager error", zap.String("job_id", jobID), zap.Error(err))
		return c.Status(fiber.StatusBadGateway).JSON(fiber.Map{"error": "upstream error"})
	}
	return c.Status(status).Send(raw)
}

// rolesHeader serialises the identity's roles into the comma-separated header
// format that internal services expect.
func rolesHeader(id *auth.Identity) string {
	return strings.Join(id.Roles, ",")
}
