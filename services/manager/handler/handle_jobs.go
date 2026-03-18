package handler

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/mirstar13/go-map-reduce/db"
	"github.com/mirstar13/go-map-reduce/pkg/middleware/auth"
	"github.com/mirstar13/go-map-reduce/services/manager/config"
	"github.com/mirstar13/go-map-reduce/services/manager/dispatcher"
	"github.com/mirstar13/go-map-reduce/services/manager/splitter"
	"github.com/mirstar13/go-map-reduce/services/manager/supervisor"
)

// JobHandler handles all job-related HTTP routes.
type JobHandler struct {
	queries    db.Querier
	registry   *supervisor.Registry
	splitter   *splitter.Splitter
	dispatcher *dispatcher.Dispatcher
	cfg        *config.Config
	log        *zap.Logger
	// launchSupervisor is called when a new job is created; injected from main.
	launchSupervisor func(job db.Job)
}

// NewJobHandler creates a JobHandler.
func NewJobHandler(
	queries db.Querier,
	registry *supervisor.Registry,
	spl *splitter.Splitter,
	disp *dispatcher.Dispatcher,
	cfg *config.Config,
	log *zap.Logger,
	launch func(db.Job),
) *JobHandler {
	return &JobHandler{
		queries:          queries,
		registry:         registry,
		splitter:         spl,
		dispatcher:       disp,
		cfg:              cfg,
		log:              log,
		launchSupervisor: launch,
	}
}

// submitJobRequest is the expected JSON body for POST /jobs.
type submitJobRequest struct {
	MapperPath  string `json:"mapper_path"`
	ReducerPath string `json:"reducer_path"`
	InputPath   string `json:"input_path"`
	NumMappers  int32  `json:"num_mappers"`
	NumReducers int32  `json:"num_reducers"`
	InputFormat string `json:"input_format"` // "jsonl" | "text"; default "jsonl"
}

// SubmitJob handles POST /jobs.
// Called by the UI service (already authenticated). Reads identity from X-User-* headers.
func (h *JobHandler) SubmitJob(c fiber.Ctx) error {
	id := auth.GetIdentity(c)
	if id == nil {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "unauthenticated"})
	}

	var req submitJobRequest
	if err := c.Bind().JSON(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid request body"})
	}

	if req.MapperPath == "" || req.ReducerPath == "" || req.InputPath == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "mapper_path, reducer_path and input_path are required",
		})
	}
	if req.NumMappers < 1 {
		req.NumMappers = 1
	}
	if req.NumReducers < 1 {
		req.NumReducers = 1
	}
	if req.InputFormat == "" {
		req.InputFormat = "jsonl"
	}

	outputPath := fmt.Sprintf("output/jobs/%s", uuid.New().String())

	job, err := h.queries.CreateJob(c.Context(), db.CreateJobParams{
		OwnerUserID:  id.Subject,
		OwnerReplica: h.cfg.MyReplicaName,
		MapperPath:   req.MapperPath,
		ReducerPath:  req.ReducerPath,
		InputPath:    req.InputPath,
		OutputPath:   outputPath,
		NumMappers:   req.NumMappers,
		NumReducers:  req.NumReducers,
		InputFormat:  req.InputFormat,
	})
	if err != nil {
		h.log.Error("create job", zap.Error(err))
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "could not create job"})
	}

	h.log.Info("job created",
		zap.String("job_id", job.JobID.String()),
		zap.String("user", id.Subject),
		zap.String("replica", h.cfg.MyReplicaName),
	)

	// Launch the job supervisor in a background goroutine.
	h.launchSupervisor(job)

	return c.Status(fiber.StatusCreated).JSON(job)
}

// ListJobs handles GET /jobs.
// Regular users see only their own jobs; admins see all.
func (h *JobHandler) ListJobs(c fiber.Ctx) error {
	id := auth.GetIdentity(c)
	if id == nil {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "unauthenticated"})
	}

	if id.HasRole("admin") {
		jobs, err := h.queries.GetAllJobs(c.Context())
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "could not list jobs"})
		}
		return c.JSON(jobs)
	}

	jobs, err := h.queries.GetJobsByUser(c.Context(), id.Subject)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "could not list jobs"})
	}
	return c.JSON(jobs)
}

// GetJob handles GET /jobs/:id.
func (h *JobHandler) GetJob(c fiber.Ctx) error {
	jobID, err := parseJobID(c)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	job, err := h.queries.GetJob(c.Context(), jobID)
	if err != nil {
		if err == sql.ErrNoRows {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "job not found"})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "could not get job"})
	}

	if err := assertAccess(c, job); err != nil {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(job)
}

// CancelJob handles POST /jobs/:id/cancel.
func (h *JobHandler) CancelJob(c fiber.Ctx) error {
	jobID, err := parseJobID(c)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	job, err := h.queries.GetJob(c.Context(), jobID)
	if err != nil {
		if err == sql.ErrNoRows {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "job not found"})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "could not get job"})
	}

	if err := assertAccess(c, job); err != nil {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{"error": err.Error()})
	}

	if err := h.queries.CancelJob(c.Context(), jobID); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "could not cancel job"})
	}

	h.log.Info("job cancelled", zap.String("job_id", jobID.String()))
	return c.Status(fiber.StatusOK).JSON(fiber.Map{"job_id": jobID, "status": "CANCELLED"})
}

// GetJobOutput handles GET /jobs/:id/output.
// Returns the MinIO object paths of the completed reduce task outputs.
func (h *JobHandler) GetJobOutput(c fiber.Ctx) error {
	jobID, err := parseJobID(c)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	job, err := h.queries.GetJob(c.Context(), jobID)
	if err != nil {
		if err == sql.ErrNoRows {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "job not found"})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "could not get job"})
	}

	if err := assertAccess(c, job); err != nil {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{"error": err.Error()})
	}

	if job.Status != "COMPLETED" {
		return c.Status(fiber.StatusConflict).JSON(fiber.Map{
			"error":  "job is not yet completed",
			"status": job.Status,
		})
	}

	paths, err := h.queries.GetReduceTaskOutputPaths(c.Context(), jobID)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "could not get output paths"})
	}

	return c.JSON(fiber.Map{
		"job_id":       jobID,
		"output_paths": paths,
	})
}

// AdminListJobs handles GET /admin/jobs — returns all jobs.
func (h *JobHandler) AdminListJobs(c fiber.Ctx) error {
	jobs, err := h.queries.GetAllJobs(c.Context())
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "could not list jobs"})
	}
	return c.JSON(jobs)
}

func parseJobID(c fiber.Ctx) (uuid.UUID, error) {
	raw := c.Params("id")
	id, err := uuid.Parse(raw)
	if err != nil {
		return uuid.Nil, fmt.Errorf("invalid job id %q", raw)
	}
	return id, nil
}

// assertAccess returns an error if the caller is not the job owner and not an admin.
func assertAccess(c fiber.Ctx, job db.Job) error {
	id := auth.GetIdentity(c)
	if id == nil {
		return fmt.Errorf("unauthenticated")
	}
	if id.HasRole("admin") || id.Subject == job.OwnerUserID {
		return nil
	}
	return fmt.Errorf("access denied")
}

// rolesContain checks if any of the given roles are in the list.
func rolesContain(roles []string, role string) bool {
	for _, r := range roles {
		if strings.EqualFold(strings.TrimSpace(r), role) {
			return true
		}
	}
	return false
}

// ensure errgroup import is used (used by splitter in main.go context)
var _ = errgroup.Group{}
