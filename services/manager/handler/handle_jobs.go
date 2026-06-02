package handler

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"

	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/mirstar13/go-map-reduce/db"
	"github.com/mirstar13/go-map-reduce/pkg/middleware/auth"
	"github.com/mirstar13/go-map-reduce/services/manager/config"
	interfaces "github.com/mirstar13/go-map-reduce/services/manager/interface"
	"github.com/mirstar13/go-map-reduce/services/manager/supervisor"
)

// JobHandler handles all job-related HTTP routes.
type JobHandler struct {
	queries    db.Querier
	registry   *supervisor.Registry
	splitter   interfaces.Splitter
	dispatcher interfaces.Dispatcher
	cfg        *config.Config
	log        *zap.Logger
	// launchSupervisor is called when a new job is created; injected from main.
	launchSupervisor func(job db.Job)
}

// NewJobHandler creates a JobHandler.
func NewJobHandler(
	queries db.Querier,
	registry *supervisor.Registry,
	spl interfaces.Splitter,
	disp interfaces.Dispatcher,
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

// taskProgress holds completion counts for one phase.
type taskProgress struct {
	Completed int64 `json:"completed"`
	Failed    int64 `json:"failed"`
	Total     int64 `json:"total"`
}

// progressResponse is the JSON shape for GET /jobs/:id/progress.
type progressResponse struct {
	JobID          string        `json:"job_id"`
	Status         string        `json:"status"`
	NumMappers     int32         `json:"num_mappers"`
	NumReducers    int32         `json:"num_reducers"`
	MapProgress    *taskProgress `json:"map_progress,omitempty"`
	ReduceProgress *taskProgress `json:"reduce_progress,omitempty"`
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
	if req.NumMappers < 1 || req.NumReducers < 1 {
		// Auto-calculate based on input size
		var size int64
		var err error
		if h.splitter != nil {
			size, err = h.splitter.GetSize(c.Context(), h.cfg.MinioBucketInput, req.InputPath)
		} else {
			err = fmt.Errorf("splitter not available")
		}

		if err == nil {
			sizeMB := float64(size) / (1024 * 1024)
			const thresholdMB = 10.0 // 1 mapper per 10MB

			if req.NumMappers < 1 {
				req.NumMappers = int32(math.Ceil(sizeMB / thresholdMB))
				if req.NumMappers < 1 {
					req.NumMappers = 1
				}
			}

			if req.NumReducers < 1 {
				// Based on user feedback (~40 mappers, ~80 reducers), use 2x ratio
				req.NumReducers = req.NumMappers * 2
				if req.NumReducers < 1 {
					req.NumReducers = 1
				}
			}

			h.log.Info("auto-scaling tasks",
				zap.Int64("size_bytes", size),
				zap.Int32("mappers", req.NumMappers),
				zap.Int32("reducers", req.NumReducers),
			)
		} else {
			h.log.Warn("could not get input size for auto-scaling; defaulting to 1", zap.Error(err))
			if req.NumMappers < 1 {
				req.NumMappers = 1
			}
			if req.NumReducers < 1 {
				req.NumReducers = 1
			}
		}
	}
	if req.InputFormat == "" {
		req.InputFormat = "jsonl"
	}

	outputPath := fmt.Sprintf("jobs/%s", uuid.New().String())

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
		InputBucket:  h.cfg.MinioBucketInput,
		OutputBucket: h.cfg.MinioBucketOutput,
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

// DeleteJob handles DELETE /jobs/:id.
func (h *JobHandler) DeleteJob(c fiber.Ctx) error {
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

	// 1. Hard cleanup of K8s resources
	h.cleanupK8s(c.Context(), job)

	// 2. Delete from DB (cascades to tasks)
	if err := h.queries.DeleteJob(c.Context(), jobID); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "could not delete job"})
	}

	h.log.Info("job deleted", zap.String("job_id", jobID.String()))
	return c.Status(fiber.StatusOK).JSON(fiber.Map{"status": "ok"})
}

func (h *JobHandler) cleanupK8s(ctx context.Context, job db.Job) {
	shortID := job.JobID.String()
	if len(shortID) > 8 {
		shortID = shortID[:8]
	}
	_ = h.dispatcher.DeleteJob(ctx, fmt.Sprintf("build-map-%s", shortID))
	_ = h.dispatcher.DeleteJob(ctx, fmt.Sprintf("build-red-%s", shortID))

	mapJobNames, _ := h.queries.GetMapTaskJobNames(ctx, job.JobID)
	for _, name := range mapJobNames {
		if name.Valid {
			_ = h.dispatcher.DeleteJob(ctx, name.String)
		}
	}

	redJobNames, _ := h.queries.GetReduceTaskJobNames(ctx, job.JobID)
	for _, name := range redJobNames {
		if name.Valid {
			_ = h.dispatcher.DeleteJob(ctx, name.String)
		}
	}
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

// GetJobProgress handles GET /jobs/:id/progress.
// Returns task-level completion counts for progress tracking.
func (h *JobHandler) GetJobProgress(c fiber.Ctx) error {
	jobID, err := parseJobID(c)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	job, err := h.queries.GetJob(c.Context(), jobID)
	if err != nil {
		if err == sql.ErrNoRows {
			h.log.Warn("get job progress: job not found", zap.String("job_id", jobID.String()))
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "job not found"})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "could not get job"})
	}

	if err := assertAccess(c, job); err != nil {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{"error": err.Error()})
	}

	resp := progressResponse{
		JobID:       jobID.String(),
		Status:      job.Status,
		NumMappers:  job.NumMappers,
		NumReducers: job.NumReducers,
	}

	// Only query task counts when tasks exist.
	switch job.Status {
	case "MAP_PHASE", "REDUCE_PHASE", "COMPLETED", "FAILED":
		mapCounts, err := h.queries.CountMapTasksByStatus(c.Context(), jobID)
		if err == nil {
			resp.MapProgress = &taskProgress{
				Completed: mapCounts.Completed,
				Failed:    mapCounts.Failed,
				Total:     mapCounts.Total,
			}
		}
	}

	switch job.Status {
	case "REDUCE_PHASE", "COMPLETED", "FAILED":
		redCounts, err := h.queries.CountReduceTasksByStatus(c.Context(), jobID)
		if err == nil {
			resp.ReduceProgress = &taskProgress{
				Completed: redCounts.Completed,
				Failed:    redCounts.Failed,
				Total:     redCounts.Total,
			}
		}
	}

	return c.JSON(resp)
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
