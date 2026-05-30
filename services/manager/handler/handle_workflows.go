package handler

import (
	"database/sql"
	"fmt"

	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/mirstar13/go-map-reduce/db"
	"github.com/mirstar13/go-map-reduce/pkg/middleware/auth"
	"github.com/mirstar13/go-map-reduce/services/manager/config"
	"github.com/mirstar13/go-map-reduce/services/manager/workflow"
)

// WorkflowHandler handles all workflow-related HTTP routes.
type WorkflowHandler struct {
	queries          db.Querier
	cfg              *config.Config
	log              *zap.Logger
	launchSupervisor func(db.Job)
}

// NewWorkflowHandler creates a WorkflowHandler.
func NewWorkflowHandler(
	queries db.Querier,
	cfg *config.Config,
	log *zap.Logger,
	launch func(db.Job),
) *WorkflowHandler {
	return &WorkflowHandler{
		queries:          queries,
		cfg:              cfg,
		log:              log,
		launchSupervisor: launch,
	}
}

// SubmitWorkflow handles POST /workflows.
func (h *WorkflowHandler) SubmitWorkflow(c fiber.Ctx) error {
	id := auth.GetIdentity(c)
	if id == nil {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "unauthenticated"})
	}

	var spec workflow.WorkflowSpec
	if err := c.Bind().JSON(&spec); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid request body"})
	}

	if spec.Name == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "workflow name is required"})
	}

	if len(spec.Stages) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "at least one stage is required"})
	}

	// 1. Validate DAG
	stages := make([]string, 0, len(spec.Stages))
	var deps []workflow.Dependency
	for name, stage := range spec.Stages {
		stages = append(stages, name)
		for _, dep := range stage.DependsOn {
			deps = append(deps, workflow.Dependency{
				Parent: dep,
				Child:  name,
			})
		}
	}

	if err := workflow.ValidateDAG(stages, deps); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": fmt.Sprintf("invalid workflow DAG: %v", err)})
	}

	// 2. Create workflow record
	wf, err := h.queries.CreateWorkflow(c.Context(), db.CreateWorkflowParams{
		WorkflowID:  uuid.New(),
		Name:        spec.Name,
		Status:      "RUNNING",
		OwnerUserID: id.Subject,
	})
	if err != nil {
		h.log.Error("failed to create workflow", zap.Error(err))
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "could not create workflow"})
	}

	rootJobs := make([]db.Job, 0)

	// 3. Create jobs and dependencies
	for name, stageSpec := range spec.Stages {
		isRoot := len(stageSpec.DependsOn) == 0
		status := "PENDING"
		if isRoot {
			status = "SUBMITTED"
		}

		// Calculate output path for this stage
		outputPath := fmt.Sprintf("output/workflows/%s/%s", wf.WorkflowID, name)

		job, err := h.queries.CreateJob(c.Context(), db.CreateJobParams{
			OwnerUserID:  id.Subject,
			OwnerReplica: h.cfg.MyReplicaName,
			MapperPath:   stageSpec.MapperPath,
			ReducerPath:  stageSpec.ReducerPath,
			InputPath:    stageSpec.InputPath, // Might be empty for non-root stages
			OutputPath:   outputPath,
			NumMappers:   stageSpec.NumMappers,
			NumReducers:  stageSpec.NumReducers,
			InputFormat:  stageSpec.InputFormat,
			WorkflowID:   uuid.NullUUID{UUID: wf.WorkflowID, Valid: true},
			StageName:    sql.NullString{String: name, Valid: true},
		})
		if err != nil {
			h.log.Error("failed to create job for workflow stage", zap.String("stage", name), zap.Error(err))
			// Cleanup? Or just fail? Usually we should use a transaction.
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": fmt.Sprintf("failed to create job for stage %s", name)})
		}

		// Update status to PENDING if not root (CreateJob defaults to SUBMITTED)
		if !isRoot {
			err = h.queries.UpdateJobStatus(c.Context(), db.UpdateJobStatusParams{
				JobID:  job.JobID,
				Status: status,
			})
			if err != nil {
				h.log.Error("failed to update job status for workflow stage", zap.String("stage", name), zap.Error(err))
				return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to initialize workflow jobs"})
			}
			job.Status = status
		}

		// Store root jobs to launch them later
		if isRoot {
			rootJobs = append(rootJobs, job)
		}

		// Add dependencies
		for _, dep := range stageSpec.DependsOn {
			err = h.queries.AddWorkflowDependency(c.Context(), db.AddWorkflowDependencyParams{
				WorkflowID: wf.WorkflowID,
				StageName:  name,
				DependsOn:  dep,
			})
			if err != nil {
				h.log.Error("failed to add workflow dependency", zap.String("stage", name), zap.String("depends_on", dep), zap.Error(err))
				return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to initialize workflow dependencies"})
			}
		}
	}

	// 4. Launch root stages
	for _, job := range rootJobs {
		h.log.Info("launching workflow root stage",
			zap.String("workflow_id", wf.WorkflowID.String()),
			zap.String("stage", job.StageName.String),
			zap.String("job_id", job.JobID.String()),
		)
		h.launchSupervisor(job)
	}

	return c.Status(fiber.StatusCreated).JSON(wf)
}

// GetWorkflow handles GET /workflows/:id.
func (h *WorkflowHandler) GetWorkflow(c fiber.Ctx) error {
	id := auth.GetIdentity(c)
	if id == nil {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "unauthenticated"})
	}

	rawID := c.Params("id")
	wfID, err := uuid.Parse(rawID)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid workflow id"})
	}

	wf, err := h.queries.GetWorkflow(c.Context(), wfID)
	if err != nil {
		if err == sql.ErrNoRows {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "workflow not found"})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "could not get workflow"})
	}

	// RBAC check
	if !id.HasRole("admin") && id.Subject != wf.OwnerUserID {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{"error": "access denied"})
	}

	// Fetch stages
	jobs, err := h.queries.GetWorkflowStages(c.Context(), uuid.NullUUID{UUID: wfID, Valid: true})
	if err != nil {
		h.log.Error("failed to fetch workflow stages", zap.Error(err))
	}

	return c.JSON(fiber.Map{
		"workflow": wf,
		"stages":   jobs,
	})
}
