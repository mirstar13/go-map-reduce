package workflow

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/mirstar13/go-map-reduce/db"
)

// StageSpec defines a single MapReduce job within a workflow.
type StageSpec struct {
	MapperPath  string   `json:"mapper_path"`
	ReducerPath string   `json:"reducer_path"`
	InputPath   string   `json:"input_path,omitempty"` // Only for root stages
	NumMappers  int32    `json:"num_mappers"`
	NumReducers int32    `json:"num_reducers"`
	InputFormat string   `json:"input_format"`
	DependsOn   []string `json:"depends_on"`
}

// WorkflowSpec defines the entire multi-stage pipeline.
type WorkflowSpec struct {
	Name   string               `json:"name"`
	Stages map[string]StageSpec `json:"stages"`
}

// Controller manages the lifecycle of multi-stage pipelines.
type Controller struct {
	queries  db.Querier
	launcher func(db.Job)
	log      *zap.Logger
}

// NewController creates a new Workflow Controller.
func NewController(queries db.Querier, launcher func(db.Job), log *zap.Logger) *Controller {
	return &Controller{
		queries:  queries,
		launcher: launcher,
		log:      log,
	}
}

// HandleJobTerminal is called when a Job belonging to a workflow finishes (COMPLETED or FAILED).
func (c *Controller) HandleJobTerminal(ctx context.Context, jobID uuid.UUID) error {
	job, err := c.queries.GetJob(ctx, jobID)
	if err != nil {
		return fmt.Errorf("workflow controller: get job %s: %w", jobID, err)
	}

	if !job.WorkflowID.Valid || !job.StageName.Valid {
		return nil // Not part of a workflow
	}

	workflowID := job.WorkflowID.UUID
	stageName := job.StageName.String

	if job.Status == "FAILED" || job.Status == "CANCELLED" {
		c.log.Info("workflow stage failed; marking workflow as FAILED",
			zap.String("workflow_id", workflowID.String()),
			zap.String("stage", stageName),
			zap.String("status", job.Status),
		)
		_, err := c.queries.UpdateWorkflowStatus(ctx, db.UpdateWorkflowStatusParams{
			WorkflowID: workflowID,
			Status:     "FAILED",
		})
		return err
	}

	if job.Status != "COMPLETED" {
		return nil
	}

	// Find downstream stages
	downstream, err := c.queries.GetDownstreamStages(ctx, db.GetDownstreamStagesParams{
		WorkflowID: workflowID,
		DependsOn:  stageName,
	})
	if err != nil {
		return fmt.Errorf("workflow controller: get downstream: %w", err)
	}

	for _, childName := range downstream {
		if err := c.maybeLaunchStage(ctx, workflowID, childName); err != nil {
			c.log.Error("failed to launch workflow stage",
				zap.String("workflow_id", workflowID.String()),
				zap.String("stage", childName),
				zap.Error(err),
			)
		}
	}

	// Check if entire workflow is completed
	return c.maybeMarkWorkflowCompleted(ctx, workflowID)
}

func (c *Controller) maybeLaunchStage(ctx context.Context, workflowID uuid.UUID, stageName string) error {
	// Check if all parents are COMPLETED
	remaining, err := c.queries.CheckStageDependencies(ctx, db.CheckStageDependenciesParams{
		WorkflowID: workflowID,
		StageName:  stageName,
	})
	if err != nil {
		return fmt.Errorf("check dependencies: %w", err)
	}

	if remaining > 0 {
		c.log.Debug("workflow stage still has pending dependencies",
			zap.String("workflow_id", workflowID.String()),
			zap.String("stage", stageName),
			zap.Int64("pending_parents", remaining),
		)
		return nil
	}

	// All parents done! Aggregate their output paths.
	parentsOutputs, err := c.queries.GetParentsOutputPaths(ctx, db.GetParentsOutputPathsParams{
		WorkflowID: workflowID,
		StageName:  stageName,
	})
	if err != nil {
		return fmt.Errorf("get parent outputs: %w", err)
	}

	inputPath := strings.Join(parentsOutputs, ",")

	// We need the original job spec to create the new job.
	// We'll fetch the "dummy" job we created during submission which stores the config.
	// Wait, we need to know the mapper_path, num_mappers etc.
	// Let's assume the user submitted all stages as jobs with status 'PENDING' initially.
	
	existingJob, err := c.queries.GetJobByWorkflowStage(ctx, db.GetJobByWorkflowStageParams{
		WorkflowID: uuid.NullUUID{UUID: workflowID, Valid: true},
		StageName:  sql.NullString{String: stageName, Valid: true},
	})
	if err != nil {
		return fmt.Errorf("get existing stage job: %w", err)
	}

	if existingJob.Status != "PENDING" {
		return nil // Already running or finished
	}

	// Update the job with the aggregated input path and mark it SUBMITTED
	if err := c.queries.UpdateJobInputAndStatus(ctx, db.UpdateJobInputAndStatusParams{
		JobID:     existingJob.JobID,
		InputPath: inputPath,
		Status:    "SUBMITTED",
	}); err != nil {
		return fmt.Errorf("update stage job: %w", err)
	}

	// Fetch updated job to launch
	updatedJob, err := c.queries.GetJob(ctx, existingJob.JobID)
	if err != nil {
		return fmt.Errorf("get updated stage job: %w", err)
	}

	c.log.Info("launching workflow stage",
		zap.String("workflow_id", workflowID.String()),
		zap.String("stage", stageName),
		zap.String("job_id", updatedJob.JobID.String()),
	)

	// Launch its supervisor
	c.launcher(updatedJob)

	return nil
}

func (c *Controller) maybeMarkWorkflowCompleted(ctx context.Context, workflowID uuid.UUID) error {
	stages, err := c.queries.GetWorkflowStages(ctx, uuid.NullUUID{UUID: workflowID, Valid: true})
	if err != nil {
		return err
	}

	for _, s := range stages {
		if s.Status != "COMPLETED" {
			return nil
		}
	}

	_, err = c.queries.UpdateWorkflowStatus(ctx, db.UpdateWorkflowStatusParams{
		WorkflowID: workflowID,
		Status:     "COMPLETED",
	})
	return err
}
