package workflow

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/mirstar13/go-map-reduce/db"
)

var placeholderRegex = regexp.MustCompile(`\$\{([a-zA-Z0-9_-]+)\.output_path\}`)

// StageSpec defines a single MapReduce job within a workflow.
type StageSpec struct {
	MapperPath  string     `json:"mapper_path"`
	ReducerPath string     `json:"reducer_path"`
	InputPath   string     `json:"input_path,omitempty"` // Only for root stages
	NumMappers  int32      `json:"num_mappers"`
	NumReducers int32      `json:"num_reducers"`
	InputFormat string     `json:"input_format"`
	DependsOn   []string   `json:"depends_on"`
	Condition   *Condition `json:"condition,omitempty"`
}

type Condition struct {
	Metric   string `json:"metric"`   // e.g. "output_records"
	Operator string `json:"operator"` // e.g. ">", "<", "==", ">=", "<="
	Value    int64  `json:"value"`
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

	if job.Status != "COMPLETED" && job.Status != "SKIPPED" {
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
	// Check if all parents are COMPLETED or SKIPPED
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

	// Fetch all jobs for workflow to avoid N+1 queries
	workflowJobs, err := c.queries.GetWorkflowStages(ctx, uuid.NullUUID{UUID: workflowID, Valid: true})
	if err != nil {
		return fmt.Errorf("get workflow stages: %w", err)
	}

	jobsByStageName := make(map[string]db.Job)
	for _, j := range workflowJobs {
		if j.StageName.Valid {
			jobsByStageName[j.StageName.String] = j
		}
	}

	existingJob, ok := jobsByStageName[stageName]
	if !ok {
		return fmt.Errorf("get existing stage job: not found")
	}

	if existingJob.Status != "PENDING" {
		return nil // Already running or finished
	}

	deps, err := c.queries.GetWorkflowDependencies(ctx, workflowID)
	if err != nil {
		return fmt.Errorf("get stage dependencies: %w", err)
	}

	var parentJobs []db.Job
	for _, dep := range deps {
		if dep.StageName == stageName {
			// Re-fetch parent job to ensure OutputRecords are current
			pJob, err := c.queries.GetJobByWorkflowStage(ctx, db.GetJobByWorkflowStageParams{
				WorkflowID: uuid.NullUUID{UUID: workflowID, Valid: true},
				StageName:  sql.NullString{String: dep.DependsOn, Valid: true},
			})
			if err == nil {
				// Retry once if completed but records are 0 (potential race)
				if pJob.Status == "COMPLETED" && pJob.OutputRecords == 0 {
					time.Sleep(500 * time.Millisecond)
					pJob, err = c.queries.GetJobByWorkflowStage(ctx, db.GetJobByWorkflowStageParams{
						WorkflowID: uuid.NullUUID{UUID: workflowID, Valid: true},
						StageName:  sql.NullString{String: dep.DependsOn, Valid: true},
					})
				}
				if err == nil {
					parentJobs = append(parentJobs, pJob)
				}
			}
		}
	}

	return c.launchOrSkip(ctx, existingJob, parentJobs, jobsByStageName)
}

func (c *Controller) launchOrSkip(ctx context.Context, job db.Job, parents []db.Job, jobsByStageName map[string]db.Job) error {
	var cond *Condition
	if job.Condition.Valid {
		if err := json.Unmarshal(job.Condition.RawMessage, &cond); err != nil {
			return fmt.Errorf("unmarshal condition: %w", err)
		}
	}

	passed := true
	if cond != nil {
		// Aggregate metrics from parents
		var totalOutputRecords int64
		for _, p := range parents {
			if p.Status == "COMPLETED" {
				totalOutputRecords += p.OutputRecords
			}
		}

		// Evaluate against aggregate
		dummyJob := db.Job{OutputRecords: totalOutputRecords}
		passed = c.evaluateCondition(dummyJob, cond)

		c.log.Info("Condition evaluated", zap.Bool("passed", passed))
	}

	if !passed {
		c.log.Info("workflow stage condition failed; SKIPPING stage",
			zap.String("job_id", job.JobID.String()),
			zap.String("stage", job.StageName.String),
		)
		return c.queries.UpdateJobStatus(ctx, db.UpdateJobStatusParams{
			JobID:  job.JobID,
			Status: "SKIPPED",
		})
	}

	// Launch
	c.log.Info("workflow stage condition passed (or none); SUBMITTING stage",
		zap.String("job_id", job.JobID.String()),
		zap.String("stage", job.StageName.String),
	)

	// 1. Resolve placeholders if any
	if job.WorkflowID.Valid && job.InputPath != "" {
		resolved, err := c.resolvePlaceholders(job.InputPath, jobsByStageName)
		if err != nil {
			c.log.Error("failed to resolve placeholders; failing job",
				zap.String("job_id", job.JobID.String()),
				zap.Error(err),
			)
			failErr := c.queries.FailJob(ctx, db.FailJobParams{
				JobID:        job.JobID,
				ErrorMessage: sql.NullString{String: fmt.Sprintf("resolve placeholders: %v", err), Valid: true},
			})
			if failErr != nil {
				c.log.Error("failed to mark job as failed after placeholder error", zap.Error(failErr))
			}
			return fmt.Errorf("resolve placeholders: %w", err)
		}
		job.InputPath = resolved
	}

	// 2. Default to first parent if still empty
	if job.InputPath == "" && len(parents) > 0 {
		job.InputPath = parents[0].OutputPath
		job.InputBucket = parents[0].OutputBucket
	}

	// 3. Update DB with final input and status
	err := c.queries.UpdateJobInputAndStatus(ctx, db.UpdateJobInputAndStatusParams{
		JobID:       job.JobID,
		InputPath:   job.InputPath,
		InputBucket: job.InputBucket,
		Status:      "SUBMITTED",
	})
	if err != nil {
		return fmt.Errorf("update job input and status: %w", err)
	}

	// The job in DB is now SUBMITTED. We can launch the supervisor.
	// We need the full job object.
	fullJob, err := c.queries.GetJob(ctx, job.JobID)
	if err != nil {
		return err
	}

	c.launcher(fullJob)
	return nil
}

func (c *Controller) resolvePlaceholders(inputPath string, jobsByStageName map[string]db.Job) (string, error) {
	matches := placeholderRegex.FindAllStringSubmatch(inputPath, -1)
	if len(matches) == 0 {
		return inputPath, nil
	}

	resolvedPath := inputPath
	for _, match := range matches {
		fullMatch := match[0]
		referencedStage := match[1]

		job, ok := jobsByStageName[referencedStage]
		if !ok {
			return "", fmt.Errorf("resolve placeholder: get referenced stage %q: not found", referencedStage)
		}

		if job.Status != "COMPLETED" && job.Status != "SKIPPED" {
			return "", fmt.Errorf("resolve placeholder: referenced stage %q is not completed (status: %s)", referencedStage, job.Status)
		}

		resolvedPath = strings.ReplaceAll(resolvedPath, fullMatch, job.OutputPath)
	}

	return resolvedPath, nil
}

func (c *Controller) evaluateCondition(job db.Job, cond *Condition) bool {
	if cond == nil {
		return true
	}

	var val int64
	switch cond.Metric {
	case "output_records":
		val = job.OutputRecords
	default:
		return true
	}

	switch cond.Operator {
	case ">":
		return val > cond.Value
	case "<":
		return val < cond.Value
	case "==":
		return val == cond.Value
	case ">=":
		return val >= cond.Value
	case "<=":
		return val <= cond.Value
	}
	return true
}

func (c *Controller) maybeMarkWorkflowCompleted(ctx context.Context, workflowID uuid.UUID) error {
	stages, err := c.queries.GetWorkflowStages(ctx, uuid.NullUUID{UUID: workflowID, Valid: true})
	if err != nil {
		return err
	}

	for _, s := range stages {
		if s.Status != "COMPLETED" && s.Status != "SKIPPED" {
			return nil
		}
	}

	_, err = c.queries.UpdateWorkflowStatus(ctx, db.UpdateWorkflowStatusParams{
		WorkflowID: workflowID,
		Status:     "COMPLETED",
	})
	return err
}
