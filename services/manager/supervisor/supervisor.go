package supervisor

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"go.uber.org/zap"

	"github.com/mirstar13/go-map-reduce/db"
	"github.com/mirstar13/go-map-reduce/services/manager/config"
	"github.com/mirstar13/go-map-reduce/services/manager/dispatcher"
	interfaces "github.com/mirstar13/go-map-reduce/services/manager/interface"
)

// Registry keeps track of all running job supervisors on this replica.
// Handlers use it to signal supervisors when workers call back.
type Registry struct {
	mu          sync.RWMutex
	supervisors map[uuid.UUID]*Supervisor
}

// NewRegistry creates an empty Registry.
func NewRegistry() *Registry {
	return &Registry{supervisors: make(map[uuid.UUID]*Supervisor)}
}

// Register adds a supervisor to the registry.
func (r *Registry) Register(s *Supervisor) {
	r.mu.Lock()
	r.supervisors[s.jobID] = s
	r.mu.Unlock()
}

// Deregister removes a supervisor from the registry.
func (r *Registry) Deregister(jobID uuid.UUID) {
	r.mu.Lock()
	delete(r.supervisors, jobID)
	r.mu.Unlock()
}

// Notify sends a non-blocking poke to the supervisor for the given job,
// causing it to re-evaluate its state immediately.
func (r *Registry) Notify(jobID uuid.UUID) {
	r.mu.RLock()
	s, ok := r.supervisors[jobID]
	r.mu.RUnlock()
	if ok {
		select {
		case s.notify <- struct{}{}:
		default: // Already notified; drop.
		}
	}
}

// Supervisor drives a single MapReduce job through its lifecycle.
// It runs as a dedicated goroutine for the lifetime of the job.
type Supervisor struct {
	jobID      uuid.UUID
	job        db.Job
	queries    db.Querier
	splitter   interfaces.Splitter
	dispatcher interfaces.Dispatcher
	minio      *minio.Client
	cfg        *config.Config
	log        *zap.Logger
	registry   *Registry

	// notify is poked by task-callback handlers for an immediate state re-eval.
	notify chan struct{}
}

// New creates a Supervisor for the given job. It does not start the goroutine.
func New(
	job db.Job,
	queries db.Querier,
	spl interfaces.Splitter,
	disp interfaces.Dispatcher,
	minio *minio.Client,
	cfg *config.Config,
	log *zap.Logger,
	registry *Registry,
) *Supervisor {
	return &Supervisor{
		jobID:      job.JobID,
		job:        job,
		queries:    queries,
		splitter:   spl,
		dispatcher: disp,
		minio:      minio,
		cfg:        cfg,
		log:        log.With(zap.String("job_id", job.JobID.String())),
		registry:   registry,
		notify:     make(chan struct{}, 1),
	}
}

// Run is the main goroutine loop. It drives the job through its state machine
// until the job reaches a terminal state (COMPLETED, FAILED, CANCELLED).
func (s *Supervisor) Run(ctx context.Context) {
	s.registry.Register(s)
	defer s.registry.Deregister(s.jobID)

	s.log.Info("supervisor started", zap.String("status", s.job.Status))

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	// Drive immediately on first run.
	if err := s.step(ctx); err != nil {
		s.log.Error("supervisor step failed", zap.Error(err))
	}
	if s.isTerminal() {
		s.Cleanup(ctx)
		return
	}

	for {
		select {
		case <-ctx.Done():
			s.log.Info("supervisor context cancelled")
			return

		case <-s.notify:
			if err := s.step(ctx); err != nil {
				s.log.Error("supervisor step (notify) failed", zap.Error(err))
			}

		case <-ticker.C:
			if err := s.step(ctx); err != nil {
				s.log.Error("supervisor step (tick) failed", zap.Error(err))
			}
		}

		if s.isTerminal() {
			s.Cleanup(ctx)
			return
		}
	}
}

func (s *Supervisor) isTerminal() bool {
	st := s.job.Status
	return st == "COMPLETED" || st == "FAILED" || st == "CANCELLED"
}

// Cleanup removes all Kubernetes Jobs associated with this MapReduce job.
func (s *Supervisor) Cleanup(ctx context.Context) {
	s.log.Info("cleaning up Kubernetes resources")

	// 1. Delete builder jobs (known patterns)
	shortID := s.jobID.String()
	if len(shortID) > 8 {
		shortID = shortID[:8]
	}
	_ = s.dispatcher.DeleteJob(ctx, fmt.Sprintf("build-map-%s", shortID))
	_ = s.dispatcher.DeleteJob(ctx, fmt.Sprintf("build-red-%s", shortID))

	// 2. Delete map task jobs
	mapJobNames, err := s.queries.GetMapTaskJobNames(ctx, s.jobID)
	if err == nil {
		for _, name := range mapJobNames {
			if name.Valid {
				_ = s.dispatcher.DeleteJob(ctx, name.String)
			}
		}
	}

	// 3. Delete reduce task jobs
	redJobNames, err := s.queries.GetReduceTaskJobNames(ctx, s.jobID)
	if err == nil {
		for _, name := range redJobNames {
			if name.Valid {
				_ = s.dispatcher.DeleteJob(ctx, name.String)
			}
		}
	}

	s.log.Info("cleanup complete")
}

// step reads the current job status and advances the state machine by one step.
func (s *Supervisor) step(ctx context.Context) error {
	job, err := s.queries.GetJob(ctx, s.jobID)
	if err != nil {
		if err == sql.ErrNoRows {
			s.log.Warn("job not found in database; stopping supervisor")
			// We return nil so the error isn't logged as a failure, 
			// but we set a flag or just rely on the fact that s.job wasn't updated.
			// Better: set s.job.Status to a terminal state to trigger exit.
			s.job.Status = "CANCELLED" 
			return nil
		}
		return fmt.Errorf("step: get job: %w", err)
	}
	s.job = job

	switch job.Status {
	case "SUBMITTED":
		return s.doBuild(ctx)
	case "BUILDING":
		return s.checkBuildPhase(ctx)
	case "SPLITTING":
		// split already in progress; nothing to do — wait for tasks to appear
	case "MAP_PHASE":
		return s.checkMapPhase(ctx)
	case "REDUCE_PHASE":
		return s.checkReducePhase(ctx)
	case "COMPLETED", "FAILED", "CANCELLED":
		s.log.Info("job reached terminal state", zap.String("status", job.Status))
		return nil // Caller's outer loop will eventually stop — we rely on context cancellation
	}
	return nil
}

// doSplit transitions SUBMITTED → SPLITTING → MAP_PHASE by:
//  1. Computing byte-range splits from MinIO.
//  2. Creating map_task rows in the DB.
//  3. Dispatching K8s Jobs for all map tasks.
func (s *Supervisor) doSplit(ctx context.Context) error {
	s.log.Info("splitting input", zap.String("input_path", s.job.InputPath))

	if err := s.queries.UpdateJobStatus(ctx, db.UpdateJobStatusParams{
		JobID: s.jobID, Status: "SPLITTING",
	}); err != nil {
		return fmt.Errorf("split: mark SPLITTING: %w", err)
	}

	splits, err := s.splitter.Compute(ctx, s.job.InputPath, int(s.job.NumMappers))
	if err != nil {
		return s.failJob(ctx, fmt.Sprintf("split: compute splits: %v", err))
	}

	s.log.Info("computed splits", zap.Int("count", len(splits)))

	// Create map tasks in the DB.
	for _, sp := range splits {
		_, err := s.queries.CreateMapTask(ctx, db.CreateMapTaskParams{
			JobID:       s.jobID,
			TaskIndex:   int32(sp.Index),
			InputFile:   sp.File,
			InputOffset: sp.Offset,
			InputLength: sp.Length,
		})
		if err != nil {
			return s.failJob(ctx, fmt.Sprintf("split: create map task %d: %v", sp.Index, err))
		}
	}

	if err := s.queries.UpdateJobStatus(ctx, db.UpdateJobStatusParams{
		JobID: s.jobID, Status: "MAP_PHASE",
	}); err != nil {
		return fmt.Errorf("split: mark MAP_PHASE: %w", err)
	}

	// Dispatch all map workers.
	return s.dispatchPendingMapTasks(ctx)
}

// checkMapPhase dispatches any pending map tasks and checks if all are done.
func (s *Supervisor) checkMapPhase(ctx context.Context) error {
	// Dispatch any not-yet-started tasks (handles restarts / retry resets).
	if err := s.dispatchPendingMapTasks(ctx); err != nil {
		return err
	}

	counts, err := s.queries.CountMapTasksByStatus(ctx, s.jobID)
	if err != nil {
		return fmt.Errorf("map phase: count tasks: %w", err)
	}

	s.log.Debug("map phase progress",
		zap.Int64("completed", counts.Completed),
		zap.Int64("failed", counts.Failed),
		zap.Int64("total", counts.Total),
	)

	if counts.Failed > 0 && counts.Completed+counts.Failed == counts.Total {
		return s.failJob(ctx, fmt.Sprintf("%d map task(s) failed permanently", counts.Failed))
	}

	if counts.Completed == counts.Total {
		s.log.Info("all map tasks completed; transitioning to reduce phase")
		return s.startReducePhase(ctx)
	}
	return nil
}

// dispatchPendingMapTasks finds PENDING map tasks and creates K8s Jobs for them.
func (s *Supervisor) dispatchPendingMapTasks(ctx context.Context) error {
	tasks, err := s.queries.GetPendingMapTasks(ctx, db.GetPendingMapTasksParams{
		JobID: s.jobID,
		Limit: int32(s.job.NumMappers),
	})
	if err != nil {
		return fmt.Errorf("dispatch map: get pending tasks: %w", err)
	}

	for _, task := range tasks {
		if task.RetryCount >= int32(s.cfg.TaskMaxRetries) {
			s.log.Warn("map task exceeded max retries",
				zap.String("task_id", task.TaskID.String()),
				zap.Int32("retry_count", task.RetryCount),
			)
			if err := s.queries.MarkMapTaskFailed(ctx, task.TaskID); err != nil {
				s.log.Error("mark map task failed", zap.Error(err))
			}
			continue
		}

		jobName, err := s.dispatcher.DispatchMap(ctx, dispatcher.MapTaskSpec{
			TaskID:      task.TaskID.String(),
			JobID:       s.jobID.String(),
			TaskIndex:   int(task.TaskIndex),
			InputFile:   task.InputFile,
			InputOffset: task.InputOffset,
			InputLength: task.InputLength,
			MapperPath:  s.job.MapperPath,
			NumReducers: int(s.job.NumReducers),
		})
		if err != nil {
			s.log.Error("dispatch map task", zap.String("task_id", task.TaskID.String()), zap.Error(err))
			continue
		}

		if err := s.queries.MarkMapTaskRunning(ctx, db.MarkMapTaskRunningParams{
			TaskID:     task.TaskID,
			K8sJobName: sql.NullString{String: jobName, Valid: true},
		}); err != nil {
			s.log.Error("mark map task running", zap.Error(err))
		}
		s.log.Info("dispatched map task", zap.String("k8s_job", jobName), zap.Int32("index", task.TaskIndex))
	}
	return nil
}

// startReducePhase builds reduce tasks from all map outputs and dispatches them.
func (s *Supervisor) startReducePhase(ctx context.Context) error {
	if err := s.queries.UpdateJobStatus(ctx, db.UpdateJobStatusParams{
		JobID: s.jobID, Status: "REDUCE_PHASE",
	}); err != nil {
		return fmt.Errorf("reduce phase: mark status: %w", err)
	}

	// Create one reduce task row per reducer index.
	// dispatchPendingReduceTasks will query the map output locations itself
	// when building the INPUT_LOCATIONS env var for each K8s Job.
	for i := 0; i < int(s.job.NumReducers); i++ {
		_, err := s.queries.CreateReduceTask(ctx, db.CreateReduceTaskParams{
			JobID:     s.jobID,
			TaskIndex: int32(i),
		})
		if err != nil {
			return s.failJob(ctx, fmt.Sprintf("reduce phase: create task %d: %v", i, err))
		}
	}

	return s.dispatchPendingReduceTasks(ctx)
}

// checkReducePhase dispatches pending reduce tasks and checks for completion.
func (s *Supervisor) checkReducePhase(ctx context.Context) error {
	if err := s.dispatchPendingReduceTasks(ctx); err != nil {
		return err
	}

	counts, err := s.queries.CountReduceTasksByStatus(ctx, s.jobID)
	if err != nil {
		return fmt.Errorf("reduce phase: count tasks: %w", err)
	}

	s.log.Debug("reduce phase progress",
		zap.Int64("completed", counts.Completed),
		zap.Int64("failed", counts.Failed),
		zap.Int64("total", counts.Total),
	)

	if counts.Failed > 0 && counts.Completed+counts.Failed == counts.Total {
		return s.failJob(ctx, fmt.Sprintf("%d reduce task(s) failed permanently", counts.Failed))
	}

	if counts.Completed == counts.Total {
		s.log.Info("all reduce tasks completed; job done")
		return s.queries.UpdateJobStatus(ctx, db.UpdateJobStatusParams{
			JobID: s.jobID, Status: "COMPLETED",
		})
	}
	return nil
}

// dispatchPendingReduceTasks finds PENDING reduce tasks and creates K8s Jobs.
// Input locations are always re-queried from the DB so retries work correctly.
func (s *Supervisor) dispatchPendingReduceTasks(ctx context.Context) error {
	tasks, err := s.queries.GetPendingReduceTasks(ctx, db.GetPendingReduceTasksParams{
		JobID: s.jobID,
		Limit: int32(s.job.NumReducers),
	})
	if err != nil {
		return fmt.Errorf("dispatch reduce: get pending tasks: %w", err)
	}

	for _, task := range tasks {
		if task.RetryCount >= int32(s.cfg.TaskMaxRetries) {
			s.log.Warn("reduce task exceeded max retries", zap.String("task_id", task.TaskID.String()))
			if err := s.queries.MarkReduceTaskFailed(ctx, task.TaskID); err != nil {
				s.log.Error("mark reduce task failed", zap.Error(err))
			}
			continue
		}

		// Re-query map output locations for this reducer index so retries work.
		mapOutputs, _ := s.queries.GetMapTaskOutputLocations(ctx, s.jobID)
		type loc struct {
			ReducerIndex int    `json:"reducer_index"`
			Path         string `json:"path"`
		}
		var inputLocs []loc
		for _, mo := range mapOutputs {
			if !mo.OutputLocations.Valid {
				continue
			}
			var files []loc
			if err := json.Unmarshal(mo.OutputLocations.RawMessage, &files); err != nil {
				continue
			}
			for _, f := range files {
				if f.ReducerIndex == int(task.TaskIndex) {
					inputLocs = append(inputLocs, f)
				}
			}
		}
		locsJSON, _ := json.Marshal(inputLocs)

		jobName, err := s.dispatcher.DispatchReduce(ctx, dispatcher.ReduceTaskSpec{
			TaskID:         task.TaskID.String(),
			JobID:          s.jobID.String(),
			TaskIndex:      int(task.TaskIndex),
			ReducerPath:    s.job.ReducerPath,
			InputLocations: json.RawMessage(locsJSON),
		})
		if err != nil {
			s.log.Error("dispatch reduce task", zap.String("task_id", task.TaskID.String()), zap.Error(err))
			continue
		}

		if err := s.queries.MarkReduceTaskRunning(ctx, db.MarkReduceTaskRunningParams{
			TaskID:     task.TaskID,
			K8sJobName: sql.NullString{String: jobName, Valid: true},
		}); err != nil {
			s.log.Error("mark reduce task running", zap.Error(err))
		}
		s.log.Info("dispatched reduce task", zap.String("k8s_job", jobName), zap.Int32("index", task.TaskIndex))
	}
	return nil
}

// failJob marks the job as FAILED with the given reason.
func (s *Supervisor) failJob(ctx context.Context, reason string) error {
	s.log.Error("job failed", zap.String("reason", reason))
	return s.queries.FailJob(ctx, db.FailJobParams{
		JobID:        s.jobID,
		ErrorMessage: sql.NullString{String: reason, Valid: true},
	})
}

// doBuild starts the compilation of mapper and reducer plugins if needed.
func (s *Supervisor) doBuild(ctx context.Context) error {
	// Only build if the paths end in .go
	needsMapBuild := strings.HasSuffix(s.job.MapperPath, ".go")
	needsRedBuild := strings.HasSuffix(s.job.ReducerPath, ".go")

	if !needsMapBuild && !needsRedBuild {
		s.log.Info("no build needed; skipping to splitting")
		return s.doSplit(ctx)
	}

	if err := s.queries.UpdateJobStatus(ctx, db.UpdateJobStatusParams{
		JobID: s.jobID, Status: "BUILDING",
	}); err != nil {
		return fmt.Errorf("build: mark BUILDING: %w", err)
	}

	if needsMapBuild {
		cached, err := s.checkCache(ctx, "mapper", s.job.MapperPath)
		if err != nil {
			return err
		}
		if cached {
			needsMapBuild = false
		}
	}

	if needsRedBuild {
		cached, err := s.checkCache(ctx, "reducer", s.job.ReducerPath)
		if err != nil {
			return err
		}
		if cached {
			needsRedBuild = false
		}
	}

	// If both were cached, we might be able to transition immediately
	if !needsMapBuild && !needsRedBuild {
		s.log.Info("all plugins retrieved from cache; transitioning to splitting")
		return s.doSplit(ctx)
	}

	if needsMapBuild {
		outputPath := fmt.Sprintf("builds/%s/mapper", s.jobID)
		_, err := s.dispatcher.DispatchBuild(ctx, dispatcher.BuildTaskSpec{
			JobID:      s.jobID.String(),
			PluginType: "mapper",
			SourcePath: s.job.MapperPath,
			OutputPath: outputPath,
		})
		if err != nil {
			return s.failJob(ctx, fmt.Sprintf("build: dispatch mapper build: %v", err))
		}
	}

	if needsRedBuild {
		outputPath := fmt.Sprintf("builds/%s/reducer", s.jobID)
		_, err := s.dispatcher.DispatchBuild(ctx, dispatcher.BuildTaskSpec{
			JobID:      s.jobID.String(),
			PluginType: "reducer",
			SourcePath: s.job.ReducerPath,
			OutputPath: outputPath,
		})
		if err != nil {
			return s.failJob(ctx, fmt.Sprintf("build: dispatch reducer build: %v", err))
		}
	}

	return nil
}

func (s *Supervisor) checkCache(ctx context.Context, pType, sourcePath string) (bool, error) {
	// Get ETag from MinIO
	info, err := s.minio.StatObject(ctx, s.cfg.MinioBucketCode, sourcePath, minio.StatObjectOptions{})
	if err != nil {
		return false, fmt.Errorf("cache: stat source %s: %w", sourcePath, err)
	}

	cached, err := s.queries.GetCachedPlugin(ctx, info.ETag)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, fmt.Errorf("cache: check DB: %w", err)
	}

	// Verify the binary still exists in MinIO
	_, err = s.minio.StatObject(ctx, s.cfg.MinioBucketCode, cached.BinaryPath, minio.StatObjectOptions{})
	if err != nil {
		s.log.Warn("cache hit but binary missing from storage; re-building", zap.String("path", cached.BinaryPath))
		_ = s.queries.DeleteCachedPlugin(ctx, info.ETag)
		return false, nil
	}

	// Found valid cache!
	s.log.Info("plugin cache hit", zap.String("type", pType), zap.String("hash", info.ETag))
	
	if pType == "mapper" {
		err = s.queries.UpdateJobMapperPath(ctx, db.UpdateJobMapperPathParams{JobID: s.jobID, MapperPath: cached.BinaryPath})
	} else {
		err = s.queries.UpdateJobReducerPath(ctx, db.UpdateJobReducerPathParams{JobID: s.jobID, ReducerPath: cached.BinaryPath})
	}
	if err != nil {
		return false, fmt.Errorf("cache: update job path: %w", err)
	}

	_ = s.queries.UpdatePluginLastUsed(ctx, info.ETag)
	return true, nil
}


// checkBuildPhase checks if the compilation is finished.
func (s *Supervisor) checkBuildPhase(ctx context.Context) error {
	// In this implementation, the builder calls back to the Manager.
	// The Manager updates the job's mapper_path/reducer_path to the compiled binary.
	// We just need to check if both paths now point to "builds/..." or were already binaries.

	mapDone := !strings.HasSuffix(s.job.MapperPath, ".go")
	redDone := !strings.HasSuffix(s.job.ReducerPath, ".go")

	if mapDone && redDone {
		s.log.Info("build completed; transitioning to splitting")
		return s.doSplit(ctx)
	}

	return nil
}

