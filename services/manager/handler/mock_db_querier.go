package handler

import (
	"context"
	"database/sql"

	"github.com/google/uuid"
	"github.com/mirstar13/go-map-reduce/db"
)

// mockQuerier is a test double for db.Querier.
// Each method dispatches to a configurable function field;
// if the field is nil the method panics so tests fail loudly rather than silently.
type mockQuerier struct {
	cancelJobFn                    func(ctx context.Context, jobID uuid.UUID) error
	countJobsByStatusFn            func(ctx context.Context) ([]db.CountJobsByStatusRow, error)
	countMapTasksByStatusFn        func(ctx context.Context, jobID uuid.UUID) (db.CountMapTasksByStatusRow, error)
	countReduceTasksByStatusFn     func(ctx context.Context, jobID uuid.UUID) (db.CountReduceTasksByStatusRow, error)
	createJobFn                    func(ctx context.Context, arg db.CreateJobParams) (db.Job, error)
	createMapTaskFn                func(ctx context.Context, arg db.CreateMapTaskParams) (db.MapTask, error)
	createReduceTaskFn             func(ctx context.Context, arg db.CreateReduceTaskParams) (db.ReduceTask, error)
	failJobFn                      func(ctx context.Context, arg db.FailJobParams) error
	getActiveJobsByReplicaFn       func(ctx context.Context, ownerReplica string) ([]db.Job, error)
	getAllJobsFn                   func(ctx context.Context) ([]db.Job, error)
	getJobFn                       func(ctx context.Context, jobID uuid.UUID) (db.Job, error)
	getJobsByUserFn                func(ctx context.Context, ownerUserID string) ([]db.Job, error)
	getMapTaskFn                   func(ctx context.Context, taskID uuid.UUID) (db.MapTask, error)
	getMapTaskOutputLocationsFn    func(ctx context.Context, jobID uuid.UUID) ([]db.GetMapTaskOutputLocationsRow, error)
	getMapTasksByJobFn             func(ctx context.Context, jobID uuid.UUID) ([]db.MapTask, error)
	getMapTasksByJobAndStatusFn    func(ctx context.Context, arg db.GetMapTasksByJobAndStatusParams) ([]db.MapTask, error)
	getPendingMapTasksFn           func(ctx context.Context, arg db.GetPendingMapTasksParams) ([]db.MapTask, error)
	getPendingReduceTasksFn        func(ctx context.Context, arg db.GetPendingReduceTasksParams) ([]db.ReduceTask, error)
	getReduceTaskFn                func(ctx context.Context, taskID uuid.UUID) (db.ReduceTask, error)
	getReduceTaskOutputPathsFn     func(ctx context.Context, jobID uuid.UUID) ([]db.GetReduceTaskOutputPathsRow, error)
	getReduceTasksByJobFn          func(ctx context.Context, jobID uuid.UUID) ([]db.ReduceTask, error)
	getReduceTasksByJobAndStatusFn func(ctx context.Context, arg db.GetReduceTasksByJobAndStatusParams) ([]db.ReduceTask, error)
	getStaleRunningMapTasksFn      func(ctx context.Context, dollar1 sql.NullString) ([]db.MapTask, error)
	getStaleRunningReduceTasksFn   func(ctx context.Context, dollar1 sql.NullString) ([]db.ReduceTask, error)
	incrementMapTaskRetryFn        func(ctx context.Context, taskID uuid.UUID) error
	incrementReduceTaskRetryFn     func(ctx context.Context, taskID uuid.UUID) error
	markMapTaskCompletedFn         func(ctx context.Context, arg db.MarkMapTaskCompletedParams) error
	markMapTaskFailedFn            func(ctx context.Context, taskID uuid.UUID) error
	markMapTaskRunningFn           func(ctx context.Context, arg db.MarkMapTaskRunningParams) error
	markReduceTaskCompletedFn      func(ctx context.Context, arg db.MarkReduceTaskCompletedParams) error
	markReduceTaskFailedFn         func(ctx context.Context, taskID uuid.UUID) error
	markReduceTaskRunningFn        func(ctx context.Context, arg db.MarkReduceTaskRunningParams) error
	updateJobStatusFn              func(ctx context.Context, arg db.UpdateJobStatusParams) error
	updateJobMapperPathFn          func(ctx context.Context, arg db.UpdateJobMapperPathParams) error
	updateJobReducerPathFn         func(ctx context.Context, arg db.UpdateJobReducerPathParams) error
	getMapTaskJobNamesFn           func(ctx context.Context, jobID uuid.UUID) ([]sql.NullString, error)
	getReduceTaskJobNamesFn        func(ctx context.Context, jobID uuid.UUID) ([]sql.NullString, error)
	deleteJobFn                    func(ctx context.Context, jobID uuid.UUID) error
	getCachedPluginFn              func(ctx context.Context, sourceHash string) (db.PluginCache, error)
	upsertCachedPluginFn           func(ctx context.Context, arg db.UpsertCachedPluginParams) error
	updatePluginLastUsedFn         func(ctx context.Context, sourceHash string) error
	listStalePluginsFn             func(ctx context.Context, dollar1 sql.NullString) ([]db.PluginCache, error)
	deleteCachedPluginFn           func(ctx context.Context, sourceHash string) error
	addWorkflowDependencyFn        func(ctx context.Context, arg db.AddWorkflowDependencyParams) error
	checkStageDependenciesFn       func(ctx context.Context, arg db.CheckStageDependenciesParams) (int64, error)
	createWorkflowFn               func(ctx context.Context, arg db.CreateWorkflowParams) (db.Workflow, error)
	getDownstreamStagesFn          func(ctx context.Context, arg db.GetDownstreamStagesParams) ([]string, error)
	getWorkflowFn                  func(ctx context.Context, workflowID uuid.UUID) (db.Workflow, error)
	getWorkflowStagesFn            func(ctx context.Context, workflowID uuid.NullUUID) ([]db.Job, error)
	updateWorkflowStatusFn         func(ctx context.Context, arg db.UpdateWorkflowStatusParams) (db.Workflow, error)
	getCompletedWorkflowStagesCountFn func(ctx context.Context, workflowID uuid.NullUUID) (int64, error)
	getJobByWorkflowStageFn        func(ctx context.Context, arg db.GetJobByWorkflowStageParams) (db.Job, error)
	getParentsOutputPathsFn        func(ctx context.Context, arg db.GetParentsOutputPathsParams) ([]string, error)
	getWorkflowStatusFn            func(ctx context.Context, workflowID uuid.UUID) (string, error)
	updateJobInputAndStatusFn      func(ctx context.Context, arg db.UpdateJobInputAndStatusParams) error
	getWorkflowDependenciesFn      func(ctx context.Context, workflowID uuid.UUID) ([]db.GetWorkflowDependenciesRow, error)
}

// compile-time assertion
var _ db.Querier = (*mockQuerier)(nil)

func (m *mockQuerier) GetWorkflowDependencies(ctx context.Context, workflowID uuid.UUID) ([]db.GetWorkflowDependenciesRow, error) {
	if m.getWorkflowDependenciesFn != nil {
		return m.getWorkflowDependenciesFn(ctx, workflowID)
	}
	panic("GetWorkflowDependencies: not implemented")
}

func (m *mockQuerier) GetCompletedWorkflowStagesCount(ctx context.Context, workflowID uuid.NullUUID) (int64, error) {
	if m.getCompletedWorkflowStagesCountFn != nil {
		return m.getCompletedWorkflowStagesCountFn(ctx, workflowID)
	}
	panic("GetCompletedWorkflowStagesCount: not implemented")
}

func (m *mockQuerier) GetJobByWorkflowStage(ctx context.Context, arg db.GetJobByWorkflowStageParams) (db.Job, error) {
	if m.getJobByWorkflowStageFn != nil {
		return m.getJobByWorkflowStageFn(ctx, arg)
	}
	panic("GetJobByWorkflowStage: not implemented")
}

func (m *mockQuerier) GetParentsOutputPaths(ctx context.Context, arg db.GetParentsOutputPathsParams) ([]string, error) {
	if m.getParentsOutputPathsFn != nil {
		return m.getParentsOutputPathsFn(ctx, arg)
	}
	panic("GetParentsOutputPaths: not implemented")
}

func (m *mockQuerier) GetWorkflowStatus(ctx context.Context, workflowID uuid.UUID) (string, error) {
	if m.getWorkflowStatusFn != nil {
		return m.getWorkflowStatusFn(ctx, workflowID)
	}
	panic("GetWorkflowStatus: not implemented")
}

func (m *mockQuerier) UpdateJobInputAndStatus(ctx context.Context, arg db.UpdateJobInputAndStatusParams) error {
	if m.updateJobInputAndStatusFn != nil {
		return m.updateJobInputAndStatusFn(ctx, arg)
	}
	panic("UpdateJobInputAndStatus: not implemented")
}

func (m *mockQuerier) AddWorkflowDependency(ctx context.Context, arg db.AddWorkflowDependencyParams) error {
	if m.addWorkflowDependencyFn != nil {
		return m.addWorkflowDependencyFn(ctx, arg)
	}
	panic("AddWorkflowDependency: not implemented")
}

func (m *mockQuerier) CheckStageDependencies(ctx context.Context, arg db.CheckStageDependenciesParams) (int64, error) {
	if m.checkStageDependenciesFn != nil {
		return m.checkStageDependenciesFn(ctx, arg)
	}
	panic("CheckStageDependencies: not implemented")
}

func (m *mockQuerier) CreateWorkflow(ctx context.Context, arg db.CreateWorkflowParams) (db.Workflow, error) {
	if m.createWorkflowFn != nil {
		return m.createWorkflowFn(ctx, arg)
	}
	panic("CreateWorkflow: not implemented")
}

func (m *mockQuerier) GetDownstreamStages(ctx context.Context, arg db.GetDownstreamStagesParams) ([]string, error) {
	if m.getDownstreamStagesFn != nil {
		return m.getDownstreamStagesFn(ctx, arg)
	}
	panic("GetDownstreamStages: not implemented")
}

func (m *mockQuerier) GetWorkflow(ctx context.Context, workflowID uuid.UUID) (db.Workflow, error) {
	if m.getWorkflowFn != nil {
		return m.getWorkflowFn(ctx, workflowID)
	}
	panic("GetWorkflow: not implemented")
}

func (m *mockQuerier) GetWorkflowStages(ctx context.Context, workflowID uuid.NullUUID) ([]db.Job, error) {
	if m.getWorkflowStagesFn != nil {
		return m.getWorkflowStagesFn(ctx, workflowID)
	}
	panic("GetWorkflowStages: not implemented")
}

func (m *mockQuerier) UpdateWorkflowStatus(ctx context.Context, arg db.UpdateWorkflowStatusParams) (db.Workflow, error) {
	if m.updateWorkflowStatusFn != nil {
		return m.updateWorkflowStatusFn(ctx, arg)
	}
	panic("UpdateWorkflowStatus: not implemented")
}

func (m *mockQuerier) CancelJob(ctx context.Context, jobID uuid.UUID) error {
	if m.cancelJobFn != nil {
		return m.cancelJobFn(ctx, jobID)
	}
	panic("CancelJob: not implemented")
}

func (m *mockQuerier) CountJobsByStatus(ctx context.Context) ([]db.CountJobsByStatusRow, error) {
	if m.countJobsByStatusFn != nil {
		return m.countJobsByStatusFn(ctx)
	}
	panic("CountJobsByStatus: not implemented")
}

func (m *mockQuerier) CountMapTasksByStatus(ctx context.Context, jobID uuid.UUID) (db.CountMapTasksByStatusRow, error) {
	if m.countMapTasksByStatusFn != nil {
		return m.countMapTasksByStatusFn(ctx, jobID)
	}
	panic("CountMapTasksByStatus: not implemented")
}

func (m *mockQuerier) CountReduceTasksByStatus(ctx context.Context, jobID uuid.UUID) (db.CountReduceTasksByStatusRow, error) {
	if m.countReduceTasksByStatusFn != nil {
		return m.countReduceTasksByStatusFn(ctx, jobID)
	}
	panic("CountReduceTasksByStatus: not implemented")
}

func (m *mockQuerier) CreateJob(ctx context.Context, arg db.CreateJobParams) (db.Job, error) {
	if m.createJobFn != nil {
		return m.createJobFn(ctx, arg)
	}
	panic("CreateJob: not implemented")
}

func (m *mockQuerier) CreateMapTask(ctx context.Context, arg db.CreateMapTaskParams) (db.MapTask, error) {
	if m.createMapTaskFn != nil {
		return m.createMapTaskFn(ctx, arg)
	}
	panic("CreateMapTask: not implemented")
}

func (m *mockQuerier) CreateReduceTask(ctx context.Context, arg db.CreateReduceTaskParams) (db.ReduceTask, error) {
	if m.createReduceTaskFn != nil {
		return m.createReduceTaskFn(ctx, arg)
	}
	panic("CreateReduceTask: not implemented")
}

func (m *mockQuerier) FailJob(ctx context.Context, arg db.FailJobParams) error {
	if m.failJobFn != nil {
		return m.failJobFn(ctx, arg)
	}
	panic("FailJob: not implemented")
}

func (m *mockQuerier) GetActiveJobsByReplica(ctx context.Context, ownerReplica string) ([]db.Job, error) {
	if m.getActiveJobsByReplicaFn != nil {
		return m.getActiveJobsByReplicaFn(ctx, ownerReplica)
	}
	panic("GetActiveJobsByReplica: not implemented")
}

func (m *mockQuerier) GetAllJobs(ctx context.Context) ([]db.Job, error) {
	if m.getAllJobsFn != nil {
		return m.getAllJobsFn(ctx)
	}
	panic("GetAllJobs: not implemented")
}

func (m *mockQuerier) GetJob(ctx context.Context, jobID uuid.UUID) (db.Job, error) {
	if m.getJobFn != nil {
		return m.getJobFn(ctx, jobID)
	}
	panic("GetJob: not implemented")
}

func (m *mockQuerier) GetJobsByUser(ctx context.Context, ownerUserID string) ([]db.Job, error) {
	if m.getJobsByUserFn != nil {
		return m.getJobsByUserFn(ctx, ownerUserID)
	}
	panic("GetJobsByUser: not implemented")
}

func (m *mockQuerier) GetMapTask(ctx context.Context, taskID uuid.UUID) (db.MapTask, error) {
	if m.getMapTaskFn != nil {
		return m.getMapTaskFn(ctx, taskID)
	}
	panic("GetMapTask: not implemented")
}

func (m *mockQuerier) GetMapTaskOutputLocations(ctx context.Context, jobID uuid.UUID) ([]db.GetMapTaskOutputLocationsRow, error) {
	if m.getMapTaskOutputLocationsFn != nil {
		return m.getMapTaskOutputLocationsFn(ctx, jobID)
	}
	panic("GetMapTaskOutputLocations: not implemented")
}

func (m *mockQuerier) GetMapTasksByJob(ctx context.Context, jobID uuid.UUID) ([]db.MapTask, error) {
	if m.getMapTasksByJobFn != nil {
		return m.getMapTasksByJobFn(ctx, jobID)
	}
	panic("GetMapTasksByJob: not implemented")
}

func (m *mockQuerier) GetMapTasksByJobAndStatus(ctx context.Context, arg db.GetMapTasksByJobAndStatusParams) ([]db.MapTask, error) {
	if m.getMapTasksByJobAndStatusFn != nil {
		return m.getMapTasksByJobAndStatusFn(ctx, arg)
	}
	panic("GetMapTasksByJobAndStatus: not implemented")
}

func (m *mockQuerier) GetPendingMapTasks(ctx context.Context, arg db.GetPendingMapTasksParams) ([]db.MapTask, error) {
	if m.getPendingMapTasksFn != nil {
		return m.getPendingMapTasksFn(ctx, arg)
	}
	panic("GetPendingMapTasks: not implemented")
}

func (m *mockQuerier) GetPendingReduceTasks(ctx context.Context, arg db.GetPendingReduceTasksParams) ([]db.ReduceTask, error) {
	if m.getPendingReduceTasksFn != nil {
		return m.getPendingReduceTasksFn(ctx, arg)
	}
	panic("GetPendingReduceTasks: not implemented")
}

func (m *mockQuerier) GetReduceTask(ctx context.Context, taskID uuid.UUID) (db.ReduceTask, error) {
	if m.getReduceTaskFn != nil {
		return m.getReduceTaskFn(ctx, taskID)
	}
	panic("GetReduceTask: not implemented")
}

func (m *mockQuerier) GetReduceTaskOutputPaths(ctx context.Context, jobID uuid.UUID) ([]db.GetReduceTaskOutputPathsRow, error) {
	if m.getReduceTaskOutputPathsFn != nil {
		return m.getReduceTaskOutputPathsFn(ctx, jobID)
	}
	panic("GetReduceTaskOutputPaths: not implemented")
}

func (m *mockQuerier) GetReduceTasksByJob(ctx context.Context, jobID uuid.UUID) ([]db.ReduceTask, error) {
	if m.getReduceTasksByJobFn != nil {
		return m.getReduceTasksByJobFn(ctx, jobID)
	}
	panic("GetReduceTasksByJob: not implemented")
}

func (m *mockQuerier) GetReduceTasksByJobAndStatus(ctx context.Context, arg db.GetReduceTasksByJobAndStatusParams) ([]db.ReduceTask, error) {
	if m.getReduceTasksByJobAndStatusFn != nil {
		return m.getReduceTasksByJobAndStatusFn(ctx, arg)
	}
	panic("GetReduceTasksByJobAndStatus: not implemented")
}

func (m *mockQuerier) GetStaleRunningMapTasks(ctx context.Context, dollar1 sql.NullString) ([]db.MapTask, error) {
	if m.getStaleRunningMapTasksFn != nil {
		return m.getStaleRunningMapTasksFn(ctx, dollar1)
	}
	panic("GetStaleRunningMapTasks: not implemented")
}

func (m *mockQuerier) GetStaleRunningReduceTasks(ctx context.Context, dollar1 sql.NullString) ([]db.ReduceTask, error) {
	if m.getStaleRunningReduceTasksFn != nil {
		return m.getStaleRunningReduceTasksFn(ctx, dollar1)
	}
	panic("GetStaleRunningReduceTasks: not implemented")
}

func (m *mockQuerier) IncrementMapTaskRetry(ctx context.Context, taskID uuid.UUID) error {
	if m.incrementMapTaskRetryFn != nil {
		return m.incrementMapTaskRetryFn(ctx, taskID)
	}
	panic("IncrementMapTaskRetry: not implemented")
}

func (m *mockQuerier) IncrementReduceTaskRetry(ctx context.Context, taskID uuid.UUID) error {
	if m.incrementReduceTaskRetryFn != nil {
		return m.incrementReduceTaskRetryFn(ctx, taskID)
	}
	panic("IncrementReduceTaskRetry: not implemented")
}

func (m *mockQuerier) MarkMapTaskCompleted(ctx context.Context, arg db.MarkMapTaskCompletedParams) error {
	if m.markMapTaskCompletedFn != nil {
		return m.markMapTaskCompletedFn(ctx, arg)
	}
	panic("MarkMapTaskCompleted: not implemented")
}

func (m *mockQuerier) MarkMapTaskFailed(ctx context.Context, taskID uuid.UUID) error {
	if m.markMapTaskFailedFn != nil {
		return m.markMapTaskFailedFn(ctx, taskID)
	}
	panic("MarkMapTaskFailed: not implemented")
}

func (m *mockQuerier) MarkMapTaskRunning(ctx context.Context, arg db.MarkMapTaskRunningParams) error {
	if m.markMapTaskRunningFn != nil {
		return m.markMapTaskRunningFn(ctx, arg)
	}
	panic("MarkMapTaskRunning: not implemented")
}

func (m *mockQuerier) MarkReduceTaskCompleted(ctx context.Context, arg db.MarkReduceTaskCompletedParams) error {
	if m.markReduceTaskCompletedFn != nil {
		return m.markReduceTaskCompletedFn(ctx, arg)
	}
	panic("MarkReduceTaskCompleted: not implemented")
}

func (m *mockQuerier) MarkReduceTaskFailed(ctx context.Context, taskID uuid.UUID) error {
	if m.markReduceTaskFailedFn != nil {
		return m.markReduceTaskFailedFn(ctx, taskID)
	}
	panic("MarkReduceTaskFailed: not implemented")
}

func (m *mockQuerier) MarkReduceTaskRunning(ctx context.Context, arg db.MarkReduceTaskRunningParams) error {
	if m.markReduceTaskRunningFn != nil {
		return m.markReduceTaskRunningFn(ctx, arg)
	}
	panic("MarkReduceTaskRunning: not implemented")
}

func (m *mockQuerier) UpdateJobStatus(ctx context.Context, arg db.UpdateJobStatusParams) error {
	if m.updateJobStatusFn != nil {
		return m.updateJobStatusFn(ctx, arg)
	}
	panic("UpdateJobStatus: not implemented")
}

func (m *mockQuerier) UpdateJobMapperPath(ctx context.Context, arg db.UpdateJobMapperPathParams) error {
	if m.updateJobMapperPathFn != nil {
		return m.updateJobMapperPathFn(ctx, arg)
	}
	return nil
}

func (m *mockQuerier) UpdateJobReducerPath(ctx context.Context, arg db.UpdateJobReducerPathParams) error {
	if m.updateJobReducerPathFn != nil {
		return m.updateJobReducerPathFn(ctx, arg)
	}
	return nil
}

func (m *mockQuerier) GetMapTaskJobNames(ctx context.Context, jobID uuid.UUID) ([]sql.NullString, error) {
	if m.getMapTaskJobNamesFn != nil {
		return m.getMapTaskJobNamesFn(ctx, jobID)
	}
	return nil, nil
}

func (m *mockQuerier) GetReduceTaskJobNames(ctx context.Context, jobID uuid.UUID) ([]sql.NullString, error) {
	if m.getReduceTaskJobNamesFn != nil {
		return m.getReduceTaskJobNamesFn(ctx, jobID)
	}
	return nil, nil
}

func (m *mockQuerier) DeleteJob(ctx context.Context, jobID uuid.UUID) error {
	if m.deleteJobFn != nil {
		return m.deleteJobFn(ctx, jobID)
	}
	return nil
}

func (m *mockQuerier) GetCachedPlugin(ctx context.Context, sourceHash string) (db.PluginCache, error) {
	if m.getCachedPluginFn != nil {
		return m.getCachedPluginFn(ctx, sourceHash)
	}
	return db.PluginCache{}, sql.ErrNoRows
}

func (m *mockQuerier) UpsertCachedPlugin(ctx context.Context, arg db.UpsertCachedPluginParams) error {
	if m.upsertCachedPluginFn != nil {
		return m.upsertCachedPluginFn(ctx, arg)
	}
	return nil
}

func (m *mockQuerier) UpdatePluginLastUsed(ctx context.Context, sourceHash string) error {
	if m.updatePluginLastUsedFn != nil {
		return m.updatePluginLastUsedFn(ctx, sourceHash)
	}
	return nil
}

func (m *mockQuerier) ListStalePlugins(ctx context.Context, dollar1 sql.NullString) ([]db.PluginCache, error) {
	if m.listStalePluginsFn != nil {
		return m.listStalePluginsFn(ctx, dollar1)
	}
	return nil, nil
}

func (m *mockQuerier) DeleteCachedPlugin(ctx context.Context, sourceHash string) error {
	if m.deleteCachedPluginFn != nil {
		return m.deleteCachedPluginFn(ctx, sourceHash)
	}
	return nil
}



func (m *mockQuerier) IncrementJobOutputRecords(ctx context.Context, arg db.IncrementJobOutputRecordsParams) error { return nil }
