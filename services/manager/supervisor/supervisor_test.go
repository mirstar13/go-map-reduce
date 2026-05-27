package supervisor

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/mirstar13/go-map-reduce/db"
	"github.com/mirstar13/go-map-reduce/services/manager/config"
	"github.com/mirstar13/go-map-reduce/services/manager/dispatcher"
	interfaces "github.com/mirstar13/go-map-reduce/services/manager/interface"
	"github.com/mirstar13/go-map-reduce/services/manager/splitter"
	"github.com/sqlc-dev/pqtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// mockSplitter satisfies the Splitter interface.
type mockSplitter struct {
	computeFn func(ctx context.Context, objectKey string, numSplits int) ([]splitter.Split, error)
	getSizeFn func(ctx context.Context, objectKey string) (int64, error)
}

func (m *mockSplitter) Compute(ctx context.Context, key string, n int) ([]splitter.Split, error) {
	if m.computeFn != nil {
		return m.computeFn(ctx, key, n)
	}
	panic("mockSplitter.Compute: not implemented")
}

func (m *mockSplitter) GetSize(ctx context.Context, key string) (int64, error) {
	if m.getSizeFn != nil {
		return m.getSizeFn(ctx, key)
	}
	return 0, nil
}

// mockDispatcher satisfies the Dispatcher interface.
type mockDispatcher struct {
	dispatchMapFn    func(ctx context.Context, spec dispatcher.MapTaskSpec) (string, error)
	dispatchReduceFn func(ctx context.Context, spec dispatcher.ReduceTaskSpec) (string, error)
	dispatchBuildFn  func(ctx context.Context, spec dispatcher.BuildTaskSpec) (string, error)
	deleteJobFn      func(ctx context.Context, jobName string) error
}

func (m *mockDispatcher) DispatchMap(ctx context.Context, spec dispatcher.MapTaskSpec) (string, error) {
	if m.dispatchMapFn != nil {
		return m.dispatchMapFn(ctx, spec)
	}
	panic("mockDispatcher.DispatchMap: not implemented")
}

func (m *mockDispatcher) DispatchReduce(ctx context.Context, spec dispatcher.ReduceTaskSpec) (string, error) {
	if m.dispatchReduceFn != nil {
		return m.dispatchReduceFn(ctx, spec)
	}
	panic("mockDispatcher.DispatchReduce: not implemented")
}

func (m *mockDispatcher) DispatchBuild(ctx context.Context, spec dispatcher.BuildTaskSpec) (string, error) {
	if m.dispatchBuildFn != nil {
		return m.dispatchBuildFn(ctx, spec)
	}
	panic("mockDispatcher.DispatchBuild: not implemented")
}

func (m *mockDispatcher) DeleteJob(ctx context.Context, name string) error {
	if m.deleteJobFn != nil {
		return m.deleteJobFn(ctx, name)
	}
	return nil // default: no-op
}

// mockQuerier satisfies db.Querier — we only implement what the supervisor calls.
// Unimplemented methods panic loudly so failures are obvious.
type mockQuerier struct {
	cancelJobFn                   func(ctx context.Context, jobID uuid.UUID) error
	countJobsByStatusFn           func(ctx context.Context) ([]db.CountJobsByStatusRow, error)
	countMapTasksByStatusFn       func(ctx context.Context, jobID uuid.UUID) (db.CountMapTasksByStatusRow, error)
	countReduceTasksByStatusFn    func(ctx context.Context, jobID uuid.UUID) (db.CountReduceTasksByStatusRow, error)
	createJobFn                   func(ctx context.Context, arg db.CreateJobParams) (db.Job, error)
	createMapTaskFn               func(ctx context.Context, arg db.CreateMapTaskParams) (db.MapTask, error)
	createReduceTaskFn            func(ctx context.Context, arg db.CreateReduceTaskParams) (db.ReduceTask, error)
	deleteCachedPluginFn          func(ctx context.Context, sourceHash string) error
	deleteJobFn                   func(ctx context.Context, jobID uuid.UUID) error
	failJobFn                     func(ctx context.Context, arg db.FailJobParams) error
	getActiveJobsByReplicaFn      func(ctx context.Context, ownerReplica string) ([]db.Job, error)
	getAllJobsFn                  func(ctx context.Context) ([]db.Job, error)
	getCachedPluginFn             func(ctx context.Context, sourceHash string) (db.PluginCache, error)
	getJobFn                      func(ctx context.Context, jobID uuid.UUID) (db.Job, error)
	getJobsByUserFn               func(ctx context.Context, ownerUserID string) ([]db.Job, error)
	getMapTaskFn                  func(ctx context.Context, taskID uuid.UUID) (db.MapTask, error)
	getMapTaskJobNamesFn          func(ctx context.Context, jobID uuid.UUID) ([]sql.NullString, error)
	getMapTaskOutputLocationsFn   func(ctx context.Context, jobID uuid.UUID) ([]db.GetMapTaskOutputLocationsRow, error)
	getMapTasksByJobFn            func(ctx context.Context, jobID uuid.UUID) ([]db.MapTask, error)
	getMapTasksByJobAndStatusFn   func(ctx context.Context, arg db.GetMapTasksByJobAndStatusParams) ([]db.MapTask, error)
	getPendingMapTasksFn          func(ctx context.Context, arg db.GetPendingMapTasksParams) ([]db.MapTask, error)
	getPendingReduceTasksFn       func(ctx context.Context, arg db.GetPendingReduceTasksParams) ([]db.ReduceTask, error)
	getReduceTaskFn               func(ctx context.Context, taskID uuid.UUID) (db.ReduceTask, error)
	getReduceTaskJobNamesFn       func(ctx context.Context, jobID uuid.UUID) ([]sql.NullString, error)
	getReduceTaskOutputPathsFn    func(ctx context.Context, jobID uuid.UUID) ([]db.GetReduceTaskOutputPathsRow, error)
	getReduceTasksByJobFn         func(ctx context.Context, jobID uuid.UUID) ([]db.ReduceTask, error)
	getReduceTasksByJobAndStatusFn func(ctx context.Context, arg db.GetReduceTasksByJobAndStatusParams) ([]db.ReduceTask, error)
	getStaleRunningMapTasksFn     func(ctx context.Context, dollar_1 sql.NullString) ([]db.MapTask, error)
	getStaleRunningReduceTasksFn  func(ctx context.Context, dollar_1 sql.NullString) ([]db.ReduceTask, error)
	incrementMapTaskRetryFn       func(ctx context.Context, taskID uuid.UUID) error
	incrementReduceTaskRetryFn    func(ctx context.Context, taskID uuid.UUID) error
	listStalePluginsFn            func(ctx context.Context, dollar_1 sql.NullString) ([]db.PluginCache, error)
	markMapTaskCompletedFn        func(ctx context.Context, arg db.MarkMapTaskCompletedParams) error
	markMapTaskFailedFn           func(ctx context.Context, taskID uuid.UUID) error
	markMapTaskRunningFn          func(ctx context.Context, arg db.MarkMapTaskRunningParams) error
	markReduceTaskCompletedFn      func(ctx context.Context, arg db.MarkReduceTaskCompletedParams) error
	markReduceTaskFailedFn        func(ctx context.Context, taskID uuid.UUID) error
	markReduceTaskRunningFn       func(ctx context.Context, arg db.MarkReduceTaskRunningParams) error
	updateJobMapperPathFn         func(ctx context.Context, arg db.UpdateJobMapperPathParams) error
	updateJobReducerPathFn        func(ctx context.Context, arg db.UpdateJobReducerPathParams) error
	updateJobStatusFn             func(ctx context.Context, arg db.UpdateJobStatusParams) error
	updatePluginLastUsedFn        func(ctx context.Context, sourceHash string) error
	upsertCachedPluginFn          func(ctx context.Context, arg db.UpsertCachedPluginParams) error
}

var _ db.Querier = (*mockQuerier)(nil)

func (m *mockQuerier) CancelJob(ctx context.Context, jobID uuid.UUID) error {
	if m.cancelJobFn != nil {
		return m.cancelJobFn(ctx, jobID)
	}
	panic("mockQuerier.CancelJob: not implemented")
}
func (m *mockQuerier) CountJobsByStatus(ctx context.Context) ([]db.CountJobsByStatusRow, error) {
	if m.countJobsByStatusFn != nil {
		return m.countJobsByStatusFn(ctx)
	}
	panic("mockQuerier.CountJobsByStatus: not implemented")
}
func (m *mockQuerier) CountMapTasksByStatus(ctx context.Context, jobID uuid.UUID) (db.CountMapTasksByStatusRow, error) {
	if m.countMapTasksByStatusFn != nil {
		return m.countMapTasksByStatusFn(ctx, jobID)
	}
	return db.CountMapTasksByStatusRow{}, nil
}
func (m *mockQuerier) CountReduceTasksByStatus(ctx context.Context, jobID uuid.UUID) (db.CountReduceTasksByStatusRow, error) {
	if m.countReduceTasksByStatusFn != nil {
		return m.countReduceTasksByStatusFn(ctx, jobID)
	}
	return db.CountReduceTasksByStatusRow{}, nil
}
func (m *mockQuerier) CreateJob(ctx context.Context, arg db.CreateJobParams) (db.Job, error) {
	if m.createJobFn != nil {
		return m.createJobFn(ctx, arg)
	}
	panic("mockQuerier.CreateJob: not implemented")
}
func (m *mockQuerier) CreateMapTask(ctx context.Context, arg db.CreateMapTaskParams) (db.MapTask, error) {
	if m.createMapTaskFn != nil {
		return m.createMapTaskFn(ctx, arg)
	}
	return db.MapTask{}, nil
}
func (m *mockQuerier) CreateReduceTask(ctx context.Context, arg db.CreateReduceTaskParams) (db.ReduceTask, error) {
	if m.createReduceTaskFn != nil {
		return m.createReduceTaskFn(ctx, arg)
	}
	return db.ReduceTask{}, nil
}
func (m *mockQuerier) DeleteCachedPlugin(ctx context.Context, sourceHash string) error {
	if m.deleteCachedPluginFn != nil {
		return m.deleteCachedPluginFn(ctx, sourceHash)
	}
	return nil
}
func (m *mockQuerier) DeleteJob(ctx context.Context, jobID uuid.UUID) error {
	if m.deleteJobFn != nil {
		return m.deleteJobFn(ctx, jobID)
	}
	return nil
}
func (m *mockQuerier) FailJob(ctx context.Context, arg db.FailJobParams) error {
	if m.failJobFn != nil {
		return m.failJobFn(ctx, arg)
	}
	return nil
}
func (m *mockQuerier) GetActiveJobsByReplica(ctx context.Context, ownerReplica string) ([]db.Job, error) {
	if m.getActiveJobsByReplicaFn != nil {
		return m.getActiveJobsByReplicaFn(ctx, ownerReplica)
	}
	panic("mockQuerier.GetActiveJobsByReplica: not implemented")
}
func (m *mockQuerier) GetAllJobs(ctx context.Context) ([]db.Job, error) {
	if m.getAllJobsFn != nil {
		return m.getAllJobsFn(ctx)
	}
	panic("mockQuerier.GetAllJobs: not implemented")
}
func (m *mockQuerier) GetCachedPlugin(ctx context.Context, sourceHash string) (db.PluginCache, error) {
	if m.getCachedPluginFn != nil {
		return m.getCachedPluginFn(ctx, sourceHash)
	}
	return db.PluginCache{}, sql.ErrNoRows
}
func (m *mockQuerier) GetJob(ctx context.Context, jobID uuid.UUID) (db.Job, error) {
	if m.getJobFn != nil {
		return m.getJobFn(ctx, jobID)
	}
	panic("mockQuerier.GetJob: not implemented")
}
func (m *mockQuerier) GetJobsByUser(ctx context.Context, ownerUserID string) ([]db.Job, error) {
	if m.getJobsByUserFn != nil {
		return m.getJobsByUserFn(ctx, ownerUserID)
	}
	panic("mockQuerier.GetJobsByUser: not implemented")
}
func (m *mockQuerier) GetMapTask(ctx context.Context, taskID uuid.UUID) (db.MapTask, error) {
	if m.getMapTaskFn != nil {
		return m.getMapTaskFn(ctx, taskID)
	}
	panic("mockQuerier.GetMapTask: not implemented")
}
func (m *mockQuerier) GetMapTaskJobNames(ctx context.Context, jobID uuid.UUID) ([]sql.NullString, error) {
	if m.getMapTaskJobNamesFn != nil {
		return m.getMapTaskJobNamesFn(ctx, jobID)
	}
	return nil, nil
}
func (m *mockQuerier) GetMapTaskOutputLocations(ctx context.Context, jobID uuid.UUID) ([]db.GetMapTaskOutputLocationsRow, error) {
	if m.getMapTaskOutputLocationsFn != nil {
		return m.getMapTaskOutputLocationsFn(ctx, jobID)
	}
	return nil, nil
}
func (m *mockQuerier) GetMapTasksByJob(ctx context.Context, jobID uuid.UUID) ([]db.MapTask, error) {
	if m.getMapTasksByJobFn != nil {
		return m.getMapTasksByJobFn(ctx, jobID)
	}
	panic("mockQuerier.GetMapTasksByJob: not implemented")
}
func (m *mockQuerier) GetMapTasksByJobAndStatus(ctx context.Context, arg db.GetMapTasksByJobAndStatusParams) ([]db.MapTask, error) {
	if m.getMapTasksByJobAndStatusFn != nil {
		return m.getMapTasksByJobAndStatusFn(ctx, arg)
	}
	panic("mockQuerier.GetMapTasksByJobAndStatus: not implemented")
}
func (m *mockQuerier) GetPendingMapTasks(ctx context.Context, arg db.GetPendingMapTasksParams) ([]db.MapTask, error) {
	if m.getPendingMapTasksFn != nil {
		return m.getPendingMapTasksFn(ctx, arg)
	}
	return nil, nil
}
func (m *mockQuerier) GetPendingReduceTasks(ctx context.Context, arg db.GetPendingReduceTasksParams) ([]db.ReduceTask, error) {
	if m.getPendingReduceTasksFn != nil {
		return m.getPendingReduceTasksFn(ctx, arg)
	}
	return nil, nil
}
func (m *mockQuerier) GetReduceTask(ctx context.Context, taskID uuid.UUID) (db.ReduceTask, error) {
	if m.getReduceTaskFn != nil {
		return m.getReduceTaskFn(ctx, taskID)
	}
	panic("mockQuerier.GetReduceTask: not implemented")
}
func (m *mockQuerier) GetReduceTaskJobNames(ctx context.Context, jobID uuid.UUID) ([]sql.NullString, error) {
	if m.getReduceTaskJobNamesFn != nil {
		return m.getReduceTaskJobNamesFn(ctx, jobID)
	}
	return nil, nil
}
func (m *mockQuerier) GetReduceTaskOutputPaths(ctx context.Context, jobID uuid.UUID) ([]db.GetReduceTaskOutputPathsRow, error) {
	if m.getReduceTaskOutputPathsFn != nil {
		return m.getReduceTaskOutputPathsFn(ctx, jobID)
	}
	panic("mockQuerier.GetReduceTaskOutputPaths: not implemented")
}
func (m *mockQuerier) GetReduceTasksByJob(ctx context.Context, jobID uuid.UUID) ([]db.ReduceTask, error) {
	if m.getReduceTasksByJobFn != nil {
		return m.getReduceTasksByJobFn(ctx, jobID)
	}
	panic("mockQuerier.GetReduceTasksByJob: not implemented")
}
func (m *mockQuerier) GetReduceTasksByJobAndStatus(ctx context.Context, arg db.GetReduceTasksByJobAndStatusParams) ([]db.ReduceTask, error) {
	if m.getReduceTasksByJobAndStatusFn != nil {
		return m.getReduceTasksByJobAndStatusFn(ctx, arg)
	}
	panic("mockQuerier.GetReduceTasksByJobAndStatus: not implemented")
}
func (m *mockQuerier) GetStaleRunningMapTasks(ctx context.Context, dollar_1 sql.NullString) ([]db.MapTask, error) {
	if m.getStaleRunningMapTasksFn != nil {
		return m.getStaleRunningMapTasksFn(ctx, dollar_1)
	}
	panic("mockQuerier.GetStaleRunningMapTasks: not implemented")
}
func (m *mockQuerier) GetStaleRunningReduceTasks(ctx context.Context, dollar_1 sql.NullString) ([]db.ReduceTask, error) {
	if m.getStaleRunningReduceTasksFn != nil {
		return m.getStaleRunningReduceTasksFn(ctx, dollar_1)
	}
	panic("mockQuerier.GetStaleRunningReduceTasks: not implemented")
}
func (m *mockQuerier) IncrementMapTaskRetry(ctx context.Context, taskID uuid.UUID) error {
	if m.incrementMapTaskRetryFn != nil {
		return m.incrementMapTaskRetryFn(ctx, taskID)
	}
	panic("mockQuerier.IncrementMapTaskRetry: not implemented")
}
func (m *mockQuerier) IncrementReduceTaskRetry(ctx context.Context, taskID uuid.UUID) error {
	if m.incrementReduceTaskRetryFn != nil {
		return m.incrementReduceTaskRetryFn(ctx, taskID)
	}
	panic("mockQuerier.IncrementReduceTaskRetry: not implemented")
}
func (m *mockQuerier) ListStalePlugins(ctx context.Context, dollar_1 sql.NullString) ([]db.PluginCache, error) {
	if m.listStalePluginsFn != nil {
		return m.listStalePluginsFn(ctx, dollar_1)
	}
	return nil, nil
}
func (m *mockQuerier) MarkMapTaskCompleted(ctx context.Context, arg db.MarkMapTaskCompletedParams) error {
	if m.markMapTaskCompletedFn != nil {
		return m.markMapTaskCompletedFn(ctx, arg)
	}
	panic("mockQuerier.MarkMapTaskCompleted: not implemented")
}
func (m *mockQuerier) MarkMapTaskFailed(ctx context.Context, taskID uuid.UUID) error {
	if m.markMapTaskFailedFn != nil {
		return m.markMapTaskFailedFn(ctx, taskID)
	}
	return nil
}
func (m *mockQuerier) MarkMapTaskRunning(ctx context.Context, arg db.MarkMapTaskRunningParams) error {
	if m.markMapTaskRunningFn != nil {
		return m.markMapTaskRunningFn(ctx, arg)
	}
	return nil
}
func (m *mockQuerier) MarkReduceTaskCompleted(ctx context.Context, arg db.MarkReduceTaskCompletedParams) error {
	if m.markReduceTaskCompletedFn != nil {
		return m.markReduceTaskCompletedFn(ctx, arg)
	}
	panic("mockQuerier.MarkReduceTaskCompleted: not implemented")
}
func (m *mockQuerier) MarkReduceTaskFailed(ctx context.Context, taskID uuid.UUID) error {
	if m.markReduceTaskFailedFn != nil {
		return m.markReduceTaskFailedFn(ctx, taskID)
	}
	return nil
}
func (m *mockQuerier) MarkReduceTaskRunning(ctx context.Context, arg db.MarkReduceTaskRunningParams) error {
	if m.markReduceTaskRunningFn != nil {
		return m.markReduceTaskRunningFn(ctx, arg)
	}
	return nil
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
func (m *mockQuerier) UpdateJobStatus(ctx context.Context, arg db.UpdateJobStatusParams) error {
	if m.updateJobStatusFn != nil {
		return m.updateJobStatusFn(ctx, arg)
	}
	return nil
}
func (m *mockQuerier) UpdatePluginLastUsed(ctx context.Context, sourceHash string) error {
	if m.updatePluginLastUsedFn != nil {
		return m.updatePluginLastUsedFn(ctx, sourceHash)
	}
	return nil
}
func (m *mockQuerier) UpsertCachedPlugin(ctx context.Context, arg db.UpsertCachedPluginParams) error {
	if m.upsertCachedPluginFn != nil {
		return m.upsertCachedPluginFn(ctx, arg)
	}
	return nil
}

var testCfg = &config.Config{
	MyReplicaName:      "manager-0",
	TaskMaxRetries:     3,
	TaskTimeoutSeconds: 300,
	MinioBucketCode:    "plugins",
}

func newSupervisor(job db.Job, q db.Querier, spl interfaces.Splitter, disp interfaces.Dispatcher) *Supervisor {
	reg := NewRegistry()
	log, _ := zap.NewDevelopment()
	return New(job, q, spl, disp, nil, testCfg, log, reg)
}

func baseJob(status string) db.Job {
	return db.Job{
		JobID:        uuid.New(),
		OwnerUserID:  "user-1",
		OwnerReplica: "manager-0",
		Status:       status,
		MapperPath:   "code/mapper.py",
		ReducerPath:  "code/reducer.py",
		InputPath:    "input/data.jsonl",
		OutputPath:   "output/jobs/abc",
		NumMappers:   2,
		NumReducers:  2,
		InputFormat:  "jsonl",
		SubmittedAt:  time.Now(),
	}
}

func pendingMapTask(jobID uuid.UUID, index int32, retry int32) db.MapTask {
	return db.MapTask{
		TaskID:     uuid.New(),
		JobID:      jobID,
		TaskIndex:  index,
		Status:     "PENDING",
		InputFile:  "input/data.jsonl",
		RetryCount: retry,
	}
}

func pendingReduceTask(jobID uuid.UUID, index int32, retry int32) db.ReduceTask {
	return db.ReduceTask{
		TaskID:     uuid.New(),
		JobID:      jobID,
		TaskIndex:  index,
		Status:     "PENDING",
		RetryCount: retry,
	}
}

func TestStep_TerminalStatuses_ReturnNil(t *testing.T) {
	for _, status := range []string{"COMPLETED", "FAILED", "CANCELLED"} {
		t.Run(status, func(t *testing.T) {
			job := baseJob(status)
			q := &mockQuerier{
				getJobFn: func(_ context.Context, _ uuid.UUID) (db.Job, error) {
					return job, nil
				},
			}
			sup := newSupervisor(job, q, nil, nil)
			err := sup.step(context.Background())
			assert.NoError(t, err)
		})
	}
}

func TestStep_SplittingStatus_DoesNothing(t *testing.T) {
	// SPLITTING means split is already in flight — step should be a no-op.
	job := baseJob("SPLITTING")
	q := &mockQuerier{
		getJobFn: func(_ context.Context, _ uuid.UUID) (db.Job, error) {
			return job, nil
		},
	}
	sup := newSupervisor(job, q, nil, nil)
	err := sup.step(context.Background())
	assert.NoError(t, err)
}

func TestStep_GetJobError_ReturnsError(t *testing.T) {
	job := baseJob("MAP_PHASE")
	q := &mockQuerier{
		getJobFn: func(_ context.Context, _ uuid.UUID) (db.Job, error) {
			return db.Job{}, errors.New("db failure")
		},
	}
	sup := newSupervisor(job, q, nil, nil)
	err := sup.step(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "get job")
}

func TestDoSplit_Success_CreatesTasksAndDispatchesThem(t *testing.T) {
	job := baseJob("SUBMITTED")

	splits := []splitter.Split{
		{Index: 0, File: "input/data.jsonl", Offset: 0, Length: 512},
		{Index: 1, File: "input/data.jsonl", Offset: 512, Length: 512},
	}

	statusUpdates := []string{}
	tasksCreated := 0
	dispatchCalled := 0
	markRunningCalled := 0

	q := &mockQuerier{
		getJobFn: func(_ context.Context, _ uuid.UUID) (db.Job, error) {
			return job, nil
		},
		updateJobStatusFn: func(_ context.Context, a db.UpdateJobStatusParams) error {
			statusUpdates = append(statusUpdates, a.Status)
			return nil
		},
		createMapTaskFn: func(_ context.Context, a db.CreateMapTaskParams) (db.MapTask, error) {
			tasksCreated++
			assert.Equal(t, job.JobID, a.JobID)
			return db.MapTask{TaskID: uuid.New(), JobID: job.JobID, TaskIndex: a.TaskIndex}, nil
		},
		getPendingMapTasksFn: func(_ context.Context, _ db.GetPendingMapTasksParams) ([]db.MapTask, error) {
			// Return the two tasks as pending so they get dispatched.
			return []db.MapTask{
				{TaskID: uuid.New(), JobID: job.JobID, TaskIndex: 0, InputFile: "input/data.jsonl", RetryCount: 0},
				{TaskID: uuid.New(), JobID: job.JobID, TaskIndex: 1, InputFile: "input/data.jsonl", RetryCount: 0},
			}, nil
		},
		markMapTaskRunningFn: func(_ context.Context, _ db.MarkMapTaskRunningParams) error {
			markRunningCalled++
			return nil
		},
	}

	spl := &mockSplitter{
		computeFn: func(_ context.Context, key string, n int) ([]splitter.Split, error) {
			assert.Equal(t, job.InputPath, key)
			assert.Equal(t, int(job.NumMappers), n)
			return splits, nil
		},
	}

	disp := &mockDispatcher{
		dispatchMapFn: func(_ context.Context, spec dispatcher.MapTaskSpec) (string, error) {
			dispatchCalled++
			assert.Equal(t, job.JobID.String(), spec.JobID)
			assert.Equal(t, job.MapperPath, spec.MapperPath)
			assert.Equal(t, int(job.NumReducers), spec.NumReducers)
			return "map-job-" + spec.TaskID, nil
		},
	}

	sup := newSupervisor(job, q, spl, disp)
	err := sup.doSplit(context.Background())

	require.NoError(t, err)
	assert.Equal(t, []string{"SPLITTING", "MAP_PHASE"}, statusUpdates)
	assert.Equal(t, 2, tasksCreated)
	assert.Equal(t, 2, dispatchCalled)
	assert.Equal(t, 2, markRunningCalled)
}

func TestDoSplit_SplitterFails_FailsJob(t *testing.T) {
	job := baseJob("SUBMITTED")

	failCalled := false
	q := &mockQuerier{
		getJobFn:          func(_ context.Context, _ uuid.UUID) (db.Job, error) { return job, nil },
		updateJobStatusFn: func(_ context.Context, _ db.UpdateJobStatusParams) error { return nil },
		failJobFn: func(_ context.Context, a db.FailJobParams) error {
			failCalled = true
			assert.Equal(t, job.JobID, a.JobID)
			assert.Contains(t, a.ErrorMessage.String, "split")
			return nil
		},
	}

	spl := &mockSplitter{
		computeFn: func(_ context.Context, _ string, _ int) ([]splitter.Split, error) {
			return nil, errors.New("minio: connection refused")
		},
	}

	sup := newSupervisor(job, q, spl, nil)
	err := sup.doSplit(context.Background())

	require.NoError(t, err) // failJob returns nil; error is recorded in DB, not propagated
	assert.True(t, failCalled, "FailJob must be called when splitting fails")
}

func TestDoSplit_CreateMapTaskFails_FailsJob(t *testing.T) {
	job := baseJob("SUBMITTED")
	splits := []splitter.Split{{Index: 0, File: "input/data.jsonl", Offset: 0, Length: 1024}}

	failCalled := false
	q := &mockQuerier{
		getJobFn:          func(_ context.Context, _ uuid.UUID) (db.Job, error) { return job, nil },
		updateJobStatusFn: func(_ context.Context, _ db.UpdateJobStatusParams) error { return nil },
		createMapTaskFn: func(_ context.Context, _ db.CreateMapTaskParams) (db.MapTask, error) {
			return db.MapTask{}, errors.New("db: constraint violation")
		},
		failJobFn: func(_ context.Context, _ db.FailJobParams) error {
			failCalled = true
			return nil
		},
	}

	spl := &mockSplitter{
		computeFn: func(_ context.Context, _ string, _ int) ([]splitter.Split, error) {
			return splits, nil
		},
	}

	sup := newSupervisor(job, q, spl, nil)
	err := sup.doSplit(context.Background())

	require.NoError(t, err)
	assert.True(t, failCalled)
}

func TestDoSplit_MarkSplittingFails_ReturnsError(t *testing.T) {
	job := baseJob("SUBMITTED")
	q := &mockQuerier{
		getJobFn: func(_ context.Context, _ uuid.UUID) (db.Job, error) { return job, nil },
		updateJobStatusFn: func(_ context.Context, _ db.UpdateJobStatusParams) error {
			return errors.New("db: connection lost")
		},
	}

	sup := newSupervisor(job, q, nil, nil)
	err := sup.doSplit(context.Background())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "SPLITTING")
}

func TestCheckMapPhase_AllCompleted_TransitionsToReducePhase(t *testing.T) {
	job := baseJob("MAP_PHASE")

	statusUpdates := []string{}
	reducerTasksCreated := 0

	q := &mockQuerier{
		getJobFn: func(_ context.Context, _ uuid.UUID) (db.Job, error) { return job, nil },
		getPendingMapTasksFn: func(_ context.Context, _ db.GetPendingMapTasksParams) ([]db.MapTask, error) {
			return nil, nil // no pending tasks to dispatch
		},
		countMapTasksByStatusFn: func(_ context.Context, _ uuid.UUID) (db.CountMapTasksByStatusRow, error) {
			return db.CountMapTasksByStatusRow{Completed: 2, Failed: 0, Total: 2}, nil
		},
		updateJobStatusFn: func(_ context.Context, a db.UpdateJobStatusParams) error {
			statusUpdates = append(statusUpdates, a.Status)
			return nil
		},
		createReduceTaskFn: func(_ context.Context, _ db.CreateReduceTaskParams) (db.ReduceTask, error) {
			reducerTasksCreated++
			return db.ReduceTask{}, nil
		},
		getPendingReduceTasksFn: func(_ context.Context, _ db.GetPendingReduceTasksParams) ([]db.ReduceTask, error) {
			return nil, nil
		},
		getMapTaskOutputLocationsFn: func(_ context.Context, _ uuid.UUID) ([]db.GetMapTaskOutputLocationsRow, error) {
			return nil, nil
		},
	}

	sup := newSupervisor(job, q, nil, &mockDispatcher{})
	err := sup.checkMapPhase(context.Background())

	require.NoError(t, err)
	assert.Contains(t, statusUpdates, "REDUCE_PHASE")
	assert.Equal(t, int(job.NumReducers), reducerTasksCreated)
}

func TestCheckMapPhase_AllFailed_FailsJob(t *testing.T) {
	job := baseJob("MAP_PHASE")
	failCalled := false

	q := &mockQuerier{
		getJobFn: func(_ context.Context, _ uuid.UUID) (db.Job, error) { return job, nil },
		getPendingMapTasksFn: func(_ context.Context, _ db.GetPendingMapTasksParams) ([]db.MapTask, error) {
			return nil, nil
		},
		countMapTasksByStatusFn: func(_ context.Context, _ uuid.UUID) (db.CountMapTasksByStatusRow, error) {
			// 0 completed, 2 failed, 2 total → all terminal
			return db.CountMapTasksByStatusRow{Completed: 0, Failed: 2, Total: 2}, nil
		},
		failJobFn: func(_ context.Context, a db.FailJobParams) error {
			failCalled = true
			assert.True(t, a.ErrorMessage.Valid)
			return nil
		},
	}

	sup := newSupervisor(job, q, nil, nil)
	err := sup.checkMapPhase(context.Background())

	require.NoError(t, err)
	assert.True(t, failCalled)
}

func TestCheckMapPhase_PartiallyFailed_NotAllTerminal_DoesNotFail(t *testing.T) {
	// 1 completed, 1 failed, 3 total — still 1 running; do not fail yet.
	job := baseJob("MAP_PHASE")

	q := &mockQuerier{
		getJobFn: func(_ context.Context, _ uuid.UUID) (db.Job, error) { return job, nil },
		getPendingMapTasksFn: func(_ context.Context, _ db.GetPendingMapTasksParams) ([]db.MapTask, error) {
			return nil, nil
		},
		countMapTasksByStatusFn: func(_ context.Context, _ uuid.UUID) (db.CountMapTasksByStatusRow, error) {
			return db.CountMapTasksByStatusRow{Completed: 1, Failed: 1, Total: 3}, nil
		},
	}

	sup := newSupervisor(job, q, nil, nil)
	err := sup.checkMapPhase(context.Background())

	require.NoError(t, err)
	// failJob must NOT have been called — if it were, the mock would panic.
}

func TestCheckMapPhase_InProgress_DoesNotTransition(t *testing.T) {
	// 1 of 2 complete — still in progress.
	job := baseJob("MAP_PHASE")

	updateCalled := false
	q := &mockQuerier{
		getJobFn: func(_ context.Context, _ uuid.UUID) (db.Job, error) { return job, nil },
		getPendingMapTasksFn: func(_ context.Context, _ db.GetPendingMapTasksParams) ([]db.MapTask, error) {
			return nil, nil
		},
		countMapTasksByStatusFn: func(_ context.Context, _ uuid.UUID) (db.CountMapTasksByStatusRow, error) {
			return db.CountMapTasksByStatusRow{Completed: 1, Failed: 0, Total: 2}, nil
		},
		updateJobStatusFn: func(_ context.Context, _ db.UpdateJobStatusParams) error {
			updateCalled = true
			return nil
		},
	}

	sup := newSupervisor(job, q, nil, nil)
	err := sup.checkMapPhase(context.Background())

	require.NoError(t, err)
	assert.False(t, updateCalled, "UpdateJobStatus must not be called while map phase is in progress")
}

func TestDispatchPendingMapTasks_ExceedsMaxRetries_MarksFailed(t *testing.T) {
	job := baseJob("MAP_PHASE")
	task := pendingMapTask(job.JobID, 0, int32(testCfg.TaskMaxRetries)) // at limit

	markFailedCalled := false
	dispatchCalled := false

	q := &mockQuerier{
		getJobFn: func(_ context.Context, _ uuid.UUID) (db.Job, error) { return job, nil },
		getPendingMapTasksFn: func(_ context.Context, _ db.GetPendingMapTasksParams) ([]db.MapTask, error) {
			return []db.MapTask{task}, nil
		},
		markMapTaskFailedFn: func(_ context.Context, id uuid.UUID) error {
			markFailedCalled = true
			assert.Equal(t, task.TaskID, id)
			return nil
		},
	}

	disp := &mockDispatcher{
		dispatchMapFn: func(_ context.Context, _ dispatcher.MapTaskSpec) (string, error) {
			dispatchCalled = true
			return "", nil
		},
	}

	sup := newSupervisor(job, q, nil, disp)
	err := sup.dispatchPendingMapTasks(context.Background())

	require.NoError(t, err)
	assert.True(t, markFailedCalled, "MarkMapTaskFailed must be called when retry limit is reached")
	assert.False(t, dispatchCalled, "DispatchMap must NOT be called for exhausted tasks")
}

func TestDispatchPendingMapTasks_DispatchError_ContinuesToNextTask(t *testing.T) {
	// Dispatch fails for task 0 but should still attempt task 1.
	job := baseJob("MAP_PHASE")
	task0 := pendingMapTask(job.JobID, 0, 0)
	task1 := pendingMapTask(job.JobID, 1, 0)

	dispatched := []string{}
	q := &mockQuerier{
		getJobFn: func(_ context.Context, _ uuid.UUID) (db.Job, error) { return job, nil },
		getPendingMapTasksFn: func(_ context.Context, _ db.GetPendingMapTasksParams) ([]db.MapTask, error) {
			return []db.MapTask{task0, task1}, nil
		},
		markMapTaskRunningFn: func(_ context.Context, _ db.MarkMapTaskRunningParams) error { return nil },
	}

	disp := &mockDispatcher{
		dispatchMapFn: func(_ context.Context, spec dispatcher.MapTaskSpec) (string, error) {
			dispatched = append(dispatched, spec.TaskID)
			if spec.TaskID == task0.TaskID.String() {
				return "", errors.New("k8s: quota exceeded")
			}
			return "map-job-1", nil
		},
	}

	sup := newSupervisor(job, q, nil, disp)
	err := sup.dispatchPendingMapTasks(context.Background())

	require.NoError(t, err)
	assert.Len(t, dispatched, 2, "both tasks must be attempted even when the first dispatch fails")
}

func TestDispatchPendingMapTasks_Success_SetsK8sJobName(t *testing.T) {
	job := baseJob("MAP_PHASE")
	task := pendingMapTask(job.JobID, 0, 0)
	const k8sName = "map-abc12345-0"

	var capturedRunning db.MarkMapTaskRunningParams
	q := &mockQuerier{
		getJobFn: func(_ context.Context, _ uuid.UUID) (db.Job, error) { return job, nil },
		getPendingMapTasksFn: func(_ context.Context, _ db.GetPendingMapTasksParams) ([]db.MapTask, error) {
			return []db.MapTask{task}, nil
		},
		markMapTaskRunningFn: func(_ context.Context, a db.MarkMapTaskRunningParams) error {
			capturedRunning = a
			return nil
		},
	}

	disp := &mockDispatcher{
		dispatchMapFn: func(_ context.Context, _ dispatcher.MapTaskSpec) (string, error) {
			return k8sName, nil
		},
	}

	sup := newSupervisor(job, q, nil, disp)
	err := sup.dispatchPendingMapTasks(context.Background())

	require.NoError(t, err)
	assert.Equal(t, task.TaskID, capturedRunning.TaskID)
	assert.True(t, capturedRunning.K8sJobName.Valid)
	assert.Equal(t, k8sName, capturedRunning.K8sJobName.String)
}

func TestCheckReducePhase_AllCompleted_MarksJobCompleted(t *testing.T) {
	job := baseJob("REDUCE_PHASE")

	statusUpdates := []string{}
	q := &mockQuerier{
		getJobFn: func(_ context.Context, _ uuid.UUID) (db.Job, error) { return job, nil },
		getPendingReduceTasksFn: func(_ context.Context, _ db.GetPendingReduceTasksParams) ([]db.ReduceTask, error) {
			return nil, nil
		},
		countReduceTasksByStatusFn: func(_ context.Context, _ uuid.UUID) (db.CountReduceTasksByStatusRow, error) {
			return db.CountReduceTasksByStatusRow{Completed: 2, Failed: 0, Total: 2}, nil
		},
		updateJobStatusFn: func(_ context.Context, a db.UpdateJobStatusParams) error {
			statusUpdates = append(statusUpdates, a.Status)
			return nil
		},
		getMapTaskOutputLocationsFn: func(_ context.Context, _ uuid.UUID) ([]db.GetMapTaskOutputLocationsRow, error) {
			return nil, nil
		},
	}

	sup := newSupervisor(job, q, nil, &mockDispatcher{})
	err := sup.checkReducePhase(context.Background())

	require.NoError(t, err)
	assert.Contains(t, statusUpdates, "COMPLETED")
}

func TestCheckReducePhase_AllFailed_FailsJob(t *testing.T) {
	job := baseJob("REDUCE_PHASE")
	failCalled := false

	q := &mockQuerier{
		getJobFn: func(_ context.Context, _ uuid.UUID) (db.Job, error) { return job, nil },
		getPendingReduceTasksFn: func(_ context.Context, _ db.GetPendingReduceTasksParams) ([]db.ReduceTask, error) {
			return nil, nil
		},
		countReduceTasksByStatusFn: func(_ context.Context, _ uuid.UUID) (db.CountReduceTasksByStatusRow, error) {
			return db.CountReduceTasksByStatusRow{Completed: 0, Failed: 2, Total: 2}, nil
		},
		failJobFn: func(_ context.Context, _ db.FailJobParams) error {
			failCalled = true
			return nil
		},
		getMapTaskOutputLocationsFn: func(_ context.Context, _ uuid.UUID) ([]db.GetMapTaskOutputLocationsRow, error) {
			return nil, nil
		},
	}

	sup := newSupervisor(job, q, nil, &mockDispatcher{})
	err := sup.checkReducePhase(context.Background())

	require.NoError(t, err)
	assert.True(t, failCalled)
}

func TestCheckReducePhase_InProgress_DoesNotTransition(t *testing.T) {
	job := baseJob("REDUCE_PHASE")
	updateCalled := false

	q := &mockQuerier{
		getJobFn: func(_ context.Context, _ uuid.UUID) (db.Job, error) { return job, nil },
		getPendingReduceTasksFn: func(_ context.Context, _ db.GetPendingReduceTasksParams) ([]db.ReduceTask, error) {
			return nil, nil
		},
		countReduceTasksByStatusFn: func(_ context.Context, _ uuid.UUID) (db.CountReduceTasksByStatusRow, error) {
			return db.CountReduceTasksByStatusRow{Completed: 1, Failed: 0, Total: 2}, nil
		},
		updateJobStatusFn: func(_ context.Context, _ db.UpdateJobStatusParams) error {
			updateCalled = true
			return nil
		},
		getMapTaskOutputLocationsFn: func(_ context.Context, _ uuid.UUID) ([]db.GetMapTaskOutputLocationsRow, error) {
			return nil, nil
		},
	}

	sup := newSupervisor(job, q, nil, &mockDispatcher{})
	err := sup.checkReducePhase(context.Background())

	require.NoError(t, err)
	assert.False(t, updateCalled)
}

func TestDispatchPendingReduceTasks_ExceedsMaxRetries_MarksFailed(t *testing.T) {
	job := baseJob("REDUCE_PHASE")
	task := pendingReduceTask(job.JobID, 0, int32(testCfg.TaskMaxRetries))

	markFailedCalled := false
	q := &mockQuerier{
		getJobFn: func(_ context.Context, _ uuid.UUID) (db.Job, error) { return job, nil },
		getPendingReduceTasksFn: func(_ context.Context, _ db.GetPendingReduceTasksParams) ([]db.ReduceTask, error) {
			return []db.ReduceTask{task}, nil
		},
		markReduceTaskFailedFn: func(_ context.Context, id uuid.UUID) error {
			markFailedCalled = true
			assert.Equal(t, task.TaskID, id)
			return nil
		},
		getMapTaskOutputLocationsFn: func(_ context.Context, _ uuid.UUID) ([]db.GetMapTaskOutputLocationsRow, error) {
			return nil, nil
		},
	}

	sup := newSupervisor(job, q, nil, &mockDispatcher{})
	err := sup.dispatchPendingReduceTasks(context.Background())

	require.NoError(t, err)
	assert.True(t, markFailedCalled)
}

func TestDispatchPendingReduceTasks_Success_PassesCorrectInputLocations(t *testing.T) {
	job := baseJob("REDUCE_PHASE")
	task := pendingReduceTask(job.JobID, 0, 0) // reducer index 0

	// Map output: reducer 0 gets one file, reducer 1 gets another.
	rawLocs := json.RawMessage(`[{"reducer_index":0,"path":"jobs/abc/map-0-reduce-0.jsonl"},{"reducer_index":1,"path":"jobs/abc/map-0-reduce-1.jsonl"}]`)

	var capturedSpec dispatcher.ReduceTaskSpec
	q := &mockQuerier{
		getJobFn: func(_ context.Context, _ uuid.UUID) (db.Job, error) { return job, nil },
		getPendingReduceTasksFn: func(_ context.Context, _ db.GetPendingReduceTasksParams) ([]db.ReduceTask, error) {
			return []db.ReduceTask{task}, nil
		},
		getMapTaskOutputLocationsFn: func(_ context.Context, _ uuid.UUID) ([]db.GetMapTaskOutputLocationsRow, error) {
			return []db.GetMapTaskOutputLocationsRow{
				{TaskIndex: 0, OutputLocations: pqtype.NullRawMessage{RawMessage: rawLocs, Valid: true}},
			}, nil
		},
		markReduceTaskRunningFn: func(_ context.Context, _ db.MarkReduceTaskRunningParams) error { return nil },
	}

	disp := &mockDispatcher{
		dispatchReduceFn: func(_ context.Context, spec dispatcher.ReduceTaskSpec) (string, error) {
			capturedSpec = spec
			return "red-abc-0", nil
		},
	}

	sup := newSupervisor(job, q, nil, disp)
	err := sup.dispatchPendingReduceTasks(context.Background())

	require.NoError(t, err)
	assert.Equal(t, task.TaskID.String(), capturedSpec.TaskID)
	assert.Equal(t, int(task.TaskIndex), capturedSpec.TaskIndex)
	assert.Equal(t, job.ReducerPath, capturedSpec.ReducerPath)

	// Only the reducer-0 file should be in the input locations for this task.
	var locs []map[string]interface{}
	require.NoError(t, json.Unmarshal(capturedSpec.InputLocations, &locs))
	require.Len(t, locs, 1)
	assert.Equal(t, "jobs/abc/map-0-reduce-0.jsonl", locs[0]["path"])
}

func TestStartReducePhase_CreatesCorrectNumberOfReduceTasks(t *testing.T) {
	job := baseJob("MAP_PHASE")
	job.NumReducers = 3

	created := []int32{}
	q := &mockQuerier{
		getJobFn:          func(_ context.Context, _ uuid.UUID) (db.Job, error) { return job, nil },
		updateJobStatusFn: func(_ context.Context, _ db.UpdateJobStatusParams) error { return nil },
		createReduceTaskFn: func(_ context.Context, a db.CreateReduceTaskParams) (db.ReduceTask, error) {
			created = append(created, a.TaskIndex)
			return db.ReduceTask{TaskID: uuid.New(), JobID: job.JobID, TaskIndex: a.TaskIndex}, nil
		},
		getPendingReduceTasksFn: func(_ context.Context, _ db.GetPendingReduceTasksParams) ([]db.ReduceTask, error) {
			return nil, nil
		},
		getMapTaskOutputLocationsFn: func(_ context.Context, _ uuid.UUID) ([]db.GetMapTaskOutputLocationsRow, error) {
			return nil, nil
		},
	}

	sup := newSupervisor(job, q, nil, &mockDispatcher{})
	err := sup.startReducePhase(context.Background())

	require.NoError(t, err)
	assert.Equal(t, []int32{0, 1, 2}, created)
}

func TestStartReducePhase_CreateReduceTaskFails_FailsJob(t *testing.T) {
	job := baseJob("MAP_PHASE")
	failCalled := false

	q := &mockQuerier{
		getJobFn:          func(_ context.Context, _ uuid.UUID) (db.Job, error) { return job, nil },
		updateJobStatusFn: func(_ context.Context, _ db.UpdateJobStatusParams) error { return nil },
		createReduceTaskFn: func(_ context.Context, _ db.CreateReduceTaskParams) (db.ReduceTask, error) {
			return db.ReduceTask{}, errors.New("db: constraint violation")
		},
		failJobFn: func(_ context.Context, _ db.FailJobParams) error {
			failCalled = true
			return nil
		},
	}

	sup := newSupervisor(job, q, nil, nil)
	err := sup.startReducePhase(context.Background())

	require.NoError(t, err)
	assert.True(t, failCalled)
}

func TestFailJob_StoresReasonInDB(t *testing.T) {
	job := baseJob("MAP_PHASE")
	const reason = "map task 0 failed permanently"

	var stored db.FailJobParams
	q := &mockQuerier{
		getJobFn: func(_ context.Context, _ uuid.UUID) (db.Job, error) { return job, nil },
		failJobFn: func(_ context.Context, a db.FailJobParams) error {
			stored = a
			return nil
		},
	}

	sup := newSupervisor(job, q, nil, nil)
	err := sup.failJob(context.Background(), reason)

	require.NoError(t, err)
	assert.Equal(t, job.JobID, stored.JobID)
	assert.True(t, stored.ErrorMessage.Valid)
	assert.Equal(t, reason, stored.ErrorMessage.String)
}

func TestFailJob_DBError_Propagated(t *testing.T) {
	job := baseJob("MAP_PHASE")

	q := &mockQuerier{
		getJobFn: func(_ context.Context, _ uuid.UUID) (db.Job, error) { return job, nil },
		failJobFn: func(_ context.Context, _ db.FailJobParams) error {
			return errors.New("db: connection lost")
		},
	}

	sup := newSupervisor(job, q, nil, nil)
	err := sup.failJob(context.Background(), "some reason")

	require.Error(t, err)
}

func TestRun_ContextCancellation_Exits(t *testing.T) {
	// Run must exit cleanly when the context is cancelled.
	job := baseJob("COMPLETED") // terminal state — step is a no-op after the first tick

	q := &mockQuerier{
		getJobFn: func(_ context.Context, _ uuid.UUID) (db.Job, error) { return job, nil },
	}

	sup := newSupervisor(job, q, nil, &mockDispatcher{})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		sup.Run(ctx)
		close(done)
	}()

	cancel()

	select {
	case <-done:
		// exited cleanly as expected
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not exit after context cancellation")
	}
}

func TestRun_RegistersAndDeregistersFromRegistry(t *testing.T) {
	job := baseJob("SPLITTING") // non-terminal state
	reg := NewRegistry()

	q := &mockQuerier{
		getJobFn: func(_ context.Context, _ uuid.UUID) (db.Job, error) { return job, nil },
	}

	sup := New(job, q, nil, &mockDispatcher{}, nil, testCfg, zap.NewNop(), reg)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		sup.Run(ctx)
		close(done)
	}()

	// Give Run time to register.
	time.Sleep(20 * time.Millisecond)

	reg.mu.RLock()
	_, registered := reg.supervisors[job.JobID]
	reg.mu.RUnlock()
	assert.True(t, registered, "supervisor must be registered while running")

	cancel()
	<-done

	reg.mu.RLock()
	_, stillRegistered := reg.supervisors[job.JobID]
	reg.mu.RUnlock()
	assert.False(t, stillRegistered, "supervisor must be deregistered after Run exits")
}

func TestRun_NotifyChannel_TriggersStep(t *testing.T) {
	// A Notify poke must cause step() to run (beyond the initial call).
	// Use atomic.Int64 so the Run goroutine (writer) and the test goroutine
	// (reader) don't race on the counter.
	var stepCount atomic.Int64
	job := baseJob("SPLITTING") // non-terminal

	q := &mockQuerier{
		getJobFn: func(_ context.Context, _ uuid.UUID) (db.Job, error) {
			stepCount.Add(1)
			return job, nil
		},
	}

	reg := NewRegistry()
	sup := New(job, q, nil, &mockDispatcher{}, nil, testCfg, zap.NewNop(), reg)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		sup.Run(ctx)
		close(done)
	}()

	// Wait for the initial step to fire.
	time.Sleep(20 * time.Millisecond)
	initialCount := stepCount.Load()

	// Trigger an additional step via Notify.
	reg.Notify(job.JobID)
	time.Sleep(20 * time.Millisecond)

	cancel()
	<-done

	assert.Greater(t, stepCount.Load(), initialCount, "Notify must trigger at least one additional step")
}

type mockTransport struct {
	roundTripFn func(*http.Request) (*http.Response, error)
}

func (m mockTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return m.roundTripFn(req)
}

func mockMinioClient(t *testing.T) *minio.Client {
	mc, err := minio.New("localhost:9000", &minio.Options{
		Transport: mockTransport{
			roundTripFn: func(req *http.Request) (*http.Response, error) {
				if req.URL.Query().Has("location") {
					return &http.Response{
						StatusCode: 200,
						Header:     make(http.Header),
						Body:       io.NopCloser(strings.NewReader(`<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">us-east-1</LocationConstraint>`)),
					}, nil
				}
				resp := &http.Response{
					StatusCode: 200,
					Header:     make(http.Header),
					Body:       io.NopCloser(strings.NewReader("")),
				}
				resp.Header.Set("ETag", "\"mock-etag\"")
				resp.Header.Set("Last-Modified", "Mon, 02 Jan 2006 15:04:05 GMT")
				return resp, nil
			},
		},
	})
	require.NoError(t, err)
	return mc
}

func TestCleanup_DeletesAllJobs(t *testing.T) {
	job := baseJob("COMPLETED")
	deletedJobs := []string{}

	q := &mockQuerier{
		getMapTaskJobNamesFn: func(ctx context.Context, jobID uuid.UUID) ([]sql.NullString, error) {
			return []sql.NullString{{String: "map-1", Valid: true}, {String: "map-2", Valid: true}}, nil
		},
		getReduceTaskJobNamesFn: func(ctx context.Context, jobID uuid.UUID) ([]sql.NullString, error) {
			return []sql.NullString{{String: "reduce-1", Valid: true}}, nil
		},
	}

	disp := &mockDispatcher{
		deleteJobFn: func(ctx context.Context, jobName string) error {
			deletedJobs = append(deletedJobs, jobName)
			return nil
		},
	}

	sup := newSupervisor(job, q, nil, disp)
	sup.Cleanup(context.Background())

	shortID := job.JobID.String()[:8]
	assert.Contains(t, deletedJobs, "build-map-"+shortID)
	assert.Contains(t, deletedJobs, "build-red-"+shortID)
	assert.Contains(t, deletedJobs, "map-1")
	assert.Contains(t, deletedJobs, "map-2")
	assert.Contains(t, deletedJobs, "reduce-1")
	assert.Len(t, deletedJobs, 5)
}

func TestCheckBuildPhase_StillBuilding_ReturnsNil(t *testing.T) {
	job := baseJob("BUILDING")
	job.MapperPath = "mapper.go" // still building
	job.ReducerPath = "builds/abc/reducer" // done

	q := &mockQuerier{
		getJobFn: func(_ context.Context, _ uuid.UUID) (db.Job, error) { return job, nil },
	}

	sup := newSupervisor(job, q, nil, nil)
	err := sup.checkBuildPhase(context.Background())
	assert.NoError(t, err)
}

func TestCheckBuildPhase_BuildCompleted_TransitionsToSplit(t *testing.T) {
	job := baseJob("BUILDING")
	job.MapperPath = "builds/abc/mapper" // done
	job.ReducerPath = "builds/abc/reducer" // done

	statusUpdates := []string{}
	q := &mockQuerier{
		getJobFn: func(_ context.Context, _ uuid.UUID) (db.Job, error) { return job, nil },
		updateJobStatusFn: func(_ context.Context, a db.UpdateJobStatusParams) error {
			statusUpdates = append(statusUpdates, a.Status)
			return nil
		},
		createMapTaskFn: func(_ context.Context, a db.CreateMapTaskParams) (db.MapTask, error) {
			return db.MapTask{TaskID: uuid.New(), JobID: job.JobID, TaskIndex: a.TaskIndex}, nil
		},
		getPendingMapTasksFn: func(_ context.Context, _ db.GetPendingMapTasksParams) ([]db.MapTask, error) {
			return nil, nil
		},
	}

	spl := &mockSplitter{
		computeFn: func(_ context.Context, key string, n int) ([]splitter.Split, error) {
			return []splitter.Split{}, nil
		},
	}

	disp := &mockDispatcher{
		dispatchMapFn: func(ctx context.Context, spec dispatcher.MapTaskSpec) (string, error) {
			return "", nil
		},
	}

	sup := newSupervisor(job, q, spl, disp)
	err := sup.checkBuildPhase(context.Background())

	require.NoError(t, err)
	assert.Contains(t, statusUpdates, "SPLITTING")
}

func TestDoBuild_NoBuildNeeded_SkipsToSplit(t *testing.T) {
	job := baseJob("SUBMITTED")
	// Mapper and Reducer already .py
	statusUpdates := []string{}
	q := &mockQuerier{
		getJobFn: func(_ context.Context, _ uuid.UUID) (db.Job, error) { return job, nil },
		updateJobStatusFn: func(_ context.Context, a db.UpdateJobStatusParams) error {
			statusUpdates = append(statusUpdates, a.Status)
			return nil
		},
		createMapTaskFn: func(_ context.Context, a db.CreateMapTaskParams) (db.MapTask, error) {
			return db.MapTask{TaskID: uuid.New(), JobID: job.JobID, TaskIndex: a.TaskIndex}, nil
		},
		getPendingMapTasksFn: func(_ context.Context, _ db.GetPendingMapTasksParams) ([]db.MapTask, error) {
			return nil, nil
		},
	}

	spl := &mockSplitter{
		computeFn: func(_ context.Context, key string, n int) ([]splitter.Split, error) {
			return []splitter.Split{}, nil
		},
	}

	disp := &mockDispatcher{
		dispatchMapFn: func(ctx context.Context, spec dispatcher.MapTaskSpec) (string, error) {
			return "", nil
		},
	}

	sup := newSupervisor(job, q, spl, disp)
	err := sup.doBuild(context.Background())

	require.NoError(t, err)
	assert.Contains(t, statusUpdates, "SPLITTING")
}

func TestDoBuild_CacheHit_SkipsBuild(t *testing.T) {
	job := baseJob("SUBMITTED")
	job.MapperPath = "mapper.go"
	job.ReducerPath = "reducer.go"

	statusUpdates := []string{}
	q := &mockQuerier{
		getJobFn: func(_ context.Context, _ uuid.UUID) (db.Job, error) { return job, nil },
		updateJobStatusFn: func(_ context.Context, a db.UpdateJobStatusParams) error {
			statusUpdates = append(statusUpdates, a.Status)
			return nil
		},
		getCachedPluginFn: func(ctx context.Context, sourceHash string) (db.PluginCache, error) {
			return db.PluginCache{BinaryPath: "builds/cached"}, nil
		},
		updateJobMapperPathFn: func(ctx context.Context, a db.UpdateJobMapperPathParams) error { return nil },
		updateJobReducerPathFn: func(ctx context.Context, a db.UpdateJobReducerPathParams) error { return nil },
		updatePluginLastUsedFn: func(ctx context.Context, sourceHash string) error { return nil },
		createMapTaskFn: func(_ context.Context, a db.CreateMapTaskParams) (db.MapTask, error) {
			return db.MapTask{TaskID: uuid.New(), JobID: job.JobID, TaskIndex: a.TaskIndex}, nil
		},
		getPendingMapTasksFn: func(_ context.Context, _ db.GetPendingMapTasksParams) ([]db.MapTask, error) {
			return nil, nil
		},
	}

	spl := &mockSplitter{
		computeFn: func(_ context.Context, key string, n int) ([]splitter.Split, error) {
			return []splitter.Split{}, nil
		},
	}

	disp := &mockDispatcher{
		dispatchBuildFn: func(ctx context.Context, spec dispatcher.BuildTaskSpec) (string, error) {
			t.Fatal("DispatchBuild should not be called on cache hit")
			return "", nil
		},
	}

	sup := newSupervisor(job, q, spl, disp)
	sup.minio = mockMinioClient(t)

	err := sup.doBuild(context.Background())

	require.NoError(t, err)
	assert.Contains(t, statusUpdates, "BUILDING")
	assert.Contains(t, statusUpdates, "SPLITTING")
}

func TestDoBuild_CacheMiss_DispatchesBuild(t *testing.T) {
	job := baseJob("SUBMITTED")
	job.MapperPath = "mapper.go"
	job.ReducerPath = "reducer.go"

	statusUpdates := []string{}
	buildsDispatched := 0
	q := &mockQuerier{
		getJobFn: func(_ context.Context, _ uuid.UUID) (db.Job, error) { return job, nil },
		updateJobStatusFn: func(_ context.Context, a db.UpdateJobStatusParams) error {
			statusUpdates = append(statusUpdates, a.Status)
			return nil
		},
		getCachedPluginFn: func(ctx context.Context, sourceHash string) (db.PluginCache, error) {
			return db.PluginCache{}, sql.ErrNoRows
		},
	}

	disp := &mockDispatcher{
		dispatchBuildFn: func(ctx context.Context, spec dispatcher.BuildTaskSpec) (string, error) {
			buildsDispatched++
			return "build-job", nil
		},
	}

	sup := newSupervisor(job, q, nil, disp)
	sup.minio = mockMinioClient(t)

	err := sup.doBuild(context.Background())

	require.NoError(t, err)
	assert.Contains(t, statusUpdates, "BUILDING")
	assert.Equal(t, 2, buildsDispatched)
}
