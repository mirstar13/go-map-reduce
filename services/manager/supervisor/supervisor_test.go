package supervisor

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mirstar13/go-map-reduce/db"
	"github.com/mirstar13/go-map-reduce/services/manager/config"
	"github.com/mirstar13/go-map-reduce/services/manager/dispatcher"
	"github.com/mirstar13/go-map-reduce/services/manager/splitter"
	"github.com/sqlc-dev/pqtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// mockSplitter satisfies the Splitter interface.
type mockSplitter struct {
	computeFn func(ctx context.Context, objectKey string, numSplits int) ([]splitter.Split, error)
}

func (m *mockSplitter) Compute(ctx context.Context, key string, n int) ([]splitter.Split, error) {
	if m.computeFn != nil {
		return m.computeFn(ctx, key, n)
	}
	panic("mockSplitter.Compute: not implemented")
}

// mockDispatcher satisfies the Dispatcher interface.
type mockDispatcher struct {
	dispatchMapFn    func(ctx context.Context, spec dispatcher.MapTaskSpec) (string, error)
	dispatchReduceFn func(ctx context.Context, spec dispatcher.ReduceTaskSpec) (string, error)
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

func (m *mockDispatcher) DeleteJob(ctx context.Context, name string) error {
	if m.deleteJobFn != nil {
		return m.deleteJobFn(ctx, name)
	}
	return nil // default: no-op
}

// mockQuerier satisfies db.Querier — we only implement what the supervisor calls.
// Unimplemented methods panic loudly so failures are obvious.
type mockQuerier struct {
	getJobFn                    func(ctx context.Context, jobID uuid.UUID) (db.Job, error)
	updateJobStatusFn           func(ctx context.Context, arg db.UpdateJobStatusParams) error
	failJobFn                   func(ctx context.Context, arg db.FailJobParams) error
	createMapTaskFn             func(ctx context.Context, arg db.CreateMapTaskParams) (db.MapTask, error)
	getPendingMapTasksFn        func(ctx context.Context, arg db.GetPendingMapTasksParams) ([]db.MapTask, error)
	markMapTaskRunningFn        func(ctx context.Context, arg db.MarkMapTaskRunningParams) error
	markMapTaskFailedFn         func(ctx context.Context, taskID uuid.UUID) error
	countMapTasksByStatusFn     func(ctx context.Context, jobID uuid.UUID) (db.CountMapTasksByStatusRow, error)
	createReduceTaskFn          func(ctx context.Context, arg db.CreateReduceTaskParams) (db.ReduceTask, error)
	getPendingReduceTasksFn     func(ctx context.Context, arg db.GetPendingReduceTasksParams) ([]db.ReduceTask, error)
	markReduceTaskRunningFn     func(ctx context.Context, arg db.MarkReduceTaskRunningParams) error
	markReduceTaskFailedFn      func(ctx context.Context, taskID uuid.UUID) error
	countReduceTasksByStatusFn  func(ctx context.Context, jobID uuid.UUID) (db.CountReduceTasksByStatusRow, error)
	getMapTaskOutputLocationsFn func(ctx context.Context, jobID uuid.UUID) ([]db.GetMapTaskOutputLocationsRow, error)
}

var _ db.Querier = (*mockQuerier)(nil)

func (m *mockQuerier) GetJob(ctx context.Context, id uuid.UUID) (db.Job, error) {
	if m.getJobFn != nil {
		return m.getJobFn(ctx, id)
	}
	panic("mockQuerier.GetJob: not implemented")
}
func (m *mockQuerier) UpdateJobStatus(ctx context.Context, a db.UpdateJobStatusParams) error {
	if m.updateJobStatusFn != nil {
		return m.updateJobStatusFn(ctx, a)
	}
	return nil
}
func (m *mockQuerier) FailJob(ctx context.Context, a db.FailJobParams) error {
	if m.failJobFn != nil {
		return m.failJobFn(ctx, a)
	}
	return nil
}
func (m *mockQuerier) CreateMapTask(ctx context.Context, a db.CreateMapTaskParams) (db.MapTask, error) {
	if m.createMapTaskFn != nil {
		return m.createMapTaskFn(ctx, a)
	}
	return db.MapTask{}, nil
}
func (m *mockQuerier) GetPendingMapTasks(ctx context.Context, a db.GetPendingMapTasksParams) ([]db.MapTask, error) {
	if m.getPendingMapTasksFn != nil {
		return m.getPendingMapTasksFn(ctx, a)
	}
	return nil, nil
}
func (m *mockQuerier) MarkMapTaskRunning(ctx context.Context, a db.MarkMapTaskRunningParams) error {
	if m.markMapTaskRunningFn != nil {
		return m.markMapTaskRunningFn(ctx, a)
	}
	return nil
}
func (m *mockQuerier) MarkMapTaskFailed(ctx context.Context, id uuid.UUID) error {
	if m.markMapTaskFailedFn != nil {
		return m.markMapTaskFailedFn(ctx, id)
	}
	return nil
}
func (m *mockQuerier) CountMapTasksByStatus(ctx context.Context, id uuid.UUID) (db.CountMapTasksByStatusRow, error) {
	if m.countMapTasksByStatusFn != nil {
		return m.countMapTasksByStatusFn(ctx, id)
	}
	return db.CountMapTasksByStatusRow{}, nil
}
func (m *mockQuerier) CreateReduceTask(ctx context.Context, a db.CreateReduceTaskParams) (db.ReduceTask, error) {
	if m.createReduceTaskFn != nil {
		return m.createReduceTaskFn(ctx, a)
	}
	return db.ReduceTask{}, nil
}
func (m *mockQuerier) GetPendingReduceTasks(ctx context.Context, a db.GetPendingReduceTasksParams) ([]db.ReduceTask, error) {
	if m.getPendingReduceTasksFn != nil {
		return m.getPendingReduceTasksFn(ctx, a)
	}
	return nil, nil
}
func (m *mockQuerier) MarkReduceTaskRunning(ctx context.Context, a db.MarkReduceTaskRunningParams) error {
	if m.markReduceTaskRunningFn != nil {
		return m.markReduceTaskRunningFn(ctx, a)
	}
	return nil
}
func (m *mockQuerier) MarkReduceTaskFailed(ctx context.Context, id uuid.UUID) error {
	if m.markReduceTaskFailedFn != nil {
		return m.markReduceTaskFailedFn(ctx, id)
	}
	return nil
}
func (m *mockQuerier) CountReduceTasksByStatus(ctx context.Context, id uuid.UUID) (db.CountReduceTasksByStatusRow, error) {
	if m.countReduceTasksByStatusFn != nil {
		return m.countReduceTasksByStatusFn(ctx, id)
	}
	return db.CountReduceTasksByStatusRow{}, nil
}
func (m *mockQuerier) GetMapTaskOutputLocations(ctx context.Context, id uuid.UUID) ([]db.GetMapTaskOutputLocationsRow, error) {
	if m.getMapTaskOutputLocationsFn != nil {
		return m.getMapTaskOutputLocationsFn(ctx, id)
	}
	return nil, nil
}

// Remaining stubs — not called by the supervisor.
func (m *mockQuerier) CancelJob(ctx context.Context, jobID uuid.UUID) error {
	panic("not implemented")
}
func (m *mockQuerier) CountJobsByStatus(ctx context.Context) ([]db.CountJobsByStatusRow, error) {
	panic("not implemented")
}
func (m *mockQuerier) CreateJob(ctx context.Context, arg db.CreateJobParams) (db.Job, error) {
	panic("not implemented")
}
func (m *mockQuerier) GetActiveJobsByReplica(ctx context.Context, r string) ([]db.Job, error) {
	panic("not implemented")
}
func (m *mockQuerier) GetAllJobs(ctx context.Context) ([]db.Job, error) { panic("not implemented") }
func (m *mockQuerier) GetJobsByUser(ctx context.Context, uid string) ([]db.Job, error) {
	panic("not implemented")
}
func (m *mockQuerier) GetMapTask(ctx context.Context, id uuid.UUID) (db.MapTask, error) {
	panic("not implemented")
}
func (m *mockQuerier) GetMapTasksByJob(ctx context.Context, id uuid.UUID) ([]db.MapTask, error) {
	panic("not implemented")
}
func (m *mockQuerier) GetMapTasksByJobAndStatus(ctx context.Context, a db.GetMapTasksByJobAndStatusParams) ([]db.MapTask, error) {
	panic("not implemented")
}
func (m *mockQuerier) GetReduceTask(ctx context.Context, id uuid.UUID) (db.ReduceTask, error) {
	panic("not implemented")
}
func (m *mockQuerier) GetReduceTaskOutputPaths(ctx context.Context, id uuid.UUID) ([]db.GetReduceTaskOutputPathsRow, error) {
	panic("not implemented")
}
func (m *mockQuerier) GetReduceTasksByJob(ctx context.Context, id uuid.UUID) ([]db.ReduceTask, error) {
	panic("not implemented")
}
func (m *mockQuerier) GetReduceTasksByJobAndStatus(ctx context.Context, a db.GetReduceTasksByJobAndStatusParams) ([]db.ReduceTask, error) {
	panic("not implemented")
}
func (m *mockQuerier) GetStaleRunningMapTasks(ctx context.Context, d sql.NullString) ([]db.MapTask, error) {
	panic("not implemented")
}
func (m *mockQuerier) GetStaleRunningReduceTasks(ctx context.Context, d sql.NullString) ([]db.ReduceTask, error) {
	panic("not implemented")
}
func (m *mockQuerier) IncrementMapTaskRetry(ctx context.Context, id uuid.UUID) error {
	panic("not implemented")
}
func (m *mockQuerier) IncrementReduceTaskRetry(ctx context.Context, id uuid.UUID) error {
	panic("not implemented")
}
func (m *mockQuerier) MarkMapTaskCompleted(ctx context.Context, a db.MarkMapTaskCompletedParams) error {
	panic("not implemented")
}
func (m *mockQuerier) MarkReduceTaskCompleted(ctx context.Context, a db.MarkReduceTaskCompletedParams) error {
	panic("not implemented")
}

var testCfg = &config.Config{
	MyReplicaName:      "manager-0",
	TaskMaxRetries:     3,
	TaskTimeoutSeconds: 300,
}

func newSupervisor(job db.Job, q db.Querier, spl Splitter, disp Dispatcher) *Supervisor {
	reg := NewRegistry()
	return New(job, q, spl, disp, testCfg, zap.NewNop(), reg)
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

	sup := newSupervisor(job, q, nil, nil)

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
	job := baseJob("COMPLETED")
	reg := NewRegistry()

	q := &mockQuerier{
		getJobFn: func(_ context.Context, _ uuid.UUID) (db.Job, error) { return job, nil },
	}

	sup := New(job, q, nil, nil, testCfg, zap.NewNop(), reg)

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
	stepCount := 0
	job := baseJob("COMPLETED")

	q := &mockQuerier{
		getJobFn: func(_ context.Context, _ uuid.UUID) (db.Job, error) {
			stepCount++
			return job, nil
		},
	}

	reg := NewRegistry()
	sup := New(job, q, nil, nil, testCfg, zap.NewNop(), reg)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		sup.Run(ctx)
		close(done)
	}()

	// Wait for initial step.
	time.Sleep(20 * time.Millisecond)
	initialCount := stepCount

	// Trigger an additional step via Notify.
	reg.Notify(job.JobID)
	time.Sleep(20 * time.Millisecond)

	cancel()
	<-done

	assert.Greater(t, stepCount, initialCount, "Notify must trigger at least one additional step")
}
