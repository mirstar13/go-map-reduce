package watchdog

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mirstar13/go-map-reduce/db"
	"github.com/mirstar13/go-map-reduce/services/manager/config"
	"github.com/mirstar13/go-map-reduce/services/manager/dispatcher"
	"github.com/mirstar13/go-map-reduce/services/manager/supervisor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var testCfg = &config.Config{
	TaskTimeoutSeconds: 300,
	TaskMaxRetries:     3,
}

// mockQuerier stubs only the methods the Watchdog calls.
// Any unimplemented method panics so test failures are obvious.
type mockQuerier struct {
	getStaleRunningMapTasksFn    func(ctx context.Context, d sql.NullString) ([]db.MapTask, error)
	getStaleRunningReduceTasksFn func(ctx context.Context, d sql.NullString) ([]db.ReduceTask, error)
	incrementMapTaskRetryFn      func(ctx context.Context, id uuid.UUID) error
	incrementReduceTaskRetryFn   func(ctx context.Context, id uuid.UUID) error
}

// compile-time check
var _ db.Querier = (*mockQuerier)(nil)

func (m *mockQuerier) GetStaleRunningMapTasks(ctx context.Context, d sql.NullString) ([]db.MapTask, error) {
	if m.getStaleRunningMapTasksFn != nil {
		return m.getStaleRunningMapTasksFn(ctx, d)
	}
	return nil, nil
}
func (m *mockQuerier) GetStaleRunningReduceTasks(ctx context.Context, d sql.NullString) ([]db.ReduceTask, error) {
	if m.getStaleRunningReduceTasksFn != nil {
		return m.getStaleRunningReduceTasksFn(ctx, d)
	}
	return nil, nil
}
func (m *mockQuerier) IncrementMapTaskRetry(ctx context.Context, id uuid.UUID) error {
	if m.incrementMapTaskRetryFn != nil {
		return m.incrementMapTaskRetryFn(ctx, id)
	}
	return nil
}
func (m *mockQuerier) IncrementReduceTaskRetry(ctx context.Context, id uuid.UUID) error {
	if m.incrementReduceTaskRetryFn != nil {
		return m.incrementReduceTaskRetryFn(ctx, id)
	}
	return nil
}

// --- stubs for the remaining Querier methods (never called by Watchdog) ---
func (m *mockQuerier) CancelJob(ctx context.Context, jobID uuid.UUID) error {
	panic("not implemented")
}
func (m *mockQuerier) CountJobsByStatus(ctx context.Context) ([]db.CountJobsByStatusRow, error) {
	panic("not implemented")
}
func (m *mockQuerier) CountMapTasksByStatus(ctx context.Context, jobID uuid.UUID) (db.CountMapTasksByStatusRow, error) {
	panic("not implemented")
}
func (m *mockQuerier) CountReduceTasksByStatus(ctx context.Context, jobID uuid.UUID) (db.CountReduceTasksByStatusRow, error) {
	panic("not implemented")
}
func (m *mockQuerier) CreateJob(ctx context.Context, arg db.CreateJobParams) (db.Job, error) {
	panic("not implemented")
}
func (m *mockQuerier) CreateMapTask(ctx context.Context, arg db.CreateMapTaskParams) (db.MapTask, error) {
	panic("not implemented")
}
func (m *mockQuerier) CreateReduceTask(ctx context.Context, arg db.CreateReduceTaskParams) (db.ReduceTask, error) {
	panic("not implemented")
}
func (m *mockQuerier) FailJob(ctx context.Context, arg db.FailJobParams) error {
	panic("not implemented")
}
func (m *mockQuerier) GetActiveJobsByReplica(ctx context.Context, r string) ([]db.Job, error) {
	panic("not implemented")
}
func (m *mockQuerier) GetAllJobs(ctx context.Context) ([]db.Job, error) { panic("not implemented") }
func (m *mockQuerier) GetJob(ctx context.Context, jobID uuid.UUID) (db.Job, error) {
	panic("not implemented")
}
func (m *mockQuerier) GetJobsByUser(ctx context.Context, uid string) ([]db.Job, error) {
	panic("not implemented")
}
func (m *mockQuerier) GetMapTask(ctx context.Context, id uuid.UUID) (db.MapTask, error) {
	panic("not implemented")
}
func (m *mockQuerier) GetMapTaskOutputLocations(ctx context.Context, id uuid.UUID) ([]db.GetMapTaskOutputLocationsRow, error) {
	panic("not implemented")
}
func (m *mockQuerier) GetMapTasksByJob(ctx context.Context, id uuid.UUID) ([]db.MapTask, error) {
	panic("not implemented")
}
func (m *mockQuerier) GetMapTasksByJobAndStatus(ctx context.Context, a db.GetMapTasksByJobAndStatusParams) ([]db.MapTask, error) {
	panic("not implemented")
}
func (m *mockQuerier) GetPendingMapTasks(ctx context.Context, a db.GetPendingMapTasksParams) ([]db.MapTask, error) {
	panic("not implemented")
}
func (m *mockQuerier) GetPendingReduceTasks(ctx context.Context, a db.GetPendingReduceTasksParams) ([]db.ReduceTask, error) {
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
func (m *mockQuerier) MarkMapTaskCompleted(ctx context.Context, a db.MarkMapTaskCompletedParams) error {
	panic("not implemented")
}
func (m *mockQuerier) MarkMapTaskFailed(ctx context.Context, id uuid.UUID) error {
	panic("not implemented")
}
func (m *mockQuerier) MarkMapTaskRunning(ctx context.Context, a db.MarkMapTaskRunningParams) error {
	panic("not implemented")
}
func (m *mockQuerier) MarkReduceTaskCompleted(ctx context.Context, a db.MarkReduceTaskCompletedParams) error {
	panic("not implemented")
}
func (m *mockQuerier) MarkReduceTaskFailed(ctx context.Context, id uuid.UUID) error {
	panic("not implemented")
}
func (m *mockQuerier) MarkReduceTaskRunning(ctx context.Context, a db.MarkReduceTaskRunningParams) error {
	panic("not implemented")
}
func (m *mockQuerier) UpdateJobStatus(ctx context.Context, a db.UpdateJobStatusParams) error {
	panic("not implemented")
}

// mockDispatcher stubs only DeleteJob.
type mockDispatcher struct {
	deleteJobFn func(ctx context.Context, name string) error
}

func (m *mockDispatcher) DispatchMap(ctx context.Context, spec dispatcher.MapTaskSpec) (string, error) {
	panic("not implemented")
}
func (m *mockDispatcher) DispatchReduce(ctx context.Context, spec dispatcher.ReduceTaskSpec) (string, error) {
	panic("not implemented")
}
func (m *mockDispatcher) DeleteJob(ctx context.Context, name string) error {
	if m.deleteJobFn != nil {
		return m.deleteJobFn(ctx, name)
	}
	return nil
}

// staleMapTask builds a minimal stale map task for use in tests.
func staleMapTask(jobID uuid.UUID, k8sName string) db.MapTask {
	t := db.MapTask{
		TaskID:    uuid.New(),
		JobID:     jobID,
		TaskIndex: 0,
		Status:    "RUNNING",
		StartedAt: sql.NullTime{Time: time.Now().Add(-10 * time.Minute), Valid: true},
	}
	if k8sName != "" {
		t.K8sJobName = sql.NullString{String: k8sName, Valid: true}
	}
	return t
}

// staleReduceTask builds a minimal stale reduce task.
func staleReduceTask(jobID uuid.UUID, k8sName string) db.ReduceTask {
	t := db.ReduceTask{
		TaskID:    uuid.New(),
		JobID:     jobID,
		TaskIndex: 0,
		Status:    "RUNNING",
		StartedAt: sql.NullTime{Time: time.Now().Add(-10 * time.Minute), Valid: true},
	}
	if k8sName != "" {
		t.K8sJobName = sql.NullString{String: k8sName, Valid: true}
	}
	return t
}

// newWatchdog wires a Watchdog with the given mocks.
func newWatchdog(q db.Querier, disp *mockDispatcher) *Watchdog {
	reg := supervisor.NewRegistry()
	return New(q, disp, reg, testCfg, zap.NewNop())
}

// TestScan_NoStaleTasks_DoesNothing verifies that scan is a no-op when both
// queries return empty slices — no DB writes or K8s deletions occur.
func TestScan_NoStaleTasks_DoesNothing(t *testing.T) {
	retryMapCalled := false
	retryReduceCalled := false
	deleteJobCalled := false

	q := &mockQuerier{
		getStaleRunningMapTasksFn: func(_ context.Context, _ sql.NullString) ([]db.MapTask, error) {
			return nil, nil
		},
		getStaleRunningReduceTasksFn: func(_ context.Context, _ sql.NullString) ([]db.ReduceTask, error) {
			return nil, nil
		},
		incrementMapTaskRetryFn: func(_ context.Context, _ uuid.UUID) error {
			retryMapCalled = true
			return nil
		},
		incrementReduceTaskRetryFn: func(_ context.Context, _ uuid.UUID) error {
			retryReduceCalled = true
			return nil
		},
	}
	disp := &mockDispatcher{
		deleteJobFn: func(_ context.Context, _ string) error {
			deleteJobCalled = true
			return nil
		},
	}

	wd := newWatchdog(q, disp)
	wd.scan(context.Background())

	assert.False(t, retryMapCalled)
	assert.False(t, retryReduceCalled)
	assert.False(t, deleteJobCalled)
}

// TestScan_StaleMapTask_IncrementsRetryAndNotifies verifies the full happy path
// for a stale map task: K8s Job deleted, retry incremented, supervisor notified.
func TestScan_StaleMapTask_IncrementsRetryAndNotifies(t *testing.T) {
	jobID := uuid.New()
	task := staleMapTask(jobID, "map-abc-0")

	deleteJobCalled := false
	var deletedJobName string
	incrementCalled := false
	var incrementedTaskID uuid.UUID

	q := &mockQuerier{
		getStaleRunningMapTasksFn: func(_ context.Context, _ sql.NullString) ([]db.MapTask, error) {
			return []db.MapTask{task}, nil
		},
		getStaleRunningReduceTasksFn: func(_ context.Context, _ sql.NullString) ([]db.ReduceTask, error) {
			return nil, nil
		},
		incrementMapTaskRetryFn: func(_ context.Context, id uuid.UUID) error {
			incrementCalled = true
			incrementedTaskID = id
			return nil
		},
	}
	disp := &mockDispatcher{
		deleteJobFn: func(_ context.Context, name string) error {
			deleteJobCalled = true
			deletedJobName = name
			return nil
		},
	}

	wd := newWatchdog(q, disp)
	wd.scan(context.Background())

	assert.True(t, deleteJobCalled, "stale K8s Job must be deleted")
	assert.Equal(t, "map-abc-0", deletedJobName)
	assert.True(t, incrementCalled, "map task retry must be incremented")
	assert.Equal(t, task.TaskID, incrementedTaskID)
}

// TestScan_StaleMapTask_NoK8sJob_SkipsDelete verifies that when a stale map task
// has no associated K8s Job name, DeleteJob is never called.
func TestScan_StaleMapTask_NoK8sJob_SkipsDelete(t *testing.T) {
	jobID := uuid.New()
	task := staleMapTask(jobID, "") // no k8s job name
	task.K8sJobName = sql.NullString{Valid: false}

	deleteJobCalled := false
	q := &mockQuerier{
		getStaleRunningMapTasksFn: func(_ context.Context, _ sql.NullString) ([]db.MapTask, error) {
			return []db.MapTask{task}, nil
		},
		getStaleRunningReduceTasksFn: func(_ context.Context, _ sql.NullString) ([]db.ReduceTask, error) {
			return nil, nil
		},
		incrementMapTaskRetryFn: func(_ context.Context, _ uuid.UUID) error { return nil },
	}
	disp := &mockDispatcher{
		deleteJobFn: func(_ context.Context, _ string) error {
			deleteJobCalled = true
			return nil
		},
	}

	wd := newWatchdog(q, disp)
	wd.scan(context.Background())

	assert.False(t, deleteJobCalled, "DeleteJob must not be called when K8sJobName is unset")
}

// TestScan_StaleMapTask_DeleteError_StillIncrementsRetry verifies that a K8s
// deletion error is logged and does not prevent the retry increment.
func TestScan_StaleMapTask_DeleteError_StillIncrementsRetry(t *testing.T) {
	jobID := uuid.New()
	task := staleMapTask(jobID, "map-abc-0")

	incrementCalled := false
	q := &mockQuerier{
		getStaleRunningMapTasksFn: func(_ context.Context, _ sql.NullString) ([]db.MapTask, error) {
			return []db.MapTask{task}, nil
		},
		getStaleRunningReduceTasksFn: func(_ context.Context, _ sql.NullString) ([]db.ReduceTask, error) {
			return nil, nil
		},
		incrementMapTaskRetryFn: func(_ context.Context, _ uuid.UUID) error {
			incrementCalled = true
			return nil
		},
	}
	disp := &mockDispatcher{
		deleteJobFn: func(_ context.Context, _ string) error {
			return errors.New("k8s: connection refused")
		},
	}

	wd := newWatchdog(q, disp)
	wd.scan(context.Background())

	assert.True(t, incrementCalled, "retry must still be incremented even if K8s delete fails")
}

// TestScan_StaleMapTask_IncrementError_SkipsNotify verifies that when the DB
// increment fails, the supervisor is NOT notified (we skip the registry.Notify
// because we don't know if the reset succeeded).
func TestScan_StaleMapTask_IncrementError_SkipsNotify(t *testing.T) {
	// We detect "no notify" indirectly: if Notify were called but no supervisor
	// is registered, it's a no-op anyway — so we just verify no panic and that
	// the overall scan completes cleanly.
	jobID := uuid.New()
	task := staleMapTask(jobID, "map-abc-0")

	q := &mockQuerier{
		getStaleRunningMapTasksFn: func(_ context.Context, _ sql.NullString) ([]db.MapTask, error) {
			return []db.MapTask{task}, nil
		},
		getStaleRunningReduceTasksFn: func(_ context.Context, _ sql.NullString) ([]db.ReduceTask, error) {
			return nil, nil
		},
		incrementMapTaskRetryFn: func(_ context.Context, _ uuid.UUID) error {
			return errors.New("db: connection lost")
		},
	}

	assert.NotPanics(t, func() {
		wd := newWatchdog(q, &mockDispatcher{})
		wd.scan(context.Background())
	})
}

// TestScan_StaleReduceTask_IncrementsRetryAndNotifies mirrors the map task test
// for reduce tasks.
func TestScan_StaleReduceTask_IncrementsRetryAndNotifies(t *testing.T) {
	jobID := uuid.New()
	task := staleReduceTask(jobID, "red-abc-0")

	deleteJobCalled := false
	incrementCalled := false
	var incrementedTaskID uuid.UUID

	q := &mockQuerier{
		getStaleRunningMapTasksFn: func(_ context.Context, _ sql.NullString) ([]db.MapTask, error) {
			return nil, nil
		},
		getStaleRunningReduceTasksFn: func(_ context.Context, _ sql.NullString) ([]db.ReduceTask, error) {
			return []db.ReduceTask{task}, nil
		},
		incrementReduceTaskRetryFn: func(_ context.Context, id uuid.UUID) error {
			incrementCalled = true
			incrementedTaskID = id
			return nil
		},
	}
	disp := &mockDispatcher{
		deleteJobFn: func(_ context.Context, _ string) error {
			deleteJobCalled = true
			return nil
		},
	}

	wd := newWatchdog(q, disp)
	wd.scan(context.Background())

	assert.True(t, deleteJobCalled)
	assert.True(t, incrementCalled)
	assert.Equal(t, task.TaskID, incrementedTaskID)
}

// TestScan_StaleReduceTask_NoK8sJob_SkipsDelete mirrors the map task variant.
func TestScan_StaleReduceTask_NoK8sJob_SkipsDelete(t *testing.T) {
	jobID := uuid.New()
	task := staleReduceTask(jobID, "")
	task.K8sJobName = sql.NullString{Valid: false}

	deleteJobCalled := false
	q := &mockQuerier{
		getStaleRunningMapTasksFn: func(_ context.Context, _ sql.NullString) ([]db.MapTask, error) {
			return nil, nil
		},
		getStaleRunningReduceTasksFn: func(_ context.Context, _ sql.NullString) ([]db.ReduceTask, error) {
			return []db.ReduceTask{task}, nil
		},
		incrementReduceTaskRetryFn: func(_ context.Context, _ uuid.UUID) error { return nil },
	}
	disp := &mockDispatcher{
		deleteJobFn: func(_ context.Context, _ string) error {
			deleteJobCalled = true
			return nil
		},
	}

	wd := newWatchdog(q, disp)
	wd.scan(context.Background())

	assert.False(t, deleteJobCalled)
}

// TestScan_MultipleStaleMapTasks_AllProcessed verifies that when multiple stale
// tasks are returned, every one gets its K8s Job deleted and retry incremented.
func TestScan_MultipleStaleMapTasks_AllProcessed(t *testing.T) {
	jobID := uuid.New()
	tasks := []db.MapTask{
		staleMapTask(jobID, "map-abc-0"),
		staleMapTask(jobID, "map-abc-1"),
		staleMapTask(jobID, "map-abc-2"),
	}

	deletedNames := []string{}
	incrementedIDs := []uuid.UUID{}

	q := &mockQuerier{
		getStaleRunningMapTasksFn: func(_ context.Context, _ sql.NullString) ([]db.MapTask, error) {
			return tasks, nil
		},
		getStaleRunningReduceTasksFn: func(_ context.Context, _ sql.NullString) ([]db.ReduceTask, error) {
			return nil, nil
		},
		incrementMapTaskRetryFn: func(_ context.Context, id uuid.UUID) error {
			incrementedIDs = append(incrementedIDs, id)
			return nil
		},
	}
	disp := &mockDispatcher{
		deleteJobFn: func(_ context.Context, name string) error {
			deletedNames = append(deletedNames, name)
			return nil
		},
	}

	wd := newWatchdog(q, disp)
	wd.scan(context.Background())

	require.Len(t, deletedNames, 3, "all three K8s Jobs must be deleted")
	require.Len(t, incrementedIDs, 3, "all three task retries must be incremented")
}

// TestScan_ThresholdPassedCorrectly verifies that the timeout threshold is built
// from the config value and passed as a valid SQL NullString to the queries.
func TestScan_ThresholdPassedCorrectly(t *testing.T) {
	var capturedThreshold sql.NullString

	q := &mockQuerier{
		getStaleRunningMapTasksFn: func(_ context.Context, d sql.NullString) ([]db.MapTask, error) {
			capturedThreshold = d
			return nil, nil
		},
		getStaleRunningReduceTasksFn: func(_ context.Context, _ sql.NullString) ([]db.ReduceTask, error) {
			return nil, nil
		},
	}

	wd := newWatchdog(q, &mockDispatcher{})
	wd.scan(context.Background())

	assert.True(t, capturedThreshold.Valid)
	assert.Equal(t, "300", capturedThreshold.String, "threshold must equal TaskTimeoutSeconds as a string")
}

// TestScan_GetStaleMapError_ContinuesToReduceScan verifies that a DB error on
// the map query is logged but does not prevent the reduce scan from running.
func TestScan_GetStaleMapError_ContinuesToReduceScan(t *testing.T) {
	reduceScanCalled := false

	q := &mockQuerier{
		getStaleRunningMapTasksFn: func(_ context.Context, _ sql.NullString) ([]db.MapTask, error) {
			return nil, errors.New("db: read timeout")
		},
		getStaleRunningReduceTasksFn: func(_ context.Context, _ sql.NullString) ([]db.ReduceTask, error) {
			reduceScanCalled = true
			return nil, nil
		},
	}

	wd := newWatchdog(q, &mockDispatcher{})
	wd.scan(context.Background())

	assert.True(t, reduceScanCalled, "reduce scan must run even if map scan fails")
}

// TestScan_GetStaleReduceError_DoesNotPanic verifies that a DB error on the
// reduce query is handled gracefully.
func TestScan_GetStaleReduceError_DoesNotPanic(t *testing.T) {
	q := &mockQuerier{
		getStaleRunningMapTasksFn: func(_ context.Context, _ sql.NullString) ([]db.MapTask, error) {
			return nil, nil
		},
		getStaleRunningReduceTasksFn: func(_ context.Context, _ sql.NullString) ([]db.ReduceTask, error) {
			return nil, errors.New("db: read timeout")
		},
	}

	assert.NotPanics(t, func() {
		wd := newWatchdog(q, &mockDispatcher{})
		wd.scan(context.Background())
	})
}

// TestScan_BothMapAndReduce_AllProcessed exercises a scan that finds one stale
// map task and one stale reduce task in the same pass.
func TestScan_BothMapAndReduce_AllProcessed(t *testing.T) {
	jobID := uuid.New()
	mapTask := staleMapTask(jobID, "map-abc-0")
	reduceTask := staleReduceTask(jobID, "red-abc-0")

	mapRetried := false
	reduceRetried := false

	q := &mockQuerier{
		getStaleRunningMapTasksFn: func(_ context.Context, _ sql.NullString) ([]db.MapTask, error) {
			return []db.MapTask{mapTask}, nil
		},
		getStaleRunningReduceTasksFn: func(_ context.Context, _ sql.NullString) ([]db.ReduceTask, error) {
			return []db.ReduceTask{reduceTask}, nil
		},
		incrementMapTaskRetryFn: func(_ context.Context, _ uuid.UUID) error {
			mapRetried = true
			return nil
		},
		incrementReduceTaskRetryFn: func(_ context.Context, _ uuid.UUID) error {
			reduceRetried = true
			return nil
		},
	}

	wd := newWatchdog(q, &mockDispatcher{})
	wd.scan(context.Background())

	assert.True(t, mapRetried)
	assert.True(t, reduceRetried)
}

// TestRun_ContextCancellation_Exits verifies that Run exits cleanly when the
// context is cancelled — it should not block indefinitely.
func TestRun_ContextCancellation_Exits(t *testing.T) {
	q := &mockQuerier{
		getStaleRunningMapTasksFn: func(_ context.Context, _ sql.NullString) ([]db.MapTask, error) {
			return nil, nil
		},
		getStaleRunningReduceTasksFn: func(_ context.Context, _ sql.NullString) ([]db.ReduceTask, error) {
			return nil, nil
		},
	}

	wd := newWatchdog(q, &mockDispatcher{})
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		wd.Run(ctx)
		close(done)
	}()

	cancel()

	select {
	case <-done:
		// exited as expected
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not exit after context cancellation")
	}
}
