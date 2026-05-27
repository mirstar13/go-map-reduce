package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/joho/godotenv"
	_ "github.com/jackc/pgx/v5/stdlib" // pgx driver for database/sql
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/mirstar13/go-map-reduce/db"
	"github.com/sqlc-dev/pqtype"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	_ = godotenv.Overload("test.env")
	os.Exit(m.Run())
}

func requireIntegrationEnabled(t *testing.T) {
	t.Helper()
	if os.Getenv("INTEGRATION_TESTS") != "true" {
		t.Skip("skipping integration tests; set INTEGRATION_TESTS=true to run")
	}
}

func requireEnv(t *testing.T, key string) string {
	t.Helper()
	val := os.Getenv(key)
	if val == "" {
		t.Skipf("%s not set", key)
	}
	return val
}

func newIntegrationQuerier(t *testing.T) (*sql.DB, *db.Queries) {
	t.Helper()
	requireIntegrationEnabled(t)

	sqlDB, err := sql.Open("pgx", requireEnv(t, "POSTGRES_DSN"))
	require.NoError(t, err, "failed to open PostgreSQL connection")
	t.Cleanup(func() {
		_ = sqlDB.Close()
	})

	err = sqlDB.PingContext(context.Background())
	require.NoError(t, err, "failed to ping PostgreSQL")

	return sqlDB, db.New(sqlDB)
}

func newIntegrationMinio(t *testing.T) *minio.Client {
	t.Helper()
	requireIntegrationEnabled(t)
	endpoint := requireEnv(t, "MINIO_ENDPOINT")
	accessKey := requireEnv(t, "MINIO_ACCESS_KEY")
	secretKey := requireEnv(t, "MINIO_SECRET_KEY")

	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: false,
	})
	require.NoError(t, err, "failed to create MinIO client")
	return client
}

func registerJobCleanup(t *testing.T, sqlDB *sql.DB, jobID uuid.UUID) {
	t.Helper()
	t.Cleanup(func() {
		_, err := sqlDB.ExecContext(context.Background(), "DELETE FROM jobs WHERE job_id = $1", jobID)
		require.NoError(t, err, "cleanup: delete job")
	})
}

func createTestJob(t *testing.T, querier *db.Queries, ownerReplica string) db.Job {
	t.Helper()
	suffix := uuid.NewString()
	job, err := querier.CreateJob(context.Background(), db.CreateJobParams{
		OwnerUserID:  "integration-user-" + suffix,
		OwnerReplica: ownerReplica,
		MapperPath:   "code/" + suffix + "-mapper.py",
		ReducerPath:  "code/" + suffix + "-reducer.py",
		InputPath:    "input/" + suffix + ".jsonl",
		OutputPath:   "output/jobs/" + suffix,
		NumMappers:   2,
		NumReducers:  2,
		InputFormat:  "jsonl",
	})
	require.NoError(t, err, "failed to create test job")
	return job
}

func ensureBucket(t *testing.T, mc *minio.Client, bucket string) {
	t.Helper()
	ctx := context.Background()
	exists, err := mc.BucketExists(ctx, bucket)
	require.NoError(t, err, "failed to check bucket existence")
	if !exists {
		err = mc.MakeBucket(ctx, bucket, minio.MakeBucketOptions{})
		require.NoError(t, err, "failed to create bucket")
	}
}

func cleanupBucket(t *testing.T, mc *minio.Client, bucket string) {
	t.Helper()
	ctx := context.Background()

	for obj := range mc.ListObjects(ctx, bucket, minio.ListObjectsOptions{Recursive: true}) {
		require.NoError(t, obj.Err, "list object error during cleanup")
		err := mc.RemoveObject(ctx, bucket, obj.Key, minio.RemoveObjectOptions{})
		require.NoError(t, err, "remove object during cleanup")
	}

	err := mc.RemoveBucket(ctx, bucket)
	require.NoError(t, err, "remove bucket during cleanup")
}

// TestJobStorageIntegration tests the ability to store and retrieve a job using PostgreSQL and MinIO.
// This test requires a running PostgreSQL and MinIO instance.
// Set the following environment variables to configure the test:
//   POSTGRES_DSN: PostgreSQL connection string (e.g., "postgres://user:pass@localhost:5432/db")
//   MINIO_ENDPOINT: MinIO endpoint (e.g., "localhost:9000")
//   MINIO_ACCESS_KEY: MinIO access key
//   MINIO_SECRET_KEY: MinIO secret key
//   INTEGRATION_TESTS: set to "true" to enable this test.
func TestJobStorageIntegration(t *testing.T) {
	ctx := context.Background()
	sqlDB, querier := newIntegrationQuerier(t)
	minioClient := newIntegrationMinio(t)

	testBucket := "mapreduce-it-" + uuid.NewString()[:8]
	ensureBucket(t, minioClient, testBucket)
	t.Cleanup(func() {
		cleanupBucket(t, minioClient, testBucket)
	})

	inputObject := "input/test-input.jsonl"
	inputData := []byte("{\"value\":\"a\"}\n{\"value\":\"b\"}\n")
	_, err := minioClient.PutObject(
		ctx,
		testBucket,
		inputObject,
		bytes.NewReader(inputData),
		int64(len(inputData)),
		minio.PutObjectOptions{ContentType: "application/json"},
	)
	require.NoError(t, err, "failed to upload integration input object")

	insertedJob, err := querier.CreateJob(ctx, db.CreateJobParams{
		OwnerUserID:  "test-user",
		OwnerReplica: "test-replica-0",
		MapperPath:   testBucket + "/mapper.py",
		ReducerPath:  testBucket + "/reducer.py",
		InputPath:    testBucket + "/" + inputObject,
		OutputPath:   testBucket + "/output/",
		NumMappers:   2,
		NumReducers:  1,
		InputFormat:  "jsonl",
	})
	require.NoError(t, err, "failed to insert job")
	registerJobCleanup(t, sqlDB, insertedJob.JobID)

	require.NotEqual(t, uuid.UUID{}, insertedJob.JobID)
	require.Equal(t, "test-user", insertedJob.OwnerUserID)
	require.Equal(t, "test-replica-0", insertedJob.OwnerReplica)
	require.Equal(t, "SUBMITTED", insertedJob.Status)
	require.Equal(t, "jsonl", insertedJob.InputFormat)
	require.False(t, insertedJob.StartedAt.Valid)
	require.False(t, insertedJob.CompletedAt.Valid)
	require.False(t, insertedJob.ErrorMessage.Valid)

	retrievedJob, err := querier.GetJob(ctx, insertedJob.JobID)
	require.NoError(t, err, "failed to retrieve job")
	require.Equal(t, insertedJob, retrievedJob)

	_, err = minioClient.StatObject(
		ctx,
		testBucket,
		inputObject,
		minio.StatObjectOptions{},
	)
	require.NoError(t, err, "uploaded input object should exist in MinIO")
}

func TestJobStatusLifecycleIntegration(t *testing.T) {
	ctx := context.Background()
	sqlDB, querier := newIntegrationQuerier(t)

	replica := "manager-it-replica"
	activeJob := createTestJob(t, querier, replica)
	registerJobCleanup(t, sqlDB, activeJob.JobID)

	completedJob := createTestJob(t, querier, replica)
	registerJobCleanup(t, sqlDB, completedJob.JobID)
	err := querier.UpdateJobStatus(ctx, db.UpdateJobStatusParams{
		JobID:  completedJob.JobID,
		Status: "COMPLETED",
	})
	require.NoError(t, err, "failed to mark completed job")

	cancelledJob := createTestJob(t, querier, replica)
	registerJobCleanup(t, sqlDB, cancelledJob.JobID)
	err = querier.CancelJob(ctx, cancelledJob.JobID)
	require.NoError(t, err, "failed to cancel job")

	active, err := querier.GetActiveJobsByReplica(ctx, replica)
	require.NoError(t, err, "failed to load active jobs by replica")
	require.Len(t, active, 1)
	require.Equal(t, activeJob.JobID, active[0].JobID)

	err = querier.UpdateJobStatus(ctx, db.UpdateJobStatusParams{
		JobID:  activeJob.JobID,
		Status: "MAP_PHASE",
	})
	require.NoError(t, err, "failed to transition to MAP_PHASE")

	afterMapPhase, err := querier.GetJob(ctx, activeJob.JobID)
	require.NoError(t, err, "failed to get job after MAP_PHASE")
	require.Equal(t, "MAP_PHASE", afterMapPhase.Status)
	require.True(t, afterMapPhase.StartedAt.Valid)
	startedAt := afterMapPhase.StartedAt.Time

	err = querier.UpdateJobStatus(ctx, db.UpdateJobStatusParams{
		JobID:  activeJob.JobID,
		Status: "REDUCE_PHASE",
	})
	require.NoError(t, err, "failed to transition to REDUCE_PHASE")

	afterReducePhase, err := querier.GetJob(ctx, activeJob.JobID)
	require.NoError(t, err, "failed to get job after REDUCE_PHASE")
	require.Equal(t, "REDUCE_PHASE", afterReducePhase.Status)
	require.True(t, afterReducePhase.StartedAt.Valid)
	require.True(t, afterReducePhase.StartedAt.Time.Equal(startedAt))

	err = querier.CancelJob(ctx, activeJob.JobID)
	require.NoError(t, err, "failed to cancel active job")

	cancelled, err := querier.GetJob(ctx, activeJob.JobID)
	require.NoError(t, err, "failed to get cancelled job")
	require.Equal(t, "CANCELLED", cancelled.Status)
	require.True(t, cancelled.CompletedAt.Valid)

	firstCompletedAt := cancelled.CompletedAt.Time
	err = querier.CancelJob(ctx, activeJob.JobID)
	require.NoError(t, err, "second cancel should be a no-op")

	cancelledAgain, err := querier.GetJob(ctx, activeJob.JobID)
	require.NoError(t, err, "failed to get cancelled job after second cancel")
	require.Equal(t, "CANCELLED", cancelledAgain.Status)
	require.True(t, cancelledAgain.CompletedAt.Valid)
	require.True(t, cancelledAgain.CompletedAt.Time.Equal(firstCompletedAt))
}

func TestMapReduceTaskLifecycleIntegration(t *testing.T) {
	ctx := context.Background()
	sqlDB, querier := newIntegrationQuerier(t)

	job := createTestJob(t, querier, "manager-it-task-lifecycle")
	registerJobCleanup(t, sqlDB, job.JobID)

	mapTask0, err := querier.CreateMapTask(ctx, db.CreateMapTaskParams{
		JobID:       job.JobID,
		TaskIndex:   0,
		InputFile:   job.InputPath,
		InputOffset: 0,
		InputLength: 64,
	})
	require.NoError(t, err, "failed to create first map task")

	mapTask1, err := querier.CreateMapTask(ctx, db.CreateMapTaskParams{
		JobID:       job.JobID,
		TaskIndex:   1,
		InputFile:   job.InputPath,
		InputOffset: 64,
		InputLength: 64,
	})
	require.NoError(t, err, "failed to create second map task")

	mapCounts, err := querier.CountMapTasksByStatus(ctx, job.JobID)
	require.NoError(t, err, "failed to count initial map tasks")
	require.Equal(t, int64(2), mapCounts.Total)
	require.Equal(t, int64(0), mapCounts.Completed)
	require.Equal(t, int64(0), mapCounts.Failed)

	err = querier.MarkMapTaskRunning(ctx, db.MarkMapTaskRunningParams{
		TaskID:     mapTask0.TaskID,
		K8sJobName: sql.NullString{String: "map-it-0", Valid: true},
	})
	require.NoError(t, err, "failed to mark map task running")

	runningMapTask, err := querier.GetMapTask(ctx, mapTask0.TaskID)
	require.NoError(t, err, "failed to load running map task")
	require.Equal(t, "RUNNING", runningMapTask.Status)
	require.True(t, runningMapTask.StartedAt.Valid)
	require.True(t, runningMapTask.K8sJobName.Valid)

	err = querier.IncrementMapTaskRetry(ctx, mapTask0.TaskID)
	require.NoError(t, err, "failed to increment map task retry")

	pendingAgainMapTask, err := querier.GetMapTask(ctx, mapTask0.TaskID)
	require.NoError(t, err, "failed to load retried map task")
	require.Equal(t, "PENDING", pendingAgainMapTask.Status)
	require.Equal(t, int32(1), pendingAgainMapTask.RetryCount)
	require.False(t, pendingAgainMapTask.StartedAt.Valid)
	require.False(t, pendingAgainMapTask.K8sJobName.Valid)

	outputLocations, err := json.Marshal([]map[string]any{
		{"reducer_index": 0, "path": fmt.Sprintf("%s/map-0-reduce-0.txt", job.JobID)},
		{"reducer_index": 1, "path": fmt.Sprintf("%s/map-0-reduce-1.txt", job.JobID)},
	})
	require.NoError(t, err, "failed to marshal map output locations")

	err = querier.MarkMapTaskCompleted(ctx, db.MarkMapTaskCompletedParams{
		TaskID: mapTask0.TaskID,
		OutputLocations: pqtype.NullRawMessage{
			RawMessage: outputLocations,
			Valid:      true,
		},
	})
	require.NoError(t, err, "failed to mark map task completed")

	err = querier.MarkMapTaskFailed(ctx, mapTask1.TaskID)
	require.NoError(t, err, "failed to mark map task failed")

	mapCounts, err = querier.CountMapTasksByStatus(ctx, job.JobID)
	require.NoError(t, err, "failed to count final map tasks")
	require.Equal(t, int64(2), mapCounts.Total)
	require.Equal(t, int64(1), mapCounts.Completed)
	require.Equal(t, int64(1), mapCounts.Failed)

	mapOutputs, err := querier.GetMapTaskOutputLocations(ctx, job.JobID)
	require.NoError(t, err, "failed to get map output locations")
	require.Len(t, mapOutputs, 1)
	require.Equal(t, int32(0), mapOutputs[0].TaskIndex)
	require.True(t, mapOutputs[0].OutputLocations.Valid)
	require.JSONEq(t, string(outputLocations), string(mapOutputs[0].OutputLocations.RawMessage))

	reduceTask0, err := querier.CreateReduceTask(ctx, db.CreateReduceTaskParams{
		JobID:     job.JobID,
		TaskIndex: 0,
	})
	require.NoError(t, err, "failed to create first reduce task")

	reduceTask1, err := querier.CreateReduceTask(ctx, db.CreateReduceTaskParams{
		JobID:     job.JobID,
		TaskIndex: 1,
	})
	require.NoError(t, err, "failed to create second reduce task")

	err = querier.MarkReduceTaskRunning(ctx, db.MarkReduceTaskRunningParams{
		TaskID:     reduceTask0.TaskID,
		K8sJobName: sql.NullString{String: "reduce-it-0", Valid: true},
	})
	require.NoError(t, err, "failed to mark reduce task running")

	err = querier.IncrementReduceTaskRetry(ctx, reduceTask0.TaskID)
	require.NoError(t, err, "failed to increment reduce task retry")

	reducePendingAgain, err := querier.GetReduceTask(ctx, reduceTask0.TaskID)
	require.NoError(t, err, "failed to load retried reduce task")
	require.Equal(t, "PENDING", reducePendingAgain.Status)
	require.Equal(t, int32(1), reducePendingAgain.RetryCount)
	require.False(t, reducePendingAgain.StartedAt.Valid)
	require.False(t, reducePendingAgain.K8sJobName.Valid)

	finalOutput := fmt.Sprintf("output/jobs/%s/part-0.txt", job.JobID)
	err = querier.MarkReduceTaskCompleted(ctx, db.MarkReduceTaskCompletedParams{
		TaskID:     reduceTask0.TaskID,
		OutputPath: sql.NullString{String: finalOutput, Valid: true},
	})
	require.NoError(t, err, "failed to mark reduce task completed")

	err = querier.MarkReduceTaskFailed(ctx, reduceTask1.TaskID)
	require.NoError(t, err, "failed to mark reduce task failed")

	reduceCounts, err := querier.CountReduceTasksByStatus(ctx, job.JobID)
	require.NoError(t, err, "failed to count reduce tasks")
	require.Equal(t, int64(2), reduceCounts.Total)
	require.Equal(t, int64(1), reduceCounts.Completed)
	require.Equal(t, int64(1), reduceCounts.Failed)

	outputPaths, err := querier.GetReduceTaskOutputPaths(ctx, job.JobID)
	require.NoError(t, err, "failed to fetch reduce output paths")
	require.Len(t, outputPaths, 1)
	require.Equal(t, int32(0), outputPaths[0].TaskIndex)
	require.True(t, outputPaths[0].OutputPath.Valid)
	require.Equal(t, finalOutput, outputPaths[0].OutputPath.String)
}

func TestFullJobCompletionIntegration(t *testing.T) {
	ctx := context.Background()
	sqlDB, querier := newIntegrationQuerier(t)

	replica := "manager-it-full-completion"
	job := createTestJob(t, querier, replica)
	registerJobCleanup(t, sqlDB, job.JobID)

	err := querier.UpdateJobStatus(ctx, db.UpdateJobStatusParams{
		JobID:  job.JobID,
		Status: "MAP_PHASE",
	})
	require.NoError(t, err, "transition to MAP_PHASE")

	var mapTaskIDs []uuid.UUID
	for i := 0; i < int(job.NumMappers); i++ {
		task, err := querier.CreateMapTask(ctx, db.CreateMapTaskParams{
			JobID:       job.JobID,
			TaskIndex:   int32(i),
			InputFile:   job.InputPath,
			InputOffset: int64(i * 64),
			InputLength: 64,
		})
		require.NoError(t, err, "create map task")
		mapTaskIDs = append(mapTaskIDs, task.TaskID)
	}

	for i, taskID := range mapTaskIDs {
		outputLocations, _ := json.Marshal([]map[string]any{
			{"reducer_index": 0, "path": fmt.Sprintf("%s/map-%d-reduce-0.txt", job.JobID, i)},
			{"reducer_index": 1, "path": fmt.Sprintf("%s/map-%d-reduce-1.txt", job.JobID, i)},
		})
		err := querier.MarkMapTaskCompleted(ctx, db.MarkMapTaskCompletedParams{
			TaskID: taskID,
			OutputLocations: pqtype.NullRawMessage{
				RawMessage: outputLocations,
				Valid:      true,
			},
		})
		require.NoError(t, err, "mark map task completed")
	}

	mapCounts, err := querier.CountMapTasksByStatus(ctx, job.JobID)
	require.NoError(t, err)
	require.Equal(t, int64(job.NumMappers), mapCounts.Total)
	require.Equal(t, int64(job.NumMappers), mapCounts.Completed)

	err = querier.UpdateJobStatus(ctx, db.UpdateJobStatusParams{
		JobID:  job.JobID,
		Status: "REDUCE_PHASE",
	})
	require.NoError(t, err, "transition to REDUCE_PHASE")

	var reduceTaskIDs []uuid.UUID
	for i := 0; i < int(job.NumReducers); i++ {
		task, err := querier.CreateReduceTask(ctx, db.CreateReduceTaskParams{
			JobID:     job.JobID,
			TaskIndex: int32(i),
		})
		require.NoError(t, err, "create reduce task")
		reduceTaskIDs = append(reduceTaskIDs, task.TaskID)
	}

	for i, taskID := range reduceTaskIDs {
		err := querier.MarkReduceTaskCompleted(ctx, db.MarkReduceTaskCompletedParams{
			TaskID: taskID,
			OutputPath: sql.NullString{
				String: fmt.Sprintf("output/jobs/%s/part-%d.txt", job.JobID, i),
				Valid:  true,
			},
		})
		require.NoError(t, err, "mark reduce task completed")
	}

	reduceCounts, err := querier.CountReduceTasksByStatus(ctx, job.JobID)
	require.NoError(t, err)
	require.Equal(t, int64(job.NumReducers), reduceCounts.Total)
	require.Equal(t, int64(job.NumReducers), reduceCounts.Completed)

	err = querier.UpdateJobStatus(ctx, db.UpdateJobStatusParams{
		JobID:  job.JobID,
		Status: "COMPLETED",
	})
	require.NoError(t, err, "transition to COMPLETED")

	completedJob, err := querier.GetJob(ctx, job.JobID)
	require.NoError(t, err)
	require.Equal(t, "COMPLETED", completedJob.Status)
	require.True(t, completedJob.CompletedAt.Valid)

	outputPaths, err := querier.GetReduceTaskOutputPaths(ctx, job.JobID)
	require.NoError(t, err)
	require.Len(t, outputPaths, int(job.NumReducers))
}

func TestJobFailureIntegration(t *testing.T) {
	ctx := context.Background()
	sqlDB, querier := newIntegrationQuerier(t)

	replica := "manager-it-failure"
	job := createTestJob(t, querier, replica)
	registerJobCleanup(t, sqlDB, job.JobID)

	err := querier.FailJob(ctx, db.FailJobParams{
		JobID:        job.JobID,
		ErrorMessage: sql.NullString{String: "task failed after 3 retries", Valid: true},
	})
	require.NoError(t, err, "fail job")

	failedJob, err := querier.GetJob(ctx, job.JobID)
	require.NoError(t, err)
	require.Equal(t, "FAILED", failedJob.Status)
	require.True(t, failedJob.ErrorMessage.Valid)
	require.Equal(t, "task failed after 3 retries", failedJob.ErrorMessage.String)
	require.True(t, failedJob.CompletedAt.Valid)
}

func TestTaskMaxRetriesIntegration(t *testing.T) {
	ctx := context.Background()
	sqlDB, querier := newIntegrationQuerier(t)

	job := createTestJob(t, querier, "manager-it-retries")
	registerJobCleanup(t, sqlDB, job.JobID)

	task, err := querier.CreateMapTask(ctx, db.CreateMapTaskParams{
		JobID:       job.JobID,
		TaskIndex:   0,
		InputFile:   job.InputPath,
		InputOffset: 0,
		InputLength: 64,
	})
	require.NoError(t, err, "create map task")

	for i := 0; i < 3; i++ {
		err = querier.MarkMapTaskRunning(ctx, db.MarkMapTaskRunningParams{
			TaskID:     task.TaskID,
			K8sJobName: sql.NullString{String: fmt.Sprintf("map-it-%d", i), Valid: true},
		})
		require.NoError(t, err)

		err = querier.IncrementMapTaskRetry(ctx, task.TaskID)
		require.NoError(t, err)
		
		retriedTask, err := querier.GetMapTask(ctx, task.TaskID)
		require.NoError(t, err)
		require.Equal(t, "PENDING", retriedTask.Status)
		require.Equal(t, int32(i+1), retriedTask.RetryCount)
	}

	err = querier.MarkMapTaskFailed(ctx, task.TaskID)
	require.NoError(t, err)

	failedTask, err := querier.GetMapTask(ctx, task.TaskID)
	require.NoError(t, err)
	require.Equal(t, "FAILED", failedTask.Status)
}

