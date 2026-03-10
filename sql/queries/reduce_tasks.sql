-- name: CreateReduceTask :one
INSERT INTO reduce_tasks (
    job_id,
    task_index
) VALUES (
    $1, $2
)
RETURNING *;


-- name: GetReduceTask :one
SELECT * FROM reduce_tasks
WHERE task_id = $1
LIMIT 1;


-- name: GetReduceTasksByJob :many
SELECT * FROM reduce_tasks
WHERE job_id = $1
ORDER BY task_index;


-- name: GetReduceTasksByJobAndStatus :many
SELECT * FROM reduce_tasks
WHERE job_id = $1
  AND status = $2
ORDER BY task_index;


-- name: GetPendingReduceTasks :many
-- Used by the Manager to find tasks ready to dispatch as K8s Jobs.
SELECT * FROM reduce_tasks
WHERE job_id = $1
  AND status = 'PENDING'
ORDER BY task_index
LIMIT $2;


-- name: MarkReduceTaskRunning :exec
-- Called when the Manager creates the K8s Job for this task.
UPDATE reduce_tasks
SET
    status       = 'RUNNING',
    k8s_job_name = $2,
    started_at   = NOW()
WHERE task_id = $1;


-- name: MarkReduceTaskCompleted :exec
-- Called when the worker POSTs to /tasks/:id/complete.
-- output_path is the MinIO object path of the part file.
UPDATE reduce_tasks
SET
    status       = 'COMPLETED',
    output_path  = $2,
    completed_at = NOW()
WHERE task_id = $1;


-- name: MarkReduceTaskFailed :exec
UPDATE reduce_tasks
SET
    status       = 'FAILED',
    completed_at = NOW()
WHERE task_id = $1;


-- name: IncrementReduceTaskRetry :exec
UPDATE reduce_tasks
SET
    retry_count  = retry_count + 1,
    status       = 'PENDING',
    k8s_job_name = NULL,
    worker_pod   = NULL,
    started_at   = NULL
WHERE task_id = $1;


-- name: CountReduceTasksByStatus :one
-- Used by the Manager to decide when the job is fully COMPLETED.
SELECT
    COUNT(*) FILTER (WHERE status = 'COMPLETED') AS completed,
    COUNT(*) FILTER (WHERE status = 'FAILED')    AS failed,
    COUNT(*)                                      AS total
FROM reduce_tasks
WHERE job_id = $1;


-- name: GetStaleRunningReduceTasks :many
-- Used by the timeout watchdog.
SELECT * FROM reduce_tasks
WHERE status = 'RUNNING'
  AND started_at < NOW() - ($1 || ' seconds')::INTERVAL;


-- name: GetReduceTaskOutputPaths :many
-- Used to build the final job output listing.
SELECT task_index, output_path
FROM reduce_tasks
WHERE job_id = $1
  AND status = 'COMPLETED'
ORDER BY task_index;