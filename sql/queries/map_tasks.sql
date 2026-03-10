-- name: CreateMapTask :one
INSERT INTO map_tasks (
    job_id,
    task_index,
    input_file,
    input_offset,
    input_length
) VALUES (
    $1, $2, $3, $4, $5
)
RETURNING *;


-- name: GetMapTask :one
SELECT * FROM map_tasks
WHERE task_id = $1
LIMIT 1;


-- name: GetMapTasksByJob :many
SELECT * FROM map_tasks
WHERE job_id = $1
ORDER BY task_index;


-- name: GetMapTasksByJobAndStatus :many
SELECT * FROM map_tasks
WHERE job_id = $1
  AND status = $2
ORDER BY task_index;


-- name: GetPendingMapTasks :many
-- Used by the Manager to find tasks ready to dispatch as K8s Jobs.
SELECT * FROM map_tasks
WHERE job_id = $1
  AND status = 'PENDING'
ORDER BY task_index
LIMIT $2;


-- name: MarkMapTaskRunning :exec
-- Called when the Manager creates the K8s Job for this task.
UPDATE map_tasks
SET
    status       = 'RUNNING',
    k8s_job_name = $2,
    started_at   = NOW()
WHERE task_id = $1;


-- name: MarkMapTaskCompleted :exec
-- Called when the worker POSTs to /tasks/:id/complete.
-- output_locations is a JSONB array: [{"reducer_index":0,"path":"..."}]
UPDATE map_tasks
SET
    status           = 'COMPLETED',
    output_locations = $2,
    completed_at     = NOW()
WHERE task_id = $1;


-- name: MarkMapTaskFailed :exec
UPDATE map_tasks
SET
    status      = 'FAILED',
    completed_at = NOW()
WHERE task_id = $1;


-- name: IncrementMapTaskRetry :exec
UPDATE map_tasks
SET
    retry_count  = retry_count + 1,
    status       = 'PENDING',
    k8s_job_name = NULL,
    worker_pod   = NULL,
    started_at   = NULL
WHERE task_id = $1;


-- name: CountMapTasksByStatus :one
-- Atomically read how many tasks are COMPLETED vs total.
-- Used by the Manager to decide when to advance to REDUCE_PHASE.
SELECT
    COUNT(*) FILTER (WHERE status = 'COMPLETED') AS completed,
    COUNT(*) FILTER (WHERE status = 'FAILED')    AS failed,
    COUNT(*)                                      AS total
FROM map_tasks
WHERE job_id = $1;


-- name: GetStaleRunningMapTasks :many
-- Used by the timeout watchdog: find RUNNING tasks older than N seconds.
SELECT * FROM map_tasks
WHERE status = 'RUNNING'
  AND started_at < NOW() - ($1 || ' seconds')::INTERVAL;


-- name: GetMapTaskOutputLocations :many
-- Used by the Manager when building the reduce task inputs.
-- Returns all completed map tasks and their output_locations for a job.
SELECT task_index, output_locations
FROM map_tasks
WHERE job_id = $1
  AND status = 'COMPLETED'
ORDER BY task_index;