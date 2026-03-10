-- name: CreateJob :one
INSERT INTO jobs (
    owner_user_id,
    owner_replica,
    mapper_path,
    reducer_path,
    input_path,
    output_path,
    num_mappers,
    num_reducers,
    input_format
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9
)
RETURNING *;


-- name: GetJob :one
SELECT * FROM jobs
WHERE job_id = $1
LIMIT 1;


-- name: GetJobsByUser :many
SELECT * FROM jobs
WHERE owner_user_id = $1
ORDER BY submitted_at DESC;


-- name: GetAllJobs :many
SELECT * FROM jobs
ORDER BY submitted_at DESC;


-- name: GetActiveJobsByReplica :many
-- Used by the Manager on startup to re-attach to in-flight jobs it owns.
SELECT * FROM jobs
WHERE owner_replica = $1
  AND status NOT IN ('COMPLETED', 'FAILED', 'CANCELLED');


-- name: UpdateJobStatus :exec
UPDATE jobs
SET
    status       = $2,
    started_at   = CASE
                     WHEN $2 = 'MAP_PHASE' AND started_at IS NULL
                     THEN NOW()
                     ELSE started_at
                   END,
    completed_at = CASE
                     WHEN $2 IN ('COMPLETED', 'FAILED', 'CANCELLED')
                     THEN NOW()
                     ELSE completed_at
                   END
WHERE job_id = $1;


-- name: FailJob :exec
UPDATE jobs
SET
    status        = 'FAILED',
    completed_at  = NOW(),
    error_message = $2
WHERE job_id = $1;


-- name: CancelJob :exec
UPDATE jobs
SET
    status       = 'CANCELLED',
    completed_at = NOW()
WHERE job_id = $1
  AND status NOT IN ('COMPLETED', 'FAILED', 'CANCELLED');


-- name: CountJobsByStatus :many
-- Useful for an admin dashboard or metrics endpoint.
SELECT status, COUNT(*) AS count
FROM jobs
GROUP BY status;