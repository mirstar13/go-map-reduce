-- name: CreateWorkflow :one
INSERT INTO workflows (workflow_id, name, status, owner_user_id)
VALUES ($1, $2, $3, $4)
RETURNING *;

-- name: AddWorkflowDependency :exec
INSERT INTO workflow_dependencies (workflow_id, stage_name, depends_on)
VALUES ($1, $2, $3);

-- name: GetWorkflow :one
SELECT * FROM workflows WHERE workflow_id = $1;

-- name: GetWorkflowStages :many
SELECT * FROM jobs 
WHERE workflow_id = $1
ORDER BY submitted_at ASC;

-- name: GetDownstreamStages :many
SELECT stage_name FROM workflow_dependencies 
WHERE workflow_id = $1 AND depends_on = $2;

-- name: CheckStageDependencies :one
-- Returns the number of parents that are not yet COMPLETED
SELECT COUNT(*)
FROM workflow_dependencies d
JOIN jobs j ON j.workflow_id = d.workflow_id AND j.stage_name = d.depends_on
WHERE d.workflow_id = $1 AND d.stage_name = $2 AND j.status != 'COMPLETED';

-- name: UpdateWorkflowStatus :one
UPDATE workflows
SET status = $2
WHERE workflow_id = $1
RETURNING *;
