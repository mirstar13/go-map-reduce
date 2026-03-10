-- +goose Up
-- Status values: PENDING, RUNNING, COMPLETED, FAILED
CREATE TABLE map_tasks (
    task_id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id           UUID        NOT NULL REFERENCES jobs (job_id) ON DELETE CASCADE,
    task_index       INTEGER     NOT NULL,
    status           VARCHAR(32) NOT NULL DEFAULT 'PENDING',

    -- Input byte-range this task is responsible for
    input_file       VARCHAR(512) NOT NULL,
    input_offset     BIGINT       NOT NULL,
    input_length     BIGINT       NOT NULL,

    -- Kubernetes metadata — set when the K8s Job is created
    k8s_job_name     VARCHAR(63),
    worker_pod       VARCHAR(63),

    retry_count      INTEGER     NOT NULL DEFAULT 0,
    started_at       TIMESTAMPTZ,
    completed_at     TIMESTAMPTZ,

    -- JSON array of {reducer_index, path} objects written to MinIO.
    -- Set by the Manager when the worker calls POST /tasks/:id/complete.
    -- Example: [{"reducer_index":0,"path":"intermediate/jobs/abc/map-0-reduce-0.jsonl"}, ...]
    output_locations JSONB,

    CONSTRAINT uq_map_task_job_index UNIQUE (job_id, task_index)
);

CREATE INDEX idx_map_tasks_job_status ON map_tasks (job_id, status);
CREATE INDEX idx_map_tasks_status     ON map_tasks (status);

-- +goose Down
DROP TABLE IF EXISTS map_tasks;