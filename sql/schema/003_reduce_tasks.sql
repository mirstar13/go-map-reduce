-- +goose Up
-- Status values: PENDING, RUNNING, COMPLETED, FAILED
CREATE TABLE reduce_tasks (
    task_id      UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id       UUID        NOT NULL REFERENCES jobs (job_id) ON DELETE CASCADE,
    task_index   INTEGER     NOT NULL,
    status       VARCHAR(32) NOT NULL DEFAULT 'PENDING',

    -- Kubernetes metadata — set when the K8s Job is created
    k8s_job_name VARCHAR(63),
    worker_pod   VARCHAR(63),

    retry_count  INTEGER     NOT NULL DEFAULT 0,
    started_at   TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,

    -- MinIO path of the final output part file, e.g. output/jobs/abc/part-0.jsonl
    -- Set when the worker calls POST /tasks/:id/complete.
    output_path  VARCHAR(512),

    CONSTRAINT uq_reduce_task_job_index UNIQUE (job_id, task_index)
);

CREATE INDEX idx_reduce_tasks_job_status ON reduce_tasks (job_id, status);
CREATE INDEX idx_reduce_tasks_status     ON reduce_tasks (status);

-- +goose Down
DROP TABLE IF EXISTS reduce_tasks;