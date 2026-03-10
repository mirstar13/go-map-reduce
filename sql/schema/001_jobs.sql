-- +goose Up
-- Status values: SUBMITTED, SPLITTING, MAP_PHASE, REDUCE_PHASE, COMPLETED, FAILED, CANCELLED
CREATE TABLE jobs (
    job_id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    owner_user_id   VARCHAR(255) NOT NULL,
    -- Stable pod name of the Manager replica that owns this job,
    -- e.g. "manager-0". Used to re-attach on Manager restart.
    owner_replica   VARCHAR(63)  NOT NULL,
    status          VARCHAR(32)  NOT NULL DEFAULT 'SUBMITTED',

    -- MinIO object paths
    mapper_path     VARCHAR(512) NOT NULL,
    reducer_path    VARCHAR(512) NOT NULL,
    input_path      VARCHAR(512) NOT NULL,
    output_path     VARCHAR(512) NOT NULL,

    num_mappers     INTEGER      NOT NULL,
    num_reducers    INTEGER      NOT NULL,
    input_format    VARCHAR(32)  NOT NULL DEFAULT 'jsonl', -- 'jsonl' | 'text'

    submitted_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    error_message   TEXT
);

CREATE INDEX idx_jobs_owner_user          ON jobs (owner_user_id);
CREATE INDEX idx_jobs_owner_replica_status ON jobs (owner_replica, status);
CREATE INDEX idx_jobs_status              ON jobs (status);

-- +goose Down
DROP TABLE IF EXISTS jobs;
