-- +goose Up
CREATE TABLE workflows (
    workflow_id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name          TEXT NOT NULL,
    status        VARCHAR(32) NOT NULL DEFAULT 'SUBMITTED', -- SUBMITTED, RUNNING, COMPLETED, FAILED
    owner_user_id VARCHAR(255) NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Associate jobs with workflows
ALTER TABLE jobs ADD COLUMN workflow_id UUID REFERENCES workflows(workflow_id);
ALTER TABLE jobs ADD COLUMN stage_name TEXT;

-- Dependencies between stages in a workflow
CREATE TABLE workflow_dependencies (
    workflow_id    UUID REFERENCES workflows(workflow_id) ON DELETE CASCADE,
    stage_name     TEXT NOT NULL, -- The child stage
    depends_on     TEXT NOT NULL, -- The parent stage
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (workflow_id, stage_name, depends_on)
);

CREATE INDEX idx_workflows_owner_user ON workflows (owner_user_id);
CREATE INDEX idx_workflows_status     ON workflows (status);
CREATE INDEX idx_jobs_workflow_id     ON jobs (workflow_id);

-- +goose Down
DROP INDEX IF EXISTS idx_jobs_workflow_id;
DROP TABLE IF EXISTS workflow_dependencies;
ALTER TABLE jobs DROP COLUMN stage_name;
ALTER TABLE jobs DROP COLUMN workflow_id;
DROP TABLE IF EXISTS workflows;
