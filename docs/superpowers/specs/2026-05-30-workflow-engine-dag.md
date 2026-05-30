# Design Spec: MapReduce Workflow Engine (DAG Orchestrator)

## 1. Overview
The **MapReduce Workflow Engine** allows users to define and execute multi-stage MapReduce pipelines. It replaces the single-job submission model with a Directed Acyclic Graph (DAG) model, where the output of one or more stages can automatically become the input for subsequent stages. This is essential for iterative algorithms like PageRank and multi-phase analysis like Triangle Counting.

## 2. Architecture
- **Language**: Go
- **Components**:
    - **Workflow Controller**: A new internal service in the `Manager` that manages DAG state transitions and orchestration.
    - **Enhanced Supervisor**: Updated to notify the Controller upon job completion.
    - **Enhanced Splitter**: Updated to support multiple input prefixes (merging parent outputs).
- **Storage**: MinIO (used for intermediate data handoff between stages).

## 3. Database Schema
New tables and columns to track orchestration state:

```sql
-- Track overall pipeline state
CREATE TABLE workflows (
    workflow_id   UUID PRIMARY KEY,
    name          TEXT NOT NULL,
    status        TEXT NOT NULL, -- SUBMITTED, RUNNING, COMPLETED, FAILED
    owner_user_id TEXT NOT NULL,
    created_at    TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Associate existing jobs with a workflow
ALTER TABLE jobs ADD COLUMN workflow_id UUID REFERENCES workflows(workflow_id);
ALTER TABLE jobs ADD COLUMN stage_name TEXT;

-- Define the DAG structure
CREATE TABLE workflow_dependencies (
    workflow_id    UUID REFERENCES workflows(workflow_id) ON DELETE CASCADE,
    stage_name     TEXT NOT NULL, -- The child stage
    depends_on     TEXT NOT NULL, -- The parent stage
    PRIMARY KEY (workflow_id, stage_name, depends_on)
);
```

## 4. API Design

### POST /workflows
Submits a new multi-stage pipeline.

**Request Body**:
```json
{
  "name": "triangle-counting-pipeline",
  "stages": {
    "prep": {
      "mapper_path": "code/prep-m.bin",
      "reducer_path": "code/prep-r.bin",
      "input_path": "input/raw-edges/"
    },
    "count": {
      "mapper_path": "code/tri-m.bin",
      "reducer_path": "code/tri-r.bin",
      "depends_on": ["prep"]
    }
  }
}
```

### GET /workflows/:id
Returns the current status of the workflow and the status of all its component stages (jobs).

### POST /workflows/:id/resume
Attempts to restart a `FAILED` workflow from the first failing stage, preserving successful parent results.

## 5. Orchestration Logic
1. **Validation**: The Manager ensures the DAG has no cycles and all referenced code paths exist.
2. **Root Execution**: Identify stages with no `depends_on` entries and launch them as standard `Jobs`.
3. **Event-Driven Transitions**:
    - When a Job finishes, the `Supervisor` calls `WorkflowController.Notify(jobID)`.
    - The Controller identifies downstream stages.
    - If all parents of a stage are `COMPLETED`, the Controller aggregates their `output_paths` and submits the stage as a new `Job`.
4. **Data Handoff**: The `Splitter.Compute` function is updated to list all objects across multiple prefixes when multiple parents are present.

## 6. Resilience & Error Handling
- **Stop-the-World**: If a stage fails (all retries exhausted), the entire workflow is marked `FAILED`. No further stages are launched.
- **Crash Recovery**: On Manager startup, the Controller queries the `workflows` table and reconstructs the DAGs to resume orchestration of in-flight pipelines.
- **Idempotency**: Retrying a completed stage will simply return its existing output path.

## 7. Testing Strategy
- **DAG Logic Tests**: Unit tests for the cycle detection and "ready-to-run" logic using mock DB states.
- **Integration Tests**: End-to-end tests submitting a 2-stage pipeline (e.g., WordCount followed by a Sum-Aggregator) to verify data handoff.
- **Fault Injection**: Kill the Manager pod between stage transitions to verify state recovery.
