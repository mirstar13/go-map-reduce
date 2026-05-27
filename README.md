# MapReduce on Kubernetes

A distributed MapReduce implementation built in Go, designed to run on Kubernetes. This project implements a scalable, fault-tolerant parallel computation framework following the architecture described in [Dean & Ghemawat's MapReduce paper](https://www.usenix.org/legacy/publications/library/proceedings/osdi04/tech/full_papers/dean/dean.pdf).

## Documentation

| Document | Description |
|----------|-------------|
| [Local Development](test/README.md) | Setup instructions for local development and testing |
| [Word Count Example](examples/wordcount/README.md) | Classic MapReduce word frequency counter |
| [Inverted Index Example](examples/inverted-index/README.md) | Build word-to-document index |
| [Graph Algorithms](examples/graph/README.md) | Iterative graph algorithms including PageRank, BFS, and Connected Components |

## Features

- **Distributed Execution**: Scale to thousands of parallel workers via Kubernetes Jobs.
- **Fault Tolerance**: Automatic task retry with watchdog-based failure detection.
- **Plugin Caching**: Automatically hashes and caches compiled Go plugin binaries in MinIO to skip redundant builds.
- **Real-time Monitoring**: Interactive CLI `watch` command with live progress bars for all execution phases.
- **Auto-scaling**: Intelligent mapper and reducer count selection based on input dataset size.
- **Authentication**: Full SSO via Keycloak with JWT tokens and RBAC.
- **Flexible Input/Output**: JSON Lines and plain text formats with byte-range splitting.
- **Object Storage**: MinIO-based storage for input, code, intermediate, and output files.
- **CLI & API**: Comprehensive command-line interface and REST API for job management.
- **Go Plugins**: Type-safe mapper/reducer execution via HashiCorp `go-plugin`.

## Architecture

```
┌─────────────┐      ┌─────────────────┐       ┌──────────────────┐
│   CLI       │────▶│   UI Service    |─────▶│  Manager Service │
│  (mapreduce)│      │   (Gateway)     │       │  (Orchestrator)  │
└─────────────┘      └─────────────────┘       └──────────────────┘
                            │                          │
                            ▼                          ▼
                     ┌───────────────┐         ┌──────────────────┐
                     │   Keycloak    │         │  Builder Service │
                     │   (Auth)      │         │  (K8s Build Job) │
                     └───────────────┘         └──────────────────┘
                            │                          │
                            ▼                          ▼
                     ┌───────────────┐         ┌───────────────┐
                     │  PostgreSQL   │◀──────▶│  Worker Pods  │
                     │  (State DB)   │         │  (K8s Jobs)   │
                     └───────────────┘         └───────────────┘
                            │                          │
                            ▼                          ▼
                     ┌───────────────┐         ┌───────────────┐
                     │     MinIO     │◀──────▶│     MinIO     │
                     │ (Code/Cache)  │         │    (Data)     │
                     └───────────────┘         └───────────────┘
```

### Services

| Service | Description | Kubernetes Resource |
|---------|-------------|---------------------|
| **UI Service** | Public API gateway with JWT validation and streaming upload support | Deployment |
| **Manager Service** | Job orchestration, state management, and worker scheduling | StatefulSet |
| **Builder Service** | Compiles Go source code into executable plugins | batch/v1 Job |
| **Workers** | Execute map and reduce tasks using loaded plugins | batch/v1 Jobs |
| **Keycloak** | Identity provider (OpenID Connect / OAuth 2.0) | Deployment |
| **PostgreSQL** | Persistent job, task, and plugin cache storage | StatefulSet |
| **MinIO** | S3-compatible storage for code, binaries, and datasets | StatefulSet |

## Quick Start

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/)
- [Minikube](https://minikube.sigs.k8s.io/docs/start/) or a Kubernetes cluster
- [kubectl](https://kubernetes.io/docs/tasks/tools/)
- [Go 1.25+](https://golang.org/dl/) (for development)

### Makefile Commands

The project includes a `Makefile` to simplify local development and deployment:

```bash
# Start minikube and deploy all services (PostgreSQL, MinIO, Keycloak, etc.)
make minikube-start

# Build Docker images for all services locally
make docker-build

# Build and push Docker images (multi-platform)
make docker-build-push

# Run tests and generate HTML coverage report
make test-coverage
```

### Install CLI

```bash
go install ./cmd/cli
```

## Usage

### Authentication

```bash
# Login (prompts for password)
mapreduce login --server http://localhost:8081 --username alice

# Credentials stored in ~/.mapreduce/config.json
```

### Submit a Job

The CLI supports automatic scaling of workers based on input size.

```bash
# Submit a MapReduce job with auto-scaling
mapreduce jobs submit \
  --input   ./data/large_input.txt \
  --mapper  ./examples/wordcount/mapper.go \
  --reducer ./examples/wordcount/reducer.go \
  --auto \
  --format  text
```

### Monitor Jobs

```bash
# Watch progress in real-time with progress bars
mapreduce jobs watch <job-id>

# List your jobs
mapreduce jobs list

# Get detailed job status
mapreduce jobs get <job-id>

# Cancel a running job
mapreduce jobs cancel <job-id>

# Delete a job and its associated resources
mapreduce jobs delete <job-id>
```

### Benchmarking

Automate multiple iterations of a job to measure performance and cluster stability.

```bash
# Run 5 iterations of wordcount
go run cmd/benchmark-runner/main.go ./input.txt mapper.go reducer.go 5
```

## Graph Algorithms & Examples

The `examples/` directory contains a variety of jobs:

1. **Word Count**: Basic frequency counter.
2. **Inverted Index**: Builds an index mapping words to their document of origin.
3. **Graph Algorithms** (`examples/graph/`):
   - **PageRank**: Iteratively calculates node importance.
   - **BFS**: Shortest path calculation.
   - **Connected Components (CC)**: Cluster detection using Label Propagation.
   - **Triangle Counting**: Two-stage MapReduce job to count triangles in a graph.

These graph algorithms are designed to be run iteratively and benchmarked using large datasets like Google+ circles or Stack Overflow networks.

## Writing Map/Reduce Functions

Functions are written in Go and executed as plugins.

### Mapper

```go
package main

import (
    "strings"
    "github.com/mirstar13/go-map-reduce/pkg/plugin"
)

type MapperImpl struct{}

func (m *MapperImpl) Map(key, value string) ([]plugin.Record, error) {
    var records []plugin.Record
    words := strings.Fields(value)
    for _, word := range words {
        records = append(records, plugin.Record{Key: word, Value: "1"})
    }
    return records, nil
}

var Mapper plugin.Mapper = &MapperImpl{}
```

### Reducer

```go
package main

import (
    "strconv"
    "github.com/mirstar13/go-map-reduce/pkg/plugin"
)

type ReducerImpl struct{}

func (r *ReducerImpl) Reduce(key string, values []string) (string, error) {
    count := 0
    for _, val := range values {
        c, _ := strconv.Atoi(val)
        count += c
    }
    return strconv.Itoa(count), nil
}

var Reducer plugin.Reducer = &ReducerImpl{}
```

## Project Structure

```
.
├── cmd/
│   ├── cli/              # Command-line interface
│   ├── benchmark-runner/ # Automated performance testing tool
│   └── migrate/          # Database migration tool
├── db/                   # Generated sqlc code (run `sqlc generate` to update)
├── examples/             # Sample Go plugins (WordCount, Graph algorithms, etc.)
├── manifests/            # Kubernetes manifests (Deployments, Services, etc.)
├── pkg/
│   ├── plugin/           # HashiCorp go-plugin interface and RPC definitions
│   ├── middleware/       # JWT authentication and RBAC middleware
│   └── logger/           # Structured logging (zap)
├── services/
│   ├── manager/          # Job orchestration service
│   ├── ui/               # API gateway service
│   ├── builder/          # Plugin compilation service
│   └── worker/           # Task execution service
├── sql/
│   ├── queries/          # SQL queries for sqlc
│   └── schema/           # Database schema migrations
└── test/                 # E2E and integration tests
```

## Fault Tolerance

- **Watchdog**: Scans every 30s for tasks running longer than timeout.
- **Retries**: Automatically reschedules failed tasks up to 3 times on different nodes.
- **Building Phase Isolation**: Code compilation happens in isolated jobs to prevent crashing the Manager.
- **Graceful Resumption**: On Manager restart, it reconciles with PostgreSQL to resume tracking in-flight jobs.

## Testing

The project uses extensive testing, spanning unit tests for components, and end-to-end integration tests:
- **Unit Tests**: Found alongside source files (e.g., `_test.go`), focusing on individual behaviors like the splitter, watchdog, and handlers.
- **E2E Tests** (`test/e2e_test.go`): Submits full jobs and monitors completion.
- **Graph Benchmarks** (`test/graph_benchmark_test.go`): Verifies performance across intensive iteration tests.

Run tests using `make test-coverage` to view the comprehensive test coverage report.

## API Reference

### Jobs
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/jobs/:id/progress` | GET | Returns phase-specific task completion stats |
| `/jobs/:id` | DELETE | Hard cleanup of K8s resources and database records |
| `/jobs/:id/cancel` | POST | Signals the supervisor to stop execution |

## License

[MIT](LICENSE)