# MapReduce on Kubernetes

A distributed MapReduce implementation built in Go, designed to run on Kubernetes. This project implements a scalable, fault-tolerant parallel computation framework following the architecture described in [Dean & Ghemawat's MapReduce paper](https://www.usenix.org/legacy/publications/library/proceedings/osdi04/tech/full_papers/dean/dean.pdf).

## Documentation

| Document | Description |
|----------|-------------|
| [Design Document (PDF)](docs/design-document.pdf) | System design, technology choices, UML diagrams |
| [Architecture & UML](docs/architecture.md) | Detailed architecture, sequence diagrams, state machines |
| [Word Count Example](examples/wordcount/) | Classic MapReduce word frequency counter |
| [Inverted Index Example](examples/inverted-index/) | Build word-to-document index |

## Features

- **Distributed Execution**: Scale to thousands of parallel workers via Kubernetes Jobs
- **Fault Tolerance**: Automatic task retry with watchdog-based failure detection
- **Authentication**: Full SSO via Keycloak with JWT tokens and RBAC
- **Flexible Input/Output**: JSON Lines and plain text formats with byte-range splitting
- **Object Storage**: MinIO-based storage for input, code, intermediate, and output files
- **CLI & API**: Command-line interface and REST API for job management
- **Go Plugins**: Type-safe mapper/reducer execution via HashiCorp go-plugin

## Architecture

```
┌─────────────┐      ┌─────────────────┐       ┌──────────────────┐
│   CLI       │────▶│   UI Service    │─────▶│  Manager Service │
│  (mapreduce)│      │   (Gateway)     │       │  (Orchestrator)  │
└─────────────┘      └─────────────────┘       └──────────────────┘
                            │                          │
                            ▼                          ▼
                     ┌───────────────┐         ┌───────────────┐
                     │   Keycloak    │         │  Worker Pods  │
                     │   (Auth)      │         │  (K8s Jobs)   │
                     └───────────────┘         └───────────────┘
                            │                          │
                            ▼                          ▼
                     ┌───────────────┐         ┌───────────────┐
                     │  PostgreSQL   │◀─────▶│     MinIO     │
                     │  (State DB)   │         │   (Storage)   │
                     └───────────────┘         └───────────────┘
```

### Services

| Service | Description | Kubernetes Resource |
|---------|-------------|---------------------|
| **UI Service** | Public API gateway with JWT validation | Deployment |
| **Manager Service** | Job orchestration and worker scheduling | Deployment/StatefulSet |
| **Workers** | Execute map/reduce tasks | batch/v1 Jobs |
| **Keycloak** | Identity provider (OpenID Connect) | Deployment |
| **PostgreSQL** | Persistent job/task state storage | StatefulSet |
| **MinIO** | S3-compatible object storage | StatefulSet |

## Quick Start

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/)
- [Minikube](https://minikube.sigs.k8s.io/docs/start/) or a Kubernetes cluster
- [kubectl](https://kubernetes.io/docs/tasks/tools/)
- [Go 1.25+](https://golang.org/dl/) (for development)

### Deploy to Minikube

```bash
# Start minikube and deploy all services
make minikube-start

# This runs:
#   minikube start --cpus=4 --memory=6144 --disk-size=20g
#   kubectl apply -f ./manifests/
#   minikube addons enable ingress
```

### Build Docker Images

```bash
make docker-build
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

```bash
# Submit a MapReduce job
mapreduce jobs submit \
  --input ./data/input.jsonl \
  --mapper ./scripts/mapper.py \
  --reducer ./scripts/reducer.py \
  --mappers 4 \
  --reducers 2 \
  --format jsonl
```

### Monitor Jobs

```bash
# List your jobs
mapreduce jobs list

# Get job details
mapreduce jobs get <job-id>

# Cancel a running job
mapreduce jobs cancel <job-id>

# Download output
mapreduce jobs output <job-id>
```

### Admin Commands

Requires `admin` role in Keycloak.

```bash
# List all users
mapreduce admin users list

# Create a new user
mapreduce admin users create --username bob --email bob@example.com --password secret123

# Assign admin role
mapreduce admin users role <user-id> admin

# Delete a user
mapreduce admin users delete <user-id>

# List all jobs (any owner)
mapreduce admin jobs list
```

## Writing Map/Reduce Functions

Functions communicate via stdin/stdout and can be written in any language.

### Mapper

Reads input records from stdin, emits `key\tvalue` pairs to stdout.

```python
#!/usr/bin/env python3
# mapper.py - Word count mapper
import sys

for line in sys.stdin:
    for word in line.strip().split():
        print(f"{word.lower()}\t1")
```

### Reducer

Reads sorted `key\tvalue` pairs from stdin, emits aggregated results.

```python
#!/usr/bin/env python3
# reducer.py - Word count reducer
import sys
from itertools import groupby

def key_func(line):
    return line.split('\t')[0]

for key, group in groupby(sys.stdin, key_func):
    count = sum(int(line.split('\t')[1]) for line in group)
    print(f"{key}\t{count}")
```

### Input Formats

| Format | Description | Example |
|--------|-------------|---------|
| `jsonl` | JSON Lines (one JSON object per line) | `{"user": "alice", "action": "click"}` |
| `text` | Plain text (one record per line) | `Hello world` |

## Project Structure

```
.
├── cmd/
│   ├── cli/              # Command-line interface
│   │   ├── command/      # Cobra commands (jobs, admin, login)
│   │   ├── client/       # HTTP client with auth
│   │   └── config/       # CLI configuration
│   └── migrate/          # Database migration tool
├── db/                   # Generated sqlc code
├── manifests/            # Kubernetes manifests
│   ├── 00_namespace.yml
│   ├── 01_secrets.yml
│   ├── 02_configmap.yml
│   ├── 03_postgres.yml
│   ├── 04_keycloak.yml
│   ├── 05_minio.yml
│   ├── 06_manager.yml
│   ├── 07_ui.yml
│   └── 08_ingress.yml
├── pkg/
│   ├── jwks/             # JWKS key fetching/caching
│   ├── logger/           # Structured logging (zap)
│   └── middleware/
│       ├── auth/         # JWT validation middleware
│       └── rbac/         # Role-based access control
├── services/
│   ├── manager/          # Job orchestration service
│   │   ├── config/
│   │   ├── dispatcher/   # K8s Job creation
│   │   ├── handler/      # HTTP handlers
│   │   ├── splitter/     # Input file splitting
│   │   ├── supervisor/   # Job state machine
│   │   └── watchdog/     # Stale task detection
│   └── ui/               # API gateway service
│       ├── client/       # Keycloak, MinIO, Manager clients
│       ├── config/
│       └── handler/
├── sql/
│   └── queries/          # SQL queries for sqlc
├── Makefile
├── go.mod
└── sqlc.yml
```

## Development

### Run Tests

```bash
# Run all tests
go test ./...

# With coverage report
make test-coverage
```

### Generate Database Code

```bash
# After modifying sql/queries/*.sql
sqlc generate
```

### Local Development

```bash
# Start dependencies (requires Docker Compose or running K8s services)
# Then run services locally:

# Terminal 1: Manager service
cd services/manager && go run .

# Terminal 2: UI service
cd services/ui && go run .
```

## Configuration

### Environment Variables

#### UI Service
| Variable | Description | Default |
|----------|-------------|---------|
| `PORT` | HTTP listen port | `8081` |
| `MANAGER_URL` | Manager service URL | `http://manager:8080` |
| `KEYCLOAK_URL` | Keycloak base URL | `http://keycloak:8080` |
| `KEYCLOAK_REALM` | Keycloak realm name | `mapreduce` |
| `KEYCLOAK_CLIENT_ID` | OAuth client ID | `mapreduce-cli` |
| `MINIO_ENDPOINT` | MinIO endpoint | `minio:9000` |

#### Manager Service
| Variable | Description | Default |
|----------|-------------|---------|
| `PORT` | HTTP listen port | `8080` |
| `POSTGRES_DSN` | PostgreSQL connection string | - |
| `MY_REPLICA_NAME` | Pod identity for job affinity | hostname |
| `WORKER_NAMESPACE` | Namespace for worker Jobs | `mapreduce` |
| `WORKER_IMAGE` | Worker container image | `starpal/mapreduce-worker:latest` |
| `TASK_TIMEOUT_SECONDS` | Task execution timeout | `300` |
| `TASK_MAX_RETRIES` | Max retry attempts | `3` |

## Fault Tolerance

### Worker Failure Recovery

1. **Watchdog**: Scans every 30s for tasks running longer than timeout
2. **Retry**: Failed tasks are retried up to 3 times
3. **State Persistence**: All task state stored in PostgreSQL
4. **Manager Recovery**: On restart, Manager resumes monitoring in-flight jobs

### Testing Fault Tolerance

```bash
# Kill a worker pod
kubectl delete pod -n mapreduce -l app=mapreduce-worker --wait=false

# Watch the job recover
mapreduce jobs get <job-id>
```

## API Reference

### Authentication
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/auth/login` | POST | Authenticate and get token |

### Jobs
| Endpoint | Method | Auth | Description |
|----------|--------|------|-------------|
| `/jobs` | GET | user/admin | List jobs |
| `/jobs` | POST | user/admin | Submit new job |
| `/jobs/:id` | GET | user/admin | Get job details |
| `/jobs/:id/cancel` | POST | user/admin | Cancel job |
| `/jobs/:id/output` | GET | user/admin | Get output URLs |

### Files
| Endpoint | Method | Auth | Description |
|----------|--------|------|-------------|
| `/files/input` | POST | user/admin | Upload input file |
| `/files/code` | POST | user/admin | Upload mapper/reducer |

### Admin
| Endpoint | Method | Auth | Description |
|----------|--------|------|-------------|
| `/admin/users` | GET | admin | List users |
| `/admin/users` | POST | admin | Create user |
| `/admin/users/:id` | DELETE | admin | Delete user |
| `/admin/users/:id/roles` | POST | admin | Assign role |
| `/admin/jobs` | GET | admin | List all jobs |

## Technology Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Language** | Go 1.25 | Core services |
| **HTTP Framework** | Fiber v3 | REST API |
| **Database** | PostgreSQL + sqlc | Type-safe queries |
| **Object Storage** | MinIO | S3-compatible storage |
| **Authentication** | Keycloak | OpenID Connect / OAuth 2.0 |
| **Container Orchestration** | Kubernetes | Worker scheduling |
| **CLI Framework** | Cobra | Command parsing |
| **Logging** | Zap | Structured logging |
| **Testing** | Testify | Assertions and mocks |

## License

[MIT](LICENSE)

## Acknowledgments

- [MapReduce: Simplified Data Processing on Large Clusters](https://www.usenix.org/legacy/publications/library/proceedings/osdi04/tech/full_papers/dean/dean.pdf) - Dean & Ghemawat, OSDI'04
- INF-419 Principles of Distributed Systems - Technical University of Crete
