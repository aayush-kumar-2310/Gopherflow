# Workflow Orchestration Engine

A distributed, event-driven workflow orchestration system for scheduling and executing multi-stage data pipelines. Supports Python script execution, SFTP/S3 file operations, and LangChain-powered summarisation and report generation.

## Architecture

Three independent microservices communicating via Kafka, with Redis as the scheduling and state backbone and PostgreSQL as the persistent store.

```
┌─────────────────────────────────────────────────────┐
│                      VPC                            │
│                                                     │
│   ┌──────────────────┐    Kafka topics              │
│   │   Orchestrator   │──────────────────────────┐  │
│   │   (Go)           │  stage.execute.script     │  │
│   │                  │  stage.execute.langchain   │  │
│   │  - HTTP API      │  stage.file               │  │
│   │  - Cron scheduler│◄─────────────────────────┐│  │
│   │  - DAG evaluator │  stage.result             ││  │
│   │  - Result handler│                           ││  │
│   └────────┬─────────┘                           ││  │
│            │                                     ││  │
│   ┌────────▼──────────┐   ┌─────────────────┐   ││  │
│   │      Redis        │   │  Executor svc   │───┘│  │
│   │                   │   │  (Python)       │    │  │
│   │  workflow-trigger │   │                 │    │  │
│   │  stage-queue      │   │  EXECUTE_SCRIPT │    │  │
│   │  pending:{id}     │   │  SUMMARIZE      │    │  │
│   │  dependents:{id}  │   │  GENERATE_REPORT│    │  │
│   │  state:{wf}:{run} │   └─────────────────┘    │  │
│   │  seen:{jobId}     │                          │  │
│   │  lock:{jobId}     │   ┌─────────────────┐    │  │
│   │  cache:stages:{wf}│   │  File service   │────┘  │
│   └───────────────────┘   │  (Go)           │       │
│                           │                 │       │
│   ┌───────────────────┐   │  FETCH/UPLOAD   │       │
│   │    PostgreSQL     │   │  SFTP and S3    │       │
│   │  workflows+stages │   └─────────────────┘       │
│   └───────────────────┘                             │
└─────────────────────────────────────────────────────┘
```

## Key Design Decisions

### DAG dependency engine
Every stage carries a `dependsOn: []string` field. At workflow creation the orchestrator validates the graph with DFS cycle detection and builds two Redis structures:

- `dependents:{stageId}` — a Redis SET of stages to notify when this stage completes (forward map inverted at creation time)
- `pending:{workflowId}:{runId}:{stageId}` — an atomic counter initialised to `len(dependsOn)`

When a stage completes, the orchestrator calls `SMEMBERS dependents:{stageId}`, then `DECR` on each child's pending counter. Any counter that hits zero is pushed directly to `stage-queue` with `score = now`. No DB scan, no LIKE query on JSON arrays.

### Dual ZSET scheduler
Two sorted sets serve fundamentally different purposes:

| ZSET | Score | Purpose |
|------|-------|---------|
| `workflow-trigger` | Next cron Unix timestamp | Wakes up a workflow on schedule |
| `stage-queue` | `time.Now().Unix()` | Holds stages ready to execute |

The poller reads `workflow-trigger` to fire workflows, fetches stage details from DB (with Redis read-through cache), and pushes initial stages (those with `dependsOn: []`) to `stage-queue`. All subsequent stages are pushed directly by the result handler — not by the poller.

### Deduplication
Kafka delivers at-least-once. Before acting on any result message the orchestrator runs:

```
SET seen:{workflowId}:{stageId}:{retryN}  1  NX  EX 86400
```

If the key already exists, the message is a redelivery and is dropped before any state mutation.

### Distributed lock
Before a worker executes a stage it acquires:

```
SET lock:{jobId}  {workerId}  NX  PX {ttl_ms}
```

Release uses a Lua check-and-delete to prevent a stale worker from releasing a lock it no longer owns.

### Read-through cache
Stage definitions are immutable after creation. On cron fire the orchestrator checks `cache:stages:{workflowId}` before hitting the DB, reducing query load under concurrent same-schedule workflows. TTL is 1 hour.

## Stage schema

```json
{
  "stageId": "fetch-source",
  "workflowId": "wf-abc123",
  "jobType": "FETCH_SFTP",
  "dependsOn": [],
  "weight": 1,
  "uploadTarget": { "host": "sftp.example.com", "path": "/data/in" },
  "script": "",
  "prompt": ""
}
```

Supported `jobType` values: `EXECUTE_SCRIPT`, `FETCH_SFTP`, `UPLOAD_SFTP`, `FETCH_S3`, `UPLOAD_S3`, `SUMMARIZE`, `GENERATE_REPORT`

## Services

### Orchestrator (Go)
- `POST /createWorkflow` — validate, persist to DB, build Redis dependency structures
- `GET /workflows` — list all workflows
- Redis poller goroutine — fires cron-scheduled workflows and dispatches ready stages
- Kafka consumer — handles `stage.result` topic, runs DAG evaluation on each completion

### Executor service (Python)
- Kafka consumer on `stage.execute.script` and `stage.execute.langchain`
- Runs Python scripts in subprocess with isolated execution
- LangChain integration for summarisation and report generation
- Publishes to `stage.result` on completion or failure

### File service (Go)
- Kafka consumer on `stage.file`
- SFTP client for fetch and upload operations
- AWS SDK S3 client for fetch and upload operations
- Publishes to `stage.result` on completion or failure

## Retry and failure

- Each stage retries up to 3 times. `jobId` is scoped as `{workflowId}:{stageId}:{retryN}`
- On exhaustion, `workflowId` and `stageId` are pushed to a Kafka DLQ
- User is notified via SMTP on failure
- Stages with `dependsOn` referencing a failed stage remain unscheduled; the workflow is marked `PARTIAL_FAILURE` in the DB

## Redis key inventory

| Key pattern | Type | Purpose |
|-------------|------|---------|
| `workflow-trigger` | ZSET | Cron schedule queue |
| `stage-queue` | ZSET | Ready-to-execute stages |
| `lock:{jobId}` | STRING | Distributed execution lock |
| `dependents:{stageId}` | SET | Forward notification map |
| `pending:{wfId}:{runId}:{stageId}` | STRING | Atomic unblock counter |
| `state:{workflowId}:{runId}` | STRING | Live workflow state (JSON) |
| `seen:{jobId}` | STRING | Kafka dedup guard |
| `cache:stages:{workflowId}` | STRING | DB read-through cache |

## Tech stack

| Layer | Technology |
|-------|-----------|
| Orchestrator | Go, Gin, GORM, go-redis, robfig/cron |
| Executor | Python, LangChain, confluent-kafka-python |
| File service | Go, AWS SDK v2, pkg/sftp |
| Message broker | Apache Kafka |
| Cache / scheduler | Redis |
| Database | PostgreSQL |
| Auth | JWT (planned) |

## Getting started

```bash
# Clone the repo
git clone https://github.com/yourusername/workflow-orchestrator

# Start infrastructure
docker-compose up -d redis kafka postgres

# Run orchestrator
cd orchestrator && go run main.go

# Run executor service
cd executor && pip install -r requirements.txt && python main.py

# Run file service
cd file-service && go run main.go
```

## Status

Active development. Core orchestrator (HTTP API, cron scheduler, DAG validation, Redis scheduling) is functional. DB persistence layer, Kafka integration, executor service, and file service are in progress.
