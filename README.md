# Gopherflow

A two-service, event-driven workflow orchestration system in Go. Schedules multi-stage pipelines with cron, DAG dependencies, weighted execution, retries, and run history in PostgreSQL.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  WorkflowOrchestrator (Go)                                  │
│  - POST /createWorkflow, GET /workflows, GET /runs          │
│  - Cron → Redis workflow-trigger                            │
│  - DAG counters + weighted job-trigger queue                │
│  - Kafka producer (execute-stage) / consumer (responses)    │
│  - Redis stage status → PostgreSQL on run complete          │
└───────────────────────────┬─────────────────────────────────┘
                            │ Kafka
                            ▼
┌─────────────────────────────────────────────────────────────┐
│  Event_Handler (Go)                                         │
│  - EXECUTE_SCRIPT, FETCH_SFTP, UPLOAD_SFTP                  │
│  - HTTP_REQUEST, LLM (Ollama)                               │
└─────────────────────────────────────────────────────────────┘

        Redis (scheduling + live state)    PostgreSQL (definitions + history)
```

## Supported stage types

| `operation` | Description |
|-------------|-------------|
| `EXECUTE_SCRIPT` | Run inline Python via `python3 -c` |
| `FETCH_SFTP` | Download remote file |
| `UPLOAD_SFTP` | Upload local file |
| `HTTP_REQUEST` | Call an HTTP API |
| `LLM` | Send a prompt to Ollama (use for summarisation, reports, etc.) |

Each stage has `weight` (1–5). Higher weight is scheduled sooner in the weighted `job-trigger` queue.

## Retry behaviour

- Up to **3 attempts** per stage (`MaxStageAttempts`).
- On failure: Redis status `FAILED-RETRY`, then re-queued with the same weight.
- After exhaustion: `FAILED-EXHAUSTED`, dependents are `SKIPPED`, run becomes `PARTIAL_FAILURE`.
- Kafka result dedup via `seen:{workflowId}:{runId}:{stageId}:{attempt}`.

## Redis keys (runtime)

| Key | Purpose |
|-----|---------|
| `workflow-trigger` | Cron schedule (ZSET) |
| `job-trigger` | Weighted ready stages (ZSET) |
| `stage-status:{wf}:{run}:{stage}` | PENDING / RUNNING / FIN / FAILED-* / SKIPPED |
| `deps:{wf}:{stage}:{run}` | Remaining parent count |
| `children:{wf}:{parent}:{run}` | Downstream stage IDs |
| `output:{wf}:{stage}:{run}` | Stage result for dependents |
| `attempt:{wf}:{run}:{stage}` | Current attempt number |
| `run:total` / `run:done` | Progress toward finalization |

## PostgreSQL

| Table | Purpose |
|-------|---------|
| `workflows` / `stages` | Definitions |
| `workflow_runs` | One row per cron execution |
| `stage_executions` | Final per-stage outcome (flushed when run completes) |

## API

- `POST /createWorkflow` — create workflow + schedule cron
- `GET /workflows` — list definitions
- `GET /runs?workflowId=` — run history
- `GET /runs/:runId/stages` — stage outcomes for a run

## Configuration

Copy `.env.example` to `.env` (or edit the repo `.env`) and set Postgres, Redis, Kafka, and optional SMTP values. Both services load `.env` on startup via `Shared/config`.

| Variable | Default | Purpose |
|----------|---------|---------|
| `POSTGRES_DSN` | local gopherflow | GORM database |
| `REDIS_ADDR` | `localhost:6379` | Scheduling + live state |
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated brokers |
| `WORKER_POOL_SIZE` | `8` | Max concurrent stage/workflow workers per service |
| `SMTP_ENABLED` | `false` | Email on retry exhaustion |
| `SMTP_TO` | — | Recipient for failure alerts |
| `KAFKA_DLQ_TOPIC` | `stage-dlq` | Dead-letter topic for exhausted failures |
| `ORCHESTRATOR_METRICS_PORT` | `9091` | Orchestrator `/metrics` |
| `EVENT_HANDLER_METRICS_PORT` | `9094` | Executor `/metrics` (do not use 9092 — that is Kafka) |
| `LOG_LEVEL` / `LOG_FORMAT` | `info` / `json` | Structured `slog` logging |
| `KAFKA_READ_TIMEOUT_SEC` | `5` | Kafka poll timeout (avoids blocking forever) |
| `SHUTDOWN_TIMEOUT_SEC` | `15` | Graceful shutdown budget |

Create the DLQ topic in Kafka: `stage-dlq`. Metrics: `curl localhost:9091/metrics`.

## Run locally

```bash
# Infrastructure (Redis, Kafka, Postgres) must be running.
# .env lives at repo root; config loads it from cwd or parent folders.

cd /path/to/Gopherflow
go run ./WorkflowOrchestrator

# second terminal
EVENT_HANDLER_METRICS_PORT=9094 go run ./Event_Handler
```

Or from a service folder: `cd WorkflowOrchestrator && go run .` (still finds `../.env`).

## Status

Two-service MVP: orchestration, DAG, weighted queue, retries, stage status in Redis, and run history persistence are implemented. SMTP/DLQ, REST Kafka fallback, and auth are not yet included.
