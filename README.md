# Job Queue Service

A distributed background job-processing system: an HTTP API accepts jobs, a
Redis-backed priority queue holds them, and separate worker processes execute
them with retries, scheduling, crash recovery and graceful shutdown. PostgreSQL
is the source of truth for all job state.

Built with **FastAPI + SQLAlchemy 2.0 (async) + PostgreSQL + Redis**.

---

## Architecture overview

```
                    submit / query
   Client ───────────────▶ FastAPI (api)
                              │  writes job row, pushes id to queue
                              ▼
                        ┌───────────┐        ┌──────────────┐
                        │ PostgreSQL│◀──────▶│    Redis      │
                        │  (state,  │        │ queue (ZSET), │
                        │ source of │        │ scheduled,    │
                        │  truth)   │        │ DLQ, workers  │
                        └───────────┘        └──────┬────────┘
                              ▲                      │ BZPOPMAX
                              │ claim / finalize     ▼
                        ┌───────────────────────────────────┐
                        │  Worker process(es)               │
                        │   consumers + maintenance sweep    │
                        │   (scheduler + crash reaper)       │
                        └───────────────────────────────────┘
```

- **Postgres = source of truth.** Every state transition is a compare-and-set
  `UPDATE`. Redis is only a delivery mechanism; if it loses data (or a process
  dies between claiming and enqueuing) the maintenance sweep re-enqueues any
  pending job whose id has gone missing from the queue.
- **Queue** is a Redis sorted set. Score packs `priority` with an enqueue-time
  tiebreak so dequeue is highest-priority-first, FIFO within a priority.
  `BZPOPMAX` is atomic — no polling hot-loop, and each id goes to one worker.
- **Worker** runs N concurrent consumer tasks plus a leader-elected maintenance
  loop that promotes due scheduled/retry jobs and reaps crashed workers' jobs.

See **[DECISIONS.md](DECISIONS.md)** for the reasoning and trade-offs behind
each of these, and **[AI_USAGE.md](AI_USAGE.md)** for how AI tools were used.

---

## How to run

Requires Docker + Docker Compose. From the project root:

```bash
docker compose up --build
```

This starts Postgres, Redis, the API (on `http://localhost:8000`) and **2 worker
replicas**. The API creates the schema on startup; workers wait for it.

If port 8000 is already in use on your machine, pick another host port with the
`API_PORT` variable (the API still answers on that port; use it in the curls
below):

```bash
API_PORT=8080 docker compose up --build   # then curl http://localhost:8080/...
```

Scale workers up or down:

```bash
docker compose up --build --scale worker=4
```

API docs (Swagger UI) are served at `http://localhost:8000/docs`.

---

## How to submit a test job

```bash
# Submit an email job
curl -s -X POST http://localhost:8000/jobs \
  -H 'Content-Type: application/json' \
  -d '{"type":"email","payload":{"to":"user@example.com","subject":"Hi","body":"Hello"},"priority":5}'
# → {"id":"<uuid>", "status":"pending", ...}

# Check its status / result
curl -s http://localhost:8000/jobs/<uuid>

# List failed jobs
curl -s 'http://localhost:8000/jobs?status=failed'

# Health + queue stats
curl -s http://localhost:8000/health
```

Other job types:

```bash
# Webhook (80% success / 20% simulated failure → exercises retries)
curl -s -X POST http://localhost:8000/jobs -H 'Content-Type: application/json' \
  -d '{"type":"webhook","payload":{"url":"https://example.com/hook","event":"ping"}}'

# Report (returns a mock file URL)
curl -s -X POST http://localhost:8000/jobs -H 'Content-Type: application/json' \
  -d '{"type":"report","payload":{"report_type":"sales"}}'

# Batch (tracks progress %)
curl -s -X POST http://localhost:8000/jobs -H 'Content-Type: application/json' \
  -d '{"type":"batch","payload":{"items":[1,2,3,4,5]}}'

# Scheduled job (runs in the future)
curl -s -X POST http://localhost:8000/jobs -H 'Content-Type: application/json' \
  -d '{"type":"email","payload":{"to":"a@b.co","subject":"later"},"scheduled_at":"2030-01-01T00:00:00Z"}'

# Idempotent submit (same key returns the same job, no duplicate)
curl -s -X POST http://localhost:8000/jobs -H 'Content-Type: application/json' \
  -d '{"type":"email","payload":{"to":"a@b.co","subject":"x"},"idempotency_key":"order-42"}'
```

### API endpoints

| Method & path            | Purpose                                        |
|--------------------------|------------------------------------------------|
| `POST /jobs`             | Submit a job (201, or 200 on idempotency hit)  |
| `GET /jobs/{id}`         | Get status, result or error                    |
| `GET /jobs`              | List jobs; filter by `status`, `type` (paged)  |
| `GET /jobs/{id}/logs`    | Per-job structured log of state transitions    |
| `POST /jobs/{id}/cancel` | Cancel a pending/scheduled job                 |
| `POST /jobs/{id}/retry`  | Re-queue a permanently failed job              |
| `GET /health`            | Liveness + queue depth, DLQ size, live workers |

---

## How to run the tests

The test suite is fully isolated from external services (SQLite in-memory +
fakeredis stand in for Postgres/Redis), so it needs no running containers.

Inside Docker (recommended — matches the Python 3.11 target):

```bash
docker compose run --rm --no-deps api pytest -v
```

Or locally (Python 3.11+):

```bash
pip install -e ".[dev]"
pytest -v
```

The suite covers the required scenarios — submission/retrieval, completion,
failure & retry (incl. exhaustion → dead-letter), cancellation, idempotency and
priority ordering — plus worker-only tests (claim exclusivity, crash recovery,
scheduled promotion, graceful shutdown, no-duplicate-processing under
concurrency) that exercise the worker independently of the API.
