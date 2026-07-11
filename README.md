# Job Queue Service

A background job processing system. An HTTP API takes in jobs, a Redis priority
queue holds them, and separate worker processes run them. It does retries with
backoff, scheduled jobs, priorities, worker crash recovery, and graceful
shutdown. Postgres holds all the job state; Redis is just how the work gets
handed to the workers.

Stack: FastAPI, SQLAlchemy 2.0 (async), PostgreSQL, Redis.

## Architecture

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

A few things worth knowing about how it fits together:

**Postgres is the source of truth.** Every status change is a conditional
`UPDATE` that only applies if the job is still in the state we expect, so two
workers can never both process the same job. Redis only carries job ids. If
Redis loses data, or a process dies between claiming a job and putting it on the
queue, a background sweep re-adds any pending job that went missing.

**The queue is a Redis sorted set.** The score combines a job's priority with
its arrival time, so higher priority comes out first and jobs of equal priority
stay in order. Workers pull with `BZPOPMAX`, which blocks until a job shows up,
so idle workers don't sit there polling.

**Each worker** runs a handful of consumer tasks plus a background loop (only one
worker runs it at a time) that promotes scheduled and retry jobs when they come
due and recovers jobs left behind by workers that crashed.

There's more detail in [DECISIONS.md](DECISIONS.md) on why I built it this way
and the trade-offs, [AI_USAGE.md](AI_USAGE.md) on how I used AI tools, and
[SPEC.md](SPEC.md) which maps each requirement to the code and test that cover
it.

## Running it

You need Docker and Docker Compose. From the project root:

```bash
docker compose up --build
```

That brings up Postgres, Redis, the API (on `http://localhost:8000`) and 2
workers. The API creates the database tables on startup and the workers wait for
that to finish.

If something is already using port 8000 on your machine, pick a different host
port and use it in the curls below:

```bash
API_PORT=8080 docker compose up --build   # then curl http://localhost:8080/...
```

Run more workers:

```bash
docker compose up --build --scale worker=4
```

Interactive API docs (Swagger) are at `http://localhost:8000/docs` if you'd
rather click through the endpoints than use curl.

## Submitting a job

```bash
# Submit an email job
curl -s -X POST http://localhost:8000/jobs \
  -H 'Content-Type: application/json' \
  -d '{"type":"email","payload":{"to":"user@example.com","subject":"Hi","body":"Hello"},"priority":5}'
# returns {"id":"<uuid>", "status":"pending", ...}

# Check its status / result
curl -s http://localhost:8000/jobs/<uuid>

# List failed jobs
curl -s 'http://localhost:8000/jobs?status=failed'

# Health and queue stats
curl -s http://localhost:8000/health
```

The other job types:

```bash
# Webhook (80% success, 20% simulated failure so you can see retries)
curl -s -X POST http://localhost:8000/jobs -H 'Content-Type: application/json' \
  -d '{"type":"webhook","payload":{"url":"https://example.com/hook","event":"ping"}}'

# Report (returns a mock file URL)
curl -s -X POST http://localhost:8000/jobs -H 'Content-Type: application/json' \
  -d '{"type":"report","payload":{"report_type":"sales"}}'

# Batch (tracks progress percentage as it goes)
curl -s -X POST http://localhost:8000/jobs -H 'Content-Type: application/json' \
  -d '{"type":"batch","payload":{"items":[1,2,3,4,5]}}'

# Scheduled job (runs at a future time)
curl -s -X POST http://localhost:8000/jobs -H 'Content-Type: application/json' \
  -d '{"type":"email","payload":{"to":"a@b.co","subject":"later"},"scheduled_at":"2030-01-01T00:00:00Z"}'

# Idempotent submit (same key returns the same job instead of creating a duplicate)
curl -s -X POST http://localhost:8000/jobs -H 'Content-Type: application/json' \
  -d '{"type":"email","payload":{"to":"a@b.co","subject":"x"},"idempotency_key":"order-42"}'
```

### Endpoints

| Method and path          | What it does                                   |
|--------------------------|------------------------------------------------|
| `POST /jobs`             | Submit a job (201, or 200 on an idempotency hit) |
| `GET /jobs/{id}`         | Get status, result or error                    |
| `GET /jobs`              | List jobs, filter by `status` and `type` (paged) |
| `GET /jobs/{id}/logs`    | The job's log of state transitions             |
| `POST /jobs/{id}/cancel` | Cancel a pending or scheduled job              |
| `POST /jobs/{id}/retry`  | Re-queue a job that failed for good            |
| `GET /health`            | Queue depth, DLQ size, workers, per-status counts, oldest-job ages |
| `GET /health/live`       | Cheap liveness probe (what the container healthcheck uses) |
| `GET /dead-letter`       | Look at dead-letter (poison) jobs for triage   |

## Running the tests

The tests don't need any containers. They use an in-memory SQLite database and
fakeredis in place of the real Postgres and Redis, so they run on their own.

Inside Docker (this matches the Python 3.11 target):

```bash
docker compose run --rm --no-deps api pytest -v
```

Or locally, with Python 3.11 or newer:

```bash
pip install -e ".[dev]"
pytest -v
```

They cover the required scenarios (submission and retrieval, completion, failure
and retry including exhaustion to the dead-letter queue, cancellation,
idempotency, and priority ordering). There's also a separate set of tests that
drive the worker directly, without the API, for the concurrency-sensitive parts:
claim exclusivity, crash recovery, scheduled promotion, graceful shutdown, and
no job being processed twice when several workers run at once.
