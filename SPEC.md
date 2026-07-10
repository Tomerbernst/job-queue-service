# Requirements Traceability

Every assignment requirement mapped to the code that implements it and the test
that proves it. Status: DONE unless noted.

## API Endpoints
| Requirement | Implementation | Test |
|---|---|---|
| Submit a new job | `app/api/routes.py` `POST /jobs` | `tests/test_api.py::test_submit_and_retrieve_job` |
| Get job status / result / error | `routes.py` `GET /jobs/{id}` | `test_lifecycle.py::test_email_job_completes` |
| List jobs with filters (status, type) | `routes.py` `GET /jobs` | `test_api.py::test_list_jobs_with_filters` |
| Cancel a pending/scheduled job | `routes.py` `POST /jobs/{id}/cancel`, `state.cancel_job` | `test_cancel.py` |
| Retry a failed job | `routes.py` `POST /jobs/{id}/retry`, `state.retry_job` | `test_retry.py::test_manual_retry_of_failed_job` |
| Health check with queue statistics | `routes.py` `GET /health` | `test_api.py::test_health_endpoint` |

## Job Lifecycle
| Requirement | Implementation | Test |
|---|---|---|
| States: scheduled/pending/processing/completed/failed/cancelled | `app/models.py` `JobStatus` | (used throughout) |
| failed → (manual retry) → pending | `state.retry_job` | `test_retry.py::test_manual_retry_of_failed_job` |

## Job Types
| Requirement | Implementation | Test |
|---|---|---|
| Email: sleep 1–3s, mock message id | `app/jobs/email.py` | `test_lifecycle.py::test_email_job_completes` |
| Webhook: sleep 1–2s, 80/20 success | `app/jobs/webhook.py` | `test_retry.py` (forced-failure path) |
| Report: sleep 3–5s, mock file url | `app/jobs/report.py` | `test_lifecycle.py::test_report_job_returns_file_url` |
| Batch: items, progress %, summary | `app/jobs/batch.py` | `test_lifecycle.py::test_batch_job_tracks_progress` |

## Critical Implementation Details
| Requirement | Implementation | Test |
|---|---|---|
| 1. Job pickup, no duplicates | atomic `BZPOPMAX` (`redis_client.py`) + CAS claim (`state.claim_job`) | `test_worker.py::test_claim_is_exclusive`, `::test_no_duplicate_processing_under_concurrency` |
| 2. Worker crash recovery | heartbeats + `state.reap_stale_jobs`; orphan requeue `state.requeue_orphaned_pending` | `test_worker.py::test_crash_recovery_reaps_stale_processing_job`, `::test_orphaned_pending_is_requeued` |
| 3. Retry with exponential backoff (30s, 120s, then FAILED) | `state.backoff_delay`, `state.fail_job` | `test_retry.py::test_backoff_timing_matches_spec`, `::test_retry_exhaustion_marks_failed_and_dead_letters` |
| 4. Priority queue (higher first, FIFO tiebreak) | score packing `redis_client._score` | `test_priority.py` |
| 5. Scheduled jobs (not run until due) | SCHEDULED status + `state.promote_due_jobs` | `test_worker.py::test_scheduled_job_promoted_when_due`, `test_api.py::test_future_scheduled_job_is_not_enqueued` |
| 6. Idempotency (return existing, keys ≥24h) | unique index + `IntegrityError` guard (`routes.py`); rows never expire | `test_idempotency.py` |

## Data Model
| Requirement | Implementation |
|---|---|
| Job: id, type, payload, status, priority, attempts/max, error, progress, scheduling, timestamps, result, idempotency_key | `app/models.py` `Job` |
| Job Log: job, level, message, metadata, timestamp | `app/models.py` `JobLog` |

## Technical Requirements
| Must have | Where |
|---|---|
| Python 3.11+ | `Dockerfile` (python:3.11-slim), `pyproject.toml` |
| Web framework | FastAPI (`app/api`) |
| Relational DB | PostgreSQL + SQLAlchemy async |
| Queue/cache tech | Redis (`app/redis_client.py`) |
| Separate worker process | `app/worker/main.py`, `worker` service in compose |
| Retry + backoff (3 attempts) | `config.py` `max_attempts=3`, `state.fail_job` |
| Priority processing | `redis_client._score` |
| Job cancellation | `state.cancel_job` |
| docker-compose runs api + worker | `docker-compose.yml` |
| ≥6 meaningful tests | `tests/` (45 tests) |

| Should have | Where |
|---|---|
| Scheduled jobs | `state.promote_due_jobs` |
| Worker crash recovery | `state.reap_stale_jobs` + heartbeats |
| Structured JSON logging w/ job context | `app/logging.py` (structlog JSON) |
| Graceful shutdown | `Worker.request_shutdown`, compose `stop_grace_period` |
| Health endpoint w/ queue stats | `GET /health` |

| Nice to have | Where |
|---|---|
| Multiple concurrent workers | `worker_concurrency` + compose `replicas` / `--scale` |
| Progress tracking for batch | `app/jobs/batch.py` + `state.set_progress` |
| Job timeout enforcement | `asyncio.wait_for` in `Worker.process_job` |
| Dead letter queue | `state.fail_job` → `queue.dlq_push`; `GET /dead-letter` to inspect |

## Required deliverables
`README.md`, `DECISIONS.md`, `AI_USAGE.md`, `docker-compose.yml`, `Dockerfile`, `app/`, `tests/` — all present.
