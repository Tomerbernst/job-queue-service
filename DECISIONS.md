# Design Decisions

## 1. Job Pickup Strategy

**Approach chosen:** Two independent guards. (a) A Redis **sorted set** as the
queue, dequeued with `BZPOPMAX`; (b) a **compare-and-set claim** in Postgres.

`BZPOPMAX` is executed atomically server-side by Redis. With N workers blocked
on the same key, each member is handed to exactly one of them — Redis will never
deliver the same id to two workers. That alone prevents double pickup for
in-flight ids.

The second guard exists because the id in Redis and the row in Postgres can
disagree (a job may have been cancelled, or re-enqueued by the reaper after a
presumed crash). So after popping an id the worker runs:

```sql
UPDATE jobs SET status='processing', attempts=attempts+1, worker_id=:w,
       started_at=now(), heartbeat_at=now()
WHERE id=:id AND status IN ('pending','scheduled') AND (scheduled_at IS NULL OR scheduled_at<=now())
RETURNING *;
```

If this updates 0 rows, the job is not runnable (cancelled / already owned) and
the worker drops it. The `WHERE status = ...` predicate makes the claim a
CAS: under any race, exactly one `UPDATE` matches and the losers see rowcount 0.

**Why:** `BZPOPMAX` gives priority ordering *and* atomic hand-off in one
blocking call — no polling hot-loop, low latency. The DB CAS reconciles queue
state against the source of truth, which is what makes cancellation and crash
recovery safe.

**Trade-offs:** State lives in two places (Redis + Postgres). I accept that
Redis is a lossy delivery cache, not a ledger — anything important is
reconstructable from Postgres by the maintenance sweep. The alternative,
Postgres-only with `SELECT … FOR UPDATE SKIP LOCKED`, needs one fewer moving
part but either polls or needs `LISTEN/NOTIFY`, and priority + blocking pop are
clumsier; the assignment also explicitly asks for a queue/cache technology.

---

## 2. Worker Crash Recovery

**Approach chosen:** Heartbeats + a background reaper.

While a job runs, the owning worker updates `heartbeat_at` every
`heartbeat_interval` seconds (default 5s). A maintenance loop — run by whichever
worker currently holds a short-TTL Redis lock (`SET NX EX`), so only one sweeps
at a time — periodically finds jobs that are still `processing` but whose
`heartbeat_at` is older than `stale_after` (default 30s) and recovers them via
the same `fail_job` path used for normal failures: retry with backoff if
attempts remain, otherwise mark `failed` and dead-letter.

**Why:** Heartbeats detect *silent* death (SIGKILL, OOM, network partition) that
a `try/finally` cannot. A DB timestamp needs no extra infrastructure and is
trivially queryable with an index on `(status, heartbeat_at)`. The Redis lock is
only an optimisation to avoid N redundant sweeps — because every recovery step
is itself a CAS, it stays correct even if two workers sweep simultaneously.

**What happens if worker crashes mid-job:** The row is left in `processing` with
a frozen `heartbeat_at`. No consumer will touch it (its id is no longer in the
queue, and the claim CAS would reject it anyway). Within ~`stale_after` seconds
the reaper notices the stale heartbeat and calls `fail_job`, which — because
`attempts` was already incremented at claim time — either reschedules it for a
backoff retry (re-enqueued when due by the scheduler) or, if the attempt budget
is exhausted, marks it permanently `failed` and pushes it to the dead-letter
queue. Because `attempts` increments on claim rather than on success, a job that
repeatedly crashes a worker (a poison message) still exhausts its budget and
lands in the DLQ instead of looping forever.

**Note on semantics:** This is **at-least-once** execution. A worker that
finishes the actual work but dies before committing `completed` will have the
job retried. Handlers should therefore be idempotent; the completion step is a
`worker_id`-scoped CAS so a slow worker whose job was already reaped cannot
overwrite the newer owner's result.

---

## 3. Priority Queue Implementation

**Approach chosen:** Redis sorted set (`ZSET`), score =
`priority * 1e13 - enqueue_time_ms`, dequeued with `BZPOPMAX` (highest score).

Each priority occupies a disjoint numeric band (`1e13` wide); within a band the
subtracted enqueue timestamp means an earlier job has the higher score, giving
**FIFO within a priority**. The band width keeps the packed score inside
float64's 53-bit integer-safe range for the supported priority range (0–100).

**Why:** One data structure gives priority ordering, FIFO tiebreak, atomic
blocking pop, and O(log n) enqueue/dequeue — no separate queue per priority
level to fan out over, and no application-side sorting. `BZPOPMAX` blocks, so
idle workers consume no CPU (no polling).

**Trade-offs:** Float packing is bounded — priorities beyond the designed range
would collide with the FIFO bits. A cleaner-but-heavier alternative is one list
per priority level with a Lua script that pops the highest non-empty list; I
preferred the single ZSET for simplicity. Strict priority also risks starving
low-priority jobs under sustained high-priority load — acceptable here, and
noted below as future work (aging).

---

## 4. Retry Backoff Strategy

**Approach chosen:** Exponential backoff, scheduled via the same Redis ZSET used
for future-dated jobs.

On failure a job is set to `status='scheduled'` with `scheduled_at = now + delay`
where `delay = base * factor^(attempts-1)`. The maintenance sweep promotes it
back into the queue once due. This reuses the scheduled-job machinery — retries
and future-dated submissions are the same code path.

**Timing:** `base = 30s`, `factor = 4`, capped at 1h, max 3 attempts:

| Attempt | When it runs                        |
|---------|-------------------------------------|
| 1       | immediately on submit               |
| 2       | ~30s after the 1st attempt fails    |
| 3       | ~120s after the 2nd attempt fails   |
| —       | after 3 failures → `failed` + DLQ   |

(Delays are configurable via env vars; tests use sub-second values. Jitter can
be layered on to avoid thundering-herd retries — see below.)

---

## 5. One Thing I Would Do Differently With More Time

A few honest simplifications:

- **The scheduler promotes in two steps (enqueue-to-Redis, then flip status in
  Postgres) without a distributed transaction.** I ordered them enqueue-first
  and made the claim accept `scheduled`-and-due rows so a crash between the two
  steps still converges, but a single atomic hand-off (e.g. an outbox pattern,
  or doing the promotion entirely inside a Lua script against Redis-resident
  state) would be cleaner and I'd revisit it first.
- **No retry jitter yet.** The backoff is deterministic; under a correlated
  outage many jobs would retry in lockstep. I'd add randomised jitter.
- **Strict priority can starve low-priority jobs.** I'd add priority aging
  (gradually raising effective priority with wait time).
- **Schema is created with `create_all` on startup, not Alembic migrations.**
  Fine for the assignment, wrong for anything that has to evolve in production.
- **DLQ has no automated replay tooling** beyond the manual `POST /retry`
  endpoint — I'd add a DLQ inspection/requeue admin path.

---

## 6. Edge Cases & Failure Modes

### What is handled

| Failure / race | How it's covered |
|---|---|
| Two workers grab the same job | Atomic `BZPOPMAX` (one id → one worker) **and** a Postgres CAS claim (`UPDATE … WHERE status='pending'`) as a second guard. |
| Worker crashes mid-job | Heartbeats + reaper: a `processing` row with a stale `heartbeat_at` is retried with backoff (or failed + dead-lettered if exhausted). |
| Worker crashes **between `BZPOPMAX` and the claim commit** | The id is gone from Redis but the row is still `pending`; the `requeue_orphaned_pending` sweep re-enqueues any pending row missing from the queue after a grace period. |
| API crashes **between committing the row and enqueueing it** | Same orphan-recovery sweep re-enqueues it. |
| Redis loses all data | Postgres is the source of truth; the sweep rebuilds the queue from `pending`/`scheduled` rows. |
| Poison message (crashes the worker every time) | `attempts` increments at **claim** time, not on success, so a job that repeatedly kills workers still exhausts its budget → `failed` + DLQ instead of looping forever. |
| Cancel while queued | Status flips to `cancelled` (CAS); the id may linger in Redis but the claim CAS refuses it, so it never runs. |
| Duplicate submit with same idempotency key | Unique index on `idempotency_key` is authoritative; concurrent racers resolve via `IntegrityError` → the existing job is returned (HTTP 200). Rows are never expired, satisfying the ≥24h retention requirement. |
| Duplicate enqueue (sweep + API, or reaper + scheduler) | `enqueue` is an idempotent `ZADD` — a member is delivered once. |
| Transient DB error during heartbeat | The heartbeat loop swallows-and-continues, so one blip cannot stop heartbeats and cause a false "crashed" reap of a live job. |
| Graceful shutdown | SIGTERM stops dequeuing and finishes the in-flight job before exit (`stop_grace_period: 30s`). |

### Known limitations (honest)

- **At-least-once, not exactly-once.** A worker that finishes the work but dies
  before committing `completed` will have the job retried — handlers must be
  idempotent. The completion write is `worker_id`-scoped so a reaped-then-revived
  slow worker can't overwrite the new owner's result, but the *side effect* may
  run twice.
- **Strict priority can starve low-priority jobs** under sustained high-priority
  load. Fix would be priority aging.
- **DLQ grows unbounded** — no trimming/replay tooling yet.
- **`asyncio.wait_for` can't interrupt a truly CPU-blocking handler.** The mock
  jobs are cooperative (async sleeps), so timeouts work here; a real blocking
  handler would need a process/thread executor to enforce the timeout.
- **Reaping relies on per-node clocks.** Significant clock skew between workers
  could reap slightly early/late. A DB-side `now()` for all time comparisons
  would remove the dependency.
