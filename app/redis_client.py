"""Redis-backed priority queue and worker registry.

Redis is the *delivery* mechanism only — Postgres remains the source of
truth for job state. Losing Redis data never loses a job: the maintenance
sweep's ``requeue_orphaned_pending`` step re-enqueues any PENDING row whose
id is missing from the queue (see app/state.py).

Queue layout:
  jobqueue:queue    ZSET  member=job_id, score=priority-packed-with-FIFO-tiebreak
  jobqueue:dlq      LIST  JSON entries for permanently failed jobs
  jobqueue:workers  ZSET  member=worker_id, score=last-heartbeat epoch
  jobqueue:lock:maintenance  short-TTL lock electing one maintenance loop
"""
import json
from datetime import datetime, timezone

import redis.asyncio as aioredis

QUEUE_KEY = "jobqueue:queue"
DLQ_KEY = "jobqueue:dlq"
WORKERS_KEY = "jobqueue:workers"
MAINTENANCE_LOCK_KEY = "jobqueue:lock:maintenance"

# Score packing: higher priority always beats lower; within a priority,
# earlier enqueue wins (FIFO). Epoch-ms is ~1.8e12, so 1e13 per priority
# level keeps bands disjoint while staying inside float64's 2^53 integer
# precision for priorities 0..100.
_PRIORITY_BAND = 1e13


def _score(priority: int, created_at: datetime) -> float:
    return priority * _PRIORITY_BAND - created_at.timestamp() * 1000


def create_redis(redis_url: str) -> aioredis.Redis:
    return aioredis.from_url(redis_url, decode_responses=True)


class JobQueue:
    def __init__(self, redis: aioredis.Redis):
        self.redis = redis

    # --- queue ---------------------------------------------------------
    async def enqueue(self, job_id: str, priority: int, created_at: datetime) -> None:
        await self.redis.zadd(QUEUE_KEY, {job_id: _score(priority, created_at)})

    async def dequeue(self, timeout: float) -> str | None:
        """Atomically pop the highest-priority job id; None on timeout.

        BZPOPMAX is atomic server-side: with N workers blocked on it, each
        member is delivered to exactly one worker.
        """
        popped = await self.redis.bzpopmax(QUEUE_KEY, timeout=timeout)
        return popped[1] if popped else None

    async def depth(self) -> int:
        return await self.redis.zcard(QUEUE_KEY)

    async def is_queued(self, job_id: str) -> bool:
        """Whether a job id is currently sitting in the queue (ZSCORE probe)."""
        return await self.redis.zscore(QUEUE_KEY, job_id) is not None

    # --- dead letter queue --------------------------------------------
    async def dlq_push(self, entry: dict) -> None:
        await self.redis.rpush(DLQ_KEY, json.dumps(entry, default=str))

    async def dlq_length(self) -> int:
        return await self.redis.llen(DLQ_KEY)

    # --- worker registry ------------------------------------------------
    async def worker_heartbeat(self, worker_id: str) -> None:
        now = datetime.now(timezone.utc).timestamp()
        await self.redis.zadd(WORKERS_KEY, {worker_id: now})

    async def worker_deregister(self, worker_id: str) -> None:
        await self.redis.zrem(WORKERS_KEY, worker_id)

    async def active_workers(self, ttl: float) -> list[dict]:
        cutoff = datetime.now(timezone.utc).timestamp() - ttl
        # prune long-dead entries, then list live ones
        await self.redis.zremrangebyscore(WORKERS_KEY, "-inf", cutoff - 3600)
        members = await self.redis.zrangebyscore(WORKERS_KEY, cutoff, "+inf", withscores=True)
        return [
            {
                "worker_id": wid,
                "last_seen": datetime.fromtimestamp(score, tz=timezone.utc).isoformat(),
            }
            for wid, score in members
        ]

    # --- maintenance-loop election ---------------------------------------
    async def try_acquire_maintenance_lock(self, worker_id: str, ttl: float) -> bool:
        """SET NX EX lock. Refreshes TTL when we already hold it.

        Election is an optimisation (avoids N workers sweeping at once),
        not a correctness requirement — every sweep operation is a CAS.
        """
        acquired = await self.redis.set(
            MAINTENANCE_LOCK_KEY, worker_id, nx=True, ex=int(ttl)
        )
        if acquired:
            return True
        holder = await self.redis.get(MAINTENANCE_LOCK_KEY)
        if holder == worker_id:
            await self.redis.expire(MAINTENANCE_LOCK_KEY, int(ttl))
            return True
        return False

    async def ping(self) -> bool:
        try:
            await self.redis.ping()
            return True
        except Exception:
            return False
