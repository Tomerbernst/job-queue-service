"""Application configuration, driven entirely by environment variables."""
from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://jobqueue:jobqueue@localhost:5432/jobqueue"
    redis_url: str = "redis://localhost:6379/0"

    # DB connection pool. A worker opens several short-lived sessions per job
    # (claim, per-heartbeat, progress, complete) plus the maintenance loop, so
    # the default pool of 5 can throttle throughput under concurrency.
    db_pool_size: int = 10
    db_max_overflow: int = 10

    # Worker behaviour
    worker_concurrency: int = 4          # concurrent jobs per worker process
    dequeue_timeout: float = 2.0         # BZPOPMAX block time; also the shutdown latency bound
    heartbeat_interval: float = 5.0      # how often a processing job refreshes its heartbeat
    stale_after: float = 30.0            # processing job with no heartbeat for this long is presumed crashed
    maintenance_interval: float = 1.0    # scheduler/reaper sweep cadence
    worker_ttl: float = 15.0             # worker considered dead if not seen for this long

    # Retry policy: attempt 1 immediate, then base * factor^(n-1) → 30s, 120s
    max_attempts: int = 3
    retry_base_delay: float = 30.0
    retry_backoff_factor: float = 4.0
    retry_max_delay: float = 3600.0

    # Job execution
    job_speed_factor: float = 1.0        # multiplier on simulated sleeps (0 in tests)
    max_payload_bytes: int = 64 * 1024   # reject oversized payloads at the API


@lru_cache
def get_settings() -> Settings:
    return Settings()
