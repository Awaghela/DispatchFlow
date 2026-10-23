"""Runtime configuration. Every value can be overridden with an environment variable
(or a .env file), which is how the same image runs locally, in Docker and on AWS."""
from functools import lru_cache

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", case_sensitive=False)

    app_name: str = "DispatchFlow"
    environment: str = "local"
    log_level: str = "INFO"

    # --- PostgreSQL (RDS in AWS) ---
    database_url: str = "postgresql+asyncpg://dispatch:dispatch@localhost:5432/dispatchflow"
    db_pool_size: int = 10
    db_max_overflow: int = 10

    # --- Redis (ElastiCache in AWS) ---
    redis_url: str = "redis://localhost:6379/0"
    redis_enabled: bool = True
    redis_breaker_seconds: float = 2.0  # how long to skip Redis after a failure
    cache_ttl_seconds: int = 30

    # --- SQS ---
    aws_region: str = "us-east-1"
    aws_endpoint_url: str | None = None  # set for LocalStack / moto, leave empty on AWS
    sqs_queue_name: str = "dispatchflow-orders"
    sqs_dlq_name: str = "dispatchflow-orders-dlq"
    sqs_queue_url: str | None = None  # if set, skips name lookup
    sqs_dlq_url: str | None = None
    sqs_auto_create: bool = False  # convenient locally; on AWS create queues with IaC/CLI
    sqs_max_receive_count: int = 10  # receives before SQS moves a message to the DLQ
    sqs_visibility_timeout: int = 30
    sqs_wait_seconds: int = 2  # long-poll wait (max 20)

    # --- Worker ---
    worker_concurrency: int = 16
    worker_metrics_port: int = 9102
    # Where the API can read the worker's /metrics for the dashboard's Metrics view (optional).
    worker_metrics_url: str | None = "http://127.0.0.1:9102/metrics"
    max_assign_attempts: int = 6
    assignment_timeout_seconds: int = 120  # ASSIGNED but never picked up -> give the job to someone else
    sweeper_interval: float = 2.0
    redrive_after_seconds: int = 60  # CREATED/ASSIGNING with no activity this long -> re-enqueue the assignment
    retry_base_delay: int = 1  # seconds, exponential with jitter
    retry_max_delay: int = 30
    outbox_poll_interval: float = 0.2
    outbox_batch_size: int = 100
    geo_reconcile_interval: float = 20.0
    depth_poll_interval: float = 2.0

    @field_validator("aws_endpoint_url", "sqs_queue_url", "sqs_dlq_url", "worker_metrics_url", mode="before")
    @classmethod
    def _blank_is_unset(cls, v):
        # AWS_ENDPOINT_URL= (empty) must mean "real AWS", not an endpoint called "".
        return None if isinstance(v, str) and not v.strip() else v

    # --- WebSockets ---
    ws_queue_size: int = 1000
    replay_overlap: int = 50  # re-send this many ids before last_event_id (clients dedupe)
    replay_limit: int = 5000
    tailer_interval: float = 1.0

    # --- Test-only fault injection (never enable in production) ---
    chaos_worker_fail_rate: float = 0.0


@lru_cache
def get_settings() -> Settings:
    return Settings()
