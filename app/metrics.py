"""Prometheus metrics. The API exposes them at /metrics, the worker on WORKER_METRICS_PORT."""
from prometheus_client import Counter, Gauge, Histogram

HTTP_REQUESTS = Counter(
    "dispatchflow_http_requests_total", "HTTP requests", ["method", "route", "status"]
)
HTTP_LATENCY = Histogram(
    "dispatchflow_http_request_duration_seconds",
    "HTTP request latency",
    ["method", "route"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
)

ORDERS_CREATED = Counter("dispatchflow_orders_created_total", "Orders created")
ORDER_TRANSITIONS = Counter(
    "dispatchflow_order_transitions_total", "Order state transitions", ["to_status"]
)
ASSIGN_LATENCY = Histogram(
    "dispatchflow_assignment_latency_seconds",
    "Time from order creation to courier assignment",
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 20, 30, 60, 120),
)
ASSIGN_ATTEMPTS = Counter(
    "dispatchflow_assignment_attempts_total", "Assignment attempts", ["result"]
)  # assigned | no_courier | exhausted | dead_letter

DUPLICATES_ABSORBED = Counter(
    "dispatchflow_duplicates_absorbed_total",
    "Duplicate requests/messages that were safely turned into no-ops",
    ["kind"],
)  # create | transition | message
STALE_IGNORED = Counter(
    "dispatchflow_stale_updates_ignored_total",
    "Delayed / out-of-order updates ignored",
    ["kind"],
)  # transition | location | message

SQS_MESSAGES = Counter(
    "dispatchflow_sqs_messages_total", "SQS messages handled", ["type", "outcome"]
)  # outcome: ack | retry | error
SQS_PROCESS_SECONDS = Histogram(
    "dispatchflow_sqs_process_seconds",
    "Message handler duration",
    ["type"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
)
QUEUE_DEPTH = Gauge("dispatchflow_queue_depth", "SQS queue depth", ["queue", "state"])
OUTBOX_BACKLOG = Gauge("dispatchflow_outbox_backlog", "Unpublished outbox rows")
OUTBOX_PUBLISHED = Counter("dispatchflow_outbox_published_total", "Outbox rows relayed to SQS")

WS_CONNECTIONS = Gauge("dispatchflow_ws_connections", "Open WebSocket connections", ["role"])
WS_MESSAGES = Counter("dispatchflow_ws_messages_total", "WebSocket messages", ["result"])

CACHE_REQUESTS = Counter("dispatchflow_cache_requests_total", "Order cache lookups", ["result"])
REDIS_ERRORS = Counter("dispatchflow_redis_errors_total", "Redis failures (breaker trips)", ["op"])

ASSIGNMENT_TIMEOUTS = Counter(
    "dispatchflow_assignment_timeouts_total", "Assignments reclaimed because the courier never picked up"
)
ORDERS_REDRIVEN = Counter(
    "dispatchflow_orders_redriven_total", "Orders whose assignment message was re-enqueued by the reconciler"
)
