import logging

import redis

from redis_message_queue import QueueEvent, RedisMessageQueue

try:
    from prometheus_client import Counter
except ImportError:  # pragma: no cover - example keeps importable without optional dependency.
    Counter = None  # type: ignore[assignment]

log = logging.getLogger(__name__)

if Counter is not None:
    rmq_events_total = Counter(
        "rmq_events_total",
        "redis-message-queue lifecycle events",
        ["queue", "operation", "outcome", "exception_type"],
    )
else:
    rmq_events_total = None


def observe(event: QueueEvent) -> None:
    if rmq_events_total is not None:
        rmq_events_total.labels(
            event.queue,
            event.operation,
            event.outcome,
            event.exception_type or "",
        ).inc()
    if event.error is not None:
        # OpenTelemetry adapters can call span.record_exception(event.error) here.
        log.debug(
            "queue event carried exception object",
            exc_info=(type(event.error), event.error, event.error.__traceback__),
        )
    log.info(
        "queue event",
        extra={
            "queue": event.queue,
            "operation": event.operation,
            "outcome": event.outcome,
            "message_id": event.message_id,
            "claim_id": event.claim_id,
            "exception_type": event.exception_type,
            "has_error": event.error is not None,
        },
    )


client = redis.Redis.from_url("redis://localhost:6379/0")
queue = RedisMessageQueue("jobs", client=client, on_event=observe)
