"""Async production observability adapter for redis-message-queue.

Construct the Redis client and queue inside the worker process (post-fork)
to satisfy the fork-safety rules in README. Importing this module at module
top is fine; call make_queue() from your worker_main() / startup hook.
"""

import asyncio
import logging

import redis.asyncio as redis

from redis_message_queue.asyncio import QueueEvent, RedisMessageQueue

try:
    from prometheus_client import Counter, start_http_server
except ImportError:  # pragma: no cover - example keeps importable without optional dependency.
    Counter = None  # type: ignore[assignment]
    start_http_server = None  # type: ignore[assignment]

log = logging.getLogger(__name__)

if Counter is not None:
    rmq_events_total = Counter(
        "rmq_async_events_total",
        "redis_message_queue lifecycle events",
        ["queue", "operation", "outcome", "exception_type"],
    )
else:
    rmq_events_total = None


async def observe(event: QueueEvent) -> None:
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


def make_queue(
    queue_name: str = "jobs",
    url: str = "redis://localhost:6379/0",
    **kwargs: object,
) -> RedisMessageQueue:
    """Construct queue + Redis client. Call from inside worker_main()."""
    client = redis.Redis.from_url(url, retry=None)  # See AC-16 retry note in README.
    return RedisMessageQueue(queue_name, client=client, on_event=observe, **kwargs)


async def main() -> None:
    # When run as a script (single process), construction here is safe.
    if start_http_server is not None:
        start_http_server(9100)
    queue = make_queue()
    # ... your consume loop
    await queue.aclose()


if __name__ == "__main__":
    asyncio.run(main())
