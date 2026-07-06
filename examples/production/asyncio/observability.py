"""Async production observability adapter for redis-message-queue.

Construct the Redis client and queue inside the worker process (post-fork)
to satisfy the fork-safety rules in README. Importing this module at module
top is fine; call make_queue() from your worker_main() / startup hook.
Set REDIS_URL to override the default local Redis URL.
Set REDIS_MAX_CONNECTIONS to size the finite Redis connection pool.

SPAN_SINK_TRUSTED gates `event.error` export. Set to True only when your
telemetry sink is trust-equivalent to your application logs and is
access-controlled. See README "Secrets in event.error".
"""

import asyncio
import logging
import os
from dataclasses import dataclass

import redis.asyncio as redis

from redis_message_queue.asyncio import QueueEvent, RedisMessageQueue

try:
    from prometheus_client import Counter, start_http_server
except ImportError:  # pragma: no cover - example keeps importable without optional dependency.
    Counter = None  # type: ignore[assignment]
    start_http_server = None  # type: ignore[assignment]

log = logging.getLogger(__name__)

SPAN_SINK_TRUSTED = False
REDIS_MAX_CONNECTIONS = int(os.getenv("REDIS_MAX_CONNECTIONS", "32"))

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
    if event.error is not None and SPAN_SINK_TRUSTED:
        # OpenTelemetry adapters can call span.record_exception(event.error) here.
        # Only enabled when SPAN_SINK_TRUSTED is True because the exception object
        # retains message, __cause__ chain, traceback, and frame locals that may
        # contain Redis credentials, payload data, or environment values.
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


@dataclass(frozen=True)
class AsyncQueueResources:
    queue: RedisMessageQueue
    client: redis.Redis

    async def aclose(self, timeout: float | None = None) -> bool:
        try:
            return await self.queue.drain(timeout=timeout)
        finally:
            await self.client.aclose()


def make_queue(
    queue_name: str = "jobs",
    url: str | None = None,
    **kwargs: object,
) -> AsyncQueueResources:
    """Construct queue + Redis client. Call from inside worker_main()."""
    url = url or os.getenv("REDIS_URL") or "redis://localhost:6379/0"
    client = redis.Redis.from_url(
        url,
        retry=None,  # strict at-most-once for non-deduplicated publishes; see docs/operations.md#known-limitations.
        max_connections=REDIS_MAX_CONNECTIONS,
    )
    queue = RedisMessageQueue(queue_name, client=client, on_event=observe, **kwargs)
    return AsyncQueueResources(queue=queue, client=client)


async def main() -> None:
    # When run as a script (single process), construction here is safe.
    if start_http_server is not None:
        start_http_server(9100)
    resources = make_queue()
    try:
        # Use resources.queue in your consume loop.
        ...
    finally:
        await resources.aclose()


if __name__ == "__main__":
    asyncio.run(main())
