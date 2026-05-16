"""Production-shape async consumer example.

Set REDIS_URL to override the default local Redis URL.
"""

import asyncio
import logging
import os

from redis.asyncio import Redis

from redis_message_queue.asyncio import GracefulInterruptHandler, RedisMessageQueue

REDIS_CONNECTION_STRING = os.getenv("REDIS_URL") or "redis://localhost:6379/0"

log = logging.getLogger(__name__)


async def process(message: str) -> None:
    print(f"Received Message: {message}")
    await asyncio.sleep(0.5)


async def main() -> None:
    handler = GracefulInterruptHandler()
    client = Redis.from_url(REDIS_CONNECTION_STRING, decode_responses=True)
    queue = RedisMessageQueue(
        name="my_message_queue",
        client=client,
        interrupt=handler,
        visibility_timeout_seconds=300,
        heartbeat_interval_seconds=60,
        max_delivery_count=5,
        enable_failed_queue=True,
        max_failed_length=1000,
        enable_completed_queue=True,
        max_completed_length=1000,
        on_heartbeat_failure=lambda: log.warning("heartbeat failed; lease may be stale"),
    )

    try:
        while not handler.is_interrupted():
            async with queue.process_message() as message:
                if message is None:
                    continue
                try:
                    await process(message)
                except Exception:
                    log.exception("handler failed; message routed to failed queue or dead-letter queue")
                    raise
    finally:
        await client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
