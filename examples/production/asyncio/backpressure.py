"""Publish asynchronously with explicit backpressure handling.

Set REDIS_URL to override the default local Redis URL.
"""

import asyncio
import logging
import os
from itertools import count

from redis.asyncio import Redis

from redis_message_queue.asyncio import QueueBackpressureError, RedisMessageQueue

REDIS_CONNECTION_STRING = os.getenv("REDIS_URL") or "redis://localhost:6379/0"

log = logging.getLogger(__name__)


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    client = Redis.from_url(REDIS_CONNECTION_STRING, decode_responses=True)
    queue = RedisMessageQueue(
        name="my_message_queue",
        client=client,
        max_pending_length=1000,
        pending_overload_policy="raise",
    )

    try:
        for sequence in count(1):
            message = {"id": str(sequence), "body": f"work item {sequence}"}

            while True:
                try:
                    await queue.publish(message)
                    log.info("published %s", message["id"])
                    break
                except QueueBackpressureError:
                    log.warning("pending queue is full; waiting before retrying")
                    await asyncio.sleep(0.25)

            await asyncio.sleep(0.05)
    finally:
        await client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
