"""Publish with explicit backpressure instead of unbounded pending growth.

Set REDIS_URL to override the default local Redis URL.
Set REDIS_MAX_CONNECTIONS to size the finite Redis connection pool.
"""

import logging
import os
import time
from itertools import count

from redis import Redis

from redis_message_queue import QueueBackpressureError, RedisMessageQueue

REDIS_CONNECTION_STRING = os.getenv("REDIS_URL") or "redis://localhost:6379/0"
REDIS_MAX_CONNECTIONS = int(os.getenv("REDIS_MAX_CONNECTIONS", "32"))

log = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    client = Redis.from_url(
        REDIS_CONNECTION_STRING,
        decode_responses=True,
        max_connections=REDIS_MAX_CONNECTIONS,
    )
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
                    queue.publish(message)
                    log.info("published %s", message["id"])
                    break
                except QueueBackpressureError:
                    log.warning("pending queue is full; waiting before retrying")
                    time.sleep(0.25)

            time.sleep(0.05)
    finally:
        client.close()


if __name__ == "__main__":
    main()
