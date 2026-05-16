"""Production-shape sync consumer example.

Set REDIS_URL to override the default local Redis URL.
"""

import logging
import os
import time

from redis import Redis

from redis_message_queue import GracefulInterruptHandler, RedisMessageQueue

REDIS_CONNECTION_STRING = os.getenv("REDIS_URL", "redis://localhost:6379/0")

log = logging.getLogger(__name__)


def process(message: str) -> None:
    print(f"Received Message: {message}")
    time.sleep(0.5)


def main() -> None:
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
            with queue.process_message() as message:
                if message is None:
                    continue
                try:
                    process(message)
                except Exception:
                    log.exception("handler failed; message routed to failed queue or dead-letter queue")
                    raise
    finally:
        client.close()


if __name__ == "__main__":
    main()
