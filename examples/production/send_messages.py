"""Production-shape sync publisher example.

Set REDIS_URL to override the default local Redis URL.
Set REDIS_MAX_CONNECTIONS to size the finite Redis connection pool.
"""

import os
import time
from random import randint as random_number

from redis import Redis

from redis_message_queue import GracefulInterruptHandler, RedisMessageQueue

REDIS_CONNECTION_STRING = os.getenv("REDIS_URL") or "redis://localhost:6379/0"
REDIS_MAX_CONNECTIONS = int(os.getenv("REDIS_MAX_CONNECTIONS", "32"))


def create_message() -> dict[str, str]:
    message_id = str(random_number(0, 1_000_000))
    return {
        "id": message_id,
        "body": f"Hello (id={message_id})",
    }


def main() -> None:
    handler = GracefulInterruptHandler()
    client = Redis.from_url(
        REDIS_CONNECTION_STRING,
        decode_responses=True,
        max_connections=REDIS_MAX_CONNECTIONS,
    )
    queue = RedisMessageQueue(
        name="my_message_queue",
        client=client,
        deduplication=True,
        get_deduplication_key=lambda message: message["id"],
        enable_completed_queue=True,
        max_completed_length=1000,
        interrupt=handler,
    )

    try:
        while not handler.is_interrupted():
            message = create_message()
            was_published = queue.publish(message)

            if was_published:
                print(f"Success: Sent message '{message['body']}'.")
            else:
                print(f"Duplicate: Message '{message['id']}' was already sent previously.")

            time.sleep(1)
    finally:
        client.close()


if __name__ == "__main__":
    main()
