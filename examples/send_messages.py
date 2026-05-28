"""Minimal sync publisher example.

Set REDIS_URL to override the default local Redis URL.
"""

import os
import random
import time

from redis import Redis

from redis_message_queue import RedisMessageQueue

# This is a minimal demonstration. Production checklist NOT shown here:
# GracefulInterruptHandler, client.close(), bounded completed queue.
# See examples/production/.

REDIS_CONNECTION_STRING = os.getenv("REDIS_URL") or "redis://localhost:6379/0"


def create_message() -> str:
    if random.random() < 0.8:
        return f"Hello (id={random.randint(0, 1000)})"

    # 20% chance of returning "World"
    # to show how duplicate messages are prevented
    return "World"


def main():
    client = Redis.from_url(REDIS_CONNECTION_STRING, decode_responses=True)
    queue = RedisMessageQueue(
        name="my_message_queue",
        client=client,
        deduplication=True,
        get_deduplication_key=lambda message: message,
    )

    while True:
        message = create_message()
        was_published = queue.publish(message)

        if was_published:
            print(f"Success: Sent message '{message}'.")
        else:
            print(f"Duplicate: Message '{message}' was already sent previously.")

        time.sleep(1)


if __name__ == "__main__":
    main()
