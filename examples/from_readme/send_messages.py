"""README sync publisher example.

Set REDIS_URL to override the default local Redis URL.
"""

import os
import time
from random import randint as random_number

from redis import Redis

from redis_message_queue import RedisMessageQueue

# This is a minimal demonstration. Production checklist NOT shown here:
# GracefulInterruptHandler, client.close(), bounded completed queue.
# See examples/production/.

if __name__ == "__main__":
    client = Redis.from_url(os.getenv("REDIS_URL") or "redis://localhost:6379/0", decode_responses=True)
    queue = RedisMessageQueue(
        name="my_message_queue",
        client=client,
        deduplication=True,
        get_deduplication_key=lambda message: message,
    )

    while True:
        # Sending unique messages
        queue.publish(f"Hello (id={random_number(0, 1_000_000)})")
        time.sleep(1)
