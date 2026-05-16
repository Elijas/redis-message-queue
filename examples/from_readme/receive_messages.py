"""README sync consumer example.

Set REDIS_URL to override the default local Redis URL.
"""

import os

from redis import Redis

from redis_message_queue import RedisMessageQueue

# This is a minimal demonstration. Production checklist NOT shown here:
# visibility_timeout_seconds, max_delivery_count + dead_letter_queue,
# on_heartbeat_failure, GracefulInterruptHandler. See examples/production/.

if __name__ == "__main__":
    client = Redis.from_url(
        os.getenv("REDIS_URL", "redis://localhost:6379/0"),
        decode_responses=True,
    )
    queue = RedisMessageQueue("my_message_queue", client=client)

    while True:
        with queue.process_message() as message:
            if message is not None:
                print(f"Received Message: {message}")
