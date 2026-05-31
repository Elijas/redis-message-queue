"""Minimal sync consumer example.

Set REDIS_URL to override the default local Redis URL.
"""

import os
import time

from redis import Redis

from redis_message_queue import GracefulInterruptHandler, RedisMessageQueue

# This is a minimal demonstration. Production checklist NOT shown here:
# visibility_timeout_seconds, max_delivery_count + dead_letter_queue,
# on_heartbeat_failure. See examples/production/.

REDIS_CONNECTION_STRING = os.getenv("REDIS_URL") or "redis://localhost:6379/0"


def main():
    # A single handled shutdown signal (for example, Ctrl+C/SIGINT) lets the
    # loop stop between messages or after the current simulated work finishes.
    # When run through wrappers such as `uv run`, terminal interrupts may reach
    # the process group more than once; if that lands during time.sleep(...),
    # users may see a KeyboardInterrupt traceback instead of the clean Exiting...
    # line.
    handler = GracefulInterruptHandler()

    client = Redis.from_url(REDIS_CONNECTION_STRING, decode_responses=True)
    queue = RedisMessageQueue(
        name="my_message_queue",
        client=client,
        interrupt=handler,
    )
    try:
        while True:
            with queue.process_message() as message:
                if message is None:
                    if handler.is_interrupted():
                        print("Exiting...")
                        break
                else:
                    delay = 0.5
                    print(f"Received Message: '{message}'. Will pretend to process it {delay} seconds...")
                    time.sleep(delay)
                    print(f"Finished processing message '{message}' Waiting for next message.")
    finally:
        client.close()


if __name__ == "__main__":
    main()
