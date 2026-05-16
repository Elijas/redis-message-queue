"""Drain pending claims from a SIGTERM hook.

Set REDIS_URL to override the default local Redis URL.
"""

import os
import signal
import threading
import time

from redis import Redis

from redis_message_queue import QueueDrainedError, RedisMessageQueue

REDIS_CONNECTION_STRING = os.getenv("REDIS_URL") or "redis://localhost:6379/0"


def process(message: str) -> None:
    print(f"Processing: {message}")
    time.sleep(0.5)


def install_shutdown_hook(queue: RedisMessageQueue, stop: threading.Event) -> None:
    def request_shutdown(signum: int, _frame: object) -> None:
        print(f"Received signal {signum}; draining queue")
        try:
            queue.publish({"event": "shutdown_requested", "signal": signum})
        except QueueDrainedError:
            # Fires after drain begins; late publishes should be dropped or rescheduled elsewhere.
            print("Queue is already draining; skipped shutdown audit publish")
        drained = queue.drain(timeout=10)
        queue.close()
        print(f"Drain complete: {drained}")
        stop.set()

    signal.signal(signal.SIGTERM, request_shutdown)
    signal.signal(signal.SIGINT, request_shutdown)


def main() -> None:
    stop = threading.Event()
    client = Redis.from_url(REDIS_CONNECTION_STRING, decode_responses=True)
    queue = RedisMessageQueue(name="my_message_queue", client=client)

    install_shutdown_hook(queue, stop)

    try:
        while not stop.is_set():
            with queue.process_message() as message:
                if message is None:
                    continue
                process(message)
    finally:
        client.close()


if __name__ == "__main__":
    main()
