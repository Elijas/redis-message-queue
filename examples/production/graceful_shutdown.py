"""Drain pending claims from a SIGTERM hook.

Set REDIS_URL to override the default local Redis URL.
Set REDIS_MAX_CONNECTIONS to size the finite Redis connection pool.
"""

import os
import signal
import threading
import time

from redis import Redis

from redis_message_queue import QueueDrainedError, RedisMessageQueue

REDIS_CONNECTION_STRING = os.getenv("REDIS_URL") or "redis://localhost:6379/0"
REDIS_MAX_CONNECTIONS = int(os.getenv("REDIS_MAX_CONNECTIONS", "32"))


def process(message: str) -> None:
    print(f"Processing: {message}")
    time.sleep(0.5)


def install_shutdown_hook(queue: RedisMessageQueue, stop: threading.Event) -> None:
    shutdown_started = threading.Event()
    shutdown_guard = threading.Lock()

    def request_shutdown(signum: int, _frame: object) -> None:
        if not shutdown_guard.acquire(blocking=False):
            print(f"Received signal {signum}; shutdown already in progress")
            return
        try:
            if shutdown_started.is_set():
                print(f"Received signal {signum}; shutdown already in progress")
                return
            shutdown_started.set()
        finally:
            shutdown_guard.release()

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
    client = Redis.from_url(
        REDIS_CONNECTION_STRING,
        decode_responses=True,
        max_connections=REDIS_MAX_CONNECTIONS,
    )
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
