"""Drain an asyncio worker from a SIGTERM hook with queue.drain().

Set REDIS_URL to override the default local Redis URL.
Set REDIS_MAX_CONNECTIONS to size the finite Redis connection pool.
"""

import asyncio
import os
import signal

from redis.asyncio import Redis

from redis_message_queue.asyncio import QueueDrainedError, RedisMessageQueue

REDIS_CONNECTION_STRING = os.getenv("REDIS_URL") or "redis://localhost:6379/0"
REDIS_MAX_CONNECTIONS = int(os.getenv("REDIS_MAX_CONNECTIONS", "32"))


async def process(message: str) -> None:
    print(f"Processing: {message}")
    await asyncio.sleep(0.5)


async def shutdown(queue: RedisMessageQueue, stop: asyncio.Event) -> None:
    print("Received shutdown signal; draining queue")
    try:
        await queue.publish({"event": "shutdown_requested"})
    except QueueDrainedError:
        # Fires after drain begins; late publishes should be dropped or rescheduled elsewhere.
        print("Queue is already draining; skipped shutdown audit publish")
    drained = await queue.drain(timeout=10)
    print(f"Drain complete: {drained}")
    stop.set()


async def main() -> None:
    stop = asyncio.Event()
    client = Redis.from_url(
        REDIS_CONNECTION_STRING,
        decode_responses=True,
        max_connections=REDIS_MAX_CONNECTIONS,
    )
    queue = RedisMessageQueue(name="my_message_queue", client=client)
    loop = asyncio.get_running_loop()

    for signum in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(signum, lambda: loop.create_task(shutdown(queue, stop)))

    try:
        while not stop.is_set():
            async with queue.process_message() as message:
                if message is None:
                    continue
                await process(message)
    finally:
        await client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
