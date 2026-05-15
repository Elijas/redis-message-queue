"""Drain an asyncio worker from a SIGTERM hook with queue.aclose().

Run with a local Redis on redis://localhost:6379/0.
"""

import asyncio
import signal

from redis.asyncio import Redis

from redis_message_queue.asyncio import RedisMessageQueue

REDIS_CONNECTION_STRING = "redis://localhost:6379/0"


async def process(message: str) -> None:
    print(f"Processing: {message}")
    await asyncio.sleep(0.5)


async def shutdown(queue: RedisMessageQueue, stop: asyncio.Event) -> None:
    print("Received shutdown signal; draining queue")
    drained = await queue.aclose(timeout=10)
    print(f"Drain complete: {drained}")
    stop.set()


async def main() -> None:
    stop = asyncio.Event()
    client = Redis.from_url(REDIS_CONNECTION_STRING, decode_responses=True)
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
