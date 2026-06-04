"""Production-shape async publisher example.

Set REDIS_URL to override the default local Redis URL.
Set REDIS_MAX_CONNECTIONS to size the finite Redis connection pool.
"""

import asyncio
import os
from random import randint as random_number

from redis.asyncio import Redis

from redis_message_queue.asyncio import GracefulInterruptHandler, RedisMessageQueue

REDIS_CONNECTION_STRING = os.getenv("REDIS_URL") or "redis://localhost:6379/0"
REDIS_MAX_CONNECTIONS = int(os.getenv("REDIS_MAX_CONNECTIONS", "32"))


def create_message() -> dict[str, str]:
    message_id = str(random_number(0, 1_000_000))
    return {
        "id": message_id,
        "body": f"Hello (id={message_id})",
    }


async def main(handler: GracefulInterruptHandler) -> None:
    client = Redis.from_url(
        REDIS_CONNECTION_STRING,
        decode_responses=True,
        max_connections=REDIS_MAX_CONNECTIONS,
    )
    # Completed retention stores raw payload bytes. Inspect first; manual
    # replay/repair/trim/archive is application-owned. See
    # docs/configuration.md#success-and-failure-tracking.
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
            was_published = await queue.publish(message)

            if was_published:
                print(f"Success: Sent message '{message['body']}'.")
            else:
                print(f"Duplicate: Message '{message['id']}' was already sent previously.")

            await asyncio.sleep(1)
    finally:
        await client.aclose()


if __name__ == "__main__":
    interrupt_handler = GracefulInterruptHandler()
    asyncio.run(main(interrupt_handler))
