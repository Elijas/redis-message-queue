import asyncio
import random
import time

from redis.asyncio import Redis

from redis_message_queue.asyncio import RedisMessageQueue

REDIS_CONNECTION_STRING = "redis://localhost:6379/0"


def create_message() -> str:
    if random.random() < 0.8:
        return f"Hello (id={random.randint(0,1000)})"

    # 20% chance of returning "World"
    # to show how duplicate messages are prevented
    return "World"


async def main():
    client = Redis.from_url(REDIS_CONNECTION_STRING)
    queue = RedisMessageQueue(
        name="my_message_queue",
        client=client,
        deduplication=True,
    )

    while True:
        message = create_message()
        was_published = await queue.publish(message)

        if was_published:
            print(f"Success: Sent message '{message}'.")
        else:
            print(f"Duplicate: Message '{message}' was already sent previously.")

        time.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
