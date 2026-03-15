import asyncio

from redis.asyncio import Redis

from redis_message_queue.asyncio import RedisMessageQueue
from redis_message_queue.interrupt_handler import GracefulInterruptHandler

REDIS_CONNECTION_STRING = "redis://localhost:6379/0"


async def main():
    # The GracefulInterruptHandler allows us to handle Ctrl+C (SIGINT) gracefully.
    # This means that when the user sends an interrupt signal, the program will
    # not terminate immediately but will instead set a flag that can be used to
    # stop the program in an orderly fashion, allowing for any necessary cleanup.
    handler = GracefulInterruptHandler()

    client = Redis.from_url(REDIS_CONNECTION_STRING, decode_responses=True)
    queue = RedisMessageQueue(
        name="my_message_queue",
        client=client,
        interrupt=handler,
    )
    while True:
        async with queue.process_message() as message:
            if message is None:
                if handler.is_interrupted():
                    print("Exiting...")
                    break
            else:
                delay = 0.5
                print(f"Received Message: '{message}'. Will pretend to process it {delay} seconds...")
                await asyncio.sleep(delay)
                print(f"Finished processing message '{message}' Waiting for next message.")


if __name__ == "__main__":
    asyncio.run(main())
