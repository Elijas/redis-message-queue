import time

from redis import Redis

from redis_message_queue.redis_message_queue import RedisMessageQueue

REDIS_CONNECTION_STRING = "redis://localhost:6379/0"


def main():
    client = Redis.from_url(REDIS_CONNECTION_STRING, decode_responses=True)
    queue = RedisMessageQueue(
        name="my_message_queue",
        client=client,
    )
    while True:
        with queue.process_message() as message:
            if not message:
                # Note: you can specify a custom heartbeat interval
                print("Waiting for message...")

            delay = 0.5
            print(
                f"Received Message: '{message}'."
                f" Will pretend to process it {delay} seconds..."
            )
            time.sleep(delay)
            print(f"Finished processing message '{message}' Waiting for next message.")


if __name__ == "__main__":
    main()
