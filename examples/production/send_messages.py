"""Production-shape sync publisher example."""

import time
from random import randint as random_number

from redis import Redis

from redis_message_queue import GracefulInterruptHandler, RedisMessageQueue

REDIS_CONNECTION_STRING = "redis://localhost:6379/0"


def create_message() -> dict[str, str]:
    message_id = str(random_number(0, 1_000_000))
    return {
        "id": message_id,
        "body": f"Hello (id={message_id})",
    }


def main() -> None:
    handler = GracefulInterruptHandler()
    client = Redis.from_url(REDIS_CONNECTION_STRING, decode_responses=True)
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
            was_published = queue.publish(message)

            if was_published:
                print(f"Success: Sent message '{message['body']}'.")
            else:
                print(f"Duplicate: Message '{message['id']}' was already sent previously.")

            time.sleep(1)
    finally:
        client.close()


if __name__ == "__main__":
    main()
