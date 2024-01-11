import time
from random import randint as random_number

from redis import Redis

from redis_message_queue import RedisMessageQueue

client = Redis.from_url("redis://localhost:6379/0")
queue = RedisMessageQueue(
    name="my_message_queue",
    client=client,
    deduplication=True,
)

while True:
    # Sending unique messages
    queue.publish(f"Hello (id={random_number(0, 1_000_000)})")
    time.sleep(1)
