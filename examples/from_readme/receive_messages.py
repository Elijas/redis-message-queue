from redis import Redis

from redis_message_queue import RedisMessageQueue

client = Redis.from_url(
    "redis://localhost:6379/0",
    decode_responses=True,
)
queue = RedisMessageQueue("my_message_queue", client=client)

while True:
    with queue.process_message() as message:
        if message:
            print(f"Received Message: {message}")
