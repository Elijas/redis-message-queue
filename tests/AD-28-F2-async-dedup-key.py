import redis.asyncio

from redis_message_queue.asyncio import RedisMessageQueue


async def async_key(message: object) -> str:
    return "dedup-key"


# Runtime accepts async get_deduplication_key and awaits its result at publish
# time, but the public type says Callable[..., str].
queue = RedisMessageQueue(
    "q",
    client=redis.asyncio.Redis(),
    deduplication=True,
    get_deduplication_key=async_key,
)
print(queue)
