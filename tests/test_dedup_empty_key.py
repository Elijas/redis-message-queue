import fakeredis
import pytest

from redis_message_queue._exceptions import ConfigurationError
from redis_message_queue.asyncio.redis_message_queue import RedisMessageQueue as AsyncRedisMessageQueue
from redis_message_queue.redis_message_queue import RedisMessageQueue


def test_empty_custom_dedup_key_is_rejected_before_suppressing_messages():
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue(
        "ad02-empty-dedup",
        client=client,
        deduplication=True,
        get_deduplication_key=lambda _message: "",
    )
    message = {"tenant": "a", "body": "msg-1"}

    with pytest.raises(ConfigurationError) as exc_info:
        queue.publish(message)

    assert str(exc_info.value) == (
        "get_deduplication_key returned an empty string for message "
        "{'tenant': 'a', 'body': 'msg-1'}; "
        "the callable must return a non-empty, high-cardinality key"
    )
    assert client.llen(queue.key.pending) == 0
    assert client.exists(queue.key.deduplication_prefix) == 0


def test_none_custom_dedup_key_is_rejected_at_publish_time():
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue(
        "ad02-none-dedup",
        client=client,
        deduplication=True,
        get_deduplication_key=lambda _message: None,
    )
    message = {"tenant": "a", "body": "msg-1"}

    with pytest.raises(ConfigurationError) as exc_info:
        queue.publish(message)

    assert str(exc_info.value) == (
        "get_deduplication_key returned None for message "
        "{'tenant': 'a', 'body': 'msg-1'}; "
        "the callable must return a non-empty string"
    )
    assert client.llen(queue.key.pending) == 0


def test_non_str_custom_dedup_key_raises_type_error():
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue(
        "ad02-nonstr-dedup",
        client=client,
        deduplication=True,
        get_deduplication_key=lambda _message: 123,
    )

    with pytest.raises(TypeError, match="get_deduplication_key must return a str, got int"):
        queue.publish({"tenant": "a", "body": "msg-1"})

    assert client.llen(queue.key.pending) == 0


@pytest.mark.asyncio
async def test_async_empty_custom_dedup_key_is_rejected_before_suppressing_messages():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue(
        "ad02-empty-dedup-async",
        client=client,
        deduplication=True,
        get_deduplication_key=lambda _message: "",
    )
    message = {"tenant": "a", "body": "msg-1"}

    with pytest.raises(ConfigurationError) as exc_info:
        await queue.publish(message)

    assert str(exc_info.value) == (
        "get_deduplication_key returned an empty string for message "
        "{'tenant': 'a', 'body': 'msg-1'}; "
        "the callable must return a non-empty, high-cardinality key"
    )
    assert await client.llen(queue.key.pending) == 0
    assert await client.exists(queue.key.deduplication_prefix) == 0
