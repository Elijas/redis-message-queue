import asyncio
import time

import fakeredis
import pytest

from redis_message_queue import RedisMessageQueue
from redis_message_queue.asyncio import RedisMessageQueue as AsyncRedisMessageQueue

SYNC_AWAITABLE_ERROR = (
    "Handler returned an awaitable from sync queue; use the async RedisMessageQueue "
    "or wrap the handler to consume the awaitable explicitly."
)


class CancelOnCloseAwaitable:
    def __await__(self):
        if False:
            yield None
        return "unused"

    def close(self) -> None:
        raise asyncio.CancelledError("awaitable close cancelled")


def test_sync_callback_returning_none_processes_and_acks_message():
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue(
        "callback-sync-success",
        client=client,
        enable_completed_queue=True,
    )
    assert queue.publish("hello") is True

    seen = []

    def handler(message):
        seen.append(message)

    assert queue.process_message_callback(handler) is True

    assert seen == [b"hello"]
    assert client.llen(queue.key.pending) == 0
    assert client.llen(queue.key.processing) == 0
    assert client.llen(queue.key.completed) == 1


def test_sync_callback_handler_exception_uses_failed_queue_semantics():
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue(
        "callback-sync-failure",
        client=client,
        enable_failed_queue=True,
    )
    assert queue.publish("fail-me") is True

    def handler(message):
        assert message == b"fail-me"
        raise ValueError("handler failed")

    with pytest.raises(ValueError, match="handler failed"):
        queue.process_message_callback(handler)

    assert client.llen(queue.key.pending) == 0
    assert client.llen(queue.key.processing) == 0
    assert client.llen(queue.key.failed) == 1


def test_sync_callback_returning_coroutine_raises_and_leaves_message_reclaimable():
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue(
        "callback-sync-awaitable",
        client=client,
        enable_completed_queue=True,
        enable_failed_queue=True,
        visibility_timeout_seconds=1,
    )
    assert queue.publish("must-run") is True

    def handler(message):
        assert message == b"must-run"
        return asyncio.sleep(0)

    with pytest.raises(TypeError) as caught:
        queue.process_message_callback(handler)

    assert str(caught.value) == SYNC_AWAITABLE_ERROR
    assert client.llen(queue.key.pending) == 0
    assert client.llen(queue.key.processing) == 1
    assert client.llen(queue.key.completed) == 0
    assert client.llen(queue.key.failed) == 0

    time.sleep(1.1)
    reclaimed = []

    assert queue.process_message_callback(lambda message: reclaimed.append(message)) is True

    assert reclaimed == [b"must-run"]
    assert client.llen(queue.key.pending) == 0
    assert client.llen(queue.key.processing) == 0
    assert client.llen(queue.key.completed) == 1
    assert client.llen(queue.key.failed) == 0


def test_sync_callback_rejected_awaitable_cancelled_close_raises_type_error_and_leaves_message_reclaimable():
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue(
        "callback-sync-cancelled-close-awaitable",
        client=client,
        enable_completed_queue=True,
        enable_failed_queue=True,
        visibility_timeout_seconds=30,
    )
    assert queue.publish("must-run") is True

    def handler(message):
        assert message == b"must-run"
        return CancelOnCloseAwaitable()

    with pytest.raises(TypeError) as caught:
        queue.process_message_callback(handler)

    assert str(caught.value) == SYNC_AWAITABLE_ERROR
    assert client.llen(queue.key.pending) == 0
    assert client.llen(queue.key.processing) == 1
    assert client.llen(queue.key.completed) == 0
    assert client.llen(queue.key.failed) == 0


@pytest.mark.asyncio
async def test_async_callback_awaits_async_handler():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue(
        "callback-async-awaits",
        client=client,
        enable_completed_queue=True,
    )
    assert await queue.publish("hello") is True

    seen = []

    async def handler(message):
        await asyncio.sleep(0)
        seen.append(message)

    assert await queue.process_message_callback(handler) is True

    assert seen == [b"hello"]
    assert await client.llen(queue.key.pending) == 0
    assert await client.llen(queue.key.processing) == 0
    assert await client.llen(queue.key.completed) == 1


@pytest.mark.asyncio
async def test_async_callback_accepts_sync_handler():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue(
        "callback-async-sync-handler",
        client=client,
        enable_completed_queue=True,
    )
    assert await queue.publish("hello") is True

    seen = []

    def handler(message):
        seen.append(message)

    assert await queue.process_message_callback(handler) is True

    assert seen == [b"hello"]
    assert await client.llen(queue.key.pending) == 0
    assert await client.llen(queue.key.processing) == 0
    assert await client.llen(queue.key.completed) == 1
