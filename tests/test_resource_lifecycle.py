from uuid import uuid4

import fakeredis
import pytest

from redis_message_queue import RedisMessageQueue
from redis_message_queue.asyncio import RedisMessageQueue as AsyncRedisMessageQueue

N_TRANSIENT_QUEUES = 10


def _counter_pattern(prefix: str) -> str:
    return f"{prefix}*lease_token_counter"


def test_sync_transient_queue_drain_removes_lease_token_counter_metadata():
    client = fakeredis.FakeRedis()
    prefix = f"ad16-f1-sync-{uuid4().hex}"

    for i in range(N_TRANSIENT_QUEUES):
        queue = RedisMessageQueue(f"{prefix}-{i}", client=client)
        queue.publish("payload")

        with queue.process_message() as message:
            assert message == b"payload"

        assert len(client.keys(_counter_pattern(prefix))) == 1
        assert queue.drain() is True
        assert len(client.keys(_counter_pattern(prefix))) == 0


@pytest.mark.asyncio
async def test_async_transient_queue_drain_removes_lease_token_counter_metadata():
    client = fakeredis.FakeAsyncRedis()
    prefix = f"ad16-f1-async-{uuid4().hex}"

    for i in range(N_TRANSIENT_QUEUES):
        queue = AsyncRedisMessageQueue(f"{prefix}-{i}", client=client)
        await queue.publish("payload")

        async with queue.process_message() as message:
            assert message == b"payload"

        assert len(await client.keys(_counter_pattern(prefix))) == 1
        assert await queue.aclose() is True
        assert len(await client.keys(_counter_pattern(prefix))) == 0


def test_sync_stable_queue_reuse_after_counter_cleanup_still_processes_messages():
    client = fakeredis.FakeRedis()
    queue_name = f"ad16-f1-stable-sync-{uuid4().hex}"

    queue = RedisMessageQueue(queue_name, client=client)
    queue.publish("first")
    with queue.process_message() as message:
        assert message == b"first"
    assert queue.drain() is True
    assert len(client.keys(_counter_pattern(queue_name))) == 0

    fresh_queue = RedisMessageQueue(queue_name, client=client)
    fresh_queue.publish("second")
    with fresh_queue.process_message() as message:
        assert message == b"second"
    assert len(client.keys(_counter_pattern(queue_name))) == 1
    assert fresh_queue.drain() is True
    assert len(client.keys(_counter_pattern(queue_name))) == 0


@pytest.mark.asyncio
async def test_async_stable_queue_reuse_after_counter_cleanup_still_processes_messages():
    client = fakeredis.FakeAsyncRedis()
    queue_name = f"ad16-f1-stable-async-{uuid4().hex}"

    queue = AsyncRedisMessageQueue(queue_name, client=client)
    await queue.publish("first")
    async with queue.process_message() as message:
        assert message == b"first"
    assert await queue.aclose() is True
    assert len(await client.keys(_counter_pattern(queue_name))) == 0

    fresh_queue = AsyncRedisMessageQueue(queue_name, client=client)
    await fresh_queue.publish("second")
    async with fresh_queue.process_message() as message:
        assert message == b"second"
    assert len(await client.keys(_counter_pattern(queue_name))) == 1
    assert await fresh_queue.aclose() is True
    assert len(await client.keys(_counter_pattern(queue_name))) == 0
