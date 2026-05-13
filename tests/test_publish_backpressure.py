import queue as thread_queue
import threading

import fakeredis
import pytest
import redis

from redis_message_queue import QueueBackpressureError, RedisMessageQueue
from redis_message_queue.asyncio import RedisMessageQueue as AsyncRedisMessageQueue


@pytest.mark.parametrize("deduplication", [True, False])
def test_sync_raise_policy_rejects_overload(deduplication):
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue("bp-sync-raise", client=client, deduplication=deduplication, max_pending_length=1)

    assert queue.publish("first") is True
    with pytest.raises(QueueBackpressureError, match="max_pending_length=1"):
        queue.publish("second")

    assert client.llen(queue.key.pending) == 1


@pytest.mark.parametrize("policy", ["drop_oldest", "block"])
def test_sync_overload_policies(policy):
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue(
        "bp-sync-policy",
        client=client,
        deduplication=False,
        max_pending_length=1,
        pending_overload_policy=policy,
        pending_overload_block_timeout_seconds=0.01,
    )

    assert queue.publish("first") is True
    if policy == "drop_oldest":
        assert queue.publish("second") is True
        expected = b"second"
    else:
        with pytest.raises(QueueBackpressureError, match="stayed at max_pending_length=1"):
            queue.publish("second")
        expected = b"first"
    with queue.process_message() as message:
        assert message == expected
    assert client.llen(queue.key.pending) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("deduplication", [True, False])
async def test_async_raise_policy_rejects_overload(deduplication):
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue("bp-async-raise", client=client, deduplication=deduplication, max_pending_length=1)

    assert await queue.publish("first") is True
    with pytest.raises(QueueBackpressureError, match="max_pending_length=1"):
        await queue.publish("second")

    assert await client.llen(queue.key.pending) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("policy", ["drop_oldest", "block"])
async def test_async_overload_policies(policy):
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue(
        "bp-async-policy",
        client=client,
        deduplication=False,
        max_pending_length=1,
        pending_overload_policy=policy,
        pending_overload_block_timeout_seconds=0.01,
    )

    assert await queue.publish("first") is True
    if policy == "drop_oldest":
        assert await queue.publish("second") is True
        expected = b"second"
    else:
        with pytest.raises(QueueBackpressureError, match="stayed at max_pending_length=1"):
            await queue.publish("second")
        expected = b"first"
    async with queue.process_message() as message:
        assert message == expected
    assert await client.llen(queue.key.pending) == 0


@pytest.mark.integration
def test_pending_limit_is_atomic_for_concurrent_publishers(real_redis_client, real_redis_url, queue_name):
    queue = RedisMessageQueue(queue_name, client=real_redis_client, deduplication=False, max_pending_length=1)
    barrier = threading.Barrier(2)
    outcomes = thread_queue.Queue()

    def publish(index):
        client = redis.Redis.from_url(real_redis_url)
        try:
            local_queue = RedisMessageQueue(queue_name, client=client, deduplication=False, max_pending_length=1)
            barrier.wait(timeout=5)
            local_queue.publish(f"message-{index}")
            outcomes.put("published")
        except QueueBackpressureError:
            outcomes.put("backpressure")
        except BaseException as exc:
            outcomes.put(exc)
        finally:
            client.close()

    threads = [threading.Thread(target=publish, args=(index,)) for index in range(2)]
    [thread.start() for thread in threads]
    [thread.join(timeout=5) for thread in threads]

    results = [outcomes.get_nowait() for _ in range(outcomes.qsize())]
    assert not [result for result in results if isinstance(result, BaseException)]
    assert sorted(results) == ["backpressure", "published"]
    assert real_redis_client.llen(queue.key.pending) == 1
