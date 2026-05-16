import queue as thread_queue
import threading

import fakeredis
import pytest
import redis

from redis_message_queue import ConfigurationError, QueueBackpressureError, RedisMessageQueue
from redis_message_queue import _redis_gateway as sync_gateway_module
from redis_message_queue.asyncio import RedisMessageQueue as AsyncRedisMessageQueue
from redis_message_queue.asyncio import _redis_gateway as async_gateway_module

DROP_OLDEST_DEDUP_MATCH = "drop_oldest.*deduplication.*silently suppressed"


def _run_sync_block_wait(monkeypatch, block_timeout_seconds):
    gateway = sync_gateway_module.RedisGateway(
        redis_client=fakeredis.FakeRedis(),
        max_pending_length=1,
        pending_overload_policy="block",
        pending_overload_block_timeout_seconds=block_timeout_seconds,
    )
    now = 0.0
    polls = 0
    sleeps = []

    def fake_monotonic():
        return now

    def fake_sleep(duration):
        nonlocal now
        sleeps.append(duration)
        now += duration

    def overloaded_operation():
        nonlocal polls
        polls += 1
        return sync_gateway_module.PENDING_OVERLOAD_LUA_SENTINEL

    monkeypatch.setattr(sync_gateway_module.random, "random", lambda: 0.5)
    monkeypatch.setattr(sync_gateway_module.time, "monotonic", fake_monotonic)
    monkeypatch.setattr(sync_gateway_module.time, "sleep", fake_sleep)

    with pytest.raises(QueueBackpressureError, match="stayed at max_pending_length=1"):
        gateway._run_pending_backpressure_operation("pending", overloaded_operation)

    return polls, sleeps, now


class _FakeLoop:
    def __init__(self):
        self.now = 0.0

    def time(self):
        return self.now


async def _run_async_block_wait(monkeypatch, block_timeout_seconds):
    gateway = async_gateway_module.RedisGateway(
        redis_client=fakeredis.FakeAsyncRedis(),
        max_pending_length=1,
        pending_overload_policy="block",
        pending_overload_block_timeout_seconds=block_timeout_seconds,
    )
    fake_loop = _FakeLoop()
    polls = 0
    sleeps = []

    async def fake_sleep(duration):
        sleeps.append(duration)
        fake_loop.now += duration

    async def overloaded_operation():
        nonlocal polls
        polls += 1
        return async_gateway_module.PENDING_OVERLOAD_LUA_SENTINEL

    monkeypatch.setattr(async_gateway_module.random, "random", lambda: 0.5)
    monkeypatch.setattr(async_gateway_module.asyncio, "get_running_loop", lambda: fake_loop)
    monkeypatch.setattr(async_gateway_module.asyncio, "sleep", fake_sleep)

    with pytest.raises(QueueBackpressureError, match="stayed at max_pending_length=1"):
        await gateway._run_pending_backpressure_operation("pending", overloaded_operation)

    return polls, sleeps, fake_loop.now


@pytest.mark.parametrize("deduplication", [True, False])
def test_sync_raise_policy_rejects_overload(deduplication):
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue("bp-sync-raise", client=client, deduplication=deduplication, max_pending_length=1)

    assert queue.publish("first") is True
    with pytest.raises(QueueBackpressureError, match="max_pending_length=1") as caught:
        queue.publish("second")
    assert "consider increasing `max_pending_length`" in str(caught.value)
    assert "pending_overload_policy='block'" in str(caught.value)
    assert "adding consumer capacity" in str(caught.value)

    assert client.llen(queue.key.pending) == 1


@pytest.mark.parametrize("policy", ["drop_oldest", "block"])
def test_sync_overload_policies(policy):
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue(
        "bp-sync-policy",
        client=client,
        deduplication=False,
        max_delivery_count=None,
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


@pytest.mark.parametrize(
    "kwargs",
    [
        {},
        {"get_deduplication_key": None},
        {"get_deduplication_key": lambda message: "fixed"},
        {"deduplication": False, "get_deduplication_key": lambda message: "fixed"},
    ],
)
def test_sync_drop_oldest_rejects_deduplication(kwargs):
    client = fakeredis.FakeRedis()

    with pytest.raises(ConfigurationError, match=DROP_OLDEST_DEDUP_MATCH):
        RedisMessageQueue(
            "bp-sync-drop-oldest-dedup",
            client=client,
            max_pending_length=1,
            pending_overload_policy="drop_oldest",
            **kwargs,
        )


def test_sync_drop_oldest_requires_pending_cap():
    client = fakeredis.FakeRedis()

    with pytest.raises(ConfigurationError, match="drop_oldest requires max_pending_length to be set"):
        RedisMessageQueue(
            "bp-sync-drop-oldest-no-cap",
            client=client,
            deduplication=False,
            max_delivery_count=None,
            pending_overload_policy="drop_oldest",
        )


def test_sync_drop_oldest_rejects_max_delivery_count():
    client = fakeredis.FakeRedis()

    with pytest.raises(ConfigurationError, match="drop_oldest is incompatible with max_delivery_count"):
        RedisMessageQueue(
            "bp-sync-drop-oldest-dlq",
            client=client,
            deduplication=False,
            max_pending_length=1,
            max_delivery_count=1,
            pending_overload_policy="drop_oldest",
        )


def test_sync_gateway_drop_oldest_rejects_max_delivery_count():
    client = fakeredis.FakeRedis()

    with pytest.raises(ConfigurationError, match="drop_oldest is incompatible with max_delivery_count"):
        sync_gateway_module.RedisGateway(
            redis_client=client,
            max_pending_length=1,
            max_delivery_count=1,
            dead_letter_queue="bp-sync-gateway-drop-oldest-dlq:dead",
            message_visibility_timeout_seconds=10,
            pending_overload_policy="drop_oldest",
        )


def test_sync_block_policy_backs_off_during_extended_wait(monkeypatch):
    polls, sleeps, elapsed = _run_sync_block_wait(monkeypatch, 0.5)

    assert elapsed == pytest.approx(0.5)
    assert len(sleeps) == polls - 1
    assert polls < 20
    assert sleeps[:4] == pytest.approx([0.01, 0.02, 0.04, 0.05])


def test_sync_block_policy_caps_backoff_to_timeout_fraction_and_500ms(monkeypatch):
    _, tight_sleeps, _ = _run_sync_block_wait(monkeypatch, 0.1)
    _, wide_sleeps, _ = _run_sync_block_wait(monkeypatch, 10.0)

    assert max(tight_sleeps) == pytest.approx(0.01)
    assert max(wide_sleeps) == pytest.approx(0.5)


def test_sync_block_policy_zero_timeout_tries_once(monkeypatch):
    polls, sleeps, elapsed = _run_sync_block_wait(monkeypatch, 0)

    assert polls == 1
    assert sleeps == []
    assert elapsed == 0


@pytest.mark.parametrize("timeout", [-1, float("nan"), float("inf")])
def test_sync_rejects_invalid_block_timeout(timeout):
    client = fakeredis.FakeRedis()

    with pytest.raises(ConfigurationError, match="pending_overload_block_timeout_seconds"):
        RedisMessageQueue(
            "bp-sync-invalid-timeout",
            client=client,
            pending_overload_block_timeout_seconds=timeout,
        )


@pytest.mark.parametrize("max_pending_length", [0, -1])
def test_sync_rejects_non_positive_pending_cap(max_pending_length):
    client = fakeredis.FakeRedis()

    with pytest.raises(ConfigurationError, match="max_pending_length"):
        RedisMessageQueue(
            "bp-sync-invalid-cap",
            client=client,
            max_pending_length=max_pending_length,
        )


def test_sync_full_queue_dedup_hit_does_not_count_as_overload():
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue("bp-sync-dedup-hit", client=client, max_pending_length=1)

    assert queue.publish("same") is True
    assert queue.publish("same") is False
    assert client.llen(queue.key.pending) == 1


def test_sync_backpressure_emits_failure_event_before_reraising():
    client = fakeredis.FakeRedis()
    events = []
    queue = RedisMessageQueue(
        "bp-sync-event",
        client=client,
        deduplication=False,
        max_pending_length=1,
        on_event=events.append,
    )

    assert queue.publish("first") is True
    with pytest.raises(QueueBackpressureError):
        queue.publish("second")

    assert events[-1].operation == "publish"
    assert events[-1].outcome == "failure"
    assert events[-1].exception_type == "QueueBackpressureError"


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
        max_delivery_count=None,
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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kwargs",
    [
        {},
        {"get_deduplication_key": None},
        {"get_deduplication_key": lambda message: "fixed"},
        {"deduplication": False, "get_deduplication_key": lambda message: "fixed"},
    ],
)
async def test_async_drop_oldest_rejects_deduplication(kwargs):
    client = fakeredis.FakeAsyncRedis()

    with pytest.raises(ConfigurationError, match=DROP_OLDEST_DEDUP_MATCH):
        AsyncRedisMessageQueue(
            "bp-async-drop-oldest-dedup",
            client=client,
            max_pending_length=1,
            pending_overload_policy="drop_oldest",
            **kwargs,
        )


@pytest.mark.asyncio
async def test_async_drop_oldest_requires_pending_cap():
    client = fakeredis.FakeAsyncRedis()

    with pytest.raises(ConfigurationError, match="drop_oldest requires max_pending_length to be set"):
        AsyncRedisMessageQueue(
            "bp-async-drop-oldest-no-cap",
            client=client,
            deduplication=False,
            max_delivery_count=None,
            pending_overload_policy="drop_oldest",
        )


@pytest.mark.asyncio
async def test_async_drop_oldest_rejects_max_delivery_count():
    client = fakeredis.FakeAsyncRedis()

    with pytest.raises(ConfigurationError, match="drop_oldest is incompatible with max_delivery_count"):
        AsyncRedisMessageQueue(
            "bp-async-drop-oldest-dlq",
            client=client,
            deduplication=False,
            max_pending_length=1,
            max_delivery_count=1,
            pending_overload_policy="drop_oldest",
        )


def test_async_gateway_drop_oldest_rejects_max_delivery_count():
    client = fakeredis.FakeAsyncRedis()

    with pytest.raises(ConfigurationError, match="drop_oldest is incompatible with max_delivery_count"):
        async_gateway_module.RedisGateway(
            redis_client=client,
            max_pending_length=1,
            max_delivery_count=1,
            dead_letter_queue="bp-async-gateway-drop-oldest-dlq:dead",
            message_visibility_timeout_seconds=10,
            pending_overload_policy="drop_oldest",
        )


@pytest.mark.asyncio
async def test_async_block_policy_backs_off_during_extended_wait(monkeypatch):
    polls, sleeps, elapsed = await _run_async_block_wait(monkeypatch, 0.5)

    assert elapsed == pytest.approx(0.5)
    assert len(sleeps) == polls - 1
    assert polls < 20
    assert sleeps[:4] == pytest.approx([0.01, 0.02, 0.04, 0.05])


@pytest.mark.asyncio
async def test_async_block_policy_caps_backoff_to_timeout_fraction_and_500ms(monkeypatch):
    _, tight_sleeps, _ = await _run_async_block_wait(monkeypatch, 0.1)
    _, wide_sleeps, _ = await _run_async_block_wait(monkeypatch, 10.0)

    assert max(tight_sleeps) == pytest.approx(0.01)
    assert max(wide_sleeps) == pytest.approx(0.5)


@pytest.mark.asyncio
async def test_async_block_policy_zero_timeout_tries_once(monkeypatch):
    polls, sleeps, elapsed = await _run_async_block_wait(monkeypatch, 0)

    assert polls == 1
    assert sleeps == []
    assert elapsed == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("timeout", [-1, float("nan"), float("inf")])
async def test_async_rejects_invalid_block_timeout(timeout):
    client = fakeredis.FakeAsyncRedis()

    with pytest.raises(ConfigurationError, match="pending_overload_block_timeout_seconds"):
        AsyncRedisMessageQueue(
            "bp-async-invalid-timeout",
            client=client,
            pending_overload_block_timeout_seconds=timeout,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("max_pending_length", [0, -1])
async def test_async_rejects_non_positive_pending_cap(max_pending_length):
    client = fakeredis.FakeAsyncRedis()

    with pytest.raises(ConfigurationError, match="max_pending_length"):
        AsyncRedisMessageQueue(
            "bp-async-invalid-cap",
            client=client,
            max_pending_length=max_pending_length,
        )


@pytest.mark.asyncio
async def test_async_full_queue_dedup_hit_does_not_count_as_overload():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue("bp-async-dedup-hit", client=client, max_pending_length=1)

    assert await queue.publish("same") is True
    assert await queue.publish("same") is False
    assert await client.llen(queue.key.pending) == 1


@pytest.mark.asyncio
async def test_async_backpressure_emits_failure_event_before_reraising():
    client = fakeredis.FakeAsyncRedis()
    events = []

    async def on_event(event):
        events.append(event)

    queue = AsyncRedisMessageQueue(
        "bp-async-event",
        client=client,
        deduplication=False,
        max_pending_length=1,
        on_event=on_event,
    )

    assert await queue.publish("first") is True
    with pytest.raises(QueueBackpressureError):
        await queue.publish("second")

    assert events[-1].operation == "publish"
    assert events[-1].outcome == "failure"
    assert events[-1].exception_type == "QueueBackpressureError"


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
