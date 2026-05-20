"""Tests for the drain()/aclose() graceful-shutdown API (B5 / AA-05-F1/F2).

Covers the four scenarios called out in the round-5 fix bundle:
1. drain after publish-only (no in-flight, no pending claim ids) — returns True.
2. drain mid-processing with VT enabled — returns True and refuses new claims
   without disturbing the in-flight handler.
3. drain with pre-populated _pending_claim_ids — recovers them via the
   existing gateway recovery path.
4. drain with timeout=0 — returns False when pending claim ids exist.
"""

import asyncio
import threading
import time

import fakeredis
import pytest

from redis_message_queue import ConfigurationError, QueueDrainedError, RedisMessageQueue
from redis_message_queue._redis_gateway import RedisGateway
from redis_message_queue._stored_message import encode_stored_message
from redis_message_queue.asyncio import RedisMessageQueue as AsyncRedisMessageQueue
from redis_message_queue.asyncio._redis_gateway import RedisGateway as AsyncRedisGateway


def test_sync_drain_after_publish_only_returns_true():
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue("drain-clean", client=client, deduplication=False)
    queue.publish("hello")

    assert queue.drain() is True
    assert queue._draining is True
    assert queue._drained.is_set() is True


def test_sync_close_alias_after_publish_only_returns_true():
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue("close-clean", client=client, deduplication=False)
    queue.publish("hello")

    assert queue.close() is True
    assert queue._draining is True
    assert queue._drained.is_set() is True


def test_sync_publish_after_drain_raises_queue_drained_error():
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue("drain-publish-refuse", client=client, deduplication=False)
    assert queue.publish("before") is True
    assert "drained=False" in repr(queue)

    assert queue.drain() is True

    assert "drained=True" in repr(queue)
    with pytest.raises(QueueDrainedError, match="queue is drained"):
        queue.publish("after")


def test_sync_drain_is_idempotent_and_keeps_refusing_publish():
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue("drain-idempotent", client=client, deduplication=False)

    assert queue.drain() is True
    assert queue.drain() is True
    with pytest.raises(QueueDrainedError):
        queue.publish("after")


def test_sync_drained_state_is_local_to_queue_instance():
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue("drain-local", client=client, deduplication=False)

    assert queue.drain() is True
    with pytest.raises(QueueDrainedError):
        queue.publish("after")

    fresh_queue = RedisMessageQueue("drain-local", client=client, deduplication=False)
    assert fresh_queue.publish("fresh") is True


def test_sync_drain_waits_for_in_flight_publish_path():
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue("drain-publish-in-flight", client=client, deduplication=False)
    gateway: RedisGateway = queue._redis  # type: ignore[assignment]
    original_add_message = gateway.add_message

    entered_publish = threading.Event()
    release_publish = threading.Event()
    publish_result: list[bool] = []
    drain_result: list[bool] = []

    def controlled_add_message(queue_name: str, message: str) -> None:
        entered_publish.set()
        assert release_publish.wait(timeout=5)
        return original_add_message(queue_name, message)

    gateway.add_message = controlled_add_message  # type: ignore[method-assign]

    publish_thread = threading.Thread(target=lambda: publish_result.append(queue.publish("in-flight")))
    publish_thread.start()
    assert entered_publish.wait(timeout=5)

    drain_thread = threading.Thread(target=lambda: drain_result.append(queue.drain(timeout=1)))
    drain_thread.start()
    drain_thread.join(timeout=0.05)
    assert drain_result == []

    release_publish.set()
    publish_thread.join(timeout=5)
    drain_thread.join(timeout=5)

    assert publish_result == [True]
    assert drain_result == [True]
    with pytest.raises(QueueDrainedError):
        queue.publish("after")


def test_sync_drain_refuses_new_claims_after_call():
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue(
        "drain-refuse",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
    )
    queue.publish("payload")
    queue.drain()

    with queue.process_message() as message:
        assert message is None


def test_sync_drain_interrupts_already_blocked_claim_loop():
    client = fakeredis.FakeRedis()
    gateway = RedisGateway(
        redis_client=client,
        message_wait_interval_seconds=1,
        message_visibility_timeout_seconds=None,
        max_delivery_count=None,
    )
    consumer = RedisMessageQueue("drain-blocked-claim", gateway=gateway, deduplication=False)
    producer = RedisMessageQueue(
        "drain-blocked-claim",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
    )
    original_claim = gateway._claim_message_without_visibility_timeout
    first_poll = threading.Event()

    def controlled_claim(from_queue: str, to_queue: str, *, claim_id: str) -> str | bytes | None:
        first_poll.set()
        return original_claim(from_queue, to_queue, claim_id=claim_id)

    gateway._claim_message_without_visibility_timeout = controlled_claim  # type: ignore[method-assign]

    entered = threading.Event()
    observed: list[str | bytes | None] = []

    def worker() -> None:
        entered.set()
        with consumer.process_message() as message:
            observed.append(message)

    thread = threading.Thread(target=worker)
    thread.start()
    assert entered.wait(timeout=5)
    assert first_poll.wait(timeout=5)

    assert consumer.drain(timeout=1) is True
    assert producer.publish("after-drain") is True

    thread.join(timeout=5)

    assert observed == [None]


def test_sync_drain_mid_processing_completes_and_refuses_followups():
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue(
        "drain-mid",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=30,
    )
    queue.publish("first")

    started = threading.Event()
    allow_finish = threading.Event()
    completed = threading.Event()

    def worker():
        with queue.process_message() as message:
            assert message is not None
            started.set()
            allow_finish.wait(timeout=5)
        completed.set()

    thread = threading.Thread(target=worker)
    thread.start()
    try:
        assert started.wait(timeout=5)
        # Drain while a message is still in flight; no pending claim ids exist
        # because the claim path succeeded, so drain returns True quickly.
        assert queue.drain(timeout=1) is True
        # Follow-up claims yield None even though the worker thread is still
        # inside the prior process_message context.
        with queue.process_message() as second:
            assert second is None
    finally:
        allow_finish.set()
        thread.join(timeout=5)

    assert completed.is_set()


def test_sync_in_flight_handler_publish_during_drain_raises_queue_drained_error():
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue(
        "drain-handler-publish",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=30,
    )
    queue.publish("first")

    started = threading.Event()
    drain_done = threading.Event()
    completed = threading.Event()
    caught: list[type[BaseException]] = []

    def worker():
        with queue.process_message() as message:
            assert message is not None
            started.set()
            assert drain_done.wait(timeout=5)
            try:
                queue.publish("from-handler")
            except QueueDrainedError as exc:
                caught.append(type(exc))
        completed.set()

    thread = threading.Thread(target=worker)
    thread.start()
    try:
        assert started.wait(timeout=5)
        assert queue.drain(timeout=1) is True
        drain_done.set()
    finally:
        drain_done.set()
        thread.join(timeout=5)

    assert caught == [QueueDrainedError]
    assert completed.is_set()


def test_sync_drain_recovers_pre_populated_pending_claim_id():
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue(
        "drain-recover",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
    )
    queue.publish("payload")

    gateway: RedisGateway = queue._redis  # type: ignore[assignment]
    processing_key = queue.key.processing
    # Seed the same cache layout the gateway writes when an ambiguous claim
    # commits server-side but the Python client loses the response: the
    # claim-result-ids hash is the durable backstop the recovery path reads.
    seeded_claim_id = "drain-test-claim-id"
    claim_result_ids_key = gateway._claim_result_ids_key(processing_key)
    client.hset(claim_result_ids_key, seeded_claim_id, b"recovered-payload")
    gateway._set_pending_claim_id(processing_key, seeded_claim_id)

    assert gateway._pending_claim_ids[processing_key] == [seeded_claim_id]

    assert queue.drain(timeout=2) is True
    assert processing_key not in gateway._pending_claim_ids


def test_sync_drain_with_timeout_zero_returns_false_when_pending_remain():
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue(
        "drain-timeout-zero",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
    )
    gateway: RedisGateway = queue._redis  # type: ignore[assignment]
    processing_key = queue.key.processing
    gateway._set_pending_claim_id(processing_key, "stuck-claim-id")

    # timeout=0 takes the gateway's no-recovery fast path (``>=`` deadline
    # check), so the claim id stays put and the call reports False without
    # touching Redis.
    assert queue.drain(timeout=0) is False
    assert gateway._pending_claim_ids[processing_key] == ["stuck-claim-id"]


def test_sync_drain_timeout_bounds_slow_pending_claim_recovery_read():
    class SlowRecoveryClient:
        def __init__(self, latency_seconds: float) -> None:
            self.redis = fakeredis.FakeRedis()
            self.latency_seconds = latency_seconds

        def get(self, key: str) -> bytes | None:
            time.sleep(self.latency_seconds)
            return self.redis.get(key)

        def hget(self, key: str, field: str) -> bytes | None:
            time.sleep(self.latency_seconds)
            return self.redis.hget(key, field)

        def __getattr__(self, name: str) -> object:
            return getattr(self.redis, name)

    timeout_seconds = 0.05
    latency_seconds = 0.35
    client = SlowRecoveryClient(latency_seconds)
    gateway = RedisGateway(
        redis_client=client,  # type: ignore[arg-type]
        retry_budget_seconds=0,
        message_visibility_timeout_seconds=None,
    )
    queue = RedisMessageQueue("drain-slow-recovery-timeout", gateway=gateway)
    processing_key = queue.key.processing
    claim_id = "slow-drain-claim-id"
    client.redis.hset(
        gateway._claim_result_ids_key(processing_key),
        claim_id,
        encode_stored_message("recovered-payload"),
    )
    gateway._set_pending_claim_id(processing_key, claim_id)

    started = time.monotonic()
    result = queue.drain(timeout=timeout_seconds)
    elapsed = time.monotonic() - started

    assert result is False
    assert elapsed < 0.25
    assert gateway._pending_claim_ids[processing_key] == [claim_id]


def test_sync_concurrent_drain_both_return_true():
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue(
        "drain-concurrent",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
    )
    gateway: RedisGateway = queue._redis  # type: ignore[assignment]
    processing_key = queue.key.processing
    seeded_claim_id = "concurrent-drain-claim-id"
    client.hset(gateway._claim_result_ids_key(processing_key), seeded_claim_id, b"recovered-payload")
    gateway._set_pending_claim_id(processing_key, seeded_claim_id)

    original_recover = gateway._recover_pending_non_visibility_timeout_claim
    entered_recovery = threading.Event()
    release_recovery = threading.Event()
    results: dict[str, bool] = {}

    def controlled_recover(
        received_processing_key: str,
        claim_id: str,
        *,
        deadline_monotonic: float | None = None,
    ) -> bytes | str | None:
        assert deadline_monotonic is not None
        assert received_processing_key == processing_key
        assert claim_id == seeded_claim_id
        entered_recovery.set()
        assert release_recovery.wait(timeout=5)
        return original_recover(received_processing_key, claim_id)

    gateway._recover_pending_non_visibility_timeout_claim = controlled_recover  # type: ignore[method-assign]

    first = threading.Thread(target=lambda: results.setdefault("first", queue.drain(timeout=2)))
    first.start()
    assert entered_recovery.wait(timeout=5)

    second = threading.Thread(target=lambda: results.setdefault("second", queue.drain(timeout=2)))
    second.start()
    second.join(timeout=0.05)
    second_waited_for_first = second.is_alive()

    release_recovery.set()
    first.join(timeout=5)
    second.join(timeout=5)

    assert second_waited_for_first is True
    assert results == {"first": True, "second": True}
    assert processing_key not in gateway._pending_claim_ids


def test_sync_drain_rejects_negative_timeout():
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue("drain-validate", client=client, deduplication=False)
    with pytest.raises(ConfigurationError):
        queue.drain(timeout=-1)
    with pytest.raises(TypeError):
        queue.drain(timeout="soon")  # type: ignore[arg-type]


# ---- async parity ----


@pytest.mark.asyncio
async def test_async_aclose_after_publish_only_returns_true():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue("aclose-clean", client=client, deduplication=False)
    await queue.publish("hello")

    assert await queue.aclose() is True
    assert queue._draining is True
    assert queue._drained is True


@pytest.mark.asyncio
async def test_async_drain_alias_after_publish_only_returns_true():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue("async-drain-clean", client=client, deduplication=False)
    await queue.publish("hello")

    assert await queue.drain() is True
    assert queue._draining is True
    assert queue._drained is True


@pytest.mark.asyncio
async def test_async_publish_after_aclose_raises_queue_drained_error():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue("aclose-publish-refuse", client=client, deduplication=False)
    assert await queue.publish("before") is True
    assert "drained=False" in repr(queue)

    assert await queue.aclose() is True

    assert "drained=True" in repr(queue)
    with pytest.raises(QueueDrainedError, match="queue is drained"):
        await queue.publish("after")


@pytest.mark.asyncio
async def test_async_aclose_is_idempotent_and_keeps_refusing_publish():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue("aclose-idempotent", client=client, deduplication=False)

    assert await queue.aclose() is True
    assert await queue.aclose() is True
    with pytest.raises(QueueDrainedError):
        await queue.publish("after")


@pytest.mark.asyncio
async def test_async_drained_state_is_local_to_queue_instance():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue("aclose-local", client=client, deduplication=False)

    assert await queue.aclose() is True
    with pytest.raises(QueueDrainedError):
        await queue.publish("after")

    fresh_queue = AsyncRedisMessageQueue("aclose-local", client=client, deduplication=False)
    assert await fresh_queue.publish("fresh") is True


@pytest.mark.asyncio
async def test_async_aclose_waits_for_in_flight_publish_path():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue("aclose-publish-in-flight", client=client, deduplication=False)
    gateway: AsyncRedisGateway = queue._redis  # type: ignore[assignment]
    original_add_message = gateway.add_message

    entered_publish = asyncio.Event()
    release_publish = asyncio.Event()

    async def controlled_add_message(queue_name: str, message: str) -> None:
        entered_publish.set()
        await release_publish.wait()
        return await original_add_message(queue_name, message)

    gateway.add_message = controlled_add_message  # type: ignore[method-assign]

    publish_task = asyncio.create_task(queue.publish("in-flight"))
    await asyncio.wait_for(entered_publish.wait(), timeout=1)

    drain_task = asyncio.create_task(queue.aclose(timeout=1))
    await asyncio.sleep(0.05)
    assert drain_task.done() is False

    release_publish.set()
    assert await publish_task is True
    assert await drain_task is True
    with pytest.raises(QueueDrainedError):
        await queue.publish("after")


@pytest.mark.asyncio
async def test_async_aclose_refuses_new_claims_after_call():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue(
        "aclose-refuse",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
    )
    await queue.publish("payload")
    await queue.aclose()

    async with queue.process_message() as message:
        assert message is None


@pytest.mark.asyncio
async def test_async_drain_interrupts_already_blocked_claim_loop():
    client = fakeredis.FakeAsyncRedis()
    gateway = AsyncRedisGateway(
        redis_client=client,
        message_wait_interval_seconds=1,
        message_visibility_timeout_seconds=None,
        max_delivery_count=None,
    )
    consumer = AsyncRedisMessageQueue("async-drain-blocked-claim", gateway=gateway, deduplication=False)
    producer = AsyncRedisMessageQueue(
        "async-drain-blocked-claim",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
    )
    original_claim = gateway._claim_message_without_visibility_timeout
    first_poll = asyncio.Event()

    async def controlled_claim(from_queue: str, to_queue: str, *, claim_id: str) -> str | bytes | None:
        first_poll.set()
        return await original_claim(from_queue, to_queue, claim_id=claim_id)

    gateway._claim_message_without_visibility_timeout = controlled_claim  # type: ignore[method-assign]

    entered = asyncio.Event()
    observed: list[str | bytes | None] = []

    async def worker() -> None:
        entered.set()
        async with consumer.process_message() as message:
            observed.append(message)

    task = asyncio.create_task(worker())
    await asyncio.wait_for(entered.wait(), timeout=1)
    await asyncio.wait_for(first_poll.wait(), timeout=1)

    assert await consumer.drain(timeout=1) is True
    assert await producer.publish("after-drain") is True

    await asyncio.wait_for(task, timeout=5)

    assert observed == [None]


@pytest.mark.asyncio
async def test_async_in_flight_handler_publish_during_aclose_raises_queue_drained_error():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue(
        "aclose-handler-publish",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=30,
    )
    await queue.publish("first")

    started = asyncio.Event()
    drain_done = asyncio.Event()
    caught: list[type[BaseException]] = []

    async def worker() -> None:
        async with queue.process_message() as message:
            assert message is not None
            started.set()
            await asyncio.wait_for(drain_done.wait(), timeout=1)
            try:
                await queue.publish("from-handler")
            except QueueDrainedError as exc:
                caught.append(type(exc))

    task = asyncio.create_task(worker())
    await asyncio.wait_for(started.wait(), timeout=1)
    assert await queue.aclose(timeout=1) is True
    drain_done.set()
    await asyncio.wait_for(task, timeout=1)

    assert caught == [QueueDrainedError]


@pytest.mark.asyncio
async def test_async_aclose_recovers_pre_populated_pending_claim_id():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue(
        "aclose-recover",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
    )
    await queue.publish("payload")

    gateway: AsyncRedisGateway = queue._redis  # type: ignore[assignment]
    processing_key = queue.key.processing
    seeded_claim_id = "aclose-test-claim-id"
    claim_result_ids_key = gateway._claim_result_ids_key(processing_key)
    await client.hset(claim_result_ids_key, seeded_claim_id, b"recovered-payload")
    gateway._set_pending_claim_id(processing_key, seeded_claim_id)

    assert gateway._pending_claim_ids[processing_key] == [seeded_claim_id]

    assert await queue.aclose(timeout=2) is True
    assert processing_key not in gateway._pending_claim_ids


@pytest.mark.asyncio
async def test_async_aclose_with_timeout_zero_returns_false_when_pending_remain():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue(
        "aclose-timeout-zero",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
    )
    gateway: AsyncRedisGateway = queue._redis  # type: ignore[assignment]
    processing_key = queue.key.processing
    gateway._set_pending_claim_id(processing_key, "stuck-claim-id")

    # timeout=0 takes the gateway's no-recovery fast path (``>=`` deadline
    # check) and the seeded claim id remains untouched.
    result = await queue.aclose(timeout=0)
    assert result is False
    assert gateway._pending_claim_ids[processing_key] == ["stuck-claim-id"]


@pytest.mark.asyncio
async def test_async_aclose_timeout_bounds_slow_pending_claim_recovery_read():
    class SlowAsyncRecoveryClient:
        def __init__(self, latency_seconds: float) -> None:
            self.redis = fakeredis.FakeAsyncRedis()
            self.latency_seconds = latency_seconds

        async def get(self, key: str) -> bytes | None:
            await asyncio.sleep(self.latency_seconds)
            return await self.redis.get(key)

        async def hget(self, key: str, field: str) -> bytes | None:
            await asyncio.sleep(self.latency_seconds)
            return await self.redis.hget(key, field)

        def __getattr__(self, name: str) -> object:
            return getattr(self.redis, name)

    timeout_seconds = 0.05
    latency_seconds = 0.35
    client = SlowAsyncRecoveryClient(latency_seconds)
    gateway = AsyncRedisGateway(
        redis_client=client,  # type: ignore[arg-type]
        retry_budget_seconds=0,
        message_visibility_timeout_seconds=None,
    )
    queue = AsyncRedisMessageQueue("aclose-slow-recovery-timeout", gateway=gateway)
    processing_key = queue.key.processing
    claim_id = "slow-async-drain-claim-id"
    await client.redis.hset(
        gateway._claim_result_ids_key(processing_key),
        claim_id,
        encode_stored_message("recovered-payload"),
    )
    gateway._set_pending_claim_id(processing_key, claim_id)

    started = time.monotonic()
    result = await queue.aclose(timeout=timeout_seconds)
    elapsed = time.monotonic() - started

    assert result is False
    assert elapsed < 0.25
    assert gateway._pending_claim_ids[processing_key] == [claim_id]


@pytest.mark.asyncio
async def test_async_aclose_after_timeout_can_retry():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue(
        "aclose-timeout-retry",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
    )
    gateway: AsyncRedisGateway = queue._redis  # type: ignore[assignment]
    processing_key = queue.key.processing
    drain_calls = 0

    async def controlled_drainer(
        received_processing_key: str,
        *,
        deadline_monotonic: float | None,
    ) -> bool:
        nonlocal drain_calls
        assert received_processing_key == processing_key
        drain_calls += 1
        if drain_calls == 1:
            assert deadline_monotonic is not None
            return False
        assert deadline_monotonic is not None
        return True

    gateway._drain_pending_claim_ids = controlled_drainer  # type: ignore[method-assign]

    assert await queue.aclose(timeout=0) is False
    assert await queue.aclose(timeout=10) is True
    assert drain_calls == 2
    assert await queue.aclose(timeout=10) is True
    assert drain_calls == 2


@pytest.mark.asyncio
async def test_async_concurrent_aclose_returns_cached_result_and_drains_once():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue(
        "aclose-concurrent",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
    )
    gateway: AsyncRedisGateway = queue._redis  # type: ignore[assignment]
    processing_key = queue.key.processing

    entered_first_drain = asyncio.Event()
    release_first_drain = asyncio.Event()
    drain_calls = 0

    async def controlled_drainer(
        received_processing_key: str,
        *,
        deadline_monotonic: float | None,
    ) -> bool:
        nonlocal drain_calls
        assert received_processing_key == processing_key
        assert deadline_monotonic is not None
        drain_calls += 1
        if drain_calls == 1:
            entered_first_drain.set()
            await release_first_drain.wait()
            return True
        return False

    gateway._drain_pending_claim_ids = controlled_drainer  # type: ignore[method-assign]

    first = asyncio.create_task(queue.aclose(timeout=1))
    await asyncio.wait_for(entered_first_drain.wait(), timeout=1)
    second = asyncio.create_task(queue.aclose(timeout=0))
    await asyncio.sleep(0)

    release_first_drain.set()
    results = await asyncio.gather(first, second)

    assert results == [True, True]
    assert drain_calls == 1


@pytest.mark.asyncio
async def test_async_aclose_preserves_cleanup_on_cancellation():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue(
        "aclose-cancel-cleanup",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=30,
    )
    gateway: AsyncRedisGateway = queue._redis  # type: ignore[assignment]
    processing_key = queue.key.processing
    seeded_claim_id = "cancel-cleanup-claim-id"
    gateway._set_pending_claim_id(processing_key, seeded_claim_id)

    entered_drain = asyncio.Event()
    release_drain = asyncio.Future()

    async def controlled_drainer(
        received_processing_key: str,
        *,
        deadline_monotonic: float | None,
    ) -> bool:
        assert received_processing_key == processing_key
        assert deadline_monotonic is None
        entered_drain.set()
        await release_drain
        gateway._pending_claim_ids.pop(processing_key, None)
        return True

    gateway._drain_pending_claim_ids = controlled_drainer  # type: ignore[method-assign]

    task = asyncio.create_task(queue.aclose())
    await asyncio.wait_for(entered_drain.wait(), timeout=1)

    task.cancel()
    await asyncio.sleep(0)
    release_drain.set_result(None)

    with pytest.raises(asyncio.CancelledError):
        await task

    assert task.cancelled() is True
    assert processing_key not in gateway._pending_claim_ids


@pytest.mark.asyncio
async def test_async_aclose_rejects_negative_timeout():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue("aclose-validate", client=client, deduplication=False)
    with pytest.raises(ConfigurationError):
        await queue.aclose(timeout=-1)
    with pytest.raises(TypeError):
        await queue.aclose(timeout="soon")  # type: ignore[arg-type]
