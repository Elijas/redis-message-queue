"""Async integration tests for blocking BLMOVE and VT polling loop paths against real Redis.

Async mirror of test_integration_blocking_sync.py. See that file's docstring for
the full rationale on fakeredis vs real Redis behavioral gaps.
"""

import asyncio
import json
import time
from uuid import uuid4

import pytest
import redis.asyncio

from redis_message_queue.asyncio._redis_gateway import RedisGateway
from redis_message_queue.asyncio.redis_message_queue import RedisMessageQueue


def _no_retry(func):
    return func


async def _async_cleanup_keys(redis_client, prefix):
    cursor = 0
    while True:
        cursor, keys = await redis_client.scan(cursor=cursor, match=f"{prefix}*", count=100)
        if keys:
            await redis_client.delete(*keys)
        if cursor == 0:
            break


pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Blocking BLMOVE path (asyncio/_redis_gateway.py:210-216)
# ---------------------------------------------------------------------------


class TestBlmoveBlockingPath:
    """Tests for the blmove() branch: message_wait_interval > 0, no visibility timeout."""

    @pytest.mark.asyncio
    async def test_blmove_returns_message_when_already_available(self, real_async_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_async_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=1,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway, deduplication=False)
        await queue.publish("hello")

        start = time.monotonic()
        result = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        elapsed = time.monotonic() - start

        assert result is not None
        assert elapsed < 0.5

    @pytest.mark.asyncio
    async def test_blmove_blocks_then_receives_late_publish(self, real_async_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_async_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=3,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway, deduplication=False)

        async def consumer():
            return await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)

        start = time.monotonic()
        task = asyncio.create_task(consumer())
        await asyncio.sleep(0.5)
        await queue.publish("late-msg")
        result = await asyncio.wait_for(task, timeout=5)
        elapsed = time.monotonic() - start

        assert result is not None
        assert elapsed < 2.0

    @pytest.mark.asyncio
    async def test_blmove_returns_none_on_timeout(self, real_async_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_async_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=1,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway)

        start = time.monotonic()
        result = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        elapsed = time.monotonic() - start

        assert result is None
        assert elapsed >= 0.8

    @pytest.mark.asyncio
    async def test_blmove_fifo_ordering_preserved(self, real_async_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_async_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=1,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway, deduplication=False)
        for msg in ["a", "b", "c"]:
            await queue.publish(msg)

        consumed = []
        for _ in range(3):
            async with queue.process_message() as msg:
                consumed.append(msg)

        assert consumed == [b"a", b"b", b"c"]

    @pytest.mark.asyncio
    async def test_blmove_multiple_consumers_no_double_delivery(self, real_async_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_async_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=1,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway, deduplication=False)
        n = 10
        for i in range(n):
            await queue.publish(f"msg-{i}")

        async def consumer():
            return await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)

        results = await asyncio.gather(*[consumer() for _ in range(n)])
        non_none = [r for r in results if r is not None]

        assert len(non_none) == n
        assert len(set(non_none)) == n


# ---------------------------------------------------------------------------
# Visibility-timeout polling loop (asyncio/_redis_gateway.py:218-237)
# ---------------------------------------------------------------------------


class TestVisibilityTimeoutPollingLoop:
    """Tests for the 0.25s poll loop: message_wait_interval > 0 AND visibility_timeout > 0."""

    @pytest.mark.asyncio
    async def test_vt_polling_returns_message_when_available(self, real_async_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_async_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=1,
            message_visibility_timeout_seconds=2,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway)
        await queue.publish("hello")

        start = time.monotonic()
        result = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        elapsed = time.monotonic() - start

        assert result is not None
        assert elapsed < 0.5

    @pytest.mark.asyncio
    async def test_vt_polling_detects_late_publish(self, real_async_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_async_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=3,
            message_visibility_timeout_seconds=2,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway, deduplication=False)

        async def consumer():
            return await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)

        start = time.monotonic()
        task = asyncio.create_task(consumer())
        await asyncio.sleep(0.5)
        await queue.publish("late-msg")
        result = await asyncio.wait_for(task, timeout=5)
        elapsed = time.monotonic() - start

        assert result is not None
        assert elapsed < 2.0

    @pytest.mark.asyncio
    async def test_vt_polling_reclaims_expired_during_wait(self, real_async_redis_client, queue_name):
        first_gateway = RedisGateway(
            redis_client=real_async_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=1,
        )
        queue = RedisMessageQueue(queue_name, gateway=first_gateway)
        await queue.publish("hello")

        first = await first_gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert first is not None

        await asyncio.sleep(1.5)

        second_gateway = RedisGateway(
            redis_client=real_async_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=3,
            message_visibility_timeout_seconds=1,
        )
        start = time.monotonic()
        second = await second_gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        elapsed = time.monotonic() - start

        assert second is not None
        assert second.stored_message == first.stored_message
        assert second.lease_token != first.lease_token
        assert elapsed < 1.0

    @pytest.mark.asyncio
    async def test_vt_polling_returns_none_on_timeout(self, real_async_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_async_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=1,
            message_visibility_timeout_seconds=2,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway)

        start = time.monotonic()
        result = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        elapsed = time.monotonic() - start

        assert result is None
        assert elapsed >= 0.8

    @pytest.mark.asyncio
    async def test_vt_polling_multiple_consumers_no_double_delivery(self, real_async_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_async_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=1,
            message_visibility_timeout_seconds=10,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway, deduplication=False)
        n = 10
        for i in range(n):
            await queue.publish(f"msg-{i}")

        async def consumer():
            r = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
            if r is not None:
                return r.stored_message
            return None

        results = await asyncio.gather(*[consumer() for _ in range(n)])
        non_none = [r for r in results if r is not None]

        assert len(non_none) == n
        assert len(set(non_none)) == n


# ---------------------------------------------------------------------------
# Dict message JSON round-trip through real Redis bytes
# ---------------------------------------------------------------------------


class TestDictMessageRoundTrip:
    @pytest.mark.asyncio
    async def test_dict_publish_consume_round_trip(self, real_async_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_async_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway)
        original = {"key": "value", "count": 42}
        await queue.publish(original)

        async with queue.process_message() as msg:
            assert msg is not None
            parsed = json.loads(msg)
            assert parsed == original

    @pytest.mark.asyncio
    async def test_nested_dict_round_trip(self, real_async_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_async_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway)
        original = {"outer": {"inner": [1, 2, 3]}, "list": [{"a": 1}, {"b": 2}]}
        await queue.publish(original)

        async with queue.process_message() as msg:
            assert msg is not None
            parsed = json.loads(msg)
            assert parsed == original

    @pytest.mark.asyncio
    async def test_unicode_dict_round_trip(self, real_async_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_async_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway)
        original = {"emoji": "\U0001f389", "japanese": "\u65e5\u672c\u8a9e", "math": "\u2211\u222b\u2202"}
        await queue.publish(original)

        async with queue.process_message() as msg:
            assert msg is not None
            parsed = json.loads(msg)
            assert parsed == original


# ---------------------------------------------------------------------------
# Cross-queue isolation (two queues sharing one Redis)
# ---------------------------------------------------------------------------


class TestCrossQueueIsolation:
    @pytest.mark.asyncio
    async def test_two_queues_do_not_interfere(self, real_async_redis_client, queue_name):
        second_name = f"rmq-integ-{uuid4().hex[:12]}"
        gateway = RedisGateway(
            redis_client=real_async_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
        )
        queue_a = RedisMessageQueue(queue_name, gateway=gateway, deduplication=False)
        queue_b = RedisMessageQueue(second_name, gateway=gateway, deduplication=False)

        try:
            await queue_a.publish("msg-a")
            await queue_b.publish("msg-b")

            async with queue_a.process_message() as msg_a:
                assert msg_a == b"msg-a"
            async with queue_b.process_message() as msg_b:
                assert msg_b == b"msg-b"

            async with queue_a.process_message() as msg:
                assert msg is None
            async with queue_b.process_message() as msg:
                assert msg is None
        finally:
            await _async_cleanup_keys(real_async_redis_client, second_name)

    @pytest.mark.asyncio
    async def test_cross_queue_isolation_with_visibility_timeout(self, real_async_redis_client, queue_name):
        second_name = f"rmq-integ-{uuid4().hex[:12]}"
        gateway = RedisGateway(
            redis_client=real_async_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=10,
        )
        queue_a = RedisMessageQueue(queue_name, gateway=gateway)
        queue_b = RedisMessageQueue(second_name, gateway=gateway)

        try:
            await queue_a.publish("msg-a")
            await queue_b.publish("msg-b")

            claimed_a = await gateway.wait_for_message_and_move(queue_a.key.pending, queue_a.key.processing)
            claimed_b = await gateway.wait_for_message_and_move(queue_b.key.pending, queue_b.key.processing)
            assert claimed_a is not None and claimed_b is not None

            lease_deadlines_a = f"{queue_a.key.processing}:lease_deadlines"
            lease_deadlines_b = f"{queue_b.key.processing}:lease_deadlines"
            assert await real_async_redis_client.zcard(lease_deadlines_a) == 1
            assert await real_async_redis_client.zcard(lease_deadlines_b) == 1

            assert await real_async_redis_client.llen(queue_a.key.processing) == 1
            assert await real_async_redis_client.llen(queue_b.key.processing) == 1
        finally:
            await _async_cleanup_keys(real_async_redis_client, second_name)


# ---------------------------------------------------------------------------
# Heartbeat asyncio task lifecycle with real Redis
# ---------------------------------------------------------------------------


class TestHeartbeatTaskLifecycle:
    @pytest.mark.asyncio
    async def test_heartbeat_task_terminates_after_success(self, real_async_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_async_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=2,
        )
        queue = RedisMessageQueue(
            queue_name,
            gateway=gateway,
            heartbeat_interval_seconds=0.3,
        )
        await queue.publish("hello")

        async with queue.process_message() as msg:
            assert msg == b"hello"
            heartbeat_tasks = [t for t in asyncio.all_tasks() if t.get_name() == "redis-message-queue-lease-heartbeat"]
            assert len(heartbeat_tasks) == 1

        await asyncio.sleep(0.1)
        heartbeat_tasks = [t for t in asyncio.all_tasks() if t.get_name() == "redis-message-queue-lease-heartbeat"]
        assert len(heartbeat_tasks) == 0

    @pytest.mark.asyncio
    async def test_heartbeat_task_terminates_after_failure(self, real_async_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_async_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=2,
        )
        queue = RedisMessageQueue(
            queue_name,
            gateway=gateway,
            heartbeat_interval_seconds=0.3,
        )
        await queue.publish("hello")

        with pytest.raises(ValueError):
            async with queue.process_message() as msg:
                assert msg == b"hello"
                heartbeat_tasks = [
                    t for t in asyncio.all_tasks() if t.get_name() == "redis-message-queue-lease-heartbeat"
                ]
                assert len(heartbeat_tasks) == 1
                raise ValueError("boom")

        await asyncio.sleep(0.1)
        heartbeat_tasks = [t for t in asyncio.all_tasks() if t.get_name() == "redis-message-queue-lease-heartbeat"]
        assert len(heartbeat_tasks) == 0


# ---------------------------------------------------------------------------
# Queue state persistence across client reconnection
# ---------------------------------------------------------------------------


class TestStatePersistenceAcrossReconnection:
    @pytest.mark.asyncio
    async def test_queue_state_survives_reconnect(self, real_async_redis_client, real_redis_url, queue_name):
        gateway_a = RedisGateway(
            redis_client=real_async_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
        )
        queue_a = RedisMessageQueue(queue_name, gateway=gateway_a, deduplication=False)
        await queue_a.publish("hello")

        client_b = redis.asyncio.Redis.from_url(real_redis_url)
        try:
            gateway_b = RedisGateway(
                redis_client=client_b,
                retry_strategy=_no_retry,
                message_wait_interval_seconds=0,
            )
            queue_b = RedisMessageQueue(queue_name, gateway=gateway_b, deduplication=False)
            async with queue_b.process_message() as msg:
                assert msg == b"hello"
        finally:
            await client_b.aclose()

    @pytest.mark.asyncio
    async def test_lease_metadata_survives_reconnect(self, real_async_redis_client, real_redis_url, queue_name):
        gateway_a = RedisGateway(
            redis_client=real_async_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=10,
        )
        queue_a = RedisMessageQueue(queue_name, gateway=gateway_a)
        await queue_a.publish("hello")

        claimed = await gateway_a.wait_for_message_and_move(queue_a.key.pending, queue_a.key.processing)
        assert claimed is not None

        client_b = redis.asyncio.Redis.from_url(real_redis_url)
        try:
            gateway_b = RedisGateway(
                redis_client=client_b,
                retry_strategy=_no_retry,
                message_wait_interval_seconds=0,
                message_visibility_timeout_seconds=10,
            )
            renewed = await gateway_b.renew_message_lease(
                queue_a.key.processing, claimed.stored_message, claimed.lease_token
            )
            assert renewed is True
        finally:
            await client_b.aclose()
