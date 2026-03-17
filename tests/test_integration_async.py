import asyncio

import pytest

from redis_message_queue.asyncio._redis_gateway import RedisGateway
from redis_message_queue.asyncio.redis_message_queue import RedisMessageQueue


def _no_retry(func):
    return func


pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# 3A. Publish Deduplication
# ---------------------------------------------------------------------------


class TestPublishDeduplication:
    @pytest.mark.asyncio
    async def test_dedup_rejects_duplicate(self, real_async_redis_client, queue_name):
        queue = RedisMessageQueue(queue_name, client=real_async_redis_client)
        assert await queue.publish("hello") is True
        assert await queue.publish("hello") is False
        assert await real_async_redis_client.llen(queue.key.pending) == 1

    @pytest.mark.asyncio
    async def test_dedup_key_has_real_ttl(self, real_async_redis_client, queue_name):
        queue = RedisMessageQueue(queue_name, client=real_async_redis_client)
        await queue.publish("hello")
        dedup_key = queue.key.deduplication("hello")
        ttl = await real_async_redis_client.ttl(dedup_key)
        assert 3500 < ttl <= 3600

    @pytest.mark.asyncio
    async def test_dedup_atomicity_key_and_queue_consistent(self, real_async_redis_client, queue_name):
        queue = RedisMessageQueue(queue_name, client=real_async_redis_client)
        await queue.publish("hello")
        dedup_key = queue.key.deduplication("hello")
        assert await real_async_redis_client.exists(dedup_key) == 1
        assert await real_async_redis_client.llen(queue.key.pending) == 1

    @pytest.mark.asyncio
    async def test_different_messages_both_enqueued(self, real_async_redis_client, queue_name):
        queue = RedisMessageQueue(queue_name, client=real_async_redis_client)
        assert await queue.publish("msg-a") is True
        assert await queue.publish("msg-b") is True
        assert await real_async_redis_client.llen(queue.key.pending) == 2

    @pytest.mark.asyncio
    async def test_no_dedup_allows_duplicates(self, real_async_redis_client, queue_name):
        queue = RedisMessageQueue(queue_name, client=real_async_redis_client, deduplication=False)
        await queue.publish("hello")
        await queue.publish("hello")
        assert await real_async_redis_client.llen(queue.key.pending) == 2


# ---------------------------------------------------------------------------
# 3B. Queue Ordering (Multiple Producers/Consumers)
# ---------------------------------------------------------------------------


class TestQueueOrdering:
    @pytest.mark.asyncio
    async def test_fifo_ordering_single_producer(self, real_async_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_async_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
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
    async def test_concurrent_publish_all_delivered(self, real_async_redis_client, queue_name):
        queue = RedisMessageQueue(queue_name, client=real_async_redis_client, deduplication=False)
        n = 20
        await asyncio.gather(*[queue.publish(f"msg-{i}") for i in range(n)])
        assert await real_async_redis_client.llen(queue.key.pending) == n

    @pytest.mark.asyncio
    async def test_concurrent_consume_no_double_delivery(self, real_async_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_async_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway, deduplication=False)
        n = 20
        for i in range(n):
            await queue.publish(f"msg-{i}")

        consumed = []

        async def consume():
            async with queue.process_message() as msg:
                if msg is not None:
                    consumed.append(msg)

        await asyncio.gather(*[consume() for _ in range(n)])

        assert len(consumed) == n
        assert len(set(consumed)) == n


# ---------------------------------------------------------------------------
# 3C. Processing -> Completed / Failed Transitions
# ---------------------------------------------------------------------------


class TestProcessingTransitions:
    @pytest.mark.asyncio
    async def test_success_moves_to_completed(self, real_async_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_async_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway, enable_completed_queue=True)
        await queue.publish("hello")

        async with queue.process_message() as msg:
            assert msg == b"hello"

        assert await real_async_redis_client.llen(queue.key.processing) == 0
        assert await real_async_redis_client.llen(queue.key.completed) == 1
        assert await real_async_redis_client.lpop(queue.key.completed) == b"hello"

    @pytest.mark.asyncio
    async def test_failure_moves_to_failed(self, real_async_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_async_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway, enable_failed_queue=True)
        await queue.publish("hello")

        with pytest.raises(ValueError):
            async with queue.process_message() as msg:
                assert msg == b"hello"
                raise ValueError("boom")

        assert await real_async_redis_client.llen(queue.key.processing) == 0
        assert await real_async_redis_client.llen(queue.key.failed) == 1
        assert await real_async_redis_client.lpop(queue.key.failed) == b"hello"

    @pytest.mark.asyncio
    async def test_success_without_completed_removes(self, real_async_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_async_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway, enable_completed_queue=False)
        await queue.publish("hello")

        async with queue.process_message() as msg:
            assert msg == b"hello"

        assert await real_async_redis_client.llen(queue.key.pending) == 0
        assert await real_async_redis_client.llen(queue.key.processing) == 0
        assert await real_async_redis_client.llen(queue.key.completed) == 0

    @pytest.mark.asyncio
    async def test_failure_without_failed_removes(self, real_async_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_async_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway, enable_failed_queue=False)
        await queue.publish("hello")

        with pytest.raises(ValueError):
            async with queue.process_message() as msg:
                assert msg == b"hello"
                raise ValueError("boom")

        assert await real_async_redis_client.llen(queue.key.pending) == 0
        assert await real_async_redis_client.llen(queue.key.processing) == 0
        assert await real_async_redis_client.llen(queue.key.failed) == 0

    @pytest.mark.asyncio
    async def test_completed_stores_decoded_payload(self, real_async_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_async_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway, enable_completed_queue=True)
        await queue.publish("hello")

        async with queue.process_message():
            pass

        stored = await real_async_redis_client.lpop(queue.key.completed)
        assert stored == b"hello"
        assert not stored.startswith(b"\x1eRMQ1:")


# ---------------------------------------------------------------------------
# 3D. Visibility-Timeout Reclaim
# ---------------------------------------------------------------------------


class TestVisibilityTimeoutReclaim:
    @pytest.mark.asyncio
    async def test_expired_message_reclaimed(self, real_async_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_async_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=1,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway)
        await queue.publish("hello")

        first = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert first is not None

        await asyncio.sleep(1.5)

        second = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert second is not None
        assert first.stored_message == second.stored_message
        assert first.lease_token != second.lease_token

    @pytest.mark.asyncio
    async def test_not_reclaimed_before_expiry(self, real_async_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_async_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=1,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway)
        await queue.publish("hello")

        first = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert first is not None

        second = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert second is None

    @pytest.mark.asyncio
    async def test_real_time_used_for_lease_deadlines(self, real_async_redis_client, queue_name):
        timeout_seconds = 2
        gateway = RedisGateway(
            redis_client=real_async_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=timeout_seconds,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway)
        await queue.publish("hello")

        await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)

        processing_key = queue.key.processing
        lease_deadlines_key = f"{processing_key}:lease_deadlines"
        stored_message = await real_async_redis_client.lindex(processing_key, 0)
        deadline_ms = await real_async_redis_client.zscore(lease_deadlines_key, stored_message)

        redis_time = await real_async_redis_client.time()
        now_ms = redis_time[0] * 1000 + redis_time[1] // 1000
        expected_ms = now_ms + timeout_seconds * 1000

        assert abs(deadline_ms - expected_ms) < 500


# ---------------------------------------------------------------------------
# 3E. Heartbeat Lease Renewal
# ---------------------------------------------------------------------------


class TestHeartbeatLeaseRenewal:
    @pytest.mark.asyncio
    async def test_heartbeat_prevents_redelivery(self, real_async_redis_client, queue_name):
        queue_gateway = RedisGateway(
            redis_client=real_async_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=1,
        )
        rival_gateway = RedisGateway(
            redis_client=real_async_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=1,
        )
        queue = RedisMessageQueue(
            queue_name,
            gateway=queue_gateway,
            heartbeat_interval_seconds=0.3,
        )
        await queue.publish("hello")

        async with queue.process_message() as msg:
            assert msg == b"hello"
            await asyncio.sleep(2)
            rival = await rival_gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
            assert rival is None

    @pytest.mark.asyncio
    async def test_manual_renewal_extends_deadline(self, real_async_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_async_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=2,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway)
        await queue.publish("hello")

        claimed = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        lease_deadlines_key = f"{queue.key.processing}:lease_deadlines"
        original_deadline = await real_async_redis_client.zscore(lease_deadlines_key, claimed.stored_message)

        await asyncio.sleep(0.1)

        assert (
            await gateway.renew_message_lease(queue.key.processing, claimed.stored_message, claimed.lease_token) is True
        )
        new_deadline = await real_async_redis_client.zscore(lease_deadlines_key, claimed.stored_message)
        assert new_deadline > original_deadline

    @pytest.mark.asyncio
    async def test_stale_renewal_rejected_after_redelivery(self, real_async_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_async_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=1,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway)
        await queue.publish("hello")

        first = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        await asyncio.sleep(1.5)
        second = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert second is not None

        assert await gateway.renew_message_lease(queue.key.processing, first.stored_message, first.lease_token) is False
        assert (
            await gateway.renew_message_lease(queue.key.processing, second.stored_message, second.lease_token) is True
        )


# ---------------------------------------------------------------------------
# 3F. Stale Worker Ack/Fail Rejection
# ---------------------------------------------------------------------------


class TestStaleWorkerRejection:
    @pytest.mark.asyncio
    async def test_stale_remove_ignored_after_redelivery(self, real_async_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_async_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=1,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway)
        await queue.publish("hello")

        first = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        await asyncio.sleep(1.5)
        second = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert second is not None

        await gateway.remove_message(queue.key.processing, first.stored_message, lease_token=first.lease_token)
        assert await real_async_redis_client.llen(queue.key.processing) == 1

        await gateway.remove_message(queue.key.processing, second.stored_message, lease_token=second.lease_token)
        assert await real_async_redis_client.llen(queue.key.processing) == 0

    @pytest.mark.asyncio
    async def test_stale_complete_ignored_after_redelivery(self, real_async_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_async_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=1,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway, enable_completed_queue=True)
        await queue.publish("hello")

        first_ctx = queue.process_message()
        first_msg = await first_ctx.__aenter__()
        assert first_msg == b"hello"

        await asyncio.sleep(1.5)

        second_ctx = queue.process_message()
        second_msg = await second_ctx.__aenter__()
        assert second_msg == b"hello"

        # Old consumer exits -- stale token, should NOT move to completed
        await first_ctx.__aexit__(None, None, None)
        assert await real_async_redis_client.llen(queue.key.processing) == 1
        assert await real_async_redis_client.llen(queue.key.completed) == 0

        # New consumer exits -- valid token, should move to completed
        await second_ctx.__aexit__(None, None, None)
        assert await real_async_redis_client.llen(queue.key.processing) == 0
        assert await real_async_redis_client.llen(queue.key.completed) == 1

    @pytest.mark.asyncio
    async def test_stale_fail_ignored_after_redelivery(self, real_async_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_async_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=1,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway, enable_failed_queue=True)
        await queue.publish("hello")

        first_ctx = queue.process_message()
        first_msg = await first_ctx.__aenter__()
        assert first_msg == b"hello"

        await asyncio.sleep(1.5)

        second_ctx = queue.process_message()
        second_msg = await second_ctx.__aenter__()
        assert second_msg == b"hello"

        # Old consumer fails -- stale token, should NOT move to failed
        try:
            await first_ctx.__aexit__(ValueError, ValueError("old boom"), None)
        except ValueError:
            pass
        assert await real_async_redis_client.llen(queue.key.processing) == 1
        assert await real_async_redis_client.llen(queue.key.failed) == 0

        # New consumer fails -- valid token, should move to failed
        try:
            await second_ctx.__aexit__(ValueError, ValueError("new boom"), None)
        except ValueError:
            pass
        assert await real_async_redis_client.llen(queue.key.processing) == 0
        assert await real_async_redis_client.llen(queue.key.failed) == 1
