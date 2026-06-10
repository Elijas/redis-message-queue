import uuid

import fakeredis
import pytest

from redis_message_queue import EventOperation, QueueEvent
from redis_message_queue._redis_gateway import RedisGateway
from redis_message_queue._stored_message import extract_stored_message_id
from redis_message_queue.asyncio._redis_gateway import (
    RedisGateway as AsyncRedisGateway,
)
from redis_message_queue.asyncio.redis_message_queue import (
    RedisMessageQueue as AsyncRedisMessageQueue,
)
from redis_message_queue.redis_message_queue import RedisMessageQueue


class TestSyncVisibilityTimeoutValidation:
    def test_queue_rejects_zero_visibility_timeout(self):
        with pytest.raises(ValueError, match="visibility_timeout_seconds"):
            RedisMessageQueue("test", client=fakeredis.FakeRedis(), visibility_timeout_seconds=0)

    def test_queue_rejects_non_int_visibility_timeout(self):
        with pytest.raises(TypeError, match="visibility_timeout_seconds"):
            RedisMessageQueue("test", client=fakeredis.FakeRedis(), visibility_timeout_seconds=1.5)

    def test_queue_rejects_visibility_timeout_with_gateway(self):
        gateway = RedisGateway(redis_client=fakeredis.FakeRedis(), retry_budget_seconds=0)
        with pytest.raises(ValueError, match="visibility_timeout_seconds"):
            RedisMessageQueue("test", gateway=gateway, visibility_timeout_seconds=30)

    def test_gateway_rejects_zero_visibility_timeout(self):
        with pytest.raises(ValueError, match="message_visibility_timeout_seconds"):
            RedisGateway(
                redis_client=fakeredis.FakeRedis(),
                retry_budget_seconds=0,
                message_visibility_timeout_seconds=0,
            )

    def test_gateway_rejects_non_int_visibility_timeout(self):
        with pytest.raises(TypeError, match="message_visibility_timeout_seconds"):
            RedisGateway(
                redis_client=fakeredis.FakeRedis(),
                retry_budget_seconds=0,
                message_visibility_timeout_seconds=1.5,
            )

    def test_queue_rejects_negative_visibility_timeout(self):
        with pytest.raises(ValueError, match="visibility_timeout_seconds"):
            RedisMessageQueue("test", client=fakeredis.FakeRedis(), visibility_timeout_seconds=-1)

    def test_queue_rejects_bool_visibility_timeout(self):
        with pytest.raises(TypeError, match="visibility_timeout_seconds"):
            RedisMessageQueue("test", client=fakeredis.FakeRedis(), visibility_timeout_seconds=True)


class TestAsyncVisibilityTimeoutValidation:
    def test_queue_rejects_zero_visibility_timeout(self):
        with pytest.raises(ValueError, match="visibility_timeout_seconds"):
            AsyncRedisMessageQueue("test", client=fakeredis.FakeAsyncRedis(), visibility_timeout_seconds=0)

    def test_queue_rejects_non_int_visibility_timeout(self):
        with pytest.raises(TypeError, match="visibility_timeout_seconds"):
            AsyncRedisMessageQueue("test", client=fakeredis.FakeAsyncRedis(), visibility_timeout_seconds=1.5)

    def test_queue_rejects_visibility_timeout_with_gateway(self):
        gateway = AsyncRedisGateway(redis_client=fakeredis.FakeAsyncRedis(), retry_budget_seconds=0)
        with pytest.raises(ValueError, match="visibility_timeout_seconds"):
            AsyncRedisMessageQueue("test", gateway=gateway, visibility_timeout_seconds=30)

    def test_gateway_rejects_zero_visibility_timeout(self):
        with pytest.raises(ValueError, match="message_visibility_timeout_seconds"):
            AsyncRedisGateway(
                redis_client=fakeredis.FakeAsyncRedis(),
                retry_budget_seconds=0,
                message_visibility_timeout_seconds=0,
            )

    def test_gateway_rejects_non_int_visibility_timeout(self):
        with pytest.raises(TypeError, match="message_visibility_timeout_seconds"):
            AsyncRedisGateway(
                redis_client=fakeredis.FakeAsyncRedis(),
                retry_budget_seconds=0,
                message_visibility_timeout_seconds=1.5,
            )

    def test_queue_rejects_negative_visibility_timeout(self):
        with pytest.raises(ValueError, match="visibility_timeout_seconds"):
            AsyncRedisMessageQueue("test", client=fakeredis.FakeAsyncRedis(), visibility_timeout_seconds=-1)

    def test_queue_rejects_bool_visibility_timeout(self):
        with pytest.raises(TypeError, match="visibility_timeout_seconds"):
            AsyncRedisMessageQueue("test", client=fakeredis.FakeAsyncRedis(), visibility_timeout_seconds=True)


class TestSyncVisibilityTimeoutRecovery:
    def test_gateway_reclaims_expired_message_with_new_lease_token(self):
        client = fakeredis.FakeRedis()
        gateway = RedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=30,
        )
        queue = RedisMessageQueue("test", gateway=gateway)
        queue.publish("hello")

        first = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        processing_message = client.lindex(queue.key.processing, 0)
        client.zadd(gateway._lease_deadlines_key(queue.key.processing), {processing_message: 0})

        second = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)

        assert first.stored_message == second.stored_message
        assert first.lease_token != second.lease_token
        assert client.llen(queue.key.pending) == 0
        assert client.llen(queue.key.processing) == 1

    def test_gateway_rejects_stale_remove_after_redelivery(self):
        client = fakeredis.FakeRedis()
        gateway = RedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=30,
        )
        queue = RedisMessageQueue("test", gateway=gateway)
        queue.publish("hello")

        first = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        processing_message = client.lindex(queue.key.processing, 0)
        client.zadd(gateway._lease_deadlines_key(queue.key.processing), {processing_message: 0})
        second = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)

        gateway.remove_message(queue.key.processing, first.stored_message, lease_token=first.lease_token)
        assert client.llen(queue.key.processing) == 1

        gateway.remove_message(queue.key.processing, second.stored_message, lease_token=second.lease_token)
        assert client.llen(queue.key.processing) == 0

    def test_queue_ignores_stale_success_cleanup_after_redelivery(self):
        client = fakeredis.FakeRedis()
        gateway = RedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=30,
        )
        queue = RedisMessageQueue("test", gateway=gateway, enable_completed_queue=True)
        queue.publish("hello")

        first_context = queue.process_message()
        first_message = first_context.__enter__()
        processing_message = client.lindex(queue.key.processing, 0)
        client.zadd(gateway._lease_deadlines_key(queue.key.processing), {processing_message: 0})

        second_context = queue.process_message()
        second_message = second_context.__enter__()

        assert first_message == second_message == b"hello"

        first_context.__exit__(None, None, None)
        assert client.llen(queue.key.processing) == 1
        assert client.llen(queue.key.completed) == 0

        second_context.__exit__(None, None, None)
        assert client.llen(queue.key.processing) == 0
        assert client.lpop(queue.key.completed) == b"hello"

    def test_gateway_rejects_stale_move_after_redelivery(self):
        client = fakeredis.FakeRedis()
        gateway = RedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=30,
        )
        queue = RedisMessageQueue("test", gateway=gateway, enable_completed_queue=True)
        queue.publish("hello")

        first = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        processing_message = client.lindex(queue.key.processing, 0)
        client.zadd(gateway._lease_deadlines_key(queue.key.processing), {processing_message: 0})
        second = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)

        result = gateway.move_message(
            queue.key.processing, queue.key.completed, first.stored_message, lease_token=first.lease_token
        )
        assert result is False
        assert client.llen(queue.key.processing) == 1

        result = gateway.move_message(
            queue.key.processing, queue.key.completed, second.stored_message, lease_token=second.lease_token
        )
        assert result is True
        assert client.llen(queue.key.processing) == 0
        assert client.lpop(queue.key.completed) == b"hello"
        assert client.zcard(gateway._lease_deadlines_key(queue.key.processing)) == 0
        assert client.hlen(gateway._lease_tokens_key(queue.key.processing)) == 0

    def test_gateway_rejects_stale_lease_renewal_after_redelivery(self):
        client = fakeredis.FakeRedis()
        gateway = RedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=30,
        )
        queue = RedisMessageQueue("test", gateway=gateway)
        queue.publish("hello")

        first = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        processing_message = client.lindex(queue.key.processing, 0)
        client.zadd(gateway._lease_deadlines_key(queue.key.processing), {processing_message: 0})
        second = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)

        stale_result = gateway.renew_message_lease(queue.key.processing, first.stored_message, first.lease_token)
        assert stale_result is False

        fresh_result = gateway.renew_message_lease(queue.key.processing, second.stored_message, second.lease_token)
        assert fresh_result is True

        deadline = client.zscore(gateway._lease_deadlines_key(queue.key.processing), second.stored_message)
        assert deadline > 0


class TestSyncBatchReclaim:
    def test_reclaim_events_include_message_id_and_delivery_count(self):
        client = fakeredis.FakeRedis()
        events: list[QueueEvent] = []
        gateway = RedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=30,
        )
        queue = RedisMessageQueue("test", gateway=gateway, deduplication=False, on_event=events.append)
        for i in range(3):
            queue.publish(f"msg-{i}")

        claimed = []
        for _ in range(3):
            c = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
            assert c is not None
            claimed.append(c)
        expected_ids = {extract_stored_message_id(c.stored_message) for c in claimed}

        lease_deadlines_key = gateway._lease_deadlines_key(queue.key.processing)
        for c in claimed:
            client.zadd(lease_deadlines_key, {c.stored_message: 0})

        assert gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing) is not None

        reclaim_events = [event for event in events if event.operation is EventOperation.CLAIM_RECLAIM]
        assert len(reclaim_events) == 3
        assert {event.message_id for event in reclaim_events} == expected_ids
        assert {event.delivery_count for event in reclaim_events} == {1}
        assert all(event.max_delivery_count is None for event in reclaim_events)

    def test_single_poll_reclaims_multiple_expired_messages(self):
        client = fakeredis.FakeRedis()
        gateway = RedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=30,
        )
        queue = RedisMessageQueue("test", gateway=gateway, deduplication=False)
        for i in range(5):
            queue.publish(f"msg-{i}")

        # Claim all 5 messages
        claimed = []
        for _ in range(5):
            c = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
            claimed.append(c)
        assert client.llen(queue.key.pending) == 0
        assert client.llen(queue.key.processing) == 5

        # Expire all leases by setting deadline to 0
        lease_deadlines_key = gateway._lease_deadlines_key(queue.key.processing)
        for c in claimed:
            client.zadd(lease_deadlines_key, {c.stored_message: 0})

        # Single poll should batch-reclaim all 5 expired, then claim 1
        result = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert result is not None
        assert client.llen(queue.key.pending) == 4
        assert client.llen(queue.key.processing) == 1

    def test_reclaimed_message_claimed_before_fresh_pending(self):
        client = fakeredis.FakeRedis()
        gateway = RedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=30,
        )
        queue = RedisMessageQueue("test", gateway=gateway, deduplication=False)
        queue.publish("old-message")

        first = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        processing_message = client.lindex(queue.key.processing, 0)
        client.zadd(gateway._lease_deadlines_key(queue.key.processing), {processing_message: 0})

        queue.publish("fresh-message")

        second = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert second is not None
        assert second.stored_message == first.stored_message

    def test_lease_metadata_empty_after_all_messages_processed(self):
        client = fakeredis.FakeRedis()
        gateway = RedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=30,
        )
        queue = RedisMessageQueue("test", gateway=gateway, deduplication=False)
        queue.publish("msg-a")
        queue.publish("msg-b")

        for _ in range(2):
            with queue.process_message() as msg:
                assert msg is not None

        lease_deadlines_key = gateway._lease_deadlines_key(queue.key.processing)
        lease_tokens_key = gateway._lease_tokens_key(queue.key.processing)
        assert client.zcard(lease_deadlines_key) == 0
        assert client.hlen(lease_tokens_key) == 0
        assert client.llen(queue.key.processing) == 0

    def test_reclaim_replaces_metadata_then_cleans_on_completion(self):
        client = fakeredis.FakeRedis()
        gateway = RedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=30,
        )
        queue = RedisMessageQueue("test", gateway=gateway)
        queue.publish("hello")

        first = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        processing_message = client.lindex(queue.key.processing, 0)
        client.zadd(gateway._lease_deadlines_key(queue.key.processing), {processing_message: 0})

        second = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert second is not None
        assert second.lease_token != first.lease_token

        lease_deadlines_key = gateway._lease_deadlines_key(queue.key.processing)
        lease_tokens_key = gateway._lease_tokens_key(queue.key.processing)
        assert client.zcard(lease_deadlines_key) == 1
        assert client.hlen(lease_tokens_key) == 1

        gateway.remove_message(queue.key.processing, second.stored_message, lease_token=second.lease_token)
        assert client.zcard(lease_deadlines_key) == 0
        assert client.hlen(lease_tokens_key) == 0
        assert client.llen(queue.key.processing) == 0

    def test_batch_reclaim_requeues_oldest_deadline_first(self):
        client = fakeredis.FakeRedis()
        gateway = RedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=30,
        )
        queue = RedisMessageQueue("test", gateway=gateway, deduplication=False)
        for i in range(3):
            queue.publish(f"msg-{i}")

        claimed = []
        for _ in range(3):
            c = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
            claimed.append(c)

        # Set distinct expired deadlines: oldest → newest
        lease_deadlines_key = gateway._lease_deadlines_key(queue.key.processing)
        client.zadd(lease_deadlines_key, {claimed[0].stored_message: 100})
        client.zadd(lease_deadlines_key, {claimed[1].stored_message: 200})
        client.zadd(lease_deadlines_key, {claimed[2].stored_message: 300})

        # Reclaim should return oldest-deadline first
        first = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        second = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        third = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)

        assert first.stored_message == claimed[0].stored_message
        assert second.stored_message == claimed[1].stored_message
        assert third.stored_message == claimed[2].stored_message

    def test_expired_lease_reclaimed_exactly_once_and_emits_event(self):
        # Regression guard for the RPUSH-before-LREM reclaim reorder: one expired lease is
        # redelivered exactly once (the speculative RPUSH must not leave a duplicate in
        # pending) and emits exactly one CLAIM_RECLAIM event.
        client = fakeredis.FakeRedis()
        events: list[QueueEvent] = []
        gateway = RedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=30,
        )
        queue = RedisMessageQueue("test", gateway=gateway, deduplication=False, on_event=events.append)
        queue.publish("reclaim-me")

        first = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert first is not None
        expected_id = extract_stored_message_id(first.stored_message)
        client.zadd(gateway._lease_deadlines_key(queue.key.processing), {first.stored_message: 0})

        second = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)

        assert second is not None
        assert second.stored_message == first.stored_message
        assert second.lease_token != first.lease_token
        assert client.llen(queue.key.pending) == 0
        assert client.lrange(queue.key.processing, 0, -1) == [first.stored_message]

        reclaim_events = [event for event in events if event.operation is EventOperation.CLAIM_RECLAIM]
        assert len(reclaim_events) == 1
        assert reclaim_events[0].message_id == expected_id
        assert reclaim_events[0].delivery_count == 1

    def test_reclaim_compensates_torn_deadline_without_phantom_pending(self):
        # Exercises the else-branch compensation added with the RPUSH-before-LREM reorder.
        # A torn deadline entry whose member is NOT in processing is speculatively RPUSHed
        # to pending; LREM(processing) then returns 0, so the else branch must remove the
        # speculative copy (no phantom), while a genuinely reclaimable sibling in the same
        # reclaim batch is still recovered exactly once.
        client = fakeredis.FakeRedis()
        events: list[QueueEvent] = []
        gateway = RedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=30,
        )
        queue = RedisMessageQueue("test", gateway=gateway, deduplication=False, on_event=events.append)
        queue.publish("reclaim-me")

        good = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert good is not None
        good_id = extract_stored_message_id(good.stored_message)

        lease_deadlines_key = gateway._lease_deadlines_key(queue.key.processing)
        client.zadd(lease_deadlines_key, {good.stored_message: 0})
        # Torn state: an expired deadline whose member was never (or is no longer) in processing.
        phantom = b"phantom-not-in-processing"
        client.zadd(lease_deadlines_key, {phantom: 0})
        assert phantom not in client.lrange(queue.key.processing, 0, -1)

        reclaimed = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)

        # The genuine sibling is reclaimed exactly once.
        assert reclaimed is not None
        assert reclaimed.stored_message == good.stored_message
        assert client.lrange(queue.key.processing, 0, -1) == [good.stored_message]
        # The torn member leaves NO phantom in pending: the else branch undid the speculative RPUSH.
        assert client.llen(queue.key.pending) == 0
        assert phantom not in client.lrange(queue.key.pending, 0, -1)
        # ZREM ran inside the loop, so the torn deadline is gone regardless of the LREM outcome.
        assert client.zscore(lease_deadlines_key, phantom) is None

        reclaim_events = [event for event in events if event.operation is EventOperation.CLAIM_RECLAIM]
        assert len(reclaim_events) == 1
        assert reclaim_events[0].message_id == good_id


class TestAsyncBatchReclaim:
    @pytest.mark.asyncio
    async def test_reclaim_events_include_message_id_and_delivery_count(self):
        client = fakeredis.FakeAsyncRedis()
        events: list[QueueEvent] = []

        async def observe(event: QueueEvent) -> None:
            events.append(event)

        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=30,
        )
        queue = AsyncRedisMessageQueue("test", gateway=gateway, deduplication=False, on_event=observe)
        for i in range(3):
            await queue.publish(f"msg-{i}")

        claimed = []
        for _ in range(3):
            c = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
            assert c is not None
            claimed.append(c)
        expected_ids = {extract_stored_message_id(c.stored_message) for c in claimed}

        lease_deadlines_key = gateway._lease_deadlines_key(queue.key.processing)
        for c in claimed:
            await client.zadd(lease_deadlines_key, {c.stored_message: 0})

        assert await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing) is not None

        reclaim_events = [event for event in events if event.operation is EventOperation.CLAIM_RECLAIM]
        assert len(reclaim_events) == 3
        assert {event.message_id for event in reclaim_events} == expected_ids
        assert {event.delivery_count for event in reclaim_events} == {1}
        assert all(event.max_delivery_count is None for event in reclaim_events)

    @pytest.mark.asyncio
    async def test_single_poll_reclaims_multiple_expired_messages(self):
        client = fakeredis.FakeAsyncRedis()
        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=30,
        )
        queue = AsyncRedisMessageQueue("test", gateway=gateway, deduplication=False)
        for i in range(5):
            await queue.publish(f"msg-{i}")

        claimed = []
        for _ in range(5):
            c = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
            claimed.append(c)
        assert await client.llen(queue.key.pending) == 0
        assert await client.llen(queue.key.processing) == 5

        lease_deadlines_key = gateway._lease_deadlines_key(queue.key.processing)
        for c in claimed:
            await client.zadd(lease_deadlines_key, {c.stored_message: 0})

        result = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert result is not None
        assert await client.llen(queue.key.pending) == 4
        assert await client.llen(queue.key.processing) == 1

    @pytest.mark.asyncio
    async def test_reclaimed_message_claimed_before_fresh_pending(self):
        client = fakeredis.FakeAsyncRedis()
        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=30,
        )
        queue = AsyncRedisMessageQueue("test", gateway=gateway, deduplication=False)
        await queue.publish("old-message")

        first = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        processing_message = await client.lindex(queue.key.processing, 0)
        await client.zadd(gateway._lease_deadlines_key(queue.key.processing), {processing_message: 0})

        await queue.publish("fresh-message")

        second = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert second is not None
        assert second.stored_message == first.stored_message

    @pytest.mark.asyncio
    async def test_lease_metadata_empty_after_all_messages_processed(self):
        client = fakeredis.FakeAsyncRedis()
        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=30,
        )
        queue = AsyncRedisMessageQueue("test", gateway=gateway, deduplication=False)
        await queue.publish("msg-a")
        await queue.publish("msg-b")

        for _ in range(2):
            async with queue.process_message() as msg:
                assert msg is not None

        lease_deadlines_key = gateway._lease_deadlines_key(queue.key.processing)
        lease_tokens_key = gateway._lease_tokens_key(queue.key.processing)
        assert await client.zcard(lease_deadlines_key) == 0
        assert await client.hlen(lease_tokens_key) == 0
        assert await client.llen(queue.key.processing) == 0

    @pytest.mark.asyncio
    async def test_reclaim_replaces_metadata_then_cleans_on_completion(self):
        client = fakeredis.FakeAsyncRedis()
        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=30,
        )
        queue = AsyncRedisMessageQueue("test", gateway=gateway)
        await queue.publish("hello")

        first = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        processing_message = await client.lindex(queue.key.processing, 0)
        await client.zadd(gateway._lease_deadlines_key(queue.key.processing), {processing_message: 0})

        second = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert second is not None
        assert second.lease_token != first.lease_token

        lease_deadlines_key = gateway._lease_deadlines_key(queue.key.processing)
        lease_tokens_key = gateway._lease_tokens_key(queue.key.processing)
        assert await client.zcard(lease_deadlines_key) == 1
        assert await client.hlen(lease_tokens_key) == 1

        await gateway.remove_message(queue.key.processing, second.stored_message, lease_token=second.lease_token)
        assert await client.zcard(lease_deadlines_key) == 0
        assert await client.hlen(lease_tokens_key) == 0
        assert await client.llen(queue.key.processing) == 0

    @pytest.mark.asyncio
    async def test_batch_reclaim_requeues_oldest_deadline_first(self):
        client = fakeredis.FakeAsyncRedis()
        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=30,
        )
        queue = AsyncRedisMessageQueue("test", gateway=gateway, deduplication=False)
        for i in range(3):
            await queue.publish(f"msg-{i}")

        claimed = []
        for _ in range(3):
            c = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
            claimed.append(c)

        # Set distinct expired deadlines: oldest → newest
        lease_deadlines_key = gateway._lease_deadlines_key(queue.key.processing)
        await client.zadd(lease_deadlines_key, {claimed[0].stored_message: 100})
        await client.zadd(lease_deadlines_key, {claimed[1].stored_message: 200})
        await client.zadd(lease_deadlines_key, {claimed[2].stored_message: 300})

        # Reclaim should return oldest-deadline first
        first = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        second = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        third = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)

        assert first.stored_message == claimed[0].stored_message
        assert second.stored_message == claimed[1].stored_message
        assert third.stored_message == claimed[2].stored_message

    @pytest.mark.asyncio
    async def test_expired_lease_reclaimed_exactly_once_and_emits_event(self):
        client = fakeredis.FakeAsyncRedis()
        events: list[QueueEvent] = []

        async def observe(event: QueueEvent) -> None:
            events.append(event)

        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=30,
        )
        queue = AsyncRedisMessageQueue("test", gateway=gateway, deduplication=False, on_event=observe)
        await queue.publish("reclaim-me")

        first = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert first is not None
        expected_id = extract_stored_message_id(first.stored_message)
        await client.zadd(gateway._lease_deadlines_key(queue.key.processing), {first.stored_message: 0})

        second = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)

        assert second is not None
        assert second.stored_message == first.stored_message
        assert second.lease_token != first.lease_token
        assert await client.llen(queue.key.pending) == 0
        assert await client.lrange(queue.key.processing, 0, -1) == [first.stored_message]

        reclaim_events = [event for event in events if event.operation is EventOperation.CLAIM_RECLAIM]
        assert len(reclaim_events) == 1
        assert reclaim_events[0].message_id == expected_id
        assert reclaim_events[0].delivery_count == 1

    @pytest.mark.asyncio
    async def test_reclaim_compensates_torn_deadline_without_phantom_pending(self):
        client = fakeredis.FakeAsyncRedis()
        events: list[QueueEvent] = []

        async def observe(event: QueueEvent) -> None:
            events.append(event)

        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=30,
        )
        queue = AsyncRedisMessageQueue("test", gateway=gateway, deduplication=False, on_event=observe)
        await queue.publish("reclaim-me")

        good = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert good is not None
        good_id = extract_stored_message_id(good.stored_message)

        lease_deadlines_key = gateway._lease_deadlines_key(queue.key.processing)
        await client.zadd(lease_deadlines_key, {good.stored_message: 0})
        phantom = b"phantom-not-in-processing"
        await client.zadd(lease_deadlines_key, {phantom: 0})
        assert phantom not in (await client.lrange(queue.key.processing, 0, -1))

        reclaimed = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)

        assert reclaimed is not None
        assert reclaimed.stored_message == good.stored_message
        assert await client.lrange(queue.key.processing, 0, -1) == [good.stored_message]
        assert await client.llen(queue.key.pending) == 0
        assert phantom not in (await client.lrange(queue.key.pending, 0, -1))
        assert await client.zscore(lease_deadlines_key, phantom) is None

        reclaim_events = [event for event in events if event.operation is EventOperation.CLAIM_RECLAIM]
        assert len(reclaim_events) == 1
        assert reclaim_events[0].message_id == good_id


class TestSyncBatchReclaimBoundary:
    """Tests for the LIMIT 100 cap in the batch reclaim Lua script.

    When >100 messages expire simultaneously, recovery requires multiple poll
    cycles. Each poll reclaims up to 100 expired entries + claims 1.
    """

    def test_reclaim_over_100_requires_multiple_polls(self):
        client = fakeredis.FakeRedis()
        gateway = RedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=30,
        )
        queue = RedisMessageQueue("test", gateway=gateway, deduplication=False)
        n = 105
        for i in range(n):
            queue.publish(f"msg-{i}")

        # Claim all 105 messages
        claimed = []
        for _ in range(n):
            c = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
            assert c is not None
            claimed.append(c)
        assert client.llen(queue.key.pending) == 0
        assert client.llen(queue.key.processing) == n

        # Expire all leases
        lease_deadlines_key = gateway._lease_deadlines_key(queue.key.processing)
        for c in claimed:
            client.zadd(lease_deadlines_key, {c.stored_message: 0})

        # First poll: reclaims 100 expired, requeues them, then claims 1
        first = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert first is not None
        assert client.llen(queue.key.pending) == 99
        assert client.llen(queue.key.processing) == 6  # 5 never reclaimed + 1 just claimed

        # Second poll: reclaims remaining 5 expired, requeues them, then claims 1
        second = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert second is not None
        assert client.llen(queue.key.pending) == 103
        assert client.llen(queue.key.processing) == 2  # 0 expired remain + 2 freshly claimed

    def test_all_105_eventually_recovered(self):
        client = fakeredis.FakeRedis()
        gateway = RedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=30,
        )
        queue = RedisMessageQueue("test", gateway=gateway, deduplication=False)
        n = 105
        for i in range(n):
            queue.publish(f"msg-{i}")

        # Claim all, then expire all leases
        claimed = []
        for _ in range(n):
            c = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
            claimed.append(c)
        lease_deadlines_key = gateway._lease_deadlines_key(queue.key.processing)
        for c in claimed:
            client.zadd(lease_deadlines_key, {c.stored_message: 0})

        # Drain the queue by repeatedly claiming and acking
        recovered = []
        for _ in range(n):
            result = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
            assert result is not None
            gateway.remove_message(queue.key.processing, result.stored_message, lease_token=result.lease_token)
            recovered.append(result.stored_message)

        # All 105 messages should be recovered exactly once
        assert len(recovered) == n
        assert len(set(recovered)) == n
        assert client.llen(queue.key.pending) == 0
        assert client.llen(queue.key.processing) == 0

        # All lease metadata should be clean
        lease_tokens_key = gateway._lease_tokens_key(queue.key.processing)
        assert client.zcard(lease_deadlines_key) == 0
        assert client.hlen(lease_tokens_key) == 0


class TestAsyncBatchReclaimBoundary:
    """Async counterpart for the LIMIT 100 boundary tests."""

    @pytest.mark.asyncio
    async def test_reclaim_over_100_requires_multiple_polls(self):
        client = fakeredis.FakeAsyncRedis()
        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=30,
        )
        queue = AsyncRedisMessageQueue("test", gateway=gateway, deduplication=False)
        n = 105
        for i in range(n):
            await queue.publish(f"msg-{i}")

        claimed = []
        for _ in range(n):
            c = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
            assert c is not None
            claimed.append(c)
        assert await client.llen(queue.key.pending) == 0
        assert await client.llen(queue.key.processing) == n

        lease_deadlines_key = gateway._lease_deadlines_key(queue.key.processing)
        for c in claimed:
            await client.zadd(lease_deadlines_key, {c.stored_message: 0})

        first = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert first is not None
        assert await client.llen(queue.key.pending) == 99
        assert await client.llen(queue.key.processing) == 6

        second = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert second is not None
        assert await client.llen(queue.key.pending) == 103
        assert await client.llen(queue.key.processing) == 2

    @pytest.mark.asyncio
    async def test_all_105_eventually_recovered(self):
        client = fakeredis.FakeAsyncRedis()
        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=30,
        )
        queue = AsyncRedisMessageQueue("test", gateway=gateway, deduplication=False)
        n = 105
        for i in range(n):
            await queue.publish(f"msg-{i}")

        claimed = []
        for _ in range(n):
            c = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
            claimed.append(c)
        lease_deadlines_key = gateway._lease_deadlines_key(queue.key.processing)
        for c in claimed:
            await client.zadd(lease_deadlines_key, {c.stored_message: 0})

        recovered = []
        for _ in range(n):
            result = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
            assert result is not None
            await gateway.remove_message(queue.key.processing, result.stored_message, lease_token=result.lease_token)
            recovered.append(result.stored_message)

        assert len(recovered) == n
        assert len(set(recovered)) == n
        assert await client.llen(queue.key.pending) == 0
        assert await client.llen(queue.key.processing) == 0

        lease_tokens_key = gateway._lease_tokens_key(queue.key.processing)
        assert await client.zcard(lease_deadlines_key) == 0
        assert await client.hlen(lease_tokens_key) == 0


class TestAsyncVisibilityTimeoutRecovery:
    @pytest.mark.asyncio
    async def test_gateway_reclaims_expired_message_with_new_lease_token(self):
        client = fakeredis.FakeAsyncRedis()
        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=30,
        )
        queue = AsyncRedisMessageQueue("test", gateway=gateway)
        await queue.publish("hello")

        first = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        processing_message = await client.lindex(queue.key.processing, 0)
        await client.zadd(gateway._lease_deadlines_key(queue.key.processing), {processing_message: 0})

        second = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)

        assert first.stored_message == second.stored_message
        assert first.lease_token != second.lease_token
        assert await client.llen(queue.key.pending) == 0
        assert await client.llen(queue.key.processing) == 1

    @pytest.mark.asyncio
    async def test_gateway_rejects_stale_remove_after_redelivery(self):
        client = fakeredis.FakeAsyncRedis()
        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=30,
        )
        queue = AsyncRedisMessageQueue("test", gateway=gateway)
        await queue.publish("hello")

        first = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        processing_message = await client.lindex(queue.key.processing, 0)
        await client.zadd(gateway._lease_deadlines_key(queue.key.processing), {processing_message: 0})
        second = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)

        await gateway.remove_message(queue.key.processing, first.stored_message, lease_token=first.lease_token)
        assert await client.llen(queue.key.processing) == 1

        await gateway.remove_message(queue.key.processing, second.stored_message, lease_token=second.lease_token)
        assert await client.llen(queue.key.processing) == 0

    @pytest.mark.asyncio
    async def test_queue_ignores_stale_success_cleanup_after_redelivery(self):
        client = fakeredis.FakeAsyncRedis()
        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=30,
        )
        queue = AsyncRedisMessageQueue("test", gateway=gateway, enable_completed_queue=True)
        await queue.publish("hello")

        first_context = queue.process_message()
        first_message = await first_context.__aenter__()
        processing_message = await client.lindex(queue.key.processing, 0)
        await client.zadd(gateway._lease_deadlines_key(queue.key.processing), {processing_message: 0})

        second_context = queue.process_message()
        second_message = await second_context.__aenter__()

        assert first_message == second_message == b"hello"

        await first_context.__aexit__(None, None, None)
        assert await client.llen(queue.key.processing) == 1
        assert await client.llen(queue.key.completed) == 0

        await second_context.__aexit__(None, None, None)
        assert await client.llen(queue.key.processing) == 0
        assert await client.lpop(queue.key.completed) == b"hello"

    @pytest.mark.asyncio
    async def test_gateway_rejects_stale_move_after_redelivery(self):
        client = fakeredis.FakeAsyncRedis()
        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=30,
        )
        queue = AsyncRedisMessageQueue("test", gateway=gateway, enable_completed_queue=True)
        await queue.publish("hello")

        first = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        processing_message = await client.lindex(queue.key.processing, 0)
        await client.zadd(gateway._lease_deadlines_key(queue.key.processing), {processing_message: 0})
        second = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)

        result = await gateway.move_message(
            queue.key.processing, queue.key.completed, first.stored_message, lease_token=first.lease_token
        )
        assert result is False
        assert await client.llen(queue.key.processing) == 1

        result = await gateway.move_message(
            queue.key.processing, queue.key.completed, second.stored_message, lease_token=second.lease_token
        )
        assert result is True
        assert await client.llen(queue.key.processing) == 0
        assert await client.lpop(queue.key.completed) == b"hello"
        assert await client.zcard(gateway._lease_deadlines_key(queue.key.processing)) == 0
        assert await client.hlen(gateway._lease_tokens_key(queue.key.processing)) == 0

    @pytest.mark.asyncio
    async def test_gateway_rejects_stale_lease_renewal_after_redelivery(self):
        client = fakeredis.FakeAsyncRedis()
        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=30,
        )
        queue = AsyncRedisMessageQueue("test", gateway=gateway)
        await queue.publish("hello")

        first = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        processing_message = await client.lindex(queue.key.processing, 0)
        await client.zadd(gateway._lease_deadlines_key(queue.key.processing), {processing_message: 0})
        second = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)

        stale_result = await gateway.renew_message_lease(queue.key.processing, first.stored_message, first.lease_token)
        assert stale_result is False

        fresh_result = await gateway.renew_message_lease(
            queue.key.processing, second.stored_message, second.lease_token
        )
        assert fresh_result is True

        deadline = await client.zscore(gateway._lease_deadlines_key(queue.key.processing), second.stored_message)
        assert deadline > 0


class TestSyncReclaimGCsDeliveryCountsForExternallyRemovedMessage:
    """When a leased message leaves processing outside the lease protocol (external
    LREM/DEL, or a non-lease remove on a VT queue), the message exists in neither
    pending nor processing. The expiry-reclaim sweep must GC its delivery_counts
    entry too; otherwise it leaks permanently in the no-TTL hash and wedges
    _cleanup_drained_lease_token_counter (which requires HLEN(delivery_counts)==0).
    """

    def test_external_lrem_then_reclaim_gcs_delivery_counts_and_unwedges_counter(self):
        client = fakeredis.FakeRedis()
        gateway = RedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=1,
        )
        queue = RedisMessageQueue("test", gateway=gateway, deduplication=False)
        pending, processing = queue.key.pending, queue.key.processing

        queue.publish("victim")
        claimed = gateway._claim_visible_message(pending, processing, claim_id=uuid.uuid4().hex)
        assert claimed is not None
        assert client.hlen(gateway._delivery_counts_key(processing)) == 1

        # External removal (the documented removed==0 trigger): a direct LREM on
        # the processing list while the lease is still valid.
        assert client.lrem(processing, 1, claimed.stored_message) == 1

        # Expire the lease and run the expiry-reclaim sweep via a subsequent claim.
        client.zadd(gateway._lease_deadlines_key(processing), {claimed.stored_message: 0})
        gc_claim = gateway._claim_visible_message(pending, processing, claim_id=uuid.uuid4().hex)
        assert gc_claim is None

        # All per-message lease state, including delivery_counts, is now GC'd.
        assert client.hlen(gateway._delivery_counts_key(processing)) == 0
        assert client.zcard(gateway._lease_deadlines_key(processing)) == 0
        assert client.hlen(gateway._lease_tokens_key(processing)) == 0
        assert client.llen(processing) == 0
        assert client.llen(pending) == 0

        # The lease-token counter cleanup is no longer wedged.
        assert gateway._cleanup_drained_lease_token_counter(processing) is True
        assert client.get(gateway._lease_token_counter_key(processing)) is None

    def test_non_lease_ack_then_reclaim_gcs_delivery_counts(self):
        client = fakeredis.FakeRedis()
        gateway = RedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=1,
        )
        queue = RedisMessageQueue("test", gateway=gateway, deduplication=False)
        pending, processing = queue.key.pending, queue.key.processing

        queue.publish("victim")
        claimed = gateway._claim_visible_message(pending, processing, claim_id=uuid.uuid4().hex)
        assert claimed is not None

        # Misuse path: ack WITHOUT the lease token removes the message from
        # processing but orphans the lease metadata for the reclaim loop.
        assert gateway.remove_message(processing, claimed.stored_message) is True
        assert client.llen(processing) == 0
        assert client.hlen(gateway._delivery_counts_key(processing)) == 1

        client.zadd(gateway._lease_deadlines_key(processing), {claimed.stored_message: 0})
        gc_claim = gateway._claim_visible_message(pending, processing, claim_id=uuid.uuid4().hex)
        assert gc_claim is None

        assert client.hlen(gateway._delivery_counts_key(processing)) == 0
        assert client.zcard(gateway._lease_deadlines_key(processing)) == 0
        assert client.hlen(gateway._lease_tokens_key(processing)) == 0
        assert client.llen(pending) == 0
        assert gateway._cleanup_drained_lease_token_counter(processing) is True

    def test_redelivery_path_preserves_delivery_count(self):
        # The successful-reclaim branch must KEEP the count so it keeps
        # incrementing across redeliveries (the DLQ contract). Only the
        # not-in-processing branch GCs the count.
        client = fakeredis.FakeRedis()
        gateway = RedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=1,
        )
        queue = RedisMessageQueue("test", gateway=gateway, deduplication=False)
        pending, processing = queue.key.pending, queue.key.processing

        queue.publish("retry-me")
        first = gateway._claim_visible_message(pending, processing, claim_id=uuid.uuid4().hex)
        assert first is not None
        delivery_counts_key = gateway._delivery_counts_key(processing)
        assert client.hget(delivery_counts_key, first.stored_message) == b"1"

        # Lease expires; the message is still in processing, so reclaim REDELIVERS
        # it and the count must survive and increment.
        client.zadd(gateway._lease_deadlines_key(processing), {first.stored_message: 0})
        second = gateway._claim_visible_message(pending, processing, claim_id=uuid.uuid4().hex)
        assert second is not None
        assert second.stored_message == first.stored_message
        assert second.lease_token != first.lease_token
        assert client.hget(delivery_counts_key, second.stored_message) == b"2"


class TestAsyncReclaimGCsDeliveryCountsForExternallyRemovedMessage:
    @pytest.mark.asyncio
    async def test_external_lrem_then_reclaim_gcs_delivery_counts_and_unwedges_counter(self):
        client = fakeredis.FakeAsyncRedis()
        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=1,
        )
        queue = AsyncRedisMessageQueue("test", gateway=gateway, deduplication=False)
        pending, processing = queue.key.pending, queue.key.processing

        await queue.publish("victim")
        claimed = await gateway._claim_visible_message(pending, processing, claim_id=uuid.uuid4().hex)
        assert claimed is not None
        assert await client.hlen(gateway._delivery_counts_key(processing)) == 1

        assert await client.lrem(processing, 1, claimed.stored_message) == 1

        await client.zadd(gateway._lease_deadlines_key(processing), {claimed.stored_message: 0})
        gc_claim = await gateway._claim_visible_message(pending, processing, claim_id=uuid.uuid4().hex)
        assert gc_claim is None

        assert await client.hlen(gateway._delivery_counts_key(processing)) == 0
        assert await client.zcard(gateway._lease_deadlines_key(processing)) == 0
        assert await client.hlen(gateway._lease_tokens_key(processing)) == 0
        assert await client.llen(processing) == 0
        assert await client.llen(pending) == 0

        assert await gateway._cleanup_drained_lease_token_counter(processing) is True
        assert await client.get(gateway._lease_token_counter_key(processing)) is None

    @pytest.mark.asyncio
    async def test_non_lease_ack_then_reclaim_gcs_delivery_counts(self):
        client = fakeredis.FakeAsyncRedis()
        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=1,
        )
        queue = AsyncRedisMessageQueue("test", gateway=gateway, deduplication=False)
        pending, processing = queue.key.pending, queue.key.processing

        await queue.publish("victim")
        claimed = await gateway._claim_visible_message(pending, processing, claim_id=uuid.uuid4().hex)
        assert claimed is not None

        assert await gateway.remove_message(processing, claimed.stored_message) is True
        assert await client.llen(processing) == 0
        assert await client.hlen(gateway._delivery_counts_key(processing)) == 1

        await client.zadd(gateway._lease_deadlines_key(processing), {claimed.stored_message: 0})
        gc_claim = await gateway._claim_visible_message(pending, processing, claim_id=uuid.uuid4().hex)
        assert gc_claim is None

        assert await client.hlen(gateway._delivery_counts_key(processing)) == 0
        assert await client.zcard(gateway._lease_deadlines_key(processing)) == 0
        assert await client.hlen(gateway._lease_tokens_key(processing)) == 0
        assert await client.llen(pending) == 0
        assert await gateway._cleanup_drained_lease_token_counter(processing) is True

    @pytest.mark.asyncio
    async def test_redelivery_path_preserves_delivery_count(self):
        client = fakeredis.FakeAsyncRedis()
        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=1,
        )
        queue = AsyncRedisMessageQueue("test", gateway=gateway, deduplication=False)
        pending, processing = queue.key.pending, queue.key.processing

        await queue.publish("retry-me")
        first = await gateway._claim_visible_message(pending, processing, claim_id=uuid.uuid4().hex)
        assert first is not None
        delivery_counts_key = gateway._delivery_counts_key(processing)
        assert await client.hget(delivery_counts_key, first.stored_message) == b"1"

        await client.zadd(gateway._lease_deadlines_key(processing), {first.stored_message: 0})
        second = await gateway._claim_visible_message(pending, processing, claim_id=uuid.uuid4().hex)
        assert second is not None
        assert second.stored_message == first.stored_message
        assert second.lease_token != first.lease_token
        assert await client.hget(delivery_counts_key, second.stored_message) == b"2"
