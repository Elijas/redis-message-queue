import fakeredis
import pytest

from redis_message_queue._redis_gateway import RedisGateway
from redis_message_queue.asyncio._redis_gateway import (
    RedisGateway as AsyncRedisGateway,
)
from redis_message_queue.asyncio.redis_message_queue import (
    RedisMessageQueue as AsyncRedisMessageQueue,
)
from redis_message_queue.redis_message_queue import RedisMessageQueue


def _no_retry(func):
    return func


class TestSyncVisibilityTimeoutValidation:
    def test_queue_rejects_zero_visibility_timeout(self):
        with pytest.raises(ValueError, match="visibility_timeout_seconds"):
            RedisMessageQueue("test", client=fakeredis.FakeRedis(), visibility_timeout_seconds=0)

    def test_queue_rejects_non_int_visibility_timeout(self):
        with pytest.raises(TypeError, match="visibility_timeout_seconds"):
            RedisMessageQueue("test", client=fakeredis.FakeRedis(), visibility_timeout_seconds=1.5)

    def test_queue_rejects_visibility_timeout_with_gateway(self):
        gateway = RedisGateway(redis_client=fakeredis.FakeRedis(), retry_strategy=_no_retry)
        with pytest.raises(ValueError, match="visibility_timeout_seconds"):
            RedisMessageQueue("test", gateway=gateway, visibility_timeout_seconds=30)

    def test_gateway_rejects_zero_visibility_timeout(self):
        with pytest.raises(ValueError, match="message_visibility_timeout_seconds"):
            RedisGateway(
                redis_client=fakeredis.FakeRedis(),
                retry_strategy=_no_retry,
                message_visibility_timeout_seconds=0,
            )

    def test_gateway_rejects_non_int_visibility_timeout(self):
        with pytest.raises(TypeError, match="message_visibility_timeout_seconds"):
            RedisGateway(
                redis_client=fakeredis.FakeRedis(),
                retry_strategy=_no_retry,
                message_visibility_timeout_seconds=1.5,
            )


class TestAsyncVisibilityTimeoutValidation:
    def test_queue_rejects_zero_visibility_timeout(self):
        with pytest.raises(ValueError, match="visibility_timeout_seconds"):
            AsyncRedisMessageQueue("test", client=fakeredis.FakeAsyncRedis(), visibility_timeout_seconds=0)

    def test_queue_rejects_non_int_visibility_timeout(self):
        with pytest.raises(TypeError, match="visibility_timeout_seconds"):
            AsyncRedisMessageQueue("test", client=fakeredis.FakeAsyncRedis(), visibility_timeout_seconds=1.5)

    def test_queue_rejects_visibility_timeout_with_gateway(self):
        gateway = AsyncRedisGateway(redis_client=fakeredis.FakeAsyncRedis(), retry_strategy=_no_retry)
        with pytest.raises(ValueError, match="visibility_timeout_seconds"):
            AsyncRedisMessageQueue("test", gateway=gateway, visibility_timeout_seconds=30)

    def test_gateway_rejects_zero_visibility_timeout(self):
        with pytest.raises(ValueError, match="message_visibility_timeout_seconds"):
            AsyncRedisGateway(
                redis_client=fakeredis.FakeAsyncRedis(),
                retry_strategy=_no_retry,
                message_visibility_timeout_seconds=0,
            )

    def test_gateway_rejects_non_int_visibility_timeout(self):
        with pytest.raises(TypeError, match="message_visibility_timeout_seconds"):
            AsyncRedisGateway(
                redis_client=fakeredis.FakeAsyncRedis(),
                retry_strategy=_no_retry,
                message_visibility_timeout_seconds=1.5,
            )


class TestSyncVisibilityTimeoutRecovery:
    def test_gateway_reclaims_expired_message_with_new_lease_token(self):
        client = fakeredis.FakeRedis()
        gateway = RedisGateway(
            redis_client=client,
            retry_strategy=_no_retry,
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
            retry_strategy=_no_retry,
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
            retry_strategy=_no_retry,
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


class TestSyncBatchReclaim:
    def test_single_poll_reclaims_multiple_expired_messages(self):
        client = fakeredis.FakeRedis()
        gateway = RedisGateway(
            redis_client=client,
            retry_strategy=_no_retry,
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
            retry_strategy=_no_retry,
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
            retry_strategy=_no_retry,
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
            retry_strategy=_no_retry,
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
            retry_strategy=_no_retry,
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


class TestAsyncBatchReclaim:
    @pytest.mark.asyncio
    async def test_single_poll_reclaims_multiple_expired_messages(self):
        client = fakeredis.FakeAsyncRedis()
        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_strategy=_no_retry,
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
            retry_strategy=_no_retry,
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
            retry_strategy=_no_retry,
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
            retry_strategy=_no_retry,
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
            retry_strategy=_no_retry,
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


class TestAsyncVisibilityTimeoutRecovery:
    @pytest.mark.asyncio
    async def test_gateway_reclaims_expired_message_with_new_lease_token(self):
        client = fakeredis.FakeAsyncRedis()
        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_strategy=_no_retry,
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
            retry_strategy=_no_retry,
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
            retry_strategy=_no_retry,
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
