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
