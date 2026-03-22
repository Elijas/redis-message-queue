import asyncio
import time

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


class TestSyncHeartbeatValidation:
    def test_queue_rejects_non_numeric_heartbeat_interval(self):
        with pytest.raises(TypeError, match="heartbeat_interval_seconds"):
            RedisMessageQueue(
                "test",
                client=fakeredis.FakeRedis(),
                visibility_timeout_seconds=30,
                heartbeat_interval_seconds="fast",
            )

    def test_queue_rejects_zero_heartbeat_interval(self):
        with pytest.raises(ValueError, match="heartbeat_interval_seconds"):
            RedisMessageQueue(
                "test",
                client=fakeredis.FakeRedis(),
                visibility_timeout_seconds=30,
                heartbeat_interval_seconds=0,
            )

    def test_queue_rejects_heartbeat_without_visibility_timeout_for_client_path(self):
        with pytest.raises(ValueError, match="requires 'visibility_timeout_seconds'"):
            RedisMessageQueue(
                "test",
                client=fakeredis.FakeRedis(),
                heartbeat_interval_seconds=0.5,
            )

    def test_queue_rejects_negative_heartbeat_interval(self):
        with pytest.raises(ValueError, match="heartbeat_interval_seconds"):
            RedisMessageQueue(
                "test",
                client=fakeredis.FakeRedis(),
                visibility_timeout_seconds=30,
                heartbeat_interval_seconds=-1,
            )

    def test_queue_rejects_heartbeat_not_less_than_visibility_timeout(self):
        with pytest.raises(ValueError, match="no more than half"):
            RedisMessageQueue(
                "test",
                client=fakeredis.FakeRedis(),
                visibility_timeout_seconds=30,
                heartbeat_interval_seconds=20,
            )

    def test_queue_rejects_heartbeat_with_gateway_without_visibility_timeout(self):
        gateway = RedisGateway(redis_client=fakeredis.FakeRedis(), retry_strategy=_no_retry)
        with pytest.raises(ValueError, match="configured visibility timeout"):
            RedisMessageQueue(
                "test",
                gateway=gateway,
                heartbeat_interval_seconds=0.5,
            )

    def test_queue_rejects_heartbeat_with_gateway_without_public_visibility_timeout(self):
        from redis_message_queue._abstract_redis_gateway import AbstractRedisGateway

        class OpaqueGateway(AbstractRedisGateway):
            def publish_message(self, queue, message, dedup_key):
                return True

            def add_message(self, queue, message):
                pass

            def move_message(self, from_queue, to_queue, message, *, lease_token=None):
                pass

            def remove_message(self, queue, message, *, lease_token=None):
                pass

            def renew_message_lease(self, queue, message, lease_token):
                return True

            def wait_for_message_and_move(self, from_queue, to_queue):
                return None

        with pytest.raises(ValueError, match="expose 'message_visibility_timeout_seconds'"):
            RedisMessageQueue(
                "test",
                gateway=OpaqueGateway(),
                heartbeat_interval_seconds=0.5,
            )

    def test_queue_rejects_nan_heartbeat_interval(self):
        with pytest.raises(ValueError, match="finite number"):
            RedisMessageQueue(
                "test",
                client=fakeredis.FakeRedis(),
                visibility_timeout_seconds=30,
                heartbeat_interval_seconds=float("nan"),
            )

    def test_queue_rejects_inf_heartbeat_interval(self):
        with pytest.raises(ValueError, match="finite number"):
            RedisMessageQueue(
                "test",
                client=fakeredis.FakeRedis(),
                visibility_timeout_seconds=30,
                heartbeat_interval_seconds=float("inf"),
            )

    def test_queue_rejects_bool_heartbeat_interval(self):
        with pytest.raises(TypeError, match="heartbeat_interval_seconds"):
            RedisMessageQueue(
                "test",
                client=fakeredis.FakeRedis(),
                visibility_timeout_seconds=30,
                heartbeat_interval_seconds=True,
            )


class TestAsyncHeartbeatValidation:
    def test_queue_rejects_non_numeric_heartbeat_interval(self):
        with pytest.raises(TypeError, match="heartbeat_interval_seconds"):
            AsyncRedisMessageQueue(
                "test",
                client=fakeredis.FakeAsyncRedis(),
                visibility_timeout_seconds=30,
                heartbeat_interval_seconds="fast",
            )

    def test_queue_rejects_zero_heartbeat_interval(self):
        with pytest.raises(ValueError, match="heartbeat_interval_seconds"):
            AsyncRedisMessageQueue(
                "test",
                client=fakeredis.FakeAsyncRedis(),
                visibility_timeout_seconds=30,
                heartbeat_interval_seconds=0,
            )

    def test_queue_rejects_heartbeat_without_visibility_timeout_for_client_path(self):
        with pytest.raises(ValueError, match="requires 'visibility_timeout_seconds'"):
            AsyncRedisMessageQueue(
                "test",
                client=fakeredis.FakeAsyncRedis(),
                heartbeat_interval_seconds=0.5,
            )

    def test_queue_rejects_negative_heartbeat_interval(self):
        with pytest.raises(ValueError, match="heartbeat_interval_seconds"):
            AsyncRedisMessageQueue(
                "test",
                client=fakeredis.FakeAsyncRedis(),
                visibility_timeout_seconds=30,
                heartbeat_interval_seconds=-1,
            )

    def test_queue_rejects_heartbeat_not_less_than_visibility_timeout(self):
        with pytest.raises(ValueError, match="no more than half"):
            AsyncRedisMessageQueue(
                "test",
                client=fakeredis.FakeAsyncRedis(),
                visibility_timeout_seconds=30,
                heartbeat_interval_seconds=20,
            )

    def test_queue_rejects_heartbeat_with_gateway_without_visibility_timeout(self):
        gateway = AsyncRedisGateway(redis_client=fakeredis.FakeAsyncRedis(), retry_strategy=_no_retry)
        with pytest.raises(ValueError, match="configured visibility timeout"):
            AsyncRedisMessageQueue(
                "test",
                gateway=gateway,
                heartbeat_interval_seconds=0.5,
            )

    def test_queue_rejects_heartbeat_with_gateway_without_public_visibility_timeout(self):
        from redis_message_queue.asyncio._abstract_redis_gateway import AbstractRedisGateway

        class OpaqueGateway(AbstractRedisGateway):
            async def publish_message(self, queue, message, dedup_key):
                return True

            async def add_message(self, queue, message):
                pass

            async def move_message(self, from_queue, to_queue, message, *, lease_token=None):
                pass

            async def remove_message(self, queue, message, *, lease_token=None):
                pass

            async def renew_message_lease(self, queue, message, lease_token):
                return True

            async def wait_for_message_and_move(self, from_queue, to_queue):
                return None

        with pytest.raises(ValueError, match="expose 'message_visibility_timeout_seconds'"):
            AsyncRedisMessageQueue(
                "test",
                gateway=OpaqueGateway(),
                heartbeat_interval_seconds=0.5,
            )

    def test_queue_rejects_nan_heartbeat_interval(self):
        with pytest.raises(ValueError, match="finite number"):
            AsyncRedisMessageQueue(
                "test",
                client=fakeredis.FakeAsyncRedis(),
                visibility_timeout_seconds=30,
                heartbeat_interval_seconds=float("nan"),
            )

    def test_queue_rejects_inf_heartbeat_interval(self):
        with pytest.raises(ValueError, match="finite number"):
            AsyncRedisMessageQueue(
                "test",
                client=fakeredis.FakeAsyncRedis(),
                visibility_timeout_seconds=30,
                heartbeat_interval_seconds=float("inf"),
            )

    def test_queue_rejects_bool_heartbeat_interval(self):
        with pytest.raises(TypeError, match="heartbeat_interval_seconds"):
            AsyncRedisMessageQueue(
                "test",
                client=fakeredis.FakeAsyncRedis(),
                visibility_timeout_seconds=30,
                heartbeat_interval_seconds=True,
            )


class TestSyncLeaseRenewal:
    def test_gateway_renew_message_lease_extends_deadline(self):
        client = fakeredis.FakeRedis()
        gateway = RedisGateway(
            redis_client=client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=30,
        )
        queue = RedisMessageQueue("test", gateway=gateway)
        queue.publish("hello")

        claimed = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        processing_message = client.lindex(queue.key.processing, 0)
        lease_key = gateway._lease_deadlines_key(queue.key.processing)
        original_deadline = client.zscore(lease_key, processing_message)

        time.sleep(0.01)

        assert gateway.renew_message_lease(queue.key.processing, claimed.stored_message, claimed.lease_token) is True
        assert client.zscore(lease_key, processing_message) > original_deadline

    def test_gateway_rejects_stale_renewal_after_redelivery(self):
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
        lease_key = gateway._lease_deadlines_key(queue.key.processing)
        client.zadd(lease_key, {processing_message: 0})
        second = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        current_deadline = client.zscore(lease_key, processing_message)

        assert gateway.renew_message_lease(queue.key.processing, first.stored_message, first.lease_token) is False
        assert client.zscore(lease_key, processing_message) == current_deadline
        assert gateway.renew_message_lease(queue.key.processing, second.stored_message, second.lease_token) is True

    def test_queue_heartbeat_prevents_redelivery_of_long_running_message(self):
        client = fakeredis.FakeRedis()
        queue_gateway = RedisGateway(
            redis_client=client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=1,
        )
        rival_gateway = RedisGateway(
            redis_client=client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=1,
        )
        queue = RedisMessageQueue(
            "test",
            gateway=queue_gateway,
            heartbeat_interval_seconds=0.2,
        )
        queue.publish("hello")

        with queue.process_message() as message:
            assert message == b"hello"
            time.sleep(1.2)
            assert rival_gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing) is None

        assert client.llen(queue.key.processing) == 0


class TestAsyncLeaseRenewal:
    @pytest.mark.asyncio
    async def test_gateway_renew_message_lease_extends_deadline(self):
        client = fakeredis.FakeAsyncRedis()
        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=30,
        )
        queue = AsyncRedisMessageQueue("test", gateway=gateway)
        await queue.publish("hello")

        claimed = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        processing_message = await client.lindex(queue.key.processing, 0)
        lease_key = gateway._lease_deadlines_key(queue.key.processing)
        original_deadline = await client.zscore(lease_key, processing_message)

        await asyncio.sleep(0.01)

        assert await gateway.renew_message_lease(queue.key.processing, claimed.stored_message, claimed.lease_token) is True
        assert await client.zscore(lease_key, processing_message) > original_deadline

    @pytest.mark.asyncio
    async def test_gateway_rejects_stale_renewal_after_redelivery(self):
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
        lease_key = gateway._lease_deadlines_key(queue.key.processing)
        await client.zadd(lease_key, {processing_message: 0})
        second = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        current_deadline = await client.zscore(lease_key, processing_message)

        assert await gateway.renew_message_lease(queue.key.processing, first.stored_message, first.lease_token) is False
        assert await client.zscore(lease_key, processing_message) == current_deadline
        assert await gateway.renew_message_lease(queue.key.processing, second.stored_message, second.lease_token) is True

    @pytest.mark.asyncio
    async def test_queue_heartbeat_prevents_redelivery_of_long_running_message(self):
        client = fakeredis.FakeAsyncRedis()
        queue_gateway = AsyncRedisGateway(
            redis_client=client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=1,
        )
        rival_gateway = AsyncRedisGateway(
            redis_client=client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=1,
        )
        queue = AsyncRedisMessageQueue(
            "test",
            gateway=queue_gateway,
            heartbeat_interval_seconds=0.2,
        )
        await queue.publish("hello")

        async with queue.process_message() as message:
            assert message == b"hello"
            await asyncio.sleep(1.2)
            assert await rival_gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing) is None

        assert await client.llen(queue.key.processing) == 0
