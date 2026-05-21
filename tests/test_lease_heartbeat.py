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
from redis_message_queue.redis_message_queue import RedisMessageQueue, _LeaseHeartbeat


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
                visibility_timeout_seconds=None,
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
        with pytest.raises(ValueError, match="less than half"):
            RedisMessageQueue(
                "test",
                client=fakeredis.FakeRedis(),
                visibility_timeout_seconds=30,
                heartbeat_interval_seconds=20,
            )

    def test_queue_rejects_heartbeat_with_gateway_without_visibility_timeout(self):
        gateway = RedisGateway(redis_client=fakeredis.FakeRedis(), retry_budget_seconds=0)
        with pytest.raises(ValueError, match="configured visibility timeout"):
            RedisMessageQueue(
                "test",
                gateway=gateway,
                heartbeat_interval_seconds=0.5,
            )

    def test_queue_rejects_heartbeat_with_gateway_inherited_visibility_timeout_default(self):
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

            def renew_message_lease(self, queue, message, lease_token, **_kwargs):
                return True

            def wait_for_message_and_move(self, from_queue, to_queue):
                return None

            def trim_queue(self, queue, max_length):
                pass

        with pytest.raises(ValueError, match="configured visibility timeout"):
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
                visibility_timeout_seconds=None,
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
        with pytest.raises(ValueError, match="less than half"):
            AsyncRedisMessageQueue(
                "test",
                client=fakeredis.FakeAsyncRedis(),
                visibility_timeout_seconds=30,
                heartbeat_interval_seconds=20,
            )

    def test_queue_rejects_heartbeat_with_gateway_without_visibility_timeout(self):
        gateway = AsyncRedisGateway(redis_client=fakeredis.FakeAsyncRedis(), retry_budget_seconds=0)
        with pytest.raises(ValueError, match="configured visibility timeout"):
            AsyncRedisMessageQueue(
                "test",
                gateway=gateway,
                heartbeat_interval_seconds=0.5,
            )

    def test_queue_rejects_heartbeat_with_gateway_inherited_visibility_timeout_default(self):
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

            async def renew_message_lease(self, queue, message, lease_token, **_kwargs):
                return True

            async def wait_for_message_and_move(self, from_queue, to_queue):
                return None

            async def trim_queue(self, queue, max_length):
                pass

        with pytest.raises(ValueError, match="configured visibility timeout"):
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


class TestSyncOnHeartbeatFailureValidation:
    def test_non_callable_raises_type_error(self):
        with pytest.raises(TypeError, match="'on_heartbeat_failure' must be callable"):
            RedisMessageQueue(
                "test",
                client=fakeredis.FakeRedis(),
                visibility_timeout_seconds=30,
                heartbeat_interval_seconds=5,
                on_heartbeat_failure=42,
            )

    @pytest.mark.parametrize("invalid_value", ["not_callable", True, 3.14, [1, 2], {"a": 1}])
    def test_non_callable_values_raise_type_error(self, invalid_value):
        with pytest.raises(TypeError, match="'on_heartbeat_failure' must be callable"):
            RedisMessageQueue(
                "test",
                client=fakeredis.FakeRedis(),
                visibility_timeout_seconds=30,
                heartbeat_interval_seconds=5,
                on_heartbeat_failure=invalid_value,
            )

    def test_async_callable_raises_type_error(self):
        async def async_callback():
            pass

        with pytest.raises(TypeError, match="async callable"):
            RedisMessageQueue(
                "test",
                client=fakeredis.FakeRedis(),
                visibility_timeout_seconds=30,
                heartbeat_interval_seconds=5,
                on_heartbeat_failure=async_callback,
            )

    def test_async_callable_object_raises_type_error(self):
        class AsyncCallback:
            async def __call__(self):
                pass

        with pytest.raises(TypeError, match="async callable"):
            RedisMessageQueue(
                "test",
                client=fakeredis.FakeRedis(),
                visibility_timeout_seconds=30,
                heartbeat_interval_seconds=5,
                on_heartbeat_failure=AsyncCallback(),
            )

    def test_without_heartbeat_interval_raises_value_error(self):
        with pytest.raises(ValueError, match="requires 'heartbeat_interval_seconds'"):
            RedisMessageQueue(
                "test",
                client=fakeredis.FakeRedis(),
                visibility_timeout_seconds=30,
                on_heartbeat_failure=lambda: None,
            )

    def test_none_is_accepted(self):
        q = RedisMessageQueue(
            "test",
            client=fakeredis.FakeRedis(),
            visibility_timeout_seconds=30,
            heartbeat_interval_seconds=5,
            on_heartbeat_failure=None,
        )
        assert q._on_heartbeat_failure is None

    def test_lambda_is_accepted(self):
        fn = lambda: None
        q = RedisMessageQueue(
            "test",
            client=fakeredis.FakeRedis(),
            visibility_timeout_seconds=30,
            heartbeat_interval_seconds=5,
            on_heartbeat_failure=fn,
        )
        assert q._on_heartbeat_failure is fn

    def test_callable_object_is_accepted(self):
        class MyCallback:
            def __call__(self):
                pass

        obj = MyCallback()
        q = RedisMessageQueue(
            "test",
            client=fakeredis.FakeRedis(),
            visibility_timeout_seconds=30,
            heartbeat_interval_seconds=5,
            on_heartbeat_failure=obj,
        )
        assert q._on_heartbeat_failure is obj


class TestAsyncOnHeartbeatFailureValidation:
    def test_non_callable_raises_type_error(self):
        with pytest.raises(TypeError, match="'on_heartbeat_failure' must be callable"):
            AsyncRedisMessageQueue(
                "test",
                client=fakeredis.FakeAsyncRedis(),
                visibility_timeout_seconds=30,
                heartbeat_interval_seconds=5,
                on_heartbeat_failure=42,
            )

    @pytest.mark.parametrize("invalid_value", ["not_callable", True, 3.14, [1, 2], {"a": 1}])
    def test_non_callable_values_raise_type_error(self, invalid_value):
        with pytest.raises(TypeError, match="'on_heartbeat_failure' must be callable"):
            AsyncRedisMessageQueue(
                "test",
                client=fakeredis.FakeAsyncRedis(),
                visibility_timeout_seconds=30,
                heartbeat_interval_seconds=5,
                on_heartbeat_failure=invalid_value,
            )

    def test_async_callable_is_accepted(self):
        async def async_callback():
            pass

        q = AsyncRedisMessageQueue(
            "test",
            client=fakeredis.FakeAsyncRedis(),
            visibility_timeout_seconds=30,
            heartbeat_interval_seconds=5,
            on_heartbeat_failure=async_callback,
        )
        assert q._on_heartbeat_failure is async_callback

    def test_async_callable_object_is_accepted(self):
        class AsyncCallback:
            async def __call__(self):
                pass

        callback = AsyncCallback()
        q = AsyncRedisMessageQueue(
            "test",
            client=fakeredis.FakeAsyncRedis(),
            visibility_timeout_seconds=30,
            heartbeat_interval_seconds=5,
            on_heartbeat_failure=callback,
        )
        assert q._on_heartbeat_failure is callback

    def test_without_heartbeat_interval_raises_value_error(self):
        with pytest.raises(ValueError, match="requires 'heartbeat_interval_seconds'"):
            AsyncRedisMessageQueue(
                "test",
                client=fakeredis.FakeAsyncRedis(),
                visibility_timeout_seconds=30,
                on_heartbeat_failure=lambda: None,
            )

    def test_none_is_accepted(self):
        q = AsyncRedisMessageQueue(
            "test",
            client=fakeredis.FakeAsyncRedis(),
            visibility_timeout_seconds=30,
            heartbeat_interval_seconds=5,
            on_heartbeat_failure=None,
        )
        assert q._on_heartbeat_failure is None

    def test_lambda_is_accepted(self):
        fn = lambda: None
        q = AsyncRedisMessageQueue(
            "test",
            client=fakeredis.FakeAsyncRedis(),
            visibility_timeout_seconds=30,
            heartbeat_interval_seconds=5,
            on_heartbeat_failure=fn,
        )
        assert q._on_heartbeat_failure is fn


class TestSyncLeaseHeartbeatLifecycle:
    def test_stop_without_start_is_a_noop(self):
        heartbeat = _LeaseHeartbeat(
            interval_seconds=0.05,
            renew_message_lease=lambda: True,
        )
        # Must not raise, and must not call join() on an unstarted thread.
        heartbeat.stop()
        assert heartbeat._thread.ident is None

    def test_started_flag_is_derived_from_thread_ident(self):
        heartbeat = _LeaseHeartbeat(
            interval_seconds=0.05,
            renew_message_lease=lambda: True,
        )
        assert heartbeat._thread.ident is None

        heartbeat.start()
        try:
            # Thread.ident is assigned by Thread.start() itself, so it becomes
            # non-None synchronously before start() returns — no race window.
            assert heartbeat._thread.ident is not None
        finally:
            heartbeat.stop()

        # ident survives the thread exit, so stop() can still detect prior start.
        assert heartbeat._thread.ident is not None

    def test_stop_is_idempotent_after_thread_exits(self):
        heartbeat = _LeaseHeartbeat(
            interval_seconds=0.05,
            renew_message_lease=lambda: True,
        )
        heartbeat.start()
        heartbeat.stop()
        heartbeat.stop()  # must not raise


class TestSyncLeaseRenewal:
    def test_gateway_renew_message_lease_extends_deadline(self):
        client = fakeredis.FakeRedis()
        gateway = RedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
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
            retry_budget_seconds=0,
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
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=1,
        )
        rival_gateway = RedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
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
            retry_budget_seconds=0,
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

        result = await gateway.renew_message_lease(queue.key.processing, claimed.stored_message, claimed.lease_token)
        assert result is True
        assert await client.zscore(lease_key, processing_message) > original_deadline

    @pytest.mark.asyncio
    async def test_gateway_rejects_stale_renewal_after_redelivery(self):
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
        lease_key = gateway._lease_deadlines_key(queue.key.processing)
        await client.zadd(lease_key, {processing_message: 0})
        second = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        current_deadline = await client.zscore(lease_key, processing_message)

        stale_result = await gateway.renew_message_lease(queue.key.processing, first.stored_message, first.lease_token)
        assert stale_result is False
        assert await client.zscore(lease_key, processing_message) == current_deadline
        fresh_result = await gateway.renew_message_lease(
            queue.key.processing, second.stored_message, second.lease_token
        )
        assert fresh_result is True

    @pytest.mark.asyncio
    async def test_queue_heartbeat_prevents_redelivery_of_long_running_message(self):
        client = fakeredis.FakeAsyncRedis()
        queue_gateway = AsyncRedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=1,
        )
        rival_gateway = AsyncRedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
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
