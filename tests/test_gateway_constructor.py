import fakeredis
import pytest

from redis_message_queue._redis_gateway import RedisGateway
from redis_message_queue.asyncio._redis_gateway import (
    RedisGateway as AsyncRedisGateway,
)
from redis_message_queue.interrupt_handler._interface import (
    BaseGracefulInterruptHandler,
)


def _no_retry(func):
    return func


class _FakeInterrupt(BaseGracefulInterruptHandler):
    def is_interrupted(self) -> bool:
        return False


class TestSyncGatewayConstructorValidation:
    def test_non_callable_retry_strategy_raises_type_error(self):
        with pytest.raises(TypeError, match="retry_strategy"):
            RedisGateway(redis_client=fakeredis.FakeRedis(), retry_strategy=42)

    def test_non_interrupt_handler_raises_type_error(self):
        with pytest.raises(TypeError, match="interrupt"):
            RedisGateway(redis_client=fakeredis.FakeRedis(), interrupt="bad")

    def test_retry_strategy_and_interrupt_are_accepted(self):
        interrupt = _FakeInterrupt()
        gateway = RedisGateway(
            redis_client=fakeredis.FakeRedis(),
            retry_strategy=_no_retry,
            interrupt=interrupt,
        )
        assert gateway._interrupt is interrupt
        assert callable(gateway._retry_strategy)

    def test_zero_dedup_ttl_raises_value_error(self):
        with pytest.raises(ValueError, match="message_deduplication_log_ttl_seconds"):
            RedisGateway(
                redis_client=fakeredis.FakeRedis(),
                retry_strategy=_no_retry,
                message_deduplication_log_ttl_seconds=0,
            )

    def test_negative_dedup_ttl_raises_value_error(self):
        with pytest.raises(ValueError, match="message_deduplication_log_ttl_seconds"):
            RedisGateway(
                redis_client=fakeredis.FakeRedis(),
                retry_strategy=_no_retry,
                message_deduplication_log_ttl_seconds=-1,
            )

    def test_float_dedup_ttl_raises_type_error(self):
        with pytest.raises(TypeError, match="message_deduplication_log_ttl_seconds"):
            RedisGateway(
                redis_client=fakeredis.FakeRedis(),
                retry_strategy=_no_retry,
                message_deduplication_log_ttl_seconds=1.5,
            )

    def test_bool_dedup_ttl_raises_type_error(self):
        with pytest.raises(TypeError, match="message_deduplication_log_ttl_seconds"):
            RedisGateway(
                redis_client=fakeredis.FakeRedis(),
                retry_strategy=_no_retry,
                message_deduplication_log_ttl_seconds=True,
            )

    def test_negative_wait_interval_raises_value_error(self):
        with pytest.raises(ValueError, match="message_wait_interval_seconds"):
            RedisGateway(
                redis_client=fakeredis.FakeRedis(),
                retry_strategy=_no_retry,
                message_wait_interval_seconds=-1,
            )

    def test_float_wait_interval_raises_type_error(self):
        with pytest.raises(TypeError, match="message_wait_interval_seconds"):
            RedisGateway(
                redis_client=fakeredis.FakeRedis(),
                retry_strategy=_no_retry,
                message_wait_interval_seconds=1.5,
            )

    def test_bool_wait_interval_raises_type_error(self):
        with pytest.raises(TypeError, match="message_wait_interval_seconds"):
            RedisGateway(
                redis_client=fakeredis.FakeRedis(),
                retry_strategy=_no_retry,
                message_wait_interval_seconds=True,
            )

    def test_negative_visibility_timeout_raises_value_error(self):
        with pytest.raises(ValueError, match="message_visibility_timeout_seconds"):
            RedisGateway(
                redis_client=fakeredis.FakeRedis(),
                retry_strategy=_no_retry,
                message_visibility_timeout_seconds=-1,
            )

    def test_bool_visibility_timeout_raises_type_error(self):
        with pytest.raises(TypeError, match="message_visibility_timeout_seconds"):
            RedisGateway(
                redis_client=fakeredis.FakeRedis(),
                retry_strategy=_no_retry,
                message_visibility_timeout_seconds=True,
            )

    def test_async_retry_strategy_raises_type_error(self):
        async def async_retry(func):
            return func

        with pytest.raises(TypeError, match="async callable"):
            RedisGateway(redis_client=fakeredis.FakeRedis(), retry_strategy=async_retry)

    def test_async_callable_object_retry_strategy_raises_type_error(self):
        class AsyncRetry:
            async def __call__(self, func):
                return func

        with pytest.raises(TypeError, match="async callable"):
            RedisGateway(redis_client=fakeredis.FakeRedis(), retry_strategy=AsyncRetry())

    def test_async_redis_client_raises_type_error(self):
        with pytest.raises(TypeError, match="async Redis client"):
            RedisGateway(redis_client=fakeredis.FakeAsyncRedis())

    def test_sync_pipeline_raises_type_error(self):
        pipeline = fakeredis.FakeRedis().pipeline()
        with pytest.raises(TypeError, match="Pipeline"):
            RedisGateway(redis_client=pipeline)

    def test_valid_defaults_accepted(self):
        gateway = RedisGateway(redis_client=fakeredis.FakeRedis(), retry_strategy=_no_retry)
        assert gateway.message_visibility_timeout_seconds is None

    def test_valid_explicit_params_accepted(self):
        gateway = RedisGateway(
            redis_client=fakeredis.FakeRedis(),
            retry_strategy=_no_retry,
            message_deduplication_log_ttl_seconds=120,
            message_wait_interval_seconds=10,
            message_visibility_timeout_seconds=30,
        )
        assert gateway.message_visibility_timeout_seconds == 30


class TestAsyncGatewayConstructorValidation:
    def test_non_callable_retry_strategy_raises_type_error(self):
        with pytest.raises(TypeError, match="retry_strategy"):
            AsyncRedisGateway(redis_client=fakeredis.FakeAsyncRedis(), retry_strategy=42)

    def test_non_interrupt_handler_raises_type_error(self):
        with pytest.raises(TypeError, match="interrupt"):
            AsyncRedisGateway(redis_client=fakeredis.FakeAsyncRedis(), interrupt="bad")

    def test_retry_strategy_and_interrupt_are_accepted(self):
        interrupt = _FakeInterrupt()
        gateway = AsyncRedisGateway(
            redis_client=fakeredis.FakeAsyncRedis(),
            retry_strategy=_no_retry,
            interrupt=interrupt,
        )
        assert gateway._interrupt is interrupt
        assert callable(gateway._retry_strategy)

    def test_zero_dedup_ttl_raises_value_error(self):
        with pytest.raises(ValueError, match="message_deduplication_log_ttl_seconds"):
            AsyncRedisGateway(
                redis_client=fakeredis.FakeAsyncRedis(),
                retry_strategy=_no_retry,
                message_deduplication_log_ttl_seconds=0,
            )

    def test_negative_dedup_ttl_raises_value_error(self):
        with pytest.raises(ValueError, match="message_deduplication_log_ttl_seconds"):
            AsyncRedisGateway(
                redis_client=fakeredis.FakeAsyncRedis(),
                retry_strategy=_no_retry,
                message_deduplication_log_ttl_seconds=-1,
            )

    def test_float_dedup_ttl_raises_type_error(self):
        with pytest.raises(TypeError, match="message_deduplication_log_ttl_seconds"):
            AsyncRedisGateway(
                redis_client=fakeredis.FakeAsyncRedis(),
                retry_strategy=_no_retry,
                message_deduplication_log_ttl_seconds=1.5,
            )

    def test_bool_dedup_ttl_raises_type_error(self):
        with pytest.raises(TypeError, match="message_deduplication_log_ttl_seconds"):
            AsyncRedisGateway(
                redis_client=fakeredis.FakeAsyncRedis(),
                retry_strategy=_no_retry,
                message_deduplication_log_ttl_seconds=True,
            )

    def test_negative_wait_interval_raises_value_error(self):
        with pytest.raises(ValueError, match="message_wait_interval_seconds"):
            AsyncRedisGateway(
                redis_client=fakeredis.FakeAsyncRedis(),
                retry_strategy=_no_retry,
                message_wait_interval_seconds=-1,
            )

    def test_float_wait_interval_raises_type_error(self):
        with pytest.raises(TypeError, match="message_wait_interval_seconds"):
            AsyncRedisGateway(
                redis_client=fakeredis.FakeAsyncRedis(),
                retry_strategy=_no_retry,
                message_wait_interval_seconds=1.5,
            )

    def test_bool_wait_interval_raises_type_error(self):
        with pytest.raises(TypeError, match="message_wait_interval_seconds"):
            AsyncRedisGateway(
                redis_client=fakeredis.FakeAsyncRedis(),
                retry_strategy=_no_retry,
                message_wait_interval_seconds=True,
            )

    def test_negative_visibility_timeout_raises_value_error(self):
        with pytest.raises(ValueError, match="message_visibility_timeout_seconds"):
            AsyncRedisGateway(
                redis_client=fakeredis.FakeAsyncRedis(),
                retry_strategy=_no_retry,
                message_visibility_timeout_seconds=-1,
            )

    def test_bool_visibility_timeout_raises_type_error(self):
        with pytest.raises(TypeError, match="message_visibility_timeout_seconds"):
            AsyncRedisGateway(
                redis_client=fakeredis.FakeAsyncRedis(),
                retry_strategy=_no_retry,
                message_visibility_timeout_seconds=True,
            )

    def test_async_retry_strategy_raises_type_error(self):
        async def async_retry(func):
            return func

        with pytest.raises(TypeError, match="async callable"):
            AsyncRedisGateway(redis_client=fakeredis.FakeAsyncRedis(), retry_strategy=async_retry)

    def test_async_callable_object_retry_strategy_raises_type_error(self):
        class AsyncRetry:
            async def __call__(self, func):
                return func

        with pytest.raises(TypeError, match="async callable"):
            AsyncRedisGateway(redis_client=fakeredis.FakeAsyncRedis(), retry_strategy=AsyncRetry())

    def test_sync_redis_client_raises_type_error(self):
        with pytest.raises(TypeError, match="sync Redis client"):
            AsyncRedisGateway(redis_client=fakeredis.FakeRedis())

    def test_async_pipeline_raises_type_error(self):
        pipeline = fakeredis.FakeAsyncRedis().pipeline()
        with pytest.raises(TypeError, match="Pipeline"):
            AsyncRedisGateway(redis_client=pipeline)

    def test_valid_defaults_accepted(self):
        gateway = AsyncRedisGateway(redis_client=fakeredis.FakeAsyncRedis(), retry_strategy=_no_retry)
        assert gateway.message_visibility_timeout_seconds is None

    def test_valid_explicit_params_accepted(self):
        gateway = AsyncRedisGateway(
            redis_client=fakeredis.FakeAsyncRedis(),
            retry_strategy=_no_retry,
            message_deduplication_log_ttl_seconds=120,
            message_wait_interval_seconds=10,
            message_visibility_timeout_seconds=30,
        )
        assert gateway.message_visibility_timeout_seconds == 30


class TestOperationResultTTLFloor:
    """The operation-result cache TTL must outlive tenacity's retry budget.

    ``stop_after_delay(120)`` in ``_config.py`` gives retries a 120s window.
    If the TTL floor equals that budget, a retry arriving at the boundary
    finds the cache just expired and re-runs LREM, wrongly returning 0.
    """

    @pytest.mark.parametrize(
        "visibility_timeout,expected_ttl_ms",
        [
            (None, "300000"),
            (10, "300000"),
            (120, "300000"),
            (299, "300000"),
            (300, "300000"),
            (600, "600000"),
        ],
    )
    def test_sync_gateway_ttl_floor(self, visibility_timeout, expected_ttl_ms):
        gateway = RedisGateway(
            redis_client=fakeredis.FakeRedis(),
            retry_strategy=_no_retry,
            message_visibility_timeout_seconds=visibility_timeout,
        )
        assert gateway._operation_result_ttl_ms() == expected_ttl_ms
        assert gateway._lease_operation_result_ttl_ms() == expected_ttl_ms

    @pytest.mark.parametrize(
        "visibility_timeout,expected_ttl_ms",
        [
            (None, "300000"),
            (10, "300000"),
            (120, "300000"),
            (299, "300000"),
            (300, "300000"),
            (600, "600000"),
        ],
    )
    def test_async_gateway_ttl_floor(self, visibility_timeout, expected_ttl_ms):
        gateway = AsyncRedisGateway(
            redis_client=fakeredis.FakeAsyncRedis(),
            retry_strategy=_no_retry,
            message_visibility_timeout_seconds=visibility_timeout,
        )
        assert gateway._operation_result_ttl_ms() == expected_ttl_ms
        assert gateway._lease_operation_result_ttl_ms() == expected_ttl_ms
