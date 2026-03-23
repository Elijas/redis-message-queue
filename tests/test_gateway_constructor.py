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

    def test_retry_strategy_and_interrupt_raises_value_error(self):
        with pytest.raises(ValueError, match="retry_strategy.*interrupt"):
            RedisGateway(
                redis_client=fakeredis.FakeRedis(),
                retry_strategy=_no_retry,
                interrupt=_FakeInterrupt(),
            )

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

    def test_async_redis_client_raises_type_error(self):
        with pytest.raises(TypeError, match="async Redis client"):
            RedisGateway(redis_client=fakeredis.FakeAsyncRedis())

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

    def test_retry_strategy_and_interrupt_raises_value_error(self):
        with pytest.raises(ValueError, match="retry_strategy.*interrupt"):
            AsyncRedisGateway(
                redis_client=fakeredis.FakeAsyncRedis(),
                retry_strategy=_no_retry,
                interrupt=_FakeInterrupt(),
            )

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

        with pytest.raises(TypeError, match="async function"):
            AsyncRedisGateway(redis_client=fakeredis.FakeAsyncRedis(), retry_strategy=async_retry)

    def test_sync_redis_client_raises_type_error(self):
        with pytest.raises(TypeError, match="sync Redis client"):
            AsyncRedisGateway(redis_client=fakeredis.FakeRedis())

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
