from unittest.mock import MagicMock

import fakeredis
import pytest
import redis.exceptions

from redis_message_queue._config import (
    interruptable_retry,
    is_redis_retryable_exception,
    retry_if_exception,
)
from redis_message_queue._redis_gateway import RedisGateway
from redis_message_queue.asyncio._redis_gateway import (
    RedisGateway as AsyncRedisGateway,
)
from redis_message_queue.interrupt_handler._interface import (
    BaseGracefulInterruptHandler,
)


class TestRetryableConnectionErrors:
    """ConnectionError subclasses that ARE transient and should be retried."""

    def test_plain_connection_error_is_retryable(self):
        exc = redis.exceptions.ConnectionError("connection refused")
        assert is_redis_retryable_exception(exc) is True


class TestNonRetryableConnectionErrors:
    """ConnectionError subclasses that are permanent and must NOT be retried."""

    def test_authentication_error_is_not_retryable(self):
        exc = redis.exceptions.AuthenticationError("wrong password")
        assert is_redis_retryable_exception(exc) is False

    def test_authorization_error_is_not_retryable(self):
        exc = redis.exceptions.AuthorizationError("no permission")
        assert is_redis_retryable_exception(exc) is False

    def test_max_connections_error_is_not_retryable(self):
        exc = redis.exceptions.MaxConnectionsError("pool exhausted")
        assert is_redis_retryable_exception(exc) is False


class TestRetryableNonConnectionErrors:
    """Non-ConnectionError exceptions that ARE transient and should be retried."""

    def test_timeout_error_is_retryable(self):
        exc = redis.exceptions.TimeoutError("timed out")
        assert is_redis_retryable_exception(exc) is True

    def test_busy_loading_error_is_retryable(self):
        exc = redis.exceptions.BusyLoadingError("loading dataset")
        assert is_redis_retryable_exception(exc) is True

    def test_cluster_down_error_is_retryable(self):
        exc = redis.exceptions.ClusterDownError("cluster down")
        assert is_redis_retryable_exception(exc) is True

    def test_try_again_error_is_retryable(self):
        exc = redis.exceptions.TryAgainError("try again")
        assert is_redis_retryable_exception(exc) is True

    def test_read_only_error_is_retryable(self):
        exc = redis.exceptions.ReadOnlyError("readonly")
        assert is_redis_retryable_exception(exc) is True


class TestNonRetryableOtherErrors:
    """Exceptions that are not transient and must NOT be retried."""

    def test_response_error_is_not_retryable(self):
        exc = redis.exceptions.ResponseError("wrong number of args")
        assert is_redis_retryable_exception(exc) is False

    def test_data_error_is_not_retryable(self):
        exc = redis.exceptions.DataError("invalid data")
        assert is_redis_retryable_exception(exc) is False

    def test_generic_exception_is_not_retryable(self):
        exc = Exception("something went wrong")
        assert is_redis_retryable_exception(exc) is False

    def test_value_error_is_not_retryable(self):
        exc = ValueError("bad value")
        assert is_redis_retryable_exception(exc) is False


class TestInterruptableRetryFalsyHandler:
    """A falsy interrupt handler (e.g. __bool__=False) that is not None must
    still be checked for interrupts."""

    def test_falsy_interrupt_handler_is_still_checked(self):
        class FalsyInterruptHandler(BaseGracefulInterruptHandler):
            def __bool__(self):
                return False

            def is_interrupted(self) -> bool:
                return True

        handler = FalsyInterruptHandler()
        retry_obj = interruptable_retry(
            interrupt=handler,
            get_parent_retry=lambda: retry_if_exception(is_redis_retryable_exception),
        )

        # Set up a retry_state with a retryable exception, so the parent
        # predicate would return True ("keep retrying").  The interrupt handler
        # must override that and return False ("stop retrying").
        outcome = MagicMock()
        outcome.failed = True
        outcome.exception.return_value = redis.exceptions.ConnectionError("gone")
        retry_state = MagicMock()
        retry_state.outcome = outcome

        assert retry_obj(retry_state) is False


class TestGatewayTTLValidation:
    def test_sync_gateway_ttl_zero_raises(self):
        with pytest.raises(ValueError, match="deduplication_log_ttl"):
            RedisGateway(
                redis_client=fakeredis.FakeRedis(),
                message_deduplication_log_ttl_seconds=0,
            )

    def test_sync_gateway_ttl_negative_raises(self):
        with pytest.raises(ValueError, match="deduplication_log_ttl"):
            RedisGateway(
                redis_client=fakeredis.FakeRedis(),
                message_deduplication_log_ttl_seconds=-5,
            )

    def test_async_gateway_ttl_zero_raises(self):
        with pytest.raises(ValueError, match="deduplication_log_ttl"):
            AsyncRedisGateway(
                redis_client=fakeredis.FakeAsyncRedis(),
                message_deduplication_log_ttl_seconds=0,
            )

    def test_async_gateway_ttl_negative_raises(self):
        with pytest.raises(ValueError, match="deduplication_log_ttl"):
            AsyncRedisGateway(
                redis_client=fakeredis.FakeAsyncRedis(),
                message_deduplication_log_ttl_seconds=-5,
            )


    def test_sync_gateway_ttl_bool_raises(self):
        with pytest.raises(TypeError, match="deduplication_log_ttl"):
            RedisGateway(
                redis_client=fakeredis.FakeRedis(),
                message_deduplication_log_ttl_seconds=True,
            )

    def test_sync_gateway_ttl_float_raises(self):
        with pytest.raises(TypeError, match="deduplication_log_ttl"):
            RedisGateway(
                redis_client=fakeredis.FakeRedis(),
                message_deduplication_log_ttl_seconds=1.5,
            )

    def test_async_gateway_ttl_bool_raises(self):
        with pytest.raises(TypeError, match="deduplication_log_ttl"):
            AsyncRedisGateway(
                redis_client=fakeredis.FakeAsyncRedis(),
                message_deduplication_log_ttl_seconds=True,
            )

    def test_async_gateway_ttl_float_raises(self):
        with pytest.raises(TypeError, match="deduplication_log_ttl"):
            AsyncRedisGateway(
                redis_client=fakeredis.FakeAsyncRedis(),
                message_deduplication_log_ttl_seconds=1.5,
            )


class TestGatewayWaitIntervalValidation:
    def test_sync_gateway_wait_interval_negative_raises(self):
        with pytest.raises(ValueError, match="wait_interval"):
            RedisGateway(
                redis_client=fakeredis.FakeRedis(),
                message_wait_interval_seconds=-1,
            )

    def test_sync_gateway_wait_interval_zero_is_accepted(self):
        gw = RedisGateway(
            redis_client=fakeredis.FakeRedis(),
            message_wait_interval_seconds=0,
        )
        assert gw._message_wait_interval_seconds == 0

    def test_async_gateway_wait_interval_negative_raises(self):
        with pytest.raises(ValueError, match="wait_interval"):
            AsyncRedisGateway(
                redis_client=fakeredis.FakeAsyncRedis(),
                message_wait_interval_seconds=-1,
            )

    def test_sync_gateway_wait_interval_bool_raises(self):
        with pytest.raises(TypeError, match="wait_interval"):
            RedisGateway(
                redis_client=fakeredis.FakeRedis(),
                message_wait_interval_seconds=True,
            )

    def test_sync_gateway_wait_interval_float_raises(self):
        with pytest.raises(TypeError, match="wait_interval"):
            RedisGateway(
                redis_client=fakeredis.FakeRedis(),
                message_wait_interval_seconds=1.5,
            )

    def test_async_gateway_wait_interval_bool_raises(self):
        with pytest.raises(TypeError, match="wait_interval"):
            AsyncRedisGateway(
                redis_client=fakeredis.FakeAsyncRedis(),
                message_wait_interval_seconds=True,
            )

    def test_async_gateway_wait_interval_float_raises(self):
        with pytest.raises(TypeError, match="wait_interval"):
            AsyncRedisGateway(
                redis_client=fakeredis.FakeAsyncRedis(),
                message_wait_interval_seconds=1.5,
            )

    def test_async_gateway_wait_interval_zero_is_accepted(self):
        gw = AsyncRedisGateway(
            redis_client=fakeredis.FakeAsyncRedis(),
            message_wait_interval_seconds=0,
        )
        assert gw._message_wait_interval_seconds == 0


class TestFalsyRetryStrategyAccepted:
    """A retry_strategy that is falsy (e.g. __bool__=False) but not None must
    be used, not silently replaced by the default."""

    def test_sync_gateway_uses_falsy_retry_strategy(self):
        class FalsyStrategy:
            def __bool__(self):
                return False

            def __call__(self, func):
                return func

        strategy = FalsyStrategy()
        gw = RedisGateway(
            redis_client=fakeredis.FakeRedis(),
            retry_strategy=strategy,
        )
        assert gw._retry_strategy is strategy

    def test_async_gateway_uses_falsy_retry_strategy(self):
        class FalsyStrategy:
            def __bool__(self):
                return False

            def __call__(self, func):
                return func

        strategy = FalsyStrategy()
        gw = AsyncRedisGateway(
            redis_client=fakeredis.FakeAsyncRedis(),
            retry_strategy=strategy,
        )
        assert gw._retry_strategy is strategy


class TestGatewayRetryStrategyValidation:
    """retry_strategy must be callable (or None for the default)."""

    def test_sync_gateway_rejects_int_retry_strategy(self):
        with pytest.raises(TypeError, match="'retry_strategy' must be callable"):
            RedisGateway(redis_client=fakeredis.FakeRedis(), retry_strategy=42)

    def test_sync_gateway_rejects_string_retry_strategy(self):
        with pytest.raises(TypeError, match="'retry_strategy' must be callable"):
            RedisGateway(redis_client=fakeredis.FakeRedis(), retry_strategy="bad")

    def test_async_gateway_rejects_int_retry_strategy(self):
        with pytest.raises(TypeError, match="'retry_strategy' must be callable"):
            AsyncRedisGateway(redis_client=fakeredis.FakeAsyncRedis(), retry_strategy=42)

    def test_async_gateway_rejects_string_retry_strategy(self):
        with pytest.raises(TypeError, match="'retry_strategy' must be callable"):
            AsyncRedisGateway(redis_client=fakeredis.FakeAsyncRedis(), retry_strategy="bad")

    def test_sync_gateway_accepts_callable_retry_strategy(self):
        gw = RedisGateway(redis_client=fakeredis.FakeRedis(), retry_strategy=lambda f: f)
        assert callable(gw._retry_strategy)

    def test_async_gateway_accepts_callable_retry_strategy(self):
        gw = AsyncRedisGateway(redis_client=fakeredis.FakeAsyncRedis(), retry_strategy=lambda f: f)
        assert callable(gw._retry_strategy)

    def test_sync_gateway_accepts_none_retry_strategy(self):
        gw = RedisGateway(redis_client=fakeredis.FakeRedis(), retry_strategy=None)
        assert callable(gw._retry_strategy)

    def test_async_gateway_accepts_none_retry_strategy(self):
        gw = AsyncRedisGateway(redis_client=fakeredis.FakeAsyncRedis(), retry_strategy=None)
        assert callable(gw._retry_strategy)


class TestGatewayInterruptValidation:
    """interrupt must be a BaseGracefulInterruptHandler (or None)."""

    def test_sync_gateway_rejects_string_interrupt(self):
        with pytest.raises(TypeError, match="'interrupt' must be a BaseGracefulInterruptHandler"):
            RedisGateway(redis_client=fakeredis.FakeRedis(), interrupt="bad")

    def test_sync_gateway_rejects_int_interrupt(self):
        with pytest.raises(TypeError, match="'interrupt' must be a BaseGracefulInterruptHandler"):
            RedisGateway(redis_client=fakeredis.FakeRedis(), interrupt=42)

    def test_async_gateway_rejects_string_interrupt(self):
        with pytest.raises(TypeError, match="'interrupt' must be a BaseGracefulInterruptHandler"):
            AsyncRedisGateway(redis_client=fakeredis.FakeAsyncRedis(), interrupt="bad")

    def test_async_gateway_rejects_int_interrupt(self):
        with pytest.raises(TypeError, match="'interrupt' must be a BaseGracefulInterruptHandler"):
            AsyncRedisGateway(redis_client=fakeredis.FakeAsyncRedis(), interrupt=42)

    def test_sync_gateway_accepts_none_interrupt(self):
        gw = RedisGateway(redis_client=fakeredis.FakeRedis(), interrupt=None)
        assert gw is not None

    def test_async_gateway_accepts_none_interrupt(self):
        gw = AsyncRedisGateway(redis_client=fakeredis.FakeAsyncRedis(), interrupt=None)
        assert gw is not None

    def test_sync_gateway_accepts_valid_interrupt_handler(self):
        class StubHandler(BaseGracefulInterruptHandler):
            def is_interrupted(self) -> bool:
                return False

        RedisGateway(redis_client=fakeredis.FakeRedis(), interrupt=StubHandler())

    def test_async_gateway_accepts_valid_interrupt_handler(self):
        class StubHandler(BaseGracefulInterruptHandler):
            def is_interrupted(self) -> bool:
                return False

        AsyncRedisGateway(redis_client=fakeredis.FakeAsyncRedis(), interrupt=StubHandler())
