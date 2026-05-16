from unittest.mock import MagicMock

import fakeredis
import pytest
import redis.exceptions
from tenacity import stop_after_attempt, wait_none

import redis_message_queue._config as config
from redis_message_queue._config import (
    interruptable_retry,
    is_redis_retryable_exception,
    retry_if_exception,
)
from redis_message_queue._exceptions import RedisMessageQueueError, RetryBudgetExhaustedError
from redis_message_queue._redis_gateway import RedisGateway
from redis_message_queue.asyncio._redis_gateway import (
    RedisGateway as AsyncRedisGateway,
)
from redis_message_queue.interrupt_handler._interface import (
    BaseGracefulInterruptHandler,
)


class _AlwaysTimeoutSyncClient:
    def eval(self, *args, **kwargs):
        raise redis.exceptions.TimeoutError("timed out")


class _AlwaysTimeoutAsyncClient:
    async def eval(self, *args, **kwargs):
        raise redis.exceptions.TimeoutError("timed out")


def _raise_timeout(*_args):
    raise redis.exceptions.TimeoutError("claim timed out")


async def _async_raise_timeout(*_args):
    raise redis.exceptions.TimeoutError("claim timed out")


async def _async_recover_none(*_args):
    return None


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

    def test_max_connections_error_is_retryable(self):
        exc = redis.exceptions.MaxConnectionsError("pool exhausted")
        assert is_redis_retryable_exception(exc) is True


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


class TestGatewayCustomRetryWithInterrupt:
    """Custom retry decorators must coexist with interrupt-aware gateway polling."""

    def _make_interrupt(self):
        class StubHandler(BaseGracefulInterruptHandler):
            def is_interrupted(self) -> bool:
                return False

        return StubHandler()

    def test_sync_gateway_accepts_retry_strategy_with_interrupt(self):
        interrupt = self._make_interrupt()
        gw = RedisGateway(
            redis_client=fakeredis.FakeRedis(),
            retry_budget_seconds=0,
            interrupt=interrupt,
        )
        assert gw._interrupt is interrupt
        assert callable(gw._retry_strategy)

    def test_async_gateway_accepts_retry_strategy_with_interrupt(self):
        interrupt = self._make_interrupt()
        gw = AsyncRedisGateway(
            redis_client=fakeredis.FakeAsyncRedis(),
            retry_budget_seconds=0,
            interrupt=interrupt,
        )
        assert gw._interrupt is interrupt
        assert callable(gw._retry_strategy)

    def test_sync_gateway_accepts_retry_strategy_without_interrupt(self):
        gw = RedisGateway(
            redis_client=fakeredis.FakeRedis(),
            retry_budget_seconds=0,
        )
        assert callable(gw._retry_strategy)

    def test_async_gateway_accepts_retry_strategy_without_interrupt(self):
        gw = AsyncRedisGateway(
            redis_client=fakeredis.FakeAsyncRedis(),
            retry_budget_seconds=0,
        )
        assert callable(gw._retry_strategy)


class TestDefaultRetryStrategyExceptionType:
    def test_sync_gateway_wraps_retry_exhaustion(self, monkeypatch):
        monkeypatch.setattr(config, "stop_after_delay", lambda _seconds: stop_after_attempt(2))
        monkeypatch.setattr(config, "wait_exponential_jitter", lambda **kwargs: wait_none())

        gateway = RedisGateway(redis_client=_AlwaysTimeoutSyncClient())

        with pytest.raises(RedisMessageQueueError) as caught:
            gateway.publish_message("pending", "message", "dedup:message")

        exc = caught.value
        assert isinstance(exc, RetryBudgetExhaustedError)
        assert "verify Redis connectivity" in str(exc)
        assert "retry_budget_seconds" in str(exc)
        assert isinstance(exc, redis.exceptions.RedisError)
        assert isinstance(exc.__cause__, redis.exceptions.TimeoutError)
        assert str(exc.__cause__) == "timed out"

    @pytest.mark.asyncio
    async def test_async_gateway_wraps_retry_exhaustion(self, monkeypatch):
        monkeypatch.setattr(config, "stop_after_delay", lambda _seconds: stop_after_attempt(2))
        monkeypatch.setattr(config, "wait_exponential_jitter", lambda **kwargs: wait_none())

        gateway = AsyncRedisGateway(redis_client=_AlwaysTimeoutAsyncClient())

        with pytest.raises(RedisMessageQueueError) as caught:
            await gateway.publish_message("pending", "message", "dedup:message")

        exc = caught.value
        assert isinstance(exc, RetryBudgetExhaustedError)
        assert isinstance(exc, redis.exceptions.RedisError)
        assert isinstance(exc.__cause__, redis.exceptions.TimeoutError)
        assert str(exc.__cause__) == "timed out"


class TestManualClaimRetryExceptionType:
    def test_sync_nonblocking_claim_retry_exhaustion_is_wrapped(self):
        gateway = RedisGateway(
            redis_client=fakeredis.FakeRedis(),
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
        )

        with pytest.raises(RetryBudgetExhaustedError) as caught:
            gateway._wait_for_claim(
                "pending",
                "processing",
                recover_pending_claim=lambda _to_queue, _claim_id: None,
                claim_message=_raise_timeout,
                non_blocking_retry_log="retrying: %s",
                polling_retry_log="retrying: %s",
            )

        assert isinstance(caught.value, RedisMessageQueueError)
        assert isinstance(caught.value.__cause__, redis.exceptions.TimeoutError)
        assert str(caught.value.__cause__) == "claim timed out"

    def test_sync_polling_claim_retry_exhaustion_is_wrapped(self):
        gateway = RedisGateway(
            redis_client=fakeredis.FakeRedis(),
            retry_budget_seconds=0,
            message_wait_interval_seconds=1,
        )
        deadline = 100.0

        def monotonic():
            nonlocal deadline
            deadline += 2.0
            return deadline

        with pytest.MonkeyPatch.context() as monkeypatch:
            monkeypatch.setattr("redis_message_queue._redis_gateway.time.monotonic", monotonic)
            with pytest.raises(RetryBudgetExhaustedError) as caught:
                gateway._wait_for_claim(
                    "pending",
                    "processing",
                    recover_pending_claim=lambda _to_queue, _claim_id: None,
                    claim_message=_raise_timeout,
                    non_blocking_retry_log="retrying: %s",
                    polling_retry_log="retrying: %s",
                )

        assert isinstance(caught.value, RedisMessageQueueError)
        assert isinstance(caught.value.__cause__, redis.exceptions.TimeoutError)
        assert str(caught.value.__cause__) == "claim timed out"

    @pytest.mark.asyncio
    async def test_async_nonblocking_claim_retry_exhaustion_is_wrapped(self):
        gateway = AsyncRedisGateway(
            redis_client=fakeredis.FakeAsyncRedis(),
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
        )

        with pytest.raises(RetryBudgetExhaustedError) as caught:
            await gateway._wait_for_claim(
                "pending",
                "processing",
                recover_pending_claim=_async_recover_none,
                claim_message=_async_raise_timeout,
                non_blocking_retry_log="retrying: %s",
                polling_retry_log="retrying: %s",
            )

        assert isinstance(caught.value, RedisMessageQueueError)
        assert isinstance(caught.value.__cause__, redis.exceptions.TimeoutError)
        assert str(caught.value.__cause__) == "claim timed out"

    @pytest.mark.asyncio
    async def test_async_polling_claim_retry_exhaustion_is_wrapped(self, monkeypatch):
        gateway = AsyncRedisGateway(
            redis_client=fakeredis.FakeAsyncRedis(),
            retry_budget_seconds=0,
            message_wait_interval_seconds=1,
        )

        class FakeLoop:
            def __init__(self) -> None:
                self.now = 100.0

            def time(self) -> float:
                self.now += 2.0
                return self.now

        monkeypatch.setattr("redis_message_queue.asyncio._redis_gateway.asyncio.get_running_loop", FakeLoop)

        with pytest.raises(RetryBudgetExhaustedError) as caught:
            await gateway._wait_for_claim(
                "pending",
                "processing",
                recover_pending_claim=_async_recover_none,
                claim_message=_async_raise_timeout,
                non_blocking_retry_log="retrying: %s",
                polling_retry_log="retrying: %s",
            )

        assert isinstance(caught.value, RedisMessageQueueError)
        assert isinstance(caught.value.__cause__, redis.exceptions.TimeoutError)
        assert str(caught.value.__cause__) == "claim timed out"
