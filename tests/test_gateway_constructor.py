import math

import fakeredis
import pytest
import redis.asyncio
import redis.asyncio.sentinel
import redis.sentinel

from redis_message_queue._redis_gateway import RedisGateway
from redis_message_queue.asyncio._redis_gateway import (
    RedisGateway as AsyncRedisGateway,
)
from redis_message_queue.interrupt_handler._interface import (
    BaseGracefulInterruptHandler,
)
from tests.conftest import close_async_redis_client


class _FakeInterrupt(BaseGracefulInterruptHandler):
    def is_interrupted(self) -> bool:
        return False


class TestSyncGatewayConstructorValidation:
    def test_non_interrupt_handler_raises_type_error(self):
        with pytest.raises(TypeError, match="interrupt"):
            RedisGateway(redis_client=fakeredis.FakeRedis(), interrupt="bad")

    def test_interrupt_is_accepted(self):
        interrupt = _FakeInterrupt()
        gateway = RedisGateway(
            redis_client=fakeredis.FakeRedis(),
            retry_budget_seconds=0,
            interrupt=interrupt,
        )
        assert gateway._interrupt is interrupt
        assert callable(gateway._retry_strategy)

    def test_zero_dedup_ttl_raises_value_error(self):
        with pytest.raises(ValueError, match="message_deduplication_log_ttl_seconds"):
            RedisGateway(
                redis_client=fakeredis.FakeRedis(),
                retry_budget_seconds=0,
                message_deduplication_log_ttl_seconds=0,
            )

    def test_negative_dedup_ttl_raises_value_error(self):
        with pytest.raises(ValueError, match="message_deduplication_log_ttl_seconds"):
            RedisGateway(
                redis_client=fakeredis.FakeRedis(),
                retry_budget_seconds=0,
                message_deduplication_log_ttl_seconds=-1,
            )

    def test_float_dedup_ttl_raises_type_error(self):
        with pytest.raises(TypeError, match="message_deduplication_log_ttl_seconds"):
            RedisGateway(
                redis_client=fakeredis.FakeRedis(),
                retry_budget_seconds=0,
                message_deduplication_log_ttl_seconds=1.5,
            )

    def test_bool_dedup_ttl_raises_type_error(self):
        with pytest.raises(TypeError, match="message_deduplication_log_ttl_seconds"):
            RedisGateway(
                redis_client=fakeredis.FakeRedis(),
                retry_budget_seconds=0,
                message_deduplication_log_ttl_seconds=True,
            )

    def test_negative_wait_interval_raises_value_error(self):
        with pytest.raises(ValueError, match="message_wait_interval_seconds"):
            RedisGateway(
                redis_client=fakeredis.FakeRedis(),
                retry_budget_seconds=0,
                message_wait_interval_seconds=-1,
            )

    def test_float_wait_interval_raises_type_error(self):
        with pytest.raises(TypeError, match="message_wait_interval_seconds"):
            RedisGateway(
                redis_client=fakeredis.FakeRedis(),
                retry_budget_seconds=0,
                message_wait_interval_seconds=1.5,
            )

    def test_bool_wait_interval_raises_type_error(self):
        with pytest.raises(TypeError, match="message_wait_interval_seconds"):
            RedisGateway(
                redis_client=fakeredis.FakeRedis(),
                retry_budget_seconds=0,
                message_wait_interval_seconds=True,
            )

    def test_negative_visibility_timeout_raises_value_error(self):
        with pytest.raises(ValueError, match="message_visibility_timeout_seconds"):
            RedisGateway(
                redis_client=fakeredis.FakeRedis(),
                retry_budget_seconds=0,
                message_visibility_timeout_seconds=-1,
            )

    def test_bool_visibility_timeout_raises_type_error(self):
        with pytest.raises(TypeError, match="message_visibility_timeout_seconds"):
            RedisGateway(
                redis_client=fakeredis.FakeRedis(),
                retry_budget_seconds=0,
                message_visibility_timeout_seconds=True,
            )

    def test_async_redis_client_raises_type_error(self):
        with pytest.raises(TypeError, match="async Redis client"):
            RedisGateway(redis_client=fakeredis.FakeAsyncRedis())

    def test_sentinel_manager_raises_actionable_type_error(self):
        sentinel = redis.sentinel.Sentinel([("localhost", 26379)])
        with pytest.raises(TypeError, match=r"Sentinel manager object.*sentinel\.master_for\(name\)"):
            RedisGateway(redis_client=sentinel)

    def test_sync_pipeline_raises_type_error(self):
        pipeline = fakeredis.FakeRedis().pipeline()
        with pytest.raises(TypeError, match="Pipeline"):
            RedisGateway(redis_client=pipeline)

    def test_single_connection_client_raises_type_error(self):
        client = redis.Redis(single_connection_client=True)
        try:
            with pytest.raises(TypeError, match=r"single-connection client"):
                RedisGateway(redis_client=client)
        finally:
            client.close()

    def test_valid_defaults_accepted(self):
        gateway = RedisGateway(redis_client=fakeredis.FakeRedis(), retry_budget_seconds=0)
        assert gateway.message_visibility_timeout_seconds is None

    def test_valid_explicit_params_accepted(self):
        gateway = RedisGateway(
            redis_client=fakeredis.FakeRedis(),
            retry_budget_seconds=0,
            message_deduplication_log_ttl_seconds=120,
            message_wait_interval_seconds=10,
            message_visibility_timeout_seconds=30,
        )
        assert gateway.message_visibility_timeout_seconds == 30


class TestAsyncGatewayConstructorValidation:
    def test_non_interrupt_handler_raises_type_error(self):
        with pytest.raises(TypeError, match="interrupt"):
            AsyncRedisGateway(redis_client=fakeredis.FakeAsyncRedis(), interrupt="bad")

    def test_interrupt_is_accepted(self):
        interrupt = _FakeInterrupt()
        gateway = AsyncRedisGateway(
            redis_client=fakeredis.FakeAsyncRedis(),
            retry_budget_seconds=0,
            interrupt=interrupt,
        )
        assert gateway._interrupt is interrupt
        assert callable(gateway._retry_strategy)

    def test_zero_dedup_ttl_raises_value_error(self):
        with pytest.raises(ValueError, match="message_deduplication_log_ttl_seconds"):
            AsyncRedisGateway(
                redis_client=fakeredis.FakeAsyncRedis(),
                retry_budget_seconds=0,
                message_deduplication_log_ttl_seconds=0,
            )

    def test_negative_dedup_ttl_raises_value_error(self):
        with pytest.raises(ValueError, match="message_deduplication_log_ttl_seconds"):
            AsyncRedisGateway(
                redis_client=fakeredis.FakeAsyncRedis(),
                retry_budget_seconds=0,
                message_deduplication_log_ttl_seconds=-1,
            )

    def test_float_dedup_ttl_raises_type_error(self):
        with pytest.raises(TypeError, match="message_deduplication_log_ttl_seconds"):
            AsyncRedisGateway(
                redis_client=fakeredis.FakeAsyncRedis(),
                retry_budget_seconds=0,
                message_deduplication_log_ttl_seconds=1.5,
            )

    def test_bool_dedup_ttl_raises_type_error(self):
        with pytest.raises(TypeError, match="message_deduplication_log_ttl_seconds"):
            AsyncRedisGateway(
                redis_client=fakeredis.FakeAsyncRedis(),
                retry_budget_seconds=0,
                message_deduplication_log_ttl_seconds=True,
            )

    def test_negative_wait_interval_raises_value_error(self):
        with pytest.raises(ValueError, match="message_wait_interval_seconds"):
            AsyncRedisGateway(
                redis_client=fakeredis.FakeAsyncRedis(),
                retry_budget_seconds=0,
                message_wait_interval_seconds=-1,
            )

    def test_float_wait_interval_raises_type_error(self):
        with pytest.raises(TypeError, match="message_wait_interval_seconds"):
            AsyncRedisGateway(
                redis_client=fakeredis.FakeAsyncRedis(),
                retry_budget_seconds=0,
                message_wait_interval_seconds=1.5,
            )

    def test_bool_wait_interval_raises_type_error(self):
        with pytest.raises(TypeError, match="message_wait_interval_seconds"):
            AsyncRedisGateway(
                redis_client=fakeredis.FakeAsyncRedis(),
                retry_budget_seconds=0,
                message_wait_interval_seconds=True,
            )

    def test_negative_visibility_timeout_raises_value_error(self):
        with pytest.raises(ValueError, match="message_visibility_timeout_seconds"):
            AsyncRedisGateway(
                redis_client=fakeredis.FakeAsyncRedis(),
                retry_budget_seconds=0,
                message_visibility_timeout_seconds=-1,
            )

    def test_bool_visibility_timeout_raises_type_error(self):
        with pytest.raises(TypeError, match="message_visibility_timeout_seconds"):
            AsyncRedisGateway(
                redis_client=fakeredis.FakeAsyncRedis(),
                retry_budget_seconds=0,
                message_visibility_timeout_seconds=True,
            )

    def test_sync_redis_client_raises_type_error(self):
        with pytest.raises(TypeError, match="sync Redis client"):
            AsyncRedisGateway(redis_client=fakeredis.FakeRedis())

    def test_async_sentinel_manager_raises_actionable_type_error(self):
        sentinel = redis.asyncio.sentinel.Sentinel([("localhost", 26379)])
        with pytest.raises(TypeError, match=r"Sentinel manager object.*sentinel\.master_for\(name\)"):
            AsyncRedisGateway(redis_client=sentinel)

    def test_async_pipeline_raises_type_error(self):
        pipeline = fakeredis.FakeAsyncRedis().pipeline()
        with pytest.raises(TypeError, match="Pipeline"):
            AsyncRedisGateway(redis_client=pipeline)

    @pytest.mark.asyncio
    async def test_single_connection_client_raises_type_error(self):
        client = redis.asyncio.Redis(single_connection_client=True)
        try:
            with pytest.raises(TypeError, match=r"single-connection client"):
                AsyncRedisGateway(redis_client=client)
        finally:
            await close_async_redis_client(client)

    def test_valid_defaults_accepted(self):
        gateway = AsyncRedisGateway(redis_client=fakeredis.FakeAsyncRedis(), retry_budget_seconds=0)
        assert gateway.message_visibility_timeout_seconds is None

    def test_valid_explicit_params_accepted(self):
        gateway = AsyncRedisGateway(
            redis_client=fakeredis.FakeAsyncRedis(),
            retry_budget_seconds=0,
            message_deduplication_log_ttl_seconds=120,
            message_wait_interval_seconds=10,
            message_visibility_timeout_seconds=30,
        )
        assert gateway.message_visibility_timeout_seconds == 30


_GATEWAY_FACTORIES = [
    pytest.param(
        lambda **kwargs: RedisGateway(redis_client=fakeredis.FakeRedis(), **kwargs),
        id="sync",
    ),
    pytest.param(
        lambda **kwargs: AsyncRedisGateway(redis_client=fakeredis.FakeAsyncRedis(), **kwargs),
        id="async",
    ),
]


class TestRetryParameterValidation:
    """The new retry knobs (budget/max-delay/initial-delay) are validated at
    construction time. ``retry_budget_seconds=0`` is the explicit "no-retry"
    form (single attempt; exceptions propagate)."""

    @pytest.mark.parametrize("make_gateway", _GATEWAY_FACTORIES)
    def test_zero_budget_disables_retry(self, make_gateway):
        gateway = make_gateway(retry_budget_seconds=0)
        # The strategy is the noop decorator: applying it returns the original.
        sentinel = lambda x: x  # noqa: E731
        assert gateway._retry_strategy(sentinel) is sentinel

    @pytest.mark.parametrize("make_gateway", _GATEWAY_FACTORIES)
    def test_negative_budget_raises_value_error(self, make_gateway):
        with pytest.raises(ValueError, match="retry_budget_seconds"):
            make_gateway(retry_budget_seconds=-1)

    @pytest.mark.parametrize("make_gateway", _GATEWAY_FACTORIES)
    def test_float_budget_raises_type_error(self, make_gateway):
        with pytest.raises(TypeError, match="retry_budget_seconds"):
            make_gateway(retry_budget_seconds=1.5)

    @pytest.mark.parametrize("make_gateway", _GATEWAY_FACTORIES)
    def test_bool_budget_raises_type_error(self, make_gateway):
        with pytest.raises(TypeError, match="retry_budget_seconds"):
            make_gateway(retry_budget_seconds=True)

    @pytest.mark.parametrize("make_gateway", _GATEWAY_FACTORIES)
    def test_zero_max_delay_raises_value_error(self, make_gateway):
        with pytest.raises(ValueError, match="retry_max_delay_seconds"):
            make_gateway(retry_max_delay_seconds=0)

    @pytest.mark.parametrize("make_gateway", _GATEWAY_FACTORIES)
    def test_negative_max_delay_raises_value_error(self, make_gateway):
        with pytest.raises(ValueError, match="retry_max_delay_seconds"):
            make_gateway(retry_max_delay_seconds=-1.0)

    @pytest.mark.parametrize("make_gateway", _GATEWAY_FACTORIES)
    def test_infinite_max_delay_raises_value_error(self, make_gateway):
        with pytest.raises(ValueError, match="retry_max_delay_seconds"):
            make_gateway(retry_max_delay_seconds=math.inf)

    @pytest.mark.parametrize("make_gateway", _GATEWAY_FACTORIES)
    def test_nan_max_delay_raises_value_error(self, make_gateway):
        with pytest.raises(ValueError, match="retry_max_delay_seconds"):
            make_gateway(retry_max_delay_seconds=math.nan)

    @pytest.mark.parametrize("make_gateway", _GATEWAY_FACTORIES)
    def test_string_max_delay_raises_type_error(self, make_gateway):
        with pytest.raises(TypeError, match="retry_max_delay_seconds"):
            make_gateway(retry_max_delay_seconds="5")

    @pytest.mark.parametrize("make_gateway", _GATEWAY_FACTORIES)
    def test_bool_max_delay_raises_type_error(self, make_gateway):
        with pytest.raises(TypeError, match="retry_max_delay_seconds"):
            make_gateway(retry_max_delay_seconds=True)

    @pytest.mark.parametrize("make_gateway", _GATEWAY_FACTORIES)
    def test_zero_initial_delay_raises_value_error(self, make_gateway):
        with pytest.raises(ValueError, match="retry_initial_delay_seconds"):
            make_gateway(retry_initial_delay_seconds=0)

    @pytest.mark.parametrize("make_gateway", _GATEWAY_FACTORIES)
    def test_negative_initial_delay_raises_value_error(self, make_gateway):
        with pytest.raises(ValueError, match="retry_initial_delay_seconds"):
            make_gateway(retry_initial_delay_seconds=-0.01)

    @pytest.mark.parametrize("make_gateway", _GATEWAY_FACTORIES)
    def test_initial_delay_exceeding_max_delay_raises(self, make_gateway):
        with pytest.raises(ValueError, match="retry_initial_delay_seconds"):
            make_gateway(retry_initial_delay_seconds=10.0, retry_max_delay_seconds=5.0)

    @pytest.mark.parametrize("make_gateway", _GATEWAY_FACTORIES)
    def test_bool_initial_delay_raises_type_error(self, make_gateway):
        with pytest.raises(TypeError, match="retry_initial_delay_seconds"):
            make_gateway(retry_initial_delay_seconds=True)

    @pytest.mark.parametrize("make_gateway", _GATEWAY_FACTORIES)
    def test_default_retry_strategy_is_callable(self, make_gateway):
        gateway = make_gateway()
        assert callable(gateway._retry_strategy)


class TestOperationResultTTLFloor:
    """The operation-result cache TTL is sized from ``retry_budget_seconds``
    so the cached result outlives the retry window with a 180s margin. Equal
    deadlines produce a boundary race where a retry arriving past the budget
    finds the cache just expired and re-runs LREM, wrongly returning 0."""

    @pytest.mark.parametrize(
        "visibility_timeout,retry_budget,expected_ttl_ms",
        [
            (None, 120, "300000"),  # default budget → 120+180 = 300
            (10, 120, "300000"),
            (120, 120, "300000"),
            (299, 120, "300000"),
            (300, 120, "300000"),
            (600, 120, "600000"),  # vt dominates
            (None, 0, "180000"),  # no-retry → just the 180s margin
            (10, 0, "180000"),
            (300, 0, "300000"),  # vt dominates
            (None, 600, "780000"),  # large budget → 600+180
            (1000, 600, "1000000"),  # vt dominates
        ],
    )
    def test_sync_gateway_ttl_floor(self, visibility_timeout, retry_budget, expected_ttl_ms):
        gateway = RedisGateway(
            redis_client=fakeredis.FakeRedis(),
            retry_budget_seconds=retry_budget,
            message_visibility_timeout_seconds=visibility_timeout,
        )
        assert gateway._operation_result_ttl_ms() == expected_ttl_ms
        assert gateway._lease_operation_result_ttl_ms() == expected_ttl_ms

    @pytest.mark.parametrize(
        "visibility_timeout,retry_budget,expected_ttl_ms",
        [
            (None, 120, "300000"),
            (10, 120, "300000"),
            (120, 120, "300000"),
            (299, 120, "300000"),
            (300, 120, "300000"),
            (600, 120, "600000"),
            (None, 0, "180000"),
            (10, 0, "180000"),
            (300, 0, "300000"),
            (None, 600, "780000"),
            (1000, 600, "1000000"),
        ],
    )
    def test_async_gateway_ttl_floor(self, visibility_timeout, retry_budget, expected_ttl_ms):
        gateway = AsyncRedisGateway(
            redis_client=fakeredis.FakeAsyncRedis(),
            retry_budget_seconds=retry_budget,
            message_visibility_timeout_seconds=visibility_timeout,
        )
        assert gateway._operation_result_ttl_ms() == expected_ttl_ms
        assert gateway._lease_operation_result_ttl_ms() == expected_ttl_ms
