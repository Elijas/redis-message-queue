import logging
import typing

import redis
import redis.exceptions
from tenacity import (
    RetryCallState,
    after_log,
    retry,
    retry_base,
    retry_if_exception,
    stop_after_delay,
    wait_exponential_jitter,
)

from redis_message_queue.interrupt_handler._interface import (
    BaseGracefulInterruptHandler,
)

logger = logging.getLogger(__name__)


def is_redis_retryable_exception(exception):
    # 1. Handle ConnectionError hierarchy (retryable except credentials/config issues)
    if isinstance(exception, redis.exceptions.ConnectionError):
        return not isinstance(
            exception,
            (
                redis.exceptions.AuthenticationError,  # Permanent credentials error
                redis.exceptions.AuthorizationError,  # Permanent permissions error
                redis.exceptions.MaxConnectionsError,  # Client-side connection pool exhaustion
            ),
        )

    # 2. Explicit retryable exceptions
    return isinstance(
        exception,
        (
            # Network/availability issues
            redis.exceptions.TimeoutError,  # Socket or server-side timeout
            redis.exceptions.BusyLoadingError,  # Server loading data
            # Cluster transient failures
            redis.exceptions.ClusterDownError,  # Covers ClusterDown + MasterDown
            redis.exceptions.TryAgainError,  # Cluster state requires retry
            # Server-side transient errors
            redis.exceptions.ReadOnlyError,  # Replica might become writable
        ),
    )


class interruptable_retry(retry_base):
    def __init__(
        self,
        interrupt: BaseGracefulInterruptHandler | None,
        get_parent_retry: typing.Callable[[], retry_base],
    ) -> None:
        self._parent_instance = get_parent_retry()
        self.interrupt = interrupt

    def __call__(self, retry_state: RetryCallState) -> bool:
        if self.interrupt is not None and self.interrupt.is_interrupted():
            return False
        return self._parent_instance.__call__(retry_state)


def get_default_redis_connection_retry_strategy(*, interrupt: BaseGracefulInterruptHandler | None = None):
    return retry(
        stop=stop_after_delay(120),
        wait=wait_exponential_jitter(initial=0.01, exp_base=2, max=5, jitter=0.1),
        retry=interruptable_retry(
            interrupt=interrupt,
            get_parent_retry=lambda: retry_if_exception(is_redis_retryable_exception),
        ),
        after=after_log(logger, logging.ERROR),
    )


DEFAULT_MESSAGE_WAIT_INTERVAL_SECONDS = 5


def validate_gateway_parameters(
    message_deduplication_log_ttl_seconds: int,
    message_wait_interval_seconds: int,
    message_visibility_timeout_seconds: int | None = None,
) -> None:
    if not isinstance(message_deduplication_log_ttl_seconds, int) or isinstance(
        message_deduplication_log_ttl_seconds, bool
    ):
        raise TypeError(
            f"'message_deduplication_log_ttl_seconds' must be an int, "
            f"got {type(message_deduplication_log_ttl_seconds).__name__}"
        )
    if not isinstance(message_wait_interval_seconds, int) or isinstance(message_wait_interval_seconds, bool):
        raise TypeError(
            f"'message_wait_interval_seconds' must be an int, got {type(message_wait_interval_seconds).__name__}"
        )
    if message_deduplication_log_ttl_seconds <= 0:
        raise ValueError(
            f"'message_deduplication_log_ttl_seconds' must be positive, got {message_deduplication_log_ttl_seconds}"
        )
    if message_wait_interval_seconds < 0:
        raise ValueError(f"'message_wait_interval_seconds' must be non-negative, got {message_wait_interval_seconds}")
    if message_visibility_timeout_seconds is not None:
        if not isinstance(message_visibility_timeout_seconds, int) or isinstance(message_visibility_timeout_seconds, bool):
            raise TypeError(
                "'message_visibility_timeout_seconds' must be an int or None, "
                f"got {type(message_visibility_timeout_seconds).__name__}"
            )
        if message_visibility_timeout_seconds <= 0:
            raise ValueError(
                "'message_visibility_timeout_seconds' must be positive when provided, "
                f"got {message_visibility_timeout_seconds}"
            )


DEFAULT_MESSAGE_DEDUPLICATION_LOG_TTL = 60 * 60  # 1 hour = 60 seconds * 60 minutes

PUBLISH_MESSAGE_LUA_SCRIPT = """
local was_set = redis.call('SET', KEYS[1], '', 'NX', 'EX', tonumber(ARGV[1]))
if was_set then
    redis.call('LPUSH', KEYS[2], ARGV[2])
    return 1
end
return 0
"""

MOVE_MESSAGE_LUA_SCRIPT = """
local removed = redis.call('LREM', KEYS[1], 1, ARGV[1])
if removed == 1 then
    redis.call('LPUSH', KEYS[2], ARGV[2])
end
return removed
"""

CLAIM_MESSAGE_WITH_VISIBILITY_TIMEOUT_LUA_SCRIPT = """
local time = redis.call('TIME')
local now_ms = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)

local expired = redis.call('ZRANGEBYSCORE', KEYS[3], '-inf', now_ms, 'LIMIT', 0, 100)
local to_requeue = {}
for i = 1, #expired do
    redis.call('ZREM', KEYS[3], expired[i])
    redis.call('HDEL', KEYS[4], expired[i])
    if redis.call('LREM', KEYS[2], 1, expired[i]) == 1 then
        table.insert(to_requeue, expired[i])
    end
end
if #to_requeue > 0 then
    redis.call('RPUSH', KEYS[1], unpack(to_requeue))
end

local stored = redis.call('LMOVE', KEYS[1], KEYS[2], 'RIGHT', 'LEFT')
if not stored then
    return false
end

local lease_token = tostring(redis.call('INCR', KEYS[5]))
redis.call('ZADD', KEYS[3], now_ms + tonumber(ARGV[1]), stored)
redis.call('HSET', KEYS[4], stored, lease_token)

return {stored, lease_token}
"""

REMOVE_MESSAGE_WITH_LEASE_TOKEN_LUA_SCRIPT = """
local current_lease_token = redis.call('HGET', KEYS[3], ARGV[1])
if current_lease_token ~= ARGV[2] then
    return 0
end

local removed = redis.call('LREM', KEYS[1], 1, ARGV[1])
if removed == 1 then
    redis.call('ZREM', KEYS[2], ARGV[1])
    redis.call('HDEL', KEYS[3], ARGV[1])
end

return removed
"""

MOVE_MESSAGE_WITH_LEASE_TOKEN_LUA_SCRIPT = """
local current_lease_token = redis.call('HGET', KEYS[4], ARGV[1])
if current_lease_token ~= ARGV[3] then
    return 0
end

local removed = redis.call('LREM', KEYS[1], 1, ARGV[1])
if removed == 1 then
    redis.call('ZREM', KEYS[3], ARGV[1])
    redis.call('HDEL', KEYS[4], ARGV[1])
    redis.call('LPUSH', KEYS[2], ARGV[2])
end

return removed
"""

RENEW_MESSAGE_LEASE_LUA_SCRIPT = """
local current_lease_token = redis.call('HGET', KEYS[2], ARGV[1])
if current_lease_token ~= ARGV[2] then
    return 0
end

local time = redis.call('TIME')
local now_ms = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
redis.call('ZADD', KEYS[1], now_ms + tonumber(ARGV[3]), ARGV[1])

return 1
"""
