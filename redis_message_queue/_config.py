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
        after=after_log(logger, logging.WARNING),
        reraise=True,
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
        if not isinstance(message_visibility_timeout_seconds, int) or isinstance(
            message_visibility_timeout_seconds, bool
        ):
            raise TypeError(
                "'message_visibility_timeout_seconds' must be an int or None, "
                f"got {type(message_visibility_timeout_seconds).__name__}"
            )
        if message_visibility_timeout_seconds <= 0:
            raise ValueError(
                "'message_visibility_timeout_seconds' must be positive when provided, "
                f"got {message_visibility_timeout_seconds}"
            )


def validate_dead_letter_parameters(
    max_delivery_count: int | None,
    dead_letter_queue: str | None,
    message_visibility_timeout_seconds: int | None,
) -> None:
    if max_delivery_count is not None:
        if not isinstance(max_delivery_count, int) or isinstance(max_delivery_count, bool):
            raise TypeError(f"'max_delivery_count' must be an int or None, got {type(max_delivery_count).__name__}")
        if max_delivery_count <= 0:
            raise ValueError(f"'max_delivery_count' must be positive, got {max_delivery_count}")
        if message_visibility_timeout_seconds is None:
            raise ValueError("'max_delivery_count' requires 'message_visibility_timeout_seconds' to be set.")
    if dead_letter_queue is not None and not isinstance(dead_letter_queue, str):
        raise TypeError(f"'dead_letter_queue' must be a str or None, got {type(dead_letter_queue).__name__}")
    if max_delivery_count is not None and not dead_letter_queue:
        raise ValueError("'dead_letter_queue' is required when 'max_delivery_count' is set.")
    if dead_letter_queue and max_delivery_count is None:
        raise ValueError("'max_delivery_count' is required when 'dead_letter_queue' is set.")


DEFAULT_MESSAGE_DEDUPLICATION_LOG_TTL = 60 * 60  # 1 hour = 60 seconds * 60 minutes

_LUA_KEY_TYPE_GUARD = """
local function redis_message_queue_key_type(key)
    local type_result = redis.call('TYPE', key)
    if type(type_result) == 'table' then
        return type_result['ok']
    end
    return type_result
end

local function redis_message_queue_require_type(key, expected_type)
    local actual_type = redis_message_queue_key_type(key)
    if actual_type ~= 'none' and actual_type ~= expected_type then
        return redis.error_reply('WRONGTYPE Operation against a key holding the wrong kind of value')
    end
    return nil
end
"""

PUBLISH_MESSAGE_LUA_SCRIPT = (
    _LUA_KEY_TYPE_GUARD
    + """
local err = redis_message_queue_require_type(KEYS[1], 'string')
if err then
    return err
end

local err = redis_message_queue_require_type(KEYS[2], 'list')
if err then
    return err
end

local err = redis_message_queue_require_type(KEYS[3], 'string')
if err then
    return err
end

local cached_result = redis.call('GET', KEYS[3])
if cached_result then
    return tonumber(cached_result)
end

local result = 0
local was_set = redis.call('SET', KEYS[1], '', 'NX', 'EX', tonumber(ARGV[1]))
if was_set then
    redis.call('LPUSH', KEYS[2], ARGV[2])
    result = 1
end

redis.call('SET', KEYS[3], tostring(result), 'PX', tonumber(ARGV[3]))
return result
"""
)

MOVE_MESSAGE_LUA_SCRIPT = (
    _LUA_KEY_TYPE_GUARD
    + """
local err = redis_message_queue_require_type(KEYS[1], 'list')
if err then
    return err
end

local err = redis_message_queue_require_type(KEYS[2], 'list')
if err then
    return err
end

local err = redis_message_queue_require_type(KEYS[3], 'hash')
if err then
    return err
end

local err = redis_message_queue_require_type(KEYS[4], 'hash')
if err then
    return err
end

local err = redis_message_queue_require_type(KEYS[5], 'string')
if err then
    return err
end

local cached_result = redis.call('GET', KEYS[5])
if cached_result then
    return tonumber(cached_result)
end

local removed = redis.call('LREM', KEYS[1], 1, ARGV[1])
if removed == 1 then
    local claim_id = redis.call('HGET', KEYS[4], ARGV[1])
    if claim_id then
        redis.call('HDEL', KEYS[3], claim_id)
        redis.call('HDEL', KEYS[4], ARGV[1])
    end
    redis.call('LPUSH', KEYS[2], ARGV[2])
end

redis.call('SET', KEYS[5], tostring(removed), 'PX', tonumber(ARGV[3]))
return removed
"""
)

REMOVE_MESSAGE_LUA_SCRIPT = (
    _LUA_KEY_TYPE_GUARD
    + """
local err = redis_message_queue_require_type(KEYS[1], 'list')
if err then
    return err
end

local err = redis_message_queue_require_type(KEYS[2], 'hash')
if err then
    return err
end

local err = redis_message_queue_require_type(KEYS[3], 'hash')
if err then
    return err
end

local err = redis_message_queue_require_type(KEYS[4], 'string')
if err then
    return err
end

local cached_result = redis.call('GET', KEYS[4])
if cached_result then
    return tonumber(cached_result)
end

local removed = redis.call('LREM', KEYS[1], 1, ARGV[1])
if removed == 1 then
    local claim_id = redis.call('HGET', KEYS[3], ARGV[1])
    if claim_id then
        redis.call('HDEL', KEYS[2], claim_id)
        redis.call('HDEL', KEYS[3], ARGV[1])
    end
end

redis.call('SET', KEYS[4], tostring(removed), 'PX', tonumber(ARGV[2]))
return removed
"""
)

CLAIM_MESSAGE_LUA_SCRIPT = (
    _LUA_KEY_TYPE_GUARD
    + """
local err = redis_message_queue_require_type(KEYS[1], 'list')
if err then
    return err
end

local err = redis_message_queue_require_type(KEYS[2], 'list')
if err then
    return err
end

local err = redis_message_queue_require_type(KEYS[3], 'string')
if err then
    return err
end

local err = redis_message_queue_require_type(KEYS[4], 'hash')
if err then
    return err
end

local err = redis_message_queue_require_type(KEYS[5], 'hash')
if err then
    return err
end

local cached_claim = redis.call('GET', KEYS[3])
if cached_claim then
    redis.call('HSET', KEYS[4], ARGV[2], cached_claim)
    redis.call('HSET', KEYS[5], cached_claim, ARGV[2])
    return cached_claim
end

local cached_recovery = redis.call('HGET', KEYS[4], ARGV[2])
if cached_recovery then
    redis.call('SET', KEYS[3], cached_recovery, 'PX', tonumber(ARGV[1]))
    redis.call('HSET', KEYS[5], cached_recovery, ARGV[2])
    return cached_recovery
end

local stored = redis.call('LMOVE', KEYS[1], KEYS[2], 'RIGHT', 'LEFT')
if not stored then
    return false
end

redis.call('SET', KEYS[3], stored, 'PX', tonumber(ARGV[1]))
redis.call('HSET', KEYS[4], ARGV[2], stored)
redis.call('HSET', KEYS[5], stored, ARGV[2])
return stored
"""
)

CLAIM_MESSAGE_WITH_VISIBILITY_TIMEOUT_LUA_SCRIPT = (
    _LUA_KEY_TYPE_GUARD
    + """
local err = redis_message_queue_require_type(KEYS[1], 'list')
if err then
    return err
end

local err = redis_message_queue_require_type(KEYS[2], 'list')
if err then
    return err
end

local err = redis_message_queue_require_type(KEYS[3], 'zset')
if err then
    return err
end

local err = redis_message_queue_require_type(KEYS[4], 'hash')
if err then
    return err
end

local err = redis_message_queue_require_type(KEYS[5], 'string')
if err then
    return err
end

local err = redis_message_queue_require_type(KEYS[6], 'hash')
if err then
    return err
end

local err = redis_message_queue_require_type(KEYS[8], 'string')
if err then
    return err
end

local err = redis_message_queue_require_type(KEYS[9], 'hash')
if err then
    return err
end

local err = redis_message_queue_require_type(KEYS[10], 'hash')
if err then
    return err
end

local err = redis_message_queue_require_type(KEYS[11], 'hash')
if err then
    return err
end

local max_delivery_count = tonumber(ARGV[2])
if max_delivery_count > 0 then
    local err = redis_message_queue_require_type(KEYS[7], 'list')
    if err then
        return err
    end
end

local function redis_message_queue_decode_claim(cached_claim)
    local ok, claim = pcall(cjson.decode, cached_claim)
    if ok and type(claim) == 'table' and type(claim[1]) == 'string' and type(claim[2]) == 'string' then
        return claim
    end
    return nil
end

local cached_claim = redis.call('GET', KEYS[8])
if cached_claim then
    local claim = redis_message_queue_decode_claim(cached_claim)
    if claim then
        redis.call('HSET', KEYS[10], ARGV[4], cached_claim)
        redis.call('HSET', KEYS[11], claim[2], ARGV[4])
        redis.call('HSET', KEYS[9], claim[2], KEYS[8])
        return {claim[1], claim[2]}
    end
    redis.call('DEL', KEYS[8])
end

local cached_recovery = redis.call('HGET', KEYS[10], ARGV[4])
if cached_recovery then
    local claim = redis_message_queue_decode_claim(cached_recovery)
    if claim then
        redis.call('SET', KEYS[8], cached_recovery, 'PX', tonumber(ARGV[3]))
        redis.call('HSET', KEYS[11], claim[2], ARGV[4])
        redis.call('HSET', KEYS[9], claim[2], KEYS[8])
        return {claim[1], claim[2]}
    end
    redis.call('HDEL', KEYS[10], ARGV[4])
end

local time = redis.call('TIME')
local now_ms = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)

local expired = redis.call('ZRANGEBYSCORE', KEYS[3], '-inf', now_ms, 'LIMIT', 0, 100)
local to_requeue = {}
for i = #expired, 1, -1 do
    local expired_lease_token = redis.call('HGET', KEYS[4], expired[i])
    redis.call('ZREM', KEYS[3], expired[i])
    redis.call('HDEL', KEYS[4], expired[i])
    if expired_lease_token then
        local claim_result_key = redis.call('HGET', KEYS[9], expired_lease_token)
        if claim_result_key then
            redis.call('DEL', claim_result_key)
            redis.call('HDEL', KEYS[9], expired_lease_token)
        end
        local claim_id = redis.call('HGET', KEYS[11], expired_lease_token)
        if claim_id then
            redis.call('HDEL', KEYS[10], claim_id)
            redis.call('HDEL', KEYS[11], expired_lease_token)
        end
    end
    if redis.call('LREM', KEYS[2], 1, expired[i]) == 1 then
        table.insert(to_requeue, expired[i])
    end
end
if #to_requeue > 0 then
    redis.call('RPUSH', KEYS[1], unpack(to_requeue))
end

local function store_claim_and_return(stored)
    local lease_token = tostring(redis.call('INCR', KEYS[5]))
    local claim_payload = cjson.encode({stored, lease_token})
    redis.call('ZADD', KEYS[3], now_ms + tonumber(ARGV[1]), stored)
    redis.call('HSET', KEYS[4], stored, lease_token)
    redis.call('SET', KEYS[8], claim_payload, 'PX', tonumber(ARGV[3]))
    redis.call('HSET', KEYS[9], lease_token, KEYS[8])
    redis.call('HSET', KEYS[10], ARGV[4], claim_payload)
    redis.call('HSET', KEYS[11], lease_token, ARGV[4])
    return {stored, lease_token}
end

local claim_attempts = 0
while claim_attempts < 100 do
    claim_attempts = claim_attempts + 1

    local stored = redis.call('LMOVE', KEYS[1], KEYS[2], 'RIGHT', 'LEFT')
    if not stored then
        return false
    end

    if max_delivery_count > 0 then
        local count = redis.call('HINCRBY', KEYS[6], stored, 1)
        if count > max_delivery_count then
            redis.call('LREM', KEYS[2], 1, stored)
            redis.call('HDEL', KEYS[6], stored)
            local dead_letter_value = stored
            local prefix = string.char(30) .. 'RMQ1:'
            if string.sub(stored, 1, string.len(prefix)) == prefix then
                local ok, envelope = pcall(cjson.decode, string.sub(stored, string.len(prefix) + 1))
                if ok and type(envelope) == 'table' and type(envelope['id']) == 'string'
                        and type(envelope['payload']) == 'string' then
                    dead_letter_value = envelope['payload']
                end
            end
            redis.call('LPUSH', KEYS[7], dead_letter_value)
        else
            return store_claim_and_return(stored)
        end
    else
        return store_claim_and_return(stored)
    end
end

return false
"""
)

REMOVE_MESSAGE_WITH_LEASE_TOKEN_LUA_SCRIPT = (
    _LUA_KEY_TYPE_GUARD
    + """
local err = redis_message_queue_require_type(KEYS[1], 'list')
if err then
    return err
end

local err = redis_message_queue_require_type(KEYS[2], 'zset')
if err then
    return err
end

local err = redis_message_queue_require_type(KEYS[3], 'hash')
if err then
    return err
end

local err = redis_message_queue_require_type(KEYS[4], 'hash')
if err then
    return err
end

local err = redis_message_queue_require_type(KEYS[5], 'hash')
if err then
    return err
end

local err = redis_message_queue_require_type(KEYS[6], 'hash')
if err then
    return err
end

local err = redis_message_queue_require_type(KEYS[7], 'hash')
if err then
    return err
end

local err = redis_message_queue_require_type(KEYS[8], 'string')
if err then
    return err
end

local current_lease_token = redis.call('HGET', KEYS[3], ARGV[1])
if current_lease_token ~= ARGV[2] then
    if redis.call('GET', KEYS[8]) then
        return 1
    end
    return 0
end

local removed = redis.call('LREM', KEYS[1], 1, ARGV[1])
if removed == 1 then
    redis.call('ZREM', KEYS[2], ARGV[1])
    redis.call('HDEL', KEYS[3], ARGV[1])
    local claim_result_key = redis.call('HGET', KEYS[5], ARGV[2])
    if claim_result_key then
        redis.call('DEL', claim_result_key)
        redis.call('HDEL', KEYS[5], ARGV[2])
    end
    local claim_id = redis.call('HGET', KEYS[7], ARGV[2])
    if claim_id then
        redis.call('HDEL', KEYS[6], claim_id)
        redis.call('HDEL', KEYS[7], ARGV[2])
    end
    if KEYS[4] then
        redis.call('HDEL', KEYS[4], ARGV[1])
    end
    redis.call('SET', KEYS[8], '1', 'PX', tonumber(ARGV[3]))
end

return removed
"""
)

MOVE_MESSAGE_WITH_LEASE_TOKEN_LUA_SCRIPT = (
    _LUA_KEY_TYPE_GUARD
    + """
local err = redis_message_queue_require_type(KEYS[1], 'list')
if err then
    return err
end

local err = redis_message_queue_require_type(KEYS[2], 'list')
if err then
    return err
end

local err = redis_message_queue_require_type(KEYS[3], 'zset')
if err then
    return err
end

local err = redis_message_queue_require_type(KEYS[4], 'hash')
if err then
    return err
end

local err = redis_message_queue_require_type(KEYS[5], 'hash')
if err then
    return err
end

local err = redis_message_queue_require_type(KEYS[6], 'hash')
if err then
    return err
end

local err = redis_message_queue_require_type(KEYS[7], 'hash')
if err then
    return err
end

local err = redis_message_queue_require_type(KEYS[8], 'hash')
if err then
    return err
end

local err = redis_message_queue_require_type(KEYS[9], 'string')
if err then
    return err
end

local current_lease_token = redis.call('HGET', KEYS[4], ARGV[1])
if current_lease_token ~= ARGV[3] then
    if redis.call('GET', KEYS[9]) then
        return 1
    end
    return 0
end

local removed = redis.call('LREM', KEYS[1], 1, ARGV[1])
if removed == 1 then
    redis.call('ZREM', KEYS[3], ARGV[1])
    redis.call('HDEL', KEYS[4], ARGV[1])
    local claim_result_key = redis.call('HGET', KEYS[6], ARGV[3])
    if claim_result_key then
        redis.call('DEL', claim_result_key)
        redis.call('HDEL', KEYS[6], ARGV[3])
    end
    local claim_id = redis.call('HGET', KEYS[8], ARGV[3])
    if claim_id then
        redis.call('HDEL', KEYS[7], claim_id)
        redis.call('HDEL', KEYS[8], ARGV[3])
    end
    if KEYS[5] then
        redis.call('HDEL', KEYS[5], ARGV[1])
    end
    redis.call('LPUSH', KEYS[2], ARGV[2])
    redis.call('SET', KEYS[9], '1', 'PX', tonumber(ARGV[4]))
end

return removed
"""
)

RENEW_MESSAGE_LEASE_LUA_SCRIPT = (
    _LUA_KEY_TYPE_GUARD
    + """
local err = redis_message_queue_require_type(KEYS[1], 'zset')
if err then
    return err
end

local err = redis_message_queue_require_type(KEYS[2], 'hash')
if err then
    return err
end

local current_lease_token = redis.call('HGET', KEYS[2], ARGV[1])
if current_lease_token ~= ARGV[2] then
    return 0
end

local time = redis.call('TIME')
local now_ms = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
redis.call('ZADD', KEYS[1], now_ms + tonumber(ARGV[3]), ARGV[1])

return 1
"""
)
