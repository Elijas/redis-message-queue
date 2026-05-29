import asyncio
import inspect
import logging
import math
import threading
import time
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

from redis_message_queue._exceptions import ConfigurationError, RetryBudgetExhaustedError
from redis_message_queue.interrupt_handler._interface import (
    BaseGracefulInterruptHandler,
)

logger = logging.getLogger(__name__)

DEFAULT_RETRY_BUDGET_SECONDS = 120
DEFAULT_RETRY_MAX_DELAY_SECONDS = 5.0
DEFAULT_RETRY_INITIAL_DELAY_SECONDS = 0.01
DEFAULT_PENDING_OVERLOAD_BLOCK_TIMEOUT_SECONDS = 1.0
INTERRUPTIBLE_RETRY_SLEEP_POLL_SECONDS = 0.05
PENDING_OVERLOAD_LUA_SENTINEL = -1
CLAIM_STORE_FAILED_LUA_SENTINEL = "\0__rmq_claim_store_failed__"
PENDING_OVERLOAD_POLICIES = ("raise", "drop_oldest", "block")
DEDUPLICATION_REQUIRES_KEY_MESSAGE = (
    "deduplication=True requires get_deduplication_key (callable returning a non-empty str). "
    "Pass a callable like `lambda msg: msg['id']` (recommended: a stable logical ID), "
    "or set deduplication=False."
)


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

    if isinstance(exception, redis.exceptions.ClusterError) and "TTL exhausted" in str(exception):
        return True

    no_script_error = getattr(redis.exceptions, "NoScriptError", None)
    if no_script_error is not None and isinstance(exception, no_script_error):
        return True

    if isinstance(exception, redis.exceptions.ResponseError) and str(exception).startswith("NOSCRIPT"):
        return True

    # 2. Explicit retryable exceptions (BusyLoadingError is a ConnectionError
    #    subclass, so it is already handled by branch 1 above)
    return isinstance(
        exception,
        (
            # Network/availability issues
            redis.exceptions.TimeoutError,  # Socket or server-side timeout
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


class _ChainedInterrupt(BaseGracefulInterruptHandler):
    """Compose interrupt handlers: trip if any composed handler trips.

    Used to merge the gateway-level interrupt (process shutdown signal) with
    a per-call stop signal (e.g. a heartbeat's ``threading.Event``) so that
    both can abort a retry loop. Each constituent handler is queried in
    order; the first that returns True short-circuits.
    """

    def __init__(self, *handlers: BaseGracefulInterruptHandler | None) -> None:
        self._handlers: tuple[BaseGracefulInterruptHandler, ...] = tuple(h for h in handlers if h is not None)

    def is_interrupted(self) -> bool:
        return any(h.is_interrupted() for h in self._handlers)


def _iter_interrupt_handlers(
    interrupt: BaseGracefulInterruptHandler | None,
) -> typing.Iterator[BaseGracefulInterruptHandler]:
    if interrupt is None:
        return
    if isinstance(interrupt, _ChainedInterrupt):
        yield from interrupt._handlers
        return
    yield interrupt


def _handler_stop_event(handler: BaseGracefulInterruptHandler) -> object | None:
    return getattr(handler, "_stop_event", None)


def _sync_interruptible_sleep(interrupt: BaseGracefulInterruptHandler):
    def sleep(timeout: int | float | None) -> None:
        if timeout is None:
            return
        deadline = time.monotonic() + max(float(timeout), 0.0)
        while True:
            if interrupt.is_interrupted():
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            interval = min(remaining, INTERRUPTIBLE_RETRY_SLEEP_POLL_SECONDS)
            for handler in _iter_interrupt_handlers(interrupt):
                stop_event = _handler_stop_event(handler)
                if isinstance(stop_event, threading.Event):
                    stop_event.wait(timeout=interval)
                    break
            else:
                time.sleep(interval)

    return sleep


def _async_interruptible_sleep(interrupt: BaseGracefulInterruptHandler):
    async def sleep(timeout: int | float | None) -> None:
        if timeout is None:
            return
        deadline = time.monotonic() + max(float(timeout), 0.0)
        while True:
            if interrupt.is_interrupted():
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            interval = min(remaining, INTERRUPTIBLE_RETRY_SLEEP_POLL_SECONDS)
            for handler in _iter_interrupt_handlers(interrupt):
                stop_event = _handler_stop_event(handler)
                if isinstance(stop_event, asyncio.Event):
                    try:
                        await asyncio.wait_for(stop_event.wait(), timeout=interval)
                    except TimeoutError:
                        pass
                    break
            else:
                await asyncio.sleep(interval)

    return sleep


def _noop_retry(func):
    return func


def _raise_retry_budget_exhausted(retry_state: RetryCallState) -> typing.NoReturn:
    exc = retry_state.outcome.exception() if retry_state.outcome is not None else None
    if exc is None:
        raise RetryBudgetExhaustedError("Redis retry budget exhausted")
    raise RetryBudgetExhaustedError(
        f"Redis retry budget exhausted after {retry_state.attempt_number} attempts"
    ) from exc


def build_retry_strategy(
    *,
    retry_budget_seconds: int,
    retry_max_delay_seconds: float,
    retry_initial_delay_seconds: float,
    interrupt: BaseGracefulInterruptHandler | None = None,
):
    if retry_budget_seconds == 0:
        return _noop_retry
    retry_kwargs = {
        "stop": stop_after_delay(retry_budget_seconds),
        "wait": wait_exponential_jitter(
            initial=retry_initial_delay_seconds,
            exp_base=2,
            max=retry_max_delay_seconds,
            jitter=0.1,
        ),
        "retry": interruptable_retry(
            interrupt=interrupt,
            get_parent_retry=lambda: retry_if_exception(is_redis_retryable_exception),
        ),
        "after": after_log(logger, logging.WARNING),
        "retry_error_callback": _raise_retry_budget_exhausted,
        "reraise": True,
    }

    def retry_decorator(func):
        if interrupt is None:
            return retry(**retry_kwargs)(func)

        sleep = (
            _async_interruptible_sleep(interrupt)
            if inspect.iscoroutinefunction(func)
            else _sync_interruptible_sleep(interrupt)
        )
        return retry(sleep=sleep, **retry_kwargs)(func)

    return retry_decorator


DEFAULT_MESSAGE_WAIT_INTERVAL_SECONDS = 5


def validate_gateway_parameters(
    message_deduplication_log_ttl_seconds: int,
    message_wait_interval_seconds: int,
    message_visibility_timeout_seconds: int | None = None,
    *,
    retry_budget_seconds: int,
    retry_max_delay_seconds: float,
    retry_initial_delay_seconds: float,
) -> None:
    if not isinstance(message_deduplication_log_ttl_seconds, int) or isinstance(
        message_deduplication_log_ttl_seconds, bool
    ):
        bool_hint = " (use True or False, not 1/0)" if isinstance(message_deduplication_log_ttl_seconds, bool) else ""
        raise TypeError(
            f"'message_deduplication_log_ttl_seconds' must be an int, "
            f"got {type(message_deduplication_log_ttl_seconds).__name__}{bool_hint}"
        )
    if not isinstance(message_wait_interval_seconds, int) or isinstance(message_wait_interval_seconds, bool):
        bool_hint = " (use True or False, not 1/0)" if isinstance(message_wait_interval_seconds, bool) else ""
        raise TypeError(
            f"'message_wait_interval_seconds' must be an int, "
            f"got {type(message_wait_interval_seconds).__name__}{bool_hint}"
        )
    if message_deduplication_log_ttl_seconds <= 0:
        raise ConfigurationError(
            f"'message_deduplication_log_ttl_seconds' must be positive, "
            f"got {message_deduplication_log_ttl_seconds}. Use a positive int in seconds "
            "(e.g., 3600) to keep deduplication keys for that window."
        )
    if message_wait_interval_seconds < 0:
        raise ConfigurationError(
            f"'message_wait_interval_seconds' must be non-negative, got {message_wait_interval_seconds}. "
            "Use 0 for non-blocking polls or a positive int in seconds to wait for messages."
        )
    if message_visibility_timeout_seconds is not None:
        if not isinstance(message_visibility_timeout_seconds, int) or isinstance(
            message_visibility_timeout_seconds, bool
        ):
            bool_hint = " (use True or False, not 1/0)" if isinstance(message_visibility_timeout_seconds, bool) else ""
            raise TypeError(
                "'message_visibility_timeout_seconds' must be an int or None, "
                f"got {type(message_visibility_timeout_seconds).__name__}{bool_hint}"
            )
        if message_visibility_timeout_seconds <= 0:
            raise ConfigurationError(
                "'message_visibility_timeout_seconds' must be positive when provided, "
                f"got {message_visibility_timeout_seconds}. Use a positive int in seconds for at-least-once "
                "redelivery or None for at-most-once processing."
            )

    if not isinstance(retry_budget_seconds, int) or isinstance(retry_budget_seconds, bool):
        bool_hint = " (use True or False, not 1/0)" if isinstance(retry_budget_seconds, bool) else ""
        raise TypeError(f"'retry_budget_seconds' must be an int, got {type(retry_budget_seconds).__name__}{bool_hint}")
    if retry_budget_seconds < 0:
        raise ConfigurationError(
            f"'retry_budget_seconds' must be non-negative, got {retry_budget_seconds}. "
            "Use 0 to disable redis-message-queue retries or a positive int in seconds to bound retry time."
        )

    if isinstance(retry_max_delay_seconds, bool) or not isinstance(retry_max_delay_seconds, (int, float)):
        bool_hint = " (use True or False, not 1/0)" if isinstance(retry_max_delay_seconds, bool) else ""
        raise TypeError(
            f"'retry_max_delay_seconds' must be a number, got {type(retry_max_delay_seconds).__name__}{bool_hint}"
        )
    if not math.isfinite(retry_max_delay_seconds) or retry_max_delay_seconds <= 0:
        raise ConfigurationError(
            f"'retry_max_delay_seconds' must be a finite positive number, got {retry_max_delay_seconds}. "
            "Use a positive number of seconds (e.g., 5.0) for the maximum retry backoff delay."
        )

    if isinstance(retry_initial_delay_seconds, bool) or not isinstance(retry_initial_delay_seconds, (int, float)):
        bool_hint = " (use True or False, not 1/0)" if isinstance(retry_initial_delay_seconds, bool) else ""
        raise TypeError(
            f"'retry_initial_delay_seconds' must be a number, "
            f"got {type(retry_initial_delay_seconds).__name__}{bool_hint}"
        )
    if not math.isfinite(retry_initial_delay_seconds) or retry_initial_delay_seconds <= 0:
        raise ConfigurationError(
            f"'retry_initial_delay_seconds' must be a finite positive number, got {retry_initial_delay_seconds}. "
            "Use a positive number of seconds (e.g., 0.01) for the first retry backoff delay."
        )
    if retry_initial_delay_seconds > retry_max_delay_seconds:
        raise ConfigurationError(
            "'retry_initial_delay_seconds' must be <= 'retry_max_delay_seconds', "
            f"got {retry_initial_delay_seconds} > {retry_max_delay_seconds}. "
            "Use an initial delay less than or equal to the max delay."
        )


def validate_dedup_configuration(
    *,
    deduplication: bool,
    get_deduplication_key: object,
) -> None:
    if deduplication and get_deduplication_key is None:
        raise ConfigurationError(DEDUPLICATION_REQUIRES_KEY_MESSAGE)


def validate_pending_backpressure_parameters(
    max_pending_length: int | None,
    pending_overload_policy: str,
    pending_overload_block_timeout_seconds: int | float,
    *,
    deduplication: bool | None = None,
    get_deduplication_key_configured: bool = False,
    max_delivery_count: int | None = None,
) -> None:
    if max_pending_length is not None:
        if not isinstance(max_pending_length, int) or isinstance(max_pending_length, bool):
            bool_hint = " (use True or False, not 1/0)" if isinstance(max_pending_length, bool) else ""
            raise TypeError(
                f"'max_pending_length' must be an int or None, got {type(max_pending_length).__name__}{bool_hint}"
            )
        if max_pending_length <= 0:
            raise ConfigurationError(
                f"'max_pending_length' must be positive when provided, got {max_pending_length}. "
                "Use a positive int (e.g., 1000) or None to disable the cap."
            )
    if not isinstance(pending_overload_policy, str):
        raise TypeError(f"'pending_overload_policy' must be a string, got {type(pending_overload_policy).__name__}")
    if pending_overload_policy not in PENDING_OVERLOAD_POLICIES:
        allowed = "', '".join(PENDING_OVERLOAD_POLICIES)
        raise ConfigurationError(
            f"'pending_overload_policy' must be one of '{allowed}', got {pending_overload_policy!r}. "
            "Use 'raise' to reject immediately, 'block' to wait for capacity, or 'drop_oldest' only for lossy queues."
        )
    if pending_overload_policy == "drop_oldest" and max_pending_length is None:
        raise ConfigurationError(
            "drop_oldest requires max_pending_length to be set. "
            "Use a positive max_pending_length to define what can be dropped, or use "
            "pending_overload_policy='raise' for an unbounded queue."
        )
    if pending_overload_policy == "block" and max_pending_length is None:
        raise ConfigurationError(
            "block requires max_pending_length to be set. "
            "Use a positive max_pending_length to define the threshold to block on, or use "
            "pending_overload_policy='raise' for an unbounded queue."
        )
    if pending_overload_policy == "drop_oldest" and (deduplication or get_deduplication_key_configured):
        raise ConfigurationError(
            "'pending_overload_policy=drop_oldest' cannot be used with deduplication because dropped messages "
            "leave their deduplication keys in Redis, causing future publishes of the same payload to be "
            "silently suppressed. Use 'raise' or 'block' for deduplicated queues, or disable deduplication if "
            "'drop_oldest' is required."
        )
    if pending_overload_policy == "drop_oldest" and max_delivery_count is not None:
        raise ConfigurationError(
            "drop_oldest is incompatible with max_delivery_count "
            "(set max_delivery_count=None or pick another policy "
            "to avoid silent loss of pending DLQ candidates). Use pending_overload_policy='raise' or 'block' "
            "when dead-letter handling is required."
        )
    if isinstance(pending_overload_block_timeout_seconds, bool) or not isinstance(
        pending_overload_block_timeout_seconds, (int, float)
    ):
        bool_hint = " (use True or False, not 1/0)" if isinstance(pending_overload_block_timeout_seconds, bool) else ""
        raise TypeError(
            "'pending_overload_block_timeout_seconds' must be a number, "
            f"got {type(pending_overload_block_timeout_seconds).__name__}{bool_hint}"
        )
    if not math.isfinite(pending_overload_block_timeout_seconds) or pending_overload_block_timeout_seconds < 0:
        raise ConfigurationError(
            "'pending_overload_block_timeout_seconds' must be a finite non-negative number, "
            f"got {pending_overload_block_timeout_seconds}. Use 0 to fail immediately under 'block' policy "
            "or a positive number of seconds to wait for capacity."
        )


def validate_dead_letter_parameters(
    max_delivery_count: int | None,
    dead_letter_queue: str | None,
    message_visibility_timeout_seconds: int | None,
) -> None:
    if max_delivery_count is not None:
        if not isinstance(max_delivery_count, int) or isinstance(max_delivery_count, bool):
            bool_hint = " (use True or False, not 1/0)" if isinstance(max_delivery_count, bool) else ""
            raise TypeError(
                f"'max_delivery_count' must be an int or None, got {type(max_delivery_count).__name__}{bool_hint}"
            )
        if max_delivery_count <= 0:
            raise ConfigurationError(
                f"'max_delivery_count' must be positive, got {max_delivery_count}. "
                "Use a positive int (e.g., 5) to dead-letter poison messages or None to disable delivery limits."
            )
        if message_visibility_timeout_seconds is None:
            raise ConfigurationError(
                "'max_delivery_count' requires 'message_visibility_timeout_seconds' to be set. "
                "Use a positive visibility timeout so failed deliveries can be counted before DLQ routing."
            )
    if dead_letter_queue is not None and not isinstance(dead_letter_queue, str):
        bool_hint = " (use True or False, not 1/0)" if isinstance(dead_letter_queue, bool) else ""
        raise TypeError(f"'dead_letter_queue' must be a str or None, got {type(dead_letter_queue).__name__}{bool_hint}")
    if isinstance(dead_letter_queue, str) and dead_letter_queue and not dead_letter_queue.strip():
        raise ConfigurationError(
            f"'dead_letter_queue' must contain non-whitespace characters; got {dead_letter_queue!r}. "
            "Use a real Redis list key name (e.g., 'jobs:dead') or None to disable DLQ routing."
        )
    if max_delivery_count is not None and not dead_letter_queue:
        raise ConfigurationError(
            "'dead_letter_queue' is required when 'max_delivery_count' is set. "
            "Use a Redis list key name for poison messages or set max_delivery_count=None."
        )
    if dead_letter_queue and max_delivery_count is None:
        raise ConfigurationError(
            "'max_delivery_count' is required when 'dead_letter_queue' is set. "
            "Use a positive max_delivery_count to route poison messages or set dead_letter_queue=None."
        )


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

local max_pending_length = tonumber(ARGV[4])
local pending_overload_policy = ARGV[5]
if max_pending_length then
    if redis.call('EXISTS', KEYS[1]) == 1 then
        redis.call('SET', KEYS[3], '0', 'PX', tonumber(ARGV[3]))
        return 0
    end
    if redis.call('LLEN', KEYS[2]) >= max_pending_length then
        if pending_overload_policy == 'drop_oldest' then
            redis.call('RPOP', KEYS[2])
        else
            return -1
        end
    end
end

local result = 0
local was_set = redis.call('SET', KEYS[1], '', 'NX', 'EX', tonumber(ARGV[1]))
if was_set then
    -- pcall guards against LPUSH OOM after the dedup key was committed.
    -- Without this, OOM would strand the publish: dedup says "already
    -- published" on retry, but the message is in no queue. Compensate by
    -- clearing the dedup key so the retry can re-attempt.
    local ok = pcall(function()
        redis.call('LPUSH', KEYS[2], ARGV[2])
    end)
    if not ok then
        redis.pcall('DEL', KEYS[1])
        return redis.error_reply('OOM during publish; dedup key cleared for retry')
    end
    result = 1
end

redis.call('SET', KEYS[3], tostring(result), 'PX', tonumber(ARGV[3]))
return result
"""
)

ADD_MESSAGE_LUA_SCRIPT = (
    _LUA_KEY_TYPE_GUARD
    + """
local err = redis_message_queue_require_type(KEYS[1], 'list')
if err then
    return err
end

local max_pending_length = tonumber(ARGV[2])
local pending_overload_policy = ARGV[3]
if max_pending_length and redis.call('LLEN', KEYS[1]) >= max_pending_length then
    if pending_overload_policy == 'drop_oldest' then
        redis.call('RPOP', KEYS[1])
    else
        return -1
    end
end

redis.call('LPUSH', KEYS[1], ARGV[1])
return 1
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

redis.call('LPUSH', KEYS[2], ARGV[2])
local removed = redis.call('LREM', KEYS[1], 1, ARGV[1])
if removed == 1 then
    local claim_id = redis.call('HGET', KEYS[4], ARGV[1])
    if claim_id then
        redis.call('HDEL', KEYS[3], claim_id)
        redis.call('HDEL', KEYS[4], ARGV[1])
    end
else
    redis.call('LREM', KEYS[2], 1, ARGV[2])
end

redis.call('SET', KEYS[5], tostring(removed), 'PX', tonumber(ARGV[3]))
return removed
"""
)

RETURN_MESSAGE_TO_PENDING_LUA_SCRIPT = (
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

redis.call('RPUSH', KEYS[2], ARGV[1])
local removed = redis.call('LREM', KEYS[1], 1, ARGV[1])
if removed == 1 then
    redis.call('HDEL', KEYS[3], ARGV[2])
    redis.call('HDEL', KEYS[4], ARGV[1])
    redis.call('DEL', KEYS[6])
else
    redis.call('LREM', KEYS[2], 1, ARGV[1])
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

local function redis_message_queue_decode_envelope(stored)
    local prefix = string.char(30) .. 'RMQ1:'
    if type(stored) ~= 'string' or string.sub(stored, 1, string.len(prefix)) ~= prefix then
        return nil
    end
    local ok, envelope = pcall(cjson.decode, string.sub(stored, string.len(prefix) + 1))
    if ok and type(envelope) == 'table' and type(envelope['id']) == 'string' then
        return envelope
    end
    return nil
end

local function redis_message_queue_message_id(stored)
    local envelope = redis_message_queue_decode_envelope(stored)
    if envelope then
        return envelope['id']
    end
    return ''
end

local time = redis.call('TIME')
local now_ms = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)

-- Cache replay paths below return the ORIGINAL claim (same lease_token) even if
-- the lease deadline has passed in wall-clock time. Safe because ack is gated by
-- the server-side HGET lease_tokens check in MOVE/REMOVE_WITH_LEASE_TOKEN: if
-- another consumer reclaimed the message, that HGET no longer matches our
-- lease_token and the ack returns 0. The expiry-reclaim loop below can then
-- clean up independently. Validating the deadline here would break legitimate
-- retry-after-network-blip recovery without improving safety.
local cached_claim = redis.call('GET', KEYS[8])
if cached_claim then
    local claim = redis_message_queue_decode_claim(cached_claim)
    if claim then
        redis.call('HSET', KEYS[10], ARGV[4], cached_claim)
        redis.call('HSET', KEYS[11], claim[2], ARGV[4])
        redis.call('HSET', KEYS[9], claim[2], KEYS[8])
        redis.call('ZADD', KEYS[3], now_ms + tonumber(ARGV[1]), claim[1])
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
        redis.call('ZADD', KEYS[3], now_ms + tonumber(ARGV[1]), claim[1])
        return {claim[1], claim[2]}
    end
    redis.call('HDEL', KEYS[10], ARGV[4])
end

-- Cap at 100 to bound Lua execution time (Redis blocks during scripts).
-- With a single consumer polling at default interval, 1000 expired leases drain in ~2.5s.
local expired = redis.call('ZRANGEBYSCORE', KEYS[3], '-inf', now_ms, 'LIMIT', 0, 100)
local to_requeue = {}
local reclaimed_events = {}
for i = #expired, 1, -1 do
    local expired_lease_token = redis.call('HGET', KEYS[4], expired[i])
    redis.call('ZREM', KEYS[3], expired[i])
    redis.call('HDEL', KEYS[4], expired[i])
    if expired_lease_token then
        local claim_result_key = redis.call('HGET', KEYS[9], expired_lease_token)
        if claim_result_key then
            -- Use pcall: in Redis Cluster, claim_result_key was read from KEYS[9] (claim_result_refs)
            -- and is therefore not in the declared EVAL KEYS[] set. Cluster may reject the DEL;
            -- TTL on the claim_result string (PX visibility_timeout_seconds) bounds the orphan.
            redis.pcall('DEL', claim_result_key)
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
        local delivery_count = redis.call('HGET', KEYS[6], expired[i])
        table.insert(reclaimed_events, {redis_message_queue_message_id(expired[i]), tostring(delivery_count or '0')})
    end
end
if #to_requeue > 0 then
    redis.call('RPUSH', KEYS[1], unpack(to_requeue))
end
local dead_lettered_events = {}
local claim_store_failed_sentinel = string.char(0) .. '__rmq_claim_store_failed__'

local function store_claim_and_return(stored)
    -- pcall guards against OOM mid-write: fail fast while preserving a live payload copy.
    local lease_token = nil
    local ok, result = pcall(function()
        redis.call('INCR', KEYS[5])
        lease_token = redis.call('GET', KEYS[5])
        local claim_payload = cjson.encode({stored, lease_token})
        redis.call('ZADD', KEYS[3], now_ms + tonumber(ARGV[1]), stored)
        redis.call('HSET', KEYS[4], stored, lease_token)
        redis.call('SET', KEYS[8], claim_payload, 'PX', tonumber(ARGV[3]))
        redis.call('HSET', KEYS[9], lease_token, KEYS[8])
        redis.call('HSET', KEYS[10], ARGV[4], claim_payload)
        redis.call('HSET', KEYS[11], lease_token, ARGV[4])
        return {stored, lease_token, reclaimed_events, dead_lettered_events}
    end)
    if not ok then
        redis.call('HINCRBY', KEYS[6], stored, -1)
        local return_result = redis.pcall('RPUSH', KEYS[1], stored)
        if type(return_result) == 'table' and return_result['err'] then
            local failure = tostring(result) .. '; return-to-pending failed: ' .. tostring(return_result['err'])
            return {claim_store_failed_sentinel, failure, stored}
        end
        redis.call('LREM', KEYS[2], 1, stored)
        redis.call('ZREM', KEYS[3], stored)
        redis.call('HDEL', KEYS[4], stored)
        redis.call('DEL', KEYS[8])
        redis.call('HDEL', KEYS[10], ARGV[4])
        if lease_token then
            redis.call('HDEL', KEYS[9], lease_token)
            redis.call('HDEL', KEYS[11], lease_token)
        end
        return {claim_store_failed_sentinel, tostring(result), stored}
    end
    return result
end

local claim_attempts = 0
while claim_attempts < 100 do
    claim_attempts = claim_attempts + 1

    local stored = redis.call('LMOVE', KEYS[1], KEYS[2], 'RIGHT', 'LEFT')
    if not stored then
        return {'', '', reclaimed_events, dead_lettered_events}
    end

    local count = redis.call('HINCRBY', KEYS[6], stored, 1)
    if max_delivery_count > 0 and count > max_delivery_count then
        -- Strip envelope to store raw payload in DLQ, consistent with completed/failed queues.
        -- The per-delivery UUID in the envelope is lost; see README dead-letter notes.
        local dead_letter_value = stored
        local envelope = redis_message_queue_decode_envelope(stored)
        if envelope and type(envelope['payload']) == 'string' then
            dead_letter_value = envelope['payload']
        end
        redis.call('LPUSH', KEYS[7], dead_letter_value)
        redis.call('LREM', KEYS[2], 1, stored)
        redis.call('HDEL', KEYS[6], stored)
        table.insert(dead_lettered_events, {redis_message_queue_message_id(stored), tostring(count)})
    else
        return store_claim_and_return(stored)
    end
end

return {'', '', reclaimed_events, dead_lettered_events}
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

-- removed == 0 means the message was externally removed (e.g., via direct
-- LREM/DEL on the processing queue) while we still hold a valid lease token.
-- We intentionally leave lease_deadlines/lease_tokens for the expiry-reclaim
-- loop to clean up. Bounded leak: heartbeat lifetime is owned by
-- process_message and stops on exit, after which the entry's deadline expires
-- naturally and the next claim_message call's expiry loop GCs the orphans.
-- Not reachable from normal library flows (which are single-script atomic).
local removed = redis.call('LREM', KEYS[1], 1, ARGV[1])
if removed == 1 then
    redis.call('ZREM', KEYS[2], ARGV[1])
    redis.call('HDEL', KEYS[3], ARGV[1])
    local claim_result_key = redis.call('HGET', KEYS[5], ARGV[2])
    if claim_result_key then
        -- Use pcall: in Redis Cluster, claim_result_key was read from KEYS[5] (claim_result_refs)
        -- and is therefore not in the declared EVAL KEYS[] set. Cluster may reject the DEL;
        -- TTL on the claim_result string (PX visibility_timeout_seconds) bounds the orphan.
        redis.pcall('DEL', claim_result_key)
        redis.call('HDEL', KEYS[5], ARGV[2])
    end
    local claim_id = redis.call('HGET', KEYS[7], ARGV[2])
    if claim_id then
        redis.call('HDEL', KEYS[6], claim_id)
        redis.call('HDEL', KEYS[7], ARGV[2])
    end
    redis.call('HDEL', KEYS[4], ARGV[1])
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

-- See REMOVE_MESSAGE_WITH_LEASE_TOKEN_LUA_SCRIPT for the bounded-leak rationale
-- on the removed == 0 branch (externally-removed message + valid lease token).
redis.call('LPUSH', KEYS[2], ARGV[2])
local removed = redis.call('LREM', KEYS[1], 1, ARGV[1])
if removed == 1 then
    redis.call('ZREM', KEYS[3], ARGV[1])
    redis.call('HDEL', KEYS[4], ARGV[1])
    local claim_result_key = redis.call('HGET', KEYS[6], ARGV[3])
    if claim_result_key then
        -- Use pcall: in Redis Cluster, claim_result_key was read from KEYS[6] (claim_result_refs)
        -- and is therefore not in the declared EVAL KEYS[] set. Cluster may reject the DEL;
        -- TTL on the claim_result string (PX visibility_timeout_seconds) bounds the orphan.
        redis.pcall('DEL', claim_result_key)
        redis.call('HDEL', KEYS[6], ARGV[3])
    end
    local claim_id = redis.call('HGET', KEYS[8], ARGV[3])
    if claim_id then
        redis.call('HDEL', KEYS[7], claim_id)
        redis.call('HDEL', KEYS[8], ARGV[3])
    end
    redis.call('HDEL', KEYS[5], ARGV[1])
    redis.call('SET', KEYS[9], '1', 'PX', tonumber(ARGV[4]))
else
    redis.call('LREM', KEYS[2], 1, ARGV[2])
end

return removed
"""
)

CLEANUP_DRAINED_LEASE_TOKEN_COUNTER_LUA_SCRIPT = (
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

local err = redis_message_queue_require_type(KEYS[5], 'string')
if err then
    return err
end

if redis.call('LLEN', KEYS[1]) == 0
    and redis.call('ZCARD', KEYS[2]) == 0
    and redis.call('HLEN', KEYS[3]) == 0
    and redis.call('HLEN', KEYS[4]) == 0 then
    redis.call('DEL', KEYS[5])
    return 1
end

return 0
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
