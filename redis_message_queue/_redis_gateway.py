import json
import logging
import queue
import random
import threading
import time
import uuid
from typing import Callable, Optional, TypeVar, cast

import redis
import redis.asyncio
import redis.sentinel

from redis_message_queue._abstract_redis_gateway import AbstractRedisGateway
from redis_message_queue._config import (
    ADD_MESSAGE_LUA_SCRIPT,
    CLAIM_MESSAGE_LUA_SCRIPT,
    CLAIM_MESSAGE_WITH_VISIBILITY_TIMEOUT_LUA_SCRIPT,
    CLAIM_STORE_FAILED_LUA_SENTINEL,
    CLEANUP_DRAINED_LEASE_TOKEN_COUNTER_LUA_SCRIPT,
    DEFAULT_MESSAGE_DEDUPLICATION_LOG_TTL,
    DEFAULT_MESSAGE_WAIT_INTERVAL_SECONDS,
    DEFAULT_PENDING_OVERLOAD_BLOCK_TIMEOUT_SECONDS,
    DEFAULT_RETRY_BUDGET_SECONDS,
    DEFAULT_RETRY_INITIAL_DELAY_SECONDS,
    DEFAULT_RETRY_MAX_DELAY_SECONDS,
    MOVE_MESSAGE_LUA_SCRIPT,
    MOVE_MESSAGE_WITH_LEASE_TOKEN_LUA_SCRIPT,
    PENDING_OVERLOAD_LUA_SENTINEL,
    PUBLISH_MESSAGE_LUA_SCRIPT,
    REMOVE_MESSAGE_LUA_SCRIPT,
    REMOVE_MESSAGE_WITH_LEASE_TOKEN_LUA_SCRIPT,
    RENEW_MESSAGE_LEASE_LUA_SCRIPT,
    RETURN_MESSAGE_TO_PENDING_LUA_SCRIPT,
    _ChainedInterrupt,
    build_retry_strategy,
    is_redis_retryable_exception,
    validate_dead_letter_parameters,
    validate_gateway_parameters,
    validate_pending_backpressure_parameters,
)
from redis_message_queue._event import EventOperation, EventOutcome
from redis_message_queue._exceptions import (
    ClaimStoreFailedError,
    ConfigurationError,
    QueueBackpressureError,
    RedisMessageQueueError,
    RetryBudgetExhaustedError,
    _set_exception_context,
    wrap_lua_response_error,
)
from redis_message_queue._stored_message import (
    ClaimedMessage,
    MessageData,
    decode_stored_message,
    encode_stored_message,
    extract_stored_message_id,
)
from redis_message_queue.interrupt_handler._interface import (
    BaseGracefulInterruptHandler,
)

logger = logging.getLogger(__name__)
_TClaim = TypeVar("_TClaim", bound=ClaimedMessage | MessageData)
_TRedisCall = TypeVar("_TRedisCall")
_MessageAttemptEvent = tuple[str | None, int]

_LEASE_DEADLINES_SUFFIX = ":lease_deadlines"
_LEASE_TOKENS_SUFFIX = ":lease_tokens"
_LEASE_TOKEN_COUNTER_SUFFIX = ":lease_token_counter"
_DELIVERY_COUNTS_SUFFIX = ":delivery_counts"
_CLAIM_RESULT_SUFFIX = ":claim_result"
_CLAIM_RESULT_REFS_SUFFIX = ":claim_result_refs"
_CLAIM_RESULT_IDS_SUFFIX = ":claim_result_ids"
_CLAIM_RESULT_BACKREFS_SUFFIX = ":claim_result_backrefs"
_OPERATION_RESULT_SUFFIX = ":operation_result"
_PUBLISH_OPERATION_RESULT_SUFFIX = ":publish_operation_result"
_OPTIONAL_DEAD_LETTER_PLACEHOLDER_SUFFIX = ":dead_letter_placeholder"
_VISIBILITY_TIMEOUT_POLL_INTERVAL_SECONDS = 0.25
_PENDING_OVERLOAD_INITIAL_BACKOFF_SECONDS = 0.010
_PENDING_OVERLOAD_MAX_BACKOFF_SECONDS = 0.500
_CLAIM_STORE_FAILED_LUA_SENTINEL_BYTES = CLAIM_STORE_FAILED_LUA_SENTINEL.encode("utf-8")
_PENDING_QUEUE_SUFFIX = "pending"
_PROCESSING_QUEUE_SUFFIX = "processing"


class _DrainDeadlineExceeded(Exception):
    """Internal sentinel for drain-only pending-claim recovery deadlines."""


def _raise_if_drain_deadline_expired(deadline_monotonic: float | None) -> None:
    if deadline_monotonic is not None and time.monotonic() >= deadline_monotonic:
        raise _DrainDeadlineExceeded


def _call_with_drain_deadline(
    call: Callable[[], _TRedisCall],
    *,
    deadline_monotonic: float | None,
) -> _TRedisCall:
    if deadline_monotonic is None:
        return call()

    remaining_seconds = deadline_monotonic - time.monotonic()
    if remaining_seconds <= 0:
        raise _DrainDeadlineExceeded

    result_queue: queue.Queue[tuple[bool, object]] = queue.Queue(maxsize=1)

    def run_call() -> None:
        try:
            result = call()
        except Exception as exc:
            result_queue.put_nowait((False, exc))
        else:
            result_queue.put_nowait((True, result))

    thread = threading.Thread(target=run_call, daemon=True)
    thread.start()
    thread.join(timeout=remaining_seconds)
    if thread.is_alive():
        raise _DrainDeadlineExceeded

    succeeded, value = result_queue.get_nowait()
    if succeeded:
        return cast(_TRedisCall, value)
    raise cast(BaseException, value)


def _coerce_lua_count(value: object) -> int:
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _decode_lua_text(value: object) -> str | None:
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    if isinstance(value, str) and value:
        return value
    return None


def _decode_lua_error(value: object) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, str) and value:
        return value
    return "unknown Lua error"


def _is_claim_store_failed_result(result: object) -> bool:
    return (
        isinstance(result, list | tuple)
        and len(result) >= 2
        and result[0] in (CLAIM_STORE_FAILED_LUA_SENTINEL, _CLAIM_STORE_FAILED_LUA_SENTINEL_BYTES)
    )


def _coerce_lua_message_attempts(value: object) -> list[_MessageAttemptEvent]:
    if not isinstance(value, list | tuple):
        return []

    attempts: list[_MessageAttemptEvent] = []
    for item in value:
        if not isinstance(item, list | tuple) or len(item) < 2:
            continue
        attempts.append((_decode_lua_text(item[0]), _coerce_lua_count(item[1])))
    return attempts


def _pending_overload_max_backoff_seconds(block_timeout_seconds: float) -> float:
    return min(_PENDING_OVERLOAD_MAX_BACKOFF_SECONDS, block_timeout_seconds / 10)


def _jitter_pending_overload_backoff_seconds(backoff_seconds: float) -> float:
    return backoff_seconds * (0.8 + 0.4 * random.random())


class RedisGateway(AbstractRedisGateway):
    """Sync Redis gateway with built-in tenacity-based retry on transient errors.

    The retry knobs (``retry_budget_seconds``, ``retry_max_delay_seconds``,
    ``retry_initial_delay_seconds``) configure the internal tenacity strategy.
    Setting ``retry_budget_seconds=0`` disables retry entirely (single attempt;
    exceptions propagate). The library uses ``retry_budget_seconds`` to size the
    operation-result cache TTL so that a successfully-acked operation cannot
    appear "not removed" to a retry that arrives after the budget elapses.

    Power-user escape hatch: to plug in a different retry library
    (``backoff``, ``asyncstdlib.retry``, custom exponential backoff, etc.) or
    fundamentally different retry semantics, subclass
    :class:`AbstractRedisGateway` and override the operation methods directly.
    """

    def __init__(
        self,
        *,
        redis_client: redis.Redis,
        retry_budget_seconds: int = DEFAULT_RETRY_BUDGET_SECONDS,
        retry_max_delay_seconds: float = DEFAULT_RETRY_MAX_DELAY_SECONDS,
        retry_initial_delay_seconds: float = DEFAULT_RETRY_INITIAL_DELAY_SECONDS,
        message_deduplication_log_ttl_seconds: Optional[int] = None,
        message_wait_interval_seconds: Optional[int] = None,
        message_visibility_timeout_seconds: Optional[int] = None,
        max_delivery_count: int | None = None,
        dead_letter_queue: str | None = None,
        max_pending_length: int | None = None,
        pending_overload_policy: str = "raise",
        pending_overload_block_timeout_seconds: float = DEFAULT_PENDING_OVERLOAD_BLOCK_TIMEOUT_SECONDS,
        interrupt: BaseGracefulInterruptHandler | None = None,
    ):
        if isinstance(redis_client, redis.asyncio.Redis):
            raise TypeError(
                "'redis_client' is an async Redis client (redis.asyncio.Redis); "
                "use the async RedisMessageQueue from redis_message_queue.asyncio instead"
            )
        if isinstance(redis_client, redis.sentinel.Sentinel):
            raise TypeError(
                "'redis_client' is a redis.sentinel.Sentinel manager object, not a Redis client. "
                "Pass sentinel.master_for(name) (or async equivalent) instead."
            )
        if isinstance(redis_client, (redis.client.Pipeline, redis.asyncio.client.Pipeline)):
            raise TypeError(
                "'redis_client' is a Pipeline, not a Redis client; "
                "Pipeline defers execution and would silently drop writes. "
                "Pass the underlying redis.Redis instance instead."
            )
        if isinstance(redis_client, redis.Redis) and isinstance(
            redis_client.connection, redis.connection.AbstractConnection
        ):
            raise TypeError(
                "'redis_client' is a redis.Redis single-connection client. "
                "redis.Redis(single_connection_client=True) pins one socket and is unsafe "
                "to inherit across forked processes. Pass a normal pooled redis.Redis "
                "client instead."
            )
        self._redis_client = redis_client
        if interrupt is not None and not isinstance(interrupt, BaseGracefulInterruptHandler):
            raise TypeError(
                "'interrupt' must be a BaseGracefulInterruptHandler instance"
                " (e.g., redis_message_queue.interrupt_handler.GracefulInterruptHandler()),"
                f" got {type(interrupt).__name__}"
            )
        self._interrupt = interrupt
        self._message_deduplication_log_ttl_seconds = (
            DEFAULT_MESSAGE_DEDUPLICATION_LOG_TTL
            if message_deduplication_log_ttl_seconds is None
            else message_deduplication_log_ttl_seconds
        )
        self._message_wait_interval_seconds = (
            DEFAULT_MESSAGE_WAIT_INTERVAL_SECONDS
            if message_wait_interval_seconds is None
            else message_wait_interval_seconds
        )
        self._message_visibility_timeout_seconds = message_visibility_timeout_seconds
        validate_gateway_parameters(
            self._message_deduplication_log_ttl_seconds,
            self._message_wait_interval_seconds,
            self._message_visibility_timeout_seconds,
            retry_budget_seconds=retry_budget_seconds,
            retry_max_delay_seconds=retry_max_delay_seconds,
            retry_initial_delay_seconds=retry_initial_delay_seconds,
        )
        validate_dead_letter_parameters(
            max_delivery_count,
            dead_letter_queue,
            self._message_visibility_timeout_seconds,
        )
        validate_pending_backpressure_parameters(
            max_pending_length,
            pending_overload_policy,
            pending_overload_block_timeout_seconds,
            max_delivery_count=max_delivery_count,
        )
        self._retry_budget_seconds = retry_budget_seconds
        self._retry_max_delay_seconds = retry_max_delay_seconds
        self._retry_initial_delay_seconds = retry_initial_delay_seconds
        self._retry_strategy = build_retry_strategy(
            retry_budget_seconds=retry_budget_seconds,
            retry_max_delay_seconds=retry_max_delay_seconds,
            retry_initial_delay_seconds=retry_initial_delay_seconds,
            interrupt=interrupt,
        )
        self._max_delivery_count = max_delivery_count
        self._dead_letter_queue = dead_letter_queue
        self._max_pending_length = max_pending_length
        self._pending_overload_policy = pending_overload_policy
        self._pending_overload_block_timeout_seconds = pending_overload_block_timeout_seconds
        self._pending_claim_ids: dict[str, list[str]] = {}
        self._in_flight_claim_ids: dict[str, set[str]] = {}
        self._recovering_claim_ids: dict[str, set[str]] = {}
        self._pending_claim_ids_lock = threading.Lock()
        self._drain_pending_claim_ids_lock = threading.Lock()
        self._last_drain_error: BaseException | None = None
        self._event_queue_name: str | None = None
        self._event_emitter: Callable[..., None] | None = None

    def _set_event_emitter(self, queue_name: str, emitter: Callable[..., None] | None) -> None:
        self._event_queue_name = queue_name
        self._event_emitter = emitter

    def _emit_event(
        self,
        operation: EventOperation | str,
        outcome: EventOutcome | str,
        *,
        message_id: str | None = None,
        claim_id: str | None = None,
        destination_queue: str | None = None,
        delivery_count: int | None = None,
        max_delivery_count: int | None = None,
        exception_type: str | None = None,
        error: BaseException | None = None,
    ) -> None:
        if self._event_emitter is None:
            return
        self._event_emitter(
            operation,
            outcome,
            message_id=message_id,
            claim_id=claim_id,
            destination_queue=destination_queue,
            delivery_count=delivery_count,
            max_delivery_count=max_delivery_count,
            exception_type=exception_type,
            error=error,
        )

    def _emit_repeated_event(
        self,
        operation: EventOperation | str,
        attempts: list[_MessageAttemptEvent],
        *,
        destination_queue: str | None = None,
        max_delivery_count: int | None = None,
    ) -> None:
        for message_id, delivery_count in attempts:
            self._emit_event(
                operation,
                "success",
                message_id=message_id,
                destination_queue=destination_queue,
                delivery_count=delivery_count,
                max_delivery_count=max_delivery_count,
            )

    def _eval(self, *args: object) -> object:
        try:
            return self._redis_client.eval(*args)
        except redis.exceptions.ResponseError as exc:
            lua_error = wrap_lua_response_error(exc)
            if lua_error is not None:
                raise lua_error from exc
            raise

    def _lua_max_pending_length(self) -> str:
        return "" if self._max_pending_length is None else str(self._max_pending_length)

    def _run_pending_backpressure_operation(self, queue: str, operation: Callable[[], object]) -> object:
        deadline = time.monotonic() + self._pending_overload_block_timeout_seconds
        backoff_seconds = _PENDING_OVERLOAD_INITIAL_BACKOFF_SECONDS
        max_backoff_seconds = _pending_overload_max_backoff_seconds(self._pending_overload_block_timeout_seconds)
        while True:
            result = operation()
            if _coerce_lua_count(result) != PENDING_OVERLOAD_LUA_SENTINEL:
                return result
            if self._pending_overload_policy != "block":
                raise QueueBackpressureError(
                    f"Pending queue {queue!r} reached max_pending_length={self._max_pending_length}",
                    queue=queue,
                    operation="publish",
                )
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise QueueBackpressureError(
                    f"Pending queue {queue!r} stayed at max_pending_length={self._max_pending_length} "
                    f"for {self._pending_overload_block_timeout_seconds} seconds",
                    queue=queue,
                    operation="publish",
                )
            sleep_seconds = min(_jitter_pending_overload_backoff_seconds(backoff_seconds), remaining)
            time.sleep(sleep_seconds)
            backoff_seconds = min(backoff_seconds * 2, max_backoff_seconds)

    @property
    def message_visibility_timeout_seconds(self) -> int | None:
        return self._message_visibility_timeout_seconds

    @property
    def max_delivery_count(self) -> int | None:
        return self._max_delivery_count

    @property
    def dead_letter_queue(self) -> str | None:
        return self._dead_letter_queue

    @property
    def is_redis_cluster(self) -> bool:
        return isinstance(self._redis_client, redis.RedisCluster)

    def _raise_if_drop_oldest_deduplicated_publish(self) -> None:
        if self._pending_overload_policy == "drop_oldest":
            raise ConfigurationError(
                "'pending_overload_policy=drop_oldest' cannot be used with RedisGateway.publish_message "
                "because dropped messages leave their deduplication keys in Redis, causing future publishes "
                "of the same payload to be silently suppressed. Use add_message for non-deduplicated lossy "
                "queues, or use 'raise' or 'block' for deduplicated publishes."
            )

    def publish_message(self, queue: str, message: str, dedup_key: str) -> bool:
        if not isinstance(dedup_key, str):
            raise TypeError(f"'dedup_key' must be a str, got {type(dedup_key).__name__}")
        if dedup_key == "":
            raise ConfigurationError(
                "'dedup_key' must be a non-empty string; "
                "an empty key would create a bare-prefix Redis marker that silently suppresses unrelated messages"
            )
        self._raise_if_drop_oldest_deduplicated_publish()
        stored_message = encode_stored_message(message)
        message_id = extract_stored_message_id(stored_message)
        operation_id = uuid.uuid4().hex
        operation_result_key = self._publish_operation_result_key(dedup_key, operation_id)

        @self._retry_strategy
        def _publish():
            result = self._run_pending_backpressure_operation(
                queue,
                lambda: self._eval(
                    PUBLISH_MESSAGE_LUA_SCRIPT,
                    3,
                    dedup_key,
                    queue,
                    operation_result_key,
                    str(self._message_deduplication_log_ttl_seconds),
                    stored_message,
                    self._publish_operation_result_ttl_ms(),
                    self._lua_max_pending_length(),
                    self._pending_overload_policy,
                ),
            )
            return bool(_coerce_lua_count(result))

        try:
            return _publish()
        except RedisMessageQueueError as exc:
            _set_exception_context(exc, queue=queue, message_id=message_id, operation="publish")
            raise
        finally:
            self._delete_operation_result_key(operation_result_key)

    def add_message(self, queue: str, message: str) -> None:
        """Non-deduplicated enqueue. Must not be retried to keep at-most-once.

        This library deliberately does not wrap the enqueue in a retry — retrying
        after the server may already have executed the command can silently
        duplicate the message. The caller can still retry (accepting duplicates).

        Note on retries: redis-py 6.0+ changed the default standalone
        ``Redis()`` / ``redis.asyncio.Redis()`` retry policy from ``None`` (no
        retry) to a multi-attempt ``ExponentialWithJitterBackoff``. The default
        attempt count varies by redis-py version, for example about 3 on
        redis-py 6-7 and about 10 on redis-py 8+. If you need strict
        at-most-once for non-deduplicated publishes, pass ``retry=None``
        explicitly when constructing the redis-py client. This library does
        not configure the redis-py client retry; it only controls its own
        retry budget on top of the client.
        """
        stored_message = encode_stored_message(message)
        message_id = extract_stored_message_id(stored_message)
        if self._max_pending_length is not None:
            try:
                self._run_pending_backpressure_operation(
                    queue,
                    lambda: self._eval(
                        ADD_MESSAGE_LUA_SCRIPT,
                        1,
                        queue,
                        stored_message,
                        self._lua_max_pending_length(),
                        self._pending_overload_policy,
                    ),
                )
            except RedisMessageQueueError as exc:
                _set_exception_context(exc, queue=queue, message_id=message_id, operation="publish")
                raise
            return
        try:
            self._redis_client.lpush(queue, stored_message)
        except RedisMessageQueueError as exc:
            _set_exception_context(exc, queue=queue, message_id=message_id, operation="publish")
            raise

    def move_message(
        self,
        from_queue: str,
        to_queue: str,
        message: MessageData,
        *,
        lease_token: str | None = None,
    ) -> bool:
        decoded_message = decode_stored_message(message)

        if lease_token is None:
            operation_id = uuid.uuid4().hex
            operation_result_key = self._operation_result_key(from_queue, operation_id)

            @self._retry_strategy
            def _move():
                return bool(
                    self._eval(
                        MOVE_MESSAGE_LUA_SCRIPT,
                        5,
                        from_queue,
                        to_queue,
                        self._claim_result_ids_key(from_queue),
                        self._claim_result_backrefs_key(from_queue),
                        operation_result_key,
                        message,
                        decoded_message,
                        self._operation_result_ttl_ms(),
                    )
                )

            try:
                return _move()
            except RedisMessageQueueError as exc:
                _set_exception_context(
                    exc,
                    queue=from_queue,
                    message_id=extract_stored_message_id(message),
                    operation="ack",
                )
                raise
            finally:
                self._delete_operation_result_key(operation_result_key)

        operation_id = uuid.uuid4().hex
        operation_result_key = self._lease_operation_result_key(from_queue, lease_token, operation_id)

        @self._retry_strategy
        def _move_with_lease():
            return bool(
                self._eval(
                    MOVE_MESSAGE_WITH_LEASE_TOKEN_LUA_SCRIPT,
                    9,
                    from_queue,
                    to_queue,
                    self._lease_deadlines_key(from_queue),
                    self._lease_tokens_key(from_queue),
                    self._delivery_counts_key(from_queue),
                    self._claim_result_refs_key(from_queue),
                    self._claim_result_ids_key(from_queue),
                    self._claim_result_backrefs_key(from_queue),
                    operation_result_key,
                    message,
                    decoded_message,
                    lease_token,
                    self._lease_operation_result_ttl_ms(),
                )
            )

        try:
            return _move_with_lease()
        except RedisMessageQueueError as exc:
            _set_exception_context(
                exc,
                queue=from_queue,
                message_id=extract_stored_message_id(message),
                operation="ack",
            )
            raise
        finally:
            self._delete_operation_result_key(operation_result_key)

    def remove_message(self, queue: str, message: MessageData, *, lease_token: str | None = None) -> bool:
        if lease_token is None:
            operation_id = uuid.uuid4().hex
            operation_result_key = self._operation_result_key(queue, operation_id)

            @self._retry_strategy
            def _remove():
                return bool(
                    self._eval(
                        REMOVE_MESSAGE_LUA_SCRIPT,
                        4,
                        queue,
                        self._claim_result_ids_key(queue),
                        self._claim_result_backrefs_key(queue),
                        operation_result_key,
                        message,
                        self._operation_result_ttl_ms(),
                    )
                )

            try:
                return _remove()
            except RedisMessageQueueError as exc:
                _set_exception_context(
                    exc,
                    queue=queue,
                    message_id=extract_stored_message_id(message),
                    operation="ack",
                )
                raise
            finally:
                self._delete_operation_result_key(operation_result_key)

        operation_id = uuid.uuid4().hex
        operation_result_key = self._lease_operation_result_key(queue, lease_token, operation_id)

        @self._retry_strategy
        def _remove_with_lease():
            return bool(
                self._eval(
                    REMOVE_MESSAGE_WITH_LEASE_TOKEN_LUA_SCRIPT,
                    8,
                    queue,
                    self._lease_deadlines_key(queue),
                    self._lease_tokens_key(queue),
                    self._delivery_counts_key(queue),
                    self._claim_result_refs_key(queue),
                    self._claim_result_ids_key(queue),
                    self._claim_result_backrefs_key(queue),
                    operation_result_key,
                    message,
                    lease_token,
                    self._lease_operation_result_ttl_ms(),
                )
            )

        try:
            return _remove_with_lease()
        except RedisMessageQueueError as exc:
            _set_exception_context(
                exc,
                queue=queue,
                message_id=extract_stored_message_id(message),
                operation="ack",
            )
            raise
        finally:
            self._delete_operation_result_key(operation_result_key)

    def renew_message_lease(
        self,
        queue: str,
        message: MessageData,
        lease_token: str,
        *,
        is_interrupted: BaseGracefulInterruptHandler | None = None,
    ) -> bool:
        if self._message_visibility_timeout_seconds is None:
            return False

        if is_interrupted is None:
            retry_strategy = self._retry_strategy
        else:
            # Per-call strategy: compose the gateway-level interrupt with the
            # caller's stop signal so a heartbeat stop short-circuits the
            # retry loop without waiting out retry_budget_seconds (AA-01-F2).
            retry_strategy = build_retry_strategy(
                retry_budget_seconds=self._retry_budget_seconds,
                retry_max_delay_seconds=self._retry_max_delay_seconds,
                retry_initial_delay_seconds=self._retry_initial_delay_seconds,
                interrupt=_ChainedInterrupt(self._interrupt, is_interrupted),
            )

        @retry_strategy
        def _renew():
            return bool(
                self._eval(
                    RENEW_MESSAGE_LEASE_LUA_SCRIPT,
                    2,
                    self._lease_deadlines_key(queue),
                    self._lease_tokens_key(queue),
                    message,
                    lease_token,
                    str(self._message_visibility_timeout_seconds * 1000),
                )
            )

        return _renew()

    def _wait_for_claim(
        self,
        from_queue: str,
        to_queue: str,
        *,
        recover_pending_claim: Callable[[str, str], _TClaim | None],
        claim_message: Callable[[str, str, str], _TClaim | None],
        non_blocking_retry_log: str,
        polling_retry_log: str,
        is_interrupted: BaseGracefulInterruptHandler | None = None,
    ) -> _TClaim | None:
        while True:
            pending_claim_id = self._acquire_pending_claim_id(to_queue)
            if pending_claim_id is None:
                break
            # clear=True on a None recovery is safe ONLY because pending_claim_id
            # is registered (in the outer finally below) strictly AFTER the
            # original eval returned or raised. Redis EVAL is atomic, so by the
            # time we observe the claim_id here, the original Lua has either
            # committed or never ran — there is no "still in flight" window. If
            # a future refactor registers the claim_id BEFORE the eval call, this
            # invariant breaks and a concurrent recovery could clear a pending
            # claim that hasn't actually committed yet.
            clear_pending_claim_id = False
            try:
                recovered_claim = recover_pending_claim(to_queue, pending_claim_id)
                clear_pending_claim_id = True
            finally:
                self._finish_pending_claim_recovery(
                    to_queue,
                    pending_claim_id,
                    clear=clear_pending_claim_id,
                )
            if recovered_claim is not None:
                if self._message_visibility_timeout_seconds is None:
                    self._delete_claim_result_key(self._claim_result_key(to_queue, pending_claim_id))
                self._emit_event("claim_reclaim", "success", claim_id=pending_claim_id)
                return recovered_claim

        if self._is_interrupted(is_interrupted):
            return None

        pending_claim_id_to_share: str | None = None
        active_claim_id: str | None = None

        def begin_active_claim(claim_id: str) -> None:
            nonlocal active_claim_id
            if active_claim_id == claim_id:
                return
            if active_claim_id is not None:
                self._finish_in_flight_claim_id(to_queue, active_claim_id)
            self._begin_in_flight_claim_id(to_queue, claim_id)
            active_claim_id = claim_id

        def finish_active_claim() -> None:
            nonlocal active_claim_id
            if active_claim_id is None:
                return
            self._finish_in_flight_claim_id(to_queue, active_claim_id)
            active_claim_id = None

        try:
            if self._message_wait_interval_seconds == 0:
                claim_id = uuid.uuid4().hex
                claim_may_need_recovery = False
                begin_active_claim(claim_id)
                if self._is_interrupted(is_interrupted):
                    return None
                try:
                    claimed_message = claim_message(from_queue, to_queue, claim_id)
                except Exception as exc:
                    if not is_redis_retryable_exception(exc):
                        pending_claim_id_to_share = claim_id
                        raise
                    claim_may_need_recovery = True
                    self._emit_event(
                        "retry_attempt",
                        "failure",
                        claim_id=claim_id,
                        exception_type=type(exc).__name__,
                        error=exc,
                    )
                    logger.warning(non_blocking_retry_log, type(exc).__name__)
                    if self._is_interrupted(is_interrupted):
                        pending_claim_id_to_share = claim_id
                        return None
                    try:
                        claimed_message = claim_message(from_queue, to_queue, claim_id)
                    except Exception as retry_exc:
                        if claim_may_need_recovery:
                            pending_claim_id_to_share = claim_id
                        self._emit_event(
                            "retry_exhausted",
                            "failure",
                            claim_id=claim_id,
                            exception_type=type(retry_exc).__name__,
                            error=retry_exc,
                        )
                        raise RetryBudgetExhaustedError(
                            "Redis retry budget exhausted during message claim",
                            queue=from_queue,
                            operation="claim",
                        ) from retry_exc
                    except BaseException:
                        pending_claim_id_to_share = claim_id
                        raise
                    return claimed_message
                except BaseException:
                    pending_claim_id_to_share = claim_id
                    raise
                return claimed_message

            deadline = time.monotonic() + self._message_wait_interval_seconds
            claim_id = uuid.uuid4().hex
            claim_may_need_recovery = False
            last_retryable_exception: Exception | None = None
            while True:
                if active_claim_id is None:
                    begin_active_claim(claim_id)
                if self._is_interrupted(is_interrupted):
                    if claim_may_need_recovery:
                        pending_claim_id_to_share = claim_id
                    return None
                try:
                    claimed_message = claim_message(from_queue, to_queue, claim_id)
                except Exception as exc:
                    if not is_redis_retryable_exception(exc):
                        pending_claim_id_to_share = claim_id
                        raise
                    claim_may_need_recovery = True
                    self._emit_event(
                        "retry_attempt",
                        "failure",
                        claim_id=claim_id,
                        exception_type=type(exc).__name__,
                        error=exc,
                    )
                    logger.warning(polling_retry_log, type(exc).__name__)
                    last_retryable_exception = exc
                except BaseException:
                    pending_claim_id_to_share = claim_id
                    raise
                else:
                    if claimed_message is not None:
                        return claimed_message
                    claim_may_need_recovery = False
                    last_retryable_exception = None
                    finish_active_claim()
                    claim_id = uuid.uuid4().hex

                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    if last_retryable_exception is not None:
                        if self._is_interrupted(is_interrupted):
                            if claim_may_need_recovery:
                                pending_claim_id_to_share = claim_id
                            return None
                        try:
                            recovered_claim = recover_pending_claim(to_queue, claim_id)
                        except Exception:
                            if claim_may_need_recovery:
                                pending_claim_id_to_share = claim_id
                            raise
                        except BaseException:
                            pending_claim_id_to_share = claim_id
                            raise
                        if recovered_claim is not None:
                            if self._message_visibility_timeout_seconds is None:
                                self._delete_claim_result_key(self._claim_result_key(to_queue, claim_id))
                            self._emit_event("claim_reclaim", "success", claim_id=claim_id)
                            return recovered_claim
                        self._emit_event(
                            "retry_exhausted",
                            "failure",
                            claim_id=claim_id,
                            exception_type=type(last_retryable_exception).__name__,
                            error=last_retryable_exception,
                        )
                        raise RetryBudgetExhaustedError(
                            "Redis retry budget exhausted during message claim",
                            queue=from_queue,
                            operation="claim",
                        ) from last_retryable_exception
                    return None
                time.sleep(min(_VISIBILITY_TIMEOUT_POLL_INTERVAL_SECONDS, remaining))
        finally:
            if pending_claim_id_to_share is not None:
                self._set_pending_claim_id(to_queue, pending_claim_id_to_share)
            finish_active_claim()

    def wait_for_message_and_move(self, from_queue: str, to_queue: str) -> ClaimedMessage | MessageData | None:
        if self._is_interrupted():
            return None
        return self._wait_for_message_and_move_interruptible(from_queue, to_queue)

    def _wait_for_message_and_move_interruptible(
        self,
        from_queue: str,
        to_queue: str,
        *,
        is_interrupted: BaseGracefulInterruptHandler | None = None,
    ) -> ClaimedMessage | MessageData | None:
        if self._is_interrupted(is_interrupted):
            return None
        if self._message_visibility_timeout_seconds is not None:
            return self._wait_for_message_with_visibility_timeout(
                from_queue,
                to_queue,
                is_interrupted=is_interrupted,
            )
        return self._wait_for_message_without_visibility_timeout(
            from_queue,
            to_queue,
            is_interrupted=is_interrupted,
        )

    def _wait_for_message_without_visibility_timeout(
        self,
        from_queue: str,
        to_queue: str,
        *,
        is_interrupted: BaseGracefulInterruptHandler | None = None,
    ) -> MessageData | None:
        return self._wait_for_claim(
            from_queue,
            to_queue,
            recover_pending_claim=self._recover_pending_non_visibility_timeout_claim,
            claim_message=lambda source, destination, claim_id: self._claim_message_without_visibility_timeout(
                source,
                destination,
                claim_id=claim_id,
            ),
            non_blocking_retry_log=(
                "Transient error during non-visibility-timeout non-blocking claim, retrying once to recover claim: %s"
            ),
            polling_retry_log="Transient error during non-visibility-timeout claim poll, will retry: %s",
            is_interrupted=is_interrupted,
        )

    def _wait_for_message_with_visibility_timeout(
        self,
        from_queue: str,
        to_queue: str,
        *,
        is_interrupted: BaseGracefulInterruptHandler | None = None,
    ) -> ClaimedMessage | None:
        return self._wait_for_claim(
            from_queue,
            to_queue,
            recover_pending_claim=self._recover_pending_visibility_timeout_claim,
            claim_message=lambda source, destination, claim_id: self._claim_visible_message(
                source,
                destination,
                claim_id=claim_id,
            ),
            non_blocking_retry_log=(
                "Transient error during visibility-timeout non-blocking claim, retrying once to recover claim: %s"
            ),
            polling_retry_log="Transient error during visibility-timeout claim poll, will retry: %s",
            is_interrupted=is_interrupted,
        )

    def _claim_message_without_visibility_timeout(
        self,
        from_queue: str,
        to_queue: str,
        *,
        claim_id: str,
    ) -> MessageData | None:
        claim_result_key = self._claim_result_key(to_queue, claim_id)
        result = self._eval(
            CLAIM_MESSAGE_LUA_SCRIPT,
            5,
            from_queue,
            to_queue,
            claim_result_key,
            self._claim_result_ids_key(to_queue),
            self._claim_result_backrefs_key(to_queue),
            self._claim_result_ttl_ms(),
            claim_id,
        )
        if result is None:
            return None

        self._delete_claim_result_key(claim_result_key)
        return result

    def _claim_visible_message(self, from_queue: str, to_queue: str, *, claim_id: str) -> ClaimedMessage | None:
        result = self._eval(
            CLAIM_MESSAGE_WITH_VISIBILITY_TIMEOUT_LUA_SCRIPT,
            11,
            from_queue,
            to_queue,
            self._lease_deadlines_key(to_queue),
            self._lease_tokens_key(to_queue),
            self._lease_token_counter_key(to_queue),
            self._delivery_counts_key(to_queue),
            self._optional_dead_letter_key(to_queue),
            self._claim_result_key(to_queue, claim_id),
            self._claim_result_refs_key(to_queue),
            self._claim_result_ids_key(to_queue),
            self._claim_result_backrefs_key(to_queue),
            str(self._message_visibility_timeout_seconds * 1000),
            str(self._max_delivery_count or 0),
            str(self._message_visibility_timeout_seconds * 1000),
            claim_id,
        )
        if result is None:
            return None

        if _is_claim_store_failed_result(result):
            stored_message = result[2] if len(result) > 2 else None
            message_id = extract_stored_message_id(stored_message) if isinstance(stored_message, (str, bytes)) else None
            raise ClaimStoreFailedError(
                f"VT claim store failed after delivery_count rollback and payload preservation: "
                f"{_decode_lua_error(result[1])}",
                queue=from_queue,
                message_id=message_id,
                operation="claim",
            )

        stored_message, lease_token = result[0], result[1]
        reclaimed_attempts = _coerce_lua_message_attempts(result[2]) if len(result) > 2 else []
        dead_lettered_attempts = _coerce_lua_message_attempts(result[3]) if len(result) > 3 else []
        self._emit_repeated_event("claim_reclaim", reclaimed_attempts)
        self._emit_repeated_event(
            "dlq",
            dead_lettered_attempts,
            destination_queue=self._dead_letter_queue,
            max_delivery_count=self._max_delivery_count,
        )
        if stored_message in ("", b"") and lease_token in ("", b""):
            return None
        if isinstance(lease_token, bytes):
            lease_token = lease_token.decode("utf-8")
        return ClaimedMessage(stored_message=stored_message, lease_token=lease_token)

    def trim_queue(self, queue: str, max_length: int) -> None:
        self._redis_client.ltrim(queue, 0, max_length - 1)

    def _lease_deadlines_key(self, processing_queue: str) -> str:
        return f"{processing_queue}{_LEASE_DEADLINES_SUFFIX}"

    def _lease_tokens_key(self, processing_queue: str) -> str:
        return f"{processing_queue}{_LEASE_TOKENS_SUFFIX}"

    def _lease_token_counter_key(self, processing_queue: str) -> str:
        return f"{processing_queue}{_LEASE_TOKEN_COUNTER_SUFFIX}"

    def _delivery_counts_key(self, processing_queue: str) -> str:
        return f"{processing_queue}{_DELIVERY_COUNTS_SUFFIX}"

    def _claim_result_key(self, processing_queue: str, claim_id: str) -> str:
        return f"{processing_queue}{_CLAIM_RESULT_SUFFIX}:{claim_id}"

    def _claim_result_refs_key(self, processing_queue: str) -> str:
        return f"{processing_queue}{_CLAIM_RESULT_REFS_SUFFIX}"

    def _claim_result_ids_key(self, processing_queue: str) -> str:
        return f"{processing_queue}{_CLAIM_RESULT_IDS_SUFFIX}"

    def _claim_result_backrefs_key(self, processing_queue: str) -> str:
        return f"{processing_queue}{_CLAIM_RESULT_BACKREFS_SUFFIX}"

    def _optional_dead_letter_key(self, processing_queue: str) -> str:
        if self._dead_letter_queue is not None:
            return self._dead_letter_queue
        return f"{processing_queue}{_OPTIONAL_DEAD_LETTER_PLACEHOLDER_SUFFIX}"

    def _publish_operation_result_key(self, dedup_key: str, operation_id: str) -> str:
        return f"{dedup_key}{_PUBLISH_OPERATION_RESULT_SUFFIX}:{operation_id}"

    def _operation_result_key(self, queue: str, operation_id: str) -> str:
        return f"{queue}{_OPERATION_RESULT_SUFFIX}:{operation_id}"

    def _lease_operation_result_key(self, processing_queue: str, lease_token: str, operation_id: str) -> str:
        return f"{processing_queue}{_OPERATION_RESULT_SUFFIX}:{lease_token}:{operation_id}"

    def _publish_operation_result_ttl_ms(self) -> str:
        return str(max(self._message_deduplication_log_ttl_seconds, 3600, self._retry_budget_seconds + 180) * 1000)

    def _operation_result_ttl_ms(self) -> str:
        # Floor is derived from the configured retry budget so the cached
        # operation result outlives the retry window with a 180s margin. Equal
        # deadlines produce a boundary race where a retry arriving past the
        # budget finds the cache just expired and re-runs the Lua, which then
        # observes LREM=0 for an already-acked message and returns False.
        #
        # Sized internally from ``retry_budget_seconds`` (which the library now
        # owns), so the relationship is a structural invariant rather than a
        # caller-supplied constraint.
        vt_seconds = self._message_visibility_timeout_seconds or 0
        return str(max(vt_seconds, self._retry_budget_seconds + 180) * 1000)

    def _lease_operation_result_ttl_ms(self) -> str:
        return self._operation_result_ttl_ms()

    def _claim_result_ttl_ms(self) -> str:
        return str(max(self._message_wait_interval_seconds, 120) * 1000)

    def _cleanup_drained_lease_token_counter(self, processing_queue: str) -> bool:
        if self._message_visibility_timeout_seconds is None:
            return False
        return bool(
            _coerce_lua_count(
                self._eval(
                    CLEANUP_DRAINED_LEASE_TOKEN_COUNTER_LUA_SCRIPT,
                    5,
                    processing_queue,
                    self._lease_deadlines_key(processing_queue),
                    self._lease_tokens_key(processing_queue),
                    self._delivery_counts_key(processing_queue),
                    self._lease_token_counter_key(processing_queue),
                )
            )
        )

    def _delete_claim_result_key(
        self,
        claim_result_key: str,
        *,
        deadline_monotonic: float | None = None,
    ) -> None:
        try:
            _call_with_drain_deadline(
                lambda: self._redis_client.delete(claim_result_key),
                deadline_monotonic=deadline_monotonic,
            )
        except _DrainDeadlineExceeded:
            raise
        except Exception:
            # Claim-result keys have bounded TTLs; this cleanup is intentionally best-effort.
            logger.warning("Failed to delete claim result key %s", claim_result_key, exc_info=True)

    def _delete_claim_result_ref(
        self,
        claim_result_refs_key: str,
        lease_token: str,
        *,
        deadline_monotonic: float | None = None,
    ) -> None:
        try:
            _call_with_drain_deadline(
                lambda: self._redis_client.hdel(claim_result_refs_key, lease_token),
                deadline_monotonic=deadline_monotonic,
            )
        except _DrainDeadlineExceeded:
            raise
        except Exception:
            # Claim-result refs have bounded TTLs; this cleanup is intentionally best-effort.
            logger.warning(
                "Failed to delete claim result reference %s[%s]",
                claim_result_refs_key,
                lease_token,
                exc_info=True,
            )

    def _delete_operation_result_key(self, operation_result_key: str) -> None:
        try:
            self._redis_client.delete(operation_result_key)
        except Exception:
            logger.debug("Failed to delete operation result key %s", operation_result_key, exc_info=True)

    def _acquire_pending_claim_id(self, processing_queue: str) -> str | None:
        with self._pending_claim_ids_lock:
            pending_claim_ids = self._pending_claim_ids.get(processing_queue)
            if not pending_claim_ids:
                return None
            recovering_claim_ids = self._recovering_claim_ids.setdefault(processing_queue, set())
            for claim_id in pending_claim_ids:
                if claim_id not in recovering_claim_ids:
                    recovering_claim_ids.add(claim_id)
                    return claim_id
            return None

    def _set_pending_claim_id(self, processing_queue: str, claim_id: str) -> None:
        with self._pending_claim_ids_lock:
            pending_claim_ids = self._pending_claim_ids.setdefault(processing_queue, [])
            if claim_id not in pending_claim_ids:
                pending_claim_ids.append(claim_id)

    def _begin_in_flight_claim_id(self, processing_queue: str, claim_id: str) -> None:
        with self._pending_claim_ids_lock:
            self._in_flight_claim_ids.setdefault(processing_queue, set()).add(claim_id)

    def _finish_in_flight_claim_id(self, processing_queue: str, claim_id: str) -> None:
        with self._pending_claim_ids_lock:
            in_flight_claim_ids = self._in_flight_claim_ids.get(processing_queue)
            if in_flight_claim_ids is None:
                return
            in_flight_claim_ids.discard(claim_id)
            if not in_flight_claim_ids:
                self._in_flight_claim_ids.pop(processing_queue, None)

    def _finish_pending_claim_recovery(
        self,
        processing_queue: str,
        claim_id: str,
        *,
        clear: bool,
    ) -> None:
        with self._pending_claim_ids_lock:
            recovering_claim_ids = self._recovering_claim_ids.get(processing_queue)
            if recovering_claim_ids is not None:
                recovering_claim_ids.discard(claim_id)
                if not recovering_claim_ids:
                    self._recovering_claim_ids.pop(processing_queue, None)
            if not clear:
                return

            pending_claim_ids = self._pending_claim_ids.get(processing_queue)
            if pending_claim_ids is None:
                return
            try:
                pending_claim_ids.remove(claim_id)
            except ValueError:
                return
            if not pending_claim_ids:
                self._pending_claim_ids.pop(processing_queue, None)

    def _recover_pending_non_visibility_timeout_claim(
        self,
        processing_queue: str,
        claim_id: str,
        *,
        deadline_monotonic: float | None = None,
    ) -> MessageData | None:
        _raise_if_drain_deadline_expired(deadline_monotonic)
        claim_result_key = self._claim_result_key(processing_queue, claim_id)
        cached_claim = _call_with_drain_deadline(
            lambda: self._redis_client.get(claim_result_key),
            deadline_monotonic=deadline_monotonic,
        )
        _raise_if_drain_deadline_expired(deadline_monotonic)
        if cached_claim is None:
            cached_claim = _call_with_drain_deadline(
                lambda: self._redis_client.hget(self._claim_result_ids_key(processing_queue), claim_id),
                deadline_monotonic=deadline_monotonic,
            )
            _raise_if_drain_deadline_expired(deadline_monotonic)
            if cached_claim is None:
                return None
        _raise_if_drain_deadline_expired(deadline_monotonic)
        return cached_claim

    def _recover_pending_visibility_timeout_claim(
        self,
        processing_queue: str,
        claim_id: str,
        *,
        deadline_monotonic: float | None = None,
    ) -> ClaimedMessage | None:
        _raise_if_drain_deadline_expired(deadline_monotonic)
        claim_result_key = self._claim_result_key(processing_queue, claim_id)
        cached_claim = _call_with_drain_deadline(
            lambda: self._redis_client.get(claim_result_key),
            deadline_monotonic=deadline_monotonic,
        )
        _raise_if_drain_deadline_expired(deadline_monotonic)
        if cached_claim is None:
            cached_claim = _call_with_drain_deadline(
                lambda: self._redis_client.hget(self._claim_result_ids_key(processing_queue), claim_id),
                deadline_monotonic=deadline_monotonic,
            )
            _raise_if_drain_deadline_expired(deadline_monotonic)
            if cached_claim is None:
                return None

        cached_claim_text = cached_claim.decode("utf-8") if isinstance(cached_claim, bytes) else cached_claim
        try:
            claim = json.loads(cached_claim_text)
        except json.JSONDecodeError:
            self._delete_claim_result_key(claim_result_key, deadline_monotonic=deadline_monotonic)
            return None

        if (
            not isinstance(claim, list)
            or len(claim) < 2
            or not isinstance(claim[0], str)
            or not isinstance(claim[1], str)
        ):
            self._delete_claim_result_key(claim_result_key, deadline_monotonic=deadline_monotonic)
            return None

        stored_message: MessageData = claim[0]
        if isinstance(cached_claim, bytes):
            stored_message = stored_message.encode("utf-8")
        lease_token = claim[1]

        self._delete_claim_result_key(claim_result_key, deadline_monotonic=deadline_monotonic)
        self._delete_claim_result_ref(
            self._claim_result_refs_key(processing_queue),
            lease_token,
            deadline_monotonic=deadline_monotonic,
        )
        _raise_if_drain_deadline_expired(deadline_monotonic)
        return ClaimedMessage(stored_message=stored_message, lease_token=lease_token)

    def _pending_queue_from_processing_queue(self, processing_queue: str) -> str:
        if not processing_queue.endswith(_PROCESSING_QUEUE_SUFFIX):
            raise RuntimeError(f"cannot derive pending queue key from processing queue {processing_queue!r}")
        return f"{processing_queue.removesuffix(_PROCESSING_QUEUE_SUFFIX)}{_PENDING_QUEUE_SUFFIX}"

    def _return_recovered_non_visibility_timeout_claim_to_pending(
        self,
        processing_queue: str,
        stored_message: MessageData,
        claim_id: str,
        *,
        deadline_monotonic: float | None,
    ) -> bool:
        pending_queue = self._pending_queue_from_processing_queue(processing_queue)
        operation_id = uuid.uuid4().hex
        operation_result_key = self._operation_result_key(processing_queue, operation_id)

        try:
            _raise_if_drain_deadline_expired(deadline_monotonic)
            result = _call_with_drain_deadline(
                lambda: self._eval(
                    RETURN_MESSAGE_TO_PENDING_LUA_SCRIPT,
                    6,
                    processing_queue,
                    pending_queue,
                    self._claim_result_ids_key(processing_queue),
                    self._claim_result_backrefs_key(processing_queue),
                    operation_result_key,
                    self._claim_result_key(processing_queue, claim_id),
                    stored_message,
                    claim_id,
                    self._operation_result_ttl_ms(),
                ),
                deadline_monotonic=deadline_monotonic,
            )
            _raise_if_drain_deadline_expired(deadline_monotonic)
            return bool(_coerce_lua_count(result))
        except RedisMessageQueueError as exc:
            _set_exception_context(
                exc,
                queue=processing_queue,
                message_id=extract_stored_message_id(stored_message),
                operation="drain",
            )
            raise
        finally:
            self._delete_operation_result_key(operation_result_key)

    def _is_interrupted(self, is_interrupted: BaseGracefulInterruptHandler | None = None) -> bool:
        return (self._interrupt is not None and self._interrupt.is_interrupted()) or (
            is_interrupted is not None and is_interrupted.is_interrupted()
        )

    def _drain_pending_claim_ids(
        self,
        processing_queue: str,
        *,
        deadline_monotonic: float | None,
    ) -> bool:
        """Recover every in-memory pending claim id for ``processing_queue``.

        Walks the same recovery path as ``_wait_for_claim`` but without
        gating on the interrupt flag, so a soft shutdown can flush
        ambiguous-claim state that would otherwise be dropped on process
        exit (AA-05-F2). No-visibility-timeout recoveries are returned to
        the pending queue before their claim ids are cleared. Returns True
        if no pending ids remain; False if the deadline fired or Redis
        errors prevented full drain.
        """
        with self._drain_pending_claim_ids_lock:
            self._last_drain_error = None
            return self._drain_pending_claim_ids_unlocked(
                processing_queue,
                deadline_monotonic=deadline_monotonic,
            )

    def _drain_pending_claim_ids_unlocked(
        self,
        processing_queue: str,
        *,
        deadline_monotonic: float | None,
    ) -> bool:
        """Recover every in-memory pending claim id for ``processing_queue``."""
        has_visibility_timeout = self._message_visibility_timeout_seconds is not None
        skipped_unresolved: set[str] = set()
        last_error: BaseException | None = None
        while True:
            # ``>=`` (not ``>``) makes ``timeout=0`` deterministically take
            # the no-recovery fast path: the deadline equals the call-time
            # ``monotonic()``, so the first iteration falls through to the
            # state-only check below.
            if deadline_monotonic is not None and time.monotonic() >= deadline_monotonic:
                last_error = TimeoutError("drain pending-claim recovery deadline expired")
                break
            with self._pending_claim_ids_lock:
                pending = self._pending_claim_ids.get(processing_queue)
                if not pending:
                    in_flight = self._in_flight_claim_ids.get(processing_queue)
                    if not in_flight:
                        return True
                    claim_id = None
                else:
                    recovering = self._recovering_claim_ids.setdefault(processing_queue, set())
                    claim_id = next(
                        (cid for cid in pending if cid not in recovering and cid not in skipped_unresolved),
                        None,
                    )
                    if claim_id is None:
                        break
                    recovering.add(claim_id)
            if claim_id is None:
                if deadline_monotonic is None:
                    time.sleep(_VISIBILITY_TIMEOUT_POLL_INTERVAL_SECONDS)
                else:
                    remaining = deadline_monotonic - time.monotonic()
                    if remaining <= 0:
                        last_error = TimeoutError("drain pending-claim recovery deadline expired")
                        break
                    time.sleep(min(_VISIBILITY_TIMEOUT_POLL_INTERVAL_SECONDS, remaining))
                continue
            clear = False
            try:
                try:
                    if has_visibility_timeout:
                        self._recover_pending_visibility_timeout_claim(
                            processing_queue,
                            claim_id,
                            deadline_monotonic=deadline_monotonic,
                        )
                        clear = True
                        continue

                    recovered_claim = self._recover_pending_non_visibility_timeout_claim(
                        processing_queue,
                        claim_id,
                        deadline_monotonic=deadline_monotonic,
                    )
                    if recovered_claim is None:
                        clear = True
                    elif self._return_recovered_non_visibility_timeout_claim_to_pending(
                        processing_queue,
                        recovered_claim,
                        claim_id,
                        deadline_monotonic=deadline_monotonic,
                    ):
                        clear = True
                    else:
                        last_error = RuntimeError(
                            f"drain recovered claim {claim_id!r} but message was not present in processing queue"
                        )
                        skipped_unresolved.add(claim_id)
                except _DrainDeadlineExceeded:
                    last_error = TimeoutError("drain pending-claim recovery deadline expired")
                    break
                except Exception as exc:
                    if not is_redis_retryable_exception(exc):
                        raise
                    last_error = exc
                    logger.warning(
                        "Transient Redis error draining pending claim %s; will retry on next drain: %s",
                        claim_id,
                        type(exc).__name__,
                    )
                    skipped_unresolved.add(claim_id)
            finally:
                self._finish_pending_claim_recovery(processing_queue, claim_id, clear=clear)
        with self._pending_claim_ids_lock:
            pending = self._pending_claim_ids.get(processing_queue)
            in_flight = self._in_flight_claim_ids.get(processing_queue)
            if pending or in_flight:
                self._last_drain_error = last_error
            return not pending and not in_flight
