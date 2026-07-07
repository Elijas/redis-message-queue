import asyncio
import json
import logging
import random
import threading
import uuid
import weakref
from typing import Awaitable, Callable, Optional, TypeVar

import redis
import redis.asyncio
import redis.asyncio.sentinel

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
    INTERRUPTIBLE_RETRY_SLEEP_POLL_SECONDS,
    MOVE_MESSAGE_LUA_SCRIPT,
    MOVE_MESSAGE_WITH_LEASE_TOKEN_LUA_SCRIPT,
    PENDING_OVERLOAD_LUA_SENTINEL,
    PUBLISH_MESSAGE_LUA_SCRIPT,
    PURGE_QUEUE_LUA_SCRIPT,
    REDRIVE_DEAD_LETTERS_LUA_SCRIPT,
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
    ReceivedPayload,
    decode_stored_message,
    encode_stored_message,
    extract_stored_message_id,
)
from redis_message_queue.asyncio._abstract_redis_gateway import AbstractRedisGateway
from redis_message_queue.interrupt_handler._interface import (
    BaseGracefulInterruptHandler,
)

logger = logging.getLogger(__name__)
_TClaim = TypeVar("_TClaim", bound=ClaimedMessage | ReceivedPayload)
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
    if deadline_monotonic is not None and asyncio.get_running_loop().time() >= deadline_monotonic:
        raise _DrainDeadlineExceeded


async def _call_with_drain_deadline(
    call: Callable[[], Awaitable[_TRedisCall]],
    *,
    deadline_monotonic: float | None,
) -> _TRedisCall:
    if deadline_monotonic is None:
        return await call()

    remaining_seconds = deadline_monotonic - asyncio.get_running_loop().time()
    if remaining_seconds <= 0:
        raise _DrainDeadlineExceeded

    try:
        return await asyncio.wait_for(call(), timeout=remaining_seconds)
    except TimeoutError as exc:
        raise _DrainDeadlineExceeded from exc


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
    # Structural check, not just a sentinel-string compare: the claim Lua's
    # failure reply is exactly {sentinel, err, stored} (3 elements), while
    # successes are 4-tuples and cache replays 2-tuples. A legitimate message
    # whose payload equals the sentinel string therefore never matches here.
    return (
        isinstance(result, list | tuple)
        and len(result) == 3
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
    """Async Redis gateway with built-in tenacity-based retry on transient errors.

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
        redis_client: redis.asyncio.Redis,
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
        if isinstance(redis_client, redis.asyncio.sentinel.Sentinel):
            raise TypeError(
                "'redis_client' is a redis.sentinel.Sentinel manager object, not a Redis client. "
                "Pass sentinel.master_for(name) (or async equivalent) instead."
            )
        if isinstance(redis_client, (redis.client.Pipeline, redis.asyncio.client.Pipeline)):
            raise TypeError(
                "'redis_client' is a Pipeline, not a Redis client; "
                "Pipeline defers execution and would silently drop writes. "
                "Pass the underlying redis.asyncio.Redis instance instead."
            )
        if isinstance(redis_client, (redis.Redis, redis.RedisCluster)) and not isinstance(
            redis_client, (redis.asyncio.Redis, redis.asyncio.RedisCluster)
        ):
            raise TypeError(
                "'redis_client' is a sync Redis client (redis.Redis); "
                "use the sync RedisMessageQueue from redis_message_queue instead"
            )
        if isinstance(redis_client, redis.asyncio.Redis) and redis_client.single_connection_client:
            raise TypeError(
                "'redis_client' is a redis.asyncio.Redis single-connection client. "
                "redis.asyncio.Redis(single_connection_client=True) pins every command to one "
                "socket behind a single lock, so lease-heartbeat renewals can queue behind a slow "
                "handler-issued command and the lease can expire. Pass a normal pooled "
                "redis.asyncio.Redis client instead."
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
        self._drain_pending_claim_ids_lock = asyncio.Lock()
        # Keyed by processing-queue key rather than a single slot: two
        # RedisMessageQueue instances are permitted to share one gateway
        # (whenever max_delivery_count is unset), and each must have its own
        # gateway-level events (retry_attempt, retry_exhausted, claim_reclaim,
        # dlq) routed to its own on_event under its own queue name.
        #
        # The emitter is held as a weakref.WeakMethod so a queue instance that
        # is dropped without draining (e.g. an abandoned per-job queue) does not
        # keep its whole object graph — locks, key manager, on_event closures —
        # pinned alive for the shared gateway's lifetime. drain() also actively
        # unregisters (see _unregister_event_emitter), which keeps this dict
        # bounded for a long-lived gateway that cycles through dynamic queue
        # names; the weakref is the backstop for the never-drained case.
        self._event_emitters: dict[str, tuple[str, weakref.WeakMethod]] = {}

    def _set_event_emitter(
        self,
        processing_queue: str,
        queue_name: str,
        emitter: Callable[..., Awaitable[None]] | None,
    ) -> None:
        if emitter is None:
            self._event_emitters.pop(processing_queue, None)
            return
        existing = self._event_emitters.get(processing_queue)
        if existing is not None:
            existing_emitter = existing[1]()
            if existing_emitter is not None and existing_emitter.__self__ is not emitter.__self__:
                # Two live queues with the same name share this gateway's single
                # per-processing-key slot, so gateway-level events for the key
                # can only reach one on_event. Last-wins (this registration)
                # rather than fail fast, because destroy-then-reconstruct of a
                # same-name queue is legitimate; but warn loudly so the silent
                # telemetry loss the previous code produced is observable.
                logger.warning(
                    "Two live RedisMessageQueue instances named %r share this gateway; "
                    "gateway-level events (retry_attempt, retry_exhausted, claim_reclaim, "
                    "dlq) for this queue now route only to the most recently constructed "
                    "instance's on_event and the earlier instance's is dropped. Give each "
                    "concurrently-live queue a distinct name, or drain the earlier instance "
                    "before constructing its replacement.",
                    queue_name,
                )
        self._event_emitters[processing_queue] = (queue_name, weakref.WeakMethod(emitter))

    def _unregister_event_emitter(
        self,
        processing_queue: str,
        emitter: Callable[..., Awaitable[None]],
    ) -> None:
        # Remove the entry only if it still belongs to this emitter's queue
        # instance. A second, still-live same-name queue may have overwritten
        # the slot (see the warning in _set_event_emitter); draining this
        # instance must not evict that live queue's emitter. A dead weakref is
        # always safe to drop.
        existing = self._event_emitters.get(processing_queue)
        if existing is None:
            return
        existing_emitter = existing[1]()
        if existing_emitter is None or existing_emitter.__self__ is emitter.__self__:
            self._event_emitters.pop(processing_queue, None)

    async def _emit_event(
        self,
        processing_queue: str,
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
        emitter_entry = self._event_emitters.get(processing_queue)
        if emitter_entry is None:
            return
        _queue_name, emitter_ref = emitter_entry
        emitter = emitter_ref()
        if emitter is None:
            # The queue was garbage-collected without draining; drop the dead
            # entry so an event to this key is a no-op rather than a crash.
            self._event_emitters.pop(processing_queue, None)
            return
        await emitter(
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

    async def _emit_repeated_event(
        self,
        processing_queue: str,
        operation: EventOperation | str,
        attempts: list[_MessageAttemptEvent],
        *,
        destination_queue: str | None = None,
        max_delivery_count: int | None = None,
    ) -> None:
        for message_id, delivery_count in attempts:
            await self._emit_event(
                processing_queue,
                operation,
                "success",
                message_id=message_id,
                destination_queue=destination_queue,
                delivery_count=delivery_count,
                max_delivery_count=max_delivery_count,
            )

    async def _eval(self, *args: object) -> object:
        try:
            return await self._redis_client.eval(*args)
        except redis.exceptions.ResponseError as exc:
            lua_error = wrap_lua_response_error(exc)
            if lua_error is not None:
                raise lua_error from exc
            raise

    def _lua_max_pending_length(self) -> str:
        return "" if self._max_pending_length is None else str(self._max_pending_length)

    def _pending_block_deadline(self) -> float:
        # Absolute bound for one gateway operation call, on the running loop's
        # clock. Computed by the caller OUTSIDE the tenacity-retried closure and
        # threaded through every retry so a transient Redis error mid-wait cannot
        # restart the block window.
        return asyncio.get_running_loop().time() + self._pending_overload_block_timeout_seconds

    async def _run_pending_backpressure_operation(
        self,
        queue: str,
        operation: Callable[[], Awaitable[object]],
        *,
        deadline_monotonic: float,
        is_interrupted: BaseGracefulInterruptHandler | None = None,
    ) -> object:
        loop = asyncio.get_running_loop()
        backoff_seconds = _PENDING_OVERLOAD_INITIAL_BACKOFF_SECONDS
        max_backoff_seconds = _pending_overload_max_backoff_seconds(self._pending_overload_block_timeout_seconds)
        while True:
            result = await operation()
            if _coerce_lua_count(result) != PENDING_OVERLOAD_LUA_SENTINEL:
                return result
            if self._pending_overload_policy != "block":
                raise QueueBackpressureError(
                    f"Pending queue {queue!r} reached max_pending_length={self._max_pending_length}",
                    queue=queue,
                    operation="publish",
                )
            remaining = deadline_monotonic - loop.time()
            if remaining <= 0:
                raise QueueBackpressureError(
                    f"Pending queue {queue!r} stayed at max_pending_length={self._max_pending_length} "
                    f"for {self._pending_overload_block_timeout_seconds} seconds",
                    queue=queue,
                    operation="publish",
                    remediation=QueueBackpressureError._BLOCK_TIMEOUT_REMEDIATION,
                )
            sleep_seconds = min(_jitter_pending_overload_backoff_seconds(backoff_seconds), remaining)
            if await self._sleep_pending_overload_backoff(sleep_seconds, is_interrupted=is_interrupted):
                raise QueueBackpressureError(
                    f"Pending queue {queue!r} block wait aborted by shutdown interrupt while at "
                    f"max_pending_length={self._max_pending_length}",
                    queue=queue,
                    operation="publish",
                    remediation=QueueBackpressureError._INTERRUPT_ABORT_REMEDIATION,
                )
            backoff_seconds = min(backoff_seconds * 2, max_backoff_seconds)

    async def _sleep_pending_overload_backoff(
        self,
        sleep_seconds: float,
        *,
        is_interrupted: BaseGracefulInterruptHandler | None = None,
    ) -> bool:
        # Slice the backoff sleep so an interrupt is observed within one poll
        # interval instead of after the full backoff. Combines the gateway-level
        # interrupt with the optional per-call ``is_interrupted`` (a queue
        # drain() forwards its drain flag here). Returns True if an interrupt
        # landed.
        loop = asyncio.get_running_loop()
        deadline = loop.time() + sleep_seconds
        while True:
            if self._is_interrupted(is_interrupted):
                return True
            remaining = deadline - loop.time()
            if remaining <= 0:
                return False
            await asyncio.sleep(min(remaining, INTERRUPTIBLE_RETRY_SLEEP_POLL_SECONDS))

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
        return isinstance(self._redis_client, redis.asyncio.RedisCluster)

    def _raise_if_drop_oldest_deduplicated_publish(self) -> None:
        if self._pending_overload_policy == "drop_oldest":
            raise ConfigurationError(
                "'pending_overload_policy=drop_oldest' cannot be used with RedisGateway.publish_message "
                "because dropped messages leave their deduplication keys in Redis, causing future publishes "
                "of the same payload to be silently suppressed. Use add_message for non-deduplicated lossy "
                "queues, or use 'raise' or 'block' for deduplicated publishes."
            )

    async def publish_message(self, queue: str, message: str, dedup_key: str) -> bool:
        return await self._publish_message_interruptible(queue, message, dedup_key)

    async def _publish_message_interruptible(
        self,
        queue: str,
        message: str,
        dedup_key: str,
        *,
        is_interrupted: BaseGracefulInterruptHandler | None = None,
    ) -> bool:
        # Private interruptible twin of ``publish_message``: the queue forwards
        # its drain flag here so a block-policy capacity wait aborts promptly on
        # drain even when no GracefulInterruptHandler is configured. Mirrors the
        # claim-path ``_wait_for_message_and_move_interruptible`` contract;
        # ``AbstractRedisGateway`` is unchanged so custom gateways without this
        # method keep prior behavior.
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
        block_deadline = self._pending_block_deadline()
        retry_strategy = self._pending_overload_retry_strategy(is_interrupted)

        @retry_strategy
        async def _publish():
            result = await self._run_pending_backpressure_operation(
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
                deadline_monotonic=block_deadline,
                is_interrupted=is_interrupted,
            )
            return bool(_coerce_lua_count(result))

        try:
            return await _publish()
        except RedisMessageQueueError as exc:
            _set_exception_context(exc, queue=queue, message_id=message_id, operation="publish")
            raise
        finally:
            await self._delete_operation_result_key(operation_result_key)

    def _pending_overload_retry_strategy(self, is_interrupted: BaseGracefulInterruptHandler | None):
        if is_interrupted is None:
            return self._retry_strategy
        # Per-call strategy chaining the gateway interrupt with the caller's
        # stop signal, so a transient Redis error mid-block-wait does not keep
        # the publish retrying past a drain (mirrors renew_message_lease).
        return build_retry_strategy(
            retry_budget_seconds=self._retry_budget_seconds,
            retry_max_delay_seconds=self._retry_max_delay_seconds,
            retry_initial_delay_seconds=self._retry_initial_delay_seconds,
            interrupt=_ChainedInterrupt(self._interrupt, is_interrupted),
        )

    async def add_message(self, queue: str, message: str) -> None:
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
        await self._add_message_interruptible(queue, message)

    async def _add_message_interruptible(
        self,
        queue: str,
        message: str,
        *,
        is_interrupted: BaseGracefulInterruptHandler | None = None,
    ) -> None:
        # Private interruptible twin of ``add_message`` (see
        # ``_publish_message_interruptible``). The block-policy capacity wait it
        # may enter is the only interruptible part; the at-most-once enqueue is
        # never retried.
        stored_message = encode_stored_message(message)
        message_id = extract_stored_message_id(stored_message)
        if self._max_pending_length is not None:
            try:
                await self._run_pending_backpressure_operation(
                    queue,
                    lambda: self._eval(
                        ADD_MESSAGE_LUA_SCRIPT,
                        1,
                        queue,
                        stored_message,
                        self._lua_max_pending_length(),
                        self._pending_overload_policy,
                    ),
                    deadline_monotonic=self._pending_block_deadline(),
                    is_interrupted=is_interrupted,
                )
            except RedisMessageQueueError as exc:
                _set_exception_context(exc, queue=queue, message_id=message_id, operation="publish")
                raise
            return
        try:
            await self._redis_client.lpush(queue, stored_message)  # type: ignore
        except RedisMessageQueueError as exc:
            _set_exception_context(exc, queue=queue, message_id=message_id, operation="publish")
            raise

    async def move_message(
        self,
        from_queue: str,
        to_queue: str,
        message: ReceivedPayload,
        *,
        lease_token: str | None = None,
    ) -> bool:
        decoded_message = decode_stored_message(message)

        if lease_token is None:
            operation_id = uuid.uuid4().hex
            operation_result_key = self._operation_result_key(from_queue, operation_id)

            @self._retry_strategy
            async def _move():
                return bool(
                    await self._eval(
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
                return await _move()
            except RedisMessageQueueError as exc:
                _set_exception_context(
                    exc,
                    queue=from_queue,
                    message_id=extract_stored_message_id(message),
                    operation="ack",
                )
                raise
            finally:
                await self._delete_operation_result_key(operation_result_key)

        operation_id = uuid.uuid4().hex
        operation_result_key = self._lease_operation_result_key(from_queue, lease_token, operation_id)

        @self._retry_strategy
        async def _move_with_lease():
            return bool(
                await self._eval(
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
            return await _move_with_lease()
        except RedisMessageQueueError as exc:
            _set_exception_context(
                exc,
                queue=from_queue,
                message_id=extract_stored_message_id(message),
                operation="ack",
            )
            raise
        finally:
            await self._delete_operation_result_key(operation_result_key)

    async def remove_message(self, queue: str, message: ReceivedPayload, *, lease_token: str | None = None) -> bool:
        if lease_token is None:
            operation_id = uuid.uuid4().hex
            operation_result_key = self._operation_result_key(queue, operation_id)

            @self._retry_strategy
            async def _remove():
                return bool(
                    await self._eval(
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
                return await _remove()
            except RedisMessageQueueError as exc:
                _set_exception_context(
                    exc,
                    queue=queue,
                    message_id=extract_stored_message_id(message),
                    operation="ack",
                )
                raise
            finally:
                await self._delete_operation_result_key(operation_result_key)

        operation_id = uuid.uuid4().hex
        operation_result_key = self._lease_operation_result_key(queue, lease_token, operation_id)

        @self._retry_strategy
        async def _remove_with_lease():
            return bool(
                await self._eval(
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
            return await _remove_with_lease()
        except RedisMessageQueueError as exc:
            _set_exception_context(
                exc,
                queue=queue,
                message_id=extract_stored_message_id(message),
                operation="ack",
            )
            raise
        finally:
            await self._delete_operation_result_key(operation_result_key)

    async def renew_message_lease(
        self,
        queue: str,
        message: ReceivedPayload,
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
        async def _renew():
            return bool(
                await self._eval(
                    RENEW_MESSAGE_LEASE_LUA_SCRIPT,
                    2,
                    self._lease_deadlines_key(queue),
                    self._lease_tokens_key(queue),
                    message,
                    lease_token,
                    str(self._message_visibility_timeout_seconds * 1000),
                )
            )

        return await _renew()

    async def _wait_for_claim(
        self,
        from_queue: str,
        to_queue: str,
        *,
        recover_pending_claim: Callable[[str, str], Awaitable[_TClaim | None]],
        claim_message: Callable[[str, str, str], Awaitable[_TClaim | None]],
        non_blocking_retry_log: str,
        polling_retry_log: str,
        is_interrupted: BaseGracefulInterruptHandler | None = None,
    ) -> _TClaim | None:
        while True:
            # clear=True on a None recovery is safe ONLY because pending_claim_id
            # is registered (in the outer finally below) strictly AFTER the
            # original eval awaited or raised. Redis EVAL is atomic, so by the
            # time we observe the claim_id here, the original Lua has either
            # committed or never ran — there is no "still in flight" window. If
            # a future refactor registers the claim_id BEFORE awaiting the eval,
            # this invariant breaks and a concurrent recovery could clear a
            # pending claim that hasn't actually committed yet.
            #
            # The acquisition happens inside the try, and _acquire_pending_claim_id
            # records the cleanup token into recovering_token BEFORE marking the
            # id recovering, so an interrupt landing anywhere in the acquisition
            # window still lets the finally release the recovering entry
            # (same hardening as begin_active_claim below).
            recovering_token: list[str] = []
            clear_pending_claim_id = False
            try:
                pending_claim_id = self._acquire_pending_claim_id(to_queue, recovering_token)
                if pending_claim_id is None:
                    break
                recovered_claim = await recover_pending_claim(to_queue, pending_claim_id)
                clear_pending_claim_id = True
            finally:
                if recovering_token:
                    self._finish_pending_claim_recovery(
                        to_queue,
                        recovering_token[0],
                        clear=clear_pending_claim_id,
                    )
            if recovered_claim is not None:
                # The recovery already cleared this claim id from
                # _pending_claim_ids (finally above). If a BaseException
                # (CancelledError / KeyboardInterrupt) lands on the awaited
                # cleanup below, the message is still in the processing list
                # with no in-memory claim id and — in no-VT mode — no lease to
                # reclaim it, so drain() would report success while
                # the message is permanently stranded. Re-register the id so a
                # subsequent drain still recovers it (recovery is idempotent).
                try:
                    if self._message_visibility_timeout_seconds is None:
                        await self._delete_claim_result_key(self._claim_result_key(to_queue, pending_claim_id))
                    await self._emit_event(to_queue, "claim_reclaim", "success", claim_id=pending_claim_id)
                except BaseException:
                    self._set_pending_claim_id(to_queue, pending_claim_id)
                    raise
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
            # Record the cleanup token before registration so a signal landing in
            # the registration window still lets the outer finally unregister it.
            active_claim_id = claim_id
            self._begin_in_flight_claim_id(to_queue, claim_id)

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
                    claimed_message = await claim_message(from_queue, to_queue, claim_id)
                except Exception as exc:
                    if not is_redis_retryable_exception(exc):
                        pending_claim_id_to_share = claim_id
                        raise
                    claim_may_need_recovery = True
                    # The claim may already be committed server-side; publish the
                    # recovery id BEFORE the emit await so a BaseException
                    # (CancelledError) escaping the emit is still covered by the
                    # outer finally's _set_pending_claim_id. The sibling
                    # ``except BaseException`` below does not catch exceptions
                    # raised from inside this ``except Exception`` clause.
                    pending_claim_id_to_share = claim_id
                    await self._emit_event(
                        to_queue,
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
                        claimed_message = await claim_message(from_queue, to_queue, claim_id)
                    except Exception as retry_exc:
                        if claim_may_need_recovery:
                            pending_claim_id_to_share = claim_id
                        await self._emit_event(
                            to_queue,
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
                    # Retry succeeded (idempotent replay returns the committed
                    # claim): the claim is handed to the caller, so clear the
                    # recovery id the failed first attempt published above.
                    pending_claim_id_to_share = None
                    return claimed_message
                except BaseException:
                    pending_claim_id_to_share = claim_id
                    raise
                return claimed_message

            loop = asyncio.get_running_loop()
            deadline = loop.time() + self._message_wait_interval_seconds
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
                    claimed_message = await claim_message(from_queue, to_queue, claim_id)
                except Exception as exc:
                    if not is_redis_retryable_exception(exc):
                        pending_claim_id_to_share = claim_id
                        raise
                    claim_may_need_recovery = True
                    # Publish the recovery id BEFORE the emit await: a
                    # CancelledError escaping the emit is not caught by the
                    # sibling ``except BaseException`` below (it is raised from
                    # within this ``except Exception`` clause), so without this
                    # the outer finally would never re-register the id.
                    pending_claim_id_to_share = claim_id
                    await self._emit_event(
                        to_queue,
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
                        # Claim handed to the caller: it is no longer a recovery
                        # candidate, so drop any id a prior retry published.
                        pending_claim_id_to_share = None
                        return claimed_message
                    claim_may_need_recovery = False
                    # Empty poll: no claim committed for this id, so it is safe.
                    pending_claim_id_to_share = None
                    last_retryable_exception = None
                    finish_active_claim()
                    claim_id = uuid.uuid4().hex

                remaining = deadline - loop.time()
                if remaining <= 0:
                    if last_retryable_exception is not None:
                        if self._is_interrupted(is_interrupted):
                            if claim_may_need_recovery:
                                pending_claim_id_to_share = claim_id
                            return None
                        try:
                            recovered_claim = await recover_pending_claim(to_queue, claim_id)
                        except Exception:
                            if claim_may_need_recovery:
                                pending_claim_id_to_share = claim_id
                            raise
                        except BaseException:
                            pending_claim_id_to_share = claim_id
                            raise
                        if recovered_claim is not None:
                            # A BaseException on the awaited cleanup below must
                            # re-register the id so drain still recovers the
                            # message; on success the claim is handed to the
                            # caller and is no longer a recovery candidate.
                            try:
                                if self._message_visibility_timeout_seconds is None:
                                    await self._delete_claim_result_key(self._claim_result_key(to_queue, claim_id))
                                await self._emit_event(to_queue, "claim_reclaim", "success", claim_id=claim_id)
                            except BaseException:
                                pending_claim_id_to_share = claim_id
                                raise
                            pending_claim_id_to_share = None
                            return recovered_claim
                        # Recovery miss against a responsive Redis: the same
                        # no-commit proof the empty-poll branch above trusts,
                        # so the claim id is provably dead — drop it instead
                        # of registering a ghost id for a later drain.
                        pending_claim_id_to_share = None
                        await self._emit_event(
                            to_queue,
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
                await asyncio.sleep(min(_VISIBILITY_TIMEOUT_POLL_INTERVAL_SECONDS, remaining))
        finally:
            if pending_claim_id_to_share is not None:
                self._set_pending_claim_id(to_queue, pending_claim_id_to_share)
            finish_active_claim()

    async def wait_for_message_and_move(
        self, from_queue: str, to_queue: str
    ) -> ClaimedMessage | ReceivedPayload | None:
        if self._is_interrupted():
            return None
        return await self._wait_for_message_and_move_interruptible(from_queue, to_queue)

    async def _wait_for_message_and_move_interruptible(
        self,
        from_queue: str,
        to_queue: str,
        *,
        is_interrupted: BaseGracefulInterruptHandler | None = None,
    ) -> ClaimedMessage | ReceivedPayload | None:
        if self._is_interrupted(is_interrupted):
            return None
        if self._message_visibility_timeout_seconds is not None:
            return await self._wait_for_message_with_visibility_timeout(
                from_queue,
                to_queue,
                is_interrupted=is_interrupted,
            )
        return await self._wait_for_message_without_visibility_timeout(
            from_queue,
            to_queue,
            is_interrupted=is_interrupted,
        )

    async def _wait_for_message_without_visibility_timeout(
        self,
        from_queue: str,
        to_queue: str,
        *,
        is_interrupted: BaseGracefulInterruptHandler | None = None,
    ) -> ReceivedPayload | None:
        return await self._wait_for_claim(
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

    async def _wait_for_message_with_visibility_timeout(
        self,
        from_queue: str,
        to_queue: str,
        *,
        is_interrupted: BaseGracefulInterruptHandler | None = None,
    ) -> ClaimedMessage | None:
        return await self._wait_for_claim(
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

    async def _claim_message_without_visibility_timeout(
        self,
        from_queue: str,
        to_queue: str,
        *,
        claim_id: str,
    ) -> ReceivedPayload | None:
        claim_result_key = self._claim_result_key(to_queue, claim_id)
        result = await self._eval(
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

        await self._delete_claim_result_key(claim_result_key)
        return result

    async def _claim_visible_message(self, from_queue: str, to_queue: str, *, claim_id: str) -> ClaimedMessage | None:
        result = await self._eval(
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
        await self._emit_repeated_event(to_queue, "claim_reclaim", reclaimed_attempts)
        await self._emit_repeated_event(
            to_queue,
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

    async def trim_queue(self, queue: str, max_length: int) -> None:
        await self._redis_client.ltrim(queue, 0, max_length - 1)

    async def queue_length(self, queue: str) -> int:
        """Return the ``LLEN`` of ``queue`` (operator inspection helper)."""
        return int(await self._redis_client.llen(queue))

    async def peek_messages(self, queue: str, count: int) -> list[ReceivedPayload]:
        """Return up to ``count`` stored messages from the head of ``queue``.

        Non-consuming: uses ``LRANGE`` and leaves the list untouched. Values are
        returned exactly as stored (envelope for pending/processing, raw payload
        for the completed/failed/dead-letter logs); the queue decodes them.
        """
        return list(await self._redis_client.lrange(queue, 0, count - 1))

    async def purge_queue(self, queue: str) -> int:
        """Atomically delete ``queue`` and return how many entries were removed."""
        return _coerce_lua_count(await self._eval(PURGE_QUEUE_LUA_SCRIPT, 1, queue))

    async def redrive_messages(
        self,
        dead_letter_queue: str,
        pending_queue: str,
        envelope_ids: list[str],
    ) -> int:
        """Atomically move up to ``len(envelope_ids)`` messages DLQ -> pending.

        Each raw dead-letter payload is re-wrapped in a fresh RMQ envelope using
        the matching id from ``envelope_ids`` so the redriven message claims
        normally and starts with a fresh delivery count. Returns how many
        messages were moved (fewer than requested when the DLQ runs empty).

        Intentionally bypasses any ``max_pending_length`` cap configured for
        ``pending_queue``: this is a low-level operator recovery primitive and
        does not check or enforce pending-list backpressure.
        """
        if not envelope_ids:
            return 0
        return _coerce_lua_count(
            await self._eval(
                REDRIVE_DEAD_LETTERS_LUA_SCRIPT,
                2,
                dead_letter_queue,
                pending_queue,
                *envelope_ids,
            )
        )

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

    async def _cleanup_drained_lease_token_counter(self, processing_queue: str) -> bool:
        if self._message_visibility_timeout_seconds is None:
            return False
        return bool(
            _coerce_lua_count(
                await self._eval(
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

    async def _delete_claim_result_key(
        self,
        claim_result_key: str,
        *,
        deadline_monotonic: float | None = None,
    ) -> None:
        try:
            await _call_with_drain_deadline(
                lambda: self._redis_client.delete(claim_result_key),
                deadline_monotonic=deadline_monotonic,
            )
        except _DrainDeadlineExceeded:
            raise
        except Exception:
            # Claim-result keys have bounded TTLs; this cleanup is intentionally best-effort.
            logger.warning("Failed to delete claim result key %s", claim_result_key, exc_info=True)

    async def _delete_claim_result_ref(
        self,
        claim_result_refs_key: str,
        lease_token: str,
        *,
        deadline_monotonic: float | None = None,
    ) -> None:
        try:
            await _call_with_drain_deadline(
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

    async def _delete_corrupt_claim_result(
        self,
        claim_result_key: str,
        processing_queue: str,
        claim_id: str,
        *,
        deadline_monotonic: float | None = None,
    ) -> None:
        # A corrupt claim result is useless to every reader, so purge both
        # ledger locations: the TTL-bounded claim_result string and the no-TTL
        # claim_result_ids hash entry (otherwise the corrupt entry lingers
        # until the claim script's expiry-reclaim cleans it).
        await self._delete_claim_result_key(claim_result_key, deadline_monotonic=deadline_monotonic)
        try:
            await _call_with_drain_deadline(
                lambda: self._redis_client.hdel(self._claim_result_ids_key(processing_queue), claim_id),
                deadline_monotonic=deadline_monotonic,
            )
        except _DrainDeadlineExceeded:
            raise
        except Exception:
            # Best-effort like the claim-result key: the expiry-reclaim loop is
            # the guaranteed janitor for this hash entry.
            logger.warning(
                "Failed to delete claim result id %s[%s]",
                self._claim_result_ids_key(processing_queue),
                claim_id,
                exc_info=True,
            )

    async def _delete_operation_result_key(self, operation_result_key: str) -> None:
        try:
            await self._redis_client.delete(operation_result_key)
        except Exception:
            logger.debug("Failed to delete operation result key %s", operation_result_key, exc_info=True)

    def _acquire_pending_claim_id(self, processing_queue: str, recovering_token: list[str]) -> str | None:
        with self._pending_claim_ids_lock:
            pending_claim_ids = self._pending_claim_ids.get(processing_queue)
            if not pending_claim_ids:
                return None
            recovering_claim_ids = self._recovering_claim_ids.setdefault(processing_queue, set())
            for claim_id in pending_claim_ids:
                if claim_id not in recovering_claim_ids:
                    # Record the cleanup token BEFORE marking the id recovering
                    # so an interrupt landing after the add still lets the
                    # caller's finally release the entry via the token.
                    recovering_token.append(claim_id)
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

    async def _recover_pending_non_visibility_timeout_claim(
        self,
        processing_queue: str,
        claim_id: str,
        *,
        deadline_monotonic: float | None = None,
    ) -> ReceivedPayload | None:
        _raise_if_drain_deadline_expired(deadline_monotonic)
        claim_result_key = self._claim_result_key(processing_queue, claim_id)
        cached_claim = await _call_with_drain_deadline(
            lambda: self._redis_client.get(claim_result_key),
            deadline_monotonic=deadline_monotonic,
        )
        _raise_if_drain_deadline_expired(deadline_monotonic)
        if cached_claim is None:
            cached_claim = await _call_with_drain_deadline(
                lambda: self._redis_client.hget(self._claim_result_ids_key(processing_queue), claim_id),
                deadline_monotonic=deadline_monotonic,
            )
            _raise_if_drain_deadline_expired(deadline_monotonic)
            if cached_claim is None:
                return None
        _raise_if_drain_deadline_expired(deadline_monotonic)
        return cached_claim

    async def _recover_pending_visibility_timeout_claim(
        self,
        processing_queue: str,
        claim_id: str,
        *,
        deadline_monotonic: float | None = None,
        rearm_lease: bool = True,
    ) -> ClaimedMessage | None:
        _raise_if_drain_deadline_expired(deadline_monotonic)
        claim_result_key = self._claim_result_key(processing_queue, claim_id)
        cached_claim = await _call_with_drain_deadline(
            lambda: self._redis_client.get(claim_result_key),
            deadline_monotonic=deadline_monotonic,
        )
        _raise_if_drain_deadline_expired(deadline_monotonic)
        if cached_claim is None:
            cached_claim = await _call_with_drain_deadline(
                lambda: self._redis_client.hget(self._claim_result_ids_key(processing_queue), claim_id),
                deadline_monotonic=deadline_monotonic,
            )
            _raise_if_drain_deadline_expired(deadline_monotonic)
            if cached_claim is None:
                return None

        try:
            cached_claim_text = cached_claim.decode("utf-8") if isinstance(cached_claim, bytes) else cached_claim
            claim = json.loads(cached_claim_text)
        except (UnicodeDecodeError, json.JSONDecodeError):
            # The claim Lua stores cjson.encode({stored, lease_token}) with raw
            # payload bytes, so a foreign non-UTF-8 payload yields a claim
            # result this side cannot parse. Treat it exactly like corrupt
            # JSON: purge the ledger entries and let lease expiry redeliver.
            await self._delete_corrupt_claim_result(
                claim_result_key, processing_queue, claim_id, deadline_monotonic=deadline_monotonic
            )
            return None

        if (
            not isinstance(claim, list)
            or len(claim) < 2
            or not isinstance(claim[0], str)
            or not isinstance(claim[1], str)
        ):
            await self._delete_corrupt_claim_result(
                claim_result_key, processing_queue, claim_id, deadline_monotonic=deadline_monotonic
            )
            return None

        stored_message: ReceivedPayload = claim[0]
        if isinstance(cached_claim, bytes):
            try:
                stored_message = stored_message.encode("utf-8")
            except UnicodeEncodeError:
                # A bare surrogate escape in the claim JSON cannot come from the
                # claim Lua (cjson doubles backslashes and never emits surrogate
                # escapes), so the value is tampered/corrupt: same purge-and-miss
                # handling as the parse failures above.
                await self._delete_corrupt_claim_result(
                    claim_result_key, processing_queue, claim_id, deadline_monotonic=deadline_monotonic
                )
                return None
        lease_token = claim[1]

        await self._delete_claim_result_key(claim_result_key, deadline_monotonic=deadline_monotonic)
        _raise_if_drain_deadline_expired(deadline_monotonic)
        await self._delete_claim_result_ref(
            self._claim_result_refs_key(processing_queue),
            lease_token,
            deadline_monotonic=deadline_monotonic,
        )
        _raise_if_drain_deadline_expired(deadline_monotonic)
        # Re-arm the lease before handing the recovered claim back. The in-Lua
        # replay paths ZADD a fresh ``now_ms + visibility_timeout`` deadline on
        # every cached-claim / cached-recovery replay (see the design note in
        # CLAIM_MESSAGE_WITH_VISIBILITY_TIMEOUT_LUA_SCRIPT); a Python-side
        # recovery must do the same or the message resumes processing on a
        # mostly/fully elapsed lease and is instantly reclaimed by another
        # worker (guaranteed duplicate processing). renew is token-gated: a
        # False result means the lease token is gone (another worker already
        # reclaimed the message), so treat the recovery as a miss and let the
        # message redeliver normally. Drain-cleanup callers pass rearm_lease
        # False: they discard the claim and rely on lease expiry to redeliver,
        # so extending the deadline would only delay that reclaim.
        if rearm_lease and not await self.renew_message_lease(processing_queue, stored_message, lease_token):
            return None
        return ClaimedMessage(stored_message=stored_message, lease_token=lease_token)

    def _pending_queue_from_processing_queue(self, processing_queue: str) -> str:
        if not processing_queue.endswith(_PROCESSING_QUEUE_SUFFIX):
            raise RuntimeError(f"cannot derive pending queue key from processing queue {processing_queue!r}")
        return f"{processing_queue.removesuffix(_PROCESSING_QUEUE_SUFFIX)}{_PENDING_QUEUE_SUFFIX}"

    async def _return_recovered_non_visibility_timeout_claim_to_pending(
        self,
        processing_queue: str,
        stored_message: ReceivedPayload,
        claim_id: str,
        *,
        deadline_monotonic: float | None,
    ) -> bool:
        pending_queue = self._pending_queue_from_processing_queue(processing_queue)
        operation_id = uuid.uuid4().hex
        operation_result_key = self._operation_result_key(processing_queue, operation_id)

        try:
            _raise_if_drain_deadline_expired(deadline_monotonic)
            result = await _call_with_drain_deadline(
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
            await self._delete_operation_result_key(operation_result_key)

    def _is_interrupted(self, is_interrupted: BaseGracefulInterruptHandler | None = None) -> bool:
        return (self._interrupt is not None and self._interrupt.is_interrupted()) or (
            is_interrupted is not None and is_interrupted.is_interrupted()
        )

    async def _drain_pending_claim_ids(
        self,
        processing_queue: str,
        *,
        deadline_monotonic: float | None,
    ) -> tuple[bool, BaseException | None]:
        """Async sibling of the sync drain helper (AA-05-F2).

        Kept separate from the sync implementation per AF11 (sync/async
        gateways stay duplicated). Walks the same recovery path as
        ``_wait_for_claim`` but without gating on the interrupt flag so a
        soft shutdown can flush ambiguous-claim state. No-visibility-timeout
        recoveries are returned to the pending queue before their claim ids
        are cleared. Returns ``(True, None)`` if no pending ids remain, or
        ``(False, error)`` on deadline expiry or Redis errors, where
        ``error`` is the failure that caused *this* queue's drain to stall.
        The error is returned rather than stashed on gateway-wide state so
        that two queues sharing one gateway each see their own drain's
        error instead of racing to overwrite a shared slot.
        """
        async with self._drain_pending_claim_ids_lock:
            return await self._drain_pending_claim_ids_unlocked(
                processing_queue,
                deadline_monotonic=deadline_monotonic,
            )

    async def _drain_pending_claim_ids_unlocked(
        self,
        processing_queue: str,
        *,
        deadline_monotonic: float | None,
    ) -> tuple[bool, BaseException | None]:
        """Recover every in-memory pending claim id for ``processing_queue``."""
        has_visibility_timeout = self._message_visibility_timeout_seconds is not None
        skipped_unresolved: set[str] = set()
        loop = asyncio.get_running_loop()
        last_error: BaseException | None = None
        while True:
            # See sync sibling: ``>=`` makes ``timeout=0`` deterministically
            # take the no-recovery fast path.
            if deadline_monotonic is not None and loop.time() >= deadline_monotonic:
                last_error = TimeoutError("drain pending-claim recovery deadline expired")
                break
            claim_id = None
            clear = False
            try:
                with self._pending_claim_ids_lock:
                    pending = self._pending_claim_ids.get(processing_queue)
                    if not pending:
                        in_flight = self._in_flight_claim_ids.get(processing_queue)
                        if not in_flight:
                            return True, None
                    else:
                        recovering = self._recovering_claim_ids.setdefault(processing_queue, set())
                        candidate = next(
                            (cid for cid in pending if cid not in recovering and cid not in skipped_unresolved),
                            None,
                        )
                        if candidate is not None:
                            # Record the cleanup token (claim_id) BEFORE adding
                            # to the recovering set so an interrupt landing
                            # after the add still reaches the finally below
                            # (same hardening as begin_active_claim in
                            # _wait_for_claim).
                            claim_id = candidate
                            recovering.add(candidate)
                        elif all(cid in skipped_unresolved for cid in pending):
                            # Every remaining id already failed during this
                            # drain pass; another iteration cannot make
                            # progress, so surface the recorded failure.
                            break
                        # else: the remaining ids are mid-recovery by a
                        # concurrent consumer — fall through to the poll-wait
                        # below (same treatment as in-flight claims) instead
                        # of giving up before the deadline.
                if claim_id is None:
                    if deadline_monotonic is None:
                        await asyncio.sleep(_VISIBILITY_TIMEOUT_POLL_INTERVAL_SECONDS)
                    else:
                        remaining = deadline_monotonic - loop.time()
                        if remaining <= 0:
                            last_error = TimeoutError("drain pending-claim recovery deadline expired")
                            break
                        await asyncio.sleep(min(_VISIBILITY_TIMEOUT_POLL_INTERVAL_SECONDS, remaining))
                    continue
                try:
                    if has_visibility_timeout:
                        await self._recover_pending_visibility_timeout_claim(
                            processing_queue,
                            claim_id,
                            deadline_monotonic=deadline_monotonic,
                            rearm_lease=False,
                        )
                        clear = True
                        continue

                    recovered_claim = await self._recover_pending_non_visibility_timeout_claim(
                        processing_queue,
                        claim_id,
                        deadline_monotonic=deadline_monotonic,
                    )
                    if recovered_claim is None:
                        clear = True
                    elif await self._return_recovered_non_visibility_timeout_claim_to_pending(
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
                if claim_id is not None:
                    self._finish_pending_claim_recovery(processing_queue, claim_id, clear=clear)
        with self._pending_claim_ids_lock:
            pending = self._pending_claim_ids.get(processing_queue)
            in_flight = self._in_flight_claim_ids.get(processing_queue)
            drained = not pending and not in_flight
            return drained, None if drained else last_error
