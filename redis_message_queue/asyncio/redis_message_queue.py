import asyncio
import hashlib
import inspect
import logging
import math
import time
import uuid
from contextlib import asynccontextmanager
from typing import AsyncIterator, Awaitable, Callable, Literal, Optional, TypeVar

import redis.asyncio
import redis.exceptions

from redis_message_queue._config import (
    DEFAULT_PENDING_OVERLOAD_BLOCK_TIMEOUT_SECONDS,
    validate_dedup_configuration,
    validate_pending_backpressure_parameters,
)
from redis_message_queue._event import EventOperation, EventOutcome, QueueEvent
from redis_message_queue._exceptions import (
    CleanupFailedError,
    ConfigurationError,
    DrainFailedError,
    GatewayContractError,
    MalformedStoredMessageError,
    QueueDrainedError,
    RedisMessageQueueError,
    _set_exception_context,
)
from redis_message_queue._payload_limits import (
    serialize_dict_payload_with_limit,
    validate_max_payload_depth,
    validate_payload_limit_parameter,
    validate_str_payload_size,
    validate_str_payload_utf8_encodable,
)
from redis_message_queue._queue_key_manager import QueueKeyManager, validate_callable_deduplication_key
from redis_message_queue._queue_stats import QueueStats
from redis_message_queue._redis_cluster import (
    plain_redis_cluster_client_error,
    redis_info_reports_cluster_enabled,
    validate_queue_keys_for_redis_cluster,
)
from redis_message_queue._stored_message import (
    ClaimedMessage,
    PublishPayload,
    ReceivedPayload,
    decode_stored_message,
    extract_stored_message_id,
)
from redis_message_queue._warnings import warn_runtime_warning
from redis_message_queue.asyncio._abstract_redis_gateway import AbstractRedisGateway
from redis_message_queue.asyncio._redis_gateway import RedisGateway
from redis_message_queue.interrupt_handler import BaseGracefulInterruptHandler

logger = logging.getLogger(__name__)
_T = TypeVar("_T")
_GATEWAY_BOUND_PENDING_QUEUE_ATTR = "_rmq_bound_pending_queue"
_DEFAULT_VISIBILITY_TIMEOUT_SECONDS = 300
_DEFAULT_MAX_DELIVERY_COUNT = 10
_DEFAULT_MAX_COMPLETED_LENGTH = 1000
_DEFAULT_MAX_FAILED_LENGTH = 1000
_AUTO_DEAD_LETTER_QUEUE_SUFFIX = "dlq"
# Valid sources for peek() and targets for purge(). ``processing`` can be
# peeked but never purged: purging it would strand in-flight message leases.
_PEEK_SOURCES = ("pending", "processing", "completed", "failed", "dead_letter")
_PURGE_TARGETS = ("pending", "completed", "failed", "dead_letter")
# Bound each redrive Lua call so it never blocks Redis for long, mirroring the
# 100-message cap on the visibility-timeout reclaim/claim loops.
_REDRIVE_BATCH_SIZE = 100

_STALE_LEASE_ACK_WARNING = (
    "Message cleanup after successful processing was a no-op: "
    "the lease expired and the message was likely reclaimed by another consumer. "
    "This is expected at-least-once delivery behavior under visibility timeout."
)
_STALE_LEASE_NACK_WARNING = (
    "Message cleanup after failed processing was a no-op: "
    "the lease expired and the message was likely reclaimed by another consumer. "
    "This is expected at-least-once delivery behavior under visibility timeout."
)


def _duration_ms(started_at: float) -> float:
    return (time.perf_counter() - started_at) * 1000


def _hash_lease_token(lease_token: str | None) -> str | None:
    if lease_token is None:
        return None
    return hashlib.sha256(lease_token.encode("utf-8")).hexdigest()[:16]


def _warning_exception_name(exc: BaseException) -> str:
    if isinstance(exc, GatewayContractError):
        return "TypeError"
    return type(exc).__name__


def _warn_runtime_warning(message: str, *, stacklevel: int) -> None:
    warn_runtime_warning(message, stacklevel=stacklevel + 1)


def _find_non_string_dict_keys(value: object) -> list[object]:
    non_str_keys: list[object] = []
    seen: set[int] = set()

    stack = [value]
    while stack:
        current = stack.pop()
        if not isinstance(current, (dict, list, tuple)):
            continue
        current_id = id(current)
        if current_id in seen:
            continue
        seen.add(current_id)
        if isinstance(current, dict):
            children = []
            for key, child in current.items():
                if not isinstance(key, str):
                    non_str_keys.append(key)
                children.append(child)
            stack.extend(reversed(children))
        else:
            stack.extend(reversed(current))
    return non_str_keys


def _validate_strict_payload_types(value: object) -> None:
    seen: set[int] = set()

    def visit(current: object, path: str) -> None:
        current_type = type(current)
        if current is None or current_type in (bool, int, float, str):
            return
        if current_type is tuple:
            raise TypeError(
                f"strict_payload_types=True: value at {path} is a tuple; "
                "JSON does not preserve tuples (becomes list). "
                "Either convert to list explicitly or disable strict mode."
            )
        if current_type in (set, frozenset):
            raise TypeError(
                f"strict_payload_types=True: value at {path} is a {current_type.__name__}; "
                f"JSON does not support {current_type.__name__} values. "
                "Either convert to list explicitly or disable strict mode."
            )
        if current_type is list:
            current_id = id(current)
            if current_id in seen:
                return
            seen.add(current_id)
            for index, child in enumerate(current):
                visit(child, f"{path}[{index}]")
            return
        if current_type is dict:
            current_id = id(current)
            if current_id in seen:
                return
            seen.add(current_id)
            for key, child in current.items():
                visit(child, f"{path}[{key!r}]")
            return
        raise TypeError(
            f"strict_payload_types=True: value at {path} has type {current_type.__name__}; "
            f"JSON does not preserve {current_type.__name__}. "
            "Either convert to a JSON-native value explicitly or disable strict mode."
        )

    visit(value, "message")


class _TaskBaseException(Exception):
    def __init__(self, original: BaseException):
        super().__init__(str(original))
        self.original = original


async def _run_operation_in_task(operation: Awaitable[_T]) -> _T:
    try:
        return await operation
    except BaseException as exc:
        if isinstance(exc, Exception):
            raise
        raise _TaskBaseException(exc) from None


def _consume_task_exception(task: "asyncio.Task[_T]") -> None:
    if not task.cancelled():
        task.exception()


async def _await_preserving_cancellation(operation: Awaitable[_T]) -> _T:
    """Finish cleanup before propagating task cancellation."""

    task = asyncio.create_task(_run_operation_in_task(operation))
    task.add_done_callback(_consume_task_exception)
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        try:
            await asyncio.shield(task)
        except _TaskBaseException as exc:
            raise exc.original
        raise
    except _TaskBaseException as exc:
        raise exc.original


async def _await_suppressing_external_cancellation(
    operation: Awaitable[_T],
) -> "tuple[_T, asyncio.CancelledError | None]":
    """Finish cleanup before re-raising the original processing error.

    Suppresses a single external cancellation: if the caller cancels us during
    the await, retry once on a shielded task so the operation can finish and
    its result can be returned. On a second cancellation, the inner shield
    raises CancelledError that is not caught by the inner ``except`` (which
    only matches ``_TaskBaseException``) and therefore propagates out of this
    helper — there is no path that returns ``None`` under repeated cancel.
    The inner task keeps running to completion because it's a shielded
    ``create_task``, so no handle leaks. The caller handles any escaped
    CancelledError via ``except BaseException`` and re-raises the original
    processing error.

    Returns ``(result, swallowed_cancel)`` where ``swallowed_cancel`` is the
    first suppressed ``CancelledError`` (or ``None`` if the await completed
    without being cancelled). Callers that re-raise an original processing
    error should append the swallowed cancellation to the END of that error's
    ``__context__`` chain so stdlib timeout machinery can find it: on Python
    3.13+, ``asyncio.timeout`` walks the context chain and splices a
    ``TimeoutError`` ahead of the ``CancelledError`` it finds there. Without
    this, an ``asyncio.timeout`` deadline that fires here leaves no trace for
    the user to catch. The heartbeat-stop call site ignores the returned
    cancellation (nothing is re-raised on a successful stop). See
    ``process_message`` for the user-facing consequences.
    """

    task = asyncio.create_task(_run_operation_in_task(operation))
    task.add_done_callback(_consume_task_exception)
    try:
        return await asyncio.shield(task), None
    except asyncio.CancelledError as swallowed_cancel:
        try:
            return await asyncio.shield(task), swallowed_cancel
        except _TaskBaseException as exc:
            raise exc.original
    except _TaskBaseException as exc:
        raise exc.original


def _append_cancellation_to_context_chain(
    error: BaseException, swallowed_cancel: "asyncio.CancelledError | None"
) -> None:
    """Attach a suppressed cancellation at the end of ``error``'s context chain.

    Walks ``error.__context__`` links to the last node and sets the
    cancellation there. Never overwrites an existing link and never creates a
    cycle: if ``swallowed_cancel`` (or ``error`` itself) already appears
    anywhere in the chain, this is a no-op. This only ADDS information; it does
    not change which exception propagates.
    """

    if swallowed_cancel is None:
        return
    if swallowed_cancel is error:
        return
    seen: set[int] = {id(error)}
    node = error
    while node.__context__ is not None:
        node = node.__context__
        if node is swallowed_cancel:
            return  # already in the chain; do nothing
        if id(node) in seen:
            return  # pre-existing cycle; do not touch
        seen.add(id(node))
    node.__context__ = swallowed_cancel


def _validate_heartbeat_interval_seconds(
    heartbeat_interval_seconds: int | float | None,
    visibility_timeout_seconds: int | None,
    *,
    require_visibility_timeout_message: str | None = None,
) -> int | float | None:
    if heartbeat_interval_seconds is None:
        return None
    if not isinstance(heartbeat_interval_seconds, (int, float)) or isinstance(heartbeat_interval_seconds, bool):
        raise TypeError(
            f"'heartbeat_interval_seconds' must be a number or None, got {type(heartbeat_interval_seconds).__name__}"
        )
    if isinstance(heartbeat_interval_seconds, float) and not math.isfinite(heartbeat_interval_seconds):
        raise ConfigurationError(
            f"'heartbeat_interval_seconds' must be a finite number, got {heartbeat_interval_seconds}"
        )
    if heartbeat_interval_seconds <= 0:
        raise ConfigurationError(
            f"'heartbeat_interval_seconds' must be positive when provided, got {heartbeat_interval_seconds}"
        )
    if visibility_timeout_seconds is None:
        if require_visibility_timeout_message is None:
            require_visibility_timeout_message = (
                "'heartbeat_interval_seconds' requires a configured visibility timeout."
            )
        raise ConfigurationError(require_visibility_timeout_message)
    if heartbeat_interval_seconds >= visibility_timeout_seconds / 2:
        raise ConfigurationError(
            "'heartbeat_interval_seconds' must be less than half of 'visibility_timeout_seconds' "
            f"({heartbeat_interval_seconds} >= {visibility_timeout_seconds / 2})"
        )
    return heartbeat_interval_seconds


def _get_gateway_visibility_timeout_seconds(gateway: AbstractRedisGateway) -> int | None:
    visibility_timeout_seconds = _get_optional_gateway_visibility_timeout_seconds(gateway)
    if visibility_timeout_seconds is not None:
        return visibility_timeout_seconds
    if not hasattr(gateway, "message_visibility_timeout_seconds"):
        raise ConfigurationError(
            "'heartbeat_interval_seconds' requires the gateway to "
            "expose 'message_visibility_timeout_seconds' (configured to a positive int)."
        )
    return None


def _get_optional_gateway_visibility_timeout_seconds(gateway: AbstractRedisGateway) -> int | None:
    """Extract the visibility timeout from a custom gateway via duck-type check.

    ``message_visibility_timeout_seconds`` is not on the abstract interface because
    it is configuration, not protocol — non-lease gateways should not be forced to
    expose it. However, it IS required when ``heartbeat_interval_seconds`` is set,
    because the heartbeat interval must be validated against the visibility timeout.
    """
    if not hasattr(gateway, "message_visibility_timeout_seconds"):
        return None
    visibility_timeout_seconds = getattr(gateway, "message_visibility_timeout_seconds")
    if visibility_timeout_seconds is not None:
        if not isinstance(visibility_timeout_seconds, int) or isinstance(visibility_timeout_seconds, bool):
            raise GatewayContractError(
                "'gateway.message_visibility_timeout_seconds' must be an int or None, "
                f"got {type(visibility_timeout_seconds).__name__}. "
                "See AbstractRedisGateway docstring for the visibility-timeout contract."
            )
        if visibility_timeout_seconds <= 0:
            raise ConfigurationError(
                "'gateway.message_visibility_timeout_seconds' must be positive when provided, "
                f"got {visibility_timeout_seconds}"
            )
    return visibility_timeout_seconds


def _validate_cluster_configuration(
    key_manager: QueueKeyManager,
    *,
    client: redis.asyncio.Redis | None = None,
    gateway: AbstractRedisGateway | None = None,
    dead_letter_queue: str | None = None,
) -> bool:
    if client is not None:
        if isinstance(client, redis.asyncio.RedisCluster):
            validate_queue_keys_for_redis_cluster(key_manager, dead_letter_queue=dead_letter_queue)
            return False
        return type(client) is redis.asyncio.Redis
    if gateway is None or not gateway.is_redis_cluster:
        return False
    validate_queue_keys_for_redis_cluster(
        key_manager,
        dead_letter_queue=gateway.dead_letter_queue,
    )
    return False


async def _plain_redis_client_reports_cluster(client: redis.asyncio.Redis) -> bool:
    try:
        info = await client.info("cluster")
    except redis.exceptions.RedisError as exc:
        logger.warning(
            "Could not verify whether plain Redis client is connected to a Redis Cluster node; "
            "trusting the provided client: %s",
            exc,
        )
        return False
    return redis_info_reports_cluster_enabled(info)


def _derive_dead_letter_queue(name: str, key_separator: str) -> str:
    return f"{name}{key_separator}{_AUTO_DEAD_LETTER_QUEUE_SUFFIX}"


def _bind_dead_letter_gateway_to_queue(
    gateway: AbstractRedisGateway,
    queue_pending_key: str,
    queue_processing_key: str,
) -> None:
    """Validate and bind a dead-letter-enabled gateway to this queue.

    The check is not thread-safe: constructing ``RedisMessageQueue`` instances
    concurrently on multiple threads with the same DLQ-enabled gateway can
    race past this guardrail. Queue construction is expected to happen
    serially at application startup — if you need multiple queues sharing a
    DLQ-enabled gateway, construct them on a single thread.
    """

    max_delivery_count = gateway.max_delivery_count
    if max_delivery_count is None:
        return

    if gateway.dead_letter_queue in (queue_pending_key, queue_processing_key):
        raise ConfigurationError(
            "'dead_letter_queue' must be distinct from the queue's pending and processing Redis keys. "
            "Use a separate Redis list key for poison messages."
        )

    bound_pending_key = getattr(gateway, _GATEWAY_BOUND_PENDING_QUEUE_ATTR, None)
    if bound_pending_key is None:
        setattr(gateway, _GATEWAY_BOUND_PENDING_QUEUE_ATTR, queue_pending_key)
        return
    if bound_pending_key != queue_pending_key:
        raise ConfigurationError(
            "A gateway configured with 'max_delivery_count' cannot be reused across different queues. "
            "Create a separate gateway per queue when dead-letter routing is enabled."
        )


def _should_skip_message_cleanup(exc: BaseException) -> bool:
    """Return True for fatal shutdown exceptions that should not ack."""

    if isinstance(exc, (KeyboardInterrupt, SystemExit, GeneratorExit)):
        return True
    if isinstance(exc, asyncio.CancelledError):
        current_task = asyncio.current_task()
        return current_task is not None and current_task.cancelling() > 0
    return False


class _StopEventInterrupt(BaseGracefulInterruptHandler):
    """Adapt an ``asyncio.Event`` to the interrupt-handler protocol.

    Lets the gateway's tenacity retry predicate observe a heartbeat's stop
    signal so ``stop()`` can short-circuit an in-flight retry loop rather
    than waiting out ``retry_budget_seconds`` (AA-01-F2). Mirrors the sync
    variant in ``redis_message_queue.redis_message_queue``.
    """

    def __init__(self, stop_event: asyncio.Event) -> None:
        self._stop_event = stop_event

    def is_interrupted(self) -> bool:
        return self._stop_event.is_set()


class _DrainInterrupt(BaseGracefulInterruptHandler):
    def __init__(self, is_draining: Callable[[], bool]) -> None:
        self._is_draining = is_draining

    def is_interrupted(self) -> bool:
        return self._is_draining()


class _LeaseHeartbeat:
    def __init__(
        self,
        *,
        interval_seconds: float,
        renew_message_lease: Callable[[], Awaitable[bool]],
        on_heartbeat_failure: Callable[[], Awaitable[None] | None] | None = None,
        emit_event: Callable[..., Awaitable[None]] | None = None,
        message_id: str | None = None,
        lease_token_hash: str | None = None,
        stop_event: asyncio.Event | None = None,
    ):
        self._interval_seconds = interval_seconds
        self._renew_message_lease = renew_message_lease
        self._on_heartbeat_failure = on_heartbeat_failure
        self._emit_event = emit_event
        self._message_id = message_id
        self._lease_token_hash = lease_token_hash
        self._task: asyncio.Task | None = None
        # Accept an externally-constructed stop event so the queue can share
        # it with the renewal closure (which forwards it as ``is_interrupted``
        # to the gateway). Mirrors the sync heartbeat (AA-01-F2).
        self._stop_event = stop_event if stop_event is not None else asyncio.Event()
        self._suppress_failure_callback = asyncio.Event()

    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="redis-message-queue-lease-heartbeat")

    async def stop(self) -> None:
        if self._task is None:
            return
        self._stop_event.set()
        self._task.cancel()
        try:
            await asyncio.wait_for(
                asyncio.shield(self._task),
                timeout=max(self._interval_seconds * 2, 0.1),
            )
        except asyncio.CancelledError:
            current_task = asyncio.current_task()
            if current_task is not None and current_task.cancelling():
                raise
        except asyncio.TimeoutError:
            logger.warning(
                "Heartbeat task did not stop within timeout; "
                "it will exit on its own but may briefly renew a stale lease"
            )
            await self._emit(
                "heartbeat_stop_timeout",
                "failure",
                message_id=self._message_id,
                lease_token_hash=self._lease_token_hash,
            )
            # Guarded like every other heartbeat warn site: stop() runs in
            # process_message's finally after the ack committed, so a warning
            # promoted to an error by an app-level filter would misreport a
            # committed message as a failure.
            _warn_runtime_warning(
                "Heartbeat did not stop within timeout; it may briefly renew a stale lease before exiting",
                stacklevel=2,
            )

    async def _emit(self, operation: EventOperation | str, outcome: EventOutcome | str, **kwargs: object) -> None:
        if self._emit_event is not None:
            await self._emit_event(operation, outcome, **kwargs)

    def suppress_failure_callback(self) -> None:
        """Best-effort signal to suppress the next failure callback.

        The flag is checked before each callback invocation, but there is an
        inherent race: if the heartbeat task already crossed the check and is
        about to call the user callback when ``suppress_failure_callback()``
        runs, the callback still fires. Treat ``on_heartbeat_failure`` as
        advisory — it MAY be invoked after a successful ``process_message``
        exit when a final renewal coincided with the success path.
        """
        self._suppress_failure_callback.set()

    async def _invoke_failure_callback(self) -> None:
        if self._stop_event.is_set() or self._suppress_failure_callback.is_set() or self._on_heartbeat_failure is None:
            return
        try:
            result = self._on_heartbeat_failure()
            if inspect.isawaitable(result):
                await result
        except asyncio.CancelledError as exc:
            current_task = asyncio.current_task()
            if current_task is not None and current_task.cancelling() > 0:
                raise
            logger.exception("on_heartbeat_failure callback raised an exception")
            _warn_runtime_warning(
                f"on_heartbeat_failure callback raised {type(exc).__name__}",
                stacklevel=1,
            )
        except Exception as exc:
            logger.exception("on_heartbeat_failure callback raised an exception")
            _warn_runtime_warning(
                f"on_heartbeat_failure callback raised {type(exc).__name__}",
                stacklevel=1,
            )

    async def _report_renewal_failure(self, exc: BaseException) -> None:
        if self._stop_event.is_set():
            return
        logger.exception("Failed to renew message lease")
        await self._emit(
            "lease_renew_failed",
            "failure",
            message_id=self._message_id,
            lease_token_hash=self._lease_token_hash,
            exception_type=type(exc).__name__,
            error=exc,
        )
        _warn_runtime_warning(
            "Failed to renew message lease "
            f"({_warning_exception_name(exc)}); message will be reclaimed by another consumer "
            "when the visibility timeout expires",
            stacklevel=1,
        )
        await self._invoke_failure_callback()

    async def _run(self) -> None:
        try:
            while True:
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=self._interval_seconds)
                    return
                except asyncio.TimeoutError:
                    pass

                if self._stop_event.is_set():
                    return
                try:
                    renewed = await self._renew_message_lease()
                    if self._stop_event.is_set():
                        return
                    if not isinstance(renewed, bool):
                        raise GatewayContractError(
                            f"gateway.renew_message_lease() must return bool, got {type(renewed).__name__}. "
                            "See AbstractRedisGateway.renew_message_lease for the full contract."
                        )
                except asyncio.CancelledError as exc:
                    current_task = asyncio.current_task()
                    if self._stop_event.is_set() or (current_task is not None and current_task.cancelling() > 0):
                        raise
                    await self._report_renewal_failure(exc)
                    return
                except Exception as exc:
                    await self._report_renewal_failure(exc)
                    return
                if not renewed:
                    await self._emit(
                        "lease_renew_failed",
                        "skipped",
                        message_id=self._message_id,
                        lease_token_hash=self._lease_token_hash,
                    )
                    await self._invoke_failure_callback()
                    return
                await self._emit(
                    "lease_renew",
                    "success",
                    message_id=self._message_id,
                    lease_token_hash=self._lease_token_hash,
                )
        except asyncio.CancelledError:
            return


class RedisMessageQueue:
    """Async Redis-backed message queue.

    The queue object is not an async context manager and holds no queue-scope
    resource that requires teardown. Use ``async with q.process_message() as
    msg:`` for per-message processing. Synchronous callbacks are accepted where
    documented, but they run on the event loop and should be quick.
    """

    def __init__(
        self,
        name: str,
        *,
        gateway: Optional[AbstractRedisGateway] = None,
        client: Optional[redis.asyncio.Redis] = None,
        deduplication: bool = False,
        enable_completed_queue: bool = False,
        enable_failed_queue: bool = False,
        strict_envelope_decoding: bool = False,
        visibility_timeout_seconds: int | None = _DEFAULT_VISIBILITY_TIMEOUT_SECONDS,
        heartbeat_interval_seconds: int | float | None = None,
        max_completed_length: int | None = _DEFAULT_MAX_COMPLETED_LENGTH,
        max_failed_length: int | None = _DEFAULT_MAX_FAILED_LENGTH,
        max_delivery_count: int | None = _DEFAULT_MAX_DELIVERY_COUNT,
        max_pending_length: int | None = None,
        pending_overload_policy: Literal["raise", "drop_oldest", "block"] = "raise",
        pending_overload_block_timeout_seconds: float = DEFAULT_PENDING_OVERLOAD_BLOCK_TIMEOUT_SECONDS,
        key_separator: str = "::",
        get_deduplication_key: Optional[Callable[[PublishPayload], str | Awaitable[str]]] = None,
        strict_payload_types: bool = False,
        max_payload_bytes: int | None = None,
        max_payload_depth: int | None = None,
        interrupt: BaseGracefulInterruptHandler | None = None,
        on_heartbeat_failure: Callable[[], Awaitable[None] | None] | None = None,
        on_event: Callable[[QueueEvent], Awaitable[None]] | None = None,
    ):
        """Create a queue bound to an async Redis client or custom gateway.

        ``visibility_timeout_seconds`` defaults to 300. To disable
        lease-based crash recovery, set both ``visibility_timeout_seconds=None``
        and ``max_delivery_count=None``; messages left in ``processing`` by a
        crashed worker are then not reclaimed automatically.

        ``visibility_timeout_seconds`` is a Redis server-time lease, not a
        handler runtime limit. Long-running handlers are not interrupted; if the
        lease expires, another consumer can reclaim and process the same message
        concurrently. A forward step in the Redis server clock can make a live
        lease appear expired before that much real processing time has elapsed.

        ``max_delivery_count`` defaults to 10 on the built-in ``client=`` path.
        A message is delivered at most this many times; the claim that would
        exceed the count routes the message to the auto-derived dead-letter
        queue instead of redelivering it. Set it to ``None`` for unlimited
        redelivery.

        When ``gateway=`` is supplied, queue-level defaults are not transferred
        to the gateway. Configure lease, dead-letter, and backpressure settings
        such as ``message_visibility_timeout_seconds``, ``max_delivery_count``,
        and ``max_pending_length`` on the gateway itself.

        ``deduplication=True`` requires ``get_deduplication_key`` to be a
        callable that returns a non-empty string. Use a stable logical ID for
        the deduplication keyspace.

        Set ``strict_envelope_decoding=True`` if this Redis is shared with
        sibling task libraries (Celery, RQ, Dramatiq) to fail-fast on foreign
        payloads instead of yielding non-rmq bytes to handlers.

        ``strict_payload_types=True`` validates dict payload values before
        publish and rejects Python-only or lossy JSON types such as tuples,
        sets, bytes, and datetime objects with a path-aware ``TypeError``.
        The default ``False`` preserves the existing ``json.dumps`` behavior.

        ``max_payload_bytes`` and ``max_payload_depth`` default to ``None``
        (unbounded). Set positive integers to reject oversized serialized
        payloads or overly deep dict/list payload trees before enqueue.

        ``max_pending_length`` defaults to ``None`` (unbounded). Set it to a
        positive integer to cap pending-list depth during publish.

        ``pending_overload_policy`` controls publishes when the pending list is
        full: ``"raise"`` raises ``QueueBackpressureError``, ``"block"``
        waits until capacity is available or the block timeout expires, and
        ``"drop_oldest"`` evicts the oldest pending message before enqueueing
        the new one. ``"drop_oldest"`` requires ``max_pending_length`` and is
        not compatible with deduplication or ``max_delivery_count``.
        ``"block"`` also requires ``max_pending_length`` (the threshold to
        block on); only the default ``"raise"`` operates on an unbounded queue.

        ``pending_overload_block_timeout_seconds`` bounds how long ``"block"``
        waits for capacity before raising ``QueueBackpressureError``. ``0``
        performs a single immediate capacity check.

        ``key_separator`` only controls generated Redis key names; rmq has no
        fixed library prefix. Do not customize it to overlap another Redis
        task library's namespace, such as ``":queue:"`` with RQ-style keys.

        ``interrupt`` accepts a ``BaseGracefulInterruptHandler``; pass
        ``GracefulInterruptHandler()`` for prompt Ctrl-C / termination handling
        in polling waits. ``on_heartbeat_failure`` is a zero-argument callable
        or coroutine callable invoked when lease renewal fails. It runs on the
        heartbeat's background thread (sync queue) or on the event loop (async
        queue); in the async queue it MUST NOT block (no ``time.sleep``,
        blocking I/O, or CPU-heavy work; use ``await``), because a blocking
        callback freezes the event loop, delaying lease renewal for every other
        in-flight message (whose leases can then expire and be reclaimed) and
        stalling ``drain()``. Keep it quick and offload slow work yourself.
        ``on_event`` is telemetry only: a callable returning an awaitable and
        receiving
        best-effort QueueEvent lifecycle notifications. Callback failures are
        logged and converted to RuntimeWarning without influencing ack/nack or
        any other message outcome. Do not use it for correctness-critical
        callbacks or follow-up writes. ``on_event`` is awaited inline in the
        current asyncio task and may execute while an internal publish/drain
        lock is held, so the callback must not call back into the same queue
        instance's ``publish()`` or ``drain()``; that lock is
        non-reentrant, so re-entering deadlocks the caller permanently.
        """
        self.key = QueueKeyManager(name, key_separator=key_separator)
        if not isinstance(deduplication, bool):
            raise TypeError(
                f"'deduplication' must be a bool, got {type(deduplication).__name__} (use True or False, not 1/0)"
            )
        if not isinstance(enable_completed_queue, bool):
            raise TypeError(
                f"'enable_completed_queue' must be a bool, got {type(enable_completed_queue).__name__}"
                " (use True or False, not 1/0)"
            )
        if not isinstance(enable_failed_queue, bool):
            raise TypeError(
                f"'enable_failed_queue' must be a bool, got {type(enable_failed_queue).__name__}"
                " (use True or False, not 1/0)"
            )
        if not isinstance(strict_envelope_decoding, bool):
            raise TypeError(
                "'strict_envelope_decoding' must be a bool, "
                f"got {type(strict_envelope_decoding).__name__} (use True or False, not 1/0)"
            )
        if not isinstance(strict_payload_types, bool):
            raise TypeError(
                f"'strict_payload_types' must be a bool, got {type(strict_payload_types).__name__}"
                " (use True or False, not 1/0)"
            )
        max_payload_bytes = validate_payload_limit_parameter("max_payload_bytes", max_payload_bytes)
        max_payload_depth = validate_payload_limit_parameter("max_payload_depth", max_payload_depth)
        if max_completed_length is not None:
            if not isinstance(max_completed_length, int) or isinstance(max_completed_length, bool):
                bool_hint = " (use True or False, not 1/0)" if isinstance(max_completed_length, bool) else ""
                raise TypeError(
                    "'max_completed_length' must be an int or None, "
                    f"got {type(max_completed_length).__name__}{bool_hint}"
                )
            if max_completed_length <= 0:
                raise ConfigurationError(
                    f"'max_completed_length' must be positive when provided, got {max_completed_length}"
                )
            if not enable_completed_queue and max_completed_length != _DEFAULT_MAX_COMPLETED_LENGTH:
                raise ConfigurationError("'max_completed_length' requires 'enable_completed_queue=True'.")
        if max_failed_length is not None:
            if not isinstance(max_failed_length, int) or isinstance(max_failed_length, bool):
                bool_hint = " (use True or False, not 1/0)" if isinstance(max_failed_length, bool) else ""
                raise TypeError(
                    f"'max_failed_length' must be an int or None, got {type(max_failed_length).__name__}{bool_hint}"
                )
            if max_failed_length <= 0:
                raise ConfigurationError(f"'max_failed_length' must be positive when provided, got {max_failed_length}")
            if not enable_failed_queue and max_failed_length != _DEFAULT_MAX_FAILED_LENGTH:
                raise ConfigurationError("'max_failed_length' requires 'enable_failed_queue=True'.")
        if max_delivery_count is not None:
            if not isinstance(max_delivery_count, int) or isinstance(max_delivery_count, bool):
                bool_hint = " (use True or False, not 1/0)" if isinstance(max_delivery_count, bool) else ""
                raise TypeError(
                    f"'max_delivery_count' must be an int or None, got {type(max_delivery_count).__name__}{bool_hint}"
                )
            if max_delivery_count <= 0:
                raise ConfigurationError(
                    f"'max_delivery_count' must be positive when provided, got {max_delivery_count}"
                )
        if visibility_timeout_seconds is not None:
            if not isinstance(visibility_timeout_seconds, int) or isinstance(visibility_timeout_seconds, bool):
                bool_hint = " (use True or False, not 1/0)" if isinstance(visibility_timeout_seconds, bool) else ""
                raise TypeError(
                    "'visibility_timeout_seconds' must be an int or None, "
                    f"got {type(visibility_timeout_seconds).__name__}{bool_hint}"
                )
            if visibility_timeout_seconds <= 0:
                raise ConfigurationError(
                    f"'visibility_timeout_seconds' must be positive when provided, got {visibility_timeout_seconds}"
                )
        get_deduplication_key_was_configured = get_deduplication_key is not None
        if get_deduplication_key is not None and not callable(get_deduplication_key):
            raise TypeError(
                f"'get_deduplication_key' must be callable, got {type(get_deduplication_key).__name__}."
                " Expected a function that takes the message (PublishPayload) and returns a str"
                " (or an awaitable thereof)."
                " Example: get_deduplication_key=lambda msg: msg['user_id']"
            )
        validate_dedup_configuration(
            deduplication=deduplication,
            get_deduplication_key=get_deduplication_key,
        )
        if gateway is not None:
            # Before the generic validator so gateway-incompat wins over the drop_oldest runaround; non-default only.
            if pending_overload_policy != "raise":
                raise ConfigurationError(
                    "'pending_overload_policy' cannot be provided alongside 'gateway'."
                    " Configure publish backpressure on the gateway directly instead."
                )
            if pending_overload_block_timeout_seconds != DEFAULT_PENDING_OVERLOAD_BLOCK_TIMEOUT_SECONDS:
                raise ConfigurationError(
                    "'pending_overload_block_timeout_seconds' cannot be provided alongside 'gateway'."
                    " Configure publish backpressure on the gateway directly instead."
                )
        validate_pending_backpressure_parameters(
            max_pending_length,
            pending_overload_policy,
            pending_overload_block_timeout_seconds,
            deduplication=deduplication,
            get_deduplication_key_configured=get_deduplication_key_was_configured,
            max_delivery_count=max_delivery_count,
        )
        if not deduplication and get_deduplication_key_was_configured:
            raise ConfigurationError("'get_deduplication_key' cannot be provided when 'deduplication' is disabled.")
        if on_heartbeat_failure is not None and not callable(on_heartbeat_failure):
            raise TypeError(
                f"'on_heartbeat_failure' must be callable, got {type(on_heartbeat_failure).__name__}."
                " Expected a zero-arg function (or coroutine function) invoked when lease renewal fails."
            )
        if on_event is not None and not callable(on_event):
            raise TypeError(f"'on_event' must be callable, got {type(on_event).__name__}.")
        self._queue_name = name
        self._on_event = on_event
        # Queue-local soft-drain flag. See sync queue ``_draining`` docstring
        # (AA-05-F1/F2, AC-03). Distinct from the gateway-level ``interrupt``
        # handler; set via ``drain()`` instead of an
        # asyncio.Event because reads are already atomic and the drain path
        # owns the only writer.
        self._draining = False
        self._drained = False
        self._publish_lock = asyncio.Lock()
        self._drain_lock = asyncio.Lock()
        self._cluster_validation_lock = asyncio.Lock()
        self._drain_result: bool | None = None
        self._deduplication = deduplication
        self._enable_completed_queue = enable_completed_queue
        self._enable_failed_queue = enable_failed_queue
        self._strict_envelope_decoding = strict_envelope_decoding
        self._max_completed_length = max_completed_length
        self._max_failed_length = max_failed_length
        self._max_delivery_count = max_delivery_count
        self._get_deduplication_key = get_deduplication_key
        self._strict_payload_types = strict_payload_types
        self._max_payload_bytes = max_payload_bytes
        self._max_payload_depth = max_payload_depth
        self._heartbeat_interval_seconds = None
        self._warned_no_lease_for_heartbeat = False
        self._requires_claimed_message = False
        self._plain_redis_cluster_probe_client: redis.asyncio.Redis | None = None

        if gateway is not None:
            visibility_timeout_was_configured = visibility_timeout_seconds not in (
                None,
                _DEFAULT_VISIBILITY_TIMEOUT_SECONDS,
            )
            if client is not None or interrupt is not None or visibility_timeout_was_configured:
                raise ConfigurationError(
                    "'gateway' cannot be provided alongside 'client', 'interrupt', or 'visibility_timeout_seconds'."
                    " Configure the gateway directly instead."
                )
            if not isinstance(gateway, AbstractRedisGateway):
                try:
                    from redis_message_queue import AbstractRedisGateway as _SyncAbstractRedisGateway
                except ImportError:
                    _SyncAbstractRedisGateway = ()  # type: ignore[assignment]
                if isinstance(gateway, _SyncAbstractRedisGateway):
                    raise GatewayContractError(
                        "'gateway' is a sync AbstractRedisGateway; "
                        "use the sync RedisMessageQueue from redis_message_queue instead"
                    )
                raise GatewayContractError(
                    "'gateway' must be an AbstractRedisGateway"
                    " (subclass redis_message_queue.asyncio.AbstractRedisGateway),"
                    f" got {type(gateway).__name__}"
                )
            gateway_visibility_timeout_seconds = _get_optional_gateway_visibility_timeout_seconds(gateway)
            self._requires_claimed_message = gateway_visibility_timeout_seconds is not None
            _validate_cluster_configuration(self.key, gateway=gateway)
            if heartbeat_interval_seconds is not None:
                gateway_visibility_timeout_seconds = _get_gateway_visibility_timeout_seconds(gateway)
                self._heartbeat_interval_seconds = _validate_heartbeat_interval_seconds(
                    heartbeat_interval_seconds,
                    gateway_visibility_timeout_seconds,
                    require_visibility_timeout_message=(
                        "'heartbeat_interval_seconds' with 'gateway' requires a gateway with"
                        " a configured visibility timeout."
                    ),
                )
            max_delivery_count_was_configured = max_delivery_count not in (
                None,
                _DEFAULT_MAX_DELIVERY_COUNT,
            )
            if max_delivery_count_was_configured:
                raise ConfigurationError(
                    "'max_delivery_count' cannot be provided alongside 'gateway'."
                    " Configure 'max_delivery_count' and 'dead_letter_queue' on the gateway directly instead."
                )
            if max_pending_length is not None:
                raise ConfigurationError(
                    "'max_pending_length' cannot be provided alongside 'gateway'."
                    " Configure publish backpressure on the gateway directly instead."
                )
            _bind_dead_letter_gateway_to_queue(gateway, self.key.pending, self.key.processing)
            self._max_delivery_count = None
            self._redis = gateway
        elif client is None:
            raise ConfigurationError("Either 'client' or 'gateway' must be provided.")
        else:
            dead_letter_queue = (
                _derive_dead_letter_queue(name, key_separator) if max_delivery_count is not None else None
            )
            if _validate_cluster_configuration(self.key, client=client, dead_letter_queue=dead_letter_queue):
                self._plain_redis_cluster_probe_client = client
            self._heartbeat_interval_seconds = _validate_heartbeat_interval_seconds(
                heartbeat_interval_seconds,
                visibility_timeout_seconds,
                require_visibility_timeout_message=(
                    "'heartbeat_interval_seconds' requires 'visibility_timeout_seconds'"
                    " when using the built-in client path."
                ),
            )
            if max_delivery_count is not None and visibility_timeout_seconds is None:
                raise ConfigurationError("'max_delivery_count' requires 'visibility_timeout_seconds' to be set.")
            self._requires_claimed_message = visibility_timeout_seconds is not None
            self._redis = RedisGateway(
                redis_client=client,
                interrupt=interrupt,
                message_visibility_timeout_seconds=visibility_timeout_seconds,
                max_delivery_count=max_delivery_count,
                dead_letter_queue=dead_letter_queue,
                max_pending_length=max_pending_length,
                pending_overload_policy=pending_overload_policy,
                pending_overload_block_timeout_seconds=pending_overload_block_timeout_seconds,
            )

        if on_heartbeat_failure is not None and self._heartbeat_interval_seconds is None:
            raise ConfigurationError("'on_heartbeat_failure' requires 'heartbeat_interval_seconds' to be set.")
        self._on_heartbeat_failure = on_heartbeat_failure
        set_event_emitter = getattr(self._redis, "_set_event_emitter", None)
        if callable(set_event_emitter):
            set_event_emitter(self.key.processing, self._queue_name, self._emit_event)

    async def _emit_event(
        self,
        operation: EventOperation | str,
        outcome: EventOutcome | str,
        *,
        message_id: str | None = None,
        claim_id: str | None = None,
        lease_token_hash: str | None = None,
        destination_queue: str | None = None,
        delivery_count: int | None = None,
        max_delivery_count: int | None = None,
        exception_type: str | None = None,
        error: BaseException | None = None,
        duration_ms: float | None = None,
        timeout_seconds: float | None = None,
        pending_claim_ids: int | None = None,
    ) -> None:
        if self._on_event is None:
            return
        event = QueueEvent(
            queue=self._queue_name,
            operation=EventOperation(operation),
            outcome=EventOutcome(outcome),
            message_id=message_id,
            claim_id=claim_id,
            lease_token_hash=lease_token_hash,
            destination_queue=destination_queue,
            delivery_count=delivery_count,
            max_delivery_count=max_delivery_count,
            exception_type=exception_type,
            error=error,
            duration_ms=duration_ms,
            timeout_seconds=timeout_seconds,
            pending_claim_ids=pending_claim_ids,
        )
        try:
            result = self._on_event(event)
            if not inspect.isawaitable(result):
                raise TypeError(
                    f"'on_event' must return an awaitable, got {type(result).__name__}. "
                    "Use an async callable or return an awaitable."
                )
            await result
        except asyncio.CancelledError as exc:
            current_task = asyncio.current_task()
            if current_task is not None and current_task.cancelling() > 0:
                raise
            logger.exception("on_event callback raised an exception")
            _warn_runtime_warning(
                f"on_event callback raised {type(exc).__name__}",
                stacklevel=2,
            )
        except Exception as exc:
            logger.exception("on_event callback raised an exception")
            _warn_runtime_warning(
                f"on_event callback raised {type(exc).__name__}",
                stacklevel=2,
            )

    def _pending_claim_ids_count(self) -> int | None:
        pending_claim_ids = getattr(self._redis, "_pending_claim_ids", None)
        if not isinstance(pending_claim_ids, dict):
            return None
        in_flight_claim_ids = getattr(self._redis, "_in_flight_claim_ids", None)

        lock = getattr(self._redis, "_pending_claim_ids_lock", None)
        if lock is None:
            pending = pending_claim_ids.get(self.key.processing)
            in_flight = in_flight_claim_ids.get(self.key.processing) if isinstance(in_flight_claim_ids, dict) else None
            pending_count = len(pending) if pending is not None else 0
            in_flight_count = len(in_flight) if in_flight is not None else 0
            return pending_count + in_flight_count

        with lock:
            pending = pending_claim_ids.get(self.key.processing)
            in_flight = in_flight_claim_ids.get(self.key.processing) if isinstance(in_flight_claim_ids, dict) else None
            pending_count = len(pending) if pending is not None else 0
            in_flight_count = len(in_flight) if in_flight is not None else 0
            return pending_count + in_flight_count

    def _drain_failure_error(
        self,
        timeout_seconds: float | None,
        pending_claim_ids: int | None,
        drain_error: BaseException | None,
    ) -> BaseException:
        if isinstance(drain_error, BaseException):
            return self._wrap_drain_failure_error(drain_error)

        pending_count = "unknown" if pending_claim_ids is None else str(pending_claim_ids)
        if timeout_seconds is not None:
            return self._wrap_drain_failure_error(
                TimeoutError(f"drain left {pending_count} pending claim id(s) before the timeout")
            )
        return self._wrap_drain_failure_error(RuntimeError(f"drain left {pending_count} pending claim id(s)"))

    def _wrap_drain_failure_error(self, raw_error: BaseException) -> BaseException:
        if isinstance(raw_error, RedisMessageQueueError):
            _set_exception_context(raw_error, queue=self._queue_name, operation="drain")
            return raw_error

        wrapped_error = DrainFailedError(
            str(raw_error),
            queue=self._queue_name,
            operation="drain",
        )
        wrapped_error.__cause__ = raw_error
        return wrapped_error

    async def _ensure_plain_redis_client_is_not_cluster(self) -> None:
        client = self._plain_redis_cluster_probe_client
        if client is None:
            return
        async with self._cluster_validation_lock:
            client = self._plain_redis_cluster_probe_client
            if client is None:
                return
            if await _plain_redis_client_reports_cluster(client):
                raise plain_redis_cluster_client_error(type(client).__name__)
            self._plain_redis_cluster_probe_client = None

    async def publish(self, message: PublishPayload) -> bool:
        """Publish a message.

        Dict messages are serialized via ``json.dumps(message, sort_keys=True)``.
        All dict keys must be strings; non-string keys raise
        ``TypeError`` to avoid silent ``json.dumps`` coercion that would
        collapse distinct keys into the same dedup key (e.g. ``{1: "x"}``
        vs ``{"1": "x"}``).

        Dict payloads are JSON-encoded data, not Python object serialization.
        JSON does not preserve every Python type: by default tuples become
        lists, raw set values raise unless converted to lists before publish,
        and custom objects raise. Use ``strict_payload_types=True`` to reject
        Python-only or lossy types before enqueue with a path-aware
        ``TypeError``. Plan dict payload schemas in JSON-native types only.

        Str messages must be UTF-8-encodable: a value containing a lone
        surrogate (a common artifact of ``surrogateescape`` decoding or
        ``os.fsdecode``) raises ``ValueError`` before anything is enqueued,
        because it would fail every downstream decode after storage.

        Deduplication and publish retry-safety markers are Redis TTL keys. A
        large forward step in Redis server expiration time during a retry
        window can expire those markers before the Python-side monotonic retry
        budget elapses, allowing a duplicate publish under that extreme
        anomaly.

        Payload validation (key/type/depth checks) and ``json.dumps`` run
        synchronously on the event loop while holding the publish lock — they
        are not offloaded to a thread. A large payload stalls the event loop
        for the duration of that synchronous work, delaying every other
        coroutine on the loop including heartbeat renewals. Set
        ``max_payload_bytes`` and keep payloads modest for async publishers.
        """
        async with self._publish_lock:
            if self._drained:
                started_at = time.perf_counter()
                drained_error = QueueDrainedError("queue is drained", queue=self._queue_name, operation="drain")
                # The drained refusal is a caller-visible publish failure;
                # publish/failure events follow exceptions (docs/observability.md),
                # so this path must not be silent while the drain-aborted
                # blocked publish next to it is evented.
                await self._emit_event(
                    "publish",
                    "failure",
                    exception_type=type(drained_error).__name__,
                    error=drained_error,
                    duration_ms=_duration_ms(started_at),
                )
                raise drained_error
            return await self._publish_unlocked(message)

    async def _publish_unlocked(self, message: PublishPayload) -> bool:
        started_at = time.perf_counter()
        try:
            await self._ensure_plain_redis_client_is_not_cluster()
            if not isinstance(message, (str, dict)):
                raise TypeError(f"'message' must be a str or dict, got {type(message).__name__}")
            if isinstance(message, dict):
                non_str_keys = _find_non_string_dict_keys(message)
                if non_str_keys:
                    raise TypeError(
                        "'message' dict keys must all be strings; "
                        f"got non-string keys: {non_str_keys[:3]}" + (" (and more)" if len(non_str_keys) > 3 else "")
                    )
                if self._strict_payload_types:
                    _validate_strict_payload_types(message)
                validate_max_payload_depth(message, self._max_payload_depth)
                message_str = serialize_dict_payload_with_limit(message, self._max_payload_bytes)
            else:
                validate_str_payload_utf8_encodable(message)
                validate_str_payload_size(message, self._max_payload_bytes)
                message_str = message

            if not self._deduplication:
                result = await self._add_message(message_str)
                if result is not None:
                    raise GatewayContractError(
                        f"gateway.add_message() must return None, got {type(result).__name__}. "
                        "See AbstractRedisGateway.add_message for the full contract."
                    )
            else:
                try:
                    dedup_key = self._get_deduplication_key(message)
                    if inspect.isawaitable(dedup_key):
                        dedup_key = await dedup_key
                except asyncio.CancelledError as exc:
                    current_task = asyncio.current_task()
                    if current_task is not None and current_task.cancelling() > 0:
                        raise
                    _set_exception_context(exc, queue=self._queue_name, operation="publish")
                    await self._emit_event(
                        "publish",
                        "failure",
                        exception_type=type(exc).__name__,
                        error=exc,
                        duration_ms=_duration_ms(started_at),
                    )
                    raise
                dedup_key = validate_callable_deduplication_key(dedup_key, message)
                dedup_key = self.key.deduplication(dedup_key)
                result = await self._publish_message(message_str, dedup_key)
                if not isinstance(result, bool):
                    raise GatewayContractError(
                        f"gateway.publish_message() must return bool, got {type(result).__name__}. "
                        "See AbstractRedisGateway.publish_message for the full contract."
                    )
        except Exception as exc:
            _set_exception_context(exc, queue=self._queue_name, operation="publish")
            await self._emit_event(
                "publish",
                "failure",
                exception_type=type(exc).__name__,
                error=exc,
                duration_ms=_duration_ms(started_at),
            )
            raise

        if not self._deduplication:
            await self._emit_event("publish", "success", duration_ms=_duration_ms(started_at))
            return True
        await self._emit_event(
            "publish" if result else "publish_dedup_hit",
            "success" if result else "skipped",
            duration_ms=_duration_ms(started_at),
        )
        return result

    @asynccontextmanager
    async def process_message(self) -> AsyncIterator[Optional[ReceivedPayload]]:
        """Claim and process one message.

        Yields ``str`` if your client uses ``decode_responses=True``, else
        ``bytes``. Match the client setting to the type your handler expects.

        Important: exceptions raised inside the ``async with`` block are
        terminal. rmq is a payload queue, not a task framework; handler
        exceptions do not requeue the message. With
        ``enable_failed_queue=False``, the message is removed from
        ``processing``; with ``enable_failed_queue=True``, it is moved to the
        failed list.

        If the task is cancelled after a message is claimed and cleanup cannot
        run, the claimed message and lease metadata remain in Redis until a
        later consumer claim triggers visibility-timeout reclaim. With
        visibility timeouts enabled, this is at-least-once recovery semantics:
        the message is delayed by the lease, not lost. Use ``drain()`` for an
        explicit async drain path during shutdown.

        Cancellation observability on the failure path: when a handler raises,
        a cancellation or ``asyncio.timeout`` deadline that lands during the
        failure-path cleanup is suppressed so the cleanup finishes and the
        handler exception propagates. Message state stays correct. The
        suppressed cancellation is preserved at the end of the raised handler
        exception's ``__context__`` chain so the deadline stays discoverable:

        - An expired ``asyncio.timeout`` surfaces as the handler error (not
          ``TimeoutError``), with the cancellation kept in its context chain.
          On Python 3.13+, ``asyncio.timeout`` splices a ``TimeoutError`` ahead
          of that preserved ``CancelledError``; on Python 3.12 the
          ``CancelledError`` itself is what you find in the chain.
        - On Python 3.12, a deadline that expires mid-ack whose ack also fails
          is not discoverable from the raised ``CleanupFailedError`` (Python
          3.13+ chains a ``TimeoutError`` into its context).
        - A task cancelled during failure-path cleanup completes with the
          handler exception, not as ``cancelled()``; do not gate shutdown logic
          on ``task.cancelled()`` here.

        See docs/configuration.md "Cancellation observability on the async
        failure path" for details and mitigations.
        """
        claim_started_at = time.perf_counter()
        if self._draining:
            await self._emit_event("claim_empty", "skipped", duration_ms=_duration_ms(claim_started_at))
            # Yield to the event loop before returning ``None``. When ``on_event``
            # is unset ``_emit_event`` returns without ever suspending, so a
            # post-drain ``while True: async with process_message() ...`` consume
            # loop would otherwise spin without a single suspension point,
            # starving sibling tasks (including the ``await worker_task`` shutdown
            # sequence). ``asyncio.sleep(0)`` is a real yield to the scheduler.
            await asyncio.sleep(0)
            yield None
            return
        try:
            await self._ensure_plain_redis_client_is_not_cluster()
            claimed_message = await self._wait_for_message_and_move()
            if claimed_message is not None:
                if not isinstance(claimed_message, (ClaimedMessage, str, bytes)):
                    raise GatewayContractError(
                        f"gateway.wait_for_message_and_move() must return ClaimedMessage, str, bytes, or None; "
                        f"got {type(claimed_message).__name__}. "
                        "See AbstractRedisGateway.wait_for_message_and_move for the full contract."
                    )

                stored_message = claimed_message
                lease_token = None
                if isinstance(claimed_message, ClaimedMessage):
                    stored_message = claimed_message.stored_message
                    lease_token = claimed_message.lease_token
                lease_token_hash = _hash_lease_token(lease_token)

                if lease_token is None and self._heartbeat_interval_seconds is not None:
                    if not self._warned_no_lease_for_heartbeat:
                        self._warned_no_lease_for_heartbeat = True
                        no_lease_token_warning = (
                            "Heartbeat is configured but the gateway returned no lease token. "
                            "The heartbeat will not run for this message. Ensure the gateway "
                            "returns ClaimedMessage from wait_for_message_and_move() when "
                            "visibility timeouts are in use."
                        )
                        logger.warning(no_lease_token_warning)
                        _warn_runtime_warning(no_lease_token_warning, stacklevel=2)

                if lease_token is None and self._requires_claimed_message:
                    raise GatewayContractError(
                        "gateways with visibility timeouts must return ClaimedMessage from "
                        "wait_for_message_and_move(); got plain ReceivedPayload without a lease token"
                    )
        except Exception as exc:
            _set_exception_context(exc, queue=self._queue_name, operation="claim")
            await self._emit_event(
                "claim",
                "failure",
                exception_type=type(exc).__name__,
                error=exc,
                duration_ms=_duration_ms(claim_started_at),
            )
            raise
        if claimed_message is None:
            await self._emit_event("claim_empty", "skipped", duration_ms=_duration_ms(claim_started_at))
            yield None
            return

        message_id = None
        try:
            message_id = extract_stored_message_id(
                stored_message,
                strict_envelope_decoding=self._strict_envelope_decoding,
            )
            message = decode_stored_message(
                stored_message,
                strict_envelope_decoding=self._strict_envelope_decoding,
            )
        except MalformedStoredMessageError as exc:
            _set_exception_context(exc, queue=self._queue_name, message_id=message_id, operation="claim")
            await self._emit_event(
                "claim",
                "failure",
                message_id=message_id,
                lease_token_hash=lease_token_hash,
                exception_type=type(exc).__name__,
                error=exc,
                duration_ms=_duration_ms(claim_started_at),
            )
            raise
        await self._emit_event(
            "claim",
            "success",
            message_id=message_id,
            lease_token_hash=lease_token_hash,
            duration_ms=_duration_ms(claim_started_at),
        )

        lease_heartbeat = self._build_lease_heartbeat(stored_message, lease_token, message_id, lease_token_hash)
        finished_without_error = False
        processing_started_at = time.perf_counter()
        heartbeat_start_failed = False
        try:
            # start() must be inside this try so its finally always stop()s the
            # heartbeat; an exception in the start()->yield window would
            # otherwise orphan it.
            if lease_heartbeat is not None:
                try:
                    lease_heartbeat.start()
                except BaseException:
                    # A start() failure (e.g. RuntimeError from create_task on
                    # a closing loop) fires before any handler code ran;
                    # nacking would destroy a message no handler ever saw. Flag
                    # it so the except below skips cleanup and the claim stays
                    # in ``processing`` for visibility-timeout reclaim,
                    # matching the fatal-signal path.
                    heartbeat_start_failed = True
                    raise
            yield message  # type: ignore
        except BaseException as exc:
            skip_cleanup = heartbeat_start_failed or _should_skip_message_cleanup(exc)
            if lease_heartbeat is not None:
                lease_heartbeat.suppress_failure_callback()
            if skip_cleanup:
                raise
            await self._emit_event(
                "failed",
                "failure",
                message_id=message_id,
                lease_token_hash=lease_token_hash,
                destination_queue=self.key.failed if self._enable_failed_queue else None,
                exception_type=type(exc).__name__,
                error=exc,
                duration_ms=_duration_ms(processing_started_at),
            )
            cleanup_started_at = time.perf_counter()
            cleanup_op_succeeded = False
            swallowed_cancel: "asyncio.CancelledError | None" = None
            try:
                if self._enable_failed_queue:
                    applied, swallowed_cancel = await _await_suppressing_external_cancellation(
                        self._move_processed_message(self.key.failed, stored_message, lease_token)
                    )
                else:
                    applied, swallowed_cancel = await _await_suppressing_external_cancellation(
                        self._remove_processed_message(stored_message, lease_token)
                    )
                # The Redis cleanup committed here; an exception in the emits
                # below must not be misreported as a cleanup failure.
                cleanup_op_succeeded = True
                await self._emit_event(
                    "nack",
                    "success" if applied else "skipped",
                    message_id=message_id,
                    lease_token_hash=lease_token_hash,
                    duration_ms=_duration_ms(cleanup_started_at),
                )
                if lease_token is not None and not applied:
                    logger.warning(_STALE_LEASE_NACK_WARNING)
                    await self._emit_event(
                        "stale_lease_nack",
                        "skipped",
                        message_id=message_id,
                        lease_token_hash=lease_token_hash,
                    )
                    _warn_runtime_warning(_STALE_LEASE_NACK_WARNING, stacklevel=2)
            except BaseException as cleanup_exc:
                # Only a failure of the cleanup op itself is a cleanup failure.
                # If the op committed (cleanup_op_succeeded) the exception came
                # from a post-cleanup emit (e.g. a cancel or fatal signal landing
                # in on_event); skip the spurious cleanup_failed event but keep
                # the same propagation as before (the handler error re-raises at
                # the trailing ``raise``; a fatal signal still propagates).
                if not cleanup_op_succeeded:
                    # The handler exception is the user-visible failure; cleanup failure is secondary.
                    logger.exception("Failed to clean up message from processing queue")
                    await self._emit_event(
                        "cleanup_failed",
                        "failure",
                        message_id=message_id,
                        lease_token_hash=lease_token_hash,
                        exception_type=type(cleanup_exc).__name__,
                        error=cleanup_exc,
                        duration_ms=_duration_ms(cleanup_started_at),
                    )
                # Fatal shutdown signals must propagate (handler exception chained as
                # __context__) so a Ctrl-C is not swallowed; the message stays in
                # processing for visibility-timeout reclaim. Deliberately not
                # _should_skip_message_cleanup() here: that helper also matches a
                # second-cancellation CancelledError escaping
                # _await_suppressing_external_cancellation, which this block must
                # keep swallowing so the original processing error re-raises.
                if isinstance(cleanup_exc, (KeyboardInterrupt, SystemExit, GeneratorExit)):
                    raise
                if not cleanup_op_succeeded:
                    _warn_runtime_warning(
                        f"Cleanup raised after handler exception ({_warning_exception_name(cleanup_exc)}); "
                        "see logs for both tracebacks",
                        stacklevel=2,
                    )
            # Preserve a cancellation suppressed during cleanup so it rides the
            # re-raised handler error's context chain. On Python 3.13+ this lets
            # an ``asyncio.timeout`` deadline that fired here splice a
            # ``TimeoutError`` into the chain; on 3.12 it leaves the
            # ``CancelledError`` discoverable. Append-only: nothing is overwritten.
            _append_cancellation_to_context_chain(exc, swallowed_cancel)
            raise
        else:
            if lease_heartbeat is not None:
                lease_heartbeat.suppress_failure_callback()
            cleanup_started_at = time.perf_counter()
            if self._enable_completed_queue:
                cleanup_operation = self._move_processed_message(self.key.completed, stored_message, lease_token)
            else:
                cleanup_operation = self._remove_processed_message(stored_message, lease_token)
            try:
                applied = await _await_preserving_cancellation(cleanup_operation)
            except Exception as cleanup_exc:
                _set_exception_context(
                    cleanup_exc,
                    queue=self._queue_name,
                    message_id=message_id,
                    operation="ack",
                )
                await self._emit_event(
                    "cleanup_failed",
                    "failure",
                    message_id=message_id,
                    lease_token_hash=lease_token_hash,
                    exception_type=type(cleanup_exc).__name__,
                    error=cleanup_exc,
                    duration_ms=_duration_ms(cleanup_started_at),
                )
                raise CleanupFailedError(
                    "Cleanup after successful processing failed",
                    queue=self._queue_name,
                    message_id=message_id,
                    operation="cleanup",
                ) from cleanup_exc
            if self._enable_completed_queue:
                await self._emit_event(
                    "completed",
                    "success" if applied else "skipped",
                    message_id=message_id,
                    lease_token_hash=lease_token_hash,
                    destination_queue=self.key.completed,
                    duration_ms=_duration_ms(processing_started_at),
                )
            await self._emit_event(
                "ack",
                "success" if applied else "skipped",
                message_id=message_id,
                lease_token_hash=lease_token_hash,
                duration_ms=_duration_ms(cleanup_started_at),
            )
            if lease_token is not None and not applied:
                logger.warning(_STALE_LEASE_ACK_WARNING)
                await self._emit_event(
                    "stale_lease_ack",
                    "skipped",
                    message_id=message_id,
                    lease_token_hash=lease_token_hash,
                )
                _warn_runtime_warning(_STALE_LEASE_ACK_WARNING, stacklevel=2)
            finished_without_error = True
        finally:
            if lease_heartbeat is not None:
                if finished_without_error:
                    await _await_preserving_cancellation(lease_heartbeat.stop())
                else:
                    # Stopping the heartbeat raises nothing on success, so the
                    # swallowed cancellation has no error to ride; discard it.
                    await _await_suppressing_external_cancellation(lease_heartbeat.stop())

    async def process_message_callback(
        self,
        handler: Callable[[ReceivedPayload], Awaitable[None] | None],
    ) -> bool:
        """Claim one message and invoke ``handler(message)``.

        This is the callback-shaped sibling to ``process_message()``. It keeps
        the context-manager API unchanged while letting the async queue await
        async handlers and also accept quick sync handlers.

        Returns ``True`` when a message was claimed, the handler was called,
        and the message was acked. Returns ``False`` when no message was
        available.
        """
        async with self.process_message() as message:
            if message is None:
                return False
            result = handler(message)
            if inspect.isawaitable(result):
                await result
        return True

    async def _publish_message(self, message_str: str, dedup_key: str) -> bool:
        # Use the gateway's private interruptible publish when available so a
        # block-policy capacity wait aborts on drain even without a configured
        # GracefulInterruptHandler. Duck-typed exactly like
        # ``_wait_for_message_and_move`` so custom gateways stay unaffected.
        interruptible_publish = getattr(self._redis, "_publish_message_interruptible", None)
        if callable(interruptible_publish):
            return await interruptible_publish(
                self.key.pending,
                message_str,
                dedup_key,
                is_interrupted=_DrainInterrupt(lambda: self._draining),
            )
        return await self._redis.publish_message(self.key.pending, message_str, dedup_key)

    async def _add_message(self, message_str: str) -> None:
        interruptible_add = getattr(self._redis, "_add_message_interruptible", None)
        if callable(interruptible_add):
            return await interruptible_add(
                self.key.pending,
                message_str,
                is_interrupted=_DrainInterrupt(lambda: self._draining),
            )
        return await self._redis.add_message(self.key.pending, message_str)

    async def _wait_for_message_and_move(self) -> ClaimedMessage | ReceivedPayload | None:
        interruptible_wait = getattr(self._redis, "_wait_for_message_and_move_interruptible", None)
        if callable(interruptible_wait):
            return await interruptible_wait(
                self.key.pending,
                self.key.processing,
                is_interrupted=_DrainInterrupt(lambda: self._draining),
            )
        return await self._redis.wait_for_message_and_move(
            self.key.pending,
            self.key.processing,
        )

    async def _move_processed_message(
        self,
        destination_queue: str,
        stored_message: ReceivedPayload,
        lease_token: str | None,
    ) -> bool:
        if lease_token is None:
            result = await self._redis.move_message(self.key.processing, destination_queue, stored_message)
        else:
            result = await self._redis.move_message(
                self.key.processing,
                destination_queue,
                stored_message,
                lease_token=lease_token,
            )
        if not isinstance(result, bool):
            raise GatewayContractError(
                f"gateway.move_message() must return bool, got {type(result).__name__}. "
                "See AbstractRedisGateway.move_message for the full contract."
            )
        # Non-lease cleanup still trims on any call. Built-in gateways now
        # replay the original result after retryable drops, and an extra trim
        # is harmless for custom gateways that conservatively return False.
        if result or lease_token is None:
            await self._trim_if_needed(destination_queue)
        return result

    async def _trim_if_needed(self, destination_queue: str) -> None:
        max_length = None
        if destination_queue == self.key.completed and self._max_completed_length is not None:
            max_length = self._max_completed_length
        elif destination_queue == self.key.failed and self._max_failed_length is not None:
            max_length = self._max_failed_length
        if max_length is not None:
            try:
                await self._redis.trim_queue(destination_queue, max_length)
            except Exception as exc:
                logger.warning("Failed to trim queue %s", destination_queue, exc_info=True)
                await self._emit_event(
                    "trim_failed",
                    "failure",
                    destination_queue=destination_queue,
                    exception_type=type(exc).__name__,
                    error=exc,
                )
                _warn_runtime_warning(
                    f"Failed to trim queue {destination_queue} ({type(exc).__name__}); list may exceed max_*_length",
                    stacklevel=3,
                )

    async def _remove_processed_message(self, stored_message: ReceivedPayload, lease_token: str | None) -> bool:
        if lease_token is None:
            result = await self._redis.remove_message(self.key.processing, stored_message)
        else:
            result = await self._redis.remove_message(self.key.processing, stored_message, lease_token=lease_token)
        if not isinstance(result, bool):
            raise GatewayContractError(
                f"gateway.remove_message() must return bool, got {type(result).__name__}. "
                "See AbstractRedisGateway.remove_message for the full contract."
            )
        return result

    async def drain(self, timeout: float | None = None) -> bool:
        """Refuse new publishes/claims and drain in-flight pending claim ids.

        This drains this queue instance's in-flight work; it does not touch
        the underlying async Redis client. The caller still owns
        ``client.aclose()`` and must call it separately to release the
        connection pool.

        Sets a queue-local drain flag so subsequent ``process_message()``
        calls yield ``None`` without claiming and subsequent ``publish()``
        calls raise ``QueueDrainedError``. It then awaits the gateway's
        pending-claim-id recovery loop. Returns ``True`` if all pending claim
        ids were recovered, ``False`` if the deadline fired or a transient
        Redis error left ids pending. In no-visibility-timeout queues,
        recovered messages are returned to pending before the claim id is
        cleared.

        Unlike ``asyncio.CancelledError`` (hard-abort, leaves messages
        claimed for VT-reclaim), ``drain()`` is the explicit-drain
        shutdown path: in-flight handlers continue to natural completion,
        but no further claims are taken. Callers must await any
        in-flight ``process_message`` tasks separately — ``drain()`` does
        not cancel them.

        A publisher already blocked in a ``pending_overload_policy="block"``
        capacity wait is interrupted promptly: ``drain()`` forwards the drain
        flag into that wait (no ``GracefulInterruptHandler`` required), so the
        blocked publish aborts with ``QueueBackpressureError`` and releases the
        publish lock instead of holding it until the block timeout elapses. A
        publish that arrives between the flag being set and ``drain()``
        acquiring the publish lock may still complete if capacity is available.

        ``timeout`` is measured with the event loop's monotonic clock, but
        visibility leases being recovered are anchored to Redis server
        ``TIME``. A forward step in the Redis server clock can make leases
        eligible for reclaim earlier than real elapsed handler time.

        ``drain()`` is queue-instance and process-local. A separate process,
        or a separate ``RedisMessageQueue`` instance using the same Redis keys,
        is not marked drained by this call. For multi-process graceful
        shutdown, each process must drain its own queue instances.

        To drain on a signal, schedule this coroutine on the loop (for
        example ``loop.add_signal_handler(signal.SIGTERM, lambda:
        asyncio.create_task(queue.drain(timeout=25)))``). Because the drain
        runs as a task and the internal locks are cooperative
        ``asyncio.Lock``s, a publish task holding the publish lock keeps
        running and releases it (a blocked capacity wait aborts on the drain
        flag), so the async queue cannot self-deadlock the way a sync signal
        handler interrupting ``publish()`` on its own thread could.
        """
        if timeout is not None and (not isinstance(timeout, (int, float)) or isinstance(timeout, bool)):
            raise TypeError(f"'timeout' must be a number or None, got {type(timeout).__name__}")
        if timeout is not None and timeout < 0:
            raise ConfigurationError(f"'timeout' must be non-negative when provided, got {timeout}")
        started_at = time.perf_counter()
        timeout_seconds = None if timeout is None else float(timeout)
        async with self._drain_lock:
            cleanup_lease_counter = getattr(self._redis, "_cleanup_drained_lease_token_counter", None)
            if self._drain_result is True:
                pending_claim_ids = self._pending_claim_ids_count()
                if pending_claim_ids:
                    self._drain_result = None
                else:
                    if cleanup_lease_counter is not None:
                        await _await_preserving_cancellation(cleanup_lease_counter(self.key.processing))
                    await self._emit_event(
                        "drain",
                        "skipped",
                        duration_ms=_duration_ms(started_at),
                        timeout_seconds=timeout_seconds,
                        pending_claim_ids=pending_claim_ids,
                    )
                    return self._drain_result

            # Set the drain flag BEFORE taking _publish_lock: a publisher
            # blocked in a block-policy capacity wait holds _publish_lock for the
            # whole wait, so we could not acquire it otherwise. The blocked
            # publish observes this flag via its forwarded _DrainInterrupt and
            # aborts, releasing the lock. A publish that slips in between the
            # flag set and our lock acquisition may still complete if capacity is
            # free; that race already exists and is acceptable.
            self._draining = True
            async with self._publish_lock:
                self._drained = True
            await self._emit_event(
                "drain",
                "start",
                duration_ms=_duration_ms(started_at),
                timeout_seconds=timeout_seconds,
                pending_claim_ids=self._pending_claim_ids_count(),
            )
            drainer = getattr(self._redis, "_drain_pending_claim_ids", None)
            if drainer is None:
                if cleanup_lease_counter is not None:
                    await _await_preserving_cancellation(cleanup_lease_counter(self.key.processing))
                self._drain_result = True
                await self._emit_event(
                    "drain",
                    "success",
                    duration_ms=_duration_ms(started_at),
                    timeout_seconds=timeout_seconds,
                    pending_claim_ids=None,
                )
                return self._drain_result
            loop = asyncio.get_running_loop()
            deadline_monotonic = None if timeout_seconds is None else (loop.time() + timeout_seconds)
            raw_result = await _await_preserving_cancellation(
                drainer(self.key.processing, deadline_monotonic=deadline_monotonic)
            )
            # Older/test-double drainers may still return a bare bool; the real
            # gateway returns (drained, error) so each queue attributes its own
            # drain failure instead of reading a gateway-wide slot that a
            # concurrently draining queue sharing the same gateway could have
            # already overwritten.
            if isinstance(raw_result, tuple):
                drained, drain_error = raw_result
            else:
                drained, drain_error = raw_result, None
            if drained:
                if cleanup_lease_counter is not None:
                    await _await_preserving_cancellation(cleanup_lease_counter(self.key.processing))
                self._drain_result = True
                await self._emit_event(
                    "drain",
                    "success",
                    duration_ms=_duration_ms(started_at),
                    timeout_seconds=timeout_seconds,
                    pending_claim_ids=self._pending_claim_ids_count(),
                )
            else:
                self._drain_result = None
                pending_claim_ids = self._pending_claim_ids_count()
                error = self._drain_failure_error(timeout_seconds, pending_claim_ids, drain_error)
                await self._emit_event(
                    "drain",
                    "failure",
                    exception_type=type(error).__name__,
                    error=error,
                    duration_ms=_duration_ms(started_at),
                    timeout_seconds=timeout_seconds,
                    pending_claim_ids=pending_claim_ids,
                )
            return drained

    @property
    def is_draining(self) -> bool:
        """Whether this queue instance has begun refusing new work.

        ``True`` once ``drain()`` has set the queue-local drain
        flag: subsequent ``process_message()`` calls yield ``None`` without
        claiming and subsequent ``publish()`` calls raise ``QueueDrainedError``.
        The flag is set at the start of the drain, before pending-claim-id
        recovery runs, so this can read ``True`` while ``drain()`` is still
        in progress. Read-only and process-local: it reflects only this
        instance's drain state, not other processes or instances sharing the
        same Redis keys.
        """
        return self._draining

    @property
    def is_drained(self) -> bool:
        """Whether this queue instance's drain flag has been fully applied.

        ``True`` once ``drain()`` has taken the publish lock and
        committed the drain flag, guaranteeing no in-flight ``publish()`` can
        still be mid-flight past this point. This does not imply pending-claim-id
        recovery succeeded; use the boolean returned by ``drain()``
        for recovery success. Read-only and process-local.
        """
        return self._drained

    async def stats(self) -> QueueStats:
        """Return a snapshot of this queue's Redis list depths.

        ``pending`` and ``processing`` are always present. ``completed``,
        ``failed``, and ``dead_letter`` are ``None`` when that feature is
        disabled for this queue (``enable_completed_queue=False``,
        ``enable_failed_queue=False``, or no dead-letter routing) and an integer
        depth otherwise. Each depth is a separate ``LLEN``, so the result is a
        best-effort snapshot rather than a single point-in-time-consistent view.
        """
        await self._ensure_plain_redis_client_is_not_cluster()
        queue_length = self._gateway_operator_method("queue_length")
        completed = None
        if self._enable_completed_queue:
            completed = self._require_int_return(await queue_length(self.key.completed), "queue_length")
        failed = None
        if self._enable_failed_queue:
            failed = self._require_int_return(await queue_length(self.key.failed), "queue_length")
        dead_letter = None
        dead_letter_key = self._redis.dead_letter_queue
        if dead_letter_key is not None:
            dead_letter = self._require_int_return(await queue_length(dead_letter_key), "queue_length")
        return QueueStats(
            pending=self._require_int_return(await queue_length(self.key.pending), "queue_length"),
            processing=self._require_int_return(await queue_length(self.key.processing), "queue_length"),
            completed=completed,
            failed=failed,
            dead_letter=dead_letter,
        )

    async def peek(self, count: int = 1, *, source: str = "pending") -> list[ReceivedPayload]:
        """Return up to ``count`` messages from ``source`` without consuming them.

        ``source`` is one of ``"pending"``, ``"processing"``, ``"completed"``,
        ``"failed"``, or ``"dead_letter"``. Messages are read from the head of
        the list (``LRANGE``) and left in place. Payloads are decoded leniently:
        pending/processing envelopes are unwrapped to the published payload, and
        completed/failed/dead-letter entries (stored as raw payloads) are
        returned as-is. Requesting the ``dead_letter`` source when no
        dead-letter queue is configured raises ``ConfigurationError``.
        """
        if isinstance(count, bool) or not isinstance(count, int):
            raise TypeError(f"'count' must be an int, got {type(count).__name__}")
        if count < 1:
            raise ConfigurationError(f"'count' must be >= 1, got {count}")
        if source not in _PEEK_SOURCES:
            raise ConfigurationError(f"'source' must be one of {_PEEK_SOURCES}, got {source!r}")
        await self._ensure_plain_redis_client_is_not_cluster()
        key = self._resolve_queue_key(source)
        peek_messages = self._gateway_operator_method("peek_messages")
        raw_messages = await peek_messages(key, count)
        if not isinstance(raw_messages, list):
            raise GatewayContractError(
                f"gateway.peek_messages() must return a list, got {type(raw_messages).__name__}."
            )
        return [decode_stored_message(message, strict_envelope_decoding=False) for message in raw_messages]

    async def redrive_dead_letters(self, max_messages: int | None = None) -> int:
        """Move dead-lettered messages back to pending and return how many moved.

        Requires a configured dead-letter queue (``ConfigurationError``
        otherwise). ``max_messages=None`` redrives every message currently in
        the dead-letter queue; a positive integer caps the number moved. Each
        message is moved atomically and re-wrapped in a fresh envelope, which
        resets its delivery count, so a redriven message is redelivered up to
        ``max_delivery_count`` times again instead of being dead-lettered on its
        next claim. Oldest dead-letter entries are moved first.
        """
        if max_messages is not None:
            if isinstance(max_messages, bool) or not isinstance(max_messages, int):
                raise TypeError(f"'max_messages' must be an int or None, got {type(max_messages).__name__}")
            if max_messages < 1:
                raise ConfigurationError(f"'max_messages' must be >= 1 when provided, got {max_messages}")
        dead_letter_key = self._redis.dead_letter_queue
        if dead_letter_key is None:
            raise ConfigurationError(
                "no dead-letter queue is configured for this queue; nothing to redrive "
                "(set 'max_delivery_count' on the 'client=' path, or 'dead_letter_queue' on the gateway)."
            )
        await self._ensure_plain_redis_client_is_not_cluster()
        redrive_messages = self._gateway_operator_method("redrive_messages")
        moved_total = 0
        remaining = max_messages
        while remaining is None or remaining > 0:
            chunk = _REDRIVE_BATCH_SIZE if remaining is None else min(_REDRIVE_BATCH_SIZE, remaining)
            envelope_ids = [uuid.uuid4().hex for _ in range(chunk)]
            moved = self._require_int_return(
                await redrive_messages(dead_letter_key, self.key.pending, envelope_ids),
                "redrive_messages",
            )
            moved_total += moved
            if remaining is not None:
                remaining -= moved
            if moved < chunk:
                break
        return moved_total

    async def purge(self, *, target: str) -> int:
        """Delete every message in ``target`` and return how many were removed.

        Destructive and irreversible. ``target`` must be named explicitly (no
        default) and is one of ``"pending"``, ``"completed"``, ``"failed"``, or
        ``"dead_letter"``. Purging ``"processing"`` is rejected because it holds
        in-flight message leases that purging would corrupt. Purging a
        ``dead_letter`` target when no dead-letter queue is configured raises
        ``ConfigurationError``. Only the target list is deleted: deduplication
        markers and lease metadata are left untouched.
        """
        if target == "processing":
            raise ConfigurationError(
                "refusing to purge 'processing': it holds in-flight message leases and purging it "
                "would corrupt active claims. Let messages complete or expire, or drain the queue instead."
            )
        if target not in _PURGE_TARGETS:
            raise ConfigurationError(f"'target' must be one of {_PURGE_TARGETS}, got {target!r}")
        await self._ensure_plain_redis_client_is_not_cluster()
        key = self._resolve_queue_key(target)
        purge_queue = self._gateway_operator_method("purge_queue")
        return self._require_int_return(await purge_queue(key), "purge_queue")

    def _resolve_queue_key(self, source: str) -> str:
        if source == "pending":
            return self.key.pending
        if source == "processing":
            return self.key.processing
        if source == "completed":
            return self.key.completed
        if source == "failed":
            return self.key.failed
        # source == "dead_letter"
        dead_letter_key = self._redis.dead_letter_queue
        if dead_letter_key is None:
            raise ConfigurationError(
                "no dead-letter queue is configured for this queue "
                "(set 'max_delivery_count' on the 'client=' path, or 'dead_letter_queue' on the gateway)."
            )
        return dead_letter_key

    def _gateway_operator_method(self, name: str) -> Callable[..., Awaitable[object]]:
        method = getattr(self._redis, name, None)
        if not callable(method):
            raise GatewayContractError(
                f"the gateway {type(self._redis).__name__} does not implement '{name}'; "
                "stats(), peek(), redrive_dead_letters(), and purge() require the built-in gateway "
                "(the 'client=' constructor) or a custom gateway that provides these operator methods."
            )
        return method

    def _require_int_return(self, value: object, method: str) -> int:
        if not isinstance(value, int) or isinstance(value, bool):
            raise GatewayContractError(f"gateway.{method}() must return an int, got {type(value).__name__}.")
        return value

    def __repr__(self) -> str:
        return f"<RedisMessageQueue name={self._queue_name!r} drained={self._drained}>"

    def _build_lease_heartbeat(
        self,
        stored_message: ReceivedPayload,
        lease_token: str | None,
        message_id: str | None,
        lease_token_hash: str | None,
    ) -> _LeaseHeartbeat | None:
        if lease_token is None or self._heartbeat_interval_seconds is None:
            return None
        # Construct the stop signal alongside the heartbeat so the renewal
        # closure can forward it to the gateway as ``is_interrupted``. This
        # is what makes ``stop()`` interrupt a retrying renewal (AA-01-F2).
        stop_event = asyncio.Event()
        stop_interrupt = _StopEventInterrupt(stop_event)
        return _LeaseHeartbeat(
            interval_seconds=float(self._heartbeat_interval_seconds),
            renew_message_lease=lambda: self._redis.renew_message_lease(
                self.key.processing,
                stored_message,
                lease_token,
                is_interrupted=stop_interrupt,
            ),
            on_heartbeat_failure=self._on_heartbeat_failure,
            emit_event=self._emit_event,
            message_id=message_id,
            lease_token_hash=lease_token_hash,
            stop_event=stop_event,
        )
