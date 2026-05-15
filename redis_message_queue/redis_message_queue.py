import hashlib
import inspect
import json
import logging
import math
import threading
import time
import warnings
from contextlib import contextmanager
from typing import Callable, Iterator, Literal, Optional

import redis
import redis.exceptions

from redis_message_queue._abstract_redis_gateway import AbstractRedisGateway
from redis_message_queue._callable_utils import is_async_callable
from redis_message_queue._config import (
    DEFAULT_PENDING_OVERLOAD_BLOCK_TIMEOUT_SECONDS,
    validate_pending_backpressure_parameters,
)
from redis_message_queue._event import EventOperation, EventOutcome, QueueEvent
from redis_message_queue._exceptions import ConfigurationError, GatewayContractError
from redis_message_queue._queue_key_manager import QueueKeyManager
from redis_message_queue._redis_cluster import validate_queue_keys_for_redis_cluster
from redis_message_queue._redis_gateway import RedisGateway
from redis_message_queue._stored_message import (
    ClaimedMessage,
    MessageData,
    decode_stored_message,
    extract_stored_message_id,
)
from redis_message_queue.interrupt_handler import BaseGracefulInterruptHandler

logger = logging.getLogger(__name__)
_GATEWAY_BOUND_PENDING_QUEUE_ATTR = "_rmq_bound_pending_queue"
_DEFAULT_VISIBILITY_TIMEOUT_SECONDS = 300
_DEFAULT_MAX_DELIVERY_COUNT = 10
_DEFAULT_MAX_COMPLETED_LENGTH = 1000
_DEFAULT_MAX_FAILED_LENGTH = 1000
_AUTO_DEAD_LETTER_QUEUE_SUFFIX = "dlq"

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
    client: redis.Redis | None = None,
    gateway: AbstractRedisGateway | None = None,
    dead_letter_queue: str | None = None,
) -> None:
    if client is not None and isinstance(client, redis.RedisCluster):
        validate_queue_keys_for_redis_cluster(key_manager, dead_letter_queue=dead_letter_queue)
        return
    if gateway is None or not gateway.is_redis_cluster:
        return
    validate_queue_keys_for_redis_cluster(
        key_manager,
        dead_letter_queue=gateway.dead_letter_queue,
    )


def _derive_dead_letter_queue(name: str, key_separator: str) -> str:
    return f"{name}{key_separator}{_AUTO_DEAD_LETTER_QUEUE_SUFFIX}"


def _canonical_bytes(message: str | dict) -> bytes:
    if isinstance(message, dict):
        return json.dumps(message, sort_keys=True, allow_nan=False).encode("utf-8")
    return message.encode("utf-8")


def _default_get_deduplication_key(message: str | dict) -> str:
    return hashlib.sha256(_canonical_bytes(message)).hexdigest()


def _bind_dead_letter_gateway_to_queue(gateway: AbstractRedisGateway, queue_pending_key: str) -> None:
    """Reject reuse of a dead-letter-enabled gateway across different queues.

    The check is not thread-safe: constructing ``RedisMessageQueue`` instances
    concurrently on multiple threads with the same DLQ-enabled gateway can
    race past this guardrail. Queue construction is expected to happen
    serially at application startup — if you need multiple queues sharing a
    DLQ-enabled gateway, construct them on a single thread.
    """

    max_delivery_count = gateway.max_delivery_count
    if max_delivery_count is None:
        return

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
    """Return True for fatal process-exit exceptions that should not ack."""

    return isinstance(exc, (KeyboardInterrupt, SystemExit, GeneratorExit))


def _close_or_cancel_awaitable(awaitable: object) -> None:
    """Best-effort cleanup for an awaitable we reject without awaiting.

    Coroutines and async generators expose ``close()``; asyncio Tasks and
    Futures expose ``cancel()``. Swallows any error so callers can still
    raise the real TypeError without being masked by cleanup failures.
    """
    close = getattr(awaitable, "close", None)
    if callable(close):
        try:
            close()
            return
        except Exception:
            pass
    cancel = getattr(awaitable, "cancel", None)
    if callable(cancel):
        try:
            cancel()
        except Exception:
            pass


class _StopEventInterrupt(BaseGracefulInterruptHandler):
    """Adapt a ``threading.Event`` to the interrupt-handler protocol.

    Lets the gateway's tenacity retry predicate observe a heartbeat's stop
    signal so ``stop()`` can short-circuit an in-flight retry loop rather
    than waiting out ``retry_budget_seconds`` (AA-01-F2).
    """

    def __init__(self, stop_event: threading.Event) -> None:
        self._stop_event = stop_event

    def is_interrupted(self) -> bool:
        return self._stop_event.is_set()


class _LeaseHeartbeat:
    def __init__(
        self,
        *,
        interval_seconds: float,
        renew_message_lease: Callable[[], bool],
        on_heartbeat_failure: Callable[[], None] | None = None,
        emit_event: Callable[..., None] | None = None,
        message_id: str | None = None,
        lease_token_hash: str | None = None,
        stop_event: threading.Event | None = None,
    ):
        self._interval_seconds = interval_seconds
        self._renew_message_lease = renew_message_lease
        self._on_heartbeat_failure = on_heartbeat_failure
        self._emit_event = emit_event
        self._message_id = message_id
        self._lease_token_hash = lease_token_hash
        # Accept an externally-constructed stop event so the queue can share
        # it with the renewal closure (which forwards it as ``is_interrupted``
        # to the gateway). Default preserves the prior self-owned semantics.
        self._stop_event = stop_event if stop_event is not None else threading.Event()
        self._suppress_failure_callback = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name="redis-message-queue-lease-heartbeat",
            daemon=True,
        )

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        if self._thread.ident is None:
            return
        self._stop_event.set()
        self._thread.join(timeout=max(self._interval_seconds * 2, 0.1))
        if self._thread.is_alive():
            logger.warning(
                "Heartbeat thread did not stop within timeout; "
                "it will exit on its own but may briefly renew a stale lease"
            )
            self._emit(
                "heartbeat_stop_timeout",
                "failure",
                message_id=self._message_id,
                lease_token_hash=self._lease_token_hash,
            )
            warnings.warn(
                "Heartbeat did not stop within timeout; it may briefly renew a stale lease before exiting",
                RuntimeWarning,
                stacklevel=2,
            )

    def _emit(self, operation: EventOperation, outcome: EventOutcome, **kwargs: object) -> None:
        if self._emit_event is not None:
            self._emit_event(operation, outcome, **kwargs)

    def suppress_failure_callback(self) -> None:
        """Best-effort signal to suppress the next failure callback.

        The flag is checked before each callback invocation, but there is an
        inherent race: if the heartbeat thread already crossed the check and
        is about to call the user callback when ``suppress_failure_callback()``
        runs, the callback still fires. Treat ``on_heartbeat_failure`` as
        advisory — it MAY be invoked after a successful ``process_message``
        exit when a final renewal coincided with the success path.
        """
        self._suppress_failure_callback.set()

    def _invoke_failure_callback(self) -> None:
        if self._stop_event.is_set() or self._suppress_failure_callback.is_set() or self._on_heartbeat_failure is None:
            return
        try:
            result = self._on_heartbeat_failure()
            if inspect.isawaitable(result):
                _close_or_cancel_awaitable(result)
                raise TypeError(
                    "'on_heartbeat_failure' returned an awaitable; "
                    "use the async RedisMessageQueue from redis_message_queue.asyncio instead"
                )
        except Exception as exc:
            logger.exception("on_heartbeat_failure callback raised an exception")
            warnings.warn(
                f"on_heartbeat_failure callback raised {type(exc).__name__}",
                RuntimeWarning,
                stacklevel=1,
            )

    def _run(self) -> None:
        # No explicit _is_interrupted() check here. Heartbeat lifetime is owned
        # by process_message, which sets _stop_event in its finally block on any
        # exit path including interrupt-induced shutdown. The renew call itself
        # uses the gateway's retry strategy, whose interruptable_retry shortens
        # the retry budget when an interrupt is observed.
        while not self._stop_event.wait(self._interval_seconds):
            try:
                renewed = self._renew_message_lease()
                if self._stop_event.is_set():
                    return
                if not isinstance(renewed, bool):
                    raise GatewayContractError(
                        f"gateway.renew_message_lease() must return bool, got {type(renewed).__name__}. "
                        "See AbstractRedisGateway.renew_message_lease for the full contract."
                    )
            except Exception as exc:
                if self._stop_event.is_set():
                    return
                logger.exception("Failed to renew message lease")
                self._emit(
                    "lease_renew_failed",
                    "failure",
                    message_id=self._message_id,
                    lease_token_hash=self._lease_token_hash,
                    exception_type=type(exc).__name__,
                )
                warnings.warn(
                    "Failed to renew message lease "
                    f"({_warning_exception_name(exc)}); message will be reclaimed by another consumer "
                    "when the visibility timeout expires",
                    RuntimeWarning,
                    stacklevel=1,
                )
                self._invoke_failure_callback()
                return
            if not renewed:
                self._emit(
                    "lease_renew_failed",
                    "skipped",
                    message_id=self._message_id,
                    lease_token_hash=self._lease_token_hash,
                )
                self._invoke_failure_callback()
                return
            self._emit(
                "lease_renew",
                "success",
                message_id=self._message_id,
                lease_token_hash=self._lease_token_hash,
            )


class RedisMessageQueue:
    """Synchronous Redis-backed message queue.

    The queue object is not a context manager and holds no queue-scope resource
    that requires teardown. Use ``with q.process_message() as msg:`` for
    per-message processing. Do not use this sync queue inside async handlers
    unless blocking the event loop is acceptable.
    """

    def __init__(
        self,
        name: str,
        *,
        gateway: Optional[AbstractRedisGateway] = None,
        client: Optional[redis.Redis] = None,
        deduplication: bool = True,
        enable_completed_queue: bool = False,
        enable_failed_queue: bool = False,
        visibility_timeout_seconds: int | None = _DEFAULT_VISIBILITY_TIMEOUT_SECONDS,
        heartbeat_interval_seconds: int | float | None = None,
        max_completed_length: int | None = _DEFAULT_MAX_COMPLETED_LENGTH,
        max_failed_length: int | None = _DEFAULT_MAX_FAILED_LENGTH,
        max_delivery_count: int | None = _DEFAULT_MAX_DELIVERY_COUNT,
        max_pending_length: int | None = None,
        pending_overload_policy: Literal["raise", "drop_oldest", "block"] = "raise",
        pending_overload_block_timeout_seconds: float = DEFAULT_PENDING_OVERLOAD_BLOCK_TIMEOUT_SECONDS,
        key_separator: str = "::",
        get_deduplication_key: Optional[Callable[[str | dict], str]] = _default_get_deduplication_key,
        interrupt: BaseGracefulInterruptHandler | None = None,
        on_heartbeat_failure: Callable[[], None] | None = None,
        on_event: Callable[[QueueEvent], None] | None = None,
    ):
        """Create a queue bound to a Redis client or custom gateway.

        ``visibility_timeout_seconds`` defaults to 300. Set it to ``None`` to
        disable lease-based crash recovery; messages left in ``processing`` by a
        crashed worker are then not reclaimed automatically.

        ``max_delivery_count`` defaults to 10 on the built-in ``client=`` path.
        Messages reclaimed more than this many times are routed to the
        auto-derived dead-letter queue. Set it to ``None`` for unlimited
        redelivery.

        ``get_deduplication_key`` defaults to a SHA-256 hash of the canonical
        message string. Passing ``None`` uses the literal serialized message as
        the deduplication key; passing a callable lets you define a custom
        keyspace.

        ``max_pending_length`` defaults to ``None`` (unbounded). Set it to a
        positive integer to cap pending-list depth during publish. Overload is
        handled according to ``pending_overload_policy``.

        ``interrupt`` accepts a ``BaseGracefulInterruptHandler``; pass
        ``GracefulInterruptHandler()`` for prompt Ctrl-C / termination handling
        in polling waits. ``on_heartbeat_failure`` is a zero-argument callable
        invoked when lease renewal fails. ``on_event`` receives best-effort
        QueueEvent lifecycle notifications; callback failures are logged and
        converted to RuntimeWarning without interrupting queue operations.
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
        validate_pending_backpressure_parameters(
            max_pending_length,
            pending_overload_policy,
            pending_overload_block_timeout_seconds,
        )
        get_deduplication_key_was_configured = (
            get_deduplication_key is not None and get_deduplication_key is not _default_get_deduplication_key
        )
        if get_deduplication_key is not None and not callable(get_deduplication_key):
            raise TypeError(
                f"'get_deduplication_key' must be callable, got {type(get_deduplication_key).__name__}."
                " Expected a function that takes the message (str | dict) and returns a str."
                " Example: get_deduplication_key=lambda msg: msg['user_id']"
            )
        if get_deduplication_key is not None and is_async_callable(get_deduplication_key):
            raise TypeError(
                "'get_deduplication_key' is an async callable; "
                "use the async RedisMessageQueue from redis_message_queue.asyncio instead"
            )
        if not deduplication and get_deduplication_key_was_configured:
            raise ConfigurationError("'get_deduplication_key' cannot be provided when 'deduplication' is disabled.")
        if on_heartbeat_failure is not None and not callable(on_heartbeat_failure):
            raise TypeError(
                f"'on_heartbeat_failure' must be callable, got {type(on_heartbeat_failure).__name__}."
                " Expected a zero-arg function invoked when lease renewal fails."
            )
        if on_heartbeat_failure is not None and is_async_callable(on_heartbeat_failure):
            raise TypeError(
                "'on_heartbeat_failure' is an async callable; "
                "use the async RedisMessageQueue from redis_message_queue.asyncio instead"
            )
        if on_event is not None and not callable(on_event):
            raise TypeError(f"'on_event' must be callable, got {type(on_event).__name__}.")
        if on_event is not None and is_async_callable(on_event):
            raise TypeError(
                "'on_event' is an async callable; use the async RedisMessageQueue from "
                "redis_message_queue.asyncio instead"
            )
        self._queue_name = name
        self._on_event = on_event
        # Queue-local soft-drain flag. Set via ``drain()`` to refuse new
        # claims from this queue's ``process_message()`` while letting any
        # in-flight handler exit naturally (AA-05-F1/F2). Distinct from the
        # gateway-level ``interrupt`` handler so callers can opt into drain
        # without owning a process-wide signal handler.
        self._draining = False
        self._deduplication = deduplication
        self._enable_completed_queue = enable_completed_queue
        self._enable_failed_queue = enable_failed_queue
        self._max_completed_length = max_completed_length
        self._max_failed_length = max_failed_length
        self._max_delivery_count = max_delivery_count
        self._get_deduplication_key = get_deduplication_key
        self._heartbeat_interval_seconds = None
        self._warned_no_lease_for_heartbeat = False
        self._requires_claimed_message = False

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
                    from redis_message_queue.asyncio import AbstractRedisGateway as _AsyncAbstractRedisGateway
                except ImportError:
                    _AsyncAbstractRedisGateway = ()  # type: ignore[assignment]
                if isinstance(gateway, _AsyncAbstractRedisGateway):
                    raise GatewayContractError(
                        "'gateway' is an async AbstractRedisGateway; "
                        "use the async RedisMessageQueue from redis_message_queue.asyncio instead"
                    )
                raise GatewayContractError(
                    "'gateway' must be an AbstractRedisGateway"
                    " (subclass redis_message_queue.AbstractRedisGateway),"
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
            _bind_dead_letter_gateway_to_queue(gateway, self.key.pending)
            self._max_delivery_count = None
            self._redis = gateway
        elif client is None:
            raise ConfigurationError("Either 'client' or 'gateway' must be provided.")
        else:
            dead_letter_queue = (
                _derive_dead_letter_queue(name, key_separator) if max_delivery_count is not None else None
            )
            _validate_cluster_configuration(self.key, client=client, dead_letter_queue=dead_letter_queue)
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
            set_event_emitter(self._queue_name, self._emit_event)

    def _emit_event(
        self,
        operation: EventOperation,
        outcome: EventOutcome,
        *,
        message_id: str | None = None,
        claim_id: str | None = None,
        lease_token_hash: str | None = None,
        destination_queue: str | None = None,
        exception_type: str | None = None,
        duration_ms: float | None = None,
    ) -> None:
        if self._on_event is None:
            return
        event = QueueEvent(
            queue=self._queue_name,
            operation=operation,
            outcome=outcome,
            message_id=message_id,
            claim_id=claim_id,
            lease_token_hash=lease_token_hash,
            destination_queue=destination_queue,
            exception_type=exception_type,
            duration_ms=duration_ms,
        )
        try:
            result = self._on_event(event)
            if inspect.isawaitable(result):
                _close_or_cancel_awaitable(result)
                raise TypeError(
                    "'on_event' returned an awaitable; use the async RedisMessageQueue "
                    "from redis_message_queue.asyncio instead"
                )
        except Exception as exc:
            logger.exception("on_event callback raised an exception")
            warnings.warn(
                f"on_event callback raised {type(exc).__name__}",
                RuntimeWarning,
                stacklevel=2,
            )

    def publish(self, message: str | dict) -> bool:
        """Publish a message.

        Dict messages are serialized via ``json.dumps(message, sort_keys=True)``.
        All top-level dict keys must be strings; non-string keys raise
        ``TypeError`` to avoid silent ``json.dumps`` coercion that would
        collapse distinct keys into the same dedup key (e.g. ``{1: "x"}``
        vs ``{"1": "x"}``). Only top-level keys are validated; nested
        dicts follow ``json.dumps`` defaults (e.g. nested non-string keys
        are silently coerced: integer keys become strings).
        """
        if not isinstance(message, (str, dict)):
            raise TypeError(f"'message' must be a str or dict, got {type(message).__name__}")
        if isinstance(message, dict):
            non_str_keys = [k for k in message if not isinstance(k, str)]
            if non_str_keys:
                raise TypeError(
                    "'message' dict keys must all be strings; "
                    f"got non-string keys: {non_str_keys[:3]}" + (" (and more)" if len(non_str_keys) > 3 else "")
                )
            message_str = json.dumps(message, sort_keys=True, allow_nan=False)
        else:
            message_str = message

        started_at = time.perf_counter()
        if not self._deduplication:
            try:
                result = self._redis.add_message(self.key.pending, message_str)
            except Exception as exc:
                self._emit_event(
                    "publish",
                    "failure",
                    exception_type=type(exc).__name__,
                    duration_ms=_duration_ms(started_at),
                )
                raise
            if result is not None:
                raise GatewayContractError(
                    f"gateway.add_message() must return None, got {type(result).__name__}. "
                    "See AbstractRedisGateway.add_message for the full contract."
                )
            self._emit_event("publish", "success", duration_ms=_duration_ms(started_at))
            return True

        if self._get_deduplication_key is not None:
            dedup_key = self._get_deduplication_key(message)
            if inspect.isawaitable(dedup_key):
                is_coroutine = inspect.iscoroutine(dedup_key)
                _close_or_cancel_awaitable(dedup_key)
                if is_coroutine:
                    raise TypeError(
                        "'get_deduplication_key' returned a coroutine; "
                        "use the async RedisMessageQueue for async callables"
                    )
                raise TypeError(
                    "'get_deduplication_key' returned an awaitable; use the async RedisMessageQueue for async callables"
                )
            if not isinstance(dedup_key, str):
                raise TypeError(f"'get_deduplication_key' must return a string, got {type(dedup_key).__name__}")
        else:
            dedup_key = message_str
        dedup_key = self.key.deduplication(dedup_key)

        try:
            result = self._redis.publish_message(self.key.pending, message_str, dedup_key)
        except Exception as exc:
            self._emit_event(
                "publish",
                "failure",
                exception_type=type(exc).__name__,
                duration_ms=_duration_ms(started_at),
            )
            raise
        if not isinstance(result, bool):
            raise GatewayContractError(
                f"gateway.publish_message() must return bool, got {type(result).__name__}. "
                "See AbstractRedisGateway.publish_message for the full contract."
            )
        self._emit_event(
            "publish" if result else "publish_dedup_hit",
            "success" if result else "skipped",
            duration_ms=_duration_ms(started_at),
        )
        return result

    @contextmanager
    def process_message(self) -> Iterator[Optional[MessageData]]:
        """Claim and process one message.

        Yields ``str`` if your client uses ``decode_responses=True``, else
        ``bytes``. Match the client setting to the type your handler expects.
        """
        claim_started_at = time.perf_counter()
        if self._draining:
            self._emit_event("claim_empty", "skipped", duration_ms=_duration_ms(claim_started_at))
            yield None
            return
        try:
            claimed_message = self._redis.wait_for_message_and_move(
                self.key.pending,
                self.key.processing,
            )
        except Exception as exc:
            self._emit_event(
                "claim",
                "failure",
                exception_type=type(exc).__name__,
                duration_ms=_duration_ms(claim_started_at),
            )
            raise
        if claimed_message is None:
            self._emit_event("claim_empty", "skipped", duration_ms=_duration_ms(claim_started_at))
            yield None
            return

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
        message_id = extract_stored_message_id(stored_message)
        lease_token_hash = _hash_lease_token(lease_token)
        self._emit_event(
            "claim",
            "success",
            message_id=message_id,
            lease_token_hash=lease_token_hash,
            duration_ms=_duration_ms(claim_started_at),
        )

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
                warnings.warn(no_lease_token_warning, RuntimeWarning, stacklevel=2)

        if lease_token is None and self._requires_claimed_message:
            raise GatewayContractError(
                "gateways with visibility timeouts must return ClaimedMessage from "
                "wait_for_message_and_move(); got plain MessageData without a lease token"
            )

        message = decode_stored_message(stored_message)
        lease_heartbeat = self._build_lease_heartbeat(stored_message, lease_token, message_id, lease_token_hash)
        if lease_heartbeat is not None:
            lease_heartbeat.start()
        processing_started_at = time.perf_counter()
        try:
            yield message  # type: ignore
        except BaseException as exc:
            skip_cleanup = _should_skip_message_cleanup(exc)
            if lease_heartbeat is not None:
                lease_heartbeat.suppress_failure_callback()
            if skip_cleanup:
                raise
            self._emit_event(
                "failed",
                "failure",
                message_id=message_id,
                lease_token_hash=lease_token_hash,
                destination_queue=self.key.failed if self._enable_failed_queue else None,
                exception_type=type(exc).__name__,
                duration_ms=_duration_ms(processing_started_at),
            )
            cleanup_started_at = time.perf_counter()
            try:
                if self._enable_failed_queue:
                    applied = self._move_processed_message(self.key.failed, stored_message, lease_token)
                else:
                    applied = self._remove_processed_message(stored_message, lease_token)
                self._emit_event(
                    "nack",
                    "success" if applied else "skipped",
                    message_id=message_id,
                    lease_token_hash=lease_token_hash,
                    duration_ms=_duration_ms(cleanup_started_at),
                )
                if lease_token is not None and not applied:
                    logger.warning(_STALE_LEASE_NACK_WARNING)
                    self._emit_event(
                        "stale_lease_nack",
                        "skipped",
                        message_id=message_id,
                        lease_token_hash=lease_token_hash,
                    )
                    warnings.warn(_STALE_LEASE_NACK_WARNING, RuntimeWarning, stacklevel=2)
            except BaseException as cleanup_exc:
                logger.exception("Failed to clean up message from processing queue")
                self._emit_event(
                    "cleanup_failed",
                    "failure",
                    message_id=message_id,
                    lease_token_hash=lease_token_hash,
                    exception_type=type(cleanup_exc).__name__,
                    duration_ms=_duration_ms(cleanup_started_at),
                )
                warnings.warn(
                    f"Cleanup raised after handler exception ({_warning_exception_name(cleanup_exc)}); "
                    "see logs for both tracebacks",
                    RuntimeWarning,
                    stacklevel=2,
                )
            raise
        else:
            if lease_heartbeat is not None:
                lease_heartbeat.suppress_failure_callback()
            cleanup_started_at = time.perf_counter()
            try:
                if self._enable_completed_queue:
                    applied = self._move_processed_message(self.key.completed, stored_message, lease_token)
                else:
                    applied = self._remove_processed_message(stored_message, lease_token)
            except BaseException as cleanup_exc:
                self._emit_event(
                    "cleanup_failed",
                    "failure",
                    message_id=message_id,
                    lease_token_hash=lease_token_hash,
                    exception_type=type(cleanup_exc).__name__,
                    duration_ms=_duration_ms(cleanup_started_at),
                )
                raise
            if self._enable_completed_queue:
                self._emit_event(
                    "completed",
                    "success" if applied else "skipped",
                    message_id=message_id,
                    lease_token_hash=lease_token_hash,
                    destination_queue=self.key.completed,
                    duration_ms=_duration_ms(processing_started_at),
                )
            self._emit_event(
                "ack",
                "success" if applied else "skipped",
                message_id=message_id,
                lease_token_hash=lease_token_hash,
                duration_ms=_duration_ms(cleanup_started_at),
            )
            if lease_token is not None and not applied:
                logger.warning(_STALE_LEASE_ACK_WARNING)
                self._emit_event(
                    "stale_lease_ack",
                    "skipped",
                    message_id=message_id,
                    lease_token_hash=lease_token_hash,
                )
                warnings.warn(_STALE_LEASE_ACK_WARNING, RuntimeWarning, stacklevel=2)
        finally:
            if lease_heartbeat is not None:
                lease_heartbeat.stop()

    def _move_processed_message(
        self,
        destination_queue: str,
        stored_message: MessageData,
        lease_token: str | None,
    ) -> bool:
        if lease_token is None:
            result = self._redis.move_message(self.key.processing, destination_queue, stored_message)
        else:
            result = self._redis.move_message(
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
            self._trim_if_needed(destination_queue)
        return result

    def _trim_if_needed(self, destination_queue: str) -> None:
        max_length = None
        if destination_queue == self.key.completed and self._max_completed_length is not None:
            max_length = self._max_completed_length
        elif destination_queue == self.key.failed and self._max_failed_length is not None:
            max_length = self._max_failed_length
        if max_length is not None:
            try:
                self._redis.trim_queue(destination_queue, max_length)
            except Exception as exc:
                logger.warning("Failed to trim queue %s", destination_queue, exc_info=True)
                self._emit_event(
                    "trim_failed",
                    "failure",
                    destination_queue=destination_queue,
                    exception_type=type(exc).__name__,
                )
                warnings.warn(
                    f"Failed to trim queue {destination_queue} ({type(exc).__name__}); list may exceed max_*_length",
                    RuntimeWarning,
                    stacklevel=3,
                )

    def _remove_processed_message(self, stored_message: MessageData, lease_token: str | None) -> bool:
        if lease_token is None:
            result = self._redis.remove_message(self.key.processing, stored_message)
        else:
            result = self._redis.remove_message(self.key.processing, stored_message, lease_token=lease_token)
        if not isinstance(result, bool):
            raise GatewayContractError(
                f"gateway.remove_message() must return bool, got {type(result).__name__}. "
                "See AbstractRedisGateway.remove_message for the full contract."
            )
        return result

    def drain(self, timeout: float | None = None) -> bool:
        """Refuse new claims and drain in-flight pending claim ids.

        Sets a queue-local drain flag so subsequent ``process_message()``
        calls yield ``None`` without claiming, then walks the gateway's
        in-memory ``_pending_claim_ids`` to recover any ambiguous claims
        that an interrupt-aware shutdown would otherwise drop on process
        exit (AA-05-F2).

        ``timeout`` bounds the pending-claim-id recovery loop in seconds;
        ``None`` waits indefinitely, ``0`` skips the loop entirely. The
        flag is set regardless of the timeout value.

        Returns ``True`` if all pending claim ids were recovered (or none
        were present); ``False`` if recovery hit the deadline or a
        transient Redis error left claim ids pending.

        Drain does **not** cancel in-flight ``process_message`` handlers;
        the caller must coordinate handler exits via its own scheduling
        (joining threads / awaiting tasks). Heartbeat stop remains
        best-effort per ``_LeaseHeartbeat.stop()``'s contract (AA-05-F3).
        """
        if timeout is not None and (not isinstance(timeout, (int, float)) or isinstance(timeout, bool)):
            raise TypeError(f"'timeout' must be a number or None, got {type(timeout).__name__}")
        if timeout is not None and timeout < 0:
            raise ConfigurationError(f"'timeout' must be non-negative when provided, got {timeout}")
        self._draining = True
        drainer = getattr(self._redis, "_drain_pending_claim_ids", None)
        if drainer is None:
            return True
        deadline_monotonic = None if timeout is None else (time.monotonic() + float(timeout))
        return drainer(self.key.processing, deadline_monotonic=deadline_monotonic)

    def _build_lease_heartbeat(
        self,
        stored_message: MessageData,
        lease_token: str | None,
        message_id: str | None,
        lease_token_hash: str | None,
    ) -> _LeaseHeartbeat | None:
        if lease_token is None or self._heartbeat_interval_seconds is None:
            return None
        # Construct the stop signal alongside the heartbeat so the renewal
        # closure can forward it to the gateway as ``is_interrupted``. This
        # is what makes ``stop()`` interrupt a retrying renewal (AA-01-F2).
        stop_event = threading.Event()
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
