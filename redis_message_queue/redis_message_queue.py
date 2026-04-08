import inspect
import json
import logging
import math
import threading
from contextlib import contextmanager
from typing import Callable, Iterator, Optional

import redis
import redis.exceptions

from redis_message_queue._abstract_redis_gateway import AbstractRedisGateway
from redis_message_queue._callable_utils import is_async_callable
from redis_message_queue._queue_key_manager import QueueKeyManager
from redis_message_queue._redis_cluster import validate_queue_keys_for_redis_cluster
from redis_message_queue._redis_gateway import RedisGateway
from redis_message_queue._stored_message import ClaimedMessage, MessageData, decode_stored_message
from redis_message_queue.interrupt_handler import BaseGracefulInterruptHandler

logger = logging.getLogger(__name__)
_GATEWAY_BOUND_PENDING_QUEUE_ATTR = "_rmq_bound_pending_queue"


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
        raise ValueError(f"'heartbeat_interval_seconds' must be a finite number, got {heartbeat_interval_seconds}")
    if heartbeat_interval_seconds <= 0:
        raise ValueError(
            f"'heartbeat_interval_seconds' must be positive when provided, got {heartbeat_interval_seconds}"
        )
    if visibility_timeout_seconds is None:
        if require_visibility_timeout_message is None:
            require_visibility_timeout_message = (
                "'heartbeat_interval_seconds' requires a configured visibility timeout."
            )
        raise ValueError(require_visibility_timeout_message)
    if heartbeat_interval_seconds > visibility_timeout_seconds / 2:
        raise ValueError(
            "'heartbeat_interval_seconds' must be no more than half of 'visibility_timeout_seconds' "
            f"({heartbeat_interval_seconds} > {visibility_timeout_seconds / 2})"
        )
    return heartbeat_interval_seconds


def _get_gateway_visibility_timeout_seconds(gateway: AbstractRedisGateway) -> int | None:
    visibility_timeout_seconds = _get_optional_gateway_visibility_timeout_seconds(gateway)
    if visibility_timeout_seconds is not None:
        return visibility_timeout_seconds
    if not hasattr(gateway, "message_visibility_timeout_seconds"):
        raise ValueError(
            "'heartbeat_interval_seconds' with 'gateway' requires a gateway that "
            "must expose 'message_visibility_timeout_seconds'."
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
            raise TypeError(
                "'gateway.message_visibility_timeout_seconds' must be an int or None, "
                f"got {type(visibility_timeout_seconds).__name__}"
            )
        if visibility_timeout_seconds <= 0:
            raise ValueError(
                "'gateway.message_visibility_timeout_seconds' must be positive when provided, "
                f"got {visibility_timeout_seconds}"
            )
    return visibility_timeout_seconds


def _validate_cluster_configuration(
    key_manager: QueueKeyManager,
    *,
    client: redis.Redis | None = None,
    gateway: AbstractRedisGateway | None = None,
) -> None:
    if client is not None and isinstance(client, redis.RedisCluster):
        validate_queue_keys_for_redis_cluster(key_manager)
        return
    if gateway is None or not getattr(gateway, "is_redis_cluster", False):
        return
    validate_queue_keys_for_redis_cluster(
        key_manager,
        dead_letter_queue=getattr(gateway, "dead_letter_queue", None),
    )


def _bind_dead_letter_gateway_to_queue(gateway: AbstractRedisGateway, queue_pending_key: str) -> None:
    """Reject reuse of a dead-letter-enabled gateway across different queues."""

    max_delivery_count = getattr(gateway, "max_delivery_count", None)
    if max_delivery_count is None:
        return

    bound_pending_key = getattr(gateway, _GATEWAY_BOUND_PENDING_QUEUE_ATTR, None)
    if bound_pending_key is None:
        setattr(gateway, _GATEWAY_BOUND_PENDING_QUEUE_ATTR, queue_pending_key)
        return
    if bound_pending_key != queue_pending_key:
        raise ValueError(
            "A gateway configured with 'max_delivery_count' cannot be reused across different queues. "
            "Create a separate gateway per queue when dead-letter routing is enabled."
        )


def _should_skip_message_cleanup(exc: BaseException) -> bool:
    """Return True for fatal process-exit exceptions that should not ack."""

    return isinstance(exc, (KeyboardInterrupt, SystemExit, GeneratorExit))


class _LeaseHeartbeat:
    def __init__(
        self,
        *,
        interval_seconds: float,
        renew_message_lease: Callable[[], bool],
        on_heartbeat_failure: Callable[[], None] | None = None,
    ):
        self._interval_seconds = interval_seconds
        self._renew_message_lease = renew_message_lease
        self._on_heartbeat_failure = on_heartbeat_failure
        self._stop_event = threading.Event()
        self._suppress_failure_callback = threading.Event()
        self._started = False
        self._thread = threading.Thread(
            target=self._run,
            name="redis-message-queue-lease-heartbeat",
            daemon=True,
        )

    def start(self) -> None:
        self._thread.start()
        self._started = True

    def stop(self) -> None:
        if not self._started:
            return
        self._stop_event.set()
        self._thread.join(timeout=max(self._interval_seconds * 2, 0.1))
        if self._thread.is_alive():
            logger.warning(
                "Heartbeat thread did not stop within timeout; "
                "it will exit on its own but may briefly renew a stale lease"
            )

    def suppress_failure_callback(self) -> None:
        self._suppress_failure_callback.set()

    def _invoke_failure_callback(self) -> None:
        if self._stop_event.is_set() or self._suppress_failure_callback.is_set() or self._on_heartbeat_failure is None:
            return
        try:
            result = self._on_heartbeat_failure()
            if inspect.isawaitable(result):
                if inspect.iscoroutine(result):
                    result.close()
                raise TypeError(
                    "'on_heartbeat_failure' returned an awaitable; "
                    "use the async RedisMessageQueue from redis_message_queue.asyncio instead"
                )
        except Exception:
            logger.exception("on_heartbeat_failure callback raised an exception")

    def _run(self) -> None:
        while not self._stop_event.wait(self._interval_seconds):
            try:
                renewed = self._renew_message_lease()
                if self._stop_event.is_set():
                    return
                if not isinstance(renewed, bool):
                    raise TypeError(f"gateway.renew_message_lease() must return bool, got {type(renewed).__name__}")
            except Exception:
                if self._stop_event.is_set():
                    return
                logger.exception("Failed to renew message lease")
                self._invoke_failure_callback()
                return
            if not renewed:
                self._invoke_failure_callback()
                return


class RedisMessageQueue:
    def __init__(
        self,
        name: str,
        *,
        gateway: Optional[AbstractRedisGateway] = None,
        client: Optional[redis.Redis] = None,
        deduplication: bool = True,
        enable_completed_queue: bool = False,
        enable_failed_queue: bool = False,
        visibility_timeout_seconds: int | None = None,
        heartbeat_interval_seconds: int | float | None = None,
        max_completed_length: int | None = None,
        max_failed_length: int | None = None,
        max_delivery_count: int | None = None,
        key_separator: str = "::",
        get_deduplication_key: Optional[Callable] = None,
        interrupt: BaseGracefulInterruptHandler | None = None,
        on_heartbeat_failure: Callable[[], None] | None = None,
    ):
        self.key = QueueKeyManager(name, key_separator=key_separator)
        if not isinstance(deduplication, bool):
            raise TypeError(f"'deduplication' must be a bool, got {type(deduplication).__name__}")
        if not isinstance(enable_completed_queue, bool):
            raise TypeError(f"'enable_completed_queue' must be a bool, got {type(enable_completed_queue).__name__}")
        if not isinstance(enable_failed_queue, bool):
            raise TypeError(f"'enable_failed_queue' must be a bool, got {type(enable_failed_queue).__name__}")
        if max_completed_length is not None:
            if not isinstance(max_completed_length, int) or isinstance(max_completed_length, bool):
                raise TypeError(
                    f"'max_completed_length' must be an int or None, got {type(max_completed_length).__name__}"
                )
            if max_completed_length <= 0:
                raise ValueError(f"'max_completed_length' must be positive when provided, got {max_completed_length}")
            if not enable_completed_queue:
                raise ValueError("'max_completed_length' requires 'enable_completed_queue=True'.")
        if max_failed_length is not None:
            if not isinstance(max_failed_length, int) or isinstance(max_failed_length, bool):
                raise TypeError(f"'max_failed_length' must be an int or None, got {type(max_failed_length).__name__}")
            if max_failed_length <= 0:
                raise ValueError(f"'max_failed_length' must be positive when provided, got {max_failed_length}")
            if not enable_failed_queue:
                raise ValueError("'max_failed_length' requires 'enable_failed_queue=True'.")
        if max_delivery_count is not None:
            if not isinstance(max_delivery_count, int) or isinstance(max_delivery_count, bool):
                raise TypeError(f"'max_delivery_count' must be an int or None, got {type(max_delivery_count).__name__}")
            if max_delivery_count <= 0:
                raise ValueError(f"'max_delivery_count' must be positive when provided, got {max_delivery_count}")
        if visibility_timeout_seconds is not None:
            if not isinstance(visibility_timeout_seconds, int) or isinstance(visibility_timeout_seconds, bool):
                raise TypeError(
                    "'visibility_timeout_seconds' must be an int or None, "
                    f"got {type(visibility_timeout_seconds).__name__}"
                )
            if visibility_timeout_seconds <= 0:
                raise ValueError(
                    f"'visibility_timeout_seconds' must be positive when provided, got {visibility_timeout_seconds}"
                )
        if get_deduplication_key is not None and not callable(get_deduplication_key):
            raise TypeError(f"'get_deduplication_key' must be callable, got {type(get_deduplication_key).__name__}")
        if get_deduplication_key is not None and inspect.iscoroutinefunction(get_deduplication_key):
            raise TypeError(
                "'get_deduplication_key' is an async callable; "
                "use the async RedisMessageQueue from redis_message_queue.asyncio instead"
            )
        if not deduplication and get_deduplication_key is not None:
            raise ValueError("'get_deduplication_key' cannot be provided when 'deduplication' is disabled.")
        if on_heartbeat_failure is not None and not callable(on_heartbeat_failure):
            raise TypeError(f"'on_heartbeat_failure' must be callable, got {type(on_heartbeat_failure).__name__}")
        if on_heartbeat_failure is not None and is_async_callable(on_heartbeat_failure):
            raise TypeError(
                "'on_heartbeat_failure' is an async callable; "
                "use the async RedisMessageQueue from redis_message_queue.asyncio instead"
            )
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
            if client is not None or interrupt is not None or visibility_timeout_seconds is not None:
                raise ValueError(
                    "'gateway' cannot be provided alongside 'client', 'interrupt', or 'visibility_timeout_seconds'."
                    " Configure the gateway directly instead."
                )
            if not isinstance(gateway, AbstractRedisGateway):
                raise TypeError(f"'gateway' must be an AbstractRedisGateway, got {type(gateway).__name__}")
            gateway_visibility_timeout_seconds = _get_optional_gateway_visibility_timeout_seconds(gateway)
            self._requires_claimed_message = gateway_visibility_timeout_seconds is not None
            _bind_dead_letter_gateway_to_queue(gateway, self.key.pending)
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
            if max_delivery_count is not None:
                raise ValueError(
                    "'max_delivery_count' cannot be provided alongside 'gateway'."
                    " Configure 'max_delivery_count' and 'dead_letter_queue' on the gateway directly instead."
                )
            self._redis = gateway
        elif client is None:
            raise ValueError("Either 'client' or 'gateway' must be provided.")
        else:
            _validate_cluster_configuration(self.key, client=client)
            self._heartbeat_interval_seconds = _validate_heartbeat_interval_seconds(
                heartbeat_interval_seconds,
                visibility_timeout_seconds,
                require_visibility_timeout_message=(
                    "'heartbeat_interval_seconds' requires 'visibility_timeout_seconds'"
                    " when using the built-in client path."
                ),
            )
            if max_delivery_count is not None and visibility_timeout_seconds is None:
                raise ValueError("'max_delivery_count' requires 'visibility_timeout_seconds' to be set.")
            self._requires_claimed_message = visibility_timeout_seconds is not None
            self._redis = RedisGateway(
                redis_client=client,
                interrupt=interrupt,
                message_visibility_timeout_seconds=visibility_timeout_seconds,
                max_delivery_count=max_delivery_count,
                dead_letter_queue=self.key.dead_letter if max_delivery_count is not None else None,
            )

        if on_heartbeat_failure is not None and self._heartbeat_interval_seconds is None:
            raise ValueError("'on_heartbeat_failure' requires 'heartbeat_interval_seconds' to be set.")
        self._on_heartbeat_failure = on_heartbeat_failure

    def publish(self, message: str | dict) -> bool:
        if not isinstance(message, (str, dict)):
            raise TypeError(f"'message' must be a str or dict, got {type(message).__name__}")
        if isinstance(message, dict):
            message_str = json.dumps(message, sort_keys=True)
        else:
            message_str = message

        if not self._deduplication:
            result = self._redis.add_message(self.key.pending, message_str)
            if result is not None:
                raise TypeError(f"gateway.add_message() must return None, got {type(result).__name__}")
            return True

        if self._get_deduplication_key is not None:
            dedup_key = self._get_deduplication_key(message)
            if inspect.isawaitable(dedup_key):
                if inspect.iscoroutine(dedup_key):
                    dedup_key.close()
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

        result = self._redis.publish_message(self.key.pending, message_str, dedup_key)
        if not isinstance(result, bool):
            raise TypeError(f"gateway.publish_message() must return bool, got {type(result).__name__}")
        return result

    @contextmanager
    def process_message(self) -> Iterator[Optional[MessageData]]:
        claimed_message = self._redis.wait_for_message_and_move(
            self.key.pending,
            self.key.processing,
        )
        if claimed_message is None:
            yield None
            return

        if not isinstance(claimed_message, (ClaimedMessage, str, bytes)):
            raise TypeError(
                f"gateway.wait_for_message_and_move() must return ClaimedMessage, str, bytes, or None; "
                f"got {type(claimed_message).__name__}"
            )

        stored_message = claimed_message
        lease_token = None
        if isinstance(claimed_message, ClaimedMessage):
            stored_message = claimed_message.stored_message
            lease_token = claimed_message.lease_token

        if lease_token is None and self._requires_claimed_message:
            raise TypeError(
                "gateways with visibility timeouts must return ClaimedMessage from "
                "wait_for_message_and_move(); got plain MessageData without a lease token"
            )

        if lease_token is None and self._heartbeat_interval_seconds is not None:
            if not self._warned_no_lease_for_heartbeat:
                self._warned_no_lease_for_heartbeat = True
                logger.warning(
                    "Heartbeat is configured but the gateway returned no lease token. "
                    "The heartbeat will not run for this message. Ensure the gateway "
                    "returns ClaimedMessage from wait_for_message_and_move() when "
                    "visibility timeouts are in use."
                )

        message = decode_stored_message(stored_message)
        lease_heartbeat = self._build_lease_heartbeat(stored_message, lease_token)
        if lease_heartbeat is not None:
            lease_heartbeat.start()
        try:
            yield message  # type: ignore
        except BaseException as exc:
            skip_cleanup = _should_skip_message_cleanup(exc)
            if lease_heartbeat is not None:
                lease_heartbeat.suppress_failure_callback()
            if skip_cleanup:
                raise
            try:
                if self._enable_failed_queue:
                    applied = self._move_processed_message(self.key.failed, stored_message, lease_token)
                else:
                    applied = self._remove_processed_message(stored_message, lease_token)
                if lease_token is not None and not applied:
                    logger.warning(
                        "Message cleanup after failed processing was a no-op: "
                        "the lease expired and the message was likely reclaimed by another consumer. "
                        "This is expected at-least-once delivery behavior under visibility timeout."
                    )
            except BaseException:
                logger.exception("Failed to clean up message from processing queue")
            raise
        else:
            if lease_heartbeat is not None:
                lease_heartbeat.suppress_failure_callback()
            if self._enable_completed_queue:
                applied = self._move_processed_message(self.key.completed, stored_message, lease_token)
            else:
                applied = self._remove_processed_message(stored_message, lease_token)
            if lease_token is not None and not applied:
                logger.warning(
                    "Message cleanup after successful processing was a no-op: "
                    "the lease expired and the message was likely reclaimed by another consumer. "
                    "This is expected at-least-once delivery behavior under visibility timeout."
                )
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
            raise TypeError(f"gateway.move_message() must return bool, got {type(result).__name__}")
        if result:
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
            except Exception:
                logger.warning("Failed to trim queue %s", destination_queue, exc_info=True)

    def _remove_processed_message(self, stored_message: MessageData, lease_token: str | None) -> bool:
        if lease_token is None:
            result = self._redis.remove_message(self.key.processing, stored_message)
        else:
            result = self._redis.remove_message(self.key.processing, stored_message, lease_token=lease_token)
        if not isinstance(result, bool):
            raise TypeError(f"gateway.remove_message() must return bool, got {type(result).__name__}")
        return result

    def _build_lease_heartbeat(
        self,
        stored_message: MessageData,
        lease_token: str | None,
    ) -> _LeaseHeartbeat | None:
        if lease_token is None or self._heartbeat_interval_seconds is None:
            return None
        return _LeaseHeartbeat(
            interval_seconds=float(self._heartbeat_interval_seconds),
            renew_message_lease=lambda: self._redis.renew_message_lease(
                self.key.processing,
                stored_message,
                lease_token,
            ),
            on_heartbeat_failure=self._on_heartbeat_failure,
        )
