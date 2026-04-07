import logging
import time
import uuid
from typing import Callable, Optional

import redis
import redis.asyncio

from redis_message_queue._abstract_redis_gateway import AbstractRedisGateway
from redis_message_queue._callable_utils import is_async_callable
from redis_message_queue._config import (
    CLAIM_MESSAGE_WITH_VISIBILITY_TIMEOUT_LUA_SCRIPT,
    DEFAULT_MESSAGE_DEDUPLICATION_LOG_TTL,
    DEFAULT_MESSAGE_WAIT_INTERVAL_SECONDS,
    MOVE_MESSAGE_LUA_SCRIPT,
    MOVE_MESSAGE_WITH_LEASE_TOKEN_LUA_SCRIPT,
    PUBLISH_MESSAGE_LUA_SCRIPT,
    REMOVE_MESSAGE_WITH_LEASE_TOKEN_LUA_SCRIPT,
    RENEW_MESSAGE_LEASE_LUA_SCRIPT,
    get_default_redis_connection_retry_strategy,
    is_redis_retryable_exception,
    validate_dead_letter_parameters,
    validate_gateway_parameters,
)
from redis_message_queue._stored_message import (
    ClaimedMessage,
    MessageData,
    decode_stored_message,
    encode_stored_message,
)
from redis_message_queue.interrupt_handler._interface import (
    BaseGracefulInterruptHandler,
)

logger = logging.getLogger(__name__)

_LEASE_DEADLINES_SUFFIX = ":lease_deadlines"
_LEASE_TOKENS_SUFFIX = ":lease_tokens"
_LEASE_TOKEN_COUNTER_SUFFIX = ":lease_token_counter"
_DELIVERY_COUNTS_SUFFIX = ":delivery_counts"
_CLAIM_RESULT_SUFFIX = ":claim_result"
_CLAIM_RESULT_REFS_SUFFIX = ":claim_result_refs"
_LEASE_OPERATION_RESULT_SUFFIX = ":lease_operation_result"
_OPTIONAL_DEAD_LETTER_PLACEHOLDER_SUFFIX = ":dead_letter_placeholder"
_VISIBILITY_TIMEOUT_POLL_INTERVAL_SECONDS = 0.25


class RedisGateway(AbstractRedisGateway):
    def __init__(
        self,
        *,
        redis_client: redis.Redis,
        retry_strategy: Optional[Callable] = None,
        message_deduplication_log_ttl_seconds: Optional[int] = None,
        message_wait_interval_seconds: Optional[int] = None,
        message_visibility_timeout_seconds: Optional[int] = None,
        max_delivery_count: int | None = None,
        dead_letter_queue: str | None = None,
        interrupt: BaseGracefulInterruptHandler | None = None,
    ):
        if isinstance(redis_client, redis.asyncio.Redis):
            raise TypeError(
                "'redis_client' is an async Redis client (redis.asyncio.Redis); "
                "use the async RedisGateway from redis_message_queue.asyncio instead"
            )
        self._redis_client = redis_client
        if retry_strategy is not None and not callable(retry_strategy):
            raise TypeError(f"'retry_strategy' must be callable, got {type(retry_strategy).__name__}")
        if retry_strategy is not None and is_async_callable(retry_strategy):
            raise TypeError(
                "'retry_strategy' is an async callable; "
                "use the async RedisGateway from redis_message_queue.asyncio instead"
            )
        if interrupt is not None and not isinstance(interrupt, BaseGracefulInterruptHandler):
            raise TypeError(f"'interrupt' must be a BaseGracefulInterruptHandler, got {type(interrupt).__name__}")
        self._interrupt = interrupt
        self._retry_strategy = (
            get_default_redis_connection_retry_strategy(interrupt=interrupt)
            if retry_strategy is None
            else retry_strategy
        )
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
        )
        validate_dead_letter_parameters(
            max_delivery_count,
            dead_letter_queue,
            self._message_visibility_timeout_seconds,
        )
        self._max_delivery_count = max_delivery_count
        self._dead_letter_queue = dead_letter_queue

    @property
    def message_visibility_timeout_seconds(self) -> int | None:
        return self._message_visibility_timeout_seconds

    @property
    def max_delivery_count(self) -> int | None:
        return self._max_delivery_count

    def publish_message(self, queue: str, message: str, dedup_key: str) -> bool:
        stored_message = encode_stored_message(message)

        @self._retry_strategy
        def _publish():
            return bool(
                self._redis_client.eval(
                    PUBLISH_MESSAGE_LUA_SCRIPT,
                    2,
                    dedup_key,
                    queue,
                    str(self._message_deduplication_log_ttl_seconds),
                    stored_message,
                )
            )

        return _publish()

    def add_message(self, queue: str, message: str) -> None:
        # Retrying LPUSH after the server may already have executed it can
        # silently duplicate the message. Let the exception propagate so the
        # caller can decide whether to retry (accepting potential duplicates).
        stored_message = encode_stored_message(message)
        self._redis_client.lpush(queue, stored_message)

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

            @self._retry_strategy
            def _move():
                return bool(
                    self._redis_client.eval(
                        MOVE_MESSAGE_LUA_SCRIPT,
                        2,
                        from_queue,
                        to_queue,
                        message,
                        decoded_message,
                    )
                )

            return _move()

        operation_id = uuid.uuid4().hex
        operation_result_key = self._lease_operation_result_key(from_queue, lease_token, operation_id)

        @self._retry_strategy
        def _move_with_lease():
            return bool(
                self._redis_client.eval(
                    MOVE_MESSAGE_WITH_LEASE_TOKEN_LUA_SCRIPT,
                    7,
                    from_queue,
                    to_queue,
                    self._lease_deadlines_key(from_queue),
                    self._lease_tokens_key(from_queue),
                    self._delivery_counts_key(from_queue),
                    self._claim_result_refs_key(from_queue),
                    operation_result_key,
                    message,
                    decoded_message,
                    lease_token,
                    self._lease_operation_result_ttl_ms(),
                )
            )

        try:
            return _move_with_lease()
        finally:
            self._delete_lease_operation_result_key(operation_result_key)

    def remove_message(self, queue: str, message: MessageData, *, lease_token: str | None = None) -> bool:
        if lease_token is None:

            @self._retry_strategy
            def _remove():
                return bool(self._redis_client.lrem(queue, 1, message))  # type: ignore

            return _remove()

        operation_id = uuid.uuid4().hex
        operation_result_key = self._lease_operation_result_key(queue, lease_token, operation_id)

        @self._retry_strategy
        def _remove_with_lease():
            return bool(
                self._redis_client.eval(
                    REMOVE_MESSAGE_WITH_LEASE_TOKEN_LUA_SCRIPT,
                    6,
                    queue,
                    self._lease_deadlines_key(queue),
                    self._lease_tokens_key(queue),
                    self._delivery_counts_key(queue),
                    self._claim_result_refs_key(queue),
                    operation_result_key,
                    message,
                    lease_token,
                    self._lease_operation_result_ttl_ms(),
                )
            )

        try:
            return _remove_with_lease()
        finally:
            self._delete_lease_operation_result_key(operation_result_key)

    def renew_message_lease(self, queue: str, message: MessageData, lease_token: str) -> bool:
        if self._message_visibility_timeout_seconds is None:
            return False

        @self._retry_strategy
        def _renew():
            return bool(
                self._redis_client.eval(
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

    def wait_for_message_and_move(self, from_queue: str, to_queue: str) -> ClaimedMessage | MessageData | None:
        if self._is_interrupted():
            return None
        if self._message_visibility_timeout_seconds is not None:
            return self._wait_for_message_with_visibility_timeout(from_queue, to_queue)

        # Retrying a move after the server may already have mutated queue state can
        # consume an extra message or hide the one already moved into processing.
        if self._message_wait_interval_seconds == 0:
            return self._redis_client.lmove(from_queue, to_queue, "RIGHT", "LEFT")
        if self._interrupt is None:
            return self._redis_client.blmove(
                from_queue,
                to_queue,
                timeout=self._message_wait_interval_seconds,
                src="RIGHT",
                dest="LEFT",
            )
        deadline = time.monotonic() + self._message_wait_interval_seconds
        while True:
            if self._is_interrupted():
                return None
            message = self._redis_client.lmove(from_queue, to_queue, "RIGHT", "LEFT")
            if message is not None:
                return message
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            time.sleep(min(_VISIBILITY_TIMEOUT_POLL_INTERVAL_SECONDS, remaining))

    def _wait_for_message_with_visibility_timeout(self, from_queue: str, to_queue: str) -> ClaimedMessage | None:
        if self._is_interrupted():
            return None
        if self._message_wait_interval_seconds == 0:
            claim_id = uuid.uuid4().hex
            try:
                return self._claim_visible_message(from_queue, to_queue, claim_id=claim_id)
            except Exception as exc:
                if not is_redis_retryable_exception(exc):
                    raise
                logger.warning(
                    "Transient error during visibility-timeout non-blocking claim, "
                    "retrying once to recover claim: %s",
                    exc,
                )
                return self._claim_visible_message(from_queue, to_queue, claim_id=claim_id)

        deadline = time.monotonic() + self._message_wait_interval_seconds
        claim_id = uuid.uuid4().hex
        last_retryable_exception: Exception | None = None
        while True:
            if self._is_interrupted():
                return None
            try:
                claimed_message = self._claim_visible_message(from_queue, to_queue, claim_id=claim_id)
            except Exception as exc:
                if not is_redis_retryable_exception(exc):
                    raise
                logger.warning("Transient error during visibility-timeout claim poll, will retry: %s", exc)
                last_retryable_exception = exc
            else:
                if claimed_message is not None:
                    return claimed_message
                last_retryable_exception = None
                claim_id = uuid.uuid4().hex

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                if last_retryable_exception is not None:
                    try:
                        claimed_message = self._claim_visible_message(from_queue, to_queue, claim_id=claim_id)
                    except Exception as exc:
                        if not is_redis_retryable_exception(exc):
                            raise
                        raise exc
                    if claimed_message is not None:
                        return claimed_message
                    raise last_retryable_exception
                return None
            time.sleep(min(_VISIBILITY_TIMEOUT_POLL_INTERVAL_SECONDS, remaining))

    def _claim_visible_message(self, from_queue: str, to_queue: str, *, claim_id: str) -> ClaimedMessage | None:
        result = self._redis_client.eval(
            CLAIM_MESSAGE_WITH_VISIBILITY_TIMEOUT_LUA_SCRIPT,
            9,
            from_queue,
            to_queue,
            self._lease_deadlines_key(to_queue),
            self._lease_tokens_key(to_queue),
            self._lease_token_counter_key(to_queue),
            self._delivery_counts_key(to_queue),
            self._optional_dead_letter_key(to_queue),
            self._claim_result_key(to_queue, claim_id),
            self._claim_result_refs_key(to_queue),
            str(self._message_visibility_timeout_seconds * 1000),
            str(self._max_delivery_count or 0),
            str(self._message_visibility_timeout_seconds * 1000),
        )
        if result is None:
            return None

        stored_message, lease_token = result
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

    def _optional_dead_letter_key(self, processing_queue: str) -> str:
        if self._dead_letter_queue is not None:
            return self._dead_letter_queue
        return f"{processing_queue}{_OPTIONAL_DEAD_LETTER_PLACEHOLDER_SUFFIX}"

    def _lease_operation_result_key(self, processing_queue: str, lease_token: str, operation_id: str) -> str:
        return f"{processing_queue}{_LEASE_OPERATION_RESULT_SUFFIX}:{lease_token}:{operation_id}"

    def _lease_operation_result_ttl_ms(self) -> str:
        ttl_seconds = self._message_visibility_timeout_seconds
        if ttl_seconds is None:
            ttl_seconds = 120
        return str(max(ttl_seconds, 120) * 1000)

    def _delete_lease_operation_result_key(self, operation_result_key: str) -> None:
        try:
            self._redis_client.delete(operation_result_key)
        except Exception:
            logger.debug("Failed to delete lease operation result key %s", operation_result_key, exc_info=True)

    def _is_interrupted(self) -> bool:
        return self._interrupt is not None and self._interrupt.is_interrupted()
