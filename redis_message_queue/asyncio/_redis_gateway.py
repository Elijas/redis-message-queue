import asyncio
import json
import logging
import threading
import uuid
from typing import Awaitable, Callable, Optional, TypeVar

import redis
import redis.asyncio

from redis_message_queue._callable_utils import is_async_callable
from redis_message_queue._config import (
    CLAIM_MESSAGE_LUA_SCRIPT,
    CLAIM_MESSAGE_WITH_VISIBILITY_TIMEOUT_LUA_SCRIPT,
    DEFAULT_MESSAGE_DEDUPLICATION_LOG_TTL,
    DEFAULT_MESSAGE_WAIT_INTERVAL_SECONDS,
    MOVE_MESSAGE_LUA_SCRIPT,
    MOVE_MESSAGE_WITH_LEASE_TOKEN_LUA_SCRIPT,
    PUBLISH_MESSAGE_LUA_SCRIPT,
    REMOVE_MESSAGE_LUA_SCRIPT,
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
from redis_message_queue.asyncio._abstract_redis_gateway import AbstractRedisGateway
from redis_message_queue.interrupt_handler._interface import (
    BaseGracefulInterruptHandler,
)

logger = logging.getLogger(__name__)
_TClaim = TypeVar("_TClaim", bound=ClaimedMessage | MessageData)

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


class RedisGateway(AbstractRedisGateway):
    def __init__(
        self,
        *,
        redis_client: redis.asyncio.Redis,
        retry_strategy: Optional[Callable] = None,
        message_deduplication_log_ttl_seconds: Optional[int] = None,
        message_wait_interval_seconds: Optional[int] = None,
        message_visibility_timeout_seconds: Optional[int] = None,
        max_delivery_count: int | None = None,
        dead_letter_queue: str | None = None,
        interrupt: BaseGracefulInterruptHandler | None = None,
    ):
        if isinstance(redis_client, redis.Redis) and not isinstance(redis_client, redis.asyncio.Redis):
            raise TypeError(
                "'redis_client' is a sync Redis client (redis.Redis); "
                "use the sync RedisGateway from redis_message_queue instead"
            )
        if isinstance(redis_client, (redis.client.Pipeline, redis.asyncio.client.Pipeline)):
            raise TypeError(
                "'redis_client' is a Pipeline, not a Redis client; "
                "Pipeline defers execution and would silently drop writes. "
                "Pass the underlying redis.asyncio.Redis instance instead."
            )
        self._redis_client = redis_client
        if retry_strategy is not None and not callable(retry_strategy):
            raise TypeError(f"'retry_strategy' must be callable, got {type(retry_strategy).__name__}")
        if retry_strategy is not None and is_async_callable(retry_strategy):
            raise TypeError(
                "'retry_strategy' must not be an async callable. "
                "Provide a synchronous callable decorator (e.g., tenacity.retry(...))"
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
        self._pending_claim_ids: dict[str, list[str]] = {}
        self._recovering_claim_ids: dict[str, set[str]] = {}
        self._pending_claim_ids_lock = threading.Lock()

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

    async def publish_message(self, queue: str, message: str, dedup_key: str) -> bool:
        stored_message = encode_stored_message(message)
        operation_id = uuid.uuid4().hex
        operation_result_key = self._publish_operation_result_key(dedup_key, operation_id)

        @self._retry_strategy
        async def _publish():
            return bool(
                await self._redis_client.eval(
                    PUBLISH_MESSAGE_LUA_SCRIPT,
                    3,
                    dedup_key,
                    queue,
                    operation_result_key,
                    str(self._message_deduplication_log_ttl_seconds),
                    stored_message,
                    self._publish_operation_result_ttl_ms(),
                )
            )

        try:
            return await _publish()
        finally:
            await self._delete_operation_result_key(operation_result_key)

    async def add_message(self, queue: str, message: str) -> None:
        """Non-deduplicated enqueue. Must not be retried to keep at-most-once.

        This library deliberately does not wrap the LPUSH in a retry — retrying
        after the server may already have executed the command can silently
        duplicate the message. The caller can still retry (accepting duplicates).

        Note: a client-level retry policy bypasses this guarantee. If the
        ``redis.asyncio.Redis`` client was constructed with ``retry=Retry(...)``,
        redis-py retries on ``ConnectionError``/``TimeoutError`` below this
        call and may duplicate. Pass ``retry=None`` (the default) when strict
        at-most-once is required for non-deduplicated publishes.
        """
        stored_message = encode_stored_message(message)
        await self._redis_client.lpush(queue, stored_message)  # type: ignore

    async def move_message(
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
            async def _move():
                return bool(
                    await self._redis_client.eval(
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
            finally:
                await self._delete_operation_result_key(operation_result_key)

        operation_id = uuid.uuid4().hex
        operation_result_key = self._lease_operation_result_key(from_queue, lease_token, operation_id)

        @self._retry_strategy
        async def _move_with_lease():
            return bool(
                await self._redis_client.eval(
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
        finally:
            await self._delete_operation_result_key(operation_result_key)

    async def remove_message(self, queue: str, message: MessageData, *, lease_token: str | None = None) -> bool:
        if lease_token is None:
            operation_id = uuid.uuid4().hex
            operation_result_key = self._operation_result_key(queue, operation_id)

            @self._retry_strategy
            async def _remove():
                return bool(
                    await self._redis_client.eval(
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
            finally:
                await self._delete_operation_result_key(operation_result_key)

        operation_id = uuid.uuid4().hex
        operation_result_key = self._lease_operation_result_key(queue, lease_token, operation_id)

        @self._retry_strategy
        async def _remove_with_lease():
            return bool(
                await self._redis_client.eval(
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
        finally:
            await self._delete_operation_result_key(operation_result_key)

    async def renew_message_lease(self, queue: str, message: MessageData, lease_token: str) -> bool:
        if self._message_visibility_timeout_seconds is None:
            return False

        @self._retry_strategy
        async def _renew():
            return bool(
                await self._redis_client.eval(
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
    ) -> _TClaim | None:
        while True:
            pending_claim_id = self._acquire_pending_claim_id(to_queue)
            if pending_claim_id is None:
                break
            clear_pending_claim_id = False
            try:
                recovered_claim = await recover_pending_claim(to_queue, pending_claim_id)
                clear_pending_claim_id = True
            finally:
                self._finish_pending_claim_recovery(
                    to_queue,
                    pending_claim_id,
                    clear=clear_pending_claim_id,
                )
            if recovered_claim is not None:
                return recovered_claim

        if self._is_interrupted():
            return None

        pending_claim_id_to_share: str | None = None
        try:
            if self._message_wait_interval_seconds == 0:
                claim_id = uuid.uuid4().hex
                claim_may_need_recovery = False
                try:
                    claimed_message = await claim_message(from_queue, to_queue, claim_id)
                except Exception as exc:
                    if not is_redis_retryable_exception(exc):
                        raise
                    claim_may_need_recovery = True
                    logger.warning(non_blocking_retry_log, exc)
                    if self._is_interrupted():
                        pending_claim_id_to_share = claim_id
                        return None
                    try:
                        claimed_message = await claim_message(from_queue, to_queue, claim_id)
                    except Exception:
                        if claim_may_need_recovery:
                            pending_claim_id_to_share = claim_id
                        raise
                    except BaseException:
                        pending_claim_id_to_share = claim_id
                        raise
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
                if self._is_interrupted():
                    if claim_may_need_recovery:
                        pending_claim_id_to_share = claim_id
                    return None
                try:
                    claimed_message = await claim_message(from_queue, to_queue, claim_id)
                except Exception as exc:
                    if not is_redis_retryable_exception(exc):
                        if claim_may_need_recovery:
                            pending_claim_id_to_share = claim_id
                        raise
                    claim_may_need_recovery = True
                    logger.warning(polling_retry_log, exc)
                    last_retryable_exception = exc
                except BaseException:
                    pending_claim_id_to_share = claim_id
                    raise
                else:
                    if claimed_message is not None:
                        return claimed_message
                    claim_may_need_recovery = False
                    last_retryable_exception = None
                    claim_id = uuid.uuid4().hex

                remaining = deadline - loop.time()
                if remaining <= 0:
                    if last_retryable_exception is not None:
                        if self._is_interrupted():
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
                            return recovered_claim
                        raise last_retryable_exception
                    return None
                await asyncio.sleep(min(_VISIBILITY_TIMEOUT_POLL_INTERVAL_SECONDS, remaining))
        finally:
            if pending_claim_id_to_share is not None:
                self._set_pending_claim_id(to_queue, pending_claim_id_to_share)

    async def wait_for_message_and_move(self, from_queue: str, to_queue: str) -> ClaimedMessage | MessageData | None:
        if self._is_interrupted():
            return None
        if self._message_visibility_timeout_seconds is not None:
            return await self._wait_for_message_with_visibility_timeout(from_queue, to_queue)
        return await self._wait_for_message_without_visibility_timeout(from_queue, to_queue)

    async def _wait_for_message_without_visibility_timeout(self, from_queue: str, to_queue: str) -> MessageData | None:
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
        )

    async def _wait_for_message_with_visibility_timeout(self, from_queue: str, to_queue: str) -> ClaimedMessage | None:
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
        )

    async def _claim_message_without_visibility_timeout(
        self,
        from_queue: str,
        to_queue: str,
        *,
        claim_id: str,
    ) -> MessageData | None:
        claim_result_key = self._claim_result_key(to_queue, claim_id)
        result = await self._redis_client.eval(
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
        result = await self._redis_client.eval(
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

        stored_message, lease_token = result
        if isinstance(lease_token, bytes):
            lease_token = lease_token.decode("utf-8")
        return ClaimedMessage(stored_message=stored_message, lease_token=lease_token)

    async def trim_queue(self, queue: str, max_length: int) -> None:
        await self._redis_client.ltrim(queue, 0, max_length - 1)

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
        return str(max(self._message_deduplication_log_ttl_seconds, 3600) * 1000)

    def _operation_result_ttl_ms(self) -> str:
        # Floor is 300s so the cached result outlives tenacity's
        # stop_after_delay(120) retry budget with margin. Equal deadlines
        # produce a boundary race where a retry arriving past 120s finds the
        # cache just expired and wrongly returns 0.
        #
        # This is ALSO an upper bound on any caller-supplied ``retry_strategy``:
        # a custom retry budget longer than max(visibility_timeout, 300) can
        # step past this TTL and re-run the Lua with a stale cache, causing an
        # already-acked move/remove to report False. Documented in README under
        # the custom gateway section.
        ttl_seconds = self._message_visibility_timeout_seconds
        if ttl_seconds is None:
            ttl_seconds = 120
        return str(max(ttl_seconds, 300) * 1000)

    def _lease_operation_result_ttl_ms(self) -> str:
        return self._operation_result_ttl_ms()

    def _claim_result_ttl_ms(self) -> str:
        return str(max(self._message_wait_interval_seconds, 120) * 1000)

    async def _delete_claim_result_key(self, claim_result_key: str) -> None:
        try:
            await self._redis_client.delete(claim_result_key)
        except Exception:
            logger.debug("Failed to delete claim result key %s", claim_result_key, exc_info=True)

    async def _delete_claim_result_ref(self, claim_result_refs_key: str, lease_token: str) -> None:
        try:
            await self._redis_client.hdel(claim_result_refs_key, lease_token)
        except Exception:
            logger.debug(
                "Failed to delete claim result reference %s[%s]",
                claim_result_refs_key,
                lease_token,
                exc_info=True,
            )

    async def _delete_operation_result_key(self, operation_result_key: str) -> None:
        try:
            await self._redis_client.delete(operation_result_key)
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
    ) -> MessageData | None:
        claim_result_key = self._claim_result_key(processing_queue, claim_id)
        cached_claim = await self._redis_client.get(claim_result_key)
        if cached_claim is None:
            if self._is_interrupted():
                return None
            cached_claim = await self._redis_client.hget(self._claim_result_ids_key(processing_queue), claim_id)
            if cached_claim is None:
                return None
        await self._delete_claim_result_key(claim_result_key)
        return cached_claim

    async def _recover_pending_visibility_timeout_claim(
        self,
        processing_queue: str,
        claim_id: str,
    ) -> ClaimedMessage | None:
        claim_result_key = self._claim_result_key(processing_queue, claim_id)
        cached_claim = await self._redis_client.get(claim_result_key)
        if cached_claim is None:
            if self._is_interrupted():
                return None
            cached_claim = await self._redis_client.hget(self._claim_result_ids_key(processing_queue), claim_id)
            if cached_claim is None:
                return None

        cached_claim_text = cached_claim.decode("utf-8") if isinstance(cached_claim, bytes) else cached_claim
        try:
            claim = json.loads(cached_claim_text)
        except json.JSONDecodeError:
            await self._delete_claim_result_key(claim_result_key)
            return None

        if (
            not isinstance(claim, list)
            or len(claim) < 2
            or not isinstance(claim[0], str)
            or not isinstance(claim[1], str)
        ):
            await self._delete_claim_result_key(claim_result_key)
            return None

        stored_message: MessageData = claim[0]
        if isinstance(cached_claim, bytes):
            stored_message = stored_message.encode("utf-8")
        lease_token = claim[1]

        await self._delete_claim_result_key(claim_result_key)
        await self._delete_claim_result_ref(self._claim_result_refs_key(processing_queue), lease_token)
        return ClaimedMessage(stored_message=stored_message, lease_token=lease_token)

    def _is_interrupted(self) -> bool:
        return self._interrupt is not None and self._interrupt.is_interrupted()
