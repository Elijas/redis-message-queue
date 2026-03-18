import asyncio
import logging
from typing import Callable, Optional

import redis.asyncio

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

_LEASE_DEADLINES_SUFFIX = ":lease_deadlines"
_LEASE_TOKENS_SUFFIX = ":lease_tokens"
_LEASE_TOKEN_COUNTER_SUFFIX = ":lease_token_counter"
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
        interrupt: BaseGracefulInterruptHandler | None = None,
    ):
        self._redis_client = redis_client
        if retry_strategy is not None and not callable(retry_strategy):
            raise TypeError(f"'retry_strategy' must be callable, got {type(retry_strategy).__name__}")
        if interrupt is not None and not isinstance(interrupt, BaseGracefulInterruptHandler):
            raise TypeError(f"'interrupt' must be a BaseGracefulInterruptHandler, got {type(interrupt).__name__}")
        if retry_strategy is not None and interrupt is not None:
            raise ValueError(
                "'retry_strategy' and 'interrupt' cannot both be provided."
                " Either use the default retry strategy with 'interrupt',"
                " or provide a custom 'retry_strategy' that handles interrupts directly."
            )
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

    @property
    def message_visibility_timeout_seconds(self) -> int | None:
        return self._message_visibility_timeout_seconds

    async def publish_message(self, queue: str, message: str, dedup_key: str) -> bool:
        stored_message = encode_stored_message(message)

        @self._retry_strategy
        async def _publish():
            return bool(
                await self._redis_client.eval(
                    PUBLISH_MESSAGE_LUA_SCRIPT,
                    2,
                    dedup_key,
                    queue,
                    str(self._message_deduplication_log_ttl_seconds),
                    stored_message,
                )
            )

        return await _publish()

    async def add_message(self, queue: str, message: str) -> None:
        stored_message = encode_stored_message(message)

        @self._retry_strategy
        async def _add():
            await self._redis_client.lpush(queue, stored_message)  # type: ignore

        await _add()

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

            @self._retry_strategy
            async def _move():
                return bool(
                    await self._redis_client.eval(
                        MOVE_MESSAGE_LUA_SCRIPT,
                        2,
                        from_queue,
                        to_queue,
                        message,
                        decoded_message,
                    )
                )

            return await _move()

        @self._retry_strategy
        async def _move_with_lease():
            return bool(
                await self._redis_client.eval(
                    MOVE_MESSAGE_WITH_LEASE_TOKEN_LUA_SCRIPT,
                    4,
                    from_queue,
                    to_queue,
                    self._lease_deadlines_key(from_queue),
                    self._lease_tokens_key(from_queue),
                    message,
                    decoded_message,
                    lease_token,
                )
            )

        return await _move_with_lease()

    async def remove_message(self, queue: str, message: MessageData, *, lease_token: str | None = None) -> bool:
        if lease_token is None:

            @self._retry_strategy
            async def _remove():
                return bool(await self._redis_client.lrem(queue, 1, message))  # type: ignore

            return await _remove()

        @self._retry_strategy
        async def _remove_with_lease():
            return bool(
                await self._redis_client.eval(
                    REMOVE_MESSAGE_WITH_LEASE_TOKEN_LUA_SCRIPT,
                    3,
                    queue,
                    self._lease_deadlines_key(queue),
                    self._lease_tokens_key(queue),
                    message,
                    lease_token,
                )
            )

        return await _remove_with_lease()

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

    async def wait_for_message_and_move(self, from_queue: str, to_queue: str) -> ClaimedMessage | MessageData | None:
        if self._message_visibility_timeout_seconds is not None:
            return await self._wait_for_message_with_visibility_timeout(from_queue, to_queue)

        # Retrying a move after the server may already have mutated queue state can
        # consume an extra message or hide the one already moved into processing.
        if self._message_wait_interval_seconds == 0:
            return await self._redis_client.lmove(from_queue, to_queue, "RIGHT", "LEFT")
        return await self._redis_client.blmove(
            from_queue,
            to_queue,
            timeout=self._message_wait_interval_seconds,
            src="RIGHT",
            dest="LEFT",
        )

    async def _wait_for_message_with_visibility_timeout(self, from_queue: str, to_queue: str) -> ClaimedMessage | None:
        if self._message_wait_interval_seconds == 0:
            return await self._claim_visible_message(from_queue, to_queue)

        deadline = asyncio.get_running_loop().time() + self._message_wait_interval_seconds
        while True:
            try:
                claimed_message = await self._claim_visible_message(from_queue, to_queue)
            except Exception as exc:
                if not is_redis_retryable_exception(exc):
                    raise
                logger.warning("Transient error during visibility-timeout claim poll, will retry: %s", exc)
            else:
                if claimed_message is not None:
                    return claimed_message

            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return None
            await asyncio.sleep(min(_VISIBILITY_TIMEOUT_POLL_INTERVAL_SECONDS, remaining))

    async def _claim_visible_message(self, from_queue: str, to_queue: str) -> ClaimedMessage | None:
        result = await self._redis_client.eval(
            CLAIM_MESSAGE_WITH_VISIBILITY_TIMEOUT_LUA_SCRIPT,
            5,
            from_queue,
            to_queue,
            self._lease_deadlines_key(to_queue),
            self._lease_tokens_key(to_queue),
            self._lease_token_counter_key(to_queue),
            str(self._message_visibility_timeout_seconds * 1000),
        )
        if result is None:
            return None

        stored_message, lease_token = result
        if isinstance(lease_token, bytes):
            lease_token = lease_token.decode("utf-8")
        return ClaimedMessage(stored_message=stored_message, lease_token=lease_token)

    def _lease_deadlines_key(self, processing_queue: str) -> str:
        return f"{processing_queue}{_LEASE_DEADLINES_SUFFIX}"

    def _lease_tokens_key(self, processing_queue: str) -> str:
        return f"{processing_queue}{_LEASE_TOKENS_SUFFIX}"

    def _lease_token_counter_key(self, processing_queue: str) -> str:
        return f"{processing_queue}{_LEASE_TOKEN_COUNTER_SUFFIX}"
