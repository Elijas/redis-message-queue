from typing import Callable, Optional

import redis.asyncio
import redis.exceptions

from redis_message_queue._config import (
    DEFAULT_MESSAGE_DEDUPLICATION_LOG_TTL,
    DEFAULT_MESSAGE_WAIT_INTERVAL_SECONDS,
    PUBLISH_MESSAGE_LUA_SCRIPT,
    get_default_redis_connection_retry_strategy,
    is_redis_unknown_command_error,
    validate_gateway_parameters,
)
from redis_message_queue._stored_message import MessageData, decode_stored_message, encode_stored_message
from redis_message_queue.asyncio._abstract_redis_gateway import AbstractRedisGateway
from redis_message_queue.interrupt_handler._interface import (
    BaseGracefulInterruptHandler,
)


class RedisGateway(AbstractRedisGateway):
    def __init__(
        self,
        *,
        redis_client: redis.asyncio.Redis,
        retry_strategy: Optional[Callable] = None,
        message_deduplication_log_ttl_seconds: Optional[int] = None,
        message_wait_interval_seconds: Optional[int] = None,
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
        validate_gateway_parameters(
            self._message_deduplication_log_ttl_seconds,
            self._message_wait_interval_seconds,
        )

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

    async def move_message(self, from_queue: str, to_queue: str, message: MessageData) -> None:
        decoded_message = decode_stored_message(message)

        @self._retry_strategy
        async def _move():
            async with self._redis_client.pipeline(transaction=True) as pipe:
                pipe.lpush(to_queue, decoded_message)
                pipe.lrem(from_queue, 1, message)  # type: ignore
                await pipe.execute()

        await _move()

    async def remove_message(self, queue: str, message: MessageData) -> None:
        @self._retry_strategy
        async def _remove():
            await self._redis_client.lrem(queue, 1, message)  # type: ignore

        await _remove()

    async def wait_for_message_and_move(self, from_queue: str, to_queue: str):
        @self._retry_strategy
        async def _wait():
            # Redis treats BLMOVE timeout=0 as "block forever", so use LMOVE
            # to preserve the library's non-blocking zero-timeout behavior.
            if self._message_wait_interval_seconds == 0:
                try:
                    return await self._redis_client.lmove(from_queue, to_queue, "RIGHT", "LEFT")
                except redis.exceptions.ResponseError as exc:
                    if not is_redis_unknown_command_error(exc, "lmove"):
                        raise
                    return await self._redis_client.rpoplpush(from_queue, to_queue)
            try:
                return await self._redis_client.blmove(
                    from_queue,
                    to_queue,
                    timeout=self._message_wait_interval_seconds,
                    src="RIGHT",
                    dest="LEFT",
                )
            except redis.exceptions.ResponseError as exc:
                if not is_redis_unknown_command_error(exc, "blmove"):
                    raise
                return await self._redis_client.brpoplpush(
                    from_queue,
                    to_queue,
                    self._message_wait_interval_seconds,
                )

        return await _wait()
