from typing import Callable, Optional

import redis.asyncio
import redis.exceptions

from redis_message_queue._abstract_redis_gateway import AbstractRedisGateway
from redis_message_queue._config import (
    DEFAULT_MESSAGE_DEDUPLICATION_LOG_TTL,
    DEFAULT_MESSAGE_WAIT_INTERVAL_SECONDS,
    DEFAULT_REDIS_CONNECTION_RETRY_STRATEGY,
)


class RedisGateway(AbstractRedisGateway):
    def __init__(
        self,
        *,
        redis_client: redis.asyncio.Redis,
        retry_strategy: Optional[Callable] = None,
        message_deduplication_log_ttl_seconds: Optional[int] = None,
        message_wait_interval_seconds: Optional[int] = None,
    ):
        self._redis_client = redis_client
        self._retry_strategy = retry_strategy or DEFAULT_REDIS_CONNECTION_RETRY_STRATEGY
        self._message_deduplication_log_ttl_seconds = (
            message_deduplication_log_ttl_seconds
            or DEFAULT_MESSAGE_DEDUPLICATION_LOG_TTL
        )
        self._message_wait_interval_seconds = (
            message_wait_interval_seconds or DEFAULT_MESSAGE_WAIT_INTERVAL_SECONDS
        )

    async def add_if_absent(self, key: str, value: str = "") -> bool:
        @self._retry_strategy
        async def _insert():
            async with self._redis_client.pipeline(transaction=True) as pipe:
                pipe.setnx(name=key, value=value)
                pipe.expire(name=key, time=self._message_deduplication_log_ttl_seconds)
                result = await pipe.execute()
                return result[0]  # Only interested in the result of setnx

        return await _insert()

    async def add_message(self, queue: str, message: str) -> None:
        @self._retry_strategy
        async def _add():
            await self._redis_client.lpush(queue, message)  # type: ignore

        await _add()

    async def move_message(
        self, from_queue: str, to_queue: str, message: bytes
    ) -> None:
        @self._retry_strategy
        async def _move():
            async with self._redis_client.pipeline(transaction=True) as pipe:
                pipe.lpush(to_queue, message)
                pipe.lrem(from_queue, 1, message)  # type: ignore
                await pipe.execute()

        await _move()

    async def remove_message(self, queue: str, message: bytes) -> None:
        @self._retry_strategy
        async def _remove():
            await self._redis_client.lrem(queue, 1, message)  # type: ignore

        await _remove()

    async def wait_for_message_and_move(self, from_queue: str, to_queue: str):
        @self._retry_strategy
        async def _wait():
            return await self._redis_client.brpoplpush(
                from_queue,
                to_queue,
                timeout=self._message_wait_interval_seconds,
            )  # type: ignore

        return await _wait()
