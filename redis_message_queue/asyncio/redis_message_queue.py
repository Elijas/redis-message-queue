import inspect
import json
import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator, Callable, Optional

import redis.asyncio
import redis.exceptions

from redis_message_queue._queue_key_manager import QueueKeyManager
from redis_message_queue.asyncio._abstract_redis_gateway import AbstractRedisGateway
from redis_message_queue.asyncio._redis_gateway import RedisGateway
from redis_message_queue.interrupt_handler import BaseGracefulInterruptHandler

logger = logging.getLogger(__name__)


class RedisMessageQueue:
    def __init__(
        self,
        name: str,
        *,
        gateway: Optional[AbstractRedisGateway] = None,
        client: Optional[redis.asyncio.Redis] = None,
        deduplication: bool = True,
        enable_completed_queue: bool = False,
        enable_failed_queue: bool = False,
        key_separator: str = "::",
        get_deduplication_key: Optional[Callable] = None,
        interrupt: BaseGracefulInterruptHandler | None = None,
    ):
        self.key = QueueKeyManager(name, key_separator=key_separator)
        if not isinstance(deduplication, bool):
            raise TypeError(f"'deduplication' must be a bool, got {type(deduplication).__name__}")
        if not isinstance(enable_completed_queue, bool):
            raise TypeError(f"'enable_completed_queue' must be a bool, got {type(enable_completed_queue).__name__}")
        if not isinstance(enable_failed_queue, bool):
            raise TypeError(f"'enable_failed_queue' must be a bool, got {type(enable_failed_queue).__name__}")
        if get_deduplication_key is not None and not callable(get_deduplication_key):
            raise TypeError(f"'get_deduplication_key' must be callable, got {type(get_deduplication_key).__name__}")
        self._deduplication = deduplication
        self._enable_completed_queue = enable_completed_queue
        self._enable_failed_queue = enable_failed_queue
        self._get_deduplication_key = get_deduplication_key

        if gateway is not None:
            if client is not None or interrupt is not None:
                raise ValueError(
                    "'gateway' cannot be provided alongside 'client' or 'interrupt'."
                    " Configure the gateway directly instead."
                )
            self._redis = gateway
        elif client is None:
            raise ValueError("Either 'client' or 'gateway' must be provided.")
        else:
            self._redis = RedisGateway(redis_client=client, interrupt=interrupt)

    async def publish(self, message: str | dict) -> bool:
        if not isinstance(message, (str, dict)):
            raise TypeError(f"'message' must be a str or dict, got {type(message).__name__}")
        if isinstance(message, dict):
            message_str = json.dumps(message, sort_keys=True)
        else:
            message_str = message

        if not self._deduplication:
            await self._redis.add_message(self.key.pending, message_str)
            return True

        if self._get_deduplication_key is not None:
            dedup_key = self._get_deduplication_key(message)
            if inspect.isawaitable(dedup_key):
                dedup_key = await dedup_key
            if not isinstance(dedup_key, str):
                raise TypeError(f"'get_deduplication_key' must return a string, got {type(dedup_key).__name__}")
        else:
            dedup_key = message_str
        dedup_key = self.key.deduplication(dedup_key)

        return await self._redis.publish_message(self.key.pending, message_str, dedup_key)

    @asynccontextmanager
    async def process_message(self) -> AsyncIterator[Optional[bytes]]:
        message = await self._redis.wait_for_message_and_move(
            self.key.pending,
            self.key.processing,
        )
        if message is None:
            yield None
            return

        try:
            yield message  # type: ignore
        except BaseException:
            try:
                if self._enable_failed_queue:
                    await self._redis.move_message(self.key.processing, self.key.failed, message)  # type: ignore
                else:
                    await self._redis.remove_message(self.key.processing, message)  # type: ignore
            except BaseException:
                logger.exception("Failed to clean up message from processing queue")
            raise
        else:
            if self._enable_completed_queue:
                await self._redis.move_message(self.key.processing, self.key.completed, message)  # type: ignore
            else:
                await self._redis.remove_message(self.key.processing, message)  # type: ignore
