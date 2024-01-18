from contextlib import asynccontextmanager
from typing import Optional

import redis.asyncio
import redis.exceptions

from redis_message_queue._queue_key_manager import QueueKeyManager
from redis_message_queue.asyncio._abstract_redis_gateway import AbstractRedisGateway
from redis_message_queue.asyncio._redis_gateway import RedisGateway


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
    ):
        self._redis_client = client
        self.key = QueueKeyManager(name, key_separator=key_separator)
        self._deduplication = deduplication
        self._enable_completed_queue = enable_completed_queue
        self._enable_failed_queue = enable_failed_queue

        if gateway:
            self._redis = gateway
        elif not client:
            raise ValueError("Either 'client' or 'gateway' must be provided.")
        else:
            self._redis = RedisGateway(redis_client=client)

    async def publish(self, message: str, *, get_deduplication_key=None) -> bool:
        if get_deduplication_key:
            key = get_deduplication_key(message)
        else:
            key = message
        full_key = self.key.deduplication(key)
        if not self._deduplication or await self._redis.add_if_absent(full_key):
            await self._redis.add_message(self.key.pending, message)
            return True
        return False

    @asynccontextmanager
    async def process_message(self):
        message = await self._redis.wait_for_message_and_move(
            self.key.pending,
            self.key.processing,
        )
        if not message:
            yield None
            return

        try:
            yield message  # type: ignore
            if self._enable_completed_queue:
                await self._redis.move_message(self.key.processing, self.key.completed, message)  # type: ignore
            else:
                await self._redis.remove_message(self.key.processing, message)  # type: ignore
        except Exception:
            if self._enable_failed_queue:
                await self._redis.move_message(self.key.failed, message)  # type: ignore
            else:
                await self._redis.remove_message(self.key.processing, message)  # type: ignore
            raise
