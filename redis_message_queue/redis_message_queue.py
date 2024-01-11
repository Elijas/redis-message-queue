from contextlib import contextmanager
from typing import Iterator, Optional

import redis
import redis.exceptions

from redis_message_queue._abstract_redis_gateway import AbstractRedisGateway
from redis_message_queue._queue_key_manager import QueueKeyManager
from redis_message_queue._redis_gateway import RedisGateway


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
    ):
        self._redis_client = client
        self.key = QueueKeyManager(name)
        self._deduplication = deduplication
        self._enable_completed_queue = enable_completed_queue
        self._enable_failed_queue = enable_failed_queue

        if gateway:
            self._redis = gateway
        elif not client:
            raise ValueError("Either 'client' or 'gateway' must be provided.")
        else:
            self._redis = RedisGateway(redis_client=client)

    def publish(self, message: str) -> bool:
        key = self.key.deduplication(message)
        if not self._deduplication or self._redis.add_if_absent(key):
            self._redis.add_message(self.key.pending, message)
            return True
        return False

    @contextmanager
    def process_message(self) -> Iterator[Optional[bytes]]:
        message = self._redis.wait_for_message_and_move(
            self.key.pending,
            self.key.processing,
        )
        if not message:
            yield None
            return

        try:
            yield message  # type: ignore
            if self._enable_completed_queue:
                self._redis.move_message(self.key.processing, self.key.completed, message)  # type: ignore
            else:
                self._redis.remove_message(self.key.processing, message)  # type: ignore
        except Exception:
            if self._enable_failed_queue:
                self._redis.move_message(self.key.failed, message)  # type: ignore
            else:
                self._redis.remove_message(self.key.processing, message)  # type: ignore
            raise
