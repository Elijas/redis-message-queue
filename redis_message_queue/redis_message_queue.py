import json
import logging
from contextlib import contextmanager
from typing import Callable, Iterator, Optional

import redis
import redis.exceptions

from redis_message_queue._abstract_redis_gateway import AbstractRedisGateway
from redis_message_queue._queue_key_manager import QueueKeyManager
from redis_message_queue._redis_gateway import RedisGateway
from redis_message_queue.interrupt_handler import BaseGracefulInterruptHandler

logger = logging.getLogger(__name__)


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
        key_separator: str = "::",
        get_deduplication_key: Optional[Callable] = None,
        interrupt: BaseGracefulInterruptHandler | None = None,
    ):
        self.key = QueueKeyManager(name, key_separator=key_separator)
        self._deduplication = deduplication
        self._enable_completed_queue = enable_completed_queue
        self._enable_failed_queue = enable_failed_queue
        self._get_deduplication_key = get_deduplication_key

        if gateway is not None:
            self._redis = gateway
        elif client is None:
            raise ValueError("Either 'client' or 'gateway' must be provided.")
        else:
            self._redis = RedisGateway(redis_client=client, interrupt=interrupt)

    def publish(self, message: str | dict) -> bool:
        if isinstance(message, dict):
            message_str = json.dumps(message, sort_keys=True)
        else:
            message_str = message

        if not self._deduplication:
            self._redis.add_message(self.key.pending, message_str)
            return True

        if self._get_deduplication_key:
            dedup_key = self._get_deduplication_key(message)
        else:
            dedup_key = message_str
        dedup_key = self.key.deduplication(dedup_key)

        return self._redis.publish_message(self.key.pending, message_str, dedup_key)

    @contextmanager
    def process_message(self) -> Iterator[Optional[bytes]]:
        message = self._redis.wait_for_message_and_move(
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
                    self._redis.move_message(self.key.processing, self.key.failed, message)  # type: ignore
                else:
                    self._redis.remove_message(self.key.processing, message)  # type: ignore
            except Exception:
                logger.exception("Failed to clean up message from processing queue")
            raise
        else:
            if self._enable_completed_queue:
                self._redis.move_message(self.key.processing, self.key.completed, message)  # type: ignore
            else:
                self._redis.remove_message(self.key.processing, message)  # type: ignore
