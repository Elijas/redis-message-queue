import inspect
import json
import logging
from contextlib import contextmanager
from typing import Callable, Iterator, Optional

import redis
import redis.exceptions

from redis_message_queue._abstract_redis_gateway import AbstractRedisGateway
from redis_message_queue._queue_key_manager import QueueKeyManager
from redis_message_queue._redis_gateway import RedisGateway
from redis_message_queue._stored_message import MessageData, decode_stored_message
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
            if not isinstance(gateway, AbstractRedisGateway):
                raise TypeError(f"'gateway' must be an AbstractRedisGateway, got {type(gateway).__name__}")
            self._redis = gateway
        elif client is None:
            raise ValueError("Either 'client' or 'gateway' must be provided.")
        else:
            self._redis = RedisGateway(redis_client=client, interrupt=interrupt)

    def publish(self, message: str | dict) -> bool:
        if not isinstance(message, (str, dict)):
            raise TypeError(f"'message' must be a str or dict, got {type(message).__name__}")
        if isinstance(message, dict):
            message_str = json.dumps(message, sort_keys=True)
        else:
            message_str = message

        if not self._deduplication:
            self._redis.add_message(self.key.pending, message_str)
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
                    "'get_deduplication_key' returned an awaitable; "
                    "use the async RedisMessageQueue for async callables"
                )
            if not isinstance(dedup_key, str):
                raise TypeError(f"'get_deduplication_key' must return a string, got {type(dedup_key).__name__}")
        else:
            dedup_key = message_str
        dedup_key = self.key.deduplication(dedup_key)

        return self._redis.publish_message(self.key.pending, message_str, dedup_key)

    @contextmanager
    def process_message(self) -> Iterator[Optional[MessageData]]:
        stored_message = self._redis.wait_for_message_and_move(
            self.key.pending,
            self.key.processing,
        )
        if stored_message is None:
            yield None
            return

        message = decode_stored_message(stored_message)
        try:
            yield message  # type: ignore
        except BaseException:
            try:
                if self._enable_failed_queue:
                    self._redis.move_message(self.key.processing, self.key.failed, stored_message)
                else:
                    self._redis.remove_message(self.key.processing, stored_message)
            except BaseException:
                logger.exception("Failed to clean up message from processing queue")
            raise
        else:
            if self._enable_completed_queue:
                self._redis.move_message(self.key.processing, self.key.completed, stored_message)
            else:
                self._redis.remove_message(self.key.processing, stored_message)
