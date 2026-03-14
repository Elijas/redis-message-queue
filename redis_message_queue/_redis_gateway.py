from typing import Callable, Optional

import redis
import redis.exceptions

from redis_message_queue._abstract_redis_gateway import AbstractRedisGateway
from redis_message_queue._config import (
    DEFAULT_MESSAGE_DEDUPLICATION_LOG_TTL,
    DEFAULT_MESSAGE_WAIT_INTERVAL_SECONDS,
    PUBLISH_MESSAGE_LUA_SCRIPT,
    get_default_redis_connection_retry_strategy,
    validate_gateway_parameters,
)
from redis_message_queue.interrupt_handler._interface import (
    BaseGracefulInterruptHandler,
)


class RedisGateway(AbstractRedisGateway):
    def __init__(
        self,
        *,
        redis_client: redis.Redis,
        retry_strategy: Optional[Callable] = None,
        message_deduplication_log_ttl_seconds: Optional[int] = None,
        message_wait_interval_seconds: Optional[int] = None,
        interrupt: BaseGracefulInterruptHandler | None = None,
    ):
        self._redis_client = redis_client
        self._retry_strategy = (
            get_default_redis_connection_retry_strategy(interrupt=interrupt) if retry_strategy is None else retry_strategy
        )
        self._message_deduplication_log_ttl_seconds = (
            DEFAULT_MESSAGE_DEDUPLICATION_LOG_TTL if message_deduplication_log_ttl_seconds is None else message_deduplication_log_ttl_seconds
        )
        self._message_wait_interval_seconds = (
            DEFAULT_MESSAGE_WAIT_INTERVAL_SECONDS if message_wait_interval_seconds is None else message_wait_interval_seconds
        )
        validate_gateway_parameters(
            self._message_deduplication_log_ttl_seconds,
            self._message_wait_interval_seconds,
        )

    def publish_message(self, queue: str, message: str, dedup_key: str) -> bool:
        @self._retry_strategy
        def _publish():
            return bool(self._redis_client.eval(
                PUBLISH_MESSAGE_LUA_SCRIPT,
                2,
                dedup_key,
                queue,
                str(self._message_deduplication_log_ttl_seconds),
                message,
            ))

        return _publish()

    def add_message(self, queue: str, message: str) -> None:
        @self._retry_strategy
        def _add():
            self._redis_client.lpush(queue, message)

        _add()

    def move_message(self, from_queue: str, to_queue: str, message: bytes) -> None:
        @self._retry_strategy
        def _move():
            with self._redis_client.pipeline(transaction=True) as pipe:
                pipe.lpush(to_queue, message)
                pipe.lrem(from_queue, 1, message)  # type: ignore
                pipe.execute()

        _move()

    def remove_message(self, queue: str, message: bytes) -> None:
        @self._retry_strategy
        def _remove():
            self._redis_client.lrem(queue, 1, message)  # type: ignore

        _remove()

    def wait_for_message_and_move(self, from_queue: str, to_queue: str):
        @self._retry_strategy
        def _wait():
            return self._redis_client.blmove(
                from_queue,
                to_queue,
                timeout=self._message_wait_interval_seconds,
                src="RIGHT",
                dest="LEFT",
            )

        return _wait()
