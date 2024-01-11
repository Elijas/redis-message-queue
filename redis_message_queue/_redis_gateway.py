import logging
from typing import Callable, Optional

import redis
import redis.exceptions
from tenacity import (
    after_log,
    retry,
    retry_if_exception,
    stop_after_delay,
    wait_exponential_jitter,
)

from redis_message_queue._abstract_redis_gateway import AbstractRedisGateway

logger = logging.getLogger(__name__)


def is_redis_retryable_exception(exception):
    return isinstance(
        exception,
        (
            redis.exceptions.ConnectionError,
            redis.exceptions.TimeoutError,
            redis.exceptions.BusyLoadingError,
            redis.exceptions.ClusterDownError,
            redis.exceptions.TryAgainError,
        ),
    )


DEFAULT_REDIS_CONNECTION_RETRY_STRATEGY = retry(
    stop=stop_after_delay(120),
    wait=wait_exponential_jitter(initial=0.01, exp_base=2, max=5, jitter=0.1),
    retry=retry_if_exception(is_redis_retryable_exception),
    after=after_log(logger, logging.ERROR),
)
DEFAULT_MESSAGE_WAIT_INTERVAL_SECONDS = 5
DEFAULT_MESSAGE_DEDUPLICATION_LOG_TTL = 60 * 60  # 1 hour = 60 seconds * 60 minutes


class RedisGateway(AbstractRedisGateway):
    def __init__(
        self,
        *,
        redis_client: redis.Redis,
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

    def add_if_absent(self, key: str, value: str = "") -> bool:
        @self._retry_strategy
        def _insert():
            pipe = self._redis_client.pipeline(transaction=True)
            pipe.setnx(name=key, value=value)
            pipe.expire(name=key, time=self._message_deduplication_log_ttl_seconds)
            return pipe.execute()[0]  # Only interested in the result of setnx

        return _insert()

    def add_message(self, queue: str, message: str) -> None:
        @self._retry_strategy
        def _add():
            self._redis_client.lpush(queue, message)

        _add()

    def move_message(self, from_queue: str, to_queue: str, message: bytes) -> None:
        @self._retry_strategy
        def _move():
            pipe = self._redis_client.pipeline(transaction=True)
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
            return self._redis_client.brpoplpush(
                from_queue,
                to_queue,
                timeout=self._message_wait_interval_seconds,
            )

        return _wait()
