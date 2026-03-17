import time
from typing import Callable, Optional

import redis

from redis_message_queue._abstract_redis_gateway import AbstractRedisGateway
from redis_message_queue._config import (
    CLAIM_MESSAGE_WITH_VISIBILITY_TIMEOUT_LUA_SCRIPT,
    DEFAULT_MESSAGE_DEDUPLICATION_LOG_TTL,
    DEFAULT_MESSAGE_WAIT_INTERVAL_SECONDS,
    MOVE_MESSAGE_LUA_SCRIPT,
    MOVE_MESSAGE_WITH_LEASE_TOKEN_LUA_SCRIPT,
    PUBLISH_MESSAGE_LUA_SCRIPT,
    RENEW_MESSAGE_LEASE_LUA_SCRIPT,
    REMOVE_MESSAGE_WITH_LEASE_TOKEN_LUA_SCRIPT,
    get_default_redis_connection_retry_strategy,
    validate_gateway_parameters,
)
from redis_message_queue._stored_message import ClaimedMessage, MessageData, decode_stored_message, encode_stored_message
from redis_message_queue.interrupt_handler._interface import (
    BaseGracefulInterruptHandler,
)

_LEASE_DEADLINES_SUFFIX = ":lease_deadlines"
_LEASE_TOKENS_SUFFIX = ":lease_tokens"
_LEASE_TOKEN_COUNTER_SUFFIX = ":lease_token_counter"
_VISIBILITY_TIMEOUT_POLL_INTERVAL_SECONDS = 0.25


class RedisGateway(AbstractRedisGateway):
    def __init__(
        self,
        *,
        redis_client: redis.Redis,
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

    def publish_message(self, queue: str, message: str, dedup_key: str) -> bool:
        stored_message = encode_stored_message(message)

        @self._retry_strategy
        def _publish():
            return bool(
                self._redis_client.eval(
                    PUBLISH_MESSAGE_LUA_SCRIPT,
                    2,
                    dedup_key,
                    queue,
                    str(self._message_deduplication_log_ttl_seconds),
                    stored_message,
                )
            )

        return _publish()

    def add_message(self, queue: str, message: str) -> None:
        stored_message = encode_stored_message(message)
        self._redis_client.lpush(queue, stored_message)

    def move_message(
        self,
        from_queue: str,
        to_queue: str,
        message: MessageData,
        *,
        lease_token: str | None = None,
    ) -> None:
        decoded_message = decode_stored_message(message)

        if lease_token is None:
            @self._retry_strategy
            def _move():
                self._redis_client.eval(
                    MOVE_MESSAGE_LUA_SCRIPT,
                    2,
                    from_queue,
                    to_queue,
                    message,
                    decoded_message,
                )

            _move()
            return

        @self._retry_strategy
        def _move_with_lease():
            self._redis_client.eval(
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

        _move_with_lease()

    def remove_message(self, queue: str, message: MessageData, *, lease_token: str | None = None) -> None:
        if lease_token is None:
            @self._retry_strategy
            def _remove():
                self._redis_client.lrem(queue, 1, message)  # type: ignore

            _remove()
            return

        @self._retry_strategy
        def _remove_with_lease():
            self._redis_client.eval(
                REMOVE_MESSAGE_WITH_LEASE_TOKEN_LUA_SCRIPT,
                3,
                queue,
                self._lease_deadlines_key(queue),
                self._lease_tokens_key(queue),
                message,
                lease_token,
            )

        _remove_with_lease()

    def renew_message_lease(self, queue: str, message: MessageData, lease_token: str) -> bool:
        if self._message_visibility_timeout_seconds is None:
            return False

        @self._retry_strategy
        def _renew():
            return bool(
                self._redis_client.eval(
                    RENEW_MESSAGE_LEASE_LUA_SCRIPT,
                    2,
                    self._lease_deadlines_key(queue),
                    self._lease_tokens_key(queue),
                    message,
                    lease_token,
                    str(self._message_visibility_timeout_seconds * 1000),
                )
            )

        return _renew()

    def wait_for_message_and_move(self, from_queue: str, to_queue: str) -> ClaimedMessage | MessageData | None:
        if self._message_visibility_timeout_seconds is not None:
            return self._wait_for_message_with_visibility_timeout(from_queue, to_queue)

        # Retrying a move after the server may already have mutated queue state can
        # consume an extra message or hide the one already moved into processing.
        if self._message_wait_interval_seconds == 0:
            return self._redis_client.lmove(from_queue, to_queue, "RIGHT", "LEFT")
        return self._redis_client.blmove(
            from_queue,
            to_queue,
            timeout=self._message_wait_interval_seconds,
            src="RIGHT",
            dest="LEFT",
        )

    def _wait_for_message_with_visibility_timeout(self, from_queue: str, to_queue: str) -> ClaimedMessage | None:
        if self._message_wait_interval_seconds == 0:
            return self._claim_visible_message(from_queue, to_queue)

        deadline = time.monotonic() + self._message_wait_interval_seconds
        while True:
            claimed_message = self._claim_visible_message(from_queue, to_queue)
            if claimed_message is not None:
                return claimed_message

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            time.sleep(min(_VISIBILITY_TIMEOUT_POLL_INTERVAL_SECONDS, remaining))

    def _claim_visible_message(self, from_queue: str, to_queue: str) -> ClaimedMessage | None:
        result = self._redis_client.eval(
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
