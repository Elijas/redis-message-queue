import pytest
import redis.exceptions

from redis_message_queue import CleanupFailedError, RedisMessageQueue, RedisMessageQueueError
from redis_message_queue._abstract_redis_gateway import AbstractRedisGateway
from redis_message_queue._stored_message import ClaimedMessage, MessageData, encode_stored_message
from redis_message_queue.asyncio import RedisMessageQueue as AsyncRedisMessageQueue
from redis_message_queue.asyncio._abstract_redis_gateway import AbstractRedisGateway as AsyncAbstractRedisGateway


class _CleanupFailingGateway(AbstractRedisGateway):
    message_visibility_timeout_seconds = 1

    def __init__(self) -> None:
        self.message = encode_stored_message("payload")
        self.cleanup_error = redis.exceptions.ConnectionError("cleanup connection failed")

    def publish_message(self, queue: str, message: str, dedup_key: str) -> bool:
        return True

    def add_message(self, queue: str, message: str) -> None:
        self.message = encode_stored_message(message)

    def wait_for_message_and_move(self, from_queue: str, to_queue: str) -> ClaimedMessage | MessageData | None:
        return ClaimedMessage(self.message, "lease-token")

    def remove_message(self, queue: str, message: MessageData, *, lease_token: str | None = None) -> bool:
        raise self.cleanup_error

    def move_message(
        self,
        from_queue: str,
        to_queue: str,
        message: MessageData,
        *,
        lease_token: str | None = None,
    ) -> bool:
        raise self.cleanup_error

    def renew_message_lease(self, queue: str, message: MessageData, lease_token: str, **_kwargs) -> bool:
        return True

    def trim_queue(self, queue: str, max_length: int) -> None:
        return None


class _AsyncCleanupFailingGateway(AsyncAbstractRedisGateway):
    message_visibility_timeout_seconds = 1

    def __init__(self) -> None:
        self.message = encode_stored_message("payload")
        self.cleanup_error = redis.exceptions.ConnectionError("cleanup connection failed")

    async def publish_message(self, queue: str, message: str, dedup_key: str) -> bool:
        return True

    async def add_message(self, queue: str, message: str) -> None:
        self.message = encode_stored_message(message)

    async def wait_for_message_and_move(self, from_queue: str, to_queue: str) -> ClaimedMessage | MessageData | None:
        return ClaimedMessage(self.message, "lease-token")

    async def remove_message(self, queue: str, message: MessageData, *, lease_token: str | None = None) -> bool:
        raise self.cleanup_error

    async def move_message(
        self,
        from_queue: str,
        to_queue: str,
        message: MessageData,
        *,
        lease_token: str | None = None,
    ) -> bool:
        raise self.cleanup_error

    async def renew_message_lease(self, queue: str, message: MessageData, lease_token: str, **_kwargs) -> bool:
        return True

    async def trim_queue(self, queue: str, max_length: int) -> None:
        return None


def test_sync_cleanup_after_success_raises_cleanup_failed_error():
    gateway = _CleanupFailingGateway()
    queue = RedisMessageQueue("cleanup-sync", gateway=gateway)

    with pytest.raises(RedisMessageQueueError) as caught:
        with queue.process_message() as message:
            assert message == "payload"

    exc = caught.value
    assert isinstance(exc, CleanupFailedError)
    assert exc.__cause__ is gateway.cleanup_error


def test_sync_cleanup_after_handler_error_preserves_handler_exception():
    gateway = _CleanupFailingGateway()
    queue = RedisMessageQueue("cleanup-sync", gateway=gateway)

    with pytest.warns(RuntimeWarning, match="Cleanup raised after handler exception"):
        with pytest.raises(ValueError, match="handler failed") as caught:
            with queue.process_message():
                raise ValueError("handler failed")

    assert not isinstance(caught.value, CleanupFailedError)


@pytest.mark.asyncio
async def test_async_cleanup_after_success_raises_cleanup_failed_error():
    gateway = _AsyncCleanupFailingGateway()
    queue = AsyncRedisMessageQueue("cleanup-async", gateway=gateway)

    with pytest.raises(RedisMessageQueueError) as caught:
        async with queue.process_message() as message:
            assert message == "payload"

    exc = caught.value
    assert isinstance(exc, CleanupFailedError)
    assert exc.__cause__ is gateway.cleanup_error


@pytest.mark.asyncio
async def test_async_cleanup_after_handler_error_preserves_handler_exception():
    gateway = _AsyncCleanupFailingGateway()
    queue = AsyncRedisMessageQueue("cleanup-async", gateway=gateway)

    with pytest.warns(RuntimeWarning, match="Cleanup raised after handler exception"):
        with pytest.raises(ValueError, match="handler failed") as caught:
            async with queue.process_message():
                raise ValueError("handler failed")

    assert not isinstance(caught.value, CleanupFailedError)
