import fakeredis
import pytest

from redis_message_queue import CleanupFailedError, QueueDrainedError, RedisMessageQueue
from redis_message_queue._abstract_redis_gateway import AbstractRedisGateway
from redis_message_queue._stored_message import (
    ClaimedMessage,
    MessageData,
    encode_stored_message,
    extract_stored_message_id,
)


class _CleanupFailingGateway(AbstractRedisGateway):
    message_visibility_timeout_seconds = 1

    def __init__(self) -> None:
        self.message = encode_stored_message("payload")
        self.cleanup_error = ConnectionError("cleanup failed")

    def publish_message(self, queue: str, message: str, dedup_key: str) -> bool:
        self.message = encode_stored_message(message)
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

    def renew_message_lease(self, queue: str, message: MessageData, lease_token: str, **_kwargs: object) -> bool:
        return True

    def trim_queue(self, queue: str, max_length: int) -> None:
        return None


def test_queue_drained_error_exposes_drain_context() -> None:
    queue = RedisMessageQueue("context-drain", client=fakeredis.FakeRedis(), deduplication=False)

    assert queue.drain() is True
    with pytest.raises(QueueDrainedError, match="queue is drained") as caught:
        queue.publish("after")

    assert caught.value.queue == "context-drain"
    assert caught.value.message_id is None
    assert caught.value.operation == "drain"


def test_cleanup_failed_error_during_ack_exposes_message_context() -> None:
    gateway = _CleanupFailingGateway()
    queue = RedisMessageQueue("context-cleanup", gateway=gateway)

    with pytest.raises(CleanupFailedError, match="Cleanup after successful processing failed") as caught:
        with queue.process_message() as message:
            assert message == "payload"

    assert caught.value.queue == "context-cleanup"
    assert caught.value.message_id == extract_stored_message_id(gateway.message)
    assert caught.value.operation == "cleanup"
    assert caught.value.__cause__ is gateway.cleanup_error
