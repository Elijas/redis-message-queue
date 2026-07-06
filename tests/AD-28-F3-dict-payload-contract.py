from redis_message_queue import (
    AbstractRedisGateway,
    ClaimedMessage,
    ReceivedPayload,
    RedisMessageQueue,
)
from redis_message_queue.interrupt_handler import BaseGracefulInterruptHandler


class Gateway(AbstractRedisGateway):
    def publish_message(self, queue: str, message: str, dedup_key: str) -> bool:
        return True

    def add_message(self, queue: str, message: str) -> None:
        return None

    def move_message(
        self,
        from_queue: str,
        to_queue: str,
        message: ReceivedPayload,
        *,
        lease_token: str | None = None,
    ) -> bool:
        return True

    def remove_message(self, queue: str, message: ReceivedPayload, *, lease_token: str | None = None) -> bool:
        return True

    def renew_message_lease(
        self,
        queue: str,
        message: ReceivedPayload,
        lease_token: str,
        *,
        is_interrupted: BaseGracefulInterruptHandler | None = None,
    ) -> bool:
        return True

    def wait_for_message_and_move(self, from_queue: str, to_queue: str) -> ClaimedMessage | ReceivedPayload | None:
        return None

    def trim_queue(self, queue: str, max_length: int) -> None:
        return None


queue = RedisMessageQueue("q", gateway=Gateway())

# mypy accepts this because publish() is typed as str | dict[Any, Any].
# Runtime rejects it before Redis I/O because dict keys must be str.
queue.publish({1: "not a string key"})
