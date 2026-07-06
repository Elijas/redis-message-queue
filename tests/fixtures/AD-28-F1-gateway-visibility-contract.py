from redis_message_queue import (
    AbstractRedisGateway,
    ClaimedMessage,
    ReceivedPayload,
    RedisMessageQueue,
)
from redis_message_queue.interrupt_handler import BaseGracefulInterruptHandler


class NoVisibilityGateway(AbstractRedisGateway):
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


# Original user probe: before AD-28-F1, mypy accepted this because
# AbstractRedisGateway did not declare message_visibility_timeout_seconds.
# Runtime still raises ConfigurationError because the inherited default is None.
RedisMessageQueue("q", gateway=NoVisibilityGateway(), heartbeat_interval_seconds=1)

# Positive control for AD-28-F1: the abstract base now declares the property as
# int | None, so code that treats a custom gateway's default as a lease timeout
# gets a static error instead of an invisible member.
visibility_timeout_seconds: int = NoVisibilityGateway().message_visibility_timeout_seconds
