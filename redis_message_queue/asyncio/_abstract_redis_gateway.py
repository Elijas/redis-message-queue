from abc import ABC, abstractmethod

from redis_message_queue._stored_message import ClaimedMessage, MessageData


class AbstractRedisGateway(ABC):
    @abstractmethod
    async def publish_message(self, queue: str, message: str, dedup_key: str) -> bool:
        pass

    @abstractmethod
    async def add_message(self, queue: str, message: str) -> None:
        pass

    @abstractmethod
    async def move_message(
        self,
        from_queue: str,
        to_queue: str,
        message: MessageData,
        *,
        lease_token: str | None = None,
    ) -> None:
        pass

    @abstractmethod
    async def remove_message(self, queue: str, message: MessageData, *, lease_token: str | None = None) -> None:
        pass

    @abstractmethod
    async def renew_message_lease(self, queue: str, message: MessageData, lease_token: str) -> bool:
        pass

    @abstractmethod
    async def wait_for_message_and_move(self, from_queue: str, to_queue: str) -> ClaimedMessage | MessageData | None:
        pass
