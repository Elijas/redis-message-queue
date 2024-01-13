from abc import ABC, abstractmethod


class AbstractRedisGateway(ABC):
    @abstractmethod
    async def add_if_absent(self, key: str, value: str = "") -> bool:
        pass

    @abstractmethod
    async def add_message(self, queue: str, message: str) -> None:
        pass

    @abstractmethod
    async def move_message(
        self, from_queue: str, to_queue: str, message: bytes
    ) -> None:
        pass

    @abstractmethod
    async def remove_message(self, queue: str, message: bytes) -> None:
        pass

    @abstractmethod
    async def wait_for_message_and_move(self, from_queue: str, to_queue: str):
        pass
