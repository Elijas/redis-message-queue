from abc import ABC, abstractmethod


class AbstractRedisGateway(ABC):
    @abstractmethod
    def add_if_absent(self, key: str, value: str = "") -> bool:
        pass

    @abstractmethod
    def add_message(self, queue: str, message: str) -> None:
        pass

    @abstractmethod
    def move_message(self, from_queue: str, to_queue: str, message: bytes) -> None:
        pass

    @abstractmethod
    def remove_message(self, queue: str, message: bytes) -> None:
        pass

    @abstractmethod
    def wait_for_message_and_move(self, from_queue: str, to_queue: str):
        pass
