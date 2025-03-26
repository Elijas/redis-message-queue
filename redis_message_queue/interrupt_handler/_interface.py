from abc import ABC, abstractmethod


class BaseGracefulInterruptHandler(ABC):
    @abstractmethod
    def is_interrupted(self) -> bool:
        pass
