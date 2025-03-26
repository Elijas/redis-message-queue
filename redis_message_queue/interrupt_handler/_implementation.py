import signal
from typing import Iterable

from redis_message_queue.interrupt_handler._interface import (
    BaseGracefulInterruptHandler,
)


class GracefulInterruptHandler(BaseGracefulInterruptHandler):
    _DEFAULT_SIGNALS = (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)

    def __init__(
        self,
        verbose: bool = True,
        signals: Iterable[signal.Signals] = _DEFAULT_SIGNALS,
    ):
        self._interrupted = False
        self._verbose = verbose
        self._signals = signals
        for sig in self._signals:
            signal.signal(sig, self._signal_handler)

    def is_interrupted(self) -> bool:
        return self._interrupted

    def _signal_handler(self, signum, frame):
        if self._verbose:
            print(f"Received signal: {signal.strsignal(signum)}")
        self._interrupted = True
