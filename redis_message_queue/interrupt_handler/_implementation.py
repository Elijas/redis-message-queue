import signal
from typing import Iterable

from redis_message_queue.interrupt_handler._interface import (
    BaseGracefulInterruptHandler,
)


class GracefulInterruptHandler(BaseGracefulInterruptHandler):
    _DEFAULT_SIGNALS = (
        (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)
        if hasattr(signal, "SIGHUP")
        else (signal.SIGINT, signal.SIGTERM)
    )

    def __init__(
        self,
        verbose: bool = True,
        signals: Iterable[signal.Signals] = _DEFAULT_SIGNALS,
    ):
        if not isinstance(verbose, bool):
            raise TypeError(f"'verbose' must be a bool, got {type(verbose).__name__}")
        if isinstance(signals, str) or not hasattr(signals, '__iter__'):
            raise TypeError(
                f"'signals' must be an iterable of signal.Signals, got {type(signals).__name__}"
            )
        signals = tuple(signals)
        if not signals:
            raise ValueError("'signals' must contain at least one signal")
        for i, sig in enumerate(signals):
            if not isinstance(sig, signal.Signals):
                raise TypeError(
                    f"'signals' must contain signal.Signals members, "
                    f"got {type(sig).__name__} at position {i}"
                )
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
