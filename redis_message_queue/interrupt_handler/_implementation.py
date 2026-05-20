import os
import signal
import sys
from typing import Iterable

from redis_message_queue.interrupt_handler._interface import (
    BaseGracefulInterruptHandler,
)


def _is_default_signal_handler(sig: signal.Signals, handler) -> bool:
    if handler == signal.SIG_DFL:
        return True
    return sig == signal.SIGINT and handler is signal.default_int_handler


def _is_graceful_interrupt_handler(handler) -> bool:
    return callable(handler) and hasattr(handler, "__self__") and isinstance(handler.__self__, GracefulInterruptHandler)


class GracefulInterruptHandler(BaseGracefulInterruptHandler):
    """Signal-based interrupt handler for graceful consumer shutdown.

    Registers signal handlers for the specified signals (default: SIGINT, SIGTERM,
    SIGHUP) and flips ``is_interrupted()`` to ``True`` when any of them fires.

    Only **one** ``GracefulInterruptHandler`` may be active per signal, and it
    only claims signals that are still using Python's default disposition.
    Creating a second handler for a signal that is already owned by another
    instance raises ``ValueError``. Trying to install this handler over any
    other pre-existing signal handler also raises ``ValueError``. If you need
    multiple shutdown hooks on the same signal, use a single handler and fan
    out in your own code.

    Signal handlers are **not restored** unless ``reset()`` is called. Once
    created, the handler owns those signals for the lifetime of the process. A
    repeated signal for an owned handler falls back to the previous/default
    disposition so operators can still force termination (for example, a second
    Ctrl+C raises ``KeyboardInterrupt``).

    Process-global signal ownership cannot be safely chained. If rmq runs in
    the same process as Celery, RQ, or Dramatiq CLI workers, the libraries may
    overwrite each other's SIGTERM/SIGINT handlers. Prefer one top-level signal
    owner that calls ``queue.drain()`` or sets an application stop event, and
    run sibling workers in separate processes.
    """

    _DEFAULT_SIGNALS = (
        (signal.SIGINT, signal.SIGTERM, signal.SIGHUP) if hasattr(signal, "SIGHUP") else (signal.SIGINT, signal.SIGTERM)
    )

    def __init__(
        self,
        verbose: bool = True,
        signals: Iterable[signal.Signals] = _DEFAULT_SIGNALS,
    ):
        if not isinstance(verbose, bool):
            raise TypeError(f"'verbose' must be a bool, got {type(verbose).__name__} (use True or False, not 1/0)")
        if isinstance(signals, str) or not hasattr(signals, "__iter__"):
            raise TypeError(
                f"'signals' must be an iterable of signal.Signals members, got {type(signals).__name__}."
                " To pass a single signal, wrap it in a tuple: signals=(signal.SIGINT,)."
            )
        signals = tuple(signals)
        if not signals:
            raise ValueError("'signals' must contain at least one signal")
        for i, sig in enumerate(signals):
            if not isinstance(sig, signal.Signals):
                raise TypeError(
                    f"'signals' must contain signal.Signals members, got {type(sig).__name__} at position {i}"
                )
        for sig in signals:
            try:
                current = signal.getsignal(sig)
            except OSError:
                raise ValueError(
                    f"Signal {sig.name} cannot be caught or handled by user code."
                    " Use signals such as SIGINT, SIGTERM, or SIGHUP for graceful shutdown."
                )
            if _is_graceful_interrupt_handler(current):
                raise ValueError(
                    f"Signal {sig.name} is already owned by another GracefulInterruptHandler."
                    " Only one handler per signal is supported."
                )
            if not _is_default_signal_handler(sig, current):
                raise ValueError(
                    f"Signal {sig.name} already has a non-default handler installed."
                    " GracefulInterruptHandler refuses to replace existing handlers."
                    " If running inside asyncio.run(), create the handler before asyncio.run() starts."
                )
        self._interrupted = False
        self._verbose = verbose
        self._signals = signals
        self._previous_handlers = {sig: signal.getsignal(sig) for sig in self._signals}
        for sig in self._signals:
            try:
                signal.signal(sig, self._signal_handler)
            except ValueError as e:
                if "main thread" in str(e):
                    raise ValueError("GracefulInterruptHandler must be created on the main thread.") from e
                raise

    def is_interrupted(self) -> bool:
        return self._interrupted

    def reset(self) -> None:
        """Restore previous signal handlers and clear ownership.

        Useful in forked child processes that inherited a parent-installed
        handler. Call this in the child before constructing a new handler for
        the same signal.

        Idempotent. Safe to call more than once.
        """
        for sig, previous_handler in list(self._previous_handlers.items()):
            signal.signal(sig, previous_handler)
        self._previous_handlers.clear()

    def _signal_handler(self, signum, frame):
        sig = signal.Signals(signum)
        if self._interrupted:
            previous_handler = self._previous_handlers.get(sig, signal.SIG_DFL)
            signal.signal(sig, previous_handler)
            if callable(previous_handler):
                previous_handler(signum, frame)
                return
            os.kill(os.getpid(), signum)
            return
        self._interrupted = True
        if self._verbose:
            try:
                print(f"Received signal: {signal.strsignal(signum)}", file=sys.stderr)
            except Exception:
                pass
