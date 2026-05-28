import signal
import threading
from collections.abc import Callable
from typing import Any

import pytest

from examples.production import graceful_shutdown


@pytest.fixture()
def restore_shutdown_signal_handlers():
    saved_handlers = {signum: signal.getsignal(signum) for signum in (signal.SIGTERM, signal.SIGINT)}
    yield
    for signum, handler in saved_handlers.items():
        signal.signal(signum, handler)


class RecordingQueue:
    def __init__(self, *, reenter_on_publish: bool = False) -> None:
        self.calls: list[tuple[str, Any]] = []
        self.handler: Callable[[int, object], None] | None = None
        self.reenter_on_publish = reenter_on_publish
        self._reentered = False

    def publish(self, payload: object) -> bool:
        self.calls.append(("publish", payload))
        if self.reenter_on_publish and not self._reentered:
            self._reentered = True
            assert self.handler is not None
            self.handler(int(signal.SIGTERM), None)
        return True

    def drain(self, *, timeout: float | None = None) -> bool:
        self.calls.append(("drain", timeout))
        return True

    def close(self) -> bool:
        self.calls.append(("close", None))
        return True


def installed_sigterm_handler() -> Callable[[int, object], None]:
    handler = signal.getsignal(signal.SIGTERM)
    assert callable(handler)
    return handler


def test_production_graceful_shutdown_single_signal_drains_and_stops(restore_shutdown_signal_handlers, capsys) -> None:
    queue = RecordingQueue()
    stop = threading.Event()
    graceful_shutdown.install_shutdown_hook(queue, stop)  # type: ignore[arg-type]

    installed_sigterm_handler()(int(signal.SIGTERM), None)

    assert queue.calls == [
        ("publish", {"event": "shutdown_requested", "signal": int(signal.SIGTERM)}),
        ("drain", 10),
        ("close", None),
    ]
    assert stop.is_set() is True
    assert capsys.readouterr().out.splitlines() == [
        "Received signal 15; draining queue",
        "Drain complete: True",
    ]


def test_production_graceful_shutdown_duplicate_signal_reentry_is_idempotent(
    restore_shutdown_signal_handlers, capsys
) -> None:
    queue = RecordingQueue(reenter_on_publish=True)
    stop = threading.Event()
    graceful_shutdown.install_shutdown_hook(queue, stop)  # type: ignore[arg-type]
    queue.handler = installed_sigterm_handler()

    queue.handler(int(signal.SIGTERM), None)

    assert queue.calls == [
        ("publish", {"event": "shutdown_requested", "signal": int(signal.SIGTERM)}),
        ("drain", 10),
        ("close", None),
    ]
    assert stop.is_set() is True
    assert capsys.readouterr().out.splitlines() == [
        "Received signal 15; draining queue",
        "Received signal 15; shutdown already in progress",
        "Drain complete: True",
    ]
