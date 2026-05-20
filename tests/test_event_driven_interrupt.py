import signal
import threading
import time

import fakeredis
import pytest

from redis_message_queue import (
    EventDrivenInterruptHandler,
    RedisMessageQueue,
)
from redis_message_queue.interrupt_handler import (
    BaseGracefulInterruptHandler,
    GracefulInterruptHandler,
)


@pytest.fixture(autouse=True)
def _restore_sigterm_handler():
    saved_handler = signal.getsignal(signal.SIGTERM)
    yield
    signal.signal(signal.SIGTERM, saved_handler)


class _InterruptingEmptyRedis:
    def __init__(self, stop_event: threading.Event) -> None:
        self._redis = fakeredis.FakeRedis()
        self._stop_event = stop_event
        self.eval_calls = 0

    def eval(self, *args, **kwargs):
        self.eval_calls += 1
        result = self._redis.eval(*args, **kwargs)
        self._stop_event.set()
        return result

    def __getattr__(self, name):
        return getattr(self._redis, name)


def test_event_driven_interrupt_handler_does_not_install_signal_handlers():
    signal.signal(signal.SIGTERM, signal.SIG_DFL)

    handler = EventDrivenInterruptHandler(threading.Event())

    assert isinstance(handler, BaseGracefulInterruptHandler)
    assert signal.getsignal(signal.SIGTERM) == signal.SIG_DFL


def test_event_driven_interrupt_handler_observes_stop_event():
    stop_event = threading.Event()
    handler = EventDrivenInterruptHandler(stop_event)

    assert handler.is_interrupted() is False

    stop_event.set()

    assert handler.is_interrupted() is True


def test_event_driven_interrupt_handler_exits_queue_wait_when_event_is_set():
    stop_event = threading.Event()
    client = _InterruptingEmptyRedis(stop_event)
    interrupt = EventDrivenInterruptHandler(stop_event)
    queue = RedisMessageQueue(
        "event-driven-interrupt",
        client=client,
        interrupt=interrupt,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
    )

    started_at = time.monotonic()
    with queue.process_message() as message:
        assert message is None

    assert client.eval_calls == 1
    assert interrupt.is_interrupted() is True
    assert time.monotonic() - started_at < 1.0


def test_graceful_interrupt_handler_still_installs_signal_handler():
    signal.signal(signal.SIGTERM, signal.SIG_DFL)

    handler = GracefulInterruptHandler(verbose=False, signals=(signal.SIGTERM,))

    assert signal.getsignal(signal.SIGTERM) == handler._signal_handler
    handler._signal_handler(signal.SIGTERM, None)
    assert handler.is_interrupted() is True


def test_event_driven_interrupt_handler_does_not_replace_existing_signal_handler():
    def custom_handler(signum, frame):
        return None

    signal.signal(signal.SIGTERM, custom_handler)

    EventDrivenInterruptHandler(threading.Event())

    assert signal.getsignal(signal.SIGTERM) is custom_handler
