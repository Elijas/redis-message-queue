import asyncio
import signal
import threading
from collections.abc import Callable
from typing import Any

import pytest

from examples.production import graceful_shutdown
from examples.production.asyncio import graceful_shutdown as async_graceful_shutdown


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


class AsyncRecordingQueue:
    def __init__(self, signal_callbacks: dict[int, Callable[[], None]]) -> None:
        self.calls: list[tuple[str, Any]] = []
        self.signal_callbacks = signal_callbacks
        self.signal_sent = False

    def process_message(self):
        queue = self

        class ProcessMessage:
            async def __aenter__(self) -> None:
                if not queue.signal_sent:
                    queue.signal_sent = True
                    queue.signal_callbacks[int(signal.SIGTERM)]()
                    await asyncio.sleep(0)
                return None

            async def __aexit__(self, exc_type, exc, tb) -> bool:
                return False

        return ProcessMessage()

    async def publish(self, payload: object) -> bool:
        self.calls.append(("publish", payload))
        return True

    async def aclose(self, *, timeout: float | None = None) -> bool:
        self.calls.append(("aclose", timeout))
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


@pytest.mark.asyncio
async def test_async_production_graceful_shutdown_signal_schedules_shutdown(monkeypatch, capsys) -> None:
    loop = asyncio.get_running_loop()
    signal_callbacks: dict[int, Callable[[], None]] = {}
    created_tasks: list[asyncio.Task[None]] = []

    class FakeClient:
        closed = False

        async def aclose(self) -> None:
            self.closed = True

    class FakeRedis:
        @classmethod
        def from_url(cls, url: str, **kwargs):
            assert url == async_graceful_shutdown.REDIS_CONNECTION_STRING
            assert kwargs == {
                "decode_responses": True,
                "max_connections": async_graceful_shutdown.REDIS_MAX_CONNECTIONS,
            }
            return client

    def fake_add_signal_handler(signum: int, callback: Callable[[], None]) -> None:
        signal_callbacks[int(signum)] = callback

    original_create_task = loop.create_task

    def spy_create_task(coro):
        task = original_create_task(coro)
        created_tasks.append(task)
        return task

    def build_queue(**kwargs):
        queue_kwargs.update(kwargs)
        return queue

    client = FakeClient()
    queue = AsyncRecordingQueue(signal_callbacks)
    queue_kwargs: dict[str, object] = {}
    monkeypatch.setattr(loop, "add_signal_handler", fake_add_signal_handler)
    monkeypatch.setattr(loop, "create_task", spy_create_task)
    monkeypatch.setattr(async_graceful_shutdown, "Redis", FakeRedis)
    monkeypatch.setattr(async_graceful_shutdown, "RedisMessageQueue", build_queue)

    await async_graceful_shutdown.main()

    assert set(signal_callbacks) == {int(signal.SIGTERM), int(signal.SIGINT)}
    assert len(created_tasks) == 1
    assert created_tasks[0].done() is True
    assert queue_kwargs["name"] == "my_message_queue"
    assert queue_kwargs["client"] is client
    assert queue.calls == [
        ("publish", {"event": "shutdown_requested"}),
        ("aclose", 10),
    ]
    assert client.closed is True
    assert capsys.readouterr().out.splitlines() == [
        "Received shutdown signal; draining queue",
        "Drain complete: True",
    ]
