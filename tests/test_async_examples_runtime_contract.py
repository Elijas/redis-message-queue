import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from examples.production import backpressure as sync_backpressure
from examples.production.asyncio import backpressure as async_backpressure

REPO_ROOT = Path(__file__).resolve().parents[1]

ASYNC_EXAMPLE_MODULES = (
    "examples.asyncio.receive_messages",
    "examples.production.asyncio.receive_messages",
    "examples.production.asyncio.send_messages",
)


def _write_runtime_stubs(root: Path) -> None:
    """Stub Redis and rmq so example entrypoints can run without Redis."""
    redis_root = root / "redis"
    rmq_root = root / "redis_message_queue"
    (rmq_root / "asyncio").mkdir(parents=True)
    (rmq_root / "interrupt_handler").mkdir(parents=True)
    redis_root.mkdir()

    (redis_root / "__init__.py").write_text("", encoding="utf-8")
    (redis_root / "asyncio.py").write_text(
        textwrap.dedent(
            """
            class Redis:
                @classmethod
                def from_url(cls, *args, **kwargs):
                    return cls()

                async def aclose(self):
                    pass
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )

    (rmq_root / "__init__.py").write_text("", encoding="utf-8")
    handler_code = (
        textwrap.dedent(
            """
            import asyncio


            class GracefulInterruptHandler:
                def __init__(self, *args, **kwargs):
                    try:
                        asyncio.get_running_loop()
                    except RuntimeError:
                        pass
                    else:
                        raise ValueError(
                            "Signal SIGINT already has a non-default handler installed. "
                            "GracefulInterruptHandler refuses to replace existing handlers. "
                            "If running inside asyncio.run(), create the handler before asyncio.run() starts."
                        )
                    self._interrupted = False

                def is_interrupted(self):
                    return self._interrupted

                def interrupt(self):
                    self._interrupted = True
            """
        ).strip()
        + "\n"
    )
    (rmq_root / "interrupt_handler" / "__init__.py").write_text(handler_code, encoding="utf-8")
    (rmq_root / "asyncio" / "__init__.py").write_text(
        textwrap.dedent(
            """
            from redis_message_queue.interrupt_handler import GracefulInterruptHandler


            class _ProcessMessage:
                def __init__(self, handler):
                    self._handler = handler

                async def __aenter__(self):
                    if self._handler is not None:
                        self._handler.interrupt()
                    return None

                async def __aexit__(self, exc_type, exc, tb):
                    return False


            class RedisMessageQueue:
                def __init__(self, *args, interrupt=None, **kwargs):
                    self._handler = interrupt

                def process_message(self):
                    return _ProcessMessage(self._handler)

                async def publish(self, message):
                    if self._handler is not None:
                        self._handler.interrupt()
                    return True
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )


@pytest.mark.parametrize("module", ASYNC_EXAMPLE_MODULES)
def test_async_examples_create_interrupt_handler_before_asyncio_run(module: str, tmp_path: Path) -> None:
    _write_runtime_stubs(tmp_path)
    env = os.environ.copy()
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    env["PYTHONPATH"] = os.pathsep.join(
        path
        for path in (
            str(tmp_path),
            str(REPO_ROOT),
            env.get("PYTHONPATH", ""),
        )
        if path
    )

    result = subprocess.run(
        [sys.executable, "-m", module],
        cwd=tmp_path,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=5,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "Signal SIGINT already has a non-default handler installed" not in result.stderr


def test_sync_production_backpressure_example_retries_after_queue_backpressure(monkeypatch) -> None:
    sleep_calls: list[float] = []

    class FakeClient:
        closed = False

        def close(self) -> None:
            self.closed = True

    class FakeRedis:
        @classmethod
        def from_url(cls, url: str, **kwargs):
            assert url == sync_backpressure.REDIS_CONNECTION_STRING
            assert kwargs == {"decode_responses": True}
            return client

    class RecordingQueue:
        def __init__(self) -> None:
            self.publish_attempts: list[object] = []

        def publish(self, message: object) -> bool:
            self.publish_attempts.append(message)
            if len(self.publish_attempts) == 1:
                raise sync_backpressure.QueueBackpressureError("pending full")
            return True

    def build_queue(**kwargs):
        queue_kwargs.update(kwargs)
        return queue

    client = FakeClient()
    queue = RecordingQueue()
    queue_kwargs: dict[str, object] = {}
    monkeypatch.setattr(sync_backpressure, "count", lambda start=1: iter([start]))
    monkeypatch.setattr(sync_backpressure, "Redis", FakeRedis)
    monkeypatch.setattr(sync_backpressure, "RedisMessageQueue", build_queue)
    monkeypatch.setattr(sync_backpressure.time, "sleep", lambda seconds: sleep_calls.append(seconds))

    sync_backpressure.main()

    assert queue_kwargs["name"] == "my_message_queue"
    assert queue_kwargs["client"] is client
    assert queue_kwargs["max_pending_length"] == 1000
    assert queue_kwargs["pending_overload_policy"] == "raise"
    assert queue.publish_attempts == [
        {"id": "1", "body": "work item 1"},
        {"id": "1", "body": "work item 1"},
    ]
    assert sleep_calls == [0.25, 0.05]
    assert client.closed is True


@pytest.mark.asyncio
async def test_async_production_backpressure_example_retries_after_queue_backpressure(monkeypatch) -> None:
    sleep_calls: list[float] = []

    class FakeClient:
        closed = False

        async def aclose(self) -> None:
            self.closed = True

    class FakeRedis:
        @classmethod
        def from_url(cls, url: str, **kwargs):
            assert url == async_backpressure.REDIS_CONNECTION_STRING
            assert kwargs == {"decode_responses": True}
            return client

    class RecordingQueue:
        def __init__(self) -> None:
            self.publish_attempts: list[object] = []

        async def publish(self, message: object) -> bool:
            self.publish_attempts.append(message)
            if len(self.publish_attempts) == 1:
                raise async_backpressure.QueueBackpressureError("pending full")
            return True

    def build_queue(**kwargs):
        queue_kwargs.update(kwargs)
        return queue

    async def fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)

    client = FakeClient()
    queue = RecordingQueue()
    queue_kwargs: dict[str, object] = {}
    monkeypatch.setattr(async_backpressure, "count", lambda start=1: iter([start]))
    monkeypatch.setattr(async_backpressure, "Redis", FakeRedis)
    monkeypatch.setattr(async_backpressure, "RedisMessageQueue", build_queue)
    monkeypatch.setattr(async_backpressure.asyncio, "sleep", fake_sleep)

    await async_backpressure.main()

    assert queue_kwargs["name"] == "my_message_queue"
    assert queue_kwargs["client"] is client
    assert queue_kwargs["max_pending_length"] == 1000
    assert queue_kwargs["pending_overload_policy"] == "raise"
    assert queue.publish_attempts == [
        {"id": "1", "body": "work item 1"},
        {"id": "1", "body": "work item 1"},
    ]
    assert sleep_calls == [0.25, 0.05]
    assert client.closed is True
