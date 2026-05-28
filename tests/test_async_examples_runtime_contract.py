import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

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
