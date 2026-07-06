from __future__ import annotations

import shutil
import subprocess
import tempfile
import textwrap
from pathlib import Path

import fakeredis
import pytest

import redis_message_queue
from redis_message_queue import PublishPayload
from redis_message_queue.asyncio.redis_message_queue import RedisMessageQueue as AsyncRedisMessageQueue
from redis_message_queue.redis_message_queue import RedisMessageQueue

_REPO_ROOT = Path(__file__).resolve().parents[1]
_COPIED_F3_PROBE = _REPO_ROOT / "tests" / "AD-28-F3-dict-payload-contract.py"


def _run_mypy_strict(path: Path) -> subprocess.CompletedProcess[str]:
    mypy = shutil.which("mypy")
    if mypy is None:
        pytest.skip("mypy executable is required for PublishPayload typing regression tests")
    with tempfile.TemporaryDirectory() as tmpdir:
        config = Path(tmpdir) / "mypy.ini"
        config.write_text(
            textwrap.dedent(
                """
                [mypy]
                strict = True

                [mypy-redis_message_queue.*]
                ignore_errors = True
                """
            )
        )
        return subprocess.run(
            [mypy, "--config-file", str(config), str(path)],
            cwd=_REPO_ROOT,
            text=True,
            capture_output=True,
            check=False,
        )


def test_message_payload_is_publicly_importable() -> None:
    assert PublishPayload == (str | dict[str, object])
    assert redis_message_queue.PublishPayload == PublishPayload
    assert "PublishPayload" in redis_message_queue.__all__


def test_mypy_strict_accepts_message_payload_callback_and_string_keyed_dicts(tmp_path: Path) -> None:
    probe = tmp_path / "message_payload_accepts.py"
    probe.write_text(
        textwrap.dedent(
            """
            from collections.abc import Callable

            from redis_message_queue import (
                AbstractRedisGateway,
                ClaimedMessage,
                PublishPayload,
                ReceivedPayload,
                RedisMessageQueue,
            )
            from redis_message_queue.asyncio import AbstractRedisGateway as AsyncAbstractRedisGateway
            from redis_message_queue.asyncio import RedisMessageQueue as AsyncRedisMessageQueue
            from redis_message_queue.interrupt_handler import BaseGracefulInterruptHandler


            class Gateway(AbstractRedisGateway):
                def publish_message(self, queue: str, message: str, dedup_key: str) -> bool:
                    return True

                def add_message(self, queue: str, message: str) -> None:
                    return None

                def move_message(
                    self,
                    from_queue: str,
                    to_queue: str,
                    message: ReceivedPayload,
                    *,
                    lease_token: str | None = None,
                ) -> bool:
                    return True

                def remove_message(
                    self,
                    queue: str,
                    message: ReceivedPayload,
                    *,
                    lease_token: str | None = None,
                ) -> bool:
                    return True

                def renew_message_lease(
                    self,
                    queue: str,
                    message: ReceivedPayload,
                    lease_token: str,
                    *,
                    is_interrupted: BaseGracefulInterruptHandler | None = None,
                ) -> bool:
                    return True

                def wait_for_message_and_move(
                    self,
                    from_queue: str,
                    to_queue: str,
                ) -> ClaimedMessage | ReceivedPayload | None:
                    return None

                def trim_queue(self, queue: str, max_length: int) -> None:
                    return None


            class AsyncGateway(AsyncAbstractRedisGateway):
                async def publish_message(self, queue: str, message: str, dedup_key: str) -> bool:
                    return True

                async def add_message(self, queue: str, message: str) -> None:
                    return None

                async def move_message(
                    self,
                    from_queue: str,
                    to_queue: str,
                    message: ReceivedPayload,
                    *,
                    lease_token: str | None = None,
                ) -> bool:
                    return True

                async def remove_message(
                    self,
                    queue: str,
                    message: ReceivedPayload,
                    *,
                    lease_token: str | None = None,
                ) -> bool:
                    return True

                async def renew_message_lease(
                    self,
                    queue: str,
                    message: ReceivedPayload,
                    lease_token: str,
                    *,
                    is_interrupted: BaseGracefulInterruptHandler | None = None,
                ) -> bool:
                    return True

                async def wait_for_message_and_move(
                    self,
                    from_queue: str,
                    to_queue: str,
                ) -> ClaimedMessage | ReceivedPayload | None:
                    return None

                async def trim_queue(self, queue: str, max_length: int) -> None:
                    return None


            def get_key(message: PublishPayload) -> str:
                if isinstance(message, str):
                    return message
                return str(message.get("id", "fallback"))


            callback: Callable[[PublishPayload], str] = get_key
            sync_queue = RedisMessageQueue(
                "sync",
                gateway=Gateway(),
                deduplication=True,
                get_deduplication_key=callback,
            )
            sync_queue.publish({"k": "x"})


            async def exercise_async() -> None:
                async_queue = AsyncRedisMessageQueue(
                    "async",
                    gateway=AsyncGateway(),
                    deduplication=True,
                    get_deduplication_key=callback,
                )
                await async_queue.publish({"k": "x"})
            """
        )
    )

    result = _run_mypy_strict(probe)

    assert result.returncode == 0, result.stdout + result.stderr


def test_mypy_strict_rejects_int_keyed_sync_payload_from_copied_f3_probe() -> None:
    result = _run_mypy_strict(_COPIED_F3_PROBE)

    assert result.returncode != 0
    output = result.stdout + result.stderr
    assert "Dict entry 0 has incompatible type" in output
    assert '"int": "str"' in output
    assert 'expected "str": "object"' in output


def test_mypy_strict_rejects_int_keyed_async_payload(tmp_path: Path) -> None:
    probe = tmp_path / "message_payload_rejects_async_int_key.py"
    probe.write_text(
        textwrap.dedent(
            """
            from redis_message_queue.asyncio import RedisMessageQueue


            async def exercise(queue: RedisMessageQueue) -> None:
                await queue.publish({1: "x"})
            """
        )
    )

    result = _run_mypy_strict(probe)

    assert result.returncode != 0
    output = result.stdout + result.stderr
    assert "Dict entry 0 has incompatible type" in output
    assert '"int": "str"' in output
    assert 'expected "str": "object"' in output


def test_sync_publish_rejects_int_keyed_dict_without_runtime_behavior_change() -> None:
    queue = RedisMessageQueue("payload-sync", client=fakeredis.FakeRedis(), deduplication=False)

    with pytest.raises(TypeError, match=r"'message' dict keys must all be strings; got non-string keys: \[1\]"):
        queue.publish({1: "x"})


def test_sync_publish_accepts_string_keyed_dict() -> None:
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue("payload-sync-ok", client=client, deduplication=False)

    assert queue.publish({"k": "x"}) is True
    assert client.llen(queue.key.pending) == 1


@pytest.mark.asyncio
async def test_async_publish_rejects_int_keyed_dict_without_runtime_behavior_change() -> None:
    queue = AsyncRedisMessageQueue("payload-async", client=fakeredis.FakeAsyncRedis(), deduplication=False)

    with pytest.raises(TypeError, match=r"'message' dict keys must all be strings; got non-string keys: \[1\]"):
        await queue.publish({1: "x"})


@pytest.mark.asyncio
async def test_async_publish_accepts_string_keyed_dict() -> None:
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue("payload-async-ok", client=client, deduplication=False)

    assert await queue.publish({"k": "x"}) is True
    assert await client.llen(queue.key.pending) == 1
