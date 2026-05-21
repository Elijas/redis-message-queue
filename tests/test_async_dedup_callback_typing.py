import shutil
import subprocess
from collections.abc import Awaitable
from pathlib import Path
from typing import cast

import fakeredis
import pytest

from redis_message_queue.asyncio.redis_message_queue import RedisMessageQueue


def _sync_dedup_key(message: object) -> str:
    return f"sync:{message}"


async def _async_dedup_key(message: object) -> str:
    return f"async:{message}"


def test_async_queue_accepts_sync_dedup_key_callable() -> None:
    queue = RedisMessageQueue(
        "ad-28-f2-sync",
        client=fakeredis.FakeAsyncRedis(),
        deduplication=True,
        get_deduplication_key=_sync_dedup_key,
    )

    assert queue.key.pending == "ad-28-f2-sync::pending"


def test_async_queue_accepts_async_dedup_key_callable() -> None:
    queue = RedisMessageQueue(
        "ad-28-f2-async",
        client=fakeredis.FakeAsyncRedis(),
        deduplication=True,
        get_deduplication_key=_async_dedup_key,
    )

    assert queue.key.pending == "ad-28-f2-async::pending"


@pytest.mark.asyncio
async def test_async_dedup_key_callable_is_awaited_at_publish() -> None:
    client = fakeredis.FakeAsyncRedis()
    seen: list[object] = []

    async def dedup_key(message: object) -> str:
        seen.append(message)
        return "awaited-key"

    queue = RedisMessageQueue(
        "ad-28-f2-await",
        client=client,
        deduplication=True,
        get_deduplication_key=dedup_key,
    )

    assert await queue.publish({"tenant": "acme"}) is True
    assert seen == [{"tenant": "acme"}]
    assert await cast(Awaitable[int], client.exists(queue.key.deduplication("awaited-key"))) == 1

    assert await queue.publish({"tenant": "acme"}) is False
    assert await cast(Awaitable[int], client.llen(queue.key.pending)) == 1


def test_async_dedup_key_repro_passes_strict_mypy() -> None:
    mypy = shutil.which("mypy")
    if mypy is None:
        pytest.skip("mypy executable is required for the AD-28-F2 typing probe")
    probe = Path(__file__).with_name("AD-28-F2-async-dedup-key.py")
    result = subprocess.run(
        [mypy, "--strict", "--follow-imports=silent", str(probe)],
        cwd=Path(__file__).resolve().parents[1],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
