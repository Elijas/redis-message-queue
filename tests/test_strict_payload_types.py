import json
from datetime import datetime

import fakeredis
import pytest

from redis_message_queue.asyncio.redis_message_queue import RedisMessageQueue as AsyncRedisMessageQueue
from redis_message_queue.redis_message_queue import RedisMessageQueue


def _sync_queue(*, strict_payload_types: bool = False) -> RedisMessageQueue:
    return RedisMessageQueue(
        "test-queue",
        client=fakeredis.FakeRedis(),
        strict_payload_types=strict_payload_types,
    )


def _async_queue(*, strict_payload_types: bool = False) -> AsyncRedisMessageQueue:
    return AsyncRedisMessageQueue(
        "test-queue",
        client=fakeredis.FakeAsyncRedis(),
        strict_payload_types=strict_payload_types,
    )


def test_sync_default_mode_allows_tuple_and_consumes_list():
    queue = _sync_queue()

    assert queue.publish({"coords": (1, 2)}) is True

    with queue.process_message() as message:
        assert json.loads(message)["coords"] == [1, 2]


@pytest.mark.asyncio
async def test_async_default_mode_allows_tuple_and_consumes_list():
    queue = _async_queue()

    assert await queue.publish({"coords": (1, 2)}) is True

    async with queue.process_message() as message:
        assert json.loads(message)["coords"] == [1, 2]


def test_sync_strict_mode_rejects_tuple_with_path():
    queue = _sync_queue(strict_payload_types=True)

    with pytest.raises(TypeError) as exc_info:
        queue.publish({"coords": (1, 2)})

    assert str(exc_info.value) == (
        "strict_payload_types=True: value at message['coords'] is a tuple; "
        "JSON does not preserve tuples (becomes list). "
        "Either convert to list explicitly or disable strict mode."
    )


@pytest.mark.asyncio
async def test_async_strict_mode_rejects_tuple_with_path():
    queue = _async_queue(strict_payload_types=True)

    with pytest.raises(TypeError) as exc_info:
        await queue.publish({"coords": (1, 2)})

    assert str(exc_info.value) == (
        "strict_payload_types=True: value at message['coords'] is a tuple; "
        "JSON does not preserve tuples (becomes list). "
        "Either convert to list explicitly or disable strict mode."
    )


def test_sync_strict_mode_rejects_nested_tuple_with_full_path():
    queue = _sync_queue(strict_payload_types=True)

    with pytest.raises(TypeError) as exc_info:
        queue.publish({"nested": {"inner": (1, 2)}})

    assert "message['nested']['inner']" in str(exc_info.value)
    assert "tuple" in str(exc_info.value)


@pytest.mark.asyncio
async def test_async_strict_mode_rejects_nested_tuple_with_full_path():
    queue = _async_queue(strict_payload_types=True)

    with pytest.raises(TypeError) as exc_info:
        await queue.publish({"nested": {"inner": (1, 2)}})

    assert "message['nested']['inner']" in str(exc_info.value)
    assert "tuple" in str(exc_info.value)


@pytest.mark.parametrize(
    ("payload", "path", "type_name"),
    [
        ({"tags": {"red"}}, "message['tags']", "set"),
        ({"tags": frozenset({"red"})}, "message['tags']", "frozenset"),
        ({"blob": b"raw"}, "message['blob']", "bytes"),
        ({"when": datetime(2026, 5, 21, 12, 0, 0)}, "message['when']", "datetime"),
    ],
)
def test_sync_strict_mode_rejects_non_json_native_types(payload, path, type_name):
    queue = _sync_queue(strict_payload_types=True)

    with pytest.raises(TypeError) as exc_info:
        queue.publish(payload)

    error = str(exc_info.value)
    assert "strict_payload_types=True" in error
    assert path in error
    assert type_name in error


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("payload", "path", "type_name"),
    [
        ({"tags": {"red"}}, "message['tags']", "set"),
        ({"tags": frozenset({"red"})}, "message['tags']", "frozenset"),
        ({"blob": b"raw"}, "message['blob']", "bytes"),
        ({"when": datetime(2026, 5, 21, 12, 0, 0)}, "message['when']", "datetime"),
    ],
)
async def test_async_strict_mode_rejects_non_json_native_types(payload, path, type_name):
    queue = _async_queue(strict_payload_types=True)

    with pytest.raises(TypeError) as exc_info:
        await queue.publish(payload)

    error = str(exc_info.value)
    assert "strict_payload_types=True" in error
    assert path in error
    assert type_name in error


def test_sync_strict_mode_allows_json_native_types():
    queue = _sync_queue(strict_payload_types=True)
    payload = {
        "list": [1, "two", None],
        "dict": {"none": None, "bool": True, "int": 1, "float": 1.5, "str": "x"},
    }

    assert queue.publish(payload) is True

    with queue.process_message() as message:
        assert json.loads(message) == payload


@pytest.mark.asyncio
async def test_async_strict_mode_allows_json_native_types():
    queue = _async_queue(strict_payload_types=True)
    payload = {
        "list": [1, "two", None],
        "dict": {"none": None, "bool": True, "int": 1, "float": 1.5, "str": "x"},
    }

    assert await queue.publish(payload) is True

    async with queue.process_message() as message:
        assert json.loads(message) == payload
