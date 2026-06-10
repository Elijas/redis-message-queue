import fakeredis
import pytest

from redis_message_queue import PayloadTooDeepError, PayloadTooLargeError
from redis_message_queue.asyncio import RedisMessageQueue as AsyncRedisMessageQueue
from redis_message_queue.redis_message_queue import RedisMessageQueue


def _nested_dict(depth: int) -> dict:
    value: object = "leaf"
    for index in range(depth, 0, -1):
        value = {f"k{index}": value}
    return value  # type: ignore[return-value]


def _nested_list(depth: int) -> list:
    value: object = "leaf"
    for _ in range(depth):
        value = [value]
    return value  # type: ignore[return-value]


def _sync_queue(**kwargs: object) -> RedisMessageQueue:
    return RedisMessageQueue("test-queue", client=fakeredis.FakeRedis(), **kwargs)


def _async_queue(**kwargs: object) -> AsyncRedisMessageQueue:
    return AsyncRedisMessageQueue("test-queue", client=fakeredis.FakeAsyncRedis(), **kwargs)


def test_sync_default_allows_huge_string():
    queue = _sync_queue()

    assert queue.publish("x" * (10 * 1024 * 1024)) is True


@pytest.mark.asyncio
async def test_async_default_allows_huge_string():
    queue = _async_queue()

    assert await queue.publish("x" * (10 * 1024 * 1024)) is True


def test_sync_default_allows_deep_dict():
    queue = _sync_queue()

    assert queue.publish(_nested_dict(50)) is True


@pytest.mark.asyncio
async def test_async_default_allows_deep_dict():
    queue = _async_queue()

    assert await queue.publish(_nested_dict(50)) is True


def test_sync_max_payload_bytes_rejects_oversized_str():
    queue = _sync_queue(max_payload_bytes=1024)

    with pytest.raises(PayloadTooLargeError) as exc_info:
        queue.publish("x" * 2048)

    error = str(exc_info.value)
    assert "max_payload_bytes=1024 exceeded" in error
    assert "payload is 2048 bytes" in error
    assert "(str message)" in error


@pytest.mark.asyncio
async def test_async_max_payload_bytes_rejects_oversized_str():
    queue = _async_queue(max_payload_bytes=1024)

    with pytest.raises(PayloadTooLargeError) as exc_info:
        await queue.publish("x" * 2048)

    error = str(exc_info.value)
    assert "max_payload_bytes=1024 exceeded" in error
    assert "payload is 2048 bytes" in error
    assert "(str message)" in error


def test_sync_max_payload_bytes_rejects_oversized_dict():
    queue = _sync_queue(max_payload_bytes=1024)

    with pytest.raises(PayloadTooLargeError) as exc_info:
        queue.publish({"body": "x" * 2048})

    error = str(exc_info.value)
    assert "max_payload_bytes=1024 exceeded" in error
    assert "payload is " in error
    assert " bytes" in error
    assert "(dict message)" in error


@pytest.mark.asyncio
async def test_async_max_payload_bytes_rejects_oversized_dict():
    queue = _async_queue(max_payload_bytes=1024)

    with pytest.raises(PayloadTooLargeError) as exc_info:
        await queue.publish({"body": "x" * 2048})

    error = str(exc_info.value)
    assert "max_payload_bytes=1024 exceeded" in error
    assert "payload is " in error
    assert " bytes" in error
    assert "(dict message)" in error


def test_sync_large_max_payload_bytes_allows_tiny_payload():
    queue = _sync_queue(max_payload_bytes=10**9)

    assert queue.publish({"ok": True}) is True


@pytest.mark.asyncio
async def test_async_large_max_payload_bytes_allows_tiny_payload():
    queue = _async_queue(max_payload_bytes=10**9)

    assert await queue.publish({"ok": True}) is True


def test_sync_max_payload_depth_rejects_deep_dict_with_path():
    queue = _sync_queue(max_payload_depth=5)

    with pytest.raises(PayloadTooDeepError) as exc_info:
        queue.publish(_nested_dict(7))

    error = str(exc_info.value)
    assert "max_payload_depth=5 exceeded" in error
    assert "depth 6 reached" in error
    assert "message['k1']['k2']['k3']['k4']['k5']['k6']" in error


@pytest.mark.asyncio
async def test_async_max_payload_depth_rejects_deep_dict_with_path():
    queue = _async_queue(max_payload_depth=5)

    with pytest.raises(PayloadTooDeepError) as exc_info:
        await queue.publish(_nested_dict(7))

    error = str(exc_info.value)
    assert "max_payload_depth=5 exceeded" in error
    assert "depth 6 reached" in error
    assert "message['k1']['k2']['k3']['k4']['k5']['k6']" in error


def test_sync_max_payload_depth_allows_shallow_dict():
    queue = _sync_queue(max_payload_depth=5)

    assert queue.publish(_nested_dict(3)) is True


@pytest.mark.asyncio
async def test_async_max_payload_depth_allows_shallow_dict():
    queue = _async_queue(max_payload_depth=5)

    assert await queue.publish(_nested_dict(3)) is True


def test_sync_max_payload_depth_rejects_deep_list_inside_dict():
    queue = _sync_queue(max_payload_depth=5)

    with pytest.raises(PayloadTooDeepError) as exc_info:
        queue.publish({"items": _nested_list(7)})

    error = str(exc_info.value)
    assert "max_payload_depth=5 exceeded" in error
    assert "depth 6 reached" in error
    assert "message['items']" in error
    assert "[0][0][0][0][0]" in error


@pytest.mark.asyncio
async def test_async_max_payload_depth_rejects_deep_list_inside_dict():
    queue = _async_queue(max_payload_depth=5)

    with pytest.raises(PayloadTooDeepError) as exc_info:
        await queue.publish({"items": _nested_list(7)})

    error = str(exc_info.value)
    assert "max_payload_depth=5 exceeded" in error
    assert "depth 6 reached" in error
    assert "message['items']" in error
    assert "[0][0][0][0][0]" in error


def test_sync_depth_walker_is_iterative_for_very_deep_dict():
    queue = _sync_queue(max_payload_depth=10)

    try:
        with pytest.raises(PayloadTooDeepError):
            queue.publish(_nested_dict(5000))
    except RecursionError as exc:
        pytest.fail(f"depth guard must be iterative, got {exc!r}")


@pytest.mark.asyncio
async def test_async_depth_walker_is_iterative_for_very_deep_dict():
    queue = _async_queue(max_payload_depth=10)

    try:
        with pytest.raises(PayloadTooDeepError):
            await queue.publish(_nested_dict(5000))
    except RecursionError as exc:
        pytest.fail(f"depth guard must be iterative, got {exc!r}")


# Aliased (shared / DAG-shaped) sub-objects: json.dumps expands them into independent copies,
# so depth must be measured along every mount path, not just the first one reached.


def test_sync_aliased_dict_mounted_deep_first_is_rejected():
    queue = _sync_queue(max_payload_depth=5)
    shared = {"a": {"b": {"c": 1}}}  # leaf depth 3 below its mount point
    message = {
        "y": {"l1": {"l2": {"l3": {"l4": shared}}}},  # leaf reaches depth 8
        "x": shared,  # leaf reaches depth 4
    }

    with pytest.raises(PayloadTooDeepError) as exc_info:
        queue.publish(message)

    assert "max_payload_depth=5 exceeded" in str(exc_info.value)


def test_sync_aliased_dict_mounted_shallow_first_is_rejected():
    queue = _sync_queue(max_payload_depth=5)
    shared = {"a": {"b": {"c": 1}}}
    message = {
        "x": shared,  # shallow mount listed first
        "y": {"l1": {"l2": {"l3": {"l4": shared}}}},
    }

    with pytest.raises(PayloadTooDeepError) as exc_info:
        queue.publish(message)

    assert "max_payload_depth=5 exceeded" in str(exc_info.value)


@pytest.mark.asyncio
async def test_async_aliased_dict_mounted_deep_first_is_rejected():
    queue = _async_queue(max_payload_depth=5)
    shared = {"a": {"b": {"c": 1}}}
    message = {
        "y": {"l1": {"l2": {"l3": {"l4": shared}}}},
        "x": shared,
    }

    with pytest.raises(PayloadTooDeepError) as exc_info:
        await queue.publish(message)

    assert "max_payload_depth=5 exceeded" in str(exc_info.value)


def test_sync_aliased_list_mounted_deep_is_rejected():
    queue = _sync_queue(max_payload_depth=5)
    shared = [[["c"]]]  # leaf depth 3 below its mount point
    message = {
        "x": shared,
        "y": [[[[[shared]]]]],  # leaf reaches well past depth 5
    }

    with pytest.raises(PayloadTooDeepError) as exc_info:
        queue.publish(message)

    assert "max_payload_depth=5 exceeded" in str(exc_info.value)


def test_sync_aliased_tuple_mounted_deep_is_rejected():
    queue = _sync_queue(max_payload_depth=5)
    shared = (((("c"),),),)  # nested tuples
    message = {
        "x": shared,
        "y": ((((shared,),),),),
    }

    with pytest.raises(PayloadTooDeepError) as exc_info:
        queue.publish(message)

    assert "max_payload_depth=5 exceeded" in str(exc_info.value)


def test_sync_aliased_dict_within_limit_is_allowed():
    queue = _sync_queue(max_payload_depth=5)
    shared = {"c": 1}  # leaf at depth 1 below its mount point
    message = {
        "x": shared,  # leaf reaches depth 2
        "y": {"l1": {"l2": shared}},  # leaf reaches depth 4, still within limit
    }

    assert queue.publish(message) is True


@pytest.mark.asyncio
async def test_async_aliased_dict_within_limit_is_allowed():
    queue = _async_queue(max_payload_depth=5)
    shared = {"c": 1}
    message = {
        "x": shared,
        "y": {"l1": {"l2": shared}},
    }

    assert await queue.publish(message) is True


def test_sync_self_referencing_dict_raises_and_does_not_hang():
    queue = _sync_queue(max_payload_depth=5)
    message: dict = {}
    message["self"] = message  # 1-node cycle: infinitely deep when serialized

    with pytest.raises(PayloadTooDeepError) as exc_info:
        queue.publish(message)

    assert "max_payload_depth=5 exceeded" in str(exc_info.value)


def test_sync_two_node_cycle_raises_and_does_not_hang():
    queue = _sync_queue(max_payload_depth=5)
    a: dict = {}
    b: dict = {"a": a}
    a["b"] = b  # 2-node cycle a -> b -> a

    with pytest.raises(PayloadTooDeepError) as exc_info:
        queue.publish({"root": a})

    assert "max_payload_depth=5 exceeded" in str(exc_info.value)


@pytest.mark.asyncio
async def test_async_two_node_cycle_raises_and_does_not_hang():
    queue = _async_queue(max_payload_depth=5)
    a: dict = {}
    b: dict = {"a": a}
    a["b"] = b

    with pytest.raises(PayloadTooDeepError) as exc_info:
        await queue.publish({"root": a})

    assert "max_payload_depth=5 exceeded" in str(exc_info.value)


def test_sync_list_cycle_raises_and_does_not_hang():
    queue = _sync_queue(max_payload_depth=5)
    inner: list = []
    inner.append(inner)  # self-referencing list

    with pytest.raises(PayloadTooDeepError) as exc_info:
        queue.publish({"items": inner})

    assert "max_payload_depth=5 exceeded" in str(exc_info.value)
