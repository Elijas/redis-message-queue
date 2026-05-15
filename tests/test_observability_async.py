import warnings

import fakeredis
import pytest

from redis_message_queue.asyncio import QueueEvent, RedisMessageQueue


@pytest.mark.asyncio
async def test_async_event_hook_is_awaited_for_publish_claim_and_ack():
    client = fakeredis.FakeAsyncRedis()
    events: list[QueueEvent] = []

    async def observe(event: QueueEvent) -> None:
        events.append(event)

    queue = RedisMessageQueue("observed-async", client=client, on_event=observe)

    assert await queue.publish("hello") is True
    async with queue.process_message() as message:
        assert message == b"hello"

    operations = [event.operation for event in events]
    assert "publish" in operations
    assert "claim" in operations
    assert "ack" in operations
    assert any(event.message_id and event.lease_token_hash for event in events if event.operation == "claim")
    await client.aclose()


@pytest.mark.asyncio
async def test_async_event_callback_exception_is_warned_not_propagated():
    client = fakeredis.FakeAsyncRedis()

    async def fail(_event: QueueEvent) -> None:
        raise RuntimeError("observer down")

    queue = RedisMessageQueue("observed-async", client=client, on_event=fail)
    with pytest.warns(RuntimeWarning, match="on_event callback raised RuntimeError"):
        assert await queue.publish("hello") is True
    assert await client.llen(queue.key.pending) == 1
    await client.aclose()


@pytest.mark.asyncio
async def test_async_event_callback_exception_warning_error_filter_does_not_escape_publish():
    client = fakeredis.FakeAsyncRedis()

    async def fail(_event: QueueEvent) -> None:
        raise RuntimeError("observer down")

    queue = RedisMessageQueue("observed-async", client=client, on_event=fail)
    with pytest.warns(RuntimeWarning, match="on_event callback raised RuntimeError"):
        with warnings.catch_warnings():
            warnings.simplefilter("error", RuntimeWarning)
            assert await queue.publish("hello") is True
    assert await client.llen(queue.key.pending) == 1
    await client.aclose()
