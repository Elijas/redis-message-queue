import asyncio
import warnings

import fakeredis
import pytest
import redis

from redis_message_queue.asyncio import EventOperation, EventOutcome, QueueEvent, RedisMessageQueue


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
    assert events[0].operation is EventOperation.PUBLISH
    assert events[0].outcome is EventOutcome.SUCCESS
    assert events[0].error is None
    assert "claim" in operations
    assert "ack" in operations
    assert any(event.message_id and event.lease_token_hash for event in events if event.operation == "claim")
    assert all(event.error is None for event in events)
    await client.aclose()


@pytest.mark.asyncio
async def test_async_event_hook_accepts_function_returning_awaitable():
    client = fakeredis.FakeAsyncRedis()
    events: list[QueueEvent] = []

    async def observe_async(event: QueueEvent) -> None:
        events.append(event)

    def observe(event: QueueEvent):
        return observe_async(event)

    queue = RedisMessageQueue("observed-async", client=client, on_event=observe)

    assert await queue.publish("hello") is True

    assert [event.operation for event in events] == [EventOperation.PUBLISH]
    assert events[0].outcome is EventOutcome.SUCCESS
    await client.aclose()


@pytest.mark.asyncio
async def test_async_event_hook_warns_when_callback_returns_non_awaitable():
    client = fakeredis.FakeAsyncRedis()
    events: list[QueueEvent] = []

    def observe(event: QueueEvent) -> None:
        events.append(event)

    queue = RedisMessageQueue("observed-async", client=client, on_event=observe)

    with pytest.warns(RuntimeWarning, match="on_event callback raised TypeError"):
        assert await queue.publish("hello") is True

    assert [event.operation for event in events] == [EventOperation.PUBLISH]
    assert await client.llen(queue.key.pending) == 1
    await client.aclose()


@pytest.mark.asyncio
async def test_async_event_hook_emits_publish_failure_error_object():
    client = fakeredis.FakeAsyncRedis()
    events: list[QueueEvent] = []

    async def observe(event: QueueEvent) -> None:
        events.append(event)

    queue = RedisMessageQueue("observed-async", client=client, on_event=observe)
    await client.set(queue.key.pending, "not a list")

    with pytest.raises(redis.exceptions.ResponseError):
        await queue.publish("hello")

    event = next(event for event in events if event.operation is EventOperation.PUBLISH)
    assert event.outcome is EventOutcome.FAILURE
    assert event.exception_type is not None
    assert isinstance(event.error, redis.exceptions.ResponseError)
    await client.aclose()


@pytest.mark.asyncio
async def test_async_event_hook_emits_handler_failure_error_object():
    client = fakeredis.FakeAsyncRedis()
    events: list[QueueEvent] = []

    async def observe(event: QueueEvent) -> None:
        events.append(event)

    queue = RedisMessageQueue("observed-async", client=client, on_event=observe)
    assert await queue.publish("hello") is True

    with pytest.raises(ValueError, match="boom"):
        async with queue.process_message():
            raise ValueError("boom")

    event = next(event for event in events if event.operation is EventOperation.FAILED)
    assert event.outcome is EventOutcome.FAILURE
    assert event.exception_type == "ValueError"
    assert isinstance(event.error, ValueError)
    assert all(event.error is None for event in events if event.outcome is EventOutcome.SUCCESS)
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


@pytest.mark.asyncio
async def test_async_event_callback_cancelled_error_is_warned_not_propagated_after_claim():
    client = fakeredis.FakeAsyncRedis()

    async def fail_on_claim_success(event: QueueEvent) -> None:
        if event.operation is EventOperation.CLAIM and event.outcome is EventOutcome.SUCCESS:
            raise asyncio.CancelledError("telemetry cancelled")

    queue = RedisMessageQueue(
        "observed-async",
        client=client,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
        on_event=fail_on_claim_success,
    )
    assert await queue.publish("hello") is True

    with pytest.warns(RuntimeWarning, match="on_event callback raised CancelledError"):
        async with queue.process_message() as message:
            assert message == b"hello"

    assert await client.llen(queue.key.pending) == 0
    assert await client.llen(queue.key.processing) == 0
    await client.aclose()


@pytest.mark.asyncio
async def test_async_event_callback_external_cancellation_is_not_swallowed():
    client = fakeredis.FakeAsyncRedis()
    started = asyncio.Event()
    callback_cancelled = asyncio.Event()

    async def block_on_publish_success(event: QueueEvent) -> None:
        if event.operation is EventOperation.PUBLISH and event.outcome is EventOutcome.SUCCESS:
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                callback_cancelled.set()
                raise

    queue = RedisMessageQueue("observed-async", client=client, on_event=block_on_publish_success)
    task = asyncio.create_task(queue.publish("hello"))
    await asyncio.wait_for(started.wait(), timeout=1)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert callback_cancelled.is_set()
    assert await client.llen(queue.key.pending) == 1
    await client.aclose()
