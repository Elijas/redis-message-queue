import fakeredis
import pytest

from redis_message_queue import (
    EventOperation,
    EventOutcome,
    MalformedStoredMessageError,
    QueueEvent,
    RedisMessageQueue,
)
from redis_message_queue._stored_message import (
    _STORED_MESSAGE_PREFIX,
    decode_stored_message,
    encode_stored_message,
    extract_stored_message_id,
)
from redis_message_queue.asyncio import RedisMessageQueue as AsyncRedisMessageQueue

MALFORMED_ENVELOPES = [
    f"{_STORED_MESSAGE_PREFIX}{{",
    f'{_STORED_MESSAGE_PREFIX}{{"payload":"missing-id"}}',
    f'{_STORED_MESSAGE_PREFIX}{{"id":"missing-payload"}}',
]


def test_stored_message_decode_preserves_raw_and_valid_envelopes():
    encoded = encode_stored_message("payload")

    assert decode_stored_message("raw-payload") == "raw-payload"
    assert extract_stored_message_id("raw-payload") is None
    assert decode_stored_message(encoded) == "payload"
    assert extract_stored_message_id(encoded)


@pytest.mark.parametrize("stored_message", MALFORMED_ENVELOPES)
def test_stored_message_decode_rejects_malformed_prefixed_values(stored_message: str):
    with pytest.raises(MalformedStoredMessageError):
        decode_stored_message(stored_message)
    with pytest.raises(MalformedStoredMessageError):
        extract_stored_message_id(stored_message)


def test_sync_process_message_preserves_raw_payloads():
    client = fakeredis.FakeRedis()
    events: list[QueueEvent] = []
    queue = RedisMessageQueue("malformed-envelope-raw-sync", client=client, on_event=events.append)
    client.lpush(queue.key.pending, "raw-payload")

    with queue.process_message() as message:
        assert message == b"raw-payload"

    claim_event = next(event for event in events if event.operation is EventOperation.CLAIM)
    assert claim_event.outcome is EventOutcome.SUCCESS
    assert claim_event.message_id is None
    assert claim_event.error is None
    assert client.llen(queue.key.processing) == 0


def test_sync_process_message_preserves_valid_envelopes():
    client = fakeredis.FakeRedis()
    events: list[QueueEvent] = []
    queue = RedisMessageQueue("malformed-envelope-valid-sync", client=client, on_event=events.append)
    assert queue.publish("payload") is True

    with queue.process_message() as message:
        assert message == b"payload"

    claim_event = next(event for event in events if event.operation is EventOperation.CLAIM)
    assert claim_event.outcome is EventOutcome.SUCCESS
    assert claim_event.message_id is not None
    assert claim_event.error is None


@pytest.mark.parametrize("stored_message", MALFORMED_ENVELOPES)
def test_sync_process_message_malformed_envelope_emits_claim_failure_and_leaves_processing(stored_message: str):
    client = fakeredis.FakeRedis()
    events: list[QueueEvent] = []
    queue = RedisMessageQueue("malformed-envelope-sync", client=client, on_event=events.append)
    client.lpush(queue.key.pending, stored_message)

    with pytest.raises(MalformedStoredMessageError):
        with queue.process_message():
            raise AssertionError("malformed envelope should fail before yielding")

    claim_events = [event for event in events if event.operation is EventOperation.CLAIM]
    assert len(claim_events) == 1
    claim_event = claim_events[0]
    assert claim_event.outcome is EventOutcome.FAILURE
    assert claim_event.exception_type == "MalformedStoredMessageError"
    assert isinstance(claim_event.error, MalformedStoredMessageError)
    assert claim_event.lease_token_hash is not None
    assert client.llen(queue.key.pending) == 0
    assert client.llen(queue.key.processing) == 1
    assert client.lindex(queue.key.processing, 0) == stored_message.encode("utf-8")


@pytest.mark.asyncio
async def test_async_process_message_preserves_raw_payloads():
    client = fakeredis.FakeAsyncRedis()
    events: list[QueueEvent] = []

    async def observe(event: QueueEvent) -> None:
        events.append(event)

    queue = AsyncRedisMessageQueue("malformed-envelope-raw-async", client=client, on_event=observe)
    await client.lpush(queue.key.pending, "raw-payload")

    async with queue.process_message() as message:
        assert message == b"raw-payload"

    claim_event = next(event for event in events if event.operation is EventOperation.CLAIM)
    assert claim_event.outcome is EventOutcome.SUCCESS
    assert claim_event.message_id is None
    assert claim_event.error is None
    assert await client.llen(queue.key.processing) == 0
    await client.aclose()


@pytest.mark.asyncio
async def test_async_process_message_preserves_valid_envelopes():
    client = fakeredis.FakeAsyncRedis()
    events: list[QueueEvent] = []

    async def observe(event: QueueEvent) -> None:
        events.append(event)

    queue = AsyncRedisMessageQueue("malformed-envelope-valid-async", client=client, on_event=observe)
    assert await queue.publish("payload") is True

    async with queue.process_message() as message:
        assert message == b"payload"

    claim_event = next(event for event in events if event.operation is EventOperation.CLAIM)
    assert claim_event.outcome is EventOutcome.SUCCESS
    assert claim_event.message_id is not None
    assert claim_event.error is None
    await client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("stored_message", MALFORMED_ENVELOPES)
async def test_async_process_message_malformed_envelope_emits_claim_failure_and_leaves_processing(
    stored_message: str,
):
    client = fakeredis.FakeAsyncRedis()
    events: list[QueueEvent] = []

    async def observe(event: QueueEvent) -> None:
        events.append(event)

    queue = AsyncRedisMessageQueue("malformed-envelope-async", client=client, on_event=observe)
    await client.lpush(queue.key.pending, stored_message)

    with pytest.raises(MalformedStoredMessageError):
        async with queue.process_message():
            raise AssertionError("malformed envelope should fail before yielding")

    claim_events = [event for event in events if event.operation is EventOperation.CLAIM]
    assert len(claim_events) == 1
    claim_event = claim_events[0]
    assert claim_event.outcome is EventOutcome.FAILURE
    assert claim_event.exception_type == "MalformedStoredMessageError"
    assert isinstance(claim_event.error, MalformedStoredMessageError)
    assert claim_event.lease_token_hash is not None
    assert await client.llen(queue.key.pending) == 0
    assert await client.llen(queue.key.processing) == 1
    assert await client.lindex(queue.key.processing, 0) == stored_message.encode("utf-8")
    await client.aclose()
