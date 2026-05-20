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
    extract_stored_message_id,
)
from redis_message_queue.asyncio import RedisMessageQueue as AsyncRedisMessageQueue

FOREIGN_BINARY_PAYLOAD = b"\x80\x04\x95celery-or-rq-pickle"
FOREIGN_REDIS_PAYLOAD = b"celery-or-rq-pickle"
STRICT_ERROR = "value does not start with RMQ envelope prefix; expected an rmq-published message"
MALFORMED_PREFIXED_PAYLOAD = f"{_STORED_MESSAGE_PREFIX}{{"


def _claim_events(events: list[QueueEvent]) -> list[QueueEvent]:
    return [event for event in events if event.operation is EventOperation.CLAIM]


def _assert_claim_failure(events: list[QueueEvent]) -> None:
    claim_events = _claim_events(events)
    assert len(claim_events) == 1
    claim_event = claim_events[0]
    assert claim_event.outcome is EventOutcome.FAILURE
    assert claim_event.exception_type == "MalformedStoredMessageError"
    assert isinstance(claim_event.error, MalformedStoredMessageError)
    assert claim_event.message_id is None
    assert claim_event.lease_token_hash is not None


def _assert_claim_success(events: list[QueueEvent], *, has_message_id: bool) -> None:
    claim_events = _claim_events(events)
    assert len(claim_events) == 1
    claim_event = claim_events[0]
    assert claim_event.outcome is EventOutcome.SUCCESS
    assert claim_event.error is None
    assert bool(claim_event.message_id) is has_message_id


def test_stored_message_helpers_default_preserve_non_envelope_payloads():
    assert decode_stored_message(FOREIGN_BINARY_PAYLOAD) == FOREIGN_BINARY_PAYLOAD
    assert extract_stored_message_id(FOREIGN_BINARY_PAYLOAD) is None
    assert decode_stored_message("foreign-payload") == "foreign-payload"
    assert extract_stored_message_id("foreign-payload") is None


def test_stored_message_helpers_strict_reject_non_envelope_payloads():
    with pytest.raises(MalformedStoredMessageError, match=STRICT_ERROR):
        decode_stored_message(FOREIGN_BINARY_PAYLOAD, strict_envelope_decoding=True)
    with pytest.raises(MalformedStoredMessageError, match=STRICT_ERROR):
        extract_stored_message_id(FOREIGN_BINARY_PAYLOAD, strict_envelope_decoding=True)
    with pytest.raises(MalformedStoredMessageError, match=STRICT_ERROR):
        decode_stored_message("foreign-payload", strict_envelope_decoding=True)
    with pytest.raises(MalformedStoredMessageError, match=STRICT_ERROR):
        extract_stored_message_id("foreign-payload", strict_envelope_decoding=True)


def test_sync_default_non_envelope_payload_yields_as_is():
    client = fakeredis.FakeRedis()
    events: list[QueueEvent] = []
    queue = RedisMessageQueue("strict-envelope-default-sync", client=client, on_event=events.append)
    client.lpush(queue.key.pending, FOREIGN_REDIS_PAYLOAD)

    with queue.process_message() as message:
        assert message == FOREIGN_REDIS_PAYLOAD

    _assert_claim_success(events, has_message_id=False)
    assert client.llen(queue.key.processing) == 0


def test_sync_strict_non_envelope_payload_emits_claim_failure_and_stays_processing():
    client = fakeredis.FakeRedis()
    events: list[QueueEvent] = []
    queue = RedisMessageQueue(
        "strict-envelope-foreign-sync",
        client=client,
        strict_envelope_decoding=True,
        on_event=events.append,
    )
    client.lpush(queue.key.pending, FOREIGN_REDIS_PAYLOAD)

    with pytest.raises(MalformedStoredMessageError, match=STRICT_ERROR):
        with queue.process_message():
            raise AssertionError("strict decoding should fail before yielding")

    _assert_claim_failure(events)
    assert client.llen(queue.key.pending) == 0
    assert client.llen(queue.key.processing) == 1
    assert client.lindex(queue.key.processing, 0) == FOREIGN_REDIS_PAYLOAD


def test_sync_strict_prefix_malformed_payload_still_raises():
    client = fakeredis.FakeRedis()
    events: list[QueueEvent] = []
    queue = RedisMessageQueue(
        "strict-envelope-malformed-sync",
        client=client,
        strict_envelope_decoding=True,
        on_event=events.append,
    )
    client.lpush(queue.key.pending, MALFORMED_PREFIXED_PAYLOAD)

    with pytest.raises(MalformedStoredMessageError, match="valid JSON"):
        with queue.process_message():
            raise AssertionError("malformed envelope should fail before yielding")

    _assert_claim_failure(events)
    assert client.llen(queue.key.pending) == 0
    assert client.llen(queue.key.processing) == 1
    assert client.lindex(queue.key.processing, 0) == MALFORMED_PREFIXED_PAYLOAD.encode("utf-8")


def test_sync_strict_valid_envelope_decodes():
    client = fakeredis.FakeRedis()
    events: list[QueueEvent] = []
    queue = RedisMessageQueue(
        "strict-envelope-valid-sync",
        client=client,
        strict_envelope_decoding=True,
        on_event=events.append,
    )
    assert queue.publish("payload") is True

    with queue.process_message() as message:
        assert message == b"payload"

    _assert_claim_success(events, has_message_id=True)
    assert client.llen(queue.key.processing) == 0


@pytest.mark.asyncio
async def test_async_default_non_envelope_payload_yields_as_is():
    client = fakeredis.FakeAsyncRedis()
    events: list[QueueEvent] = []

    async def observe(event: QueueEvent) -> None:
        events.append(event)

    queue = AsyncRedisMessageQueue("strict-envelope-default-async", client=client, on_event=observe)
    await client.lpush(queue.key.pending, FOREIGN_REDIS_PAYLOAD)

    async with queue.process_message() as message:
        assert message == FOREIGN_REDIS_PAYLOAD

    _assert_claim_success(events, has_message_id=False)
    assert await client.llen(queue.key.processing) == 0
    await client.aclose()


@pytest.mark.asyncio
async def test_async_strict_non_envelope_payload_emits_claim_failure_and_stays_processing():
    client = fakeredis.FakeAsyncRedis()
    events: list[QueueEvent] = []

    async def observe(event: QueueEvent) -> None:
        events.append(event)

    queue = AsyncRedisMessageQueue(
        "strict-envelope-foreign-async",
        client=client,
        strict_envelope_decoding=True,
        on_event=observe,
    )
    await client.lpush(queue.key.pending, FOREIGN_REDIS_PAYLOAD)

    with pytest.raises(MalformedStoredMessageError, match=STRICT_ERROR):
        async with queue.process_message():
            raise AssertionError("strict decoding should fail before yielding")

    _assert_claim_failure(events)
    assert await client.llen(queue.key.pending) == 0
    assert await client.llen(queue.key.processing) == 1
    assert await client.lindex(queue.key.processing, 0) == FOREIGN_REDIS_PAYLOAD
    await client.aclose()


@pytest.mark.asyncio
async def test_async_strict_prefix_malformed_payload_still_raises():
    client = fakeredis.FakeAsyncRedis()
    events: list[QueueEvent] = []

    async def observe(event: QueueEvent) -> None:
        events.append(event)

    queue = AsyncRedisMessageQueue(
        "strict-envelope-malformed-async",
        client=client,
        strict_envelope_decoding=True,
        on_event=observe,
    )
    await client.lpush(queue.key.pending, MALFORMED_PREFIXED_PAYLOAD)

    with pytest.raises(MalformedStoredMessageError, match="valid JSON"):
        async with queue.process_message():
            raise AssertionError("malformed envelope should fail before yielding")

    _assert_claim_failure(events)
    assert await client.llen(queue.key.pending) == 0
    assert await client.llen(queue.key.processing) == 1
    assert await client.lindex(queue.key.processing, 0) == MALFORMED_PREFIXED_PAYLOAD.encode("utf-8")
    await client.aclose()


@pytest.mark.asyncio
async def test_async_strict_valid_envelope_decodes():
    client = fakeredis.FakeAsyncRedis()
    events: list[QueueEvent] = []

    async def observe(event: QueueEvent) -> None:
        events.append(event)

    queue = AsyncRedisMessageQueue(
        "strict-envelope-valid-async",
        client=client,
        strict_envelope_decoding=True,
        on_event=observe,
    )
    assert await queue.publish("payload") is True

    async with queue.process_message() as message:
        assert message == b"payload"

    _assert_claim_success(events, has_message_id=True)
    assert await client.llen(queue.key.processing) == 0
    await client.aclose()
