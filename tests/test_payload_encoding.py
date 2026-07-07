"""Payload encoding policy across publish, decode, and redrive.

One coherent rule set:

- The RMQ envelope is UTF-8 JSON text, so ``publish()`` rejects str payloads
  that cannot be encoded to UTF-8 (lone surrogates) at the boundary instead of
  letting them poison consumers, operator peek, and the dead-letter queue
  downstream. Dict payloads are unaffected: ``json.dumps`` escapes surrogates
  into plain ASCII, which round-trips losslessly.
- Bytes that are not valid UTF-8 legitimately transit the queue as raw foreign
  values. When ``redrive_dead_letters()`` must wrap one in a fresh envelope it
  uses the binary-safe ``payload_hex`` envelope field so the exact original
  bytes round-trip through redrive -> claim -> deliver -> re-dead-letter.
- Decode paths raise the library's typed ``MalformedStoredMessageError`` for
  poison envelopes instead of leaking builtin Unicode errors.
"""

import json

import fakeredis
import pytest

from redis_message_queue import (
    EventOperation,
    EventOutcome,
    MalformedStoredMessageError,
    QueueEvent,
)
from redis_message_queue._redis_gateway import RedisGateway
from redis_message_queue._stored_message import _STORED_MESSAGE_PREFIX_BYTES, decode_stored_message
from redis_message_queue.asyncio._redis_gateway import RedisGateway as AsyncRedisGateway
from redis_message_queue.asyncio.redis_message_queue import RedisMessageQueue as AsyncRedisMessageQueue
from redis_message_queue.redis_message_queue import RedisMessageQueue

LONE_SURROGATE_PAYLOAD = "bad\ud800payload"
# ASCII-only wire bytes: json.dumps(ensure_ascii=True) escapes the surrogate, so
# a foreign writer can store this (valid UTF-8 wire, poison payload) even though
# publish() rejects lone-surrogate payloads at the boundary.
LONE_SURROGATE_ENVELOPE = b'\x1eRMQ1:{"id":"deadbeef","payload":"bad\\ud800payload"}'
INVALID_UTF8_PAYLOADS = [
    b"\xff\xfe\x00binary-blob\x80",
    bytes(range(256)),
    b"\x1eRMQ1:BIN\xff\xfe\x01raw",  # foreign bytes that also collide with the envelope prefix
]


def _sync_vt_gateway(client, queue_name, *, max_delivery_count=2):
    return RedisGateway(
        redis_client=client,
        retry_budget_seconds=0,
        message_wait_interval_seconds=0,
        message_visibility_timeout_seconds=30,
        max_delivery_count=max_delivery_count,
        dead_letter_queue=f"{queue_name}::dlq",
    )


def _async_vt_gateway(client, queue_name, *, max_delivery_count=2):
    return AsyncRedisGateway(
        redis_client=client,
        retry_budget_seconds=0,
        message_wait_interval_seconds=0,
        message_visibility_timeout_seconds=30,
        max_delivery_count=max_delivery_count,
        dead_letter_queue=f"{queue_name}::dlq",
    )


def _decode_pending_envelope(stored: bytes) -> dict:
    assert stored.startswith(_STORED_MESSAGE_PREFIX_BYTES)
    return json.loads(stored[len(_STORED_MESSAGE_PREFIX_BYTES) :].decode("utf-8"))


# ---------------------------------------------------------------------------
# publish() rejects non-UTF-8-encodable str payloads at the boundary
# ---------------------------------------------------------------------------


def test_sync_publish_rejects_lone_surrogate_str_payload():
    events: list[QueueEvent] = []
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue("surrogate-publish-sync", client=client, on_event=events.append)

    with pytest.raises(ValueError, match="not UTF-8-encodable"):
        queue.publish(LONE_SURROGATE_PAYLOAD)

    assert client.llen(queue.key.pending) == 0
    publish_events = [event for event in events if event.operation is EventOperation.PUBLISH]
    assert [event.outcome for event in publish_events] == [EventOutcome.FAILURE]
    assert publish_events[0].exception_type == "ValueError"


def test_sync_publish_rejects_lone_surrogate_str_payload_with_size_limit_configured():
    # The size check encodes the payload too; the boundary validation must win
    # with the same clear error instead of a raw UnicodeEncodeError.
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue("surrogate-publish-sized-sync", client=client, max_payload_bytes=1024)

    with pytest.raises(ValueError, match="not UTF-8-encodable"):
        queue.publish(LONE_SURROGATE_PAYLOAD)

    assert client.llen(queue.key.pending) == 0


def test_sync_publish_dict_payload_with_lone_surrogate_value_round_trips():
    # Dict payloads are JSON-serialized with ensure_ascii escapes, so a lone
    # surrogate inside a value stays plain ASCII on the wire and round-trips.
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue("surrogate-dict-sync", client=client)

    assert queue.publish({"key": LONE_SURROGATE_PAYLOAD}) is True

    with queue.process_message() as message:
        assert json.loads(message) == {"key": LONE_SURROGATE_PAYLOAD}


@pytest.mark.asyncio
async def test_async_publish_rejects_lone_surrogate_str_payload():
    events: list[QueueEvent] = []

    async def observe(event: QueueEvent) -> None:
        events.append(event)

    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue("surrogate-publish-async", client=client, on_event=observe)

    with pytest.raises(ValueError, match="not UTF-8-encodable"):
        await queue.publish(LONE_SURROGATE_PAYLOAD)

    assert await client.llen(queue.key.pending) == 0
    publish_events = [event for event in events if event.operation is EventOperation.PUBLISH]
    assert [event.outcome for event in publish_events] == [EventOutcome.FAILURE]
    assert publish_events[0].exception_type == "ValueError"
    await client.aclose()


# ---------------------------------------------------------------------------
# Decode paths raise the typed error for lone-surrogate envelopes
# ---------------------------------------------------------------------------


def test_decode_stored_message_bytes_envelope_with_lone_surrogate_raises_typed_error():
    with pytest.raises(MalformedStoredMessageError, match="not UTF-8-encodable"):
        decode_stored_message(LONE_SURROGATE_ENVELOPE)


def test_decode_stored_message_str_envelope_with_lone_surrogate_round_trips():
    # decode_responses=True mode delivers the payload as str, which represents
    # the surrogate losslessly; only the bytes wire form cannot encode it.
    assert decode_stored_message(LONE_SURROGATE_ENVELOPE.decode("ascii")) == LONE_SURROGATE_PAYLOAD


def test_sync_process_message_lone_surrogate_envelope_raises_typed_error():
    client = fakeredis.FakeRedis()
    events: list[QueueEvent] = []
    queue = RedisMessageQueue("surrogate-consume-sync", client=client, on_event=events.append)
    client.lpush(queue.key.pending, LONE_SURROGATE_ENVELOPE)

    with pytest.raises(MalformedStoredMessageError):
        with queue.process_message():
            raise AssertionError("poison envelope should fail before yielding")

    claim_events = [event for event in events if event.operation is EventOperation.CLAIM]
    assert len(claim_events) == 1
    assert claim_events[0].outcome is EventOutcome.FAILURE
    assert claim_events[0].exception_type == "MalformedStoredMessageError"


@pytest.mark.asyncio
async def test_async_process_message_lone_surrogate_envelope_raises_typed_error():
    client = fakeredis.FakeAsyncRedis()
    events: list[QueueEvent] = []

    async def observe(event: QueueEvent) -> None:
        events.append(event)

    queue = AsyncRedisMessageQueue("surrogate-consume-async", client=client, on_event=observe)
    await client.lpush(queue.key.pending, LONE_SURROGATE_ENVELOPE)

    with pytest.raises(MalformedStoredMessageError):
        async with queue.process_message():
            raise AssertionError("poison envelope should fail before yielding")

    claim_events = [event for event in events if event.operation is EventOperation.CLAIM]
    assert len(claim_events) == 1
    assert claim_events[0].outcome is EventOutcome.FAILURE
    assert claim_events[0].exception_type == "MalformedStoredMessageError"
    await client.aclose()


# ---------------------------------------------------------------------------
# redrive_dead_letters() round-trips arbitrary bytes exactly (real Redis: the
# fidelity of cjson handling raw bytes is what broke here, so no fakeredis)
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_sync_redrive_round_trips_non_utf8_dlq_payloads_exactly(real_redis_client, queue_name):
    gateway = _sync_vt_gateway(real_redis_client, queue_name)
    queue = RedisMessageQueue(queue_name, gateway=gateway)
    for raw in INVALID_UTF8_PAYLOADS:
        real_redis_client.lpush(f"{queue_name}::dlq", raw)

    assert queue.redrive_dead_letters() == len(INVALID_UTF8_PAYLOADS)

    # Every redriven envelope must be decodable (valid UTF-8 JSON after the
    # prefix) and must carry the exact original bytes in the binary-safe field.
    for stored in real_redis_client.lrange(queue.key.pending, 0, -1):
        envelope = _decode_pending_envelope(stored)
        assert "payload_hex" in envelope
        assert "payload" not in envelope

    delivered = []
    for _ in INVALID_UTF8_PAYLOADS:
        claimed = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert claimed is not None
        delivered.append(decode_stored_message(claimed.stored_message))
        assert gateway.remove_message(queue.key.processing, claimed.stored_message, lease_token=claimed.lease_token)
    assert sorted(delivered) == sorted(INVALID_UTF8_PAYLOADS)


@pytest.mark.integration
def test_sync_redrive_keeps_utf8_payloads_in_plain_text_envelopes(real_redis_client, queue_name):
    gateway = _sync_vt_gateway(real_redis_client, queue_name)
    queue = RedisMessageQueue(queue_name, gateway=gateway)
    real_redis_client.lpush(f"{queue_name}::dlq", "tricky ☃ payload")

    assert queue.redrive_dead_letters() == 1

    stored = real_redis_client.lrange(queue.key.pending, 0, -1)[0]
    envelope = _decode_pending_envelope(stored)
    assert envelope["payload"] == "tricky ☃ payload"
    assert "payload_hex" not in envelope


@pytest.mark.integration
def test_sync_redriven_binary_message_dead_letters_back_to_original_bytes(real_redis_client, queue_name):
    raw = b"\xff\xfe\x00binary-blob\x80"
    gateway = _sync_vt_gateway(real_redis_client, queue_name, max_delivery_count=1)
    queue = RedisMessageQueue(queue_name, gateway=gateway)
    real_redis_client.lpush(f"{queue_name}::dlq", raw)
    assert queue.redrive_dead_letters() == 1

    claimed = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
    assert claimed is not None
    assert decode_stored_message(claimed.stored_message) == raw

    # Crash-cycle the redriven message back into the DLQ via lease expiry: the
    # dead-letter branch must strip the binary-safe envelope back to raw bytes.
    real_redis_client.zadd(gateway._lease_deadlines_key(queue.key.processing), {claimed.stored_message: 0})
    assert gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing) is None
    assert real_redis_client.lrange(f"{queue_name}::dlq", 0, -1) == [raw]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_async_redrive_round_trips_non_utf8_dlq_payloads_exactly(real_async_redis_client, queue_name):
    gateway = _async_vt_gateway(real_async_redis_client, queue_name)
    queue = AsyncRedisMessageQueue(queue_name, gateway=gateway)
    for raw in INVALID_UTF8_PAYLOADS:
        await real_async_redis_client.lpush(f"{queue_name}::dlq", raw)

    assert await queue.redrive_dead_letters() == len(INVALID_UTF8_PAYLOADS)

    for stored in await real_async_redis_client.lrange(queue.key.pending, 0, -1):
        envelope = _decode_pending_envelope(stored)
        assert "payload_hex" in envelope
        assert "payload" not in envelope

    delivered = []
    for _ in INVALID_UTF8_PAYLOADS:
        claimed = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert claimed is not None
        delivered.append(decode_stored_message(claimed.stored_message))
        assert await gateway.remove_message(
            queue.key.processing, claimed.stored_message, lease_token=claimed.lease_token
        )
    assert sorted(delivered) == sorted(INVALID_UTF8_PAYLOADS)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_async_redriven_binary_message_dead_letters_back_to_original_bytes(real_async_redis_client, queue_name):
    raw = b"\xff\xfe\x00binary-blob\x80"
    gateway = _async_vt_gateway(real_async_redis_client, queue_name, max_delivery_count=1)
    queue = AsyncRedisMessageQueue(queue_name, gateway=gateway)
    await real_async_redis_client.lpush(f"{queue_name}::dlq", raw)
    assert await queue.redrive_dead_letters() == 1

    claimed = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
    assert claimed is not None
    assert decode_stored_message(claimed.stored_message) == raw

    await real_async_redis_client.zadd(gateway._lease_deadlines_key(queue.key.processing), {claimed.stored_message: 0})
    assert await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing) is None
    assert await real_async_redis_client.lrange(f"{queue_name}::dlq", 0, -1) == [raw]
