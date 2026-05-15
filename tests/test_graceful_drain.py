"""Tests for the drain()/aclose() graceful-shutdown API (B5 / AA-05-F1/F2).

Covers the four scenarios called out in the round-5 fix bundle:
1. drain after publish-only (no in-flight, no pending claim ids) — returns True.
2. drain mid-processing with VT enabled — returns True and refuses new claims
   without disturbing the in-flight handler.
3. drain with pre-populated _pending_claim_ids — recovers them via the
   existing gateway recovery path.
4. drain with timeout=0 — returns False when pending claim ids exist.
"""

import threading

import fakeredis
import pytest

from redis_message_queue import ConfigurationError, RedisMessageQueue
from redis_message_queue._redis_gateway import RedisGateway
from redis_message_queue.asyncio import RedisMessageQueue as AsyncRedisMessageQueue
from redis_message_queue.asyncio._redis_gateway import RedisGateway as AsyncRedisGateway


def test_sync_drain_after_publish_only_returns_true():
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue("drain-clean", client=client, deduplication=False)
    queue.publish("hello")

    assert queue.drain() is True
    assert queue._draining is True


def test_sync_close_alias_after_publish_only_returns_true():
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue("close-clean", client=client, deduplication=False)
    queue.publish("hello")

    assert queue.close() is True
    assert queue._draining is True


def test_sync_drain_refuses_new_claims_after_call():
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue(
        "drain-refuse",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
    )
    queue.publish("payload")
    queue.drain()

    with queue.process_message() as message:
        assert message is None


def test_sync_drain_mid_processing_completes_and_refuses_followups():
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue(
        "drain-mid",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=30,
    )
    queue.publish("first")

    started = threading.Event()
    allow_finish = threading.Event()
    completed = threading.Event()

    def worker():
        with queue.process_message() as message:
            assert message is not None
            started.set()
            allow_finish.wait(timeout=5)
        completed.set()

    thread = threading.Thread(target=worker)
    thread.start()
    try:
        assert started.wait(timeout=5)
        # Drain while a message is still in flight; no pending claim ids exist
        # because the claim path succeeded, so drain returns True quickly.
        assert queue.drain(timeout=1) is True
        # Follow-up claims yield None even though the worker thread is still
        # inside the prior process_message context.
        with queue.process_message() as second:
            assert second is None
    finally:
        allow_finish.set()
        thread.join(timeout=5)

    assert completed.is_set()


def test_sync_drain_recovers_pre_populated_pending_claim_id():
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue(
        "drain-recover",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
    )
    queue.publish("payload")

    gateway: RedisGateway = queue._redis  # type: ignore[assignment]
    processing_key = queue.key.processing
    # Seed the same cache layout the gateway writes when an ambiguous claim
    # commits server-side but the Python client loses the response: the
    # claim-result-ids hash is the durable backstop the recovery path reads.
    seeded_claim_id = "drain-test-claim-id"
    claim_result_ids_key = gateway._claim_result_ids_key(processing_key)
    client.hset(claim_result_ids_key, seeded_claim_id, b"recovered-payload")
    gateway._set_pending_claim_id(processing_key, seeded_claim_id)

    assert gateway._pending_claim_ids[processing_key] == [seeded_claim_id]

    assert queue.drain(timeout=2) is True
    assert processing_key not in gateway._pending_claim_ids


def test_sync_drain_with_timeout_zero_returns_false_when_pending_remain():
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue(
        "drain-timeout-zero",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
    )
    gateway: RedisGateway = queue._redis  # type: ignore[assignment]
    processing_key = queue.key.processing
    gateway._set_pending_claim_id(processing_key, "stuck-claim-id")

    # timeout=0 takes the gateway's no-recovery fast path (``>=`` deadline
    # check), so the claim id stays put and the call reports False without
    # touching Redis.
    assert queue.drain(timeout=0) is False
    assert gateway._pending_claim_ids[processing_key] == ["stuck-claim-id"]


def test_sync_drain_rejects_negative_timeout():
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue("drain-validate", client=client, deduplication=False)
    with pytest.raises(ConfigurationError):
        queue.drain(timeout=-1)
    with pytest.raises(TypeError):
        queue.drain(timeout="soon")  # type: ignore[arg-type]


# ---- async parity ----


@pytest.mark.asyncio
async def test_async_aclose_after_publish_only_returns_true():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue("aclose-clean", client=client, deduplication=False)
    await queue.publish("hello")

    assert await queue.aclose() is True
    assert queue._draining is True


@pytest.mark.asyncio
async def test_async_aclose_refuses_new_claims_after_call():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue(
        "aclose-refuse",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
    )
    await queue.publish("payload")
    await queue.aclose()

    async with queue.process_message() as message:
        assert message is None


@pytest.mark.asyncio
async def test_async_aclose_recovers_pre_populated_pending_claim_id():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue(
        "aclose-recover",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
    )
    await queue.publish("payload")

    gateway: AsyncRedisGateway = queue._redis  # type: ignore[assignment]
    processing_key = queue.key.processing
    seeded_claim_id = "aclose-test-claim-id"
    claim_result_ids_key = gateway._claim_result_ids_key(processing_key)
    await client.hset(claim_result_ids_key, seeded_claim_id, b"recovered-payload")
    gateway._set_pending_claim_id(processing_key, seeded_claim_id)

    assert gateway._pending_claim_ids[processing_key] == [seeded_claim_id]

    assert await queue.aclose(timeout=2) is True
    assert processing_key not in gateway._pending_claim_ids


@pytest.mark.asyncio
async def test_async_aclose_with_timeout_zero_returns_false_when_pending_remain():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue(
        "aclose-timeout-zero",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
    )
    gateway: AsyncRedisGateway = queue._redis  # type: ignore[assignment]
    processing_key = queue.key.processing
    gateway._set_pending_claim_id(processing_key, "stuck-claim-id")

    # timeout=0 takes the gateway's no-recovery fast path (``>=`` deadline
    # check) and the seeded claim id remains untouched.
    result = await queue.aclose(timeout=0)
    assert result is False
    assert gateway._pending_claim_ids[processing_key] == ["stuck-claim-id"]


@pytest.mark.asyncio
async def test_async_aclose_rejects_negative_timeout():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue("aclose-validate", client=client, deduplication=False)
    with pytest.raises(ConfigurationError):
        await queue.aclose(timeout=-1)
    with pytest.raises(TypeError):
        await queue.aclose(timeout="soon")  # type: ignore[arg-type]
