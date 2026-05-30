"""Tests for the drain()/aclose() graceful-shutdown API (B5 / AA-05-F1/F2).

Covers the four scenarios called out in the round-5 fix bundle:
1. drain after publish-only (no in-flight, no pending claim ids) — returns True.
2. drain mid-processing with VT enabled — returns True and refuses new claims
   without disturbing the in-flight handler.
3. drain with pre-populated _pending_claim_ids — recovers them via the
   existing gateway recovery path and returns no-VT messages to pending.
4. drain with timeout=0 — returns False when pending claim ids exist.
"""

import asyncio
import threading
import time

import fakeredis
import pytest
import redis

from redis_message_queue import (
    ConfigurationError,
    DrainFailedError,
    EventOperation,
    EventOutcome,
    QueueDrainedError,
    QueueEvent,
    RedisMessageQueue,
)
from redis_message_queue._redis_gateway import RedisGateway
from redis_message_queue._redis_gateway import _DrainDeadlineExceeded as SyncDrainDeadlineExceeded
from redis_message_queue._stored_message import encode_stored_message
from redis_message_queue.asyncio import RedisMessageQueue as AsyncRedisMessageQueue
from redis_message_queue.asyncio._redis_gateway import (
    RedisGateway as AsyncRedisGateway,
)
from redis_message_queue.asyncio._redis_gateway import (
    _DrainDeadlineExceeded as AsyncDrainDeadlineExceeded,
)


def _seed_committed_no_vt_claim(
    *,
    client: fakeredis.FakeRedis,
    gateway: RedisGateway,
    from_queue: str,
    to_queue: str,
    claim_id: str,
) -> None:
    stored_message = client.rpop(from_queue)
    assert stored_message is not None
    client.lpush(to_queue, stored_message)
    client.set(
        gateway._claim_result_key(to_queue, claim_id),
        stored_message,
        px=int(gateway._claim_result_ttl_ms()),
    )
    client.hset(gateway._claim_result_ids_key(to_queue), claim_id, stored_message)
    client.hset(gateway._claim_result_backrefs_key(to_queue), stored_message, claim_id)


async def _async_seed_committed_no_vt_claim(
    *,
    client: fakeredis.FakeAsyncRedis,
    gateway: AsyncRedisGateway,
    from_queue: str,
    to_queue: str,
    claim_id: str,
) -> None:
    stored_message = await client.rpop(from_queue)
    assert stored_message is not None
    await client.lpush(to_queue, stored_message)
    await client.set(
        gateway._claim_result_key(to_queue, claim_id),
        stored_message,
        px=int(gateway._claim_result_ttl_ms()),
    )
    await client.hset(gateway._claim_result_ids_key(to_queue), claim_id, stored_message)
    await client.hset(gateway._claim_result_backrefs_key(to_queue), stored_message, claim_id)


def test_sync_drain_after_publish_only_returns_true():
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue("drain-clean", client=client, deduplication=False)
    queue.publish("hello")

    assert queue.drain() is True
    assert queue._draining is True
    assert queue._drained.is_set() is True


def test_sync_drain_emits_start_and_success_events_from_idle_queue():
    events: list[QueueEvent] = []
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue("drain-events-clean", client=client, deduplication=False, on_event=events.append)

    assert queue.drain(timeout=1) is True

    assert [(event.operation, event.outcome) for event in events] == [
        (EventOperation.DRAIN, EventOutcome.START),
        (EventOperation.DRAIN, EventOutcome.SUCCESS),
    ]
    assert [event.timeout_seconds for event in events] == [1.0, 1.0]
    assert [event.pending_claim_ids for event in events] == [0, 0]
    assert all(event.duration_ms is not None for event in events)
    assert all(event.error is None and event.exception_type is None for event in events)


def test_sync_second_drain_emits_skipped_event():
    events: list[QueueEvent] = []
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue("drain-events-skipped", client=client, deduplication=False, on_event=events.append)

    assert queue.drain() is True
    events.clear()

    assert queue.drain(timeout=2) is True

    assert [(event.operation, event.outcome) for event in events] == [(EventOperation.DRAIN, EventOutcome.SKIPPED)]
    skipped = events[0]
    assert skipped.timeout_seconds == 2.0
    assert skipped.pending_claim_ids == 0
    assert skipped.duration_ms is not None
    assert skipped.error is None
    assert skipped.exception_type is None


def test_sync_drain_pending_claim_recovery_failure_emits_failure_event():
    events: list[QueueEvent] = []
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue(
        "drain-events-failure",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
        on_event=events.append,
    )
    gateway: RedisGateway = queue._redis  # type: ignore[assignment]
    processing_key = queue.key.processing
    gateway._set_pending_claim_id(processing_key, "stuck-claim-id")
    recovery_error = redis.exceptions.ConnectionError("recovery unavailable")

    def fail_recovery(
        received_processing_key: str,
        claim_id: str,
        *,
        deadline_monotonic: float | None = None,
    ) -> bytes | str | None:
        assert received_processing_key == processing_key
        assert claim_id == "stuck-claim-id"
        assert deadline_monotonic is not None
        raise recovery_error

    gateway._recover_pending_non_visibility_timeout_claim = fail_recovery  # type: ignore[method-assign]

    assert queue.drain(timeout=1) is False

    assert [(event.operation, event.outcome) for event in events] == [
        (EventOperation.DRAIN, EventOutcome.START),
        (EventOperation.DRAIN, EventOutcome.FAILURE),
    ]
    failure = events[-1]
    assert failure.timeout_seconds == 1.0
    assert failure.pending_claim_ids == 1
    assert failure.duration_ms is not None
    assert failure.exception_type == "DrainFailedError"
    assert isinstance(failure.error, DrainFailedError)
    assert failure.error.queue == "drain-events-failure"
    assert failure.error.operation == "drain"
    assert failure.error.__cause__ is recovery_error


def test_sync_close_alias_after_publish_only_returns_true():
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue("close-clean", client=client, deduplication=False)
    queue.publish("hello")

    assert queue.close() is True
    assert queue._draining is True
    assert queue._drained.is_set() is True


def test_sync_publish_after_drain_raises_queue_drained_error():
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue("drain-publish-refuse", client=client, deduplication=False)
    assert queue.publish("before") is True
    assert "drained=False" in repr(queue)

    assert queue.drain() is True

    assert "drained=True" in repr(queue)
    with pytest.raises(QueueDrainedError, match="queue is drained"):
        queue.publish("after")


def test_sync_drain_is_idempotent_and_keeps_refusing_publish():
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue("drain-idempotent", client=client, deduplication=False)

    assert queue.drain() is True
    assert queue.drain() is True
    with pytest.raises(QueueDrainedError):
        queue.publish("after")


def test_sync_drained_state_is_local_to_queue_instance():
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue("drain-local", client=client, deduplication=False)

    assert queue.drain() is True
    with pytest.raises(QueueDrainedError):
        queue.publish("after")

    fresh_queue = RedisMessageQueue("drain-local", client=client, deduplication=False)
    assert fresh_queue.publish("fresh") is True


def test_sync_drain_waits_for_in_flight_publish_path():
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue("drain-publish-in-flight", client=client, deduplication=False)
    gateway: RedisGateway = queue._redis  # type: ignore[assignment]
    original_add_message = gateway.add_message

    entered_publish = threading.Event()
    release_publish = threading.Event()
    publish_result: list[bool] = []
    drain_result: list[bool] = []

    def controlled_add_message(queue_name: str, message: str) -> None:
        entered_publish.set()
        assert release_publish.wait(timeout=5)
        return original_add_message(queue_name, message)

    gateway.add_message = controlled_add_message  # type: ignore[method-assign]

    publish_thread = threading.Thread(target=lambda: publish_result.append(queue.publish("in-flight")))
    publish_thread.start()
    assert entered_publish.wait(timeout=5)

    drain_thread = threading.Thread(target=lambda: drain_result.append(queue.drain(timeout=1)))
    drain_thread.start()
    drain_thread.join(timeout=0.05)
    assert drain_result == []

    release_publish.set()
    publish_thread.join(timeout=5)
    drain_thread.join(timeout=5)

    assert publish_result == [True]
    assert drain_result == [True]
    with pytest.raises(QueueDrainedError):
        queue.publish("after")


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


def test_sync_drain_interrupts_already_blocked_claim_loop():
    client = fakeredis.FakeRedis()
    gateway = RedisGateway(
        redis_client=client,
        message_wait_interval_seconds=1,
        message_visibility_timeout_seconds=None,
        max_delivery_count=None,
    )
    consumer = RedisMessageQueue("drain-blocked-claim", gateway=gateway, deduplication=False)
    producer = RedisMessageQueue(
        "drain-blocked-claim",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
    )
    original_claim = gateway._claim_message_without_visibility_timeout
    first_poll = threading.Event()

    def controlled_claim(from_queue: str, to_queue: str, *, claim_id: str) -> str | bytes | None:
        first_poll.set()
        return original_claim(from_queue, to_queue, claim_id=claim_id)

    gateway._claim_message_without_visibility_timeout = controlled_claim  # type: ignore[method-assign]

    entered = threading.Event()
    observed: list[str | bytes | None] = []

    def worker() -> None:
        entered.set()
        with consumer.process_message() as message:
            observed.append(message)

    thread = threading.Thread(target=worker)
    thread.start()
    assert entered.wait(timeout=5)
    assert first_poll.wait(timeout=5)

    assert consumer.drain(timeout=1) is True
    assert producer.publish("after-drain") is True

    thread.join(timeout=5)

    assert observed == [None]


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


def test_sync_in_flight_handler_publish_during_drain_raises_queue_drained_error():
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue(
        "drain-handler-publish",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=30,
    )
    queue.publish("first")

    started = threading.Event()
    drain_done = threading.Event()
    completed = threading.Event()
    caught: list[type[BaseException]] = []

    def worker():
        with queue.process_message() as message:
            assert message is not None
            started.set()
            assert drain_done.wait(timeout=5)
            try:
                queue.publish("from-handler")
            except QueueDrainedError as exc:
                caught.append(type(exc))
        completed.set()

    thread = threading.Thread(target=worker)
    thread.start()
    try:
        assert started.wait(timeout=5)
        assert queue.drain(timeout=1) is True
        drain_done.set()
    finally:
        drain_done.set()
        thread.join(timeout=5)

    assert caught == [QueueDrainedError]
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
    seeded_claim_id = "drain-test-claim-id"
    claimed = gateway._claim_message_without_visibility_timeout(
        queue.key.pending,
        processing_key,
        claim_id=seeded_claim_id,
    )
    assert claimed is not None
    claim_result_ids_key = gateway._claim_result_ids_key(processing_key)
    claim_result_backrefs_key = gateway._claim_result_backrefs_key(processing_key)
    gateway._set_pending_claim_id(processing_key, seeded_claim_id)

    assert gateway._pending_claim_ids[processing_key] == [seeded_claim_id]
    assert client.llen(queue.key.pending) == 0
    assert client.llen(processing_key) == 1

    assert queue.drain(timeout=2) is True
    assert processing_key not in gateway._pending_claim_ids
    assert client.lrange(queue.key.pending, 0, -1) == [claimed]
    assert client.llen(processing_key) == 0
    assert client.hget(claim_result_ids_key, seeded_claim_id) is None
    assert client.hget(claim_result_backrefs_key, claimed) is None

    fresh_queue = RedisMessageQueue(
        "drain-recover",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
    )
    with fresh_queue.process_message() as message:
        assert message == b"payload"


def test_sync_drain_waits_for_racing_ambiguous_claim_registration():
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue(
        "drain-racing-claim",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
    )
    queue.publish("payload")
    gateway: RedisGateway = queue._redis  # type: ignore[assignment]
    processing_key = queue.key.processing

    claim_committed = threading.Event()
    allow_error = threading.Event()
    claim_ids: list[str] = []

    def racing_claim(from_queue: str, to_queue: str, *, claim_id: str) -> str | bytes | None:
        _seed_committed_no_vt_claim(
            client=client,
            gateway=gateway,
            from_queue=from_queue,
            to_queue=to_queue,
            claim_id=claim_id,
        )
        claim_ids.append(claim_id)
        claim_committed.set()
        assert allow_error.wait(timeout=5)
        raise redis.exceptions.ConnectionError("ambiguous claim after commit")

    gateway._claim_message_without_visibility_timeout = racing_claim  # type: ignore[method-assign]
    observed: list[str | bytes | None] = []

    def worker() -> None:
        with queue.process_message() as message:
            observed.append(message)

    thread = threading.Thread(target=worker)
    thread.start()
    assert claim_committed.wait(timeout=5)

    assert queue.drain(timeout=0.05) is False
    assert gateway._pending_claim_ids.get(processing_key) is None

    allow_error.set()
    thread.join(timeout=5)
    assert not thread.is_alive()
    assert observed == [None]
    assert gateway._pending_claim_ids.get(processing_key) == claim_ids

    assert queue.drain(timeout=1) is True
    assert processing_key not in gateway._pending_claim_ids
    assert client.llen(processing_key) == 0

    fresh_queue = RedisMessageQueue(
        "drain-racing-claim",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
    )
    with fresh_queue.process_message() as message:
        assert message == b"payload"


def test_sync_drain_preserves_string_only_recovery_token_until_requeue_succeeds():
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue(
        "drain-string-only-recover",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
    )
    gateway: RedisGateway = queue._redis  # type: ignore[assignment]
    processing_key = queue.key.processing
    claim_id = "drain-string-only-claim-id"
    stored_message = encode_stored_message("payload").encode("utf-8")
    claim_result_key = gateway._claim_result_key(processing_key, claim_id)

    client.lpush(processing_key, stored_message)
    client.set(claim_result_key, stored_message, px=120_000)
    gateway._set_pending_claim_id(processing_key, claim_id)

    original_return = gateway._return_recovered_non_visibility_timeout_claim_to_pending

    def timeout_before_requeue(*_args: object, **_kwargs: object) -> bool:
        raise SyncDrainDeadlineExceeded

    gateway._return_recovered_non_visibility_timeout_claim_to_pending = timeout_before_requeue  # type: ignore[method-assign]

    assert queue.drain(timeout=2) is False
    assert client.get(claim_result_key) == stored_message
    assert gateway._pending_claim_ids[processing_key] == [claim_id]
    assert client.llen(queue.key.pending) == 0
    assert client.llen(processing_key) == 1

    gateway._return_recovered_non_visibility_timeout_claim_to_pending = original_return  # type: ignore[method-assign]

    assert queue.drain(timeout=2) is True
    assert client.get(claim_result_key) is None
    assert processing_key not in gateway._pending_claim_ids
    assert client.lrange(queue.key.pending, 0, -1) == [stored_message]
    assert client.llen(processing_key) == 0


def test_sync_drain_keeps_claim_id_pending_when_recovered_message_cannot_be_requeued():
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue(
        "drain-recover-missing-processing",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
    )
    gateway: RedisGateway = queue._redis  # type: ignore[assignment]
    processing_key = queue.key.processing
    seeded_claim_id = "drain-missing-processing-claim-id"
    stored_message = encode_stored_message("payload")
    client.hset(gateway._claim_result_ids_key(processing_key), seeded_claim_id, stored_message)
    client.hset(gateway._claim_result_backrefs_key(processing_key), stored_message, seeded_claim_id)
    gateway._set_pending_claim_id(processing_key, seeded_claim_id)

    assert queue.drain(timeout=2) is False
    assert gateway._pending_claim_ids[processing_key] == [seeded_claim_id]
    assert client.llen(queue.key.pending) == 0
    assert client.llen(processing_key) == 0


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


def test_sync_drain_timeout_bounds_slow_pending_claim_recovery_read():
    class SlowRecoveryClient:
        def __init__(self, latency_seconds: float) -> None:
            self.redis = fakeredis.FakeRedis()
            self.latency_seconds = latency_seconds

        def get(self, key: str) -> bytes | None:
            time.sleep(self.latency_seconds)
            return self.redis.get(key)

        def hget(self, key: str, field: str) -> bytes | None:
            time.sleep(self.latency_seconds)
            return self.redis.hget(key, field)

        def __getattr__(self, name: str) -> object:
            return getattr(self.redis, name)

    timeout_seconds = 0.05
    latency_seconds = 0.35
    client = SlowRecoveryClient(latency_seconds)
    gateway = RedisGateway(
        redis_client=client,  # type: ignore[arg-type]
        retry_budget_seconds=0,
        message_visibility_timeout_seconds=None,
    )
    queue = RedisMessageQueue("drain-slow-recovery-timeout", gateway=gateway)
    processing_key = queue.key.processing
    claim_id = "slow-drain-claim-id"
    client.redis.hset(
        gateway._claim_result_ids_key(processing_key),
        claim_id,
        encode_stored_message("recovered-payload"),
    )
    gateway._set_pending_claim_id(processing_key, claim_id)

    started = time.monotonic()
    result = queue.drain(timeout=timeout_seconds)
    elapsed = time.monotonic() - started

    assert result is False
    assert elapsed < 0.25
    assert gateway._pending_claim_ids[processing_key] == [claim_id]


def test_sync_concurrent_drain_both_return_true():
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue(
        "drain-concurrent",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
    )
    gateway: RedisGateway = queue._redis  # type: ignore[assignment]
    processing_key = queue.key.processing
    seeded_claim_id = "concurrent-drain-claim-id"
    queue.publish("payload")
    claimed = gateway._claim_message_without_visibility_timeout(
        queue.key.pending,
        processing_key,
        claim_id=seeded_claim_id,
    )
    assert claimed is not None
    gateway._set_pending_claim_id(processing_key, seeded_claim_id)

    original_recover = gateway._recover_pending_non_visibility_timeout_claim
    entered_recovery = threading.Event()
    release_recovery = threading.Event()
    results: dict[str, bool] = {}

    def controlled_recover(
        received_processing_key: str,
        claim_id: str,
        *,
        deadline_monotonic: float | None = None,
    ) -> bytes | str | None:
        assert deadline_monotonic is not None
        assert received_processing_key == processing_key
        assert claim_id == seeded_claim_id
        entered_recovery.set()
        assert release_recovery.wait(timeout=5)
        return original_recover(received_processing_key, claim_id)

    gateway._recover_pending_non_visibility_timeout_claim = controlled_recover  # type: ignore[method-assign]

    first = threading.Thread(target=lambda: results.setdefault("first", queue.drain(timeout=2)))
    first.start()
    assert entered_recovery.wait(timeout=5)

    second = threading.Thread(target=lambda: results.setdefault("second", queue.drain(timeout=2)))
    second.start()
    second.join(timeout=0.05)
    second_waited_for_first = second.is_alive()

    release_recovery.set()
    first.join(timeout=5)
    second.join(timeout=5)

    assert second_waited_for_first is True
    assert results == {"first": True, "second": True}
    assert processing_key not in gateway._pending_claim_ids
    assert client.lrange(queue.key.pending, 0, -1) == [claimed]
    assert client.llen(processing_key) == 0


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
    assert queue._drained is True


@pytest.mark.asyncio
async def test_async_drain_alias_emits_start_and_success_events_from_idle_queue():
    events: list[QueueEvent] = []

    async def observe(event: QueueEvent) -> None:
        events.append(event)

    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue("async-drain-events-clean", client=client, deduplication=False, on_event=observe)

    assert await queue.drain(timeout=1) is True

    assert [(event.operation, event.outcome) for event in events] == [
        (EventOperation.DRAIN, EventOutcome.START),
        (EventOperation.DRAIN, EventOutcome.SUCCESS),
    ]
    assert [event.timeout_seconds for event in events] == [1.0, 1.0]
    assert [event.pending_claim_ids for event in events] == [0, 0]
    assert all(event.duration_ms is not None for event in events)
    assert all(event.error is None and event.exception_type is None for event in events)


@pytest.mark.asyncio
async def test_async_second_aclose_emits_skipped_event():
    events: list[QueueEvent] = []

    async def observe(event: QueueEvent) -> None:
        events.append(event)

    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue("async-drain-events-skipped", client=client, deduplication=False, on_event=observe)

    assert await queue.aclose() is True
    events.clear()

    assert await queue.aclose(timeout=2) is True

    assert [(event.operation, event.outcome) for event in events] == [(EventOperation.DRAIN, EventOutcome.SKIPPED)]
    skipped = events[0]
    assert skipped.timeout_seconds == 2.0
    assert skipped.pending_claim_ids == 0
    assert skipped.duration_ms is not None
    assert skipped.error is None
    assert skipped.exception_type is None


@pytest.mark.asyncio
async def test_async_aclose_pending_claim_recovery_failure_emits_failure_event():
    events: list[QueueEvent] = []

    async def observe(event: QueueEvent) -> None:
        events.append(event)

    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue(
        "async-drain-events-failure",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
        on_event=observe,
    )
    gateway: AsyncRedisGateway = queue._redis  # type: ignore[assignment]
    processing_key = queue.key.processing
    gateway._set_pending_claim_id(processing_key, "stuck-claim-id")
    recovery_error = redis.exceptions.ConnectionError("recovery unavailable")

    async def fail_recovery(
        received_processing_key: str,
        claim_id: str,
        *,
        deadline_monotonic: float | None = None,
    ) -> bytes | str | None:
        assert received_processing_key == processing_key
        assert claim_id == "stuck-claim-id"
        assert deadline_monotonic is not None
        raise recovery_error

    gateway._recover_pending_non_visibility_timeout_claim = fail_recovery  # type: ignore[method-assign]

    assert await queue.aclose(timeout=1) is False

    assert [(event.operation, event.outcome) for event in events] == [
        (EventOperation.DRAIN, EventOutcome.START),
        (EventOperation.DRAIN, EventOutcome.FAILURE),
    ]
    failure = events[-1]
    assert failure.timeout_seconds == 1.0
    assert failure.pending_claim_ids == 1
    assert failure.duration_ms is not None
    assert failure.exception_type == "DrainFailedError"
    assert isinstance(failure.error, DrainFailedError)
    assert failure.error.queue == "async-drain-events-failure"
    assert failure.error.operation == "drain"
    assert failure.error.__cause__ is recovery_error


@pytest.mark.asyncio
async def test_async_drain_alias_pending_claim_recovery_failure_wraps_failure_event_error():
    events: list[QueueEvent] = []

    async def observe(event: QueueEvent) -> None:
        events.append(event)

    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue(
        "async-drain-alias-events-failure",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
        on_event=observe,
    )
    gateway: AsyncRedisGateway = queue._redis  # type: ignore[assignment]
    processing_key = queue.key.processing
    gateway._set_pending_claim_id(processing_key, "stuck-alias-claim-id")
    recovery_error = redis.exceptions.ConnectionError("alias recovery unavailable")

    async def fail_recovery(
        received_processing_key: str,
        claim_id: str,
        *,
        deadline_monotonic: float | None = None,
    ) -> bytes | str | None:
        assert received_processing_key == processing_key
        assert claim_id == "stuck-alias-claim-id"
        assert deadline_monotonic is not None
        raise recovery_error

    gateway._recover_pending_non_visibility_timeout_claim = fail_recovery  # type: ignore[method-assign]

    assert await queue.drain(timeout=1) is False

    assert [(event.operation, event.outcome) for event in events] == [
        (EventOperation.DRAIN, EventOutcome.START),
        (EventOperation.DRAIN, EventOutcome.FAILURE),
    ]
    failure = events[-1]
    assert failure.timeout_seconds == 1.0
    assert failure.pending_claim_ids == 1
    assert failure.duration_ms is not None
    assert failure.exception_type == "DrainFailedError"
    assert isinstance(failure.error, DrainFailedError)
    assert failure.error.queue == "async-drain-alias-events-failure"
    assert failure.error.operation == "drain"
    assert failure.error.__cause__ is recovery_error


@pytest.mark.asyncio
async def test_async_drain_alias_after_publish_only_returns_true():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue("async-drain-clean", client=client, deduplication=False)
    await queue.publish("hello")

    assert await queue.drain() is True
    assert queue._draining is True
    assert queue._drained is True


@pytest.mark.asyncio
async def test_async_publish_after_aclose_raises_queue_drained_error():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue("aclose-publish-refuse", client=client, deduplication=False)
    assert await queue.publish("before") is True
    assert "drained=False" in repr(queue)

    assert await queue.aclose() is True

    assert "drained=True" in repr(queue)
    with pytest.raises(QueueDrainedError, match="queue is drained"):
        await queue.publish("after")


@pytest.mark.asyncio
async def test_async_aclose_is_idempotent_and_keeps_refusing_publish():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue("aclose-idempotent", client=client, deduplication=False)

    assert await queue.aclose() is True
    assert await queue.aclose() is True
    with pytest.raises(QueueDrainedError):
        await queue.publish("after")


@pytest.mark.asyncio
async def test_async_drained_state_is_local_to_queue_instance():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue("aclose-local", client=client, deduplication=False)

    assert await queue.aclose() is True
    with pytest.raises(QueueDrainedError):
        await queue.publish("after")

    fresh_queue = AsyncRedisMessageQueue("aclose-local", client=client, deduplication=False)
    assert await fresh_queue.publish("fresh") is True


@pytest.mark.asyncio
async def test_async_aclose_waits_for_in_flight_publish_path():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue("aclose-publish-in-flight", client=client, deduplication=False)
    gateway: AsyncRedisGateway = queue._redis  # type: ignore[assignment]
    original_add_message = gateway.add_message

    entered_publish = asyncio.Event()
    release_publish = asyncio.Event()

    async def controlled_add_message(queue_name: str, message: str) -> None:
        entered_publish.set()
        await release_publish.wait()
        return await original_add_message(queue_name, message)

    gateway.add_message = controlled_add_message  # type: ignore[method-assign]

    publish_task = asyncio.create_task(queue.publish("in-flight"))
    await asyncio.wait_for(entered_publish.wait(), timeout=1)

    drain_task = asyncio.create_task(queue.aclose(timeout=1))
    await asyncio.sleep(0.05)
    assert drain_task.done() is False

    release_publish.set()
    assert await publish_task is True
    assert await drain_task is True
    with pytest.raises(QueueDrainedError):
        await queue.publish("after")


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
async def test_async_drain_interrupts_already_blocked_claim_loop():
    client = fakeredis.FakeAsyncRedis()
    gateway = AsyncRedisGateway(
        redis_client=client,
        message_wait_interval_seconds=1,
        message_visibility_timeout_seconds=None,
        max_delivery_count=None,
    )
    consumer = AsyncRedisMessageQueue("async-drain-blocked-claim", gateway=gateway, deduplication=False)
    producer = AsyncRedisMessageQueue(
        "async-drain-blocked-claim",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
    )
    original_claim = gateway._claim_message_without_visibility_timeout
    first_poll = asyncio.Event()

    async def controlled_claim(from_queue: str, to_queue: str, *, claim_id: str) -> str | bytes | None:
        first_poll.set()
        return await original_claim(from_queue, to_queue, claim_id=claim_id)

    gateway._claim_message_without_visibility_timeout = controlled_claim  # type: ignore[method-assign]

    entered = asyncio.Event()
    observed: list[str | bytes | None] = []

    async def worker() -> None:
        entered.set()
        async with consumer.process_message() as message:
            observed.append(message)

    task = asyncio.create_task(worker())
    await asyncio.wait_for(entered.wait(), timeout=1)
    await asyncio.wait_for(first_poll.wait(), timeout=1)

    assert await consumer.drain(timeout=1) is True
    assert await producer.publish("after-drain") is True

    await asyncio.wait_for(task, timeout=5)

    assert observed == [None]


@pytest.mark.asyncio
async def test_async_in_flight_handler_publish_during_aclose_raises_queue_drained_error():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue(
        "aclose-handler-publish",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=30,
    )
    await queue.publish("first")

    started = asyncio.Event()
    drain_done = asyncio.Event()
    caught: list[type[BaseException]] = []

    async def worker() -> None:
        async with queue.process_message() as message:
            assert message is not None
            started.set()
            await asyncio.wait_for(drain_done.wait(), timeout=1)
            try:
                await queue.publish("from-handler")
            except QueueDrainedError as exc:
                caught.append(type(exc))

    task = asyncio.create_task(worker())
    await asyncio.wait_for(started.wait(), timeout=1)
    assert await queue.aclose(timeout=1) is True
    drain_done.set()
    await asyncio.wait_for(task, timeout=1)

    assert caught == [QueueDrainedError]


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
    claimed = await gateway._claim_message_without_visibility_timeout(
        queue.key.pending,
        processing_key,
        claim_id=seeded_claim_id,
    )
    assert claimed is not None
    claim_result_ids_key = gateway._claim_result_ids_key(processing_key)
    claim_result_backrefs_key = gateway._claim_result_backrefs_key(processing_key)
    gateway._set_pending_claim_id(processing_key, seeded_claim_id)

    assert gateway._pending_claim_ids[processing_key] == [seeded_claim_id]
    assert await client.llen(queue.key.pending) == 0
    assert await client.llen(processing_key) == 1

    assert await queue.aclose(timeout=2) is True
    assert processing_key not in gateway._pending_claim_ids
    assert await client.lrange(queue.key.pending, 0, -1) == [claimed]
    assert await client.llen(processing_key) == 0
    assert await client.hget(claim_result_ids_key, seeded_claim_id) is None
    assert await client.hget(claim_result_backrefs_key, claimed) is None

    fresh_queue = AsyncRedisMessageQueue(
        "aclose-recover",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
    )
    async with fresh_queue.process_message() as message:
        assert message == b"payload"


@pytest.mark.asyncio
async def test_async_aclose_waits_for_racing_ambiguous_claim_registration():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue(
        "aclose-racing-claim",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
    )
    await queue.publish("payload")
    gateway: AsyncRedisGateway = queue._redis  # type: ignore[assignment]
    processing_key = queue.key.processing

    claim_committed = asyncio.Event()
    allow_error = asyncio.Event()
    claim_ids: list[str] = []

    async def racing_claim(from_queue: str, to_queue: str, *, claim_id: str) -> str | bytes | None:
        await _async_seed_committed_no_vt_claim(
            client=client,
            gateway=gateway,
            from_queue=from_queue,
            to_queue=to_queue,
            claim_id=claim_id,
        )
        claim_ids.append(claim_id)
        claim_committed.set()
        await asyncio.wait_for(allow_error.wait(), timeout=5)
        raise redis.exceptions.ConnectionError("ambiguous async claim after commit")

    gateway._claim_message_without_visibility_timeout = racing_claim  # type: ignore[method-assign]
    observed: list[str | bytes | None] = []

    async def worker() -> None:
        async with queue.process_message() as message:
            observed.append(message)

    task = asyncio.create_task(worker())
    await asyncio.wait_for(claim_committed.wait(), timeout=5)

    assert await queue.aclose(timeout=0.05) is False
    assert gateway._pending_claim_ids.get(processing_key) is None

    allow_error.set()
    await asyncio.wait_for(task, timeout=5)
    assert observed == [None]
    assert gateway._pending_claim_ids.get(processing_key) == claim_ids

    assert await queue.aclose(timeout=1) is True
    assert processing_key not in gateway._pending_claim_ids
    assert await client.llen(processing_key) == 0

    fresh_queue = AsyncRedisMessageQueue(
        "aclose-racing-claim",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
    )
    async with fresh_queue.process_message() as message:
        assert message == b"payload"


@pytest.mark.asyncio
async def test_async_aclose_preserves_string_only_recovery_token_until_requeue_succeeds():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue(
        "aclose-string-only-recover",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
    )
    gateway: AsyncRedisGateway = queue._redis  # type: ignore[assignment]
    processing_key = queue.key.processing
    claim_id = "aclose-string-only-claim-id"
    stored_message = encode_stored_message("payload").encode("utf-8")
    claim_result_key = gateway._claim_result_key(processing_key, claim_id)

    await client.lpush(processing_key, stored_message)
    await client.set(claim_result_key, stored_message, px=120_000)
    gateway._set_pending_claim_id(processing_key, claim_id)

    original_return = gateway._return_recovered_non_visibility_timeout_claim_to_pending

    async def timeout_before_requeue(*_args: object, **_kwargs: object) -> bool:
        raise AsyncDrainDeadlineExceeded

    gateway._return_recovered_non_visibility_timeout_claim_to_pending = timeout_before_requeue  # type: ignore[method-assign]

    assert await queue.aclose(timeout=2) is False
    assert await client.get(claim_result_key) == stored_message
    assert gateway._pending_claim_ids[processing_key] == [claim_id]
    assert await client.llen(queue.key.pending) == 0
    assert await client.llen(processing_key) == 1

    gateway._return_recovered_non_visibility_timeout_claim_to_pending = original_return  # type: ignore[method-assign]

    assert await queue.aclose(timeout=2) is True
    assert await client.get(claim_result_key) is None
    assert processing_key not in gateway._pending_claim_ids
    assert await client.lrange(queue.key.pending, 0, -1) == [stored_message]
    assert await client.llen(processing_key) == 0


@pytest.mark.asyncio
async def test_async_aclose_keeps_claim_id_pending_when_recovered_message_cannot_be_requeued():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue(
        "aclose-recover-missing-processing",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
    )
    gateway: AsyncRedisGateway = queue._redis  # type: ignore[assignment]
    processing_key = queue.key.processing
    seeded_claim_id = "aclose-missing-processing-claim-id"
    stored_message = encode_stored_message("payload")
    await client.hset(gateway._claim_result_ids_key(processing_key), seeded_claim_id, stored_message)
    await client.hset(gateway._claim_result_backrefs_key(processing_key), stored_message, seeded_claim_id)
    gateway._set_pending_claim_id(processing_key, seeded_claim_id)

    assert await queue.aclose(timeout=2) is False
    assert gateway._pending_claim_ids[processing_key] == [seeded_claim_id]
    assert await client.llen(queue.key.pending) == 0
    assert await client.llen(processing_key) == 0


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
async def test_async_aclose_timeout_bounds_slow_pending_claim_recovery_read():
    class SlowAsyncRecoveryClient:
        def __init__(self, latency_seconds: float) -> None:
            self.redis = fakeredis.FakeAsyncRedis()
            self.latency_seconds = latency_seconds

        async def get(self, key: str) -> bytes | None:
            await asyncio.sleep(self.latency_seconds)
            return await self.redis.get(key)

        async def hget(self, key: str, field: str) -> bytes | None:
            await asyncio.sleep(self.latency_seconds)
            return await self.redis.hget(key, field)

        def __getattr__(self, name: str) -> object:
            return getattr(self.redis, name)

    timeout_seconds = 0.05
    latency_seconds = 0.35
    client = SlowAsyncRecoveryClient(latency_seconds)
    gateway = AsyncRedisGateway(
        redis_client=client,  # type: ignore[arg-type]
        retry_budget_seconds=0,
        message_visibility_timeout_seconds=None,
    )
    queue = AsyncRedisMessageQueue("aclose-slow-recovery-timeout", gateway=gateway)
    processing_key = queue.key.processing
    claim_id = "slow-async-drain-claim-id"
    await client.redis.hset(
        gateway._claim_result_ids_key(processing_key),
        claim_id,
        encode_stored_message("recovered-payload"),
    )
    gateway._set_pending_claim_id(processing_key, claim_id)

    started = time.monotonic()
    result = await queue.aclose(timeout=timeout_seconds)
    elapsed = time.monotonic() - started

    assert result is False
    assert elapsed < 0.25
    assert gateway._pending_claim_ids[processing_key] == [claim_id]


@pytest.mark.asyncio
async def test_async_aclose_after_timeout_can_retry():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue(
        "aclose-timeout-retry",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
    )
    gateway: AsyncRedisGateway = queue._redis  # type: ignore[assignment]
    processing_key = queue.key.processing
    drain_calls = 0

    async def controlled_drainer(
        received_processing_key: str,
        *,
        deadline_monotonic: float | None,
    ) -> bool:
        nonlocal drain_calls
        assert received_processing_key == processing_key
        drain_calls += 1
        if drain_calls == 1:
            assert deadline_monotonic is not None
            return False
        assert deadline_monotonic is not None
        return True

    gateway._drain_pending_claim_ids = controlled_drainer  # type: ignore[method-assign]

    assert await queue.aclose(timeout=0) is False
    assert await queue.aclose(timeout=10) is True
    assert drain_calls == 2
    assert await queue.aclose(timeout=10) is True
    assert drain_calls == 2


@pytest.mark.asyncio
async def test_async_concurrent_aclose_returns_cached_result_and_drains_once():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue(
        "aclose-concurrent",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
    )
    gateway: AsyncRedisGateway = queue._redis  # type: ignore[assignment]
    processing_key = queue.key.processing

    entered_first_drain = asyncio.Event()
    release_first_drain = asyncio.Event()
    drain_calls = 0

    async def controlled_drainer(
        received_processing_key: str,
        *,
        deadline_monotonic: float | None,
    ) -> bool:
        nonlocal drain_calls
        assert received_processing_key == processing_key
        assert deadline_monotonic is not None
        drain_calls += 1
        if drain_calls == 1:
            entered_first_drain.set()
            await release_first_drain.wait()
            return True
        return False

    gateway._drain_pending_claim_ids = controlled_drainer  # type: ignore[method-assign]

    first = asyncio.create_task(queue.aclose(timeout=1))
    await asyncio.wait_for(entered_first_drain.wait(), timeout=1)
    second = asyncio.create_task(queue.aclose(timeout=0))
    await asyncio.sleep(0)

    release_first_drain.set()
    results = await asyncio.gather(first, second)

    assert results == [True, True]
    assert drain_calls == 1


@pytest.mark.asyncio
async def test_async_aclose_preserves_cleanup_on_cancellation():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue(
        "aclose-cancel-cleanup",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=30,
    )
    gateway: AsyncRedisGateway = queue._redis  # type: ignore[assignment]
    processing_key = queue.key.processing
    seeded_claim_id = "cancel-cleanup-claim-id"
    gateway._set_pending_claim_id(processing_key, seeded_claim_id)

    entered_drain = asyncio.Event()
    release_drain = asyncio.Future()

    async def controlled_drainer(
        received_processing_key: str,
        *,
        deadline_monotonic: float | None,
    ) -> bool:
        assert received_processing_key == processing_key
        assert deadline_monotonic is None
        entered_drain.set()
        await release_drain
        gateway._pending_claim_ids.pop(processing_key, None)
        return True

    gateway._drain_pending_claim_ids = controlled_drainer  # type: ignore[method-assign]

    task = asyncio.create_task(queue.aclose())
    await asyncio.wait_for(entered_drain.wait(), timeout=1)

    task.cancel()
    await asyncio.sleep(0)
    release_drain.set_result(None)

    with pytest.raises(asyncio.CancelledError):
        await task

    assert task.cancelled() is True
    assert processing_key not in gateway._pending_claim_ids


@pytest.mark.asyncio
async def test_async_aclose_rejects_negative_timeout():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue("aclose-validate", client=client, deduplication=False)
    with pytest.raises(ConfigurationError):
        await queue.aclose(timeout=-1)
    with pytest.raises(TypeError):
        await queue.aclose(timeout="soon")  # type: ignore[arg-type]


# ---- OH-C3-OBS1: drain/close result never-False invariant (regression guard) ----


def _assert_drain_result_invariant(result: object, *, surface: str, phase: str) -> None:
    """OH-C3-OBS1 guard: cached drain/close result is only ever None or True, never False."""
    # identity (not `in (None, True)`) so the 0==False / 1==True trap can't mask a forbidden value
    assert result is None or result is True, (
        f"{surface} drain-result never-False invariant violated after {phase}: expected None or True, got {result!r}"
    )


def test_sync_drain_result_only_none_or_true_across_lifecycle():
    # Standing guard for the never-False invariant the sync `is True` short-circuit
    # (redis_message_queue.py:1477) silently relies on.
    events: list[QueueEvent] = []
    queue = RedisMessageQueue(
        "obs1-sync-lifecycle",
        client=fakeredis.FakeRedis(),
        deduplication=False,
        on_event=events.append,
    )
    queue.publish("hello")

    # success
    assert queue.drain(timeout=1) is True
    assert queue._drain_result is True
    _assert_drain_result_invariant(queue._drain_result, surface="sync", phase="successful drain")

    # idempotent repeat -> short-circuits via the `is True` guard (SKIPPED, not a re-drain)
    events.clear()
    assert queue.drain(timeout=1) is True
    assert [(e.operation, e.outcome) for e in events] == [(EventOperation.DRAIN, EventOutcome.SKIPPED)]
    assert queue._drain_result is True
    _assert_drain_result_invariant(queue._drain_result, surface="sync", phase="idempotent repeat drain")

    # failed, then retried (a failed drain must NOT short-circuit; result resets to None, never False)
    failure_events: list[QueueEvent] = []
    failing_queue = RedisMessageQueue(
        "obs1-sync-failure",
        client=fakeredis.FakeRedis(),
        deduplication=False,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
        on_event=failure_events.append,
    )
    gateway: RedisGateway = failing_queue._redis  # type: ignore[assignment]
    processing_key = failing_queue.key.processing
    gateway._set_pending_claim_id(processing_key, "stuck-claim-id")

    def fail_recovery(
        received_processing_key: str,
        claim_id: str,
        *,
        deadline_monotonic: float | None = None,
    ) -> bytes | str | None:
        raise redis.exceptions.ConnectionError("recovery unavailable")

    gateway._recover_pending_non_visibility_timeout_claim = fail_recovery  # type: ignore[method-assign]

    assert failing_queue.drain(timeout=1) is False
    assert failing_queue._drain_result is None
    _assert_drain_result_invariant(failing_queue._drain_result, surface="sync", phase="failed drain")

    # retry genuinely re-enters the drainer (START+FAILURE), proving the failure did not short-circuit
    failure_events.clear()
    assert failing_queue.drain(timeout=1) is False
    assert [(e.operation, e.outcome) for e in failure_events] == [
        (EventOperation.DRAIN, EventOutcome.START),
        (EventOperation.DRAIN, EventOutcome.FAILURE),
    ]
    assert failing_queue._drain_result is None
    _assert_drain_result_invariant(failing_queue._drain_result, surface="sync", phase="failed-then-retried drain")


@pytest.mark.asyncio
async def test_async_aclose_result_only_none_or_true_across_lifecycle():
    # Async mirror of the sync lifecycle guard. Pairs with the production fix at
    # asyncio/redis_message_queue.py:1507 (`is not None` -> `is True`) aligning async
    # to sync's "only a prior success short-circuits" contract.
    events: list[QueueEvent] = []

    async def observe(event: QueueEvent) -> None:
        events.append(event)

    queue = AsyncRedisMessageQueue(
        "obs1-async-lifecycle",
        client=fakeredis.FakeAsyncRedis(),
        deduplication=False,
        on_event=observe,
    )
    await queue.publish("hello")

    # success
    assert await queue.aclose(timeout=1) is True
    assert queue._aclose_result is True
    _assert_drain_result_invariant(queue._aclose_result, surface="async", phase="successful drain")

    # idempotent repeat -> short-circuits via the `is True` guard (SKIPPED, not a re-drain)
    events.clear()
    assert await queue.aclose(timeout=1) is True
    assert [(e.operation, e.outcome) for e in events] == [(EventOperation.DRAIN, EventOutcome.SKIPPED)]
    assert queue._aclose_result is True
    _assert_drain_result_invariant(queue._aclose_result, surface="async", phase="idempotent repeat drain")

    # failed, then retried (a failed drain must NOT short-circuit; result resets to None, never False)
    failure_events: list[QueueEvent] = []

    async def observe_failure(event: QueueEvent) -> None:
        failure_events.append(event)

    failing_queue = AsyncRedisMessageQueue(
        "obs1-async-failure",
        client=fakeredis.FakeAsyncRedis(),
        deduplication=False,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
        on_event=observe_failure,
    )
    gateway: AsyncRedisGateway = failing_queue._redis  # type: ignore[assignment]
    processing_key = failing_queue.key.processing
    gateway._set_pending_claim_id(processing_key, "stuck-claim-id")

    async def fail_recovery(
        received_processing_key: str,
        claim_id: str,
        *,
        deadline_monotonic: float | None = None,
    ) -> bytes | str | None:
        raise redis.exceptions.ConnectionError("recovery unavailable")

    gateway._recover_pending_non_visibility_timeout_claim = fail_recovery  # type: ignore[method-assign]

    assert await failing_queue.aclose(timeout=1) is False
    assert failing_queue._aclose_result is None
    _assert_drain_result_invariant(failing_queue._aclose_result, surface="async", phase="failed drain")

    # retry genuinely re-enters the drainer (START+FAILURE), proving the failure did not short-circuit
    failure_events.clear()
    assert await failing_queue.aclose(timeout=1) is False
    assert [(e.operation, e.outcome) for e in failure_events] == [
        (EventOperation.DRAIN, EventOutcome.START),
        (EventOperation.DRAIN, EventOutcome.FAILURE),
    ]
    assert failing_queue._aclose_result is None
    _assert_drain_result_invariant(failing_queue._aclose_result, surface="async", phase="failed-then-retried drain")
