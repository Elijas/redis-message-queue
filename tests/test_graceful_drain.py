"""Tests for the drain() graceful-shutdown API (B5 / AA-05-F1/F2).

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
    QueueBackpressureError,
    QueueDrainedError,
    QueueEvent,
    RedisMessageQueue,
)
from redis_message_queue._abstract_redis_gateway import (
    AbstractRedisGateway as SyncAbstractRedisGateway,
)
from redis_message_queue._redis_gateway import RedisGateway
from redis_message_queue._redis_gateway import _DrainDeadlineExceeded as SyncDrainDeadlineExceeded
from redis_message_queue._stored_message import encode_stored_message
from redis_message_queue.asyncio import RedisMessageQueue as AsyncRedisMessageQueue
from redis_message_queue.asyncio._abstract_redis_gateway import (
    AbstractRedisGateway as AsyncAbstractRedisGateway,
)
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


def test_sync_drain_error_attribution_survives_sibling_queue_drain():
    """Two queues sharing one gateway (permitted whenever ``max_delivery_count``
    is unset) must each report their own drain failure. Before the fix, the
    gateway reset/wrote a single gateway-wide ``_last_drain_error`` slot under
    its drain lock, but the queue read that slot *after* releasing the lock,
    unlocked -- a sibling queue's drain running in that window could reset or
    overwrite it, so queue A's ``DrainFailedError`` ended up wrapping queue B's
    error (or nothing at all) instead of its own.

    This test reproduces that window directly: right after the gateway's own
    ``_drain_pending_claim_ids`` call for queue_a returns (and releases its
    drain lock) but before queue_a's ``drain()`` uses that result, queue_b
    (sharing the same gateway) runs a full, successful drain of its own.
    Under the old single-slot design this reset the shared slot to ``None``
    right before queue_a read it, losing queue_a's real ``recovery_error``.
    """
    client = fakeredis.FakeRedis()
    gateway = RedisGateway(
        redis_client=client,
        message_visibility_timeout_seconds=None,
        max_delivery_count=None,
    )
    queue_a = RedisMessageQueue(
        "drain-attrib-a",
        gateway=gateway,
        deduplication=False,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
    )
    queue_b = RedisMessageQueue(
        "drain-attrib-b",
        gateway=gateway,
        deduplication=False,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
    )
    processing_a = queue_a.key.processing
    gateway._set_pending_claim_id(processing_a, "stuck-claim-a")
    recovery_error = redis.exceptions.ConnectionError("queue-a-recovery-unavailable")

    def fail_recovery(
        received_processing_key: str,
        claim_id: str,
        *,
        deadline_monotonic: float | None = None,
    ) -> bytes | str | None:
        assert received_processing_key == processing_a
        assert claim_id == "stuck-claim-a"
        raise recovery_error

    gateway._recover_pending_non_visibility_timeout_claim = fail_recovery  # type: ignore[method-assign]

    original_drain_pending_claim_ids = gateway._drain_pending_claim_ids

    def racing_drain_pending_claim_ids(processing_queue, *, deadline_monotonic):
        result = original_drain_pending_claim_ids(processing_queue, deadline_monotonic=deadline_monotonic)
        if processing_queue == processing_a:
            # Simulate a sibling queue sharing this gateway completing its own
            # drain in the window between the gateway releasing its drain lock
            # for queue_a and queue_a's drain() consuming that result.
            assert queue_b.drain(timeout=10) is True
        return result

    gateway._drain_pending_claim_ids = racing_drain_pending_claim_ids  # type: ignore[method-assign]

    events: list[QueueEvent] = []
    queue_a._on_event = events.append

    assert queue_a.drain(timeout=1) is False

    failure = next(event for event in events if event.outcome is EventOutcome.FAILURE)
    assert isinstance(failure.error, DrainFailedError)
    assert failure.error.queue == "drain-attrib-a"
    assert failure.error.__cause__ is recovery_error


@pytest.mark.asyncio
async def test_async_drain_error_attribution_survives_sibling_queue_drain():
    """Async sibling of the sync drain-error attribution test above."""
    client = fakeredis.FakeAsyncRedis()
    gateway = AsyncRedisGateway(
        redis_client=client,
        message_visibility_timeout_seconds=None,
        max_delivery_count=None,
    )
    queue_a = AsyncRedisMessageQueue(
        "drain-attrib-a",
        gateway=gateway,
        deduplication=False,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
    )
    queue_b = AsyncRedisMessageQueue(
        "drain-attrib-b",
        gateway=gateway,
        deduplication=False,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
    )
    processing_a = queue_a.key.processing
    gateway._set_pending_claim_id(processing_a, "stuck-claim-a")
    recovery_error = redis.exceptions.ConnectionError("queue-a-recovery-unavailable")

    async def fail_recovery(
        received_processing_key: str,
        claim_id: str,
        *,
        deadline_monotonic: float | None = None,
    ) -> bytes | str | None:
        assert received_processing_key == processing_a
        assert claim_id == "stuck-claim-a"
        raise recovery_error

    gateway._recover_pending_non_visibility_timeout_claim = fail_recovery  # type: ignore[method-assign]

    original_drain_pending_claim_ids = gateway._drain_pending_claim_ids

    async def racing_drain_pending_claim_ids(processing_queue, *, deadline_monotonic):
        result = await original_drain_pending_claim_ids(processing_queue, deadline_monotonic=deadline_monotonic)
        if processing_queue == processing_a:
            # Simulate a sibling queue sharing this gateway completing its own
            # drain in the window between the gateway releasing its drain lock
            # for queue_a and queue_a's drain() consuming that result.
            assert await queue_b.drain(timeout=10) is True
        return result

    gateway._drain_pending_claim_ids = racing_drain_pending_claim_ids  # type: ignore[method-assign]

    events: list[QueueEvent] = []

    async def on_event(event: QueueEvent) -> None:
        events.append(event)

    queue_a._on_event = on_event

    assert await queue_a.drain(timeout=1) is False

    failure = next(event for event in events if event.outcome is EventOutcome.FAILURE)
    assert isinstance(failure.error, DrainFailedError)
    assert failure.error.queue == "drain-attrib-a"
    assert failure.error.__cause__ is recovery_error


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
    original_add_message = gateway._add_message_interruptible

    entered_publish = threading.Event()
    release_publish = threading.Event()
    publish_result: list[bool] = []
    drain_result: list[bool] = []

    def controlled_add_message(queue_name: str, message: str, **kwargs) -> None:
        entered_publish.set()
        assert release_publish.wait(timeout=5)
        return original_add_message(queue_name, message, **kwargs)

    gateway._add_message_interruptible = controlled_add_message  # type: ignore[method-assign]

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


def test_sync_drain_clears_in_flight_claim_id_after_registration_signal():
    client = fakeredis.FakeRedis()
    gateway = RedisGateway(redis_client=client, message_wait_interval_seconds=0)
    queue = RedisMessageQueue("drain-in-flight-signal", gateway=gateway)
    real_begin = gateway._begin_in_flight_claim_id

    def raising_begin(processing_queue: str, claim_id: str) -> None:
        real_begin(processing_queue, claim_id)
        raise KeyboardInterrupt("signal after in-flight claim registration")

    gateway._begin_in_flight_claim_id = raising_begin  # type: ignore[method-assign]

    with pytest.raises(KeyboardInterrupt, match="in-flight claim registration"):
        with queue.process_message():
            pass

    assert gateway._in_flight_claim_ids == {}
    assert queue.drain(timeout=None) is True


def test_sync_drain_waits_for_concurrent_pending_claim_recovery():
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue(
        "drain-concurrent-recovery-wait",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
    )
    gateway: RedisGateway = queue._redis  # type: ignore[assignment]
    processing_key = queue.key.processing
    claim_id = "concurrent-recovery-claim-id"
    # A concurrent consumer's _wait_for_claim preamble has acquired the pending
    # claim id (registered + marked recovering) and is still mid-recovery.
    gateway._set_pending_claim_id(processing_key, claim_id)
    with gateway._pending_claim_ids_lock:
        gateway._recovering_claim_ids.setdefault(processing_key, set()).add(claim_id)

    results: list[bool] = []
    drainer = threading.Thread(target=lambda: results.append(queue.drain(timeout=None)))
    drainer.start()
    # timeout=None waits indefinitely: the drain must poll for the concurrent
    # recovery to resolve instead of giving up on the recovering id.
    drainer.join(timeout=0.3)
    assert drainer.is_alive(), "drain(timeout=None) returned while a concurrent recovery was still running"

    # The concurrent consumer's recovery completes and clears the claim id.
    gateway._finish_pending_claim_recovery(processing_key, claim_id, clear=True)
    drainer.join(timeout=5)
    assert not drainer.is_alive()
    assert results == [True]
    assert processing_key not in gateway._pending_claim_ids


def test_sync_drain_deadline_bounds_wait_for_concurrent_pending_claim_recovery():
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue(
        "drain-concurrent-recovery-deadline",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
    )
    gateway: RedisGateway = queue._redis  # type: ignore[assignment]
    processing_key = queue.key.processing
    claim_id = "concurrent-recovery-deadline-claim-id"
    gateway._set_pending_claim_id(processing_key, claim_id)
    with gateway._pending_claim_ids_lock:
        gateway._recovering_claim_ids.setdefault(processing_key, set()).add(claim_id)

    timeout_seconds = 0.3
    started = time.monotonic()
    result = queue.drain(timeout=timeout_seconds)
    elapsed = time.monotonic() - started

    # The recovering id never resolves, so drain fails — but only after
    # honoring the deadline instead of returning False instantly.
    assert result is False
    assert elapsed >= timeout_seconds
    assert gateway._pending_claim_ids[processing_key] == [claim_id]


def test_sync_consumer_signal_in_pending_claim_acquisition_window_releases_recovering_entry():
    client = fakeredis.FakeRedis()
    gateway = RedisGateway(redis_client=client, message_wait_interval_seconds=0)
    queue = RedisMessageQueue("drain-acquire-signal", gateway=gateway)
    processing_key = queue.key.processing
    claim_id = "acquisition-window-claim-id"
    gateway._set_pending_claim_id(processing_key, claim_id)

    real_acquire = gateway._acquire_pending_claim_id

    def raising_acquire(*args: object, **kwargs: object) -> str | None:
        real_acquire(*args, **kwargs)
        raise KeyboardInterrupt("signal in pending-claim acquisition window")

    gateway._acquire_pending_claim_id = raising_acquire  # type: ignore[method-assign]

    with pytest.raises(KeyboardInterrupt, match="acquisition window"):
        with queue.process_message():
            pass

    gateway._acquire_pending_claim_id = real_acquire  # type: ignore[method-assign]
    # The recovering entry must be released even though the signal landed
    # before the recovery ran; the claim id itself stays pending so a later
    # drain can still resolve it (nothing committed, so recovery clears it).
    assert gateway._recovering_claim_ids == {}
    assert gateway._pending_claim_ids[processing_key] == [claim_id]
    assert queue.drain(timeout=5) is True
    assert gateway._pending_claim_ids == {}


class _KeyboardInterruptOnExitLock:
    """Wraps a lock and raises KeyboardInterrupt from the first armed exit.

    The interrupt fires AFTER the underlying lock is released, modelling a
    signal delivered in the window between a lock-guarded registration and
    the try/finally that would clean it up.
    """

    def __init__(self, inner: object) -> None:
        self._inner = inner
        self.armed = False

    def __enter__(self) -> object:
        return self._inner.__enter__()  # type: ignore[attr-defined]

    def __exit__(self, exc_type: object, exc: object, tb: object) -> bool:
        result = self._inner.__exit__(exc_type, exc, tb)  # type: ignore[attr-defined]
        if self.armed and exc_type is None:
            self.armed = False
            raise KeyboardInterrupt("signal after drain acquired a pending claim id")
        return bool(result)


def test_sync_drain_signal_after_pending_claim_selection_releases_recovering_entry():
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue(
        "drain-selection-signal",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
    )
    gateway: RedisGateway = queue._redis  # type: ignore[assignment]
    processing_key = queue.key.processing
    claim_id = "drain-selection-signal-claim-id"
    gateway._set_pending_claim_id(processing_key, claim_id)

    wrapped = _KeyboardInterruptOnExitLock(gateway._pending_claim_ids_lock)
    gateway._pending_claim_ids_lock = wrapped  # type: ignore[assignment]
    wrapped.armed = True
    # The first lock exit inside the drain loop is the one that just selected
    # the claim id and added it to the recovering set.
    with pytest.raises(KeyboardInterrupt, match="drain acquired a pending claim id"):
        gateway._drain_pending_claim_ids(processing_key, deadline_monotonic=None)

    gateway._pending_claim_ids_lock = wrapped._inner  # type: ignore[assignment]
    assert gateway._recovering_claim_ids == {}
    assert gateway._pending_claim_ids[processing_key] == [claim_id]
    assert queue.drain(timeout=5) is True
    assert gateway._pending_claim_ids == {}


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


def test_sync_drain_interrupts_in_flight_blocked_publish_without_signal_handler():
    # A publisher blocked in a block-policy capacity wait holds _publish_lock for
    # the whole wait. With NO GracefulInterruptHandler configured, drain() (from
    # another thread) must still abort it promptly via the forwarded drain flag,
    # instead of stalling for the full block timeout.
    block_timeout = 10.0
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue(
        "drain-blocked-publish",
        client=client,
        deduplication=False,
        max_delivery_count=None,
        max_pending_length=1,
        pending_overload_policy="block",
        pending_overload_block_timeout_seconds=block_timeout,
    )
    assert queue.publish("first") is True  # fills capacity

    entered = threading.Event()
    publish_outcome: list[type[BaseException]] = []

    def publisher() -> None:
        entered.set()
        try:
            queue.publish("second")
        except BaseException as exc:  # noqa: BLE001
            publish_outcome.append(type(exc))

    pub_thread = threading.Thread(target=publisher)
    pub_thread.start()
    assert entered.wait(timeout=5)
    time.sleep(0.2)  # let the publisher reach the block wait holding _publish_lock

    start = time.monotonic()
    assert queue.drain(timeout=1) is True
    drain_elapsed = time.monotonic() - start
    pub_thread.join(timeout=5)

    # Drain returned well under the (10s) block timeout instead of waiting it out.
    assert drain_elapsed < block_timeout / 2, drain_elapsed
    assert publish_outcome == [QueueBackpressureError]
    with pytest.raises(QueueDrainedError):
        queue.publish("after-drain")


def test_sync_drain_interrupts_in_flight_blocked_add_message_without_signal_handler():
    # Same as above for the non-deduplicated add_message block path.
    block_timeout = 10.0
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue(
        "drain-blocked-add",
        client=client,
        deduplication=False,
        max_delivery_count=None,
        max_pending_length=1,
        pending_overload_policy="block",
        pending_overload_block_timeout_seconds=block_timeout,
    )
    assert queue.publish("first") is True

    entered = threading.Event()
    publish_outcome: list[type[BaseException]] = []

    def publisher() -> None:
        entered.set()
        try:
            queue.publish("second")
        except BaseException as exc:  # noqa: BLE001
            publish_outcome.append(type(exc))

    pub_thread = threading.Thread(target=publisher)
    pub_thread.start()
    assert entered.wait(timeout=5)
    time.sleep(0.2)

    start = time.monotonic()
    assert queue.drain(timeout=1) is True
    drain_elapsed = time.monotonic() - start
    pub_thread.join(timeout=5)

    assert drain_elapsed < block_timeout / 2, drain_elapsed
    assert publish_outcome == [QueueBackpressureError]


class _SyncNoInterruptiblePublishGateway(SyncAbstractRedisGateway):
    """Custom gateway implementing only the public AbstractRedisGateway contract.

    It has no ``_publish_message_interruptible`` / ``_add_message_interruptible``
    (those are private extensions of the built-in gateway). The queue's
    duck-typed publish path must fall back to the public publish_message /
    add_message without raising AttributeError, preserving prior behavior.
    """

    def __init__(self) -> None:
        self.published: list[str] = []

    def publish_message(self, queue: str, message: str, dedup_key: str) -> bool:
        self.published.append(message)
        return True

    def add_message(self, queue: str, message: str) -> None:
        self.published.append(message)

    def move_message(self, from_queue, to_queue, message, *, lease_token=None) -> bool:
        return True

    def remove_message(self, queue, message, *, lease_token=None) -> bool:
        return True

    def renew_message_lease(self, queue, message, lease_token, **_kwargs) -> bool:
        return True

    def wait_for_message_and_move(self, from_queue, to_queue):
        return None

    def trim_queue(self, queue, max_length) -> None:
        pass


def test_sync_queue_falls_back_when_gateway_lacks_interruptible_publish():
    gateway = _SyncNoInterruptiblePublishGateway()
    assert not hasattr(gateway, "_publish_message_interruptible")
    queue = RedisMessageQueue(
        "drain-no-interruptible-publish",
        gateway=gateway,
        deduplication=True,
        get_deduplication_key=lambda message: message,
    )
    # Publish still works via the public method fallback (no AttributeError).
    assert queue.publish("payload") is True
    assert gateway.published == ["payload"]
    assert queue.drain() is True
    with pytest.raises(QueueDrainedError):
        queue.publish("after-drain")


# ---- async parity ----


@pytest.mark.asyncio
async def test_async_drain_after_publish_only_returns_true():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue("drain-clean", client=client, deduplication=False)
    await queue.publish("hello")

    assert await queue.drain() is True
    assert queue._draining is True
    assert queue._drained is True


@pytest.mark.asyncio
async def test_async_drain_emits_start_and_success_events_from_idle_queue():
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
async def test_async_second_drain_emits_skipped_event():
    events: list[QueueEvent] = []

    async def observe(event: QueueEvent) -> None:
        events.append(event)

    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue("async-drain-events-skipped", client=client, deduplication=False, on_event=observe)

    assert await queue.drain() is True
    events.clear()

    assert await queue.drain(timeout=2) is True

    assert [(event.operation, event.outcome) for event in events] == [(EventOperation.DRAIN, EventOutcome.SKIPPED)]
    skipped = events[0]
    assert skipped.timeout_seconds == 2.0
    assert skipped.pending_claim_ids == 0
    assert skipped.duration_ms is not None
    assert skipped.error is None
    assert skipped.exception_type is None


@pytest.mark.asyncio
async def test_async_drain_pending_claim_recovery_failure_emits_failure_event():
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
    assert failure.error.queue == "async-drain-events-failure"
    assert failure.error.operation == "drain"
    assert failure.error.__cause__ is recovery_error


@pytest.mark.asyncio
async def test_async_drain_pending_claim_recovery_failure_wraps_failure_event_error():
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
async def test_async_publish_after_drain_raises_queue_drained_error():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue("drain-publish-refuse", client=client, deduplication=False)
    assert await queue.publish("before") is True
    assert "drained=False" in repr(queue)

    assert await queue.drain() is True

    assert "drained=True" in repr(queue)
    with pytest.raises(QueueDrainedError, match="queue is drained"):
        await queue.publish("after")


@pytest.mark.asyncio
async def test_async_drain_is_idempotent_and_keeps_refusing_publish():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue("drain-idempotent", client=client, deduplication=False)

    assert await queue.drain() is True
    assert await queue.drain() is True
    with pytest.raises(QueueDrainedError):
        await queue.publish("after")


@pytest.mark.asyncio
async def test_async_drained_state_is_local_to_queue_instance():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue("drain-local", client=client, deduplication=False)

    assert await queue.drain() is True
    with pytest.raises(QueueDrainedError):
        await queue.publish("after")

    fresh_queue = AsyncRedisMessageQueue("drain-local", client=client, deduplication=False)
    assert await fresh_queue.publish("fresh") is True


@pytest.mark.asyncio
async def test_async_drain_waits_for_in_flight_publish_path():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue("drain-publish-in-flight", client=client, deduplication=False)
    gateway: AsyncRedisGateway = queue._redis  # type: ignore[assignment]
    original_add_message = gateway._add_message_interruptible

    entered_publish = asyncio.Event()
    release_publish = asyncio.Event()

    async def controlled_add_message(queue_name: str, message: str, **kwargs) -> None:
        entered_publish.set()
        await release_publish.wait()
        return await original_add_message(queue_name, message, **kwargs)

    gateway._add_message_interruptible = controlled_add_message  # type: ignore[method-assign]

    publish_task = asyncio.create_task(queue.publish("in-flight"))
    await asyncio.wait_for(entered_publish.wait(), timeout=1)

    drain_task = asyncio.create_task(queue.drain(timeout=1))
    await asyncio.sleep(0.05)
    assert drain_task.done() is False

    release_publish.set()
    assert await publish_task is True
    assert await drain_task is True
    with pytest.raises(QueueDrainedError):
        await queue.publish("after")


@pytest.mark.asyncio
async def test_async_drain_refuses_new_claims_after_call():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue(
        "drain-refuse",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
    )
    await queue.publish("payload")
    await queue.drain()

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
async def test_async_in_flight_handler_publish_during_drain_raises_queue_drained_error():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue(
        "drain-handler-publish",
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
    assert await queue.drain(timeout=1) is True
    drain_done.set()
    await asyncio.wait_for(task, timeout=1)

    assert caught == [QueueDrainedError]


@pytest.mark.asyncio
async def test_async_drain_recovers_pre_populated_pending_claim_id():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue(
        "drain-recover",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
    )
    await queue.publish("payload")

    gateway: AsyncRedisGateway = queue._redis  # type: ignore[assignment]
    processing_key = queue.key.processing
    seeded_claim_id = "drain-test-claim-id"
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

    assert await queue.drain(timeout=2) is True
    assert processing_key not in gateway._pending_claim_ids
    assert await client.lrange(queue.key.pending, 0, -1) == [claimed]
    assert await client.llen(processing_key) == 0
    assert await client.hget(claim_result_ids_key, seeded_claim_id) is None
    assert await client.hget(claim_result_backrefs_key, claimed) is None

    fresh_queue = AsyncRedisMessageQueue(
        "drain-recover",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
    )
    async with fresh_queue.process_message() as message:
        assert message == b"payload"


@pytest.mark.asyncio
async def test_async_drain_waits_for_racing_ambiguous_claim_registration():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue(
        "drain-racing-claim",
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

    assert await queue.drain(timeout=0.05) is False
    assert gateway._pending_claim_ids.get(processing_key) is None

    allow_error.set()
    await asyncio.wait_for(task, timeout=5)
    assert observed == [None]
    assert gateway._pending_claim_ids.get(processing_key) == claim_ids

    assert await queue.drain(timeout=1) is True
    assert processing_key not in gateway._pending_claim_ids
    assert await client.llen(processing_key) == 0

    fresh_queue = AsyncRedisMessageQueue(
        "drain-racing-claim",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
    )
    async with fresh_queue.process_message() as message:
        assert message == b"payload"


@pytest.mark.asyncio
async def test_async_drain_clears_in_flight_claim_id_after_registration_signal():
    client = fakeredis.FakeAsyncRedis()
    gateway = AsyncRedisGateway(redis_client=client, message_wait_interval_seconds=0)
    queue = AsyncRedisMessageQueue("drain-in-flight-signal", gateway=gateway)
    real_begin = gateway._begin_in_flight_claim_id

    def raising_begin(processing_queue: str, claim_id: str) -> None:
        real_begin(processing_queue, claim_id)
        raise KeyboardInterrupt("signal after in-flight claim registration")

    gateway._begin_in_flight_claim_id = raising_begin  # type: ignore[method-assign]

    with pytest.raises(KeyboardInterrupt, match="in-flight claim registration"):
        async with queue.process_message():
            pass

    assert gateway._in_flight_claim_ids == {}
    assert await queue.drain(timeout=None) is True


@pytest.mark.asyncio
async def test_async_drain_waits_for_concurrent_pending_claim_recovery():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue(
        "drain-concurrent-recovery-wait",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
    )
    gateway: AsyncRedisGateway = queue._redis  # type: ignore[assignment]
    processing_key = queue.key.processing
    claim_id = "concurrent-recovery-claim-id"
    # A concurrent consumer's _wait_for_claim preamble has acquired the pending
    # claim id (registered + marked recovering) and is still mid-recovery.
    gateway._set_pending_claim_id(processing_key, claim_id)
    with gateway._pending_claim_ids_lock:
        gateway._recovering_claim_ids.setdefault(processing_key, set()).add(claim_id)

    drain_task = asyncio.create_task(queue.drain(timeout=None))
    # timeout=None waits indefinitely: the drain must poll for the concurrent
    # recovery to resolve instead of giving up on the recovering id.
    await asyncio.sleep(0.3)
    assert not drain_task.done(), "drain(timeout=None) returned while a concurrent recovery was still running"

    # The concurrent consumer's recovery completes and clears the claim id.
    gateway._finish_pending_claim_recovery(processing_key, claim_id, clear=True)
    assert await asyncio.wait_for(drain_task, timeout=5) is True
    assert processing_key not in gateway._pending_claim_ids


@pytest.mark.asyncio
async def test_async_drain_deadline_bounds_wait_for_concurrent_pending_claim_recovery():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue(
        "drain-concurrent-recovery-deadline",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
    )
    gateway: AsyncRedisGateway = queue._redis  # type: ignore[assignment]
    processing_key = queue.key.processing
    claim_id = "concurrent-recovery-deadline-claim-id"
    gateway._set_pending_claim_id(processing_key, claim_id)
    with gateway._pending_claim_ids_lock:
        gateway._recovering_claim_ids.setdefault(processing_key, set()).add(claim_id)

    timeout_seconds = 0.3
    loop = asyncio.get_running_loop()
    started = loop.time()
    result = await queue.drain(timeout=timeout_seconds)
    elapsed = loop.time() - started

    # The recovering id never resolves, so drain fails — but only after
    # honoring the deadline instead of returning False instantly.
    assert result is False
    assert elapsed >= timeout_seconds
    assert gateway._pending_claim_ids[processing_key] == [claim_id]


@pytest.mark.asyncio
async def test_async_consumer_interrupt_in_pending_claim_acquisition_window_releases_recovering_entry():
    client = fakeredis.FakeAsyncRedis()
    gateway = AsyncRedisGateway(redis_client=client, message_wait_interval_seconds=0)
    queue = AsyncRedisMessageQueue("drain-acquire-signal", gateway=gateway)
    processing_key = queue.key.processing
    claim_id = "acquisition-window-claim-id"
    gateway._set_pending_claim_id(processing_key, claim_id)

    real_acquire = gateway._acquire_pending_claim_id

    def raising_acquire(*args: object, **kwargs: object) -> str | None:
        real_acquire(*args, **kwargs)
        raise KeyboardInterrupt("signal in pending-claim acquisition window")

    gateway._acquire_pending_claim_id = raising_acquire  # type: ignore[method-assign]

    with pytest.raises(KeyboardInterrupt, match="acquisition window"):
        async with queue.process_message():
            pass

    gateway._acquire_pending_claim_id = real_acquire  # type: ignore[method-assign]
    # The recovering entry must be released even though the interrupt landed
    # before the recovery ran; the claim id itself stays pending so a later
    # drain can still resolve it (nothing committed, so recovery clears it).
    assert gateway._recovering_claim_ids == {}
    assert gateway._pending_claim_ids[processing_key] == [claim_id]
    assert await queue.drain(timeout=5) is True
    assert gateway._pending_claim_ids == {}


@pytest.mark.asyncio
async def test_async_drain_interrupt_after_pending_claim_selection_releases_recovering_entry():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue(
        "drain-selection-signal",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
    )
    gateway: AsyncRedisGateway = queue._redis  # type: ignore[assignment]
    processing_key = queue.key.processing
    claim_id = "drain-selection-signal-claim-id"
    gateway._set_pending_claim_id(processing_key, claim_id)

    wrapped = _KeyboardInterruptOnExitLock(gateway._pending_claim_ids_lock)
    gateway._pending_claim_ids_lock = wrapped  # type: ignore[assignment]
    wrapped.armed = True
    # The first lock exit inside the drain loop is the one that just selected
    # the claim id and added it to the recovering set.
    with pytest.raises(KeyboardInterrupt, match="drain acquired a pending claim id"):
        await gateway._drain_pending_claim_ids(processing_key, deadline_monotonic=None)

    gateway._pending_claim_ids_lock = wrapped._inner  # type: ignore[assignment]
    assert gateway._recovering_claim_ids == {}
    assert gateway._pending_claim_ids[processing_key] == [claim_id]
    assert await queue.drain(timeout=5) is True
    assert gateway._pending_claim_ids == {}


@pytest.mark.asyncio
async def test_async_drain_preserves_string_only_recovery_token_until_requeue_succeeds():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue(
        "drain-string-only-recover",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
    )
    gateway: AsyncRedisGateway = queue._redis  # type: ignore[assignment]
    processing_key = queue.key.processing
    claim_id = "drain-string-only-claim-id"
    stored_message = encode_stored_message("payload").encode("utf-8")
    claim_result_key = gateway._claim_result_key(processing_key, claim_id)

    await client.lpush(processing_key, stored_message)
    await client.set(claim_result_key, stored_message, px=120_000)
    gateway._set_pending_claim_id(processing_key, claim_id)

    original_return = gateway._return_recovered_non_visibility_timeout_claim_to_pending

    async def timeout_before_requeue(*_args: object, **_kwargs: object) -> bool:
        raise AsyncDrainDeadlineExceeded

    gateway._return_recovered_non_visibility_timeout_claim_to_pending = timeout_before_requeue  # type: ignore[method-assign]

    assert await queue.drain(timeout=2) is False
    assert await client.get(claim_result_key) == stored_message
    assert gateway._pending_claim_ids[processing_key] == [claim_id]
    assert await client.llen(queue.key.pending) == 0
    assert await client.llen(processing_key) == 1

    gateway._return_recovered_non_visibility_timeout_claim_to_pending = original_return  # type: ignore[method-assign]

    assert await queue.drain(timeout=2) is True
    assert await client.get(claim_result_key) is None
    assert processing_key not in gateway._pending_claim_ids
    assert await client.lrange(queue.key.pending, 0, -1) == [stored_message]
    assert await client.llen(processing_key) == 0


@pytest.mark.asyncio
async def test_async_drain_keeps_claim_id_pending_when_recovered_message_cannot_be_requeued():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue(
        "drain-recover-missing-processing",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
    )
    gateway: AsyncRedisGateway = queue._redis  # type: ignore[assignment]
    processing_key = queue.key.processing
    seeded_claim_id = "drain-missing-processing-claim-id"
    stored_message = encode_stored_message("payload")
    await client.hset(gateway._claim_result_ids_key(processing_key), seeded_claim_id, stored_message)
    await client.hset(gateway._claim_result_backrefs_key(processing_key), stored_message, seeded_claim_id)
    gateway._set_pending_claim_id(processing_key, seeded_claim_id)

    assert await queue.drain(timeout=2) is False
    assert gateway._pending_claim_ids[processing_key] == [seeded_claim_id]
    assert await client.llen(queue.key.pending) == 0
    assert await client.llen(processing_key) == 0


@pytest.mark.asyncio
async def test_async_drain_with_timeout_zero_returns_false_when_pending_remain():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue(
        "drain-timeout-zero",
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
    result = await queue.drain(timeout=0)
    assert result is False
    assert gateway._pending_claim_ids[processing_key] == ["stuck-claim-id"]


@pytest.mark.asyncio
async def test_async_drain_timeout_bounds_slow_pending_claim_recovery_read():
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
    queue = AsyncRedisMessageQueue("drain-slow-recovery-timeout", gateway=gateway)
    processing_key = queue.key.processing
    claim_id = "slow-async-drain-claim-id"
    await client.redis.hset(
        gateway._claim_result_ids_key(processing_key),
        claim_id,
        encode_stored_message("recovered-payload"),
    )
    gateway._set_pending_claim_id(processing_key, claim_id)

    started = time.monotonic()
    result = await queue.drain(timeout=timeout_seconds)
    elapsed = time.monotonic() - started

    assert result is False
    assert elapsed < 0.25
    assert gateway._pending_claim_ids[processing_key] == [claim_id]


@pytest.mark.asyncio
async def test_async_drain_after_timeout_can_retry():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue(
        "drain-timeout-retry",
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

    assert await queue.drain(timeout=0) is False
    assert await queue.drain(timeout=10) is True
    assert drain_calls == 2
    assert await queue.drain(timeout=10) is True
    assert drain_calls == 2


@pytest.mark.asyncio
async def test_async_concurrent_drain_returns_cached_result_and_drains_once():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue(
        "drain-concurrent",
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

    first = asyncio.create_task(queue.drain(timeout=1))
    await asyncio.wait_for(entered_first_drain.wait(), timeout=1)
    second = asyncio.create_task(queue.drain(timeout=0))
    await asyncio.sleep(0)

    release_first_drain.set()
    results = await asyncio.gather(first, second)

    assert results == [True, True]
    assert drain_calls == 1


@pytest.mark.asyncio
async def test_async_drain_preserves_cleanup_on_cancellation():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue(
        "drain-cancel-cleanup",
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

    task = asyncio.create_task(queue.drain())
    await asyncio.wait_for(entered_drain.wait(), timeout=1)

    task.cancel()
    await asyncio.sleep(0)
    release_drain.set_result(None)

    with pytest.raises(asyncio.CancelledError):
        await task

    assert task.cancelled() is True
    assert processing_key not in gateway._pending_claim_ids


@pytest.mark.asyncio
async def test_async_drain_rejects_negative_timeout():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue("drain-validate", client=client, deduplication=False)
    with pytest.raises(ConfigurationError):
        await queue.drain(timeout=-1)
    with pytest.raises(TypeError):
        await queue.drain(timeout="soon")  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_async_drain_interrupts_in_flight_blocked_publish_without_signal_handler():
    # Async twin: a publisher task blocked in a block-policy capacity wait holds
    # _publish_lock for the whole wait. With NO GracefulInterruptHandler, drain()
    # (from another task) must abort it promptly via the forwarded drain flag.
    block_timeout = 10.0
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue(
        "async-drain-blocked-publish",
        client=client,
        deduplication=False,
        max_delivery_count=None,
        max_pending_length=1,
        pending_overload_policy="block",
        pending_overload_block_timeout_seconds=block_timeout,
    )
    assert await queue.publish("first") is True  # fills capacity

    entered = asyncio.Event()
    publish_outcome: list[type[BaseException]] = []

    async def publisher() -> None:
        entered.set()
        try:
            await queue.publish("second")
        except BaseException as exc:  # noqa: BLE001
            publish_outcome.append(type(exc))

    pub_task = asyncio.create_task(publisher())
    await asyncio.wait_for(entered.wait(), timeout=5)
    await asyncio.sleep(0.2)  # let the publisher reach the block wait holding _publish_lock

    start = time.monotonic()
    assert await queue.drain(timeout=1) is True
    drain_elapsed = time.monotonic() - start
    await asyncio.wait_for(pub_task, timeout=5)

    assert drain_elapsed < block_timeout / 2, drain_elapsed
    assert publish_outcome == [QueueBackpressureError]
    with pytest.raises(QueueDrainedError):
        await queue.publish("after-drain")


@pytest.mark.asyncio
async def test_async_drain_interrupts_in_flight_blocked_add_message_without_signal_handler():
    block_timeout = 10.0
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue(
        "async-drain-blocked-add",
        client=client,
        deduplication=False,
        max_delivery_count=None,
        max_pending_length=1,
        pending_overload_policy="block",
        pending_overload_block_timeout_seconds=block_timeout,
    )
    assert await queue.publish("first") is True

    entered = asyncio.Event()
    publish_outcome: list[type[BaseException]] = []

    async def publisher() -> None:
        entered.set()
        try:
            await queue.publish("second")
        except BaseException as exc:  # noqa: BLE001
            publish_outcome.append(type(exc))

    pub_task = asyncio.create_task(publisher())
    await asyncio.wait_for(entered.wait(), timeout=5)
    await asyncio.sleep(0.2)

    start = time.monotonic()
    assert await queue.drain(timeout=1) is True
    drain_elapsed = time.monotonic() - start
    await asyncio.wait_for(pub_task, timeout=5)

    assert drain_elapsed < block_timeout / 2, drain_elapsed
    assert publish_outcome == [QueueBackpressureError]


class _AsyncNoInterruptiblePublishGateway(AsyncAbstractRedisGateway):
    """Async twin of _SyncNoInterruptiblePublishGateway (public contract only)."""

    def __init__(self) -> None:
        self.published: list[str] = []

    async def publish_message(self, queue: str, message: str, dedup_key: str) -> bool:
        self.published.append(message)
        return True

    async def add_message(self, queue: str, message: str) -> None:
        self.published.append(message)

    async def move_message(self, from_queue, to_queue, message, *, lease_token=None) -> bool:
        return True

    async def remove_message(self, queue, message, *, lease_token=None) -> bool:
        return True

    async def renew_message_lease(self, queue, message, lease_token, **_kwargs) -> bool:
        return True

    async def wait_for_message_and_move(self, from_queue, to_queue):
        return None

    async def trim_queue(self, queue, max_length) -> None:
        pass


@pytest.mark.asyncio
async def test_async_queue_falls_back_when_gateway_lacks_interruptible_publish():
    gateway = _AsyncNoInterruptiblePublishGateway()
    assert not hasattr(gateway, "_publish_message_interruptible")
    queue = AsyncRedisMessageQueue(
        "async-drain-no-interruptible-publish",
        gateway=gateway,
        deduplication=True,
        get_deduplication_key=lambda message: message,
    )
    assert await queue.publish("payload") is True
    assert gateway.published == ["payload"]
    assert await queue.drain() is True
    with pytest.raises(QueueDrainedError):
        await queue.publish("after-drain")


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
async def test_async_drain_result_only_none_or_true_across_lifecycle():
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
    assert await queue.drain(timeout=1) is True
    assert queue._drain_result is True
    _assert_drain_result_invariant(queue._drain_result, surface="async", phase="successful drain")

    # idempotent repeat -> short-circuits via the `is True` guard (SKIPPED, not a re-drain)
    events.clear()
    assert await queue.drain(timeout=1) is True
    assert [(e.operation, e.outcome) for e in events] == [(EventOperation.DRAIN, EventOutcome.SKIPPED)]
    assert queue._drain_result is True
    _assert_drain_result_invariant(queue._drain_result, surface="async", phase="idempotent repeat drain")

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

    assert await failing_queue.drain(timeout=1) is False
    assert failing_queue._drain_result is None
    _assert_drain_result_invariant(failing_queue._drain_result, surface="async", phase="failed drain")

    # retry genuinely re-enters the drainer (START+FAILURE), proving the failure did not short-circuit
    failure_events.clear()
    assert await failing_queue.drain(timeout=1) is False
    assert [(e.operation, e.outcome) for e in failure_events] == [
        (EventOperation.DRAIN, EventOutcome.START),
        (EventOperation.DRAIN, EventOutcome.FAILURE),
    ]
    assert failing_queue._drain_result is None
    _assert_drain_result_invariant(failing_queue._drain_result, surface="async", phase="failed-then-retried drain")


# --- Cancellation/interrupt holes in the claim-recovery path -----------------
#
# These regression tests pin two invariants:
#   * A BaseException (CancelledError / KeyboardInterrupt) landing on the awaited
#     cleanup AFTER a successful recovery must re-register the claim id so drain
#     still recovers the message (no-VT mode has no lease to reclaim it).
#   * A cancellation escaping the ``retry_attempt`` emit inside an
#     ``except Exception`` claim handler must publish the (possibly committed)
#     claim id for recovery; the sibling ``except BaseException`` cannot catch it.


def _no_vt_sync_gateway(client):
    return RedisGateway(
        redis_client=client,
        message_wait_interval_seconds=0,
        message_visibility_timeout_seconds=None,
        max_delivery_count=None,
        retry_budget_seconds=0,
    )


def _no_vt_async_gateway(client, *, message_wait_interval_seconds=0):
    return AsyncRedisGateway(
        redis_client=client,
        message_wait_interval_seconds=message_wait_interval_seconds,
        message_visibility_timeout_seconds=None,
        max_delivery_count=None,
        retry_budget_seconds=0,
    )


@pytest.mark.asyncio
async def test_async_recovery_cancel_on_cleanup_keeps_claim_id_pending():
    client = fakeredis.FakeAsyncRedis()
    gateway = _no_vt_async_gateway(client)
    pending, processing = "cancel-clean:pending", "cancel-clean:processing"
    stored = encode_stored_message("strand-me")
    await client.rpush(pending, stored)
    claim_id = "cancel-cleanup-claim"
    await _async_seed_committed_no_vt_claim(
        client=client, gateway=gateway, from_queue=pending, to_queue=processing, claim_id=claim_id
    )
    gateway._set_pending_claim_id(processing, claim_id)

    async def cancel_delete(*args, **kwargs):
        raise asyncio.CancelledError()

    gateway._delete_claim_result_key = cancel_delete  # type: ignore[method-assign]

    with pytest.raises(asyncio.CancelledError):
        await gateway._wait_for_message_and_move_interruptible(pending, processing)

    # The recovery cleared the id in its finally; the cancellation guard must
    # have re-registered it so a follow-up drain still finds the message.
    assert gateway._pending_claim_ids.get(processing) == [claim_id]
    assert await client.llen(processing) == 1


def test_sync_recovery_interrupt_on_cleanup_keeps_claim_id_pending():
    client = fakeredis.FakeRedis()
    gateway = _no_vt_sync_gateway(client)
    pending, processing = "cancel-clean-sync:pending", "cancel-clean-sync:processing"
    stored = encode_stored_message("strand-me")
    client.rpush(pending, stored)
    claim_id = "interrupt-cleanup-claim"
    _seed_committed_no_vt_claim(
        client=client, gateway=gateway, from_queue=pending, to_queue=processing, claim_id=claim_id
    )
    gateway._set_pending_claim_id(processing, claim_id)

    def interrupt_delete(*args, **kwargs):
        raise KeyboardInterrupt()

    gateway._delete_claim_result_key = interrupt_delete  # type: ignore[method-assign]

    with pytest.raises(KeyboardInterrupt):
        gateway._wait_for_message_and_move_interruptible(pending, processing)

    assert gateway._pending_claim_ids.get(processing) == [claim_id]
    assert client.llen(processing) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("message_wait_interval_seconds", [0, 1], ids=["nonblocking", "polling"])
async def test_async_cancel_in_retry_emit_publishes_claim_id(message_wait_interval_seconds):
    client = fakeredis.FakeAsyncRedis()
    gateway = _no_vt_async_gateway(client, message_wait_interval_seconds=message_wait_interval_seconds)
    pending, processing = "cancel-emit:pending", "cancel-emit:processing"
    stored = encode_stored_message("ambiguous")
    await client.rpush(pending, stored)
    committed_ids: list[str] = []

    async def failing_claim(from_queue, to_queue, *, claim_id):
        await _async_seed_committed_no_vt_claim(
            client=client, gateway=gateway, from_queue=from_queue, to_queue=to_queue, claim_id=claim_id
        )
        committed_ids.append(claim_id)
        raise redis.exceptions.ConnectionError("ambiguous claim after commit")

    gateway._claim_message_without_visibility_timeout = failing_claim  # type: ignore[method-assign]

    async def cancel_emit(*args, **kwargs):
        raise asyncio.CancelledError()

    gateway._emit_event = cancel_emit  # type: ignore[method-assign]

    with pytest.raises(asyncio.CancelledError):
        await gateway._wait_for_message_and_move_interruptible(pending, processing)

    assert len(committed_ids) == 1
    # The claim committed server-side; the cancellation escaping the retry emit
    # must still publish its id so drain recovers the ambiguous message.
    assert gateway._pending_claim_ids.get(processing) == committed_ids


def test_sync_interrupt_in_retry_emit_publishes_claim_id():
    client = fakeredis.FakeRedis()
    gateway = RedisGateway(
        redis_client=client,
        message_wait_interval_seconds=1,
        message_visibility_timeout_seconds=None,
        max_delivery_count=None,
        retry_budget_seconds=0,
    )
    pending, processing = "cancel-emit-sync:pending", "cancel-emit-sync:processing"
    stored = encode_stored_message("ambiguous")
    client.rpush(pending, stored)
    committed_ids: list[str] = []

    def failing_claim(from_queue, to_queue, *, claim_id):
        _seed_committed_no_vt_claim(
            client=client, gateway=gateway, from_queue=from_queue, to_queue=to_queue, claim_id=claim_id
        )
        committed_ids.append(claim_id)
        raise redis.exceptions.ConnectionError("ambiguous claim after commit")

    gateway._claim_message_without_visibility_timeout = failing_claim  # type: ignore[method-assign]

    def interrupt_emit(*args, **kwargs):
        raise KeyboardInterrupt()

    gateway._emit_event = interrupt_emit  # type: ignore[method-assign]

    with pytest.raises(KeyboardInterrupt):
        gateway._wait_for_message_and_move_interruptible(pending, processing)

    assert len(committed_ids) == 1
    assert gateway._pending_claim_ids.get(processing) == committed_ids


# ---- is_draining / is_drained public properties + post-drain hot-spin ----


def test_sync_is_draining_is_drained_properties_before_during_after():
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue("props-sync", client=client, deduplication=False)

    # Before drain: neither flag is set.
    assert queue.is_draining is False
    assert queue.is_drained is False

    # After drain: both properties read True and match the private accessors.
    assert queue.drain() is True
    assert queue.is_draining is True
    assert queue.is_drained is True
    assert queue.is_draining is queue._draining
    assert queue.is_drained is queue._drained.is_set()


def test_sync_is_draining_true_while_drain_in_progress():
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue("props-sync-midflight", client=client, deduplication=False)

    gateway: RedisGateway = queue._redis  # type: ignore[assignment]
    original_add_message = gateway._add_message_interruptible

    inside_publish = threading.Event()
    release_publish = threading.Event()
    seen = {}

    def controlled_add_message(queue_name: str, message: str, **kwargs) -> None:
        inside_publish.set()
        assert release_publish.wait(timeout=5)
        return original_add_message(queue_name, message, **kwargs)

    gateway._add_message_interruptible = controlled_add_message  # type: ignore[method-assign]

    publisher = threading.Thread(target=lambda: queue.publish("mid"))
    publisher.start()
    assert inside_publish.wait(timeout=5)

    drainer = threading.Thread(target=lambda: seen.__setitem__("result", queue.drain(timeout=5)))
    drainer.start()

    # drain() sets _draining before it can acquire the publish lock (held by the
    # in-flight publish), so is_draining reads True while is_drained is still False.
    deadline = time.monotonic() + 2
    while not queue.is_draining and time.monotonic() < deadline:
        time.sleep(0.005)
    assert queue.is_draining is True
    assert queue.is_drained is False

    release_publish.set()
    publisher.join(5)
    drainer.join(5)
    assert seen["result"] is True
    assert queue.is_drained is True


@pytest.mark.asyncio
async def test_async_is_draining_is_drained_properties_before_during_after():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue("props-async", client=client, deduplication=False)

    assert queue.is_draining is False
    assert queue.is_drained is False

    assert await queue.drain() is True
    assert queue.is_draining is True
    assert queue.is_drained is True
    assert queue.is_draining is queue._draining
    assert queue.is_drained is queue._drained


@pytest.mark.asyncio
async def test_async_post_drain_consume_loop_yields_to_sibling_task():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue("post-drain-spin", client=client, deduplication=False)

    assert await queue.drain() is True

    ticks = 0
    stop = False

    async def ticker():
        nonlocal ticks
        while not stop:
            ticks += 1
            await asyncio.sleep(0)

    ticker_task = asyncio.create_task(ticker())
    try:
        # Standard documented post-drain consume loop. Without a real suspension
        # point in the drained fast path (on_event unset -> _emit_event returns
        # immediately), this loop would starve the sibling ticker entirely.
        for _ in range(1000):
            async with queue.process_message() as message:
                assert message is None
    finally:
        stop = True
        ticker_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await ticker_task

    # The ticker must have run at least once per drained iteration's yield.
    assert ticks > 0
