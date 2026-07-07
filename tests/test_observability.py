import asyncio
import importlib
import logging
import threading
import time
import warnings

import fakeredis
import pytest
import redis

from redis_message_queue import (
    CleanupFailedError,
    ConfigurationError,
    EventOperation,
    EventOutcome,
    GatewayContractError,
    LuaScriptError,
    QueueEvent,
    RedisMessageQueue,
    RedisMessageQueueError,
    RetryBudgetExhaustedError,
)
from redis_message_queue._abstract_redis_gateway import AbstractRedisGateway
from redis_message_queue._redis_gateway import RedisGateway
from redis_message_queue._stored_message import ClaimedMessage, ReceivedPayload, encode_stored_message
from redis_message_queue.asyncio import RedisMessageQueue as AsyncRedisMessageQueue
from redis_message_queue.asyncio._redis_gateway import RedisGateway as AsyncRedisGateway
from redis_message_queue.redis_message_queue import _LeaseHeartbeat


class _Gateway(AbstractRedisGateway):
    message_visibility_timeout_seconds = 1

    def __init__(
        self,
        *,
        remove_return: bool = True,
        move_return: bool = True,
        fail_remove: bool = False,
        fail_trim: bool = False,
        fail_renew: bool = False,
    ) -> None:
        self.remove_return = remove_return
        self.move_return = move_return
        self.fail_remove = fail_remove
        self.fail_trim = fail_trim
        self.fail_renew = fail_renew
        self.message = encode_stored_message("payload")

    def publish_message(self, queue: str, message: str, dedup_key: str) -> bool:
        self.message = encode_stored_message(message)
        return True

    def add_message(self, queue: str, message: str) -> None:
        self.message = encode_stored_message(message)

    def wait_for_message_and_move(self, from_queue: str, to_queue: str) -> ClaimedMessage | ReceivedPayload | None:
        return ClaimedMessage(self.message, "lease-token")

    def remove_message(self, queue: str, message: ReceivedPayload, *, lease_token: str | None = None) -> bool:
        if self.fail_remove:
            raise ConnectionError("cleanup failed")
        return self.remove_return

    def move_message(
        self,
        from_queue: str,
        to_queue: str,
        message: ReceivedPayload,
        *,
        lease_token: str | None = None,
    ) -> bool:
        return self.move_return

    def renew_message_lease(self, queue: str, message: ReceivedPayload, lease_token: str, **_kwargs) -> bool:
        if self.fail_renew:
            raise ConnectionError("renew failed")
        return True

    def trim_queue(self, queue: str, max_length: int) -> None:
        if self.fail_trim:
            raise ConnectionError("trim failed")


class _AlwaysFailEvalClient:
    def eval(self, *args):
        raise redis.exceptions.ConnectionError("transient connection error")


def _ops(events: list[QueueEvent]) -> list[EventOperation]:
    return [event.operation for event in events]


def test_sync_event_hook_emits_publish_claim_ack_and_empty_events():
    client = fakeredis.FakeRedis()
    events: list[QueueEvent] = []
    queue = RedisMessageQueue(
        "observed",
        client=client,
        deduplication=True,
        get_deduplication_key=lambda msg: msg,
        enable_completed_queue=True,
        on_event=events.append,
    )

    queue.publish("hello")
    queue.publish("hello")
    with queue.process_message() as message:
        assert message == b"hello"
    with queue.process_message() as message:
        assert message is None

    operations = _ops(events)
    assert "publish" in operations
    assert events[0].operation is EventOperation.PUBLISH
    assert events[0].outcome is EventOutcome.SUCCESS
    assert events[0].error is None
    assert "publish_dedup_hit" in operations
    assert "claim" in operations
    assert "completed" in operations
    assert "ack" in operations
    assert "claim_empty" in operations
    assert any(event.message_id and event.lease_token_hash for event in events if event.operation == "claim")
    assert all(event.error is None for event in events)


def test_sync_event_hook_emits_publish_failure_error_object():
    client = fakeredis.FakeRedis()
    events: list[QueueEvent] = []
    queue = RedisMessageQueue("observed", client=client, on_event=events.append)
    client.set(queue.key.pending, "not a list")

    with pytest.raises(redis.exceptions.ResponseError):
        queue.publish("hello")

    event = next(event for event in events if event.operation is EventOperation.PUBLISH)
    assert event.outcome is EventOutcome.FAILURE
    assert event.exception_type is not None
    assert isinstance(event.error, redis.exceptions.ResponseError)


def test_sync_event_hook_emits_failure_stale_cleanup_and_trim_events():
    events: list[QueueEvent] = []
    stale_ack = RedisMessageQueue("observed", gateway=_Gateway(remove_return=False), on_event=events.append)
    with pytest.warns(RuntimeWarning, match="successful processing"):
        with stale_ack.process_message():
            pass

    stale_nack = RedisMessageQueue("observed", gateway=_Gateway(remove_return=False), on_event=events.append)
    with pytest.warns(RuntimeWarning, match="failed processing"):
        with pytest.raises(ValueError, match="boom"):
            with stale_nack.process_message():
                raise ValueError("boom")

    cleanup_failed = RedisMessageQueue("observed", gateway=_Gateway(fail_remove=True), on_event=events.append)
    with pytest.warns(RuntimeWarning, match="Cleanup raised"):
        with pytest.raises(ValueError, match="boom"):
            with cleanup_failed.process_message():
                raise ValueError("boom")

    trim_failed = RedisMessageQueue(
        "observed",
        gateway=_Gateway(fail_trim=True),
        enable_completed_queue=True,
        on_event=events.append,
    )
    with pytest.warns(RuntimeWarning, match="Failed to trim"):
        with trim_failed.process_message():
            pass

    operations = _ops(events)
    assert "failed" in operations
    assert "nack" in operations
    assert "stale_lease_ack" in operations
    assert "stale_lease_nack" in operations
    assert "cleanup_failed" in operations
    assert "trim_failed" in operations
    failed_events = [event for event in events if event.operation is EventOperation.FAILED]
    assert failed_events
    assert all(isinstance(event.error, ValueError) for event in failed_events)
    cleanup_failed_events = [event for event in events if event.operation is EventOperation.CLEANUP_FAILED]
    assert cleanup_failed_events
    assert all(isinstance(event.error, ConnectionError) for event in cleanup_failed_events)
    trim_failed_event = next(event for event in events if event.operation is EventOperation.TRIM_FAILED)
    assert isinstance(trim_failed_event.error, ConnectionError)
    assert all(event.error is None for event in events if event.outcome is EventOutcome.SUCCESS)


def test_sync_event_hook_emits_lease_and_heartbeat_timeout_events():
    events: list[QueueEvent] = []
    queue = RedisMessageQueue(
        "observed",
        gateway=_Gateway(),
        heartbeat_interval_seconds=0.01,
        on_event=events.append,
    )
    with queue.process_message():
        time.sleep(0.03)

    failed_queue = RedisMessageQueue(
        "observed",
        gateway=_Gateway(fail_renew=True),
        heartbeat_interval_seconds=0.01,
        on_event=events.append,
    )
    with failed_queue.process_message():
        time.sleep(0.03)

    def emit(operation, outcome, **kwargs):
        events.append(QueueEvent("observed", operation, outcome, **kwargs))

    renew_started = threading.Event()

    def slow_renew() -> bool:
        renew_started.set()
        time.sleep(0.3)
        return True

    heartbeat = _LeaseHeartbeat(interval_seconds=0.01, renew_message_lease=slow_renew, emit_event=emit)
    heartbeat.start()
    assert renew_started.wait(timeout=1)
    with pytest.warns(RuntimeWarning, match="Heartbeat did not stop"):
        heartbeat.stop()
    heartbeat._thread.join(timeout=1)
    assert not heartbeat._thread.is_alive()

    operations = _ops(events)
    assert "lease_renew" in operations
    assert "lease_renew_failed" in operations
    assert "heartbeat_stop_timeout" in operations
    renew_failed_event = next(
        event
        for event in events
        if event.operation is EventOperation.LEASE_RENEW_FAILED and event.outcome is EventOutcome.FAILURE
    )
    assert isinstance(renew_failed_event.error, ConnectionError)
    assert all(event.error is None for event in events if event.operation is EventOperation.LEASE_RENEW)


def test_sync_event_hook_emits_reclaim_dlq_and_retry_events():
    events: list[QueueEvent] = []
    client = fakeredis.FakeRedis()
    gateway = RedisGateway(
        redis_client=client,
        message_visibility_timeout_seconds=1,
        max_delivery_count=1,
        dead_letter_queue="observed::dlq",
        message_wait_interval_seconds=0,
    )
    queue = RedisMessageQueue("observed", gateway=gateway, on_event=events.append)
    gateway.add_message(queue.key.pending, "poison")
    claim = queue._redis.wait_for_message_and_move(queue.key.pending, queue.key.processing)
    assert isinstance(claim, ClaimedMessage)
    client.zadd(queue._redis._lease_deadlines_key(queue.key.processing), {claim.stored_message: 0})
    assert queue._redis.wait_for_message_and_move(queue.key.pending, queue.key.processing) is None

    retry_gateway = RedisGateway(
        redis_client=_AlwaysFailEvalClient(),
        retry_budget_seconds=0,
        message_wait_interval_seconds=0,
    )
    retry_queue = RedisMessageQueue("observed", gateway=retry_gateway, on_event=events.append)
    with pytest.raises(RetryBudgetExhaustedError):
        with retry_queue.process_message():
            pass

    operations = _ops(events)
    assert "claim_reclaim" in operations
    assert "dlq" in operations
    assert "retry_attempt" in operations
    assert "retry_exhausted" in operations
    retry_events = [
        event for event in events if event.operation in {EventOperation.RETRY_ATTEMPT, EventOperation.RETRY_EXHAUSTED}
    ]
    assert retry_events
    assert all(isinstance(event.error, redis.exceptions.ConnectionError) for event in retry_events)


def test_sync_shared_gateway_routes_retry_events_to_the_operating_queue_not_the_last_constructed_one():
    """Two queues sharing one gateway (permitted whenever ``max_delivery_count``
    is unset) each get their own gateway-level events (``retry_attempt`` /
    ``retry_exhausted`` / ``claim_reclaim`` / ``dlq``) routed to their own
    ``on_event`` under their own queue name. Before the fix, the gateway kept a
    single ``_event_emitter`` slot that every ``RedisMessageQueue.__init__``
    unconditionally overwrote, so the *last-constructed* queue's emitter won
    regardless of which queue was actually driving the claim -- queue A's
    retry telemetry was silently rerouted to queue B's ``on_event`` (tagged
    with queue B's name) instead of reaching queue A's.
    """
    events_a: list[QueueEvent] = []
    events_b: list[QueueEvent] = []
    gateway = RedisGateway(
        redis_client=_AlwaysFailEvalClient(),
        retry_budget_seconds=0,
        message_wait_interval_seconds=0,
    )
    queue_a = RedisMessageQueue("shared-gateway-a", gateway=gateway, on_event=events_a.append)
    # Constructed AFTER queue_a: under the single-slot bug this is the emitter
    # that would win for every subsequent gateway-level event, no matter which
    # queue's claim triggered it.
    RedisMessageQueue("shared-gateway-b", gateway=gateway, on_event=events_b.append)

    with pytest.raises(RetryBudgetExhaustedError):
        with queue_a.process_message():
            pass

    assert _ops(events_a).count("retry_attempt") > 0
    assert _ops(events_a).count("retry_exhausted") > 0
    assert all(event.queue == "shared-gateway-a" for event in events_a)
    assert events_b == []


@pytest.mark.asyncio
async def test_async_shared_gateway_routes_retry_events_to_the_operating_queue_not_the_last_constructed_one():
    """Async sibling of the sync shared-gateway attribution test above."""
    events_a: list[QueueEvent] = []
    events_b: list[QueueEvent] = []

    async def on_event_a(event: QueueEvent) -> None:
        events_a.append(event)

    async def on_event_b(event: QueueEvent) -> None:
        events_b.append(event)

    gateway = AsyncRedisGateway(
        redis_client=_AlwaysFailEvalClient(),
        retry_budget_seconds=0,
        message_wait_interval_seconds=0,
    )
    queue_a = AsyncRedisMessageQueue("shared-gateway-a", gateway=gateway, on_event=on_event_a)
    AsyncRedisMessageQueue("shared-gateway-b", gateway=gateway, on_event=on_event_b)

    with pytest.raises(RetryBudgetExhaustedError):
        async with queue_a.process_message():
            pass

    assert _ops(events_a).count("retry_attempt") > 0
    assert _ops(events_a).count("retry_exhausted") > 0
    assert all(event.queue == "shared-gateway-a" for event in events_a)
    assert events_b == []


def _same_name_collision_warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [record.getMessage() for record in caplog.records if "share this gateway" in record.getMessage()]


def test_sync_same_name_queue_on_shared_gateway_warns_instead_of_silently_overwriting(caplog):
    """Two *live* queues with the same name share one gateway's single
    per-processing-key emitter slot, so the second construction overwrites the
    first (they are indistinguishable to the gateway, which only knows the
    processing key). Registration stays last-wins because destroy-then-
    reconstruct is legitimate, but it must warn instead of silently dropping the
    first instance's gateway-level telemetry.
    """
    client = fakeredis.FakeRedis()
    gateway = RedisGateway(redis_client=client)
    events_a: list[QueueEvent] = []
    events_b: list[QueueEvent] = []
    queue_a = RedisMessageQueue("dup-name", gateway=gateway, on_event=events_a.append)
    with caplog.at_level(logging.WARNING, logger="redis_message_queue._redis_gateway"):
        queue_b = RedisMessageQueue("dup-name", gateway=gateway, on_event=events_b.append)

    collisions = _same_name_collision_warnings(caplog)
    assert len(collisions) == 1
    assert "dup-name" in collisions[0]
    # Last-wins: the most recently constructed instance owns the slot.
    gateway._emit_event(queue_a.key.processing, "retry_attempt", "success")
    assert _ops(events_a) == []
    assert _ops(events_b) == ["retry_attempt"]
    assert queue_b.key.processing == queue_a.key.processing


def test_sync_reconstructing_same_name_queue_after_drain_does_not_warn(caplog):
    """Draining a queue unregisters its emitter, so reconstructing a queue with
    the same name afterwards is a legitimate recycle and must not warn (the
    least-surprise half of the same-name contract)."""
    client = fakeredis.FakeRedis()
    gateway = RedisGateway(redis_client=client)
    queue_a = RedisMessageQueue("recycled", gateway=gateway)
    assert queue_a.drain() is True
    with caplog.at_level(logging.WARNING, logger="redis_message_queue._redis_gateway"):
        queue_b = RedisMessageQueue("recycled", gateway=gateway)

    assert _same_name_collision_warnings(caplog) == []
    assert queue_b.key.processing in gateway._event_emitters


@pytest.mark.asyncio
async def test_async_same_name_queue_on_shared_gateway_warns_instead_of_silently_overwriting(caplog):
    """Async sibling of the sync same-name collision warning test."""
    client = fakeredis.FakeAsyncRedis()
    gateway = AsyncRedisGateway(redis_client=client)
    events_a: list[QueueEvent] = []
    events_b: list[QueueEvent] = []

    async def on_event_a(event: QueueEvent) -> None:
        events_a.append(event)

    async def on_event_b(event: QueueEvent) -> None:
        events_b.append(event)

    queue_a = AsyncRedisMessageQueue("dup-name", gateway=gateway, on_event=on_event_a)
    with caplog.at_level(logging.WARNING, logger="redis_message_queue.asyncio._redis_gateway"):
        queue_b = AsyncRedisMessageQueue("dup-name", gateway=gateway, on_event=on_event_b)

    collisions = _same_name_collision_warnings(caplog)
    assert len(collisions) == 1
    assert "dup-name" in collisions[0]
    await gateway._emit_event(queue_a.key.processing, "retry_attempt", "success")
    assert _ops(events_a) == []
    assert _ops(events_b) == ["retry_attempt"]
    assert queue_b is not None


@pytest.mark.asyncio
async def test_async_reconstructing_same_name_queue_after_drain_does_not_warn(caplog):
    """Async sibling of the reconstruct-after-drain no-warning test."""
    client = fakeredis.FakeAsyncRedis()
    gateway = AsyncRedisGateway(redis_client=client)
    queue_a = AsyncRedisMessageQueue("recycled", gateway=gateway)
    assert await queue_a.drain() is True
    with caplog.at_level(logging.WARNING, logger="redis_message_queue.asyncio._redis_gateway"):
        queue_b = AsyncRedisMessageQueue("recycled", gateway=gateway)

    assert _same_name_collision_warnings(caplog) == []
    assert queue_b.key.processing in gateway._event_emitters


# OH-C2-F1 pins the lost-reply cache-replay observability gap that the sibling
# test above does NOT cover (it asserts `dlq` on a *clean* claim and `retry_*` on
# a *total* eval failure, but never partial-success-then-lost-reply).
_DLQ_KEY = "observed::dlq"


def _build_dead_lettering_vt_queue(client: fakeredis.FakeRedis, events: list[QueueEvent]) -> RedisMessageQueue:
    """A visibility-timeout queue whose next claim dead-letters a poison message
    then claims a live one in a single EVAL.

    pending = [real, poison]; the VT-claim Lua LMOVEs from the RIGHT, so poison is
    popped first (seeded delivery_count 2 -> HINCRBY 3 > max_delivery_count 2 ->
    LPUSH dlq + LREM processing), then real is popped and claimed. The
    delivery_counts hash field is the envelope-encoded stored value (what LMOVE
    moves), so it must be the SAME encoded bytes that were pushed.
    encode_stored_message mints a fresh random envelope id per call, so each
    message is encoded once and reused for both the push and the delivery_counts
    seed.
    """
    gateway = RedisGateway(
        redis_client=client,
        message_visibility_timeout_seconds=30,
        max_delivery_count=2,
        dead_letter_queue=_DLQ_KEY,
        message_wait_interval_seconds=0,
        retry_budget_seconds=5,
    )
    queue = RedisMessageQueue("observed", gateway=gateway, on_event=events.append)
    real = encode_stored_message("real-payload")
    poison = encode_stored_message("poison-payload")
    client.rpush(queue.key.pending, real)
    client.rpush(queue.key.pending, poison)
    client.hset(gateway._delivery_counts_key(queue.key.processing), poison, 2)
    return queue


def test_sync_lost_reply_claim_replay_drops_dlq_event():
    """Pins OH-C2-F1: a lost-reply retry of a dead-lettering visibility-timeout
    claim emits the ``dlq`` telemetry event ZERO times, while the poison message
    is still correctly dead-lettered and the live message correctly claimed.

    THIS IS A KNOWN, INTENTIONAL, OBSERVABILITY-ONLY CONTRACT -- NOT A BUG.

    The VT-claim Lua emits ``dlq``/``claim_reclaim`` as side-channel data in its
    return value (``result[3]``/``result[2]``), consumed post-commit by the Python
    wrapper. When the first EVAL commits server-side (poison -> ``LPUSH dlq`` +
    ``LREM processing``; live message claimed; ``claim_result`` cache SET) but the
    reply is lost (retryable ``ConnectionError``), the wrapper retries with the
    SAME ``claim_id``. The retry hits the ``claim_result`` cache-replay
    (``_config.py`` ``GET KEYS[8]`` branch), which returns only a 2-element array
    ``{claim[1], claim[2]}`` carrying NO reclaimed/dead-lettered events. The emit
    site (``_redis_gateway.py`` ``_claim_visible_message``) then defaults both
    attempt lists to ``[]`` because ``len(result) <= 2``, so no ``dlq`` event
    fires. The loss is permanent: the message is now in the DLQ and will never
    re-trigger a ``dlq`` event.

    State stays correct (no data loss / no double-processing). This sits within
    the documented telemetry-only contract: ``dlq`` is a post-commit event and
    ``on_event`` is telemetry, not a saga/follow-up-write hook. We deliberately do
    NOT change production code for this LOW edge -- this test PINS the behavior so
    a future reader sees it is a chosen contract, not an oversight. If DLQ-event
    reliability ever becomes a goal (e.g. persisting the events into the
    ``claim_result`` cache payload so the replay can re-emit), THIS is the test to
    update. See finding OH-C2-F1.
    """
    # CONTROL: a clean dead-lettering claim emits `dlq` exactly once.
    control_events: list[QueueEvent] = []
    control_client = fakeredis.FakeRedis(decode_responses=True)
    control_queue = _build_dead_lettering_vt_queue(control_client, control_events)
    control_claim = control_queue._redis.wait_for_message_and_move(
        control_queue.key.pending, control_queue.key.processing
    )
    assert isinstance(control_claim, ClaimedMessage)
    assert "real" in control_claim.stored_message  # live message claimed, not the poison
    assert control_client.llen(_DLQ_KEY) == 1  # poison dead-lettered
    assert _ops(control_events).count("dlq") == 1

    # FAULT: first EVAL commits server-side, then the reply is "lost"; the retry
    # with the same claim_id hits the cache-replay, which carries no events.
    fault_events: list[QueueEvent] = []
    fault_client = fakeredis.FakeRedis(decode_responses=True)
    fault_queue = _build_dead_lettering_vt_queue(fault_client, fault_events)
    real_eval = fault_client.eval
    eval_calls = {"n": 0}

    def faulty_eval(*args, **kwargs):
        result = real_eval(*args, **kwargs)  # server-side COMMIT happens here
        eval_calls["n"] += 1
        if eval_calls["n"] == 1:
            raise redis.exceptions.ConnectionError("simulated lost reply after server commit")
        return result

    fault_client.eval = faulty_eval
    fault_claim = fault_queue._redis.wait_for_message_and_move(fault_queue.key.pending, fault_queue.key.processing)

    assert eval_calls["n"] == 2  # 1 lost reply + 1 cache-replay retry
    assert isinstance(fault_claim, ClaimedMessage)
    assert "real" in fault_claim.stored_message  # live message still correctly claimed
    assert fault_client.llen(_DLQ_KEY) == 1  # poison still correctly dead-lettered (state OK)
    assert _ops(fault_events).count("dlq") == 0  # OH-C2-F1: telemetry permanently lost on replay


@pytest.mark.asyncio
async def test_async_lost_reply_claim_replay_drops_dlq_event():
    """Async parity for OH-C2-F1 (see ``test_sync_lost_reply_claim_replay_drops_dlq_event``
    for the full contract). The Lua is shared and the async emit site
    (``asyncio/_redis_gateway.py`` ``_claim_visible_message``) is structurally
    identical to the sync one, so the lost-reply cache-replay drops the ``dlq``
    event on the async gateway too. This is the same KNOWN, INTENTIONAL,
    observability-only contract: state stays correct (poison dead-lettered, live
    message claimed); only the post-commit telemetry signal is lost on the replay.
    Pins dlq == 0; update alongside the sync test if event reliability becomes a
    goal. See finding OH-C2-F1.
    """
    fault_events: list[QueueEvent] = []

    async def on_event(event: QueueEvent) -> None:
        fault_events.append(event)

    client = fakeredis.FakeAsyncRedis(decode_responses=True)
    gateway = AsyncRedisGateway(
        redis_client=client,
        message_visibility_timeout_seconds=30,
        max_delivery_count=2,
        dead_letter_queue=_DLQ_KEY,
        message_wait_interval_seconds=0,
        retry_budget_seconds=5,
    )
    queue = AsyncRedisMessageQueue("observed", gateway=gateway, on_event=on_event)
    real = encode_stored_message("real-payload")
    poison = encode_stored_message("poison-payload")
    await client.rpush(queue.key.pending, real)
    await client.rpush(queue.key.pending, poison)
    await client.hset(gateway._delivery_counts_key(queue.key.processing), poison, 2)

    real_eval = client.eval
    eval_calls = {"n": 0}

    async def faulty_eval(*args, **kwargs):
        result = await real_eval(*args, **kwargs)  # server-side COMMIT happens here
        eval_calls["n"] += 1
        if eval_calls["n"] == 1:
            raise redis.exceptions.ConnectionError("simulated lost reply after server commit")
        return result

    client.eval = faulty_eval
    claim = await queue._redis.wait_for_message_and_move(queue.key.pending, queue.key.processing)

    assert eval_calls["n"] == 2  # 1 lost reply + 1 cache-replay retry
    assert isinstance(claim, ClaimedMessage)
    assert "real" in claim.stored_message  # live message still correctly claimed
    assert (await client.llen(_DLQ_KEY)) == 1  # poison still correctly dead-lettered (state OK)
    assert _ops(fault_events).count("dlq") == 0  # OH-C2-F1: telemetry permanently lost on replay


def test_event_callback_exception_is_warned_not_propagated():
    client = fakeredis.FakeRedis()

    def fail(_event: QueueEvent) -> None:
        raise RuntimeError("observer down")

    queue = RedisMessageQueue("observed", client=client, on_event=fail)
    with pytest.warns(RuntimeWarning, match="on_event callback raised RuntimeError"):
        assert queue.publish("hello") is True
    assert client.llen(queue.key.pending) == 1


def test_event_callback_exception_warning_error_filter_does_not_escape_publish():
    client = fakeredis.FakeRedis()

    def fail(_event: QueueEvent) -> None:
        raise RuntimeError("observer down")

    queue = RedisMessageQueue("observed", client=client, on_event=fail)
    with pytest.warns(RuntimeWarning, match="on_event callback raised RuntimeError"):
        with warnings.catch_warnings():
            warnings.simplefilter("error", RuntimeWarning)
            assert queue.publish("hello") is True
    assert client.llen(queue.key.pending) == 1


def test_event_callback_cancelled_error_is_warned_not_propagated_after_claim():
    client = fakeredis.FakeRedis()

    def fail_on_claim_success(event: QueueEvent) -> None:
        if event.operation is EventOperation.CLAIM and event.outcome is EventOutcome.SUCCESS:
            raise asyncio.CancelledError("telemetry cancelled")

    queue = RedisMessageQueue(
        "observed",
        client=client,
        visibility_timeout_seconds=None,
        max_delivery_count=None,
        on_event=fail_on_claim_success,
    )
    assert queue.publish("hello") is True

    with pytest.warns(RuntimeWarning, match="on_event callback raised CancelledError"):
        with queue.process_message() as message:
            assert message == b"hello"

    assert client.llen(queue.key.pending) == 0
    assert client.llen(queue.key.processing) == 0


def test_exception_hierarchy_subclasses_builtin_bases():
    with pytest.raises(ValueError) as config_exc:
        RedisMessageQueue("", client=fakeredis.FakeRedis())
    assert isinstance(config_exc.value, ConfigurationError)
    assert isinstance(config_exc.value, RedisMessageQueueError)

    class BadGateway(_Gateway):
        def publish_message(self, queue: str, message: str, dedup_key: str) -> bool:
            return "yes"

    queue = RedisMessageQueue(
        "observed",
        gateway=BadGateway(),
        deduplication=True,
        get_deduplication_key=lambda msg: msg,
    )
    with pytest.raises(TypeError) as contract_exc:
        queue.publish("hello")
    assert isinstance(contract_exc.value, GatewayContractError)

    client = fakeredis.FakeRedis()
    client.set("observed::pending", "not a list")
    queue = RedisMessageQueue(
        "observed",
        client=client,
        deduplication=True,
        get_deduplication_key=lambda msg: msg,
    )
    with pytest.raises(redis.exceptions.ResponseError) as lua_exc:
        queue.publish("hello")
    assert isinstance(lua_exc.value, LuaScriptError)
    assert issubclass(RetryBudgetExhaustedError, redis.exceptions.RedisError)
    assert issubclass(CleanupFailedError, RedisMessageQueueError)


def test_observability_examples_import():
    importlib.import_module("examples.production.observability")
    importlib.import_module("examples.production.asyncio.observability")


def test_sync_observability_example_returns_closable_resources(monkeypatch):
    module = importlib.import_module("examples.production.observability")

    class ClosingFakeRedis(fakeredis.FakeRedis):
        was_closed = False

        def close(self, *args, **kwargs):
            self.was_closed = True
            return super().close(*args, **kwargs)

    client = ClosingFakeRedis(decode_responses=True)
    from_url_calls: list[tuple[str, dict[str, object]]] = []

    class FakeRedisFactory:
        @classmethod
        def from_url(cls, url: str, **kwargs):
            from_url_calls.append((url, kwargs))
            return client

    monkeypatch.setattr(module.redis, "Redis", FakeRedisFactory)

    resources = module.make_queue(queue_name="jobs", url="redis://example.invalid/0")

    assert resources.client is client
    assert isinstance(resources.queue, RedisMessageQueue)
    assert from_url_calls == [
        (
            "redis://example.invalid/0",
            {"retry": None, "max_connections": module.REDIS_MAX_CONNECTIONS},
        )
    ]
    assert resources.close() is True
    assert client.was_closed is True


@pytest.mark.asyncio
async def test_async_observability_example_returns_closable_resources(monkeypatch):
    module = importlib.import_module("examples.production.asyncio.observability")

    class ClosingFakeAsyncRedis(fakeredis.FakeAsyncRedis):
        was_closed = False

        async def aclose(self, *args, **kwargs):
            self.was_closed = True
            return await super().aclose(*args, **kwargs)

    client = ClosingFakeAsyncRedis(decode_responses=True)
    from_url_calls: list[tuple[str, dict[str, object]]] = []

    class FakeRedisFactory:
        @classmethod
        def from_url(cls, url: str, **kwargs):
            from_url_calls.append((url, kwargs))
            return client

    monkeypatch.setattr(module.redis, "Redis", FakeRedisFactory)

    resources = module.make_queue(queue_name="jobs", url="redis://example.invalid/0")

    assert resources.client is client
    assert from_url_calls == [
        (
            "redis://example.invalid/0",
            {"retry": None, "max_connections": module.REDIS_MAX_CONNECTIONS},
        )
    ]
    assert await resources.aclose() is True
    assert client.was_closed is True
