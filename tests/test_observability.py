import importlib
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
from redis_message_queue._stored_message import ClaimedMessage, MessageData, encode_stored_message
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

    def wait_for_message_and_move(self, from_queue: str, to_queue: str) -> ClaimedMessage | MessageData | None:
        return ClaimedMessage(self.message, "lease-token")

    def remove_message(self, queue: str, message: MessageData, *, lease_token: str | None = None) -> bool:
        if self.fail_remove:
            raise ConnectionError("cleanup failed")
        return self.remove_return

    def move_message(
        self,
        from_queue: str,
        to_queue: str,
        message: MessageData,
        *,
        lease_token: str | None = None,
    ) -> bool:
        return self.move_return

    def renew_message_lease(self, queue: str, message: MessageData, lease_token: str, **_kwargs) -> bool:
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
    queue = RedisMessageQueue("observed", client=client, enable_completed_queue=True, on_event=events.append)

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


def test_exception_hierarchy_preserves_compatibility_catches():
    with pytest.raises(ValueError) as config_exc:
        RedisMessageQueue("", client=fakeredis.FakeRedis())
    assert isinstance(config_exc.value, ConfigurationError)
    assert isinstance(config_exc.value, RedisMessageQueueError)

    class BadGateway(_Gateway):
        def publish_message(self, queue: str, message: str, dedup_key: str) -> bool:
            return "yes"

    queue = RedisMessageQueue("observed", gateway=BadGateway())
    with pytest.raises(TypeError) as contract_exc:
        queue.publish("hello")
    assert isinstance(contract_exc.value, GatewayContractError)

    client = fakeredis.FakeRedis()
    client.set("observed::pending", "not a list")
    queue = RedisMessageQueue("observed", client=client)
    with pytest.raises(redis.exceptions.ResponseError) as lua_exc:
        queue.publish("hello")
    assert isinstance(lua_exc.value, LuaScriptError)
    assert issubclass(RetryBudgetExhaustedError, redis.exceptions.RedisError)
    assert issubclass(CleanupFailedError, RedisMessageQueueError)


def test_observability_examples_import():
    importlib.import_module("examples.production.observability")
    importlib.import_module("examples.production.asyncio.observability")
