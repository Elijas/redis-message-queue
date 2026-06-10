import queue as thread_queue
import threading
import time

import fakeredis
import pytest
import redis

from redis_message_queue import ConfigurationError, QueueBackpressureError, RedisMessageQueue
from redis_message_queue import _redis_gateway as sync_gateway_module
from redis_message_queue._config import DEFAULT_PENDING_OVERLOAD_BLOCK_TIMEOUT_SECONDS
from redis_message_queue._exceptions import RetryBudgetExhaustedError
from redis_message_queue.asyncio import RedisMessageQueue as AsyncRedisMessageQueue
from redis_message_queue.asyncio import _redis_gateway as async_gateway_module
from redis_message_queue.interrupt_handler._interface import BaseGracefulInterruptHandler

DROP_OLDEST_DEDUP_MATCH = "drop_oldest.*deduplication.*silently suppressed"


class _FlagInterrupt(BaseGracefulInterruptHandler):
    """Programmable interrupt handler for backpressure block-wait tests."""

    def __init__(self) -> None:
        self.flag = False

    def is_interrupted(self) -> bool:
        return self.flag


def _should_inject(eval_calls, raise_on, raise_every):
    if eval_calls in raise_on:
        return True
    return raise_every is not None and eval_calls % raise_every == 0


class _BlockingEvalClient:
    """Duck-typed proxy over a real client whose ``eval`` always reports the
    pending queue as overloaded (the ``-1`` block sentinel), optionally raising
    a transient ``ConnectionError`` on chosen call indices (``raise_on``) and/or
    on every ``raise_every``-th call, to drive tenacity retries mid block-wait.
    Non-eval calls fall through to the wrapped client.
    """

    def __init__(self, wrapped, *, raise_on=(), raise_every=None):
        self._wrapped = wrapped
        self._raise_on = set(raise_on)
        self._raise_every = raise_every
        self.eval_calls = 0

    def eval(self, *args):
        self.eval_calls += 1
        if _should_inject(self.eval_calls, self._raise_on, self._raise_every):
            raise redis.exceptions.ConnectionError("injected transient error mid block-wait")
        return sync_gateway_module.PENDING_OVERLOAD_LUA_SENTINEL

    def __getattr__(self, name):
        return getattr(self._wrapped, name)


class _AsyncBlockingEvalClient:
    """Async twin of ``_BlockingEvalClient``."""

    def __init__(self, wrapped, *, raise_on=(), raise_every=None):
        self._wrapped = wrapped
        self._raise_on = set(raise_on)
        self._raise_every = raise_every
        self.eval_calls = 0

    async def eval(self, *args):
        self.eval_calls += 1
        if _should_inject(self.eval_calls, self._raise_on, self._raise_every):
            raise redis.exceptions.ConnectionError("injected transient error mid block-wait")
        return async_gateway_module.PENDING_OVERLOAD_LUA_SENTINEL

    def __getattr__(self, name):
        return getattr(self._wrapped, name)


def _run_sync_block_wait(monkeypatch, block_timeout_seconds):
    gateway = sync_gateway_module.RedisGateway(
        redis_client=fakeredis.FakeRedis(),
        max_pending_length=1,
        pending_overload_policy="block",
        pending_overload_block_timeout_seconds=block_timeout_seconds,
    )
    now = 0.0
    polls = 0
    sleeps = []

    def fake_monotonic():
        return now

    def fake_backoff_sleep(duration):
        # Record the per-poll backoff and advance the fake clock by the full
        # duration. The interrupt-slicing inside the real backoff sleep is
        # covered separately by the interrupt-abort tests; here we assert the
        # backoff schedule against a single conceptual sleep per poll.
        nonlocal now
        sleeps.append(duration)
        now += duration
        return False

    def overloaded_operation():
        nonlocal polls
        polls += 1
        return sync_gateway_module.PENDING_OVERLOAD_LUA_SENTINEL

    monkeypatch.setattr(sync_gateway_module.random, "random", lambda: 0.5)
    monkeypatch.setattr(sync_gateway_module.time, "monotonic", fake_monotonic)
    monkeypatch.setattr(gateway, "_sleep_pending_overload_backoff", fake_backoff_sleep)

    deadline = gateway._pending_block_deadline()
    with pytest.raises(QueueBackpressureError, match="stayed at max_pending_length=1"):
        gateway._run_pending_backpressure_operation("pending", overloaded_operation, deadline_monotonic=deadline)

    return polls, sleeps, now


class _FakeLoop:
    def __init__(self):
        self.now = 0.0

    def time(self):
        return self.now


async def _run_async_block_wait(monkeypatch, block_timeout_seconds):
    gateway = async_gateway_module.RedisGateway(
        redis_client=fakeredis.FakeAsyncRedis(),
        max_pending_length=1,
        pending_overload_policy="block",
        pending_overload_block_timeout_seconds=block_timeout_seconds,
    )
    fake_loop = _FakeLoop()
    polls = 0
    sleeps = []

    async def fake_backoff_sleep(duration):
        # Record the per-poll backoff and advance the fake loop clock by the
        # full duration. The interrupt-slicing inside the real backoff sleep is
        # covered separately by the interrupt-abort tests.
        sleeps.append(duration)
        fake_loop.now += duration
        return False

    async def overloaded_operation():
        nonlocal polls
        polls += 1
        return async_gateway_module.PENDING_OVERLOAD_LUA_SENTINEL

    monkeypatch.setattr(async_gateway_module.random, "random", lambda: 0.5)
    monkeypatch.setattr(async_gateway_module.asyncio, "get_running_loop", lambda: fake_loop)
    monkeypatch.setattr(gateway, "_sleep_pending_overload_backoff", fake_backoff_sleep)

    deadline = gateway._pending_block_deadline()
    with pytest.raises(QueueBackpressureError, match="stayed at max_pending_length=1"):
        await gateway._run_pending_backpressure_operation("pending", overloaded_operation, deadline_monotonic=deadline)

    return polls, sleeps, fake_loop.now


@pytest.mark.parametrize("deduplication", [True, False])
def test_sync_raise_policy_rejects_overload(deduplication):
    client = fakeredis.FakeRedis()
    kwargs = {"get_deduplication_key": lambda msg: msg} if deduplication else {}
    queue = RedisMessageQueue(
        "bp-sync-raise",
        client=client,
        deduplication=deduplication,
        max_pending_length=1,
        **kwargs,
    )

    assert queue.publish("first") is True
    with pytest.raises(QueueBackpressureError, match="max_pending_length=1") as caught:
        queue.publish("second")
    assert "consider increasing `max_pending_length`" in str(caught.value)
    assert "pending_overload_policy='block'" in str(caught.value)
    assert "adding consumer capacity" in str(caught.value)

    assert client.llen(queue.key.pending) == 1


@pytest.mark.parametrize("policy", ["drop_oldest", "block"])
def test_sync_overload_policies(policy):
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue(
        "bp-sync-policy",
        client=client,
        deduplication=False,
        max_delivery_count=None,
        max_pending_length=1,
        pending_overload_policy=policy,
        pending_overload_block_timeout_seconds=0.01,
    )

    assert queue.publish("first") is True
    if policy == "drop_oldest":
        assert queue.publish("second") is True
        expected = b"second"
    else:
        with pytest.raises(QueueBackpressureError, match="stayed at max_pending_length=1"):
            queue.publish("second")
        expected = b"first"
    with queue.process_message() as message:
        assert message == expected
    assert client.llen(queue.key.pending) == 0


@pytest.mark.parametrize(
    "kwargs",
    [
        {"deduplication": True, "get_deduplication_key": lambda message: "fixed"},
        {"deduplication": False, "get_deduplication_key": lambda message: "fixed"},
    ],
)
def test_sync_drop_oldest_rejects_deduplication(kwargs):
    client = fakeredis.FakeRedis()

    with pytest.raises(ConfigurationError, match=DROP_OLDEST_DEDUP_MATCH):
        RedisMessageQueue(
            "bp-sync-drop-oldest-dedup",
            client=client,
            max_delivery_count=None,
            max_pending_length=1,
            pending_overload_policy="drop_oldest",
            **kwargs,
        )


def test_sync_drop_oldest_requires_pending_cap():
    client = fakeredis.FakeRedis()

    with pytest.raises(ConfigurationError, match="drop_oldest requires max_pending_length to be set"):
        RedisMessageQueue(
            "bp-sync-drop-oldest-no-cap",
            client=client,
            deduplication=False,
            max_delivery_count=None,
            pending_overload_policy="drop_oldest",
        )


def test_sync_block_requires_pending_cap():
    client = fakeredis.FakeRedis()

    with pytest.raises(ConfigurationError, match="block requires max_pending_length to be set"):
        RedisMessageQueue(
            "bp-sync-block-no-cap",
            client=client,
            deduplication=False,
            pending_overload_policy="block",
        )

    # block WITH a cap stays valid: the cap defines the threshold to block on.
    RedisMessageQueue(
        "bp-sync-block-with-cap",
        client=client,
        deduplication=False,
        max_pending_length=1,
        pending_overload_policy="block",
    )


def test_sync_drop_oldest_rejects_max_delivery_count():
    client = fakeredis.FakeRedis()

    with pytest.raises(ConfigurationError, match="drop_oldest is incompatible with max_delivery_count"):
        RedisMessageQueue(
            "bp-sync-drop-oldest-dlq",
            client=client,
            deduplication=False,
            max_pending_length=1,
            max_delivery_count=1,
            pending_overload_policy="drop_oldest",
        )


def test_sync_gateway_drop_oldest_rejects_max_delivery_count():
    client = fakeredis.FakeRedis()

    with pytest.raises(ConfigurationError, match="drop_oldest is incompatible with max_delivery_count"):
        sync_gateway_module.RedisGateway(
            redis_client=client,
            max_pending_length=1,
            max_delivery_count=1,
            dead_letter_queue="bp-sync-gateway-drop-oldest-dlq:dead",
            message_visibility_timeout_seconds=10,
            pending_overload_policy="drop_oldest",
        )


def test_sync_gateway_drop_oldest_rejects_deduplicated_publish():
    client = fakeredis.FakeRedis()
    gateway = sync_gateway_module.RedisGateway(
        redis_client=client,
        max_pending_length=1,
        message_visibility_timeout_seconds=None,
        pending_overload_policy="drop_oldest",
    )

    with pytest.raises(ConfigurationError, match=DROP_OLDEST_DEDUP_MATCH):
        gateway.publish_message("bp-sync-gateway-dedup:pending", "payload", "bp-sync-gateway-dedup:dedup")


def test_sync_gateway_drop_oldest_allows_non_deduplicated_add_message():
    client = fakeredis.FakeRedis()
    gateway = sync_gateway_module.RedisGateway(
        redis_client=client,
        max_pending_length=1,
        message_visibility_timeout_seconds=None,
        pending_overload_policy="drop_oldest",
    )
    queue_name = "bp-sync-gateway-add:pending"

    gateway.add_message(queue_name, "first")
    gateway.add_message(queue_name, "second")

    assert client.llen(queue_name) == 1


def test_sync_block_policy_backs_off_during_extended_wait(monkeypatch):
    polls, sleeps, elapsed = _run_sync_block_wait(monkeypatch, 0.5)

    assert elapsed == pytest.approx(0.5)
    assert len(sleeps) == polls - 1
    assert polls < 20
    assert sleeps[:4] == pytest.approx([0.01, 0.02, 0.04, 0.05])


def test_sync_block_policy_caps_backoff_to_timeout_fraction_and_500ms(monkeypatch):
    _, tight_sleeps, _ = _run_sync_block_wait(monkeypatch, 0.1)
    _, wide_sleeps, _ = _run_sync_block_wait(monkeypatch, 10.0)

    assert max(tight_sleeps) == pytest.approx(0.01)
    assert max(wide_sleeps) == pytest.approx(0.5)


def test_sync_block_policy_zero_timeout_tries_once(monkeypatch):
    polls, sleeps, elapsed = _run_sync_block_wait(monkeypatch, 0)

    assert polls == 1
    assert sleeps == []
    assert elapsed == 0


@pytest.mark.parametrize("timeout", [-1, float("nan"), float("inf")])
def test_sync_rejects_invalid_block_timeout(timeout):
    client = fakeredis.FakeRedis()

    with pytest.raises(ConfigurationError, match="pending_overload_block_timeout_seconds"):
        RedisMessageQueue(
            "bp-sync-invalid-timeout",
            client=client,
            pending_overload_block_timeout_seconds=timeout,
        )


@pytest.mark.parametrize("max_pending_length", [0, -1])
def test_sync_rejects_non_positive_pending_cap(max_pending_length):
    client = fakeredis.FakeRedis()

    with pytest.raises(ConfigurationError, match="max_pending_length"):
        RedisMessageQueue(
            "bp-sync-invalid-cap",
            client=client,
            max_pending_length=max_pending_length,
        )


def test_sync_full_queue_dedup_hit_does_not_count_as_overload():
    client = fakeredis.FakeRedis()
    queue = RedisMessageQueue(
        "bp-sync-dedup-hit",
        client=client,
        deduplication=True,
        get_deduplication_key=lambda msg: msg,
        max_pending_length=1,
    )

    assert queue.publish("same") is True
    assert queue.publish("same") is False
    assert client.llen(queue.key.pending) == 1


def test_sync_backpressure_emits_failure_event_before_reraising():
    client = fakeredis.FakeRedis()
    events = []
    queue = RedisMessageQueue(
        "bp-sync-event",
        client=client,
        deduplication=False,
        max_pending_length=1,
        on_event=events.append,
    )

    assert queue.publish("first") is True
    with pytest.raises(QueueBackpressureError):
        queue.publish("second")

    assert events[-1].operation == "publish"
    assert events[-1].outcome == "failure"
    assert events[-1].exception_type == "QueueBackpressureError"


@pytest.mark.asyncio
@pytest.mark.parametrize("deduplication", [True, False])
async def test_async_raise_policy_rejects_overload(deduplication):
    client = fakeredis.FakeAsyncRedis()
    kwargs = {"get_deduplication_key": lambda msg: msg} if deduplication else {}
    queue = AsyncRedisMessageQueue(
        "bp-async-raise",
        client=client,
        deduplication=deduplication,
        max_pending_length=1,
        **kwargs,
    )

    assert await queue.publish("first") is True
    with pytest.raises(QueueBackpressureError, match="max_pending_length=1"):
        await queue.publish("second")

    assert await client.llen(queue.key.pending) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("policy", ["drop_oldest", "block"])
async def test_async_overload_policies(policy):
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue(
        "bp-async-policy",
        client=client,
        deduplication=False,
        max_delivery_count=None,
        max_pending_length=1,
        pending_overload_policy=policy,
        pending_overload_block_timeout_seconds=0.01,
    )

    assert await queue.publish("first") is True
    if policy == "drop_oldest":
        assert await queue.publish("second") is True
        expected = b"second"
    else:
        with pytest.raises(QueueBackpressureError, match="stayed at max_pending_length=1"):
            await queue.publish("second")
        expected = b"first"
    async with queue.process_message() as message:
        assert message == expected
    assert await client.llen(queue.key.pending) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kwargs",
    [
        {"deduplication": True, "get_deduplication_key": lambda message: "fixed"},
        {"deduplication": False, "get_deduplication_key": lambda message: "fixed"},
    ],
)
async def test_async_drop_oldest_rejects_deduplication(kwargs):
    client = fakeredis.FakeAsyncRedis()

    with pytest.raises(ConfigurationError, match=DROP_OLDEST_DEDUP_MATCH):
        AsyncRedisMessageQueue(
            "bp-async-drop-oldest-dedup",
            client=client,
            max_delivery_count=None,
            max_pending_length=1,
            pending_overload_policy="drop_oldest",
            **kwargs,
        )


@pytest.mark.asyncio
async def test_async_drop_oldest_requires_pending_cap():
    client = fakeredis.FakeAsyncRedis()

    with pytest.raises(ConfigurationError, match="drop_oldest requires max_pending_length to be set"):
        AsyncRedisMessageQueue(
            "bp-async-drop-oldest-no-cap",
            client=client,
            deduplication=False,
            max_delivery_count=None,
            pending_overload_policy="drop_oldest",
        )


@pytest.mark.asyncio
async def test_async_block_requires_pending_cap():
    client = fakeredis.FakeAsyncRedis()

    with pytest.raises(ConfigurationError, match="block requires max_pending_length to be set"):
        AsyncRedisMessageQueue(
            "bp-async-block-no-cap",
            client=client,
            deduplication=False,
            pending_overload_policy="block",
        )

    # block WITH a cap stays valid: the cap defines the threshold to block on.
    AsyncRedisMessageQueue(
        "bp-async-block-with-cap",
        client=client,
        deduplication=False,
        max_pending_length=1,
        pending_overload_policy="block",
    )


@pytest.mark.asyncio
async def test_async_drop_oldest_rejects_max_delivery_count():
    client = fakeredis.FakeAsyncRedis()

    with pytest.raises(ConfigurationError, match="drop_oldest is incompatible with max_delivery_count"):
        AsyncRedisMessageQueue(
            "bp-async-drop-oldest-dlq",
            client=client,
            deduplication=False,
            max_pending_length=1,
            max_delivery_count=1,
            pending_overload_policy="drop_oldest",
        )


def test_async_gateway_drop_oldest_rejects_max_delivery_count():
    client = fakeredis.FakeAsyncRedis()

    with pytest.raises(ConfigurationError, match="drop_oldest is incompatible with max_delivery_count"):
        async_gateway_module.RedisGateway(
            redis_client=client,
            max_pending_length=1,
            max_delivery_count=1,
            dead_letter_queue="bp-async-gateway-drop-oldest-dlq:dead",
            message_visibility_timeout_seconds=10,
            pending_overload_policy="drop_oldest",
        )


@pytest.mark.asyncio
async def test_async_gateway_drop_oldest_rejects_deduplicated_publish():
    client = fakeredis.FakeAsyncRedis()
    gateway = async_gateway_module.RedisGateway(
        redis_client=client,
        max_pending_length=1,
        message_visibility_timeout_seconds=None,
        pending_overload_policy="drop_oldest",
    )

    with pytest.raises(ConfigurationError, match=DROP_OLDEST_DEDUP_MATCH):
        await gateway.publish_message("bp-async-gateway-dedup:pending", "payload", "bp-async-gateway-dedup:dedup")


@pytest.mark.asyncio
async def test_async_gateway_drop_oldest_allows_non_deduplicated_add_message():
    client = fakeredis.FakeAsyncRedis()
    gateway = async_gateway_module.RedisGateway(
        redis_client=client,
        max_pending_length=1,
        message_visibility_timeout_seconds=None,
        pending_overload_policy="drop_oldest",
    )
    queue_name = "bp-async-gateway-add:pending"

    await gateway.add_message(queue_name, "first")
    await gateway.add_message(queue_name, "second")

    assert await client.llen(queue_name) == 1


@pytest.mark.asyncio
async def test_async_block_policy_backs_off_during_extended_wait(monkeypatch):
    polls, sleeps, elapsed = await _run_async_block_wait(monkeypatch, 0.5)

    assert elapsed == pytest.approx(0.5)
    assert len(sleeps) == polls - 1
    assert polls < 20
    assert sleeps[:4] == pytest.approx([0.01, 0.02, 0.04, 0.05])


@pytest.mark.asyncio
async def test_async_block_policy_caps_backoff_to_timeout_fraction_and_500ms(monkeypatch):
    _, tight_sleeps, _ = await _run_async_block_wait(monkeypatch, 0.1)
    _, wide_sleeps, _ = await _run_async_block_wait(monkeypatch, 10.0)

    assert max(tight_sleeps) == pytest.approx(0.01)
    assert max(wide_sleeps) == pytest.approx(0.5)


@pytest.mark.asyncio
async def test_async_block_policy_zero_timeout_tries_once(monkeypatch):
    polls, sleeps, elapsed = await _run_async_block_wait(monkeypatch, 0)

    assert polls == 1
    assert sleeps == []
    assert elapsed == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("timeout", [-1, float("nan"), float("inf")])
async def test_async_rejects_invalid_block_timeout(timeout):
    client = fakeredis.FakeAsyncRedis()

    with pytest.raises(ConfigurationError, match="pending_overload_block_timeout_seconds"):
        AsyncRedisMessageQueue(
            "bp-async-invalid-timeout",
            client=client,
            pending_overload_block_timeout_seconds=timeout,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("max_pending_length", [0, -1])
async def test_async_rejects_non_positive_pending_cap(max_pending_length):
    client = fakeredis.FakeAsyncRedis()

    with pytest.raises(ConfigurationError, match="max_pending_length"):
        AsyncRedisMessageQueue(
            "bp-async-invalid-cap",
            client=client,
            max_pending_length=max_pending_length,
        )


@pytest.mark.asyncio
async def test_async_full_queue_dedup_hit_does_not_count_as_overload():
    client = fakeredis.FakeAsyncRedis()
    queue = AsyncRedisMessageQueue(
        "bp-async-dedup-hit",
        client=client,
        deduplication=True,
        get_deduplication_key=lambda msg: msg,
        max_pending_length=1,
    )

    assert await queue.publish("same") is True
    assert await queue.publish("same") is False
    assert await client.llen(queue.key.pending) == 1


@pytest.mark.asyncio
async def test_async_backpressure_emits_failure_event_before_reraising():
    client = fakeredis.FakeAsyncRedis()
    events = []

    async def on_event(event):
        events.append(event)

    queue = AsyncRedisMessageQueue(
        "bp-async-event",
        client=client,
        deduplication=False,
        max_pending_length=1,
        on_event=on_event,
    )

    assert await queue.publish("first") is True
    with pytest.raises(QueueBackpressureError):
        await queue.publish("second")

    assert events[-1].operation == "publish"
    assert events[-1].outcome == "failure"
    assert events[-1].exception_type == "QueueBackpressureError"


@pytest.mark.integration
def test_pending_limit_is_atomic_for_concurrent_publishers(real_redis_client, real_redis_url, queue_name):
    queue = RedisMessageQueue(queue_name, client=real_redis_client, deduplication=False, max_pending_length=1)
    barrier = threading.Barrier(2)
    outcomes = thread_queue.Queue()

    def publish(index):
        client = redis.Redis.from_url(real_redis_url)
        try:
            local_queue = RedisMessageQueue(queue_name, client=client, deduplication=False, max_pending_length=1)
            barrier.wait(timeout=5)
            local_queue.publish(f"message-{index}")
            outcomes.put("published")
        except QueueBackpressureError:
            outcomes.put("backpressure")
        except BaseException as exc:
            outcomes.put(exc)
        finally:
            client.close()

    threads = [threading.Thread(target=publish, args=(index,)) for index in range(2)]
    [thread.start() for thread in threads]
    [thread.join(timeout=5) for thread in threads]

    results = [outcomes.get_nowait() for _ in range(outcomes.qsize())]
    assert not [result for result in results if isinstance(result, BaseException)]
    assert sorted(results) == ["backpressure", "published"]
    assert real_redis_client.llen(queue.key.pending) == 1


# ---------------------------------------------------------------------------
# F-config-1: gateway= must REJECT non-default queue-level backpressure params
# (pending_overload_policy / pending_overload_block_timeout_seconds) rather than
# silently ignore them, and must report the gateway incompatibility BEFORE the
# generic backpressure validator so drop_oldest+gateway no longer emits the
# contradictory "requires max_pending_length" runaround.
# ---------------------------------------------------------------------------

_GATEWAY_POLICY_INCOMPAT_MATCH = r"'pending_overload_policy' cannot be provided alongside 'gateway'"
_GATEWAY_TIMEOUT_INCOMPAT_MATCH = r"'pending_overload_block_timeout_seconds' cannot be provided alongside 'gateway'"


def _bare_sync_gateway():
    return sync_gateway_module.RedisGateway(redis_client=fakeredis.FakeRedis(), retry_budget_seconds=0)


def _bare_async_gateway():
    return async_gateway_module.RedisGateway(redis_client=fakeredis.FakeAsyncRedis(), retry_budget_seconds=0)


@pytest.mark.parametrize("policy", ["block", "drop_oldest"])
def test_sync_gateway_rejects_non_default_pending_overload_policy(policy):
    with pytest.raises(ConfigurationError, match=_GATEWAY_POLICY_INCOMPAT_MATCH):
        RedisMessageQueue("bp-sync-gateway-policy", gateway=_bare_sync_gateway(), pending_overload_policy=policy)


@pytest.mark.asyncio
@pytest.mark.parametrize("policy", ["block", "drop_oldest"])
async def test_async_gateway_rejects_non_default_pending_overload_policy(policy):
    with pytest.raises(ConfigurationError, match=_GATEWAY_POLICY_INCOMPAT_MATCH):
        AsyncRedisMessageQueue("bp-async-gateway-policy", gateway=_bare_async_gateway(), pending_overload_policy=policy)


def test_sync_gateway_rejects_non_default_block_timeout():
    with pytest.raises(ConfigurationError, match=_GATEWAY_TIMEOUT_INCOMPAT_MATCH):
        RedisMessageQueue(
            "bp-sync-gateway-timeout",
            gateway=_bare_sync_gateway(),
            pending_overload_block_timeout_seconds=99.0,
        )


@pytest.mark.asyncio
async def test_async_gateway_rejects_non_default_block_timeout():
    with pytest.raises(ConfigurationError, match=_GATEWAY_TIMEOUT_INCOMPAT_MATCH):
        AsyncRedisMessageQueue(
            "bp-async-gateway-timeout",
            gateway=_bare_async_gateway(),
            pending_overload_block_timeout_seconds=99.0,
        )


def test_sync_gateway_drop_oldest_reports_incompat_without_runaround():
    # drop_oldest+gateway used to demand max_pending_length (step 1 of a 3-step runaround the
    # gateway path then forbids); it must now fail directly with the gateway-incompat error.
    with pytest.raises(ConfigurationError, match=_GATEWAY_POLICY_INCOMPAT_MATCH) as excinfo:
        RedisMessageQueue("bp-sync-gateway-drop", gateway=_bare_sync_gateway(), pending_overload_policy="drop_oldest")
    assert "requires max_pending_length" not in str(excinfo.value)


@pytest.mark.asyncio
async def test_async_gateway_drop_oldest_reports_incompat_without_runaround():
    with pytest.raises(ConfigurationError, match=_GATEWAY_POLICY_INCOMPAT_MATCH) as excinfo:
        AsyncRedisMessageQueue(
            "bp-async-gateway-drop", gateway=_bare_async_gateway(), pending_overload_policy="drop_oldest"
        )
    assert "requires max_pending_length" not in str(excinfo.value)


def test_sync_gateway_with_default_backpressure_params_is_accepted():
    gateway = _bare_sync_gateway()
    # No backpressure params: the gateway path stays legal.
    assert RedisMessageQueue("bp-sync-gateway-default", gateway=gateway)._redis is gateway
    # Passing the implicit defaults explicitly must also stay legal (non-default-only rejection).
    queue = RedisMessageQueue(
        "bp-sync-gateway-explicit-default",
        gateway=gateway,
        pending_overload_policy="raise",
        pending_overload_block_timeout_seconds=DEFAULT_PENDING_OVERLOAD_BLOCK_TIMEOUT_SECONDS,
    )
    assert queue._redis is gateway


@pytest.mark.asyncio
async def test_async_gateway_with_default_backpressure_params_is_accepted():
    gateway = _bare_async_gateway()
    assert AsyncRedisMessageQueue("bp-async-gateway-default", gateway=gateway)._redis is gateway
    queue = AsyncRedisMessageQueue(
        "bp-async-gateway-explicit-default",
        gateway=gateway,
        pending_overload_policy="raise",
        pending_overload_block_timeout_seconds=DEFAULT_PENDING_OVERLOAD_BLOCK_TIMEOUT_SECONDS,
    )
    assert queue._redis is gateway


# ---------------------------------------------------------------------------
# block-policy timeout is an ABSOLUTE bound per publish/add call: it must survive
# tenacity retries triggered by transient Redis errors (it used to restart from
# zero on every retry, so a flapping connection could block for the whole retry
# budget and surface RetryBudgetExhaustedError instead of QueueBackpressureError).
# ---------------------------------------------------------------------------


def _sync_block_gateway(client, *, block_timeout, interrupt=None):
    return sync_gateway_module.RedisGateway(
        redis_client=client,
        max_pending_length=1,
        pending_overload_policy="block",
        pending_overload_block_timeout_seconds=block_timeout,
        # Large budget so a budget-bounded wait would be clearly distinguishable
        # from a block-timeout-bounded one, with fast retry delays.
        retry_budget_seconds=30,
        retry_max_delay_seconds=0.02,
        retry_initial_delay_seconds=0.001,
        interrupt=interrupt,
    )


def _async_block_gateway(client, *, block_timeout, interrupt=None):
    return async_gateway_module.RedisGateway(
        redis_client=client,
        max_pending_length=1,
        pending_overload_policy="block",
        pending_overload_block_timeout_seconds=block_timeout,
        retry_budget_seconds=30,
        retry_max_delay_seconds=0.02,
        retry_initial_delay_seconds=0.001,
        interrupt=interrupt,
    )


def test_sync_block_deadline_survives_transient_error_mid_wait():
    # Repeated transient errors during the block wait must NOT reset the block
    # deadline. The deadline is an absolute per-call bound, so the total wait
    # stays near the block timeout (0.3s) and well under the 30s retry budget —
    # under the OLD per-attempt-deadline behavior these recurring errors kept
    # restarting the window, blocking for the whole budget instead.
    block_timeout = 0.3
    client = _BlockingEvalClient(fakeredis.FakeRedis(), raise_every=6)
    gateway = _sync_block_gateway(client, block_timeout=block_timeout)

    start = time.monotonic()
    with pytest.raises(QueueBackpressureError, match="stayed at max_pending_length=1"):
        gateway.publish_message("bp-sync-deadline:pending", "payload", "bp-sync-deadline:dedup")
    elapsed = time.monotonic() - start

    assert elapsed < 2.0, elapsed
    # The injected transient errors did fire (proving retries were driven), but
    # did not restart the window.
    assert client.eval_calls > 6


@pytest.mark.asyncio
async def test_async_block_deadline_survives_transient_error_mid_wait():
    block_timeout = 0.3
    client = _AsyncBlockingEvalClient(fakeredis.FakeAsyncRedis(), raise_every=6)
    gateway = _async_block_gateway(client, block_timeout=block_timeout)

    start = time.monotonic()
    with pytest.raises(QueueBackpressureError, match="stayed at max_pending_length=1"):
        await gateway.publish_message("bp-async-deadline:pending", "payload", "bp-async-deadline:dedup")
    elapsed = time.monotonic() - start

    assert elapsed < 2.0, elapsed
    assert client.eval_calls > 6


def test_sync_block_terminal_exception_is_backpressure_despite_transient_error():
    # Even with interleaved transient errors, once the window is spent the next
    # at-capacity verdict must surface QueueBackpressureError, never
    # RetryBudgetExhaustedError.
    client = _BlockingEvalClient(fakeredis.FakeRedis(), raise_every=4)
    gateway = _sync_block_gateway(client, block_timeout=0.2)

    with pytest.raises(QueueBackpressureError) as excinfo:
        gateway.publish_message("bp-sync-terminal:pending", "payload", "bp-sync-terminal:dedup")
    assert not isinstance(excinfo.value, RetryBudgetExhaustedError)


@pytest.mark.asyncio
async def test_async_block_terminal_exception_is_backpressure_despite_transient_error():
    client = _AsyncBlockingEvalClient(fakeredis.FakeAsyncRedis(), raise_every=4)
    gateway = _async_block_gateway(client, block_timeout=0.2)

    with pytest.raises(QueueBackpressureError) as excinfo:
        await gateway.publish_message("bp-async-terminal:pending", "payload", "bp-async-terminal:dedup")
    assert not isinstance(excinfo.value, RetryBudgetExhaustedError)


def test_sync_block_wait_aborts_promptly_on_interrupt():
    # An interrupt landing during the block wait must abort it well before the
    # (long) block timeout elapses, raising a QueueBackpressureError whose
    # message marks the abort as a shutdown interrupt.
    interrupt = _FlagInterrupt()
    client = _BlockingEvalClient(fakeredis.FakeRedis())
    gateway = _sync_block_gateway(client, block_timeout=10.0, interrupt=interrupt)

    threading.Timer(0.2, lambda: setattr(interrupt, "flag", True)).start()
    start = time.monotonic()
    with pytest.raises(QueueBackpressureError, match="aborted by shutdown interrupt"):
        gateway.publish_message("bp-sync-interrupt:pending", "payload", "bp-sync-interrupt:dedup")
    elapsed = time.monotonic() - start

    assert elapsed < 1.0, elapsed


@pytest.mark.asyncio
async def test_async_block_wait_aborts_promptly_on_interrupt():
    import asyncio

    interrupt = _FlagInterrupt()
    client = _AsyncBlockingEvalClient(fakeredis.FakeAsyncRedis())
    gateway = _async_block_gateway(client, block_timeout=10.0, interrupt=interrupt)

    async def flip_after():
        await asyncio.sleep(0.2)
        interrupt.flag = True

    flipper = asyncio.create_task(flip_after())
    start = time.monotonic()
    with pytest.raises(QueueBackpressureError, match="aborted by shutdown interrupt"):
        await gateway.publish_message("bp-async-interrupt:pending", "payload", "bp-async-interrupt:dedup")
    elapsed = time.monotonic() - start
    await flipper

    assert elapsed < 1.0, elapsed


def test_sync_block_zero_timeout_is_single_check_with_interrupt_unset():
    # timeout=0 keeps its documented single immediate capacity check even with an
    # (un-fired) interrupt handler wired in.
    interrupt = _FlagInterrupt()
    client = _BlockingEvalClient(fakeredis.FakeRedis())
    gateway = _sync_block_gateway(client, block_timeout=0, interrupt=interrupt)

    with pytest.raises(QueueBackpressureError, match="stayed at max_pending_length=1"):
        gateway.publish_message("bp-sync-zero:pending", "payload", "bp-sync-zero:dedup")
    assert client.eval_calls == 1


@pytest.mark.asyncio
async def test_async_block_zero_timeout_is_single_check_with_interrupt_unset():
    interrupt = _FlagInterrupt()
    client = _AsyncBlockingEvalClient(fakeredis.FakeAsyncRedis())
    gateway = _async_block_gateway(client, block_timeout=0, interrupt=interrupt)

    with pytest.raises(QueueBackpressureError, match="stayed at max_pending_length=1"):
        await gateway.publish_message("bp-async-zero:pending", "payload", "bp-async-zero:dedup")
    assert client.eval_calls == 1
