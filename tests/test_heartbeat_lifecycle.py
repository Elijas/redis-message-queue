import asyncio
import logging
import threading
import time

import fakeredis
import pytest
import redis.exceptions

from redis_message_queue._abstract_redis_gateway import (
    AbstractRedisGateway as SyncAbstractRedisGateway,
)
from redis_message_queue._redis_gateway import RedisGateway as BuiltinSyncRedisGateway
from redis_message_queue._stored_message import ClaimedMessage, MessageData
from redis_message_queue.asyncio._abstract_redis_gateway import (
    AbstractRedisGateway as AsyncAbstractRedisGateway,
)
from redis_message_queue.asyncio._redis_gateway import RedisGateway as BuiltinAsyncRedisGateway
from redis_message_queue.asyncio.redis_message_queue import (
    RedisMessageQueue as AsyncRedisMessageQueue,
)
from redis_message_queue.asyncio.redis_message_queue import (
    _LeaseHeartbeat as AsyncLeaseHeartbeat,
)
from redis_message_queue.redis_message_queue import (
    RedisMessageQueue,
    _LeaseHeartbeat,
)

HEARTBEAT_THREAD_NAME = "redis-message-queue-lease-heartbeat"


def _retry_once_on_connection_error(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except redis.exceptions.ConnectionError:
            return func(*args, **kwargs)

    return wrapper


def _async_retry_once_on_connection_error(func):
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except redis.exceptions.ConnectionError:
            return await func(*args, **kwargs)

    return wrapper


class _SlowAmbiguousRemoveSyncClient:
    def __init__(self) -> None:
        self.redis = fakeredis.FakeRedis()
        self._failed_remove = False

    def eval(self, script, numkeys, *args):
        result = self.redis.eval(script, numkeys, *args)
        if numkeys == 6 and len(args) == 9 and not self._failed_remove:
            self._failed_remove = True
            time.sleep(0.15)
            raise redis.exceptions.ConnectionError("lost response after remove eval")
        return result

    def __getattr__(self, name):
        return getattr(self.redis, name)


class _SlowAmbiguousRemoveAsyncClient:
    def __init__(self) -> None:
        self.redis = fakeredis.FakeAsyncRedis()
        self._failed_remove = False

    async def eval(self, script, numkeys, *args):
        result = await self.redis.eval(script, numkeys, *args)
        if numkeys == 6 and len(args) == 9 and not self._failed_remove:
            self._failed_remove = True
            await asyncio.sleep(0.15)
            raise redis.exceptions.ConnectionError("lost response after remove eval")
        return result

    def __getattr__(self, name):
        return getattr(self.redis, name)


# ---------------------------------------------------------------------------
# Spy gateways: record whether heartbeat is alive when ack fires
# ---------------------------------------------------------------------------


class _SyncSpyGateway(SyncAbstractRedisGateway):
    """Gateway that records heartbeat thread liveness during ack calls."""

    message_visibility_timeout_seconds = 10

    def __init__(self) -> None:
        self.heartbeat_alive_during_ack: bool | None = None

    def _record_heartbeat_alive(self) -> None:
        alive_threads = [t.name for t in threading.enumerate()]
        self.heartbeat_alive_during_ack = HEARTBEAT_THREAD_NAME in alive_threads

    def publish_message(self, queue: str, message: str, dedup_key: str) -> bool:
        return True

    def add_message(self, queue: str, message: str) -> None:
        pass

    def move_message(
        self,
        from_queue: str,
        to_queue: str,
        message: MessageData,
        *,
        lease_token: str | None = None,
    ) -> bool:
        self._record_heartbeat_alive()
        return True

    def remove_message(self, queue: str, message: MessageData, *, lease_token: str | None = None) -> bool:
        self._record_heartbeat_alive()
        return True

    def renew_message_lease(self, queue: str, message: MessageData, lease_token: str) -> bool:
        return True

    def wait_for_message_and_move(self, from_queue: str, to_queue: str) -> ClaimedMessage | MessageData | None:
        return ClaimedMessage(stored_message="test-message", lease_token="test-token")

    def trim_queue(self, queue, max_length):
        pass


class _AsyncSpyGateway(AsyncAbstractRedisGateway):
    """Gateway that records heartbeat task liveness during ack calls."""

    message_visibility_timeout_seconds = 10

    def __init__(self) -> None:
        self.heartbeat_alive_during_ack: bool | None = None

    def _record_heartbeat_alive(self) -> None:
        tasks = asyncio.all_tasks()
        self.heartbeat_alive_during_ack = any(t.get_name() == HEARTBEAT_THREAD_NAME for t in tasks)

    async def publish_message(self, queue: str, message: str, dedup_key: str) -> bool:
        return True

    async def add_message(self, queue: str, message: str) -> None:
        pass

    async def move_message(
        self,
        from_queue: str,
        to_queue: str,
        message: MessageData,
        *,
        lease_token: str | None = None,
    ) -> bool:
        self._record_heartbeat_alive()
        return True

    async def remove_message(self, queue: str, message: MessageData, *, lease_token: str | None = None) -> bool:
        self._record_heartbeat_alive()
        return True

    async def renew_message_lease(self, queue: str, message: MessageData, lease_token: str) -> bool:
        return True

    async def wait_for_message_and_move(self, from_queue: str, to_queue: str) -> ClaimedMessage | MessageData | None:
        return ClaimedMessage(stored_message="test-message", lease_token="test-token")

    async def trim_queue(self, queue, max_length):
        pass


class _SyncAckFailureGateway(SyncAbstractRedisGateway):
    """Gateway that raises on remove_message but returns lease tokens (heartbeat active)."""

    message_visibility_timeout_seconds = 10

    def __init__(self) -> None:
        self.heartbeat_alive_during_ack: bool | None = None

    def _record_heartbeat_alive(self) -> None:
        alive_threads = [t.name for t in threading.enumerate()]
        self.heartbeat_alive_during_ack = HEARTBEAT_THREAD_NAME in alive_threads

    def publish_message(self, queue: str, message: str, dedup_key: str) -> bool:
        return True

    def add_message(self, queue: str, message: str) -> None:
        pass

    def move_message(
        self,
        from_queue: str,
        to_queue: str,
        message: MessageData,
        *,
        lease_token: str | None = None,
    ) -> bool:
        return True

    def remove_message(self, queue: str, message: MessageData, *, lease_token: str | None = None) -> bool:
        self._record_heartbeat_alive()
        raise RuntimeError("ack failed")

    def renew_message_lease(self, queue: str, message: MessageData, lease_token: str) -> bool:
        return True

    def wait_for_message_and_move(self, from_queue: str, to_queue: str) -> ClaimedMessage | MessageData | None:
        return ClaimedMessage(stored_message="test-message", lease_token="test-token")

    def trim_queue(self, queue, max_length):
        pass


class _AsyncAckFailureGateway(AsyncAbstractRedisGateway):
    """Gateway that raises on remove_message but returns lease tokens (heartbeat active)."""

    message_visibility_timeout_seconds = 10

    def __init__(self) -> None:
        self.heartbeat_alive_during_ack: bool | None = None

    def _record_heartbeat_alive(self) -> None:
        tasks = asyncio.all_tasks()
        self.heartbeat_alive_during_ack = any(t.get_name() == HEARTBEAT_THREAD_NAME for t in tasks)

    async def publish_message(self, queue: str, message: str, dedup_key: str) -> bool:
        return True

    async def add_message(self, queue: str, message: str) -> None:
        pass

    async def move_message(
        self,
        from_queue: str,
        to_queue: str,
        message: MessageData,
        *,
        lease_token: str | None = None,
    ) -> bool:
        return True

    async def remove_message(self, queue: str, message: MessageData, *, lease_token: str | None = None) -> bool:
        self._record_heartbeat_alive()
        raise RuntimeError("ack failed")

    async def renew_message_lease(self, queue: str, message: MessageData, lease_token: str) -> bool:
        return True

    async def wait_for_message_and_move(self, from_queue: str, to_queue: str) -> ClaimedMessage | MessageData | None:
        return ClaimedMessage(stored_message="test-message", lease_token="test-token")

    async def trim_queue(self, queue, max_length):
        pass


class _SyncStaleLeaseGateway(SyncAbstractRedisGateway):
    """Gateway that simulates an expired lease: renewal returns False, ack is a no-op."""

    message_visibility_timeout_seconds = 10

    def publish_message(self, queue: str, message: str, dedup_key: str) -> bool:
        return True

    def add_message(self, queue: str, message: str) -> None:
        pass

    def move_message(
        self,
        from_queue: str,
        to_queue: str,
        message: MessageData,
        *,
        lease_token: str | None = None,
    ) -> bool:
        return False

    def remove_message(self, queue: str, message: MessageData, *, lease_token: str | None = None) -> bool:
        return False

    def renew_message_lease(self, queue: str, message: MessageData, lease_token: str) -> bool:
        return False

    def wait_for_message_and_move(self, from_queue: str, to_queue: str) -> ClaimedMessage | MessageData | None:
        return ClaimedMessage(stored_message="test-message", lease_token="test-token")

    def trim_queue(self, queue, max_length):
        pass


class _AsyncStaleLeaseGateway(AsyncAbstractRedisGateway):
    """Gateway that simulates an expired lease: renewal returns False, ack is a no-op."""

    message_visibility_timeout_seconds = 10

    async def publish_message(self, queue: str, message: str, dedup_key: str) -> bool:
        return True

    async def add_message(self, queue: str, message: str) -> None:
        pass

    async def move_message(
        self,
        from_queue: str,
        to_queue: str,
        message: MessageData,
        *,
        lease_token: str | None = None,
    ) -> bool:
        return False

    async def remove_message(self, queue: str, message: MessageData, *, lease_token: str | None = None) -> bool:
        return False

    async def renew_message_lease(self, queue: str, message: MessageData, lease_token: str) -> bool:
        return False

    async def wait_for_message_and_move(self, from_queue: str, to_queue: str) -> ClaimedMessage | MessageData | None:
        return ClaimedMessage(stored_message="test-message", lease_token="test-token")

    async def trim_queue(self, queue, max_length):
        pass


class _AsyncSlowStopGateway(AsyncAbstractRedisGateway):
    """Gateway whose renewal ignores cancellation until explicitly released."""

    message_visibility_timeout_seconds = 10

    def __init__(self) -> None:
        self.renewal_started = asyncio.Event()
        self.allow_renewal_finish = asyncio.Event()

    async def publish_message(self, queue: str, message: str, dedup_key: str) -> bool:
        return True

    async def add_message(self, queue: str, message: str) -> None:
        pass

    async def move_message(
        self,
        from_queue: str,
        to_queue: str,
        message: MessageData,
        *,
        lease_token: str | None = None,
    ) -> bool:
        return True

    async def remove_message(self, queue: str, message: MessageData, *, lease_token: str | None = None) -> bool:
        return True

    async def renew_message_lease(self, queue: str, message: MessageData, lease_token: str) -> bool:
        self.renewal_started.set()
        try:
            await self.allow_renewal_finish.wait()
        except asyncio.CancelledError:
            await self.allow_renewal_finish.wait()
        return True

    async def wait_for_message_and_move(self, from_queue: str, to_queue: str) -> ClaimedMessage | MessageData | None:
        return ClaimedMessage(stored_message="test-message", lease_token="test-token")

    async def trim_queue(self, queue, max_length):
        pass


# ---------------------------------------------------------------------------
# Finding 1: heartbeat must be alive during ack
# ---------------------------------------------------------------------------


class TestSyncHeartbeatAliveDuringAck:
    def test_heartbeat_alive_during_ack(self):
        """Heartbeat thread is still alive when remove_message fires on success path."""
        gateway = _SyncSpyGateway()
        q = RedisMessageQueue("test", gateway=gateway, heartbeat_interval_seconds=1)
        with q.process_message() as msg:
            assert msg is not None
        assert gateway.heartbeat_alive_during_ack is True

    def test_heartbeat_alive_during_failed_ack(self):
        """Heartbeat thread is still alive when move_message fires on exception path."""
        gateway = _SyncSpyGateway()
        q = RedisMessageQueue("test", gateway=gateway, heartbeat_interval_seconds=1, enable_failed_queue=True)
        with pytest.raises(RuntimeError):
            with q.process_message() as msg:
                assert msg is not None
                raise RuntimeError("processing failed")
        assert gateway.heartbeat_alive_during_ack is True


class TestAsyncHeartbeatAliveDuringAck:
    @pytest.mark.asyncio
    async def test_heartbeat_alive_during_ack(self):
        """Heartbeat task is still alive when remove_message fires on success path."""
        gateway = _AsyncSpyGateway()
        q = AsyncRedisMessageQueue("test", gateway=gateway, heartbeat_interval_seconds=1)
        async with q.process_message() as msg:
            assert msg is not None
        assert gateway.heartbeat_alive_during_ack is True

    @pytest.mark.asyncio
    async def test_heartbeat_alive_during_failed_ack(self):
        """Heartbeat task is still alive when move_message fires on exception path."""
        gateway = _AsyncSpyGateway()
        q = AsyncRedisMessageQueue("test", gateway=gateway, heartbeat_interval_seconds=1, enable_failed_queue=True)
        with pytest.raises(RuntimeError):
            async with q.process_message() as msg:
                assert msg is not None
                raise RuntimeError("processing failed")
        assert gateway.heartbeat_alive_during_ack is True


# ---------------------------------------------------------------------------
# Finding 2: stop() on never-started heartbeat must be safe
# ---------------------------------------------------------------------------


class TestStopWithoutStart:
    def test_sync_stop_without_start_is_safe(self):
        """stop() on never-started heartbeat doesn't raise."""
        hb = _LeaseHeartbeat(interval_seconds=1.0, renew_message_lease=lambda: True)
        hb.stop()  # must not raise

    @pytest.mark.asyncio
    async def test_async_stop_without_start_is_safe(self):
        """stop() on never-started async heartbeat doesn't raise (regression test)."""

        async def renew():
            return True

        hb = AsyncLeaseHeartbeat(interval_seconds=1.0, renew_message_lease=renew)
        await hb.stop()  # must not raise


# ---------------------------------------------------------------------------
# Existing heartbeat unit tests
# ---------------------------------------------------------------------------


class TestSyncHeartbeatLifecycle:
    def test_thread_is_daemon(self):
        """Heartbeat thread must be a daemon so it doesn't prevent process exit."""
        hb = _LeaseHeartbeat(
            interval_seconds=1.0,
            renew_message_lease=lambda: True,
        )
        assert hb._thread.daemon is True

    def test_heartbeat_cleanup_on_early_return(self):
        """Heartbeat thread stops when context manager exits via early return."""
        gateway = _SyncSpyGateway()
        q = RedisMessageQueue("test", gateway=gateway, heartbeat_interval_seconds=1)

        def process_and_return():
            with q.process_message() as msg:
                assert msg is not None
                return  # early return

        process_and_return()
        # Give thread a moment to be cleaned up
        time.sleep(0.05)
        alive = [t for t in threading.enumerate() if t.name == HEARTBEAT_THREAD_NAME]
        assert alive == []

    def test_stop_waits_for_thread_to_exit(self):
        hb = _LeaseHeartbeat(
            interval_seconds=0.01,
            renew_message_lease=lambda: True,
        )
        hb.start()
        assert hb._thread.is_alive()
        hb.stop()
        assert not hb._thread.is_alive()

    def test_thread_exits_when_renewal_raises(self):
        def failing_renewal():
            raise RuntimeError("redis down")

        hb = _LeaseHeartbeat(
            interval_seconds=0.01,
            renew_message_lease=failing_renewal,
        )
        hb.start()
        hb._thread.join(timeout=1.0)
        assert not hb._thread.is_alive()

    def test_thread_exits_when_renewal_returns_false(self):
        hb = _LeaseHeartbeat(
            interval_seconds=0.01,
            renew_message_lease=lambda: False,
        )
        hb.start()
        hb._thread.join(timeout=1.0)
        assert not hb._thread.is_alive()

    def test_stop_logs_warning_when_thread_outlives_join(self, caplog):
        block = threading.Event()

        def slow_renewal():
            block.wait()
            return True

        hb = _LeaseHeartbeat(
            interval_seconds=0.01,
            renew_message_lease=slow_renewal,
        )
        hb.start()

        # Wait until the thread enters the slow renewal
        time.sleep(0.05)

        with caplog.at_level(logging.WARNING, logger="redis_message_queue.redis_message_queue"):
            hb.stop()

        assert hb._thread.is_alive()
        assert "did not stop within timeout" in caplog.text

        # Unblock and verify the thread eventually exits
        block.set()
        hb._thread.join(timeout=1.0)
        assert not hb._thread.is_alive()

    def test_no_renewal_after_stop(self):
        call_count = 0
        proceed = threading.Event()

        def counting_renewal():
            nonlocal call_count
            call_count += 1
            proceed.set()
            return True

        hb = _LeaseHeartbeat(
            interval_seconds=0.01,
            renew_message_lease=counting_renewal,
        )
        hb.start()
        proceed.wait(timeout=1.0)
        hb.stop()

        count_at_stop = call_count
        time.sleep(0.05)
        assert call_count == count_at_stop

    def test_double_stop_is_safe(self):
        """Calling stop() twice must not raise."""
        hb = _LeaseHeartbeat(
            interval_seconds=0.01,
            renew_message_lease=lambda: True,
        )
        hb.start()
        hb.stop()
        hb.stop()
        assert not hb._thread.is_alive()

    def test_no_extra_renewal_after_slow_renewal_finishes_during_stop(self):
        """After stop() times out on join, the in-flight renewal finishes
        and the thread exits without starting another renewal cycle."""
        call_count = 0
        entered_renewal = threading.Event()
        unblock_renewal = threading.Event()

        def slow_renewal():
            nonlocal call_count
            call_count += 1
            entered_renewal.set()
            unblock_renewal.wait()
            return True

        hb = _LeaseHeartbeat(
            interval_seconds=0.01,
            renew_message_lease=slow_renewal,
        )
        hb.start()
        entered_renewal.wait(timeout=1.0)
        hb.stop()  # join times out because renewal is in-flight

        assert hb._thread.is_alive()

        unblock_renewal.set()
        hb._thread.join(timeout=1.0)
        assert not hb._thread.is_alive()
        assert call_count == 1


class TestAsyncHeartbeatLifecycle:
    @pytest.mark.asyncio
    async def test_stop_does_not_swallow_caller_cancellation(self):
        """stop() must not suppress CancelledError aimed at the calling task.

        If an external caller cancels a task that is awaiting stop(), the
        CancelledError must propagate — otherwise task cancellation is silently
        eaten, which can cause processing loops to continue when they should stop.
        """

        async def renew():
            return True

        hb = AsyncLeaseHeartbeat(interval_seconds=10, renew_message_lease=renew)
        hb.start()

        async def call_stop():
            await hb.stop()

        task = asyncio.create_task(call_stop())
        # Let the task reach the `await self._task` inside stop()
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    @pytest.mark.asyncio
    async def test_heartbeat_cleanup_on_early_return(self):
        """Heartbeat task is cleaned up when context manager exits via early return."""
        gateway = _AsyncSpyGateway()
        q = AsyncRedisMessageQueue("test", gateway=gateway, heartbeat_interval_seconds=1)

        async def process_and_return():
            async with q.process_message() as msg:
                assert msg is not None
                return  # early return

        await process_and_return()
        # Give event loop a tick for cleanup
        await asyncio.sleep(0)
        heartbeat_tasks = [t for t in asyncio.all_tasks() if t.get_name() == HEARTBEAT_THREAD_NAME]
        assert heartbeat_tasks == []

    @pytest.mark.asyncio
    async def test_stop_cancels_and_awaits_task(self):
        async def renew():
            return True

        hb = AsyncLeaseHeartbeat(
            interval_seconds=0.01,
            renew_message_lease=renew,
        )
        hb.start()
        assert hb._task is not None
        assert not hb._task.done()
        await hb.stop()
        assert hb._task.done()

    @pytest.mark.asyncio
    async def test_task_exits_when_renewal_raises(self):
        async def failing_renewal():
            raise RuntimeError("redis down")

        hb = AsyncLeaseHeartbeat(
            interval_seconds=0.01,
            renew_message_lease=failing_renewal,
        )
        hb.start()
        # Give the task time to hit the exception and exit
        await asyncio.sleep(0.05)
        assert hb._task.done()

    @pytest.mark.asyncio
    async def test_task_exits_when_renewal_returns_false(self):
        async def stale_renewal():
            return False

        hb = AsyncLeaseHeartbeat(
            interval_seconds=0.01,
            renew_message_lease=stale_renewal,
        )
        hb.start()
        await asyncio.sleep(0.05)
        assert hb._task.done()

    @pytest.mark.asyncio
    async def test_no_renewal_after_stop(self):
        call_count = 0
        called = asyncio.Event()

        async def counting_renewal():
            nonlocal call_count
            call_count += 1
            called.set()
            return True

        hb = AsyncLeaseHeartbeat(
            interval_seconds=0.01,
            renew_message_lease=counting_renewal,
        )
        hb.start()
        await asyncio.wait_for(called.wait(), timeout=1.0)
        await hb.stop()

        count_at_stop = call_count
        await asyncio.sleep(0.05)
        assert call_count == count_at_stop

    @pytest.mark.asyncio
    async def test_double_stop_is_safe(self):
        """Calling stop() twice must not raise."""

        async def renew():
            return True

        hb = AsyncLeaseHeartbeat(
            interval_seconds=0.01,
            renew_message_lease=renew,
        )
        hb.start()
        await hb.stop()
        await hb.stop()
        assert hb._task.done()

    @pytest.mark.asyncio
    async def test_stop_on_naturally_exited_task_is_safe(self):
        """stop() on a task that already exited (renewal returned False) must not raise."""

        async def stale_renewal():
            return False

        hb = AsyncLeaseHeartbeat(
            interval_seconds=0.01,
            renew_message_lease=stale_renewal,
        )
        hb.start()
        await asyncio.sleep(0.05)
        assert hb._task.done()
        await hb.stop()

    @pytest.mark.asyncio
    async def test_stop_cancels_during_inflight_renewal(self):
        """stop() cancels a task that is blocked inside renew_message_lease."""
        entered = asyncio.Event()

        async def blocking_renewal():
            entered.set()
            await asyncio.Event().wait()  # blocks forever
            return True

        hb = AsyncLeaseHeartbeat(
            interval_seconds=0.01,
            renew_message_lease=blocking_renewal,
        )
        hb.start()
        await entered.wait()
        await hb.stop()
        assert hb._task.done()

    @pytest.mark.asyncio
    async def test_stop_logs_warning_when_task_outlives_timeout(self, caplog):
        entered = asyncio.Event()
        unblock = asyncio.Event()

        async def uncancellable_renewal():
            entered.set()
            try:
                await unblock.wait()
            except asyncio.CancelledError:
                await unblock.wait()
            return True

        hb = AsyncLeaseHeartbeat(
            interval_seconds=0.01,
            renew_message_lease=uncancellable_renewal,
        )
        hb.start()
        await asyncio.wait_for(entered.wait(), timeout=1.0)

        with caplog.at_level(logging.WARNING, logger="redis_message_queue.asyncio.redis_message_queue"):
            await hb.stop()

        assert hb._task is not None
        assert not hb._task.done()
        assert "did not stop within timeout" in caplog.text

        unblock.set()
        await asyncio.wait_for(hb._task, timeout=1.0)
        assert hb._task.done()

    @pytest.mark.asyncio
    async def test_process_message_preserves_original_error_during_heartbeat_stop_cancellation(self, monkeypatch):
        gateway = _AsyncSlowStopGateway()
        q = AsyncRedisMessageQueue("test", gateway=gateway, heartbeat_interval_seconds=0.01)
        stop_started = asyncio.Event()
        original_stop = AsyncLeaseHeartbeat.stop

        async def instrumented_stop(self):
            stop_started.set()
            return await original_stop(self)

        monkeypatch.setattr(AsyncLeaseHeartbeat, "stop", instrumented_stop)

        async def worker():
            async with q.process_message() as msg:
                assert msg is not None
                await gateway.renewal_started.wait()
                raise ValueError("original error")

        task = asyncio.create_task(worker())
        await stop_started.wait()
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()

        gateway.allow_renewal_finish.set()

        with pytest.raises(ValueError, match="original error"):
            await task


class TestStaleLeaseDiagnostics:
    def test_sync_stale_lease_warning_after_heartbeat_self_exit(self, caplog):
        """When the lease expires server-side, process_message logs a diagnostic warning."""
        gateway = _SyncStaleLeaseGateway()
        q = RedisMessageQueue("test", gateway=gateway, heartbeat_interval_seconds=1)
        with caplog.at_level(logging.WARNING, logger="redis_message_queue.redis_message_queue"):
            with q.process_message() as msg:
                assert msg is not None
        assert "was a no-op" in caplog.text

    @pytest.mark.asyncio
    async def test_async_stale_lease_warning_after_heartbeat_self_exit(self, caplog):
        """When the lease expires server-side, async process_message logs a diagnostic warning."""
        gateway = _AsyncStaleLeaseGateway()
        q = AsyncRedisMessageQueue("test", gateway=gateway, heartbeat_interval_seconds=1)
        with caplog.at_level(logging.WARNING, logger="redis_message_queue.asyncio.redis_message_queue"):
            async with q.process_message() as msg:
                assert msg is not None
        assert "was a no-op" in caplog.text

    def test_sync_stale_lease_warning_on_exception_path(self, caplog):
        """When user code raises AND the lease expired, process_message logs a diagnostic warning."""
        gateway = _SyncStaleLeaseGateway()
        q = RedisMessageQueue("test", gateway=gateway, heartbeat_interval_seconds=1, enable_failed_queue=True)
        with caplog.at_level(logging.WARNING, logger="redis_message_queue.redis_message_queue"):
            with pytest.raises(RuntimeError, match="processing failed"):
                with q.process_message() as msg:
                    assert msg is not None
                    raise RuntimeError("processing failed")
        assert "was a no-op" in caplog.text

    @pytest.mark.asyncio
    async def test_async_stale_lease_warning_on_exception_path(self, caplog):
        """When user code raises AND the lease expired, async process_message logs a diagnostic warning."""
        gateway = _AsyncStaleLeaseGateway()
        q = AsyncRedisMessageQueue("test", gateway=gateway, heartbeat_interval_seconds=1, enable_failed_queue=True)
        with caplog.at_level(logging.WARNING, logger="redis_message_queue.asyncio.redis_message_queue"):
            with pytest.raises(RuntimeError, match="processing failed"):
                async with q.process_message() as msg:
                    assert msg is not None
                    raise RuntimeError("processing failed")
        assert "was a no-op" in caplog.text


# ---------------------------------------------------------------------------
# Ack failure on success path with active heartbeat
# ---------------------------------------------------------------------------


class TestAckFailureWithActiveHeartbeat:
    def test_sync_heartbeat_stopped_after_ack_failure(self):
        """When remove_message raises on success path, heartbeat is still stopped in finally."""
        gateway = _SyncAckFailureGateway()
        q = RedisMessageQueue("test", gateway=gateway, heartbeat_interval_seconds=1)

        # remove_message raises on the success path; the exception propagates
        # (the success path does NOT catch cleanup errors), but the finally
        # block must still stop the heartbeat.
        with pytest.raises(RuntimeError, match="ack failed"):
            with q.process_message() as msg:
                assert msg is not None

        # The heartbeat was alive when ack fired (gateway records this)
        assert gateway.heartbeat_alive_during_ack is True

        # After process_message exits, heartbeat thread must be stopped
        time.sleep(0.05)
        alive = [t for t in threading.enumerate() if t.name == HEARTBEAT_THREAD_NAME]
        assert alive == []

    @pytest.mark.asyncio
    async def test_async_heartbeat_stopped_after_ack_failure(self):
        """When remove_message raises on success path, async heartbeat is still stopped in finally."""
        gateway = _AsyncAckFailureGateway()
        q = AsyncRedisMessageQueue("test", gateway=gateway, heartbeat_interval_seconds=1)

        with pytest.raises(RuntimeError, match="ack failed"):
            async with q.process_message() as msg:
                assert msg is not None

        assert gateway.heartbeat_alive_during_ack is True

        await asyncio.sleep(0)
        heartbeat_tasks = [t for t in asyncio.all_tasks() if t.get_name() == HEARTBEAT_THREAD_NAME]
        assert heartbeat_tasks == []


# ---------------------------------------------------------------------------
# CancelledError as user exception inside async process_message
# ---------------------------------------------------------------------------


class TestCancelledErrorAsUserException:
    @pytest.mark.asyncio
    async def test_cancelled_error_raised_by_user_propagates_and_cleans_up(self):
        """When user code raises CancelledError inside process_message,
        it propagates, cleanup runs, and heartbeat stops."""
        gateway = _AsyncSpyGateway()
        q = AsyncRedisMessageQueue("test", gateway=gateway, heartbeat_interval_seconds=1)

        with pytest.raises(asyncio.CancelledError):
            async with q.process_message() as msg:
                assert msg is not None
                raise asyncio.CancelledError()

        await asyncio.sleep(0)
        heartbeat_tasks = [t for t in asyncio.all_tasks() if t.get_name() == HEARTBEAT_THREAD_NAME]
        assert heartbeat_tasks == []


# ---------------------------------------------------------------------------
# on_heartbeat_failure callback tests
# ---------------------------------------------------------------------------


class TestSyncOnHeartbeatFailureCallback:
    def test_callback_invoked_on_renewal_exception(self):
        """Callback fires when renew_message_lease raises an exception."""
        callback_fired = threading.Event()

        def failing_renewal():
            raise RuntimeError("redis down")

        hb = _LeaseHeartbeat(
            interval_seconds=0.01,
            renew_message_lease=failing_renewal,
            on_heartbeat_failure=lambda: callback_fired.set(),
        )
        hb.start()
        hb._thread.join(timeout=1.0)
        assert not hb._thread.is_alive()
        assert callback_fired.is_set()

    def test_callback_invoked_on_renewal_returns_false(self):
        """Callback fires when renew_message_lease returns False (stale lease)."""
        callback_fired = threading.Event()

        hb = _LeaseHeartbeat(
            interval_seconds=0.01,
            renew_message_lease=lambda: False,
            on_heartbeat_failure=lambda: callback_fired.set(),
        )
        hb.start()
        hb._thread.join(timeout=1.0)
        assert not hb._thread.is_alive()
        assert callback_fired.is_set()

    def test_callback_not_invoked_on_normal_stop(self):
        """Callback must NOT fire when heartbeat is stopped normally via stop()."""
        callback_fired = threading.Event()
        proceed = threading.Event()

        def counting_renewal():
            proceed.set()
            return True

        hb = _LeaseHeartbeat(
            interval_seconds=0.01,
            renew_message_lease=counting_renewal,
            on_heartbeat_failure=lambda: callback_fired.set(),
        )
        hb.start()
        proceed.wait(timeout=1.0)
        hb.stop()
        assert not callback_fired.is_set()

    def test_callback_not_invoked_when_stop_happens_during_inflight_stale_renewal(self):
        """A stale renewal that finishes after stop() begins must be treated as shutdown, not failure."""
        callback_fired = threading.Event()
        entered_renewal = threading.Event()
        unblock_renewal = threading.Event()

        def stale_renewal():
            entered_renewal.set()
            unblock_renewal.wait()
            return False

        hb = _LeaseHeartbeat(
            interval_seconds=0.01,
            renew_message_lease=stale_renewal,
            on_heartbeat_failure=lambda: callback_fired.set(),
        )
        hb.start()
        entered_renewal.wait(timeout=1.0)

        stop_thread = threading.Thread(target=hb.stop)
        stop_thread.start()
        time.sleep(0.05)
        unblock_renewal.set()

        stop_thread.join(timeout=1.0)
        hb._thread.join(timeout=1.0)
        assert not hb._thread.is_alive()
        assert not callback_fired.is_set()

    def test_callback_exception_is_logged_and_swallowed(self, caplog):
        """If the callback itself raises, the exception is logged but the thread exits cleanly."""

        def bad_callback():
            raise ValueError("callback boom")

        hb = _LeaseHeartbeat(
            interval_seconds=0.01,
            renew_message_lease=lambda: False,
            on_heartbeat_failure=bad_callback,
        )
        with caplog.at_level(logging.WARNING, logger="redis_message_queue.redis_message_queue"):
            hb.start()
            hb._thread.join(timeout=1.0)
        assert not hb._thread.is_alive()
        assert "on_heartbeat_failure callback raised an exception" in caplog.text

    def test_callback_through_queue_on_stale_lease(self):
        """End-to-end: callback fires when using a stale-lease gateway through RedisMessageQueue."""
        callback_fired = threading.Event()
        gateway = _SyncStaleLeaseGateway()
        q = RedisMessageQueue(
            "test",
            gateway=gateway,
            heartbeat_interval_seconds=0.01,
            on_heartbeat_failure=lambda: callback_fired.set(),
        )
        with q.process_message() as msg:
            assert msg is not None
            # Wait for heartbeat to detect stale lease and fire callback
            callback_fired.wait(timeout=1.0)
        assert callback_fired.is_set()


class TestAsyncOnHeartbeatFailureCallback:
    @pytest.mark.asyncio
    async def test_callback_invoked_on_renewal_exception(self):
        """Callback fires when async renew_message_lease raises an exception."""
        callback_fired = asyncio.Event()

        async def failing_renewal():
            raise RuntimeError("redis down")

        hb = AsyncLeaseHeartbeat(
            interval_seconds=0.01,
            renew_message_lease=failing_renewal,
            on_heartbeat_failure=lambda: callback_fired.set(),
        )
        hb.start()
        await asyncio.sleep(0.05)
        assert hb._task.done()
        assert callback_fired.is_set()

    @pytest.mark.asyncio
    async def test_callback_invoked_on_renewal_returns_false(self):
        """Callback fires when async renew_message_lease returns False (stale lease)."""
        callback_fired = asyncio.Event()

        async def stale_renewal():
            return False

        hb = AsyncLeaseHeartbeat(
            interval_seconds=0.01,
            renew_message_lease=stale_renewal,
            on_heartbeat_failure=lambda: callback_fired.set(),
        )
        hb.start()
        await asyncio.sleep(0.05)
        assert hb._task.done()
        assert callback_fired.is_set()

    @pytest.mark.asyncio
    async def test_callback_not_invoked_on_normal_stop(self):
        """Callback must NOT fire when async heartbeat is stopped normally via stop()."""
        callback_fired = asyncio.Event()
        called = asyncio.Event()

        async def counting_renewal():
            called.set()
            return True

        hb = AsyncLeaseHeartbeat(
            interval_seconds=0.01,
            renew_message_lease=counting_renewal,
            on_heartbeat_failure=lambda: callback_fired.set(),
        )
        hb.start()
        await asyncio.wait_for(called.wait(), timeout=1.0)
        await hb.stop()
        assert not callback_fired.is_set()

    @pytest.mark.asyncio
    async def test_callback_not_invoked_when_stop_happens_during_inflight_stale_renewal(self):
        """A stale renewal that finishes after stop() begins must be treated as shutdown, not failure."""
        callback_fired = asyncio.Event()
        entered_renewal = asyncio.Event()
        unblock_renewal = asyncio.Event()

        async def stale_renewal():
            entered_renewal.set()
            await unblock_renewal.wait()
            return False

        hb = AsyncLeaseHeartbeat(
            interval_seconds=0.01,
            renew_message_lease=stale_renewal,
            on_heartbeat_failure=lambda: callback_fired.set(),
        )
        hb.start()
        await asyncio.wait_for(entered_renewal.wait(), timeout=1.0)

        stop_task = asyncio.create_task(hb.stop())
        await asyncio.sleep(0.05)
        unblock_renewal.set()

        await asyncio.wait_for(stop_task, timeout=1.0)
        assert hb._task is not None
        assert hb._task.done()
        assert not callback_fired.is_set()

    @pytest.mark.asyncio
    async def test_callback_exception_is_logged_and_swallowed(self, caplog):
        """If the async callback itself raises, the exception is logged but the task exits cleanly."""

        def bad_callback():
            raise ValueError("callback boom")

        async def stale_renewal():
            return False

        hb = AsyncLeaseHeartbeat(
            interval_seconds=0.01,
            renew_message_lease=stale_renewal,
            on_heartbeat_failure=bad_callback,
        )
        with caplog.at_level(logging.WARNING, logger="redis_message_queue.asyncio.redis_message_queue"):
            hb.start()
            await asyncio.sleep(0.05)
        assert hb._task.done()
        assert "on_heartbeat_failure callback raised an exception" in caplog.text

    @pytest.mark.asyncio
    async def test_async_callback_is_awaited(self):
        """An async def callback is properly awaited."""
        callback_fired = asyncio.Event()

        async def async_callback():
            callback_fired.set()

        async def stale_renewal():
            return False

        hb = AsyncLeaseHeartbeat(
            interval_seconds=0.01,
            renew_message_lease=stale_renewal,
            on_heartbeat_failure=async_callback,
        )
        hb.start()
        await asyncio.sleep(0.05)
        assert hb._task.done()
        assert callback_fired.is_set()

    @pytest.mark.asyncio
    async def test_sync_callback_accepted_in_async_variant(self):
        """A plain sync callback works in the async heartbeat."""
        callback_fired = asyncio.Event()

        async def stale_renewal():
            return False

        hb = AsyncLeaseHeartbeat(
            interval_seconds=0.01,
            renew_message_lease=stale_renewal,
            on_heartbeat_failure=lambda: callback_fired.set(),
        )
        hb.start()
        await asyncio.sleep(0.05)
        assert hb._task.done()
        assert callback_fired.is_set()

    @pytest.mark.asyncio
    async def test_callback_through_queue_on_stale_lease(self):
        """End-to-end: callback fires when using a stale-lease gateway through async RedisMessageQueue."""
        callback_fired = asyncio.Event()
        gateway = _AsyncStaleLeaseGateway()
        q = AsyncRedisMessageQueue(
            "test",
            gateway=gateway,
            heartbeat_interval_seconds=0.01,
            on_heartbeat_failure=lambda: callback_fired.set(),
        )
        async with q.process_message() as msg:
            assert msg is not None
            # Wait for heartbeat to detect stale lease and fire callback
            await asyncio.wait_for(callback_fired.wait(), timeout=1.0)
        assert callback_fired.is_set()


class TestHeartbeatCallbackSuppressionDuringAck:
    def test_sync_callback_not_invoked_when_successful_ack_response_is_lost(self):
        client = _SlowAmbiguousRemoveSyncClient()
        gateway = BuiltinSyncRedisGateway(
            redis_client=client,
            retry_strategy=_retry_once_on_connection_error,
            message_visibility_timeout_seconds=30,
            message_wait_interval_seconds=0,
        )
        callback_fired = threading.Event()
        q = RedisMessageQueue(
            "test",
            gateway=gateway,
            heartbeat_interval_seconds=0.05,
            on_heartbeat_failure=lambda: callback_fired.set(),
        )

        assert q.publish("hello") is True

        with q.process_message() as msg:
            assert msg == b"hello"
            time.sleep(0.06)

        assert not callback_fired.is_set()

    @pytest.mark.asyncio
    async def test_async_callback_not_invoked_when_successful_ack_response_is_lost(self):
        client = _SlowAmbiguousRemoveAsyncClient()
        gateway = BuiltinAsyncRedisGateway(
            redis_client=client,
            retry_strategy=_async_retry_once_on_connection_error,
            message_visibility_timeout_seconds=30,
            message_wait_interval_seconds=0,
        )
        callback_fired = asyncio.Event()
        q = AsyncRedisMessageQueue(
            "test",
            gateway=gateway,
            heartbeat_interval_seconds=0.05,
            on_heartbeat_failure=lambda: callback_fired.set(),
        )

        assert await q.publish("hello") is True

        async with q.process_message() as msg:
            assert msg == b"hello"
            await asyncio.sleep(0.06)

        assert not callback_fired.is_set()
