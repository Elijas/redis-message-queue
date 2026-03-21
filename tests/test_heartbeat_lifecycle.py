import asyncio
import logging
import threading
import time

import pytest

from redis_message_queue._abstract_redis_gateway import (
    AbstractRedisGateway as SyncAbstractRedisGateway,
)
from redis_message_queue._stored_message import ClaimedMessage, MessageData
from redis_message_queue.asyncio._abstract_redis_gateway import (
    AbstractRedisGateway as AsyncAbstractRedisGateway,
)
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
