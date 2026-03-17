import asyncio
import logging
import threading
import time

import pytest

from redis_message_queue.asyncio.redis_message_queue import (
    _LeaseHeartbeat as AsyncLeaseHeartbeat,
)
from redis_message_queue.redis_message_queue import _LeaseHeartbeat


class TestSyncHeartbeatLifecycle:
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


class TestAsyncHeartbeatLifecycle:
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
