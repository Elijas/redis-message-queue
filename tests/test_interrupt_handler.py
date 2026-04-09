import signal

import fakeredis
import pytest
import redis.exceptions

import redis_message_queue._redis_gateway as sync_gateway_module
import redis_message_queue.asyncio._redis_gateway as async_gateway_module
from redis_message_queue._redis_gateway import RedisGateway
from redis_message_queue.asyncio._redis_gateway import (
    RedisGateway as AsyncRedisGateway,
)
from redis_message_queue.interrupt_handler._implementation import (
    GracefulInterruptHandler,
)
from redis_message_queue.interrupt_handler._interface import (
    BaseGracefulInterruptHandler,
)


@pytest.fixture(autouse=True)
def _restore_signal_handlers():
    """Save and restore signal handlers to prevent test pollution of pytest's SIGINT handler."""
    saved = {sig: signal.getsignal(sig) for sig in GracefulInterruptHandler._DEFAULT_SIGNALS}
    yield
    for sig, handler in saved.items():
        signal.signal(sig, handler)


class TestVerboseParameterValidation:
    @pytest.mark.parametrize("invalid_value", ["yes", "false", 1, 0, None, 2.0, []])
    def test_non_bool_verbose_raises_type_error(self, invalid_value):
        with pytest.raises(TypeError, match="'verbose' must be a bool"):
            GracefulInterruptHandler(verbose=invalid_value)

    def test_true_is_accepted(self):
        handler = GracefulInterruptHandler(verbose=True)
        assert handler._verbose is True

    def test_false_is_accepted(self):
        handler = GracefulInterruptHandler(verbose=False)
        assert handler._verbose is False

    def test_default_is_true(self):
        handler = GracefulInterruptHandler()
        assert handler._verbose is True


class TestSignalsParameterValidation:
    @pytest.mark.parametrize("invalid_value", [42, 3.14, True, None])
    def test_non_iterable_signals_raises_type_error(self, invalid_value):
        with pytest.raises(TypeError, match="'signals' must be an iterable"):
            GracefulInterruptHandler(signals=invalid_value)

    def test_string_signals_raises_type_error(self):
        with pytest.raises(TypeError, match="'signals' must be an iterable"):
            GracefulInterruptHandler(signals="SIGINT")

    @pytest.mark.parametrize("invalid_signals", [(42,), ("SIGINT",)])
    def test_non_signal_element_raises_type_error(self, invalid_signals):
        with pytest.raises(TypeError, match="signal.Signals"):
            GracefulInterruptHandler(signals=invalid_signals)

    def test_valid_signals_accepted(self):
        handler = GracefulInterruptHandler(signals=(signal.SIGINT,))
        assert handler._signals == (signal.SIGINT,)

    def test_empty_signals_raises_value_error(self):
        with pytest.raises(ValueError, match="at least one signal"):
            GracefulInterruptHandler(signals=())

    def test_empty_set_signals_raises_value_error(self):
        with pytest.raises(ValueError, match="at least one signal"):
            GracefulInterruptHandler(signals=set())

    def test_default_signals(self):
        handler = GracefulInterruptHandler()
        assert handler._signals == GracefulInterruptHandler._DEFAULT_SIGNALS


class TestDuplicateSignalRejection:
    """Creating a second handler for an already-owned signal must raise."""

    def test_same_signal_raises_value_error(self):
        GracefulInterruptHandler(signals=(signal.SIGINT,))
        with pytest.raises(ValueError, match="already owned by another GracefulInterruptHandler"):
            GracefulInterruptHandler(signals=(signal.SIGINT,))

    def test_overlapping_signals_raises_value_error(self):
        GracefulInterruptHandler(signals=(signal.SIGINT,))
        with pytest.raises(ValueError, match="SIGINT.*already owned"):
            GracefulInterruptHandler(signals=(signal.SIGINT, signal.SIGTERM))

    def test_disjoint_signals_accepted(self):
        GracefulInterruptHandler(signals=(signal.SIGINT,))
        h2 = GracefulInterruptHandler(signals=(signal.SIGTERM,))
        assert h2._signals == (signal.SIGTERM,)

    def test_after_restore_new_handler_accepted(self):
        """Once an external party restores the signal (e.g. test fixture),
        a new handler for that signal should be accepted."""
        GracefulInterruptHandler(signals=(signal.SIGINT,))
        # Simulate external restoration (like the autouse fixture does)
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        # Now a new handler should work fine
        h2 = GracefulInterruptHandler(signals=(signal.SIGINT,))
        assert h2._signals == (signal.SIGINT,)


class TestExistingSignalHandlerRejection:
    """Installing over an unrelated existing handler must raise."""

    def test_custom_sigint_handler_raises_value_error(self):
        def custom_handler(signum, frame):
            return None

        signal.signal(signal.SIGINT, custom_handler)
        with pytest.raises(ValueError, match="non-default handler installed"):
            GracefulInterruptHandler(signals=(signal.SIGINT,))

    def test_ignored_sigterm_handler_raises_value_error(self):
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        with pytest.raises(ValueError, match="non-default handler installed"):
            GracefulInterruptHandler(signals=(signal.SIGTERM,))


# ---------------------------------------------------------------------------
# Interrupt stops retry strategy mid-retry
# ---------------------------------------------------------------------------


class _ManualInterruptHandler(BaseGracefulInterruptHandler):
    """Testable interrupt handler that can be triggered programmatically."""

    def __init__(self):
        self._interrupted = False

    def interrupt(self):
        self._interrupted = True

    def is_interrupted(self) -> bool:
        return self._interrupted


class _FailThenSucceedClient:
    """Redis client that raises ConnectionError on first eval call, then succeeds.

    If the interrupt fires between the first failure and the retry, the retry
    strategy should stop and let the ConnectionError propagate.
    """

    def __init__(self, interrupt: _ManualInterruptHandler):
        self.redis = fakeredis.FakeRedis()
        self.eval_calls = 0
        self._interrupt = interrupt

    def eval(self, script, numkeys, *args):
        self.eval_calls += 1
        if self.eval_calls == 1:
            # Simulate the interrupt firing during the failure window
            self._interrupt.interrupt()
            raise redis.exceptions.ConnectionError("transient failure")
        return self.redis.eval(script, numkeys, *args)

    def __getattr__(self, name):
        return getattr(self.redis, name)


class _InterruptDuringRecoverySyncClient:
    def __init__(self, interrupt: _ManualInterruptHandler):
        self._interrupt = interrupt
        self.eval_calls = 0

    def get(self, key):
        self._interrupt.interrupt()
        return None

    def eval(self, *args, **kwargs):
        self.eval_calls += 1
        return b"message"

    def delete(self, key):
        return 1


class _InterruptDuringRecoveryAsyncClient:
    def __init__(self, interrupt: _ManualInterruptHandler):
        self._interrupt = interrupt
        self.eval_calls = 0

    async def get(self, key):
        self._interrupt.interrupt()
        return None

    async def eval(self, *args, **kwargs):
        self.eval_calls += 1
        return b"message"

    async def delete(self, key):
        return 1


class _InterruptingRetryableSyncClient:
    def __init__(self, interrupt: _ManualInterruptHandler, *, result):
        self._interrupt = interrupt
        self._result = result
        self.eval_calls = 0

    def eval(self, *args, **kwargs):
        self.eval_calls += 1
        if self.eval_calls == 1:
            self._interrupt.interrupt()
            raise redis.exceptions.ConnectionError("transient failure")
        return self._result

    def delete(self, key):
        return 1

    def hdel(self, key, field):
        return 1


class _InterruptingRetryableAsyncClient:
    def __init__(self, interrupt: _ManualInterruptHandler, *, result):
        self._interrupt = interrupt
        self._result = result
        self.eval_calls = 0

    async def eval(self, *args, **kwargs):
        self.eval_calls += 1
        if self.eval_calls == 1:
            self._interrupt.interrupt()
            raise redis.exceptions.ConnectionError("transient failure")
        return self._result

    async def delete(self, key):
        return 1

    async def hdel(self, key, field):
        return 1


class TestInterruptStopsRetryStrategy:
    """Verifies that a BaseGracefulInterruptHandler actually stops the
    default retry strategy from retrying after a retryable exception."""

    def test_interrupt_prevents_retry_on_publish(self):
        """When a retryable ConnectionError fires and the interrupt is set,
        the retry strategy must stop and let the exception propagate
        instead of retrying."""
        interrupt = _ManualInterruptHandler()
        client = _FailThenSucceedClient(interrupt)

        gateway = RedisGateway(
            redis_client=client,
            interrupt=interrupt,
            message_wait_interval_seconds=0,
        )

        # Without the interrupt, this would succeed on the second attempt.
        # With the interrupt set before the retry, the ConnectionError propagates.
        with pytest.raises(redis.exceptions.ConnectionError, match="transient failure"):
            gateway.publish_message("pending", "hello", "dedup:hello")

        # The eval was called exactly once — the retry was prevented
        assert client.eval_calls == 1

    def test_without_interrupt_retry_succeeds(self):
        """Control test: same scenario but without triggering the interrupt.
        The retry fires and the operation succeeds on the second attempt."""
        interrupt = _ManualInterruptHandler()
        client = fakeredis.FakeRedis()
        eval_calls = 0
        original_eval = client.eval

        def failing_then_succeeding_eval(script, numkeys, *args):
            nonlocal eval_calls
            eval_calls += 1
            if eval_calls == 1:
                raise redis.exceptions.ConnectionError("transient failure")
            return original_eval(script, numkeys, *args)

        client.eval = failing_then_succeeding_eval

        gateway = RedisGateway(
            redis_client=client,
            interrupt=interrupt,
            message_wait_interval_seconds=0,
        )

        # Without interrupt, the retry succeeds
        result = gateway.publish_message("pending", "hello", "dedup:hello")
        assert result is True
        assert eval_calls == 2


class TestInterruptStopsWaiting:
    def test_sync_interrupt_returns_none_without_entering_claim_poll(self):
        interrupt = _ManualInterruptHandler()
        interrupt.interrupt()

        class RecordingClient:
            def __init__(self):
                self.eval_calls = 0

            def eval(self, *args, **kwargs):
                self.eval_calls += 1
                return b"message"

        client = RecordingClient()
        gateway = RedisGateway(
            redis_client=client,
            interrupt=interrupt,
            message_wait_interval_seconds=5,
        )

        assert gateway.wait_for_message_and_move("pending", "processing") is None
        assert client.eval_calls == 0

    def test_sync_interrupt_skips_visibility_timeout_polling(self):
        interrupt = _ManualInterruptHandler()
        interrupt.interrupt()

        class RecordingClient:
            def __init__(self):
                self.eval_calls = 0

            def eval(self, *args, **kwargs):
                self.eval_calls += 1
                return None

        client = RecordingClient()
        gateway = RedisGateway(
            redis_client=client,
            interrupt=interrupt,
            message_wait_interval_seconds=5,
            message_visibility_timeout_seconds=30,
        )

        assert gateway.wait_for_message_and_move("pending", "processing") is None
        assert client.eval_calls == 0

    @pytest.mark.asyncio
    async def test_async_interrupt_returns_none_without_entering_claim_poll(self):
        interrupt = _ManualInterruptHandler()
        interrupt.interrupt()

        class RecordingClient:
            def __init__(self):
                self.eval_calls = 0

            async def eval(self, *args, **kwargs):
                self.eval_calls += 1
                return b"message"

        client = RecordingClient()
        gateway = AsyncRedisGateway(
            redis_client=client,
            interrupt=interrupt,
            message_wait_interval_seconds=5,
        )

        assert await gateway.wait_for_message_and_move("pending", "processing") is None
        assert client.eval_calls == 0

    def test_sync_interrupt_during_pending_claim_recovery_skips_new_non_blocking_claim(self):
        interrupt = _ManualInterruptHandler()
        client = _InterruptDuringRecoverySyncClient(interrupt)
        gateway = RedisGateway(
            redis_client=client,
            interrupt=interrupt,
            message_wait_interval_seconds=0,
        )
        gateway._set_pending_claim_id("processing", "claim-1")

        assert gateway.wait_for_message_and_move("pending", "processing") is None
        assert client.eval_calls == 0

    @pytest.mark.asyncio
    async def test_async_interrupt_during_pending_claim_recovery_skips_new_non_blocking_claim(self):
        interrupt = _ManualInterruptHandler()
        client = _InterruptDuringRecoveryAsyncClient(interrupt)
        gateway = AsyncRedisGateway(
            redis_client=client,
            interrupt=interrupt,
            message_wait_interval_seconds=0,
        )
        gateway._set_pending_claim_id("processing", "claim-1")

        assert await gateway.wait_for_message_and_move("pending", "processing") is None
        assert client.eval_calls == 0

    @pytest.mark.parametrize("use_visibility_timeout", [False, True])
    def test_sync_interrupt_prevents_non_blocking_claim_retry(self, use_visibility_timeout):
        interrupt = _ManualInterruptHandler()
        client = _InterruptingRetryableSyncClient(
            interrupt,
            result=[b"message", b"token"] if use_visibility_timeout else b"message",
        )
        gateway = RedisGateway(
            redis_client=client,
            interrupt=interrupt,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=30 if use_visibility_timeout else None,
        )

        assert gateway.wait_for_message_and_move("pending", "processing") is None
        assert client.eval_calls == 1
        assert gateway._pending_claim_ids["processing"]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("use_visibility_timeout", [False, True])
    async def test_async_interrupt_prevents_non_blocking_claim_retry(self, use_visibility_timeout):
        interrupt = _ManualInterruptHandler()
        client = _InterruptingRetryableAsyncClient(
            interrupt,
            result=[b"message", b"token"] if use_visibility_timeout else b"message",
        )
        gateway = AsyncRedisGateway(
            redis_client=client,
            interrupt=interrupt,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=30 if use_visibility_timeout else None,
        )

        assert await gateway.wait_for_message_and_move("pending", "processing") is None
        assert client.eval_calls == 1
        assert gateway._pending_claim_ids["processing"]

    @pytest.mark.parametrize("use_visibility_timeout", [False, True])
    def test_sync_interrupt_prevents_timeout_boundary_recovery_claim(self, monkeypatch, use_visibility_timeout):
        interrupt = _ManualInterruptHandler()
        client = _InterruptingRetryableSyncClient(
            interrupt,
            result=[b"message", b"token"] if use_visibility_timeout else b"message",
        )
        gateway = RedisGateway(
            redis_client=client,
            interrupt=interrupt,
            message_wait_interval_seconds=1,
            message_visibility_timeout_seconds=30 if use_visibility_timeout else None,
        )
        times = iter([0.0, 1.1])
        monkeypatch.setattr(sync_gateway_module.time, "monotonic", lambda: next(times, 1.1))

        assert gateway.wait_for_message_and_move("pending", "processing") is None
        assert client.eval_calls == 1
        assert gateway._pending_claim_ids["processing"]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("use_visibility_timeout", [False, True])
    async def test_async_interrupt_prevents_timeout_boundary_recovery_claim(self, monkeypatch, use_visibility_timeout):
        interrupt = _ManualInterruptHandler()
        client = _InterruptingRetryableAsyncClient(
            interrupt,
            result=[b"message", b"token"] if use_visibility_timeout else b"message",
        )
        gateway = AsyncRedisGateway(
            redis_client=client,
            interrupt=interrupt,
            message_wait_interval_seconds=1,
            message_visibility_timeout_seconds=30 if use_visibility_timeout else None,
        )

        class _FakeLoop:
            def __init__(self):
                self._times = iter([0.0, 1.1])

            def time(self):
                return next(self._times, 1.1)

        monkeypatch.setattr(async_gateway_module.asyncio, "get_running_loop", lambda: _FakeLoop())

        assert await gateway.wait_for_message_and_move("pending", "processing") is None
        assert client.eval_calls == 1
        assert gateway._pending_claim_ids["processing"]

    @pytest.mark.asyncio
    async def test_async_interrupt_skips_visibility_timeout_polling(self):
        interrupt = _ManualInterruptHandler()
        interrupt.interrupt()

        class RecordingClient:
            def __init__(self):
                self.eval_calls = 0

            async def eval(self, *args, **kwargs):
                self.eval_calls += 1
                return None

        client = RecordingClient()
        gateway = AsyncRedisGateway(
            redis_client=client,
            interrupt=interrupt,
            message_wait_interval_seconds=5,
            message_visibility_timeout_seconds=30,
        )

        assert await gateway.wait_for_message_and_move("pending", "processing") is None
        assert client.eval_calls == 0
