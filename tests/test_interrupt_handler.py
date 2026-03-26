import signal

import fakeredis
import pytest
import redis.exceptions

from redis_message_queue._redis_gateway import RedisGateway
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
