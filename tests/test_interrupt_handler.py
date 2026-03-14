import signal

import pytest

from redis_message_queue.interrupt_handler._implementation import (
    GracefulInterruptHandler,
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

    def test_empty_signals_accepted(self):
        handler = GracefulInterruptHandler(signals=())
        assert handler._signals == ()

    def test_default_signals(self):
        handler = GracefulInterruptHandler()
        assert handler._signals == GracefulInterruptHandler._DEFAULT_SIGNALS
