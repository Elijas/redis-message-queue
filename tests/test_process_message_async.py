import asyncio
import gc
import logging
import re
import sys
import warnings

import fakeredis
import pytest
from redis.cluster import key_slot

from redis_message_queue._exceptions import CleanupFailedError, ConfigurationError
from redis_message_queue._stored_message import ClaimedMessage
from redis_message_queue.asyncio._abstract_redis_gateway import AbstractRedisGateway
from redis_message_queue.asyncio._redis_gateway import RedisGateway
from redis_message_queue.asyncio.redis_message_queue import (
    RedisMessageQueue,
    _append_cancellation_to_context_chain,
    _await_preserving_cancellation,
    _await_suppressing_external_cancellation,
)


class TestConstructorClientValidation:
    def test_falsy_non_none_client_is_accepted(self):
        """A client that is falsy (e.g. __bool__=False) but not None must not be
        rejected by the constructor — it should fail later at point of use, not
        with a misleading 'must be provided' error."""

        class FalsyClient:
            def __bool__(self):
                return False

        try:
            RedisMessageQueue("test", client=FalsyClient())
        except ValueError as e:
            if "must be provided" in str(e):
                pytest.fail(f"Constructor rejected a falsy-but-provided client: {e}")


class TestConstructorNameValidation:
    def test_empty_name_raises_value_error(self):
        gateway = FakeAsyncGateway()
        with pytest.raises(ValueError, match="non-empty"):
            RedisMessageQueue("", gateway=gateway)

    def test_whitespace_only_name_raises_value_error(self):
        gateway = FakeAsyncGateway()
        with pytest.raises(ValueError, match="non-empty"):
            RedisMessageQueue("   ", gateway=gateway)

    def test_none_name_raises_type_error(self):
        gateway = FakeAsyncGateway()
        with pytest.raises(TypeError, match="must be a string"):
            RedisMessageQueue(None, gateway=gateway)


class TestConstructorKeySeparatorValidation:
    def test_none_key_separator_raises_type_error(self):
        gateway = FakeAsyncGateway()
        with pytest.raises(TypeError, match="must be a string"):
            RedisMessageQueue("test", gateway=gateway, key_separator=None)

    def test_int_key_separator_raises_type_error(self):
        gateway = FakeAsyncGateway()
        with pytest.raises(TypeError, match="must be a string"):
            RedisMessageQueue("test", gateway=gateway, key_separator=123)

    def test_bool_key_separator_raises_type_error(self):
        gateway = FakeAsyncGateway()
        with pytest.raises(TypeError, match="must be a string"):
            RedisMessageQueue("test", gateway=gateway, key_separator=True)

    def test_empty_key_separator_raises_value_error(self):
        gateway = FakeAsyncGateway()
        with pytest.raises(ValueError, match="non-empty"):
            RedisMessageQueue("test", gateway=gateway, key_separator="")

    def test_whitespace_only_key_separator_raises_value_error(self):
        gateway = FakeAsyncGateway()
        with pytest.raises(ValueError, match="non-empty"):
            RedisMessageQueue("test", gateway=gateway, key_separator="   ")


class TestConstructorConflictingParameters:
    """When gateway is provided, client and interrupt must not be silently ignored."""

    def test_gateway_and_client_raises(self):
        gateway = FakeAsyncGateway()
        with pytest.raises(ValueError, match="cannot be provided alongside"):
            RedisMessageQueue("test", gateway=gateway, client=object())

    def test_gateway_and_interrupt_raises(self):
        gateway = FakeAsyncGateway()
        with pytest.raises(ValueError, match="cannot be provided alongside"):
            RedisMessageQueue("test", gateway=gateway, interrupt=object())

    def test_gateway_alone_is_accepted(self):
        gateway = FakeAsyncGateway()
        queue = RedisMessageQueue("test", gateway=gateway)
        assert queue._redis is gateway

    def test_client_alone_is_accepted(self):
        class FakeClient:
            pass

        try:
            RedisMessageQueue("test", client=FakeClient())
        except ValueError as e:
            if "cannot be provided alongside" in str(e):
                pytest.fail(f"Constructor rejected client-only usage: {e}")


class TestConstructorGatewayValidation:
    def test_non_gateway_object_raises_type_error(self):
        with pytest.raises(TypeError, match="'gateway' must be an AbstractRedisGateway"):
            RedisMessageQueue("test", gateway=object())


class FakeAsyncGateway(AbstractRedisGateway):
    def __init__(self):
        self.message_to_return = None
        self.fail_on_move = False
        self.fail_on_remove = False
        self.move_exception = ConnectionError("Redis connection lost")
        self.remove_exception = ConnectionError("Redis connection lost")
        self.trim_exception = ConnectionError("Redis connection lost")
        self.removed_messages = []
        self.moved_messages = []
        self.move_attempts = []
        self.remove_attempts = []
        self.trim_attempts = []
        self.move_lease_tokens = []
        self.remove_lease_tokens = []
        self.move_return_value = True
        self.remove_return_value = True
        self.fail_on_trim = False

    async def publish_message(self, queue, message, dedup_key):
        return True

    async def add_message(self, queue, message):
        pass

    async def move_message(self, from_queue, to_queue, message, *, lease_token=None):
        self.move_attempts.append((from_queue, to_queue, message))
        self.move_lease_tokens.append(lease_token)
        if self.fail_on_move:
            raise self.move_exception
        self.moved_messages.append((from_queue, to_queue, message))
        return self.move_return_value

    async def remove_message(self, queue, message, *, lease_token=None):
        self.remove_attempts.append((queue, message))
        self.remove_lease_tokens.append(lease_token)
        if self.fail_on_remove:
            raise self.remove_exception
        self.removed_messages.append((queue, message))
        return self.remove_return_value

    async def renew_message_lease(self, queue, message, lease_token, **_kwargs):
        return True

    async def wait_for_message_and_move(self, from_queue, to_queue):
        return self.message_to_return

    async def trim_queue(self, queue, max_length):
        self.trim_attempts.append((queue, max_length))
        if self.fail_on_trim:
            raise self.trim_exception


class TestConstructorBooleanParameterValidation:
    @pytest.mark.parametrize("invalid_value", ["yes", "false", 1, 0, None, 2.0, []])
    def test_non_bool_deduplication_raises_type_error(self, invalid_value):
        gateway = FakeAsyncGateway()
        with pytest.raises(TypeError, match="'deduplication' must be a bool"):
            RedisMessageQueue("test", gateway=gateway, deduplication=invalid_value)

    @pytest.mark.parametrize("invalid_value", ["yes", "false", 1, 0, None, 2.0, []])
    def test_non_bool_enable_completed_queue_raises_type_error(self, invalid_value):
        gateway = FakeAsyncGateway()
        with pytest.raises(TypeError, match="'enable_completed_queue' must be a bool"):
            RedisMessageQueue("test", gateway=gateway, enable_completed_queue=invalid_value)

    @pytest.mark.parametrize("invalid_value", ["yes", "false", 1, 0, None, 2.0, []])
    def test_non_bool_enable_failed_queue_raises_type_error(self, invalid_value):
        gateway = FakeAsyncGateway()
        with pytest.raises(TypeError, match="'enable_failed_queue' must be a bool"):
            RedisMessageQueue("test", gateway=gateway, enable_failed_queue=invalid_value)

    def test_true_is_accepted(self):
        gateway = FakeAsyncGateway()
        q = RedisMessageQueue(
            "test",
            gateway=gateway,
            deduplication=True,
            get_deduplication_key=lambda msg: msg,
            enable_completed_queue=True,
            enable_failed_queue=True,
        )
        assert q._deduplication is True
        assert q._enable_completed_queue is True
        assert q._enable_failed_queue is True

    def test_false_is_accepted(self):
        gateway = FakeAsyncGateway()
        q = RedisMessageQueue(
            "test",
            gateway=gateway,
            deduplication=False,
            enable_completed_queue=False,
            enable_failed_queue=False,
        )
        assert q._deduplication is False
        assert q._enable_completed_queue is False
        assert q._enable_failed_queue is False


class TestConstructorGetDeduplicationKeyValidation:
    @pytest.mark.parametrize("invalid_value", [42, "not_callable", True, 3.14, [1, 2], {"a": 1}])
    def test_non_callable_get_deduplication_key_raises_type_error(self, invalid_value):
        gateway = FakeAsyncGateway()
        with pytest.raises(TypeError, match="'get_deduplication_key' must be callable"):
            RedisMessageQueue("test", gateway=gateway, get_deduplication_key=invalid_value)

    def test_none_is_accepted(self):
        gateway = FakeAsyncGateway()
        q = RedisMessageQueue("test", gateway=gateway, get_deduplication_key=None)
        assert q._get_deduplication_key is None

    def test_lambda_is_accepted(self):
        gateway = FakeAsyncGateway()
        fn = lambda msg: msg
        q = RedisMessageQueue("test", gateway=gateway, deduplication=True, get_deduplication_key=fn)
        assert q._get_deduplication_key is fn

    def test_callable_object_is_accepted(self):
        class MyCallable:
            def __call__(self, msg):
                return msg

        gateway = FakeAsyncGateway()
        obj = MyCallable()
        q = RedisMessageQueue("test", gateway=gateway, deduplication=True, get_deduplication_key=obj)
        assert q._get_deduplication_key is obj


class TestConstructorDeduplicationContradiction:
    def test_dedup_true_without_callable_raises_configuration_error(self):
        gateway = FakeAsyncGateway()
        with pytest.raises(ConfigurationError) as exc_info:
            RedisMessageQueue("test", gateway=gateway, deduplication=True)

        assert str(exc_info.value) == (
            "deduplication=True requires get_deduplication_key (callable returning a non-empty str). "
            "Pass a callable like `lambda msg: msg['id']` (recommended: a stable logical ID), "
            "or set deduplication=False."
        )

    def test_dedup_disabled_with_callback_raises_value_error(self):
        gateway = FakeAsyncGateway()
        with pytest.raises(ValueError, match="cannot be provided when 'deduplication' is disabled"):
            RedisMessageQueue(
                "test",
                gateway=gateway,
                deduplication=False,
                get_deduplication_key=lambda msg: msg,
            )

    def test_dedup_enabled_with_callback_is_accepted(self):
        gateway = FakeAsyncGateway()
        q = RedisMessageQueue(
            "test",
            gateway=gateway,
            deduplication=True,
            get_deduplication_key=lambda msg: msg,
        )
        assert q._deduplication is True
        assert q._get_deduplication_key is not None

    def test_dedup_disabled_without_callback_is_accepted(self):
        gateway = FakeAsyncGateway()
        q = RedisMessageQueue(
            "test",
            gateway=gateway,
            deduplication=False,
            get_deduplication_key=None,
        )
        assert q._deduplication is False
        assert q._get_deduplication_key is None


class TestProcessMessageExceptionPropagation:
    @pytest.mark.asyncio
    async def test_user_exception_propagates_when_remove_fails(self):
        gateway = FakeAsyncGateway()
        gateway.message_to_return = b"test-message"
        gateway.fail_on_remove = True
        queue = RedisMessageQueue("test", gateway=gateway)

        with pytest.warns(RuntimeWarning, match=r"Cleanup raised after handler exception \(ConnectionError\)"):
            with pytest.raises(ValueError, match="original error"):
                async with queue.process_message() as _msg:
                    raise ValueError("original error")

    @pytest.mark.asyncio
    async def test_user_exception_propagates_when_move_to_failed_fails(self):
        gateway = FakeAsyncGateway()
        gateway.message_to_return = b"test-message"
        gateway.fail_on_move = True
        queue = RedisMessageQueue("test", gateway=gateway, enable_failed_queue=True)

        with pytest.warns(RuntimeWarning, match=r"Cleanup raised after handler exception \(ConnectionError\)"):
            with pytest.raises(ValueError, match="original error"):
                async with queue.process_message() as _msg:
                    raise ValueError("original error")

    @pytest.mark.asyncio
    async def test_keyboard_interrupt_propagates_when_cleanup_fails(self):
        gateway = FakeAsyncGateway()
        gateway.message_to_return = b"test-message"
        gateway.fail_on_remove = True
        queue = RedisMessageQueue("test", gateway=gateway)

        with pytest.raises(KeyboardInterrupt):
            async with queue.process_message() as _msg:
                raise KeyboardInterrupt()

    @pytest.mark.asyncio
    async def test_user_exception_still_cleans_up_when_possible(self):
        gateway = FakeAsyncGateway()
        gateway.message_to_return = b"test-message"
        queue = RedisMessageQueue("test", gateway=gateway)

        with pytest.raises(ValueError):
            async with queue.process_message() as _msg:
                raise ValueError("boom")

        assert len(gateway.removed_messages) == 1

    @pytest.mark.asyncio
    async def test_user_exception_moves_to_failed_queue_when_enabled(self):
        gateway = FakeAsyncGateway()
        gateway.message_to_return = b"test-message"
        queue = RedisMessageQueue("test", gateway=gateway, enable_failed_queue=True)

        with pytest.raises(ValueError):
            async with queue.process_message() as _msg:
                raise ValueError("boom")

        assert len(gateway.moved_messages) == 1
        _, to_queue, _ = gateway.moved_messages[0]
        assert to_queue == queue.key.failed

    @pytest.mark.asyncio
    async def test_external_cancellation_during_failure_cleanup_preserves_original_error(self):
        client = fakeredis.FakeAsyncRedis()
        gateway = RedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
        )
        queue = RedisMessageQueue("test", gateway=gateway, deduplication=False)
        await queue.publish("test-message")

        cleanup_started = asyncio.Event()
        allow_cleanup_to_finish = asyncio.Event()
        original_remove = gateway.remove_message

        async def slow_remove(*args, **kwargs):
            cleanup_started.set()
            await allow_cleanup_to_finish.wait()
            return await original_remove(*args, **kwargs)

        gateway.remove_message = slow_remove

        async def worker():
            async with queue.process_message() as msg:
                assert msg == b"test-message"
                raise ValueError("original error")

        task = asyncio.create_task(worker())
        await cleanup_started.wait()
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()

        allow_cleanup_to_finish.set()

        with pytest.raises(ValueError, match="original error"):
            await task

        assert await client.llen(queue.key.pending) == 0
        assert await client.llen(queue.key.processing) == 0


class TestAppendCancellationToContextChain:
    """Unit tests for the append-only context-chain helper that lets a
    suppressed cancellation ride the re-raised processing error."""

    def test_none_cancel_is_noop(self):
        error = ValueError("boom")
        _append_cancellation_to_context_chain(error, None)
        assert error.__context__ is None

    def test_appends_to_error_with_no_context(self):
        error = ValueError("boom")
        cancel = asyncio.CancelledError()
        _append_cancellation_to_context_chain(error, cancel)
        assert error.__context__ is cancel

    def test_appends_at_end_without_overwriting_existing_context(self):
        inner = RuntimeError("inner")
        error = ValueError("boom")
        error.__context__ = inner
        cancel = asyncio.CancelledError()
        _append_cancellation_to_context_chain(error, cancel)
        # The pre-existing link is preserved; the cancel lands at the tail.
        assert error.__context__ is inner
        assert inner.__context__ is cancel

    def test_cancel_already_in_chain_is_noop(self):
        cancel = asyncio.CancelledError()
        error = ValueError("boom")
        error.__context__ = cancel
        _append_cancellation_to_context_chain(error, cancel)
        # No second link was created and no cycle introduced.
        assert error.__context__ is cancel
        assert cancel.__context__ is None

    def test_cancel_is_error_itself_is_noop(self):
        cancel = asyncio.CancelledError()
        _append_cancellation_to_context_chain(cancel, cancel)
        assert cancel.__context__ is None

    def test_preexisting_cycle_is_left_untouched(self):
        a = ValueError("a")
        b = RuntimeError("b")
        a.__context__ = b
        b.__context__ = a  # cycle
        cancel = asyncio.CancelledError()
        _append_cancellation_to_context_chain(a, cancel)
        # The helper must not append into a cyclic chain (would never reach a
        # tail) and must not raise.
        assert a.__context__ is b
        assert b.__context__ is a


class TestTimeoutObservabilityOnNackPath:
    """A cancellation/``asyncio.timeout`` deadline that lands during
    failure-path cleanup must ride the re-raised handler error's context chain,
    so stdlib timeout machinery (3.13+) can splice a ``TimeoutError`` into it.
    """

    async def _run_timeout_mid_nack(self, *, fail_remove: bool):
        """Drive a real ``asyncio.timeout`` that expires while nack cleanup is
        gated. Returns the user-visible exception raised by the block."""

        client = fakeredis.FakeAsyncRedis()
        gateway = RedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
        )
        queue = RedisMessageQueue("test", gateway=gateway, deduplication=False)
        await queue.publish("test-message")

        cleanup_started = asyncio.Event()
        allow_cleanup_to_finish = asyncio.Event()
        original_remove = gateway.remove_message

        async def slow_remove(*args, **kwargs):
            cleanup_started.set()
            await allow_cleanup_to_finish.wait()
            if fail_remove:
                raise ConnectionError("nack failed")
            return await original_remove(*args, **kwargs)

        gateway.remove_message = slow_remove

        captured = {}

        async def worker():
            try:
                async with asyncio.timeout(0.05):
                    async with queue.process_message() as msg:
                        assert msg == b"test-message"
                        raise ValueError("original error")
            except BaseException as exc:  # noqa: BLE001 - capturing for assertion
                captured["exc"] = exc

        task = asyncio.create_task(worker())
        await cleanup_started.wait()
        # Let the asyncio.timeout deadline fire while cleanup is gated.
        await asyncio.sleep(0.1)
        allow_cleanup_to_finish.set()
        await task
        return captured["exc"], client, queue

    @staticmethod
    def _chain(exc):
        nodes = []
        seen = set()
        node = exc.__context__
        while node is not None and id(node) not in seen:
            seen.add(id(node))
            nodes.append(node)
            node = node.__context__
        return nodes

    @pytest.mark.asyncio
    async def test_timeout_mid_nack_success_surfaces_cancel_in_chain(self):
        exc, client, queue = await self._run_timeout_mid_nack(fail_remove=False)
        # The user-visible exception is still the handler ValueError.
        assert isinstance(exc, ValueError)
        assert str(exc) == "original error"
        chain = self._chain(exc)
        # The swallowed CancelledError is discoverable on every version.
        assert any(isinstance(n, asyncio.CancelledError) for n in chain)
        # On 3.13+ stdlib splices a TimeoutError ahead of that CancelledError.
        if sys.version_info >= (3, 13):
            assert any(isinstance(n, TimeoutError) for n in chain)
        # Cleanup committed: nothing left in processing.
        assert await client.llen(queue.key.processing) == 0

    @pytest.mark.asyncio
    async def test_timeout_mid_nack_failure_surfaces_cancel_in_chain(self):
        # nack itself fails: the suppressor re-raises before returning, so no
        # cancel is captured for the handler error — but the timeout deadline
        # still must not be lost. Here the handler error re-raises and the
        # cleanup ConnectionError is chained; the deadline manifests per stdlib.
        exc, client, queue = await self._run_timeout_mid_nack(fail_remove=True)
        assert isinstance(exc, ValueError)
        assert str(exc) == "original error"
        # The message stays in processing for visibility-timeout reclaim.
        assert await client.llen(queue.key.processing) == 1

    @pytest.mark.asyncio
    async def test_no_cancel_leaves_chain_unmutated(self):
        # Plain handler failure, no timeout/cancel: the re-raised error's
        # context chain must not gain a spurious CancelledError.
        client = fakeredis.FakeAsyncRedis()
        gateway = RedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
        )
        queue = RedisMessageQueue("test", gateway=gateway, deduplication=False)
        await queue.publish("test-message")

        with pytest.raises(ValueError, match="original error") as exc_info:
            async with queue.process_message() as _msg:
                raise ValueError("original error")

        chain = self._chain(exc_info.value)
        assert not any(isinstance(n, asyncio.CancelledError) for n in chain)
        assert await client.llen(queue.key.processing) == 0

    @pytest.mark.asyncio
    async def test_handler_error_with_own_context_is_preserved(self):
        # The handler error already carries its own __context__ (a nested raise);
        # the suppressed cancel must append at the tail, leaving the existing
        # link intact.
        client = fakeredis.FakeAsyncRedis()
        gateway = RedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
        )
        queue = RedisMessageQueue("test", gateway=gateway, deduplication=False)
        await queue.publish("test-message")

        cleanup_started = asyncio.Event()
        allow_cleanup_to_finish = asyncio.Event()
        original_remove = gateway.remove_message

        async def slow_remove(*args, **kwargs):
            cleanup_started.set()
            await allow_cleanup_to_finish.wait()
            return await original_remove(*args, **kwargs)

        gateway.remove_message = slow_remove

        captured = {}

        async def worker():
            try:
                async with asyncio.timeout(0.05):
                    async with queue.process_message() as _msg:
                        try:
                            raise KeyError("nested cause")
                        except KeyError:
                            raise ValueError("original error")
            except BaseException as exc:  # noqa: BLE001
                captured["exc"] = exc

        task = asyncio.create_task(worker())
        await cleanup_started.wait()
        await asyncio.sleep(0.1)
        allow_cleanup_to_finish.set()
        await task

        exc = captured["exc"]
        assert isinstance(exc, ValueError)
        chain = self._chain(exc)
        # The pre-existing KeyError link survived.
        assert any(isinstance(n, KeyError) for n in chain)
        # The cancel was appended (somewhere after the KeyError) on every version.
        assert any(isinstance(n, asyncio.CancelledError) for n in chain)

    @pytest.mark.asyncio
    async def test_double_cancel_still_swallows_and_reraises_handler_error(self):
        # A second cancel escapes the suppressor; the handler error must still
        # re-raise (control flow unchanged from before the observability fix).
        client = fakeredis.FakeAsyncRedis()
        gateway = RedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
        )
        queue = RedisMessageQueue("test", gateway=gateway, deduplication=False)
        await queue.publish("test-message")

        cleanup_started = asyncio.Event()
        allow_cleanup_to_finish = asyncio.Event()
        original_remove = gateway.remove_message

        async def slow_remove(*args, **kwargs):
            cleanup_started.set()
            await allow_cleanup_to_finish.wait()
            return await original_remove(*args, **kwargs)

        gateway.remove_message = slow_remove

        async def worker():
            async with queue.process_message() as _msg:
                raise ValueError("original error")

        task = asyncio.create_task(worker())
        await cleanup_started.wait()
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()  # second cancel escapes the suppressor
        await asyncio.sleep(0)
        allow_cleanup_to_finish.set()
        with pytest.raises(ValueError, match="original error"):
            await task
        # The second cancel escaped the suppressor, so the nack op never
        # committed; the message stays in processing for visibility-timeout
        # reclaim. The handler ValueError re-raises regardless (control flow
        # unchanged by the observability fix).
        assert await client.llen(queue.key.processing) == 1


class TestProcessMessageCleanupBaseException:
    """When nack cleanup raises a fatal shutdown exception (KeyboardInterrupt /
    SystemExit / GeneratorExit), that exception must propagate so a Ctrl-C is
    not swallowed; the handler exception is chained as __context__.

    asyncio.CancelledError is deliberately not covered here: a second-cancellation
    CancelledError escaping ``_await_suppressing_external_cancellation`` must keep
    being swallowed so the original processing error re-raises (see
    ``test_external_cancellation_during_failure_cleanup_preserves_original_error``)."""

    @pytest.mark.asyncio
    async def test_keyboard_interrupt_in_remove_cleanup_propagates(self):
        gateway = FakeAsyncGateway()
        gateway.message_to_return = b"test-message"
        gateway.fail_on_remove = True
        gateway.remove_exception = KeyboardInterrupt()
        queue = RedisMessageQueue("test", gateway=gateway)

        with pytest.raises(KeyboardInterrupt) as exc_info:
            async with queue.process_message() as _msg:
                raise ValueError("original error")

        assert isinstance(exc_info.value.__context__, ValueError)
        assert str(exc_info.value.__context__) == "original error"

    @pytest.mark.asyncio
    async def test_keyboard_interrupt_in_move_to_failed_cleanup_propagates(self):
        gateway = FakeAsyncGateway()
        gateway.message_to_return = b"test-message"
        gateway.fail_on_move = True
        gateway.move_exception = KeyboardInterrupt()
        queue = RedisMessageQueue("test", gateway=gateway, enable_failed_queue=True)

        with pytest.raises(KeyboardInterrupt) as exc_info:
            async with queue.process_message() as _msg:
                raise ValueError("original error")

        assert isinstance(exc_info.value.__context__, ValueError)
        assert str(exc_info.value.__context__) == "original error"

    @pytest.mark.asyncio
    async def test_system_exit_in_remove_cleanup_propagates(self):
        gateway = FakeAsyncGateway()
        gateway.message_to_return = b"test-message"
        gateway.fail_on_remove = True
        gateway.remove_exception = SystemExit(1)
        queue = RedisMessageQueue("test", gateway=gateway)

        with pytest.raises(SystemExit) as exc_info:
            async with queue.process_message() as _msg:
                raise ValueError("original error")

        assert isinstance(exc_info.value.__context__, ValueError)

    @pytest.mark.asyncio
    async def test_system_exit_in_move_to_failed_cleanup_propagates(self):
        gateway = FakeAsyncGateway()
        gateway.message_to_return = b"test-message"
        gateway.fail_on_move = True
        gateway.move_exception = SystemExit(1)
        queue = RedisMessageQueue("test", gateway=gateway, enable_failed_queue=True)

        with pytest.raises(SystemExit) as exc_info:
            async with queue.process_message() as _msg:
                raise ValueError("original error")

        assert isinstance(exc_info.value.__context__, ValueError)

    @pytest.mark.asyncio
    async def test_fatal_cleanup_exception_emits_cleanup_failed_event(self):
        events = []

        async def record(event):
            events.append(event)

        gateway = FakeAsyncGateway()
        gateway.message_to_return = b"test-message"
        gateway.fail_on_remove = True
        gateway.remove_exception = KeyboardInterrupt()
        queue = RedisMessageQueue("test", gateway=gateway, on_event=record)

        with pytest.raises(KeyboardInterrupt):
            async with queue.process_message() as _msg:
                raise ValueError("original error")

        cleanup_failed = [e for e in events if e.operation == "cleanup_failed"]
        assert len(cleanup_failed) == 1
        assert cleanup_failed[0].outcome == "failure"

    @pytest.mark.asyncio
    async def test_regular_cleanup_exception_still_warns_and_propagates_handler_error(self):
        gateway = FakeAsyncGateway()
        gateway.message_to_return = b"test-message"
        gateway.fail_on_remove = True
        gateway.remove_exception = ConnectionError("Redis connection lost")
        queue = RedisMessageQueue("test", gateway=gateway)

        with pytest.warns(RuntimeWarning, match=r"Cleanup raised after handler exception \(ConnectionError\)"):
            with pytest.raises(ValueError, match="original error"):
                async with queue.process_message() as _msg:
                    raise ValueError("original error")


class TestProcessMessageNackEmitExceptionNotMisreported:
    """An exception arriving in a post-cleanup emit on the nack path must not be
    misreported as a cleanup failure: the Redis nack already committed, so no
    ``cleanup_failed`` event may follow the ``nack`` success event."""

    @pytest.mark.asyncio
    async def test_cancel_in_nack_success_emit_does_not_emit_cleanup_failed(self):
        events: list = []
        emit_reached_nack = asyncio.Event()
        release_nack_emit = asyncio.Event()

        async def on_event(event):
            events.append((event.operation.value, event.outcome.value))
            if event.operation.value == "nack":
                emit_reached_nack.set()
                await release_nack_emit.wait()

        gateway = FakeAsyncGateway()
        gateway.message_to_return = b"test-message"
        queue = RedisMessageQueue("test", gateway=gateway, on_event=on_event)

        async def worker():
            async with queue.process_message() as _msg:
                raise ValueError("handler boom")

        task = asyncio.create_task(worker())
        await emit_reached_nack.wait()
        # nack already committed; cancel lands inside on_event during the emit.
        task.cancel()
        await asyncio.sleep(0)
        release_nack_emit.set()

        with pytest.raises(ValueError, match="handler boom"):
            await task

        assert not task.cancelled()
        operations = [operation for operation, _outcome in events]
        assert "nack" in operations
        assert "cleanup_failed" not in operations
        # cleanup op committed exactly once
        assert len(gateway.removed_messages) == 1

    @pytest.mark.asyncio
    async def test_genuine_cleanup_op_failure_still_emits_cleanup_failed(self):
        events: list = []

        async def on_event(event):
            events.append((event.operation.value, event.outcome.value))

        gateway = FakeAsyncGateway()
        gateway.message_to_return = b"test-message"
        gateway.fail_on_remove = True
        gateway.remove_exception = ConnectionError("Redis connection lost")
        queue = RedisMessageQueue("test", gateway=gateway, on_event=on_event)

        with pytest.warns(RuntimeWarning, match=r"Cleanup raised after handler exception"):
            with pytest.raises(ValueError, match="handler boom"):
                async with queue.process_message() as _msg:
                    raise ValueError("handler boom")

        operations = [operation for operation, _outcome in events]
        assert ("cleanup_failed", "failure") in events
        assert "nack" not in operations


class TestProcessMessageFatalBaseException:
    @pytest.mark.parametrize("exception_factory", [KeyboardInterrupt, lambda: SystemExit(1)])
    @pytest.mark.asyncio
    async def test_visibility_timeout_fatal_exit_skips_cleanup(self, exception_factory):
        client = fakeredis.FakeAsyncRedis()
        gateway = RedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=30,
        )
        queue = RedisMessageQueue("test", gateway=gateway, deduplication=False, enable_failed_queue=True)
        await queue.publish("test-message")

        with pytest.raises(BaseException) as exc_info:
            async with queue.process_message() as msg:
                assert msg == b"test-message"
                raise exception_factory()

        assert isinstance(exc_info.value, (KeyboardInterrupt, SystemExit))
        assert await client.llen(queue.key.pending) == 0
        assert await client.llen(queue.key.failed) == 0
        assert await client.llen(queue.key.processing) == 1


class TestProcessMessageNoneHandling:
    @pytest.mark.asyncio
    async def test_timeout_yields_none(self):
        gateway = FakeAsyncGateway()
        gateway.message_to_return = None
        queue = RedisMessageQueue("test", gateway=gateway)

        async with queue.process_message() as msg:
            assert msg is None

    @pytest.mark.asyncio
    async def test_empty_bytes_message_is_not_swallowed(self):
        gateway = FakeAsyncGateway()
        gateway.message_to_return = b""
        queue = RedisMessageQueue("test", gateway=gateway)

        async with queue.process_message() as msg:
            assert msg == b""

    @pytest.mark.asyncio
    async def test_empty_bytes_message_is_cleaned_up_on_success(self):
        gateway = FakeAsyncGateway()
        gateway.message_to_return = b""
        queue = RedisMessageQueue("test", gateway=gateway)

        async with queue.process_message() as _msg:
            pass

        assert len(gateway.removed_messages) == 1


class TestProcessMessageSuccessPath:
    @pytest.mark.asyncio
    async def test_success_removes_from_processing(self):
        gateway = FakeAsyncGateway()
        gateway.message_to_return = b"test-message"
        queue = RedisMessageQueue("test", gateway=gateway)

        async with queue.process_message() as msg:
            assert msg == b"test-message"

        assert len(gateway.removed_messages) == 1
        removed_queue, _ = gateway.removed_messages[0]
        assert removed_queue == queue.key.processing

    @pytest.mark.asyncio
    async def test_success_moves_to_completed_when_enabled(self):
        gateway = FakeAsyncGateway()
        gateway.message_to_return = b"test-message"
        queue = RedisMessageQueue("test", gateway=gateway, enable_completed_queue=True)

        async with queue.process_message() as _msg:
            pass

        assert len(gateway.moved_messages) == 1
        _, to_queue, _ = gateway.moved_messages[0]
        assert to_queue == queue.key.completed

    @pytest.mark.asyncio
    async def test_timeout_does_not_clean_up(self):
        gateway = FakeAsyncGateway()
        gateway.message_to_return = None
        queue = RedisMessageQueue("test", gateway=gateway)

        async with queue.process_message() as _msg:
            pass

        assert len(gateway.removed_messages) == 0
        assert len(gateway.moved_messages) == 0

    @pytest.mark.asyncio
    async def test_external_cancellation_waits_for_success_cleanup(self):
        client = fakeredis.FakeAsyncRedis()
        gateway = RedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
        )
        queue = RedisMessageQueue("test", gateway=gateway, deduplication=False)
        await queue.publish("test-message")

        cleanup_started = asyncio.Event()
        allow_cleanup_to_finish = asyncio.Event()
        original_remove = gateway.remove_message

        async def slow_remove(*args, **kwargs):
            cleanup_started.set()
            await allow_cleanup_to_finish.wait()
            return await original_remove(*args, **kwargs)

        gateway.remove_message = slow_remove

        async def worker():
            async with queue.process_message() as msg:
                assert msg == b"test-message"

        task = asyncio.create_task(worker())
        await cleanup_started.wait()
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()

        allow_cleanup_to_finish.set()

        with pytest.raises(asyncio.CancelledError):
            await task

        assert await client.llen(queue.key.pending) == 0
        assert await client.llen(queue.key.processing) == 0

    @pytest.mark.asyncio
    async def test_external_cancellation_during_handler_leaves_message_in_processing(self):
        client = fakeredis.FakeAsyncRedis()
        gateway = RedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=30,
        )
        queue = RedisMessageQueue(
            "test",
            gateway=gateway,
            deduplication=False,
            enable_failed_queue=True,
        )
        await queue.publish("test-message")

        entered_handler = asyncio.Event()

        async def worker():
            async with queue.process_message() as msg:
                assert msg == b"test-message"
                entered_handler.set()
                await asyncio.sleep(3600)

        task = asyncio.create_task(worker())
        await entered_handler.wait()
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

        assert await client.llen(queue.key.pending) == 0
        assert await client.llen(queue.key.processing) == 1
        assert await client.llen(queue.key.failed) == 0
        assert await client.hlen(f"{queue.key.processing}:lease_tokens") == 1
        assert await client.zcard(f"{queue.key.processing}:lease_deadlines") == 1


class TestProcessMessageSuccessCleanupFailure:
    """When user code succeeds but post-success Redis cleanup fails,
    the message must NOT be routed to the failed queue."""

    @pytest.mark.asyncio
    async def test_move_to_completed_failure_does_not_move_to_failed(self):
        gateway = FakeAsyncGateway()
        gateway.message_to_return = b"test-message"
        gateway.fail_on_move = True
        queue = RedisMessageQueue(
            "test",
            gateway=gateway,
            enable_completed_queue=True,
            enable_failed_queue=True,
        )

        with pytest.raises(CleanupFailedError) as caught:
            async with queue.process_message() as _msg:
                pass

        assert caught.value.__cause__ is gateway.move_exception
        assert len(gateway.move_attempts) == 1
        assert gateway.move_attempts[0][1] == queue.key.completed

    @pytest.mark.asyncio
    async def test_remove_failure_does_not_move_to_failed(self):
        gateway = FakeAsyncGateway()
        gateway.message_to_return = b"test-message"
        gateway.fail_on_remove = True
        queue = RedisMessageQueue(
            "test",
            gateway=gateway,
            enable_completed_queue=False,
            enable_failed_queue=True,
        )

        with pytest.raises(CleanupFailedError) as caught:
            async with queue.process_message() as _msg:
                pass

        assert caught.value.__cause__ is gateway.remove_exception
        assert len(gateway.remove_attempts) == 1
        assert len(gateway.move_attempts) == 0


class TestProcessMessageWithLeaseToken:
    @pytest.mark.asyncio
    async def test_success_passes_lease_token_to_remove(self):
        gateway = FakeAsyncGateway()
        gateway.message_to_return = ClaimedMessage(stored_message=b"msg", lease_token="tk1")
        queue = RedisMessageQueue("test", gateway=gateway)

        async with queue.process_message() as _msg:
            pass

        assert len(gateway.removed_messages) == 1
        assert gateway.remove_lease_tokens == ["tk1"]

    @pytest.mark.asyncio
    async def test_success_passes_lease_token_to_move_completed(self):
        gateway = FakeAsyncGateway()
        gateway.message_to_return = ClaimedMessage(stored_message=b"msg", lease_token="tk1")
        queue = RedisMessageQueue("test", gateway=gateway, enable_completed_queue=True)

        async with queue.process_message() as _msg:
            pass

        assert len(gateway.moved_messages) == 1
        assert gateway.move_lease_tokens == ["tk1"]

    @pytest.mark.asyncio
    async def test_failure_passes_lease_token_to_move_failed(self):
        gateway = FakeAsyncGateway()
        gateway.message_to_return = ClaimedMessage(stored_message=b"msg", lease_token="tk1")
        queue = RedisMessageQueue("test", gateway=gateway, enable_failed_queue=True)

        with pytest.raises(ValueError):
            async with queue.process_message() as _msg:
                raise ValueError("boom")

        assert len(gateway.moved_messages) == 1
        _, to_queue, _ = gateway.moved_messages[0]
        assert to_queue == queue.key.failed
        assert gateway.move_lease_tokens == ["tk1"]

    @pytest.mark.asyncio
    async def test_failure_passes_lease_token_to_remove(self):
        gateway = FakeAsyncGateway()
        gateway.message_to_return = ClaimedMessage(stored_message=b"msg", lease_token="tk1")
        queue = RedisMessageQueue("test", gateway=gateway)

        with pytest.raises(ValueError):
            async with queue.process_message() as _msg:
                raise ValueError("boom")

        assert len(gateway.removed_messages) == 1
        assert gateway.remove_lease_tokens == ["tk1"]

    @pytest.mark.asyncio
    async def test_yields_decoded_stored_message_not_claimed_message(self):
        gateway = FakeAsyncGateway()
        gateway.message_to_return = ClaimedMessage(stored_message=b"msg", lease_token="tk1")
        queue = RedisMessageQueue("test", gateway=gateway)

        async with queue.process_message() as msg:
            assert msg == b"msg"
            assert not isinstance(msg, ClaimedMessage)


class TestProcessMessageStaleLease:
    @pytest.mark.asyncio
    async def test_success_path_logs_warning_when_remove_returns_false(self, caplog):
        gateway = FakeAsyncGateway()
        gateway.message_to_return = ClaimedMessage(stored_message=b"msg", lease_token="tk1")
        gateway.remove_return_value = False
        queue = RedisMessageQueue("test", gateway=gateway)

        with pytest.warns(RuntimeWarning, match="lease expired"):
            with caplog.at_level(logging.WARNING, logger="redis_message_queue.asyncio.redis_message_queue"):
                async with queue.process_message() as _msg:
                    pass

        assert any("lease expired" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_success_path_logs_warning_when_move_to_completed_returns_false(self, caplog):
        gateway = FakeAsyncGateway()
        gateway.message_to_return = ClaimedMessage(stored_message=b"msg", lease_token="tk1")
        gateway.move_return_value = False
        queue = RedisMessageQueue("test", gateway=gateway, enable_completed_queue=True)

        with pytest.warns(RuntimeWarning, match="lease expired"):
            with caplog.at_level(logging.WARNING, logger="redis_message_queue.asyncio.redis_message_queue"):
                async with queue.process_message() as _msg:
                    pass

        assert any("lease expired" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_success_path_no_warning_without_lease_token(self, caplog):
        gateway = FakeAsyncGateway()
        gateway.message_to_return = b"msg"
        gateway.remove_return_value = False
        queue = RedisMessageQueue("test", gateway=gateway)

        with caplog.at_level(logging.WARNING, logger="redis_message_queue.asyncio.redis_message_queue"):
            async with queue.process_message() as _msg:
                pass

        assert not any("lease expired" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_failure_path_stale_lease_does_not_mask_user_exception(self):
        gateway = FakeAsyncGateway()
        gateway.message_to_return = ClaimedMessage(stored_message=b"msg", lease_token="tk1")
        gateway.remove_return_value = False
        queue = RedisMessageQueue("test", gateway=gateway)

        with pytest.warns(RuntimeWarning, match="lease expired"):
            with pytest.raises(ValueError, match="original error"):
                async with queue.process_message() as _msg:
                    raise ValueError("original error")

    @pytest.mark.asyncio
    async def test_cleanup_double_fault_emits_runtime_warning(self):
        gateway = FakeAsyncGateway()
        gateway.message_to_return = ClaimedMessage(stored_message=b"msg", lease_token="tk1")
        gateway.fail_on_remove = True
        queue = RedisMessageQueue("test", gateway=gateway)

        with pytest.warns(RuntimeWarning, match=r"Cleanup raised after handler exception \(ConnectionError\)"):
            with pytest.raises(ValueError, match="original error"):
                async with queue.process_message() as _msg:
                    raise ValueError("original error")

    @pytest.mark.asyncio
    async def test_trim_failure_emits_runtime_warning(self):
        gateway = FakeAsyncGateway()
        gateway.message_to_return = b"msg"
        gateway.fail_on_trim = True
        queue = RedisMessageQueue("test", gateway=gateway, enable_completed_queue=True, max_completed_length=1)

        with pytest.warns(RuntimeWarning, match=r"Failed to trim queue .* \(ConnectionError\).*max_\*_length"):
            async with queue.process_message() as _msg:
                pass

        assert gateway.trim_attempts == [(queue.key.completed, 1)]

    @pytest.mark.asyncio
    async def test_strict_runtime_warning_filter_keeps_trim_failure_advisory(self):
        events = []

        async def observe(event):
            events.append(event)

        gateway = FakeAsyncGateway()
        gateway.message_to_return = b"msg"
        gateway.fail_on_trim = True
        queue = RedisMessageQueue(
            "test",
            gateway=gateway,
            enable_completed_queue=True,
            max_completed_length=1,
            on_event=observe,
        )

        with pytest.warns(RuntimeWarning, match=r"Failed to trim queue .*max_\*_length"):
            with warnings.catch_warnings():
                warnings.simplefilter("error", RuntimeWarning)
                async with queue.process_message() as _msg:
                    pass

        operations = [(event.operation.value, event.outcome.value) for event in events]
        assert ("trim_failed", "failure") in operations
        assert ("completed", "success") in operations
        assert ("ack", "success") in operations
        assert "cleanup_failed" not in [operation for operation, _outcome in operations]

    @pytest.mark.asyncio
    async def test_strict_runtime_warning_filter_keeps_stale_failure_warning_advisory(self):
        events = []

        async def observe(event):
            events.append(event)

        gateway = FakeAsyncGateway()
        gateway.message_to_return = ClaimedMessage(stored_message=b"msg", lease_token="tk1")
        gateway.remove_return_value = False
        queue = RedisMessageQueue("test", gateway=gateway, on_event=observe)

        with pytest.warns(RuntimeWarning, match="failed processing"):
            with warnings.catch_warnings():
                warnings.simplefilter("error", RuntimeWarning)
                with pytest.raises(ValueError, match="original error"):
                    async with queue.process_message() as _msg:
                        raise ValueError("original error")

        operations = [(event.operation.value, event.outcome.value) for event in events]
        assert ("failed", "failure") in operations
        assert ("nack", "skipped") in operations
        assert ("stale_lease_nack", "skipped") in operations
        assert "cleanup_failed" not in [operation for operation, _outcome in operations]


class TestAtMostOnceMessageLoss:
    """F1: Without visibility_timeout_seconds (the default), a consumer crash
    after claiming a message leaves it permanently orphaned in the processing
    queue. There is no reclaim mechanism, no TTL, no warning. This is
    at-most-once delivery by design."""

    @pytest.mark.asyncio
    async def test_crashed_consumer_orphans_message_permanently(self):
        client = fakeredis.FakeAsyncRedis()
        gateway = RedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
        )
        queue = RedisMessageQueue("test", gateway=gateway, deduplication=False)

        await queue.publish("important-message")
        assert await client.llen(queue.key.pending) == 1

        # Simulate consumer claiming the message (what process_message does internally).
        claimed = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert claimed is not None

        # Consumer crashes here — never calls remove_message or move_message.
        # The message is now stuck in processing with no recovery path.
        assert await client.llen(queue.key.pending) == 0
        assert await client.llen(queue.key.processing) == 1

        # No reclaim mechanism exists without visibility timeout.
        # Another consumer trying to claim gets nothing from pending.
        next_claim = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert next_claim is None

        # The message remains permanently orphaned in processing.
        assert await client.llen(queue.key.pending) == 0
        assert await client.llen(queue.key.processing) == 1


class TestCompletedQueueGrowth:
    """Explicit legacy unbounded completed/failed queues grow linearly."""

    @pytest.mark.asyncio
    async def test_completed_queue_grows_linearly_with_throughput(self):
        client = fakeredis.FakeAsyncRedis()
        gateway = RedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
        )
        queue = RedisMessageQueue(
            "test",
            gateway=gateway,
            deduplication=False,
            enable_completed_queue=True,
            max_completed_length=None,
        )

        n_messages = 50
        for i in range(n_messages):
            await queue.publish(f"message-{i}")

        for _ in range(n_messages):
            async with queue.process_message() as msg:
                assert msg is not None

        # Every successfully processed message accumulates in the completed queue.
        # Explicit max_completed_length=None disables trimming.
        assert await client.llen(queue.key.completed) == n_messages
        assert await client.llen(queue.key.processing) == 0
        assert await client.llen(queue.key.pending) == 0

    @pytest.mark.asyncio
    async def test_failed_queue_grows_linearly_with_throughput(self):
        client = fakeredis.FakeAsyncRedis()
        gateway = RedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
        )
        queue = RedisMessageQueue(
            "test",
            gateway=gateway,
            deduplication=False,
            enable_failed_queue=True,
            max_failed_length=None,
        )

        n_messages = 50
        for i in range(n_messages):
            await queue.publish(f"message-{i}")

        for _ in range(n_messages):
            with pytest.raises(RuntimeError):
                async with queue.process_message() as msg:
                    assert msg is not None
                    raise RuntimeError("processing failed")

        # Every failed message accumulates in the failed queue.
        # Explicit max_failed_length=None disables trimming.
        assert await client.llen(queue.key.failed) == n_messages
        assert await client.llen(queue.key.processing) == 0
        assert await client.llen(queue.key.pending) == 0


class TestClusterHashTagCompatibility:
    """F3: Redis Cluster hash-tag requirement.

    The key scheme uses multiple keys per operation, so Cluster deployments
    require a shared hash tag such as '{myqueue}'.
    """

    def test_hash_tagged_name_produces_colocated_keys(self):
        """All keys generated for a hash-tagged queue name must share the
        same hash tag, ensuring they map to the same Redis Cluster slot."""
        gateway = FakeAsyncGateway()
        queue = RedisMessageQueue(
            "{myqueue}",
            gateway=gateway,
            deduplication=True,
            get_deduplication_key=lambda msg: msg,
        )

        # All QueueKeyManager keys
        queue_keys = [
            queue.key.pending,
            queue.key.processing,
            queue.key.completed,
            queue.key.failed,
            queue.key.deduplication("some-message"),
        ]

        # Gateway-derived lease metadata keys (suffixed to the processing queue key)
        processing = queue.key.processing
        lease_keys = [
            f"{processing}:lease_deadlines",
            f"{processing}:lease_tokens",
            f"{processing}:lease_token_counter",
        ]

        all_keys = queue_keys + lease_keys

        # Redis Cluster computes hash slot from content between first { and first } after it.
        hash_tag_pattern = re.compile(r"\{([^}]+)\}")
        for key in all_keys:
            match = hash_tag_pattern.search(key)
            assert match is not None, f"Key {key!r} has no hash tag"
            assert match.group(1) == "myqueue", f"Key {key!r} has hash tag {match.group(1)!r}, expected 'myqueue'"

    @pytest.mark.asyncio
    async def test_hash_tagged_queue_round_trip(self):
        """Publish and process a message using a hash-tagged queue name to
        verify functional correctness with the workaround."""
        client = fakeredis.FakeAsyncRedis()
        gateway = RedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
        )
        queue = RedisMessageQueue(
            "{myqueue}",
            gateway=gateway,
            deduplication=False,
        )

        await queue.publish("hello-cluster")

        async with queue.process_message() as msg:
            assert msg is not None
            if isinstance(msg, bytes):
                msg = msg.decode("utf-8")
            assert msg == "hello-cluster"

        assert await client.llen(queue.key.pending) == 0
        assert await client.llen(queue.key.processing) == 0

    @pytest.mark.asyncio
    async def test_visibility_timeout_claim_uses_only_colocated_keys_without_dead_letter(self):
        class RecordingClient:
            def __init__(self):
                self.calls = []

            async def eval(self, script, numkeys, *args):
                self.calls.append((script, numkeys, args))
                return None

        client = RecordingClient()
        gateway = RedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_visibility_timeout_seconds=30,
        )

        await gateway._claim_visible_message("{myqueue}::pending", "{myqueue}::processing", claim_id="claim-1")

        _, numkeys, args = client.calls[0]
        keys = args[:numkeys]
        assert "" not in keys
        assert len({key_slot(key.encode("utf-8")) for key in keys}) == 1


class TestConstructorMaxCompletedLengthValidation:
    @pytest.mark.parametrize("invalid_value", ["10", 1.5, True, False, [10]])
    def test_non_int_raises_type_error(self, invalid_value):
        gateway = FakeAsyncGateway()
        with pytest.raises(TypeError, match="'max_completed_length' must be an int or None"):
            RedisMessageQueue("test", gateway=gateway, enable_completed_queue=True, max_completed_length=invalid_value)

    @pytest.mark.parametrize("invalid_value", [0, -1, -100])
    def test_non_positive_raises_value_error(self, invalid_value):
        gateway = FakeAsyncGateway()
        with pytest.raises(ValueError, match="'max_completed_length' must be positive"):
            RedisMessageQueue("test", gateway=gateway, enable_completed_queue=True, max_completed_length=invalid_value)

    def test_without_enable_completed_queue_raises_value_error(self):
        gateway = FakeAsyncGateway()
        with pytest.raises(ValueError, match="requires 'enable_completed_queue=True'"):
            RedisMessageQueue("test", gateway=gateway, max_completed_length=100)

    def test_none_is_accepted(self):
        gateway = FakeAsyncGateway()
        q = RedisMessageQueue("test", gateway=gateway, enable_completed_queue=True, max_completed_length=None)
        assert q._max_completed_length is None

    def test_default_is_1000_when_completed_queue_enabled(self):
        gateway = FakeAsyncGateway()
        q = RedisMessageQueue("test", gateway=gateway, enable_completed_queue=True)
        assert q._max_completed_length == 1000

    def test_positive_int_is_accepted(self):
        gateway = FakeAsyncGateway()
        q = RedisMessageQueue("test", gateway=gateway, enable_completed_queue=True, max_completed_length=100)
        assert q._max_completed_length == 100


class TestConstructorMaxFailedLengthValidation:
    @pytest.mark.parametrize("invalid_value", ["10", 1.5, True, False, [10]])
    def test_non_int_raises_type_error(self, invalid_value):
        gateway = FakeAsyncGateway()
        with pytest.raises(TypeError, match="'max_failed_length' must be an int or None"):
            RedisMessageQueue("test", gateway=gateway, enable_failed_queue=True, max_failed_length=invalid_value)

    @pytest.mark.parametrize("invalid_value", [0, -1, -100])
    def test_non_positive_raises_value_error(self, invalid_value):
        gateway = FakeAsyncGateway()
        with pytest.raises(ValueError, match="'max_failed_length' must be positive"):
            RedisMessageQueue("test", gateway=gateway, enable_failed_queue=True, max_failed_length=invalid_value)

    def test_without_enable_failed_queue_raises_value_error(self):
        gateway = FakeAsyncGateway()
        with pytest.raises(ValueError, match="requires 'enable_failed_queue=True'"):
            RedisMessageQueue("test", gateway=gateway, max_failed_length=100)

    def test_none_is_accepted(self):
        gateway = FakeAsyncGateway()
        q = RedisMessageQueue("test", gateway=gateway, enable_failed_queue=True, max_failed_length=None)
        assert q._max_failed_length is None

    def test_default_is_1000_when_failed_queue_enabled(self):
        gateway = FakeAsyncGateway()
        q = RedisMessageQueue("test", gateway=gateway, enable_failed_queue=True)
        assert q._max_failed_length == 1000

    def test_positive_int_is_accepted(self):
        gateway = FakeAsyncGateway()
        q = RedisMessageQueue("test", gateway=gateway, enable_failed_queue=True, max_failed_length=100)
        assert q._max_failed_length == 100


class TestBoundedCompletedQueue:
    """Completed queue is trimmed to max_completed_length after each move."""

    @pytest.mark.asyncio
    async def test_completed_queue_is_trimmed(self):
        client = fakeredis.FakeAsyncRedis()
        gateway = RedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
        )
        max_len = 10
        queue = RedisMessageQueue(
            "test",
            gateway=gateway,
            deduplication=False,
            enable_completed_queue=True,
            max_completed_length=max_len,
        )

        n_messages = 50
        for i in range(n_messages):
            await queue.publish(f"message-{i}")

        for _ in range(n_messages):
            async with queue.process_message() as msg:
                assert msg is not None

        assert await client.llen(queue.key.completed) == max_len
        assert await client.llen(queue.key.processing) == 0
        assert await client.llen(queue.key.pending) == 0

    @pytest.mark.asyncio
    async def test_no_trim_without_max_completed_length(self):
        client = fakeredis.FakeAsyncRedis()
        gateway = RedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
        )
        queue = RedisMessageQueue(
            "test",
            gateway=gateway,
            deduplication=False,
            enable_completed_queue=True,
            max_completed_length=None,
        )

        n_messages = 20
        for i in range(n_messages):
            await queue.publish(f"message-{i}")

        for _ in range(n_messages):
            async with queue.process_message() as msg:
                assert msg is not None

        assert await client.llen(queue.key.completed) == n_messages


class TestBoundedFailedQueue:
    """Failed queue is trimmed to max_failed_length after each move."""

    @pytest.mark.asyncio
    async def test_failed_queue_is_trimmed(self):
        client = fakeredis.FakeAsyncRedis()
        gateway = RedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
        )
        max_len = 10
        queue = RedisMessageQueue(
            "test",
            gateway=gateway,
            deduplication=False,
            enable_failed_queue=True,
            max_failed_length=max_len,
        )

        n_messages = 50
        for i in range(n_messages):
            await queue.publish(f"message-{i}")

        for _ in range(n_messages):
            with pytest.raises(RuntimeError):
                async with queue.process_message() as msg:
                    assert msg is not None
                    raise RuntimeError("processing failed")

        assert await client.llen(queue.key.failed) == max_len
        assert await client.llen(queue.key.processing) == 0
        assert await client.llen(queue.key.pending) == 0

    @pytest.mark.asyncio
    async def test_no_trim_without_max_failed_length(self):
        client = fakeredis.FakeAsyncRedis()
        gateway = RedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
        )
        queue = RedisMessageQueue(
            "test",
            gateway=gateway,
            deduplication=False,
            enable_failed_queue=True,
            max_failed_length=None,
        )

        n_messages = 20
        for i in range(n_messages):
            await queue.publish(f"message-{i}")

        for _ in range(n_messages):
            with pytest.raises(RuntimeError):
                async with queue.process_message() as msg:
                    assert msg is not None
                    raise RuntimeError("processing failed")

        assert await client.llen(queue.key.failed) == n_messages


class TestAwaitSuppressingExternalCancellation:
    """Pin the cancellation contract so a future refactor can't silently
    turn the triple-cancel path into an implicit ``None`` return. The caller
    in ``process_message`` uses the returned ``applied`` flag to decide
    whether to log a misleading "lease expired" warning — a ``None`` return
    would match ``not applied`` and fire the warning under pure-cancellation
    scenarios where the cleanup never ran to completion.
    """

    @pytest.mark.asyncio
    async def test_returns_value_when_not_cancelled(self):
        async def operation():
            return "ok"

        result, swallowed_cancel = await _await_suppressing_external_cancellation(operation())
        assert result == "ok"
        assert swallowed_cancel is None

    @pytest.mark.asyncio
    async def test_suppresses_single_cancel_and_returns_value(self):
        started = asyncio.Event()
        release = asyncio.Event()

        async def operation():
            started.set()
            await release.wait()
            return "finished"

        async def runner():
            return await _await_suppressing_external_cancellation(operation())

        caller = asyncio.create_task(runner())
        await started.wait()
        caller.cancel()
        await asyncio.sleep(0)
        release.set()
        result, swallowed_cancel = await caller
        assert result == "finished"
        # The single suppressed cancellation is surfaced to the caller so it
        # can ride the re-raised processing error's context chain.
        assert isinstance(swallowed_cancel, asyncio.CancelledError)

    @pytest.mark.asyncio
    async def test_double_cancel_raises_not_returns_none(self):
        started = asyncio.Event()
        release = asyncio.Event()

        async def operation():
            started.set()
            await release.wait()
            return "finished"

        async def runner():
            return await _await_suppressing_external_cancellation(operation())

        caller = asyncio.create_task(runner())
        await started.wait()
        caller.cancel()
        await asyncio.sleep(0)
        caller.cancel()
        with pytest.raises(asyncio.CancelledError):
            await caller
        # Guard against a regression that returns None implicitly: the caller
        # must NOT have a non-exception result.
        assert caller.cancelled() or caller.exception() is not None
        # Drain the orphaned shielded inner task to keep test output clean.
        release.set()
        await asyncio.sleep(0)

    @pytest.mark.parametrize(
        "helper",
        [_await_suppressing_external_cancellation, _await_preserving_cancellation],
    )
    @pytest.mark.asyncio
    async def test_double_cancel_with_inner_error_does_not_leak_unretrieved_exception(self, helper, caplog):
        """Regression: when repeated cancellation short-circuits past the inner
        shielded task and that task finished with a regular Exception, asyncio
        must not log 'Task exception was never retrieved'. The helpers attach a
        done_callback that observes the inner task's exception."""

        async def operation():
            await asyncio.sleep(0.05)
            raise RuntimeError("inner failed")

        async def runner():
            return await helper(operation())

        with caplog.at_level(logging.ERROR, logger="asyncio"):
            caller = asyncio.create_task(runner())
            await asyncio.sleep(0)
            caller.cancel()
            await asyncio.sleep(0)
            caller.cancel()
            with pytest.raises(BaseException):
                await caller
            # Let the inner task finish + be GC'd so asyncio would log the
            # "never retrieved" warning if the callback weren't there.
            await asyncio.sleep(0.1)
            gc.collect()
            await asyncio.sleep(0)

        offending = [r for r in caplog.records if "Task exception was never retrieved" in r.getMessage()]
        assert not offending, f"unexpected asyncio warning(s): {[r.getMessage() for r in offending]}"
