import pytest

from redis_message_queue._abstract_redis_gateway import AbstractRedisGateway
from redis_message_queue.redis_message_queue import RedisMessageQueue


class TestConstructorClientValidation:
    def test_falsy_non_none_client_is_accepted(self):
        """A client that is falsy (e.g. __bool__=False) but not None must not be
        rejected by the constructor — it should fail later at point of use, not
        with a misleading 'must be provided' error."""

        class FalsyClient:
            """A mock client that evaluates to falsy."""
            def __bool__(self):
                return False

        # Should NOT raise ValueError — the client was provided, it's just falsy.
        # Any error should come later when actually using the client, not here.
        try:
            RedisMessageQueue("test", client=FalsyClient())
        except ValueError as e:
            if "must be provided" in str(e):
                pytest.fail(
                    f"Constructor rejected a falsy-but-provided client: {e}"
                )


class FakeGateway(AbstractRedisGateway):
    def __init__(self):
        self.message_to_return = None
        self.fail_on_move = False
        self.fail_on_remove = False
        self.move_exception = ConnectionError("Redis connection lost")
        self.remove_exception = ConnectionError("Redis connection lost")
        self.removed_messages = []
        self.moved_messages = []
        self.move_attempts = []
        self.remove_attempts = []

    def publish_message(self, queue, message, dedup_key):
        return True

    def add_message(self, queue, message):
        pass

    def move_message(self, from_queue, to_queue, message):
        self.move_attempts.append((from_queue, to_queue, message))
        if self.fail_on_move:
            raise self.move_exception
        self.moved_messages.append((from_queue, to_queue, message))

    def remove_message(self, queue, message):
        self.remove_attempts.append((queue, message))
        if self.fail_on_remove:
            raise self.remove_exception
        self.removed_messages.append((queue, message))

    def wait_for_message_and_move(self, from_queue, to_queue):
        return self.message_to_return


class TestProcessMessageExceptionPropagation:
    def test_user_exception_propagates_when_remove_fails(self):
        gateway = FakeGateway()
        gateway.message_to_return = b"test-message"
        gateway.fail_on_remove = True
        queue = RedisMessageQueue("test", gateway=gateway)

        with pytest.raises(ValueError, match="original error"):
            with queue.process_message() as _msg:
                raise ValueError("original error")

    def test_user_exception_propagates_when_move_to_failed_fails(self):
        gateway = FakeGateway()
        gateway.message_to_return = b"test-message"
        gateway.fail_on_move = True
        queue = RedisMessageQueue("test", gateway=gateway, enable_failed_queue=True)

        with pytest.raises(ValueError, match="original error"):
            with queue.process_message() as _msg:
                raise ValueError("original error")

    def test_keyboard_interrupt_propagates_when_cleanup_fails(self):
        gateway = FakeGateway()
        gateway.message_to_return = b"test-message"
        gateway.fail_on_remove = True
        queue = RedisMessageQueue("test", gateway=gateway)

        with pytest.raises(KeyboardInterrupt):
            with queue.process_message() as _msg:
                raise KeyboardInterrupt()

    def test_user_exception_still_cleans_up_when_possible(self):
        gateway = FakeGateway()
        gateway.message_to_return = b"test-message"
        queue = RedisMessageQueue("test", gateway=gateway)

        with pytest.raises(ValueError):
            with queue.process_message() as _msg:
                raise ValueError("boom")

        assert len(gateway.removed_messages) == 1

    def test_user_exception_moves_to_failed_queue_when_enabled(self):
        gateway = FakeGateway()
        gateway.message_to_return = b"test-message"
        queue = RedisMessageQueue("test", gateway=gateway, enable_failed_queue=True)

        with pytest.raises(ValueError):
            with queue.process_message() as _msg:
                raise ValueError("boom")

        assert len(gateway.moved_messages) == 1
        _, to_queue, _ = gateway.moved_messages[0]
        assert to_queue == queue.key.failed


class TestProcessMessageCleanupBaseException:
    """When cleanup raises a non-Exception BaseException (e.g. KeyboardInterrupt),
    the original user exception must still propagate."""

    def test_user_exception_propagates_when_remove_raises_base_exception(self):
        gateway = FakeGateway()
        gateway.message_to_return = b"test-message"
        gateway.fail_on_remove = True
        gateway.remove_exception = KeyboardInterrupt()
        queue = RedisMessageQueue("test", gateway=gateway)

        with pytest.raises(ValueError, match="original error"):
            with queue.process_message() as _msg:
                raise ValueError("original error")

    def test_user_exception_propagates_when_move_to_failed_raises_base_exception(self):
        gateway = FakeGateway()
        gateway.message_to_return = b"test-message"
        gateway.fail_on_move = True
        gateway.move_exception = KeyboardInterrupt()
        queue = RedisMessageQueue("test", gateway=gateway, enable_failed_queue=True)

        with pytest.raises(ValueError, match="original error"):
            with queue.process_message() as _msg:
                raise ValueError("original error")


class TestProcessMessageNoneHandling:
    def test_timeout_yields_none(self):
        gateway = FakeGateway()
        gateway.message_to_return = None
        queue = RedisMessageQueue("test", gateway=gateway)

        with queue.process_message() as msg:
            assert msg is None

    def test_empty_bytes_message_is_not_swallowed(self):
        gateway = FakeGateway()
        gateway.message_to_return = b""
        queue = RedisMessageQueue("test", gateway=gateway)

        with queue.process_message() as msg:
            assert msg == b""

    def test_empty_bytes_message_is_cleaned_up_on_success(self):
        gateway = FakeGateway()
        gateway.message_to_return = b""
        queue = RedisMessageQueue("test", gateway=gateway)

        with queue.process_message() as _msg:
            pass

        assert len(gateway.removed_messages) == 1


class TestProcessMessageSuccessPath:
    def test_success_removes_from_processing(self):
        gateway = FakeGateway()
        gateway.message_to_return = b"test-message"
        queue = RedisMessageQueue("test", gateway=gateway)

        with queue.process_message() as msg:
            assert msg == b"test-message"

        assert len(gateway.removed_messages) == 1
        removed_queue, _ = gateway.removed_messages[0]
        assert removed_queue == queue.key.processing

    def test_success_moves_to_completed_when_enabled(self):
        gateway = FakeGateway()
        gateway.message_to_return = b"test-message"
        queue = RedisMessageQueue(
            "test", gateway=gateway, enable_completed_queue=True
        )

        with queue.process_message() as _msg:
            pass

        assert len(gateway.moved_messages) == 1
        _, to_queue, _ = gateway.moved_messages[0]
        assert to_queue == queue.key.completed

    def test_timeout_does_not_clean_up(self):
        gateway = FakeGateway()
        gateway.message_to_return = None
        queue = RedisMessageQueue("test", gateway=gateway)

        with queue.process_message() as _msg:
            pass

        assert len(gateway.removed_messages) == 0
        assert len(gateway.moved_messages) == 0


class TestProcessMessageSuccessCleanupFailure:
    """When user code succeeds but post-success Redis cleanup fails,
    the message must NOT be routed to the failed queue."""

    def test_move_to_completed_failure_does_not_move_to_failed(self):
        gateway = FakeGateway()
        gateway.message_to_return = b"test-message"
        gateway.fail_on_move = True
        queue = RedisMessageQueue(
            "test",
            gateway=gateway,
            enable_completed_queue=True,
            enable_failed_queue=True,
        )

        with pytest.raises(ConnectionError):
            with queue.process_message() as _msg:
                pass

        assert len(gateway.move_attempts) == 1
        assert gateway.move_attempts[0][1] == queue.key.completed

    def test_remove_failure_does_not_move_to_failed(self):
        gateway = FakeGateway()
        gateway.message_to_return = b"test-message"
        gateway.fail_on_remove = True
        queue = RedisMessageQueue(
            "test",
            gateway=gateway,
            enable_completed_queue=False,
            enable_failed_queue=True,
        )

        with pytest.raises(ConnectionError):
            with queue.process_message() as _msg:
                pass

        assert len(gateway.remove_attempts) == 1
        assert len(gateway.move_attempts) == 0
