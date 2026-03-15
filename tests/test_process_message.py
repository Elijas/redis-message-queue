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
                pytest.fail(f"Constructor rejected a falsy-but-provided client: {e}")


class TestConstructorNameValidation:
    def test_empty_name_raises_value_error(self):
        gateway = FakeGateway()
        with pytest.raises(ValueError, match="non-empty"):
            RedisMessageQueue("", gateway=gateway)

    def test_whitespace_only_name_raises_value_error(self):
        gateway = FakeGateway()
        with pytest.raises(ValueError, match="non-empty"):
            RedisMessageQueue("   ", gateway=gateway)

    def test_none_name_raises_type_error(self):
        gateway = FakeGateway()
        with pytest.raises(TypeError, match="must be a string"):
            RedisMessageQueue(None, gateway=gateway)


class TestConstructorKeySeparatorValidation:
    def test_none_key_separator_raises_type_error(self):
        gateway = FakeGateway()
        with pytest.raises(TypeError, match="must be a string"):
            RedisMessageQueue("test", gateway=gateway, key_separator=None)

    def test_int_key_separator_raises_type_error(self):
        gateway = FakeGateway()
        with pytest.raises(TypeError, match="must be a string"):
            RedisMessageQueue("test", gateway=gateway, key_separator=123)

    def test_bool_key_separator_raises_type_error(self):
        gateway = FakeGateway()
        with pytest.raises(TypeError, match="must be a string"):
            RedisMessageQueue("test", gateway=gateway, key_separator=True)

    def test_empty_key_separator_raises_value_error(self):
        gateway = FakeGateway()
        with pytest.raises(ValueError, match="non-empty"):
            RedisMessageQueue("test", gateway=gateway, key_separator="")

    def test_whitespace_only_key_separator_raises_value_error(self):
        gateway = FakeGateway()
        with pytest.raises(ValueError, match="non-empty"):
            RedisMessageQueue("test", gateway=gateway, key_separator="   ")


class TestConstructorConflictingParameters:
    """When gateway is provided, client and interrupt must not be silently ignored."""

    def test_gateway_and_client_raises(self):
        gateway = FakeGateway()
        with pytest.raises(ValueError, match="cannot be provided alongside"):
            RedisMessageQueue("test", gateway=gateway, client=object())

    def test_gateway_and_interrupt_raises(self):
        gateway = FakeGateway()
        with pytest.raises(ValueError, match="cannot be provided alongside"):
            RedisMessageQueue("test", gateway=gateway, interrupt=object())

    def test_gateway_alone_is_accepted(self):
        gateway = FakeGateway()
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


class TestConstructorBooleanParameterValidation:
    @pytest.mark.parametrize("invalid_value", ["yes", "false", 1, 0, None, 2.0, []])
    def test_non_bool_deduplication_raises_type_error(self, invalid_value):
        gateway = FakeGateway()
        with pytest.raises(TypeError, match="'deduplication' must be a bool"):
            RedisMessageQueue("test", gateway=gateway, deduplication=invalid_value)

    @pytest.mark.parametrize("invalid_value", ["yes", "false", 1, 0, None, 2.0, []])
    def test_non_bool_enable_completed_queue_raises_type_error(self, invalid_value):
        gateway = FakeGateway()
        with pytest.raises(TypeError, match="'enable_completed_queue' must be a bool"):
            RedisMessageQueue("test", gateway=gateway, enable_completed_queue=invalid_value)

    @pytest.mark.parametrize("invalid_value", ["yes", "false", 1, 0, None, 2.0, []])
    def test_non_bool_enable_failed_queue_raises_type_error(self, invalid_value):
        gateway = FakeGateway()
        with pytest.raises(TypeError, match="'enable_failed_queue' must be a bool"):
            RedisMessageQueue("test", gateway=gateway, enable_failed_queue=invalid_value)

    def test_true_is_accepted(self):
        gateway = FakeGateway()
        q = RedisMessageQueue(
            "test",
            gateway=gateway,
            deduplication=True,
            enable_completed_queue=True,
            enable_failed_queue=True,
        )
        assert q._deduplication is True
        assert q._enable_completed_queue is True
        assert q._enable_failed_queue is True

    def test_false_is_accepted(self):
        gateway = FakeGateway()
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
        gateway = FakeGateway()
        with pytest.raises(TypeError, match="'get_deduplication_key' must be callable"):
            RedisMessageQueue("test", gateway=gateway, get_deduplication_key=invalid_value)

    def test_none_is_accepted(self):
        gateway = FakeGateway()
        q = RedisMessageQueue("test", gateway=gateway, get_deduplication_key=None)
        assert q._get_deduplication_key is None

    def test_lambda_is_accepted(self):
        gateway = FakeGateway()
        fn = lambda msg: msg
        q = RedisMessageQueue("test", gateway=gateway, get_deduplication_key=fn)
        assert q._get_deduplication_key is fn

    def test_callable_object_is_accepted(self):
        class MyCallable:
            def __call__(self, msg):
                return msg

        gateway = FakeGateway()
        obj = MyCallable()
        q = RedisMessageQueue("test", gateway=gateway, get_deduplication_key=obj)
        assert q._get_deduplication_key is obj


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
        queue = RedisMessageQueue("test", gateway=gateway, enable_completed_queue=True)

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
