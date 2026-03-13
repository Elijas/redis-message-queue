import pytest

from redis_message_queue.asyncio._abstract_redis_gateway import AbstractRedisGateway
from redis_message_queue.asyncio.redis_message_queue import RedisMessageQueue


class FakeAsyncGateway(AbstractRedisGateway):
    def __init__(self):
        self.message_to_return = None
        self.fail_on_move = False
        self.fail_on_remove = False
        self.removed_messages = []
        self.moved_messages = []
        self.move_attempts = []
        self.remove_attempts = []

    async def publish_message(self, queue, message, dedup_key):
        return True

    async def add_message(self, queue, message):
        pass

    async def move_message(self, from_queue, to_queue, message):
        self.move_attempts.append((from_queue, to_queue, message))
        if self.fail_on_move:
            raise ConnectionError("Redis connection lost")
        self.moved_messages.append((from_queue, to_queue, message))

    async def remove_message(self, queue, message):
        self.remove_attempts.append((queue, message))
        if self.fail_on_remove:
            raise ConnectionError("Redis connection lost")
        self.removed_messages.append((queue, message))

    async def wait_for_message_and_move(self, from_queue, to_queue):
        return self.message_to_return


class TestProcessMessageExceptionPropagation:
    @pytest.mark.asyncio
    async def test_user_exception_propagates_when_remove_fails(self):
        gateway = FakeAsyncGateway()
        gateway.message_to_return = b"test-message"
        gateway.fail_on_remove = True
        queue = RedisMessageQueue("test", gateway=gateway)

        with pytest.raises(ValueError, match="original error"):
            async with queue.process_message() as msg:
                raise ValueError("original error")

    @pytest.mark.asyncio
    async def test_user_exception_propagates_when_move_to_failed_fails(self):
        gateway = FakeAsyncGateway()
        gateway.message_to_return = b"test-message"
        gateway.fail_on_move = True
        queue = RedisMessageQueue("test", gateway=gateway, enable_failed_queue=True)

        with pytest.raises(ValueError, match="original error"):
            async with queue.process_message() as msg:
                raise ValueError("original error")

    @pytest.mark.asyncio
    async def test_keyboard_interrupt_propagates_when_cleanup_fails(self):
        gateway = FakeAsyncGateway()
        gateway.message_to_return = b"test-message"
        gateway.fail_on_remove = True
        queue = RedisMessageQueue("test", gateway=gateway)

        with pytest.raises(KeyboardInterrupt):
            async with queue.process_message() as msg:
                raise KeyboardInterrupt()


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

        async with queue.process_message() as msg:
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
        queue = RedisMessageQueue(
            "test", gateway=gateway, enable_completed_queue=True
        )

        async with queue.process_message() as msg:
            pass

        assert len(gateway.moved_messages) == 1
        _, to_queue, _ = gateway.moved_messages[0]
        assert to_queue == queue.key.completed

    @pytest.mark.asyncio
    async def test_timeout_does_not_clean_up(self):
        gateway = FakeAsyncGateway()
        gateway.message_to_return = None
        queue = RedisMessageQueue("test", gateway=gateway)

        async with queue.process_message() as msg:
            pass

        assert len(gateway.removed_messages) == 0
        assert len(gateway.moved_messages) == 0


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

        with pytest.raises(ConnectionError):
            async with queue.process_message() as msg:
                pass

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

        with pytest.raises(ConnectionError):
            async with queue.process_message() as msg:
                pass

        assert len(gateway.remove_attempts) == 1
        assert len(gateway.move_attempts) == 0
