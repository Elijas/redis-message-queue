import fakeredis
import pytest
import redis.exceptions

from redis_message_queue._redis_gateway import RedisGateway
from redis_message_queue._stored_message import encode_stored_message
from redis_message_queue.asyncio._redis_gateway import (
    RedisGateway as AsyncRedisGateway,
)


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


class AmbiguousAddSyncClient:
    def __init__(self):
        self.calls = 0
        self.messages = []

    def lpush(self, queue, message):
        self.calls += 1
        self.messages.insert(0, (queue, message))
        if self.calls == 1:
            raise redis.exceptions.ConnectionError("connection lost after LPUSH")
        return len(self.messages)


class AmbiguousAddAsyncClient:
    def __init__(self):
        self.calls = 0
        self.messages = []

    async def lpush(self, queue, message):
        self.calls += 1
        self.messages.insert(0, (queue, message))
        if self.calls == 1:
            raise redis.exceptions.ConnectionError("connection lost after LPUSH")
        return len(self.messages)


class AmbiguousWaitSyncClient:
    def __init__(self):
        self.pending = [b"msg2", b"msg1"]
        self.processing = []
        self.calls = []

    def _move(self, from_queue, to_queue):
        if not self.pending:
            return None
        message = self.pending.pop()
        self.processing.insert(0, message)
        if len(self.calls) == 1:
            raise redis.exceptions.ConnectionError("connection lost after move")
        return message

    def lmove(self, from_queue, to_queue, src, dest):
        self.calls.append(("lmove", from_queue, to_queue, src, dest))
        return self._move(from_queue, to_queue)

    def blmove(self, from_queue, to_queue, timeout, src, dest):
        self.calls.append(("blmove", from_queue, to_queue, timeout, src, dest))
        return self._move(from_queue, to_queue)


class AmbiguousWaitAsyncClient:
    def __init__(self):
        self.pending = [b"msg2", b"msg1"]
        self.processing = []
        self.calls = []

    async def _move(self, from_queue, to_queue):
        if not self.pending:
            return None
        message = self.pending.pop()
        self.processing.insert(0, message)
        if len(self.calls) == 1:
            raise redis.exceptions.ConnectionError("connection lost after move")
        return message

    async def lmove(self, from_queue, to_queue, src, dest):
        self.calls.append(("lmove", from_queue, to_queue, src, dest))
        return await self._move(from_queue, to_queue)

    async def blmove(self, from_queue, to_queue, timeout, src, dest):
        self.calls.append(("blmove", from_queue, to_queue, timeout, src, dest))
        return await self._move(from_queue, to_queue)


class AmbiguousMoveSyncPipeline:
    def __init__(self, client, source_queue, destination_queue):
        self._client = client
        self._source_queue = source_queue
        self._destination_queue = destination_queue
        self._destination_message = None
        self._source_message = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def lpush(self, queue, message):
        assert queue == self._destination_queue
        self._destination_message = message

    def lrem(self, queue, count, message):
        assert queue == self._source_queue
        assert count == 1
        self._source_message = message

    def execute(self):
        self._client.pipeline_calls += 1
        if self._destination_message is not None:
            self._client.redis.lpush(self._destination_queue, self._destination_message)
        if self._source_message is not None:
            self._client.redis.lrem(self._source_queue, 1, self._source_message)
        if self._client.pipeline_calls == 1:
            raise redis.exceptions.ConnectionError("connection lost after EXEC")
        return [1, 1]


class AmbiguousMoveSyncClient:
    def __init__(self):
        self.redis = fakeredis.FakeRedis()
        self.pipeline_calls = 0
        self.eval_calls = 0

    def pipeline(self, transaction=True):
        assert transaction is True
        return AmbiguousMoveSyncPipeline(self, "processing", "completed")

    def eval(self, script, numkeys, source_queue, destination_queue, source_message, destination_message):
        self.eval_calls += 1
        removed = self.redis.lrem(source_queue, 1, source_message)
        if removed:
            self.redis.lpush(destination_queue, destination_message)
        if self.eval_calls == 1:
            raise redis.exceptions.ConnectionError("connection lost after EVAL")
        return removed


class AmbiguousMoveAsyncPipeline:
    def __init__(self, client, source_queue, destination_queue):
        self._client = client
        self._source_queue = source_queue
        self._destination_queue = destination_queue
        self._destination_message = None
        self._source_message = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def lpush(self, queue, message):
        assert queue == self._destination_queue
        self._destination_message = message

    def lrem(self, queue, count, message):
        assert queue == self._source_queue
        assert count == 1
        self._source_message = message

    async def execute(self):
        self._client.pipeline_calls += 1
        if self._destination_message is not None:
            await self._client.redis.lpush(self._destination_queue, self._destination_message)
        if self._source_message is not None:
            await self._client.redis.lrem(self._source_queue, 1, self._source_message)
        if self._client.pipeline_calls == 1:
            raise redis.exceptions.ConnectionError("connection lost after EXEC")
        return [1, 1]


class AmbiguousMoveAsyncClient:
    def __init__(self):
        self.redis = fakeredis.FakeAsyncRedis()
        self.pipeline_calls = 0
        self.eval_calls = 0

    def pipeline(self, transaction=True):
        assert transaction is True
        return AmbiguousMoveAsyncPipeline(self, "processing", "completed")

    async def eval(self, script, numkeys, source_queue, destination_queue, source_message, destination_message):
        self.eval_calls += 1
        removed = await self.redis.lrem(source_queue, 1, source_message)
        if removed:
            await self.redis.lpush(destination_queue, destination_message)
        if self.eval_calls == 1:
            raise redis.exceptions.ConnectionError("connection lost after EVAL")
        return removed


class TestSyncGatewayRetrySafety:
    def test_add_message_does_not_retry_after_ambiguous_lpush(self):
        client = AmbiguousAddSyncClient()
        gateway = RedisGateway(redis_client=client, retry_strategy=_retry_once_on_connection_error)

        with pytest.raises(redis.exceptions.ConnectionError, match="after LPUSH"):
            gateway.add_message("pending", "hello")

        assert client.calls == 1
        assert len(client.messages) == 1

    def test_wait_for_message_and_move_does_not_retry_after_ambiguous_blmove(self):
        client = AmbiguousWaitSyncClient()
        gateway = RedisGateway(
            redis_client=client,
            retry_strategy=_retry_once_on_connection_error,
            message_wait_interval_seconds=5,
        )

        with pytest.raises(redis.exceptions.ConnectionError, match="after move"):
            gateway.wait_for_message_and_move("pending", "processing")

        assert client.calls == [("blmove", "pending", "processing", 5, "RIGHT", "LEFT")]
        assert client.pending == [b"msg2"]
        assert client.processing == [b"msg1"]

    def test_move_message_is_idempotent_under_retry(self):
        client = AmbiguousMoveSyncClient()
        stored_message = encode_stored_message("hello")
        client.redis.lpush("processing", stored_message)
        gateway = RedisGateway(redis_client=client, retry_strategy=_retry_once_on_connection_error)

        gateway.move_message("processing", "completed", stored_message)

        assert client.redis.lrange("processing", 0, -1) == []
        assert client.redis.lrange("completed", 0, -1) == [b"hello"]


class TestAsyncGatewayRetrySafety:
    @pytest.mark.asyncio
    async def test_add_message_does_not_retry_after_ambiguous_lpush(self):
        client = AmbiguousAddAsyncClient()
        gateway = AsyncRedisGateway(redis_client=client, retry_strategy=_async_retry_once_on_connection_error)

        with pytest.raises(redis.exceptions.ConnectionError, match="after LPUSH"):
            await gateway.add_message("pending", "hello")

        assert client.calls == 1
        assert len(client.messages) == 1

    @pytest.mark.asyncio
    async def test_wait_for_message_and_move_does_not_retry_after_ambiguous_blmove(self):
        client = AmbiguousWaitAsyncClient()
        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_strategy=_async_retry_once_on_connection_error,
            message_wait_interval_seconds=5,
        )

        with pytest.raises(redis.exceptions.ConnectionError, match="after move"):
            await gateway.wait_for_message_and_move("pending", "processing")

        assert client.calls == [("blmove", "pending", "processing", 5, "RIGHT", "LEFT")]
        assert client.pending == [b"msg2"]
        assert client.processing == [b"msg1"]

    @pytest.mark.asyncio
    async def test_move_message_is_idempotent_under_retry(self):
        client = AmbiguousMoveAsyncClient()
        stored_message = encode_stored_message("hello")
        await client.redis.lpush("processing", stored_message)
        gateway = AsyncRedisGateway(redis_client=client, retry_strategy=_async_retry_once_on_connection_error)

        await gateway.move_message("processing", "completed", stored_message)

        assert await client.redis.lrange("processing", 0, -1) == []
        assert await client.redis.lrange("completed", 0, -1) == [b"hello"]
