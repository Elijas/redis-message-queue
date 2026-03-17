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


class AmbiguousEvalSyncClient:
    """Wraps fakeredis; eval() succeeds on server then raises ConnectionError on first call."""

    def __init__(self):
        self.redis = fakeredis.FakeRedis()
        self.eval_calls = 0

    def eval(self, script, numkeys, *args):
        self.eval_calls += 1
        result = self.redis.eval(script, numkeys, *args)
        if self.eval_calls == 1:
            raise redis.exceptions.ConnectionError("connection lost after EVAL")
        return result

    def __getattr__(self, name):
        return getattr(self.redis, name)


class AmbiguousEvalAsyncClient:
    """Wraps FakeAsyncRedis; eval() succeeds on server then raises ConnectionError on first call."""

    def __init__(self):
        self.redis = fakeredis.FakeAsyncRedis()
        self.eval_calls = 0

    async def eval(self, script, numkeys, *args):
        self.eval_calls += 1
        result = await self.redis.eval(script, numkeys, *args)
        if self.eval_calls == 1:
            raise redis.exceptions.ConnectionError("connection lost after EVAL")
        return result

    def __getattr__(self, name):
        return getattr(self.redis, name)


class AmbiguousLremSyncClient:
    """Wraps fakeredis; lrem() succeeds on server then raises ConnectionError on first call."""

    def __init__(self):
        self.redis = fakeredis.FakeRedis()
        self.lrem_calls = 0

    def lrem(self, queue, count, message):
        self.lrem_calls += 1
        result = self.redis.lrem(queue, count, message)
        if self.lrem_calls == 1:
            raise redis.exceptions.ConnectionError("connection lost after LREM")
        return result

    def __getattr__(self, name):
        return getattr(self.redis, name)


class AmbiguousLremAsyncClient:
    """Wraps FakeAsyncRedis; lrem() succeeds on server then raises ConnectionError on first call."""

    def __init__(self):
        self.redis = fakeredis.FakeAsyncRedis()
        self.lrem_calls = 0

    async def lrem(self, queue, count, message):
        self.lrem_calls += 1
        result = await self.redis.lrem(queue, count, message)
        if self.lrem_calls == 1:
            raise redis.exceptions.ConnectionError("connection lost after LREM")
        return result

    def __getattr__(self, name):
        return getattr(self.redis, name)


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

    def test_publish_message_is_idempotent_under_retry(self):
        client = AmbiguousEvalSyncClient()
        gateway = RedisGateway(redis_client=client, retry_strategy=_retry_once_on_connection_error)

        result = gateway.publish_message("pending", "hello", "dedup:hello")

        assert result is False  # retry sees SET NX fail → returns 0
        assert client.redis.llen("pending") == 1  # message was pushed on first call
        assert client.redis.exists("dedup:hello")

    def test_remove_message_is_idempotent_under_retry(self):
        client = AmbiguousLremSyncClient()
        stored_message = encode_stored_message("hello")
        client.redis.lpush("processing", stored_message)
        gateway = RedisGateway(redis_client=client, retry_strategy=_retry_once_on_connection_error)

        gateway.remove_message("processing", stored_message)

        assert client.redis.llen("processing") == 0

    def test_remove_message_with_lease_token_is_idempotent_under_retry(self):
        client = AmbiguousEvalSyncClient()
        stored_message = encode_stored_message("hello")
        client.redis.lpush("processing", stored_message)
        client.redis.zadd("processing:lease_deadlines", {stored_message: 999999999999})
        client.redis.hset("processing:lease_tokens", stored_message, "1")
        gateway = RedisGateway(redis_client=client, retry_strategy=_retry_once_on_connection_error)

        gateway.remove_message("processing", stored_message, lease_token="1")

        assert client.redis.llen("processing") == 0
        assert client.redis.zcard("processing:lease_deadlines") == 0
        assert client.redis.hlen("processing:lease_tokens") == 0

    def test_move_message_with_lease_token_is_idempotent_under_retry(self):
        client = AmbiguousEvalSyncClient()
        stored_message = encode_stored_message("hello")
        client.redis.lpush("processing", stored_message)
        client.redis.zadd("processing:lease_deadlines", {stored_message: 999999999999})
        client.redis.hset("processing:lease_tokens", stored_message, "1")
        gateway = RedisGateway(redis_client=client, retry_strategy=_retry_once_on_connection_error)

        gateway.move_message("processing", "completed", stored_message, lease_token="1")

        assert client.redis.llen("processing") == 0
        assert client.redis.lrange("completed", 0, -1) == [b"hello"]
        assert client.redis.zcard("processing:lease_deadlines") == 0
        assert client.redis.hlen("processing:lease_tokens") == 0

    def test_renew_message_lease_is_idempotent_under_retry(self):
        client = AmbiguousEvalSyncClient()
        stored_message = encode_stored_message("hello")
        client.redis.zadd("processing:lease_deadlines", {stored_message: 1000})
        client.redis.hset("processing:lease_tokens", stored_message, "1")
        gateway = RedisGateway(
            redis_client=client,
            retry_strategy=_retry_once_on_connection_error,
            message_visibility_timeout_seconds=30,
        )

        result = gateway.renew_message_lease("processing", stored_message, "1")

        assert result is True
        deadline = client.redis.zscore("processing:lease_deadlines", stored_message)
        assert deadline > 1000
        assert client.redis.hget("processing:lease_tokens", stored_message) == b"1"

    def test_wait_for_message_and_move_does_not_retry_after_ambiguous_lmove(self):
        client = AmbiguousWaitSyncClient()
        gateway = RedisGateway(
            redis_client=client,
            retry_strategy=_retry_once_on_connection_error,
            message_wait_interval_seconds=0,
        )

        with pytest.raises(redis.exceptions.ConnectionError, match="after move"):
            gateway.wait_for_message_and_move("pending", "processing")

        assert client.calls == [("lmove", "pending", "processing", "RIGHT", "LEFT")]
        assert client.pending == [b"msg2"]
        assert client.processing == [b"msg1"]

    def test_claim_visible_message_does_not_retry_after_ambiguous_eval(self):
        client = AmbiguousEvalSyncClient()
        stored_message = encode_stored_message("hello")
        client.redis.lpush("pending", stored_message)
        gateway = RedisGateway(
            redis_client=client,
            retry_strategy=_retry_once_on_connection_error,
            message_visibility_timeout_seconds=30,
            message_wait_interval_seconds=0,
        )

        with pytest.raises(redis.exceptions.ConnectionError, match="after EVAL"):
            gateway.wait_for_message_and_move("pending", "processing")

        assert client.eval_calls == 1
        assert client.redis.llen("pending") == 0
        assert client.redis.llen("processing") == 1


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

    @pytest.mark.asyncio
    async def test_publish_message_is_idempotent_under_retry(self):
        client = AmbiguousEvalAsyncClient()
        gateway = AsyncRedisGateway(redis_client=client, retry_strategy=_async_retry_once_on_connection_error)

        result = await gateway.publish_message("pending", "hello", "dedup:hello")

        assert result is False
        assert await client.redis.llen("pending") == 1
        assert await client.redis.exists("dedup:hello")

    @pytest.mark.asyncio
    async def test_remove_message_is_idempotent_under_retry(self):
        client = AmbiguousLremAsyncClient()
        stored_message = encode_stored_message("hello")
        await client.redis.lpush("processing", stored_message)
        gateway = AsyncRedisGateway(redis_client=client, retry_strategy=_async_retry_once_on_connection_error)

        await gateway.remove_message("processing", stored_message)

        assert await client.redis.llen("processing") == 0

    @pytest.mark.asyncio
    async def test_remove_message_with_lease_token_is_idempotent_under_retry(self):
        client = AmbiguousEvalAsyncClient()
        stored_message = encode_stored_message("hello")
        await client.redis.lpush("processing", stored_message)
        await client.redis.zadd("processing:lease_deadlines", {stored_message: 999999999999})
        await client.redis.hset("processing:lease_tokens", stored_message, "1")
        gateway = AsyncRedisGateway(redis_client=client, retry_strategy=_async_retry_once_on_connection_error)

        await gateway.remove_message("processing", stored_message, lease_token="1")

        assert await client.redis.llen("processing") == 0
        assert await client.redis.zcard("processing:lease_deadlines") == 0
        assert await client.redis.hlen("processing:lease_tokens") == 0

    @pytest.mark.asyncio
    async def test_move_message_with_lease_token_is_idempotent_under_retry(self):
        client = AmbiguousEvalAsyncClient()
        stored_message = encode_stored_message("hello")
        await client.redis.lpush("processing", stored_message)
        await client.redis.zadd("processing:lease_deadlines", {stored_message: 999999999999})
        await client.redis.hset("processing:lease_tokens", stored_message, "1")
        gateway = AsyncRedisGateway(redis_client=client, retry_strategy=_async_retry_once_on_connection_error)

        await gateway.move_message("processing", "completed", stored_message, lease_token="1")

        assert await client.redis.llen("processing") == 0
        assert await client.redis.lrange("completed", 0, -1) == [b"hello"]
        assert await client.redis.zcard("processing:lease_deadlines") == 0
        assert await client.redis.hlen("processing:lease_tokens") == 0

    @pytest.mark.asyncio
    async def test_renew_message_lease_is_idempotent_under_retry(self):
        client = AmbiguousEvalAsyncClient()
        stored_message = encode_stored_message("hello")
        await client.redis.zadd("processing:lease_deadlines", {stored_message: 1000})
        await client.redis.hset("processing:lease_tokens", stored_message, "1")
        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_strategy=_async_retry_once_on_connection_error,
            message_visibility_timeout_seconds=30,
        )

        result = await gateway.renew_message_lease("processing", stored_message, "1")

        assert result is True
        deadline = await client.redis.zscore("processing:lease_deadlines", stored_message)
        assert deadline > 1000
        assert await client.redis.hget("processing:lease_tokens", stored_message) == b"1"

    @pytest.mark.asyncio
    async def test_wait_for_message_and_move_does_not_retry_after_ambiguous_lmove(self):
        client = AmbiguousWaitAsyncClient()
        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_strategy=_async_retry_once_on_connection_error,
            message_wait_interval_seconds=0,
        )

        with pytest.raises(redis.exceptions.ConnectionError, match="after move"):
            await gateway.wait_for_message_and_move("pending", "processing")

        assert client.calls == [("lmove", "pending", "processing", "RIGHT", "LEFT")]
        assert client.pending == [b"msg2"]
        assert client.processing == [b"msg1"]

    @pytest.mark.asyncio
    async def test_claim_visible_message_does_not_retry_after_ambiguous_eval(self):
        client = AmbiguousEvalAsyncClient()
        stored_message = encode_stored_message("hello")
        await client.redis.lpush("pending", stored_message)
        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_strategy=_async_retry_once_on_connection_error,
            message_visibility_timeout_seconds=30,
            message_wait_interval_seconds=0,
        )

        with pytest.raises(redis.exceptions.ConnectionError, match="after EVAL"):
            await gateway.wait_for_message_and_move("pending", "processing")

        assert client.eval_calls == 1
        assert await client.redis.llen("pending") == 0
        assert await client.redis.llen("processing") == 1
