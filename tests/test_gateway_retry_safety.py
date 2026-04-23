import asyncio
import logging
import threading
import time

import fakeredis
import pytest
import redis.exceptions

import redis_message_queue._redis_gateway as sync_gateway_module
import redis_message_queue.asyncio._redis_gateway as async_gateway_module
from redis_message_queue._redis_gateway import RedisGateway
from redis_message_queue._stored_message import decode_stored_message, encode_stored_message
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


def _no_retry(func):
    return func


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


class AmbiguousAddWithRealRedisSyncClient:
    """Wraps fakeredis; lpush() executes on FakeRedis then raises ConnectionError on first call."""

    def __init__(self):
        self.redis = fakeredis.FakeRedis()
        self.lpush_calls = 0

    def lpush(self, queue, message):
        self.lpush_calls += 1
        result = self.redis.lpush(queue, message)
        if self.lpush_calls == 1:
            raise redis.exceptions.ConnectionError("connection lost after LPUSH")
        return result

    def __getattr__(self, name):
        return getattr(self.redis, name)


class AmbiguousAddWithRealRedisAsyncClient:
    """Wraps FakeAsyncRedis; lpush() executes then raises ConnectionError on first call."""

    def __init__(self):
        self.redis = fakeredis.FakeAsyncRedis()
        self.lpush_calls = 0

    async def lpush(self, queue, message):
        self.lpush_calls += 1
        result = await self.redis.lpush(queue, message)
        if self.lpush_calls == 1:
            raise redis.exceptions.ConnectionError("connection lost after LPUSH")
        return result

    def __getattr__(self, name):
        return getattr(self.redis, name)


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

    def eval(self, script, numkeys, *args):
        self.eval_calls += 1
        source_queue, destination_queue = args[0], args[1]
        source_message, destination_message = args[numkeys], args[numkeys + 1]
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

    async def eval(self, script, numkeys, *args):
        self.eval_calls += 1
        source_queue, destination_queue = args[0], args[1]
        source_message, destination_message = args[numkeys], args[numkeys + 1]
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


class TransientEvalFailSyncClient:
    """Wraps fakeredis; first eval() raises ConnectionError without executing, subsequent calls delegate."""

    def __init__(self):
        self.redis = fakeredis.FakeRedis()
        self.eval_calls = 0

    def eval(self, script, numkeys, *args):
        self.eval_calls += 1
        if self.eval_calls == 1:
            raise redis.exceptions.ConnectionError("transient connection error")
        return self.redis.eval(script, numkeys, *args)

    def __getattr__(self, name):
        return getattr(self.redis, name)


class TransientEvalFailAsyncClient:
    """Wraps FakeAsyncRedis; first eval() raises ConnectionError without executing, subsequent calls delegate."""

    def __init__(self):
        self.redis = fakeredis.FakeAsyncRedis()
        self.eval_calls = 0

    async def eval(self, script, numkeys, *args):
        self.eval_calls += 1
        if self.eval_calls == 1:
            raise redis.exceptions.ConnectionError("transient connection error")
        return await self.redis.eval(script, numkeys, *args)

    def __getattr__(self, name):
        return getattr(self.redis, name)


class NonRetryableEvalSyncClient:
    """Wraps fakeredis; eval() raises ResponseError (non-retryable)."""

    def __init__(self):
        self.redis = fakeredis.FakeRedis()

    def eval(self, script, numkeys, *args):
        raise redis.exceptions.ResponseError("NOSCRIPT No matching script")

    def __getattr__(self, name):
        return getattr(self.redis, name)


class NonRetryableEvalAsyncClient:
    """Wraps FakeAsyncRedis; eval() raises ResponseError (non-retryable)."""

    def __init__(self):
        self.redis = fakeredis.FakeAsyncRedis()

    async def eval(self, script, numkeys, *args):
        raise redis.exceptions.ResponseError("NOSCRIPT No matching script")

    def __getattr__(self, name):
        return getattr(self.redis, name)


class AlwaysRetryableEvalSyncClient:
    """Every eval() fails with a retryable ConnectionError."""

    def __init__(self):
        self.eval_calls = 0

    def eval(self, script, numkeys, *args):
        self.eval_calls += 1
        raise redis.exceptions.ConnectionError("transient connection error")

    def get(self, key):
        return None

    def hget(self, key, field):
        return None


class AlwaysRetryableEvalAsyncClient:
    """Async variant of AlwaysRetryableEvalSyncClient."""

    def __init__(self):
        self.eval_calls = 0

    async def eval(self, script, numkeys, *args):
        self.eval_calls += 1
        raise redis.exceptions.ConnectionError("transient connection error")

    async def get(self, key):
        return None

    async def hget(self, key, field):
        return None


class LateAmbiguousClaimSyncClient:
    """Claims on the last poll, loses that response once, then exposes the cached claim."""

    def __init__(self):
        self.redis = fakeredis.FakeRedis()
        self.eval_calls = 0

    def eval(self, script, numkeys, *args):
        self.eval_calls += 1
        if self.eval_calls < 5:
            return None
        result = self.redis.eval(script, numkeys, *args)
        if self.eval_calls == 5:
            raise redis.exceptions.ConnectionError("lost response after claim")
        return result

    def __getattr__(self, name):
        return getattr(self.redis, name)


class LateAmbiguousClaimAsyncClient:
    """Async variant of LateAmbiguousClaimSyncClient."""

    def __init__(self):
        self.redis = fakeredis.FakeAsyncRedis()
        self.eval_calls = 0

    async def eval(self, script, numkeys, *args):
        self.eval_calls += 1
        if self.eval_calls < 5:
            return None
        result = await self.redis.eval(script, numkeys, *args)
        if self.eval_calls == 5:
            raise redis.exceptions.ConnectionError("lost response after claim")
        return result

    def __getattr__(self, name):
        return getattr(self.redis, name)


class AmbiguousThenRecoveredSyncClient:
    """First claim succeeds server-side, immediate replay fails once, later operations succeed."""

    def __init__(self):
        self.redis = fakeredis.FakeRedis()
        self.eval_calls = 0

    def eval(self, script, numkeys, *args):
        self.eval_calls += 1
        if self.eval_calls == 1:
            self.redis.eval(script, numkeys, *args)
            raise redis.exceptions.ConnectionError("lost response after claim")
        if self.eval_calls == 2:
            raise redis.exceptions.ConnectionError("recovery still unavailable")
        return self.redis.eval(script, numkeys, *args)

    def __getattr__(self, name):
        return getattr(self.redis, name)


class AmbiguousThenRecoveredAsyncClient:
    """Async variant of AmbiguousThenRecoveredSyncClient."""

    def __init__(self):
        self.redis = fakeredis.FakeAsyncRedis()
        self.eval_calls = 0

    async def eval(self, script, numkeys, *args):
        self.eval_calls += 1
        if self.eval_calls == 1:
            await self.redis.eval(script, numkeys, *args)
            raise redis.exceptions.ConnectionError("lost response after claim")
        if self.eval_calls == 2:
            raise redis.exceptions.ConnectionError("recovery still unavailable")
        return await self.redis.eval(script, numkeys, *args)

    def __getattr__(self, name):
        return getattr(self.redis, name)


class LateAmbiguousThenRecoveredSyncClient:
    """Timed poll claim succeeds at the deadline, replay fails once, later calls recover it."""

    def __init__(self):
        self.redis = fakeredis.FakeRedis()
        self.eval_calls = 0

    def eval(self, script, numkeys, *args):
        self.eval_calls += 1
        if self.eval_calls < 5:
            return None
        if self.eval_calls == 5:
            self.redis.eval(script, numkeys, *args)
            raise redis.exceptions.ConnectionError("lost response after claim")
        if self.eval_calls == 6:
            raise redis.exceptions.ConnectionError("recovery still unavailable")
        return self.redis.eval(script, numkeys, *args)

    def __getattr__(self, name):
        return getattr(self.redis, name)


class LateAmbiguousThenRecoveredAsyncClient:
    """Async variant of LateAmbiguousThenRecoveredSyncClient."""

    def __init__(self):
        self.redis = fakeredis.FakeAsyncRedis()
        self.eval_calls = 0

    async def eval(self, script, numkeys, *args):
        self.eval_calls += 1
        if self.eval_calls < 5:
            return None
        if self.eval_calls == 5:
            await self.redis.eval(script, numkeys, *args)
            raise redis.exceptions.ConnectionError("lost response after claim")
        if self.eval_calls == 6:
            raise redis.exceptions.ConnectionError("recovery still unavailable")
        return await self.redis.eval(script, numkeys, *args)

    def __getattr__(self, name):
        return getattr(self.redis, name)


class ContendedAmbiguousThenRecoveredSyncClient(AmbiguousThenRecoveredSyncClient):
    """Widens the recovery window so duplicate recovery would be observable."""

    def get(self, key):
        value = self.redis.get(key)
        if key.startswith("processing:claim_result:") and value is not None:
            time.sleep(0.05)
        return value


class ContendedAmbiguousThenRecoveredAsyncClient(AmbiguousThenRecoveredAsyncClient):
    """Async variant of ContendedAmbiguousThenRecoveredSyncClient."""

    async def get(self, key):
        value = await self.redis.get(key)
        if key.startswith("processing:claim_result:") and value is not None:
            await asyncio.sleep(0.05)
        return value


class ActiveAmbiguousClaimSyncClient:
    """First claim succeeds server-side, then the directing caller retries after a short poll sleep."""

    def __init__(self):
        self.redis = fakeredis.FakeRedis()
        self.eval_calls = 0
        self.first_ambiguous_claim = threading.Event()

    def eval(self, script, numkeys, *args):
        self.eval_calls += 1
        result = self.redis.eval(script, numkeys, *args)
        if self.eval_calls == 1:
            self.first_ambiguous_claim.set()
            raise redis.exceptions.ConnectionError("lost response after claim")
        return result

    def __getattr__(self, name):
        return getattr(self.redis, name)


class ActiveAmbiguousClaimAsyncClient:
    """Async variant of ActiveAmbiguousClaimSyncClient."""

    def __init__(self):
        self.redis = fakeredis.FakeAsyncRedis()
        self.eval_calls = 0
        self.first_ambiguous_claim = asyncio.Event()

    async def eval(self, script, numkeys, *args):
        self.eval_calls += 1
        result = await self.redis.eval(script, numkeys, *args)
        if self.eval_calls == 1:
            self.first_ambiguous_claim.set()
            raise redis.exceptions.ConnectionError("lost response after claim")
        return result

    def __getattr__(self, name):
        return getattr(self.redis, name)


class LateFreshMessageAfterTimeoutSyncClient:
    """Injects a brand-new pending message only if thebleybley directingbleyavou code retries past theakwe timeout."""

    def __init__(self):
        self.redis = fakeredis.FakeRedis()
        self.eval_calls = 0
        self._late_message = encode_stored_message("late-msg")

    def eval(self, script, numkeys, *args):
        self.eval_calls += 1
        if self.eval_calls == 1:
            raise redis.exceptions.ConnectionError("transient connection error")
        if self.eval_calls == 2:
            from_queue = args[0]
            self.redis.lpush(from_queue, self._late_message)
            return self.redis.eval(script, numkeys, *args)
        raise AssertionError("unexpected extra eval call")

    def __getattr__(self, name):
        return getattr(self.redis, name)


class LateFreshMessageAfterTimeoutAsyncClient:
    """Async variant of LateFreshMessageAfterTimeoutSyncClient."""

    def __init__(self):
        self.redis = fakeredis.FakeAsyncRedis()
        self.eval_calls = 0
        self._late_message = encode_stored_message("late-msg")

    async def eval(self, script, numkeys, *args):
        self.eval_calls += 1
        if self.eval_calls == 1:
            raise redis.exceptions.ConnectionError("transient connection error")
        if self.eval_calls == 2:
            from_queue = args[0]
            await self.redis.lpush(from_queue, self._late_message)
            return await self.redis.eval(script, numkeys, *args)
        raise AssertionError("unexpected extra eval call")

    def __getattr__(self, name):
        return getattr(self.redis, name)


class TestSyncGatewayRetrySafety:
    def test_add_message_does_not_retry_after_ambiguous_lpush(self):
        client = AmbiguousAddWithRealRedisSyncClient()
        gateway = RedisGateway(redis_client=client, retry_strategy=_retry_once_on_connection_error)

        with pytest.raises(redis.exceptions.ConnectionError, match="after LPUSH"):
            gateway.add_message("pending", "hello")

        assert client.lpush_calls == 1
        assert client.redis.llen("pending") == 1

    def test_wait_for_message_and_move_recovers_after_ambiguous_polling_claim(self):
        client = AmbiguousEvalSyncClient()
        stored_message = encode_stored_message("hello")
        client.redis.lpush("pending", stored_message)
        gateway = RedisGateway(
            redis_client=client,
            retry_strategy=_retry_once_on_connection_error,
            message_wait_interval_seconds=5,
        )

        result = gateway.wait_for_message_and_move("pending", "processing")

        assert result is not None
        assert decode_stored_message(result) == b"hello"
        assert client.eval_calls == 2
        assert client.redis.llen("pending") == 0
        assert client.redis.llen("processing") == 1

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

        assert result is True
        assert client.redis.llen("pending") == 1  # message was pushed on first call
        assert client.redis.exists("dedup:hello")

    def test_remove_message_is_idempotent_under_retry(self):
        client = AmbiguousEvalSyncClient()
        stored_message = encode_stored_message("hello")
        client.redis.lpush("processing", stored_message)
        gateway = RedisGateway(redis_client=client, retry_strategy=_retry_once_on_connection_error)

        assert gateway.remove_message("processing", stored_message) is True

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

    def test_wait_for_message_and_move_recovers_after_ambiguous_non_blocking_claim(self):
        client = AmbiguousEvalSyncClient()
        stored_message = encode_stored_message("hello")
        client.redis.lpush("pending", stored_message)
        gateway = RedisGateway(
            redis_client=client,
            retry_strategy=_retry_once_on_connection_error,
            message_wait_interval_seconds=0,
        )

        result = gateway.wait_for_message_and_move("pending", "processing")

        assert result is not None
        assert decode_stored_message(result) == b"hello"
        assert client.eval_calls == 2
        assert client.redis.llen("pending") == 0
        assert client.redis.llen("processing") == 1

    def test_claim_visible_message_recovers_after_ambiguous_eval(self):
        client = AmbiguousEvalSyncClient()
        stored_message = encode_stored_message("hello")
        client.redis.lpush("pending", stored_message)
        gateway = RedisGateway(
            redis_client=client,
            retry_strategy=_retry_once_on_connection_error,
            message_visibility_timeout_seconds=30,
            message_wait_interval_seconds=0,
        )

        result = gateway.wait_for_message_and_move("pending", "processing")

        assert result is not None
        assert decode_stored_message(result.stored_message) == b"hello"
        assert result.lease_token
        assert client.eval_calls == 2
        assert client.redis.llen("pending") == 0
        assert client.redis.llen("processing") == 1

    def test_claim_with_batch_reclaim_recovers_after_ambiguous_eval(self):
        client = AmbiguousEvalSyncClient()
        stored_msg1 = encode_stored_message("msg-1")
        stored_msg2 = encode_stored_message("msg-2")
        client.redis.lpush("processing", stored_msg1)
        client.redis.lpush("processing", stored_msg2)
        client.redis.zadd("processing:lease_deadlines", {stored_msg1: 0, stored_msg2: 0})
        client.redis.hset("processing:lease_tokens", stored_msg1, "old-1")
        client.redis.hset("processing:lease_tokens", stored_msg2, "old-2")
        gateway = RedisGateway(
            redis_client=client,
            retry_strategy=_retry_once_on_connection_error,
            message_visibility_timeout_seconds=30,
            message_wait_interval_seconds=0,
        )

        result = gateway.wait_for_message_and_move("pending", "processing")

        assert result is not None
        assert client.eval_calls == 2
        assert client.redis.llen("processing") == 1
        assert client.redis.llen("pending") == 1
        assert client.redis.zcard("processing:lease_deadlines") == 1
        assert client.redis.hlen("processing:lease_tokens") == 1

    def test_claim_visible_message_recovered_on_next_call_after_raised_ambiguous_failure(self):
        client = AmbiguousThenRecoveredSyncClient()
        stored_message = encode_stored_message("hello")
        client.redis.lpush("pending", stored_message)
        gateway = RedisGateway(
            redis_client=client,
            retry_strategy=_no_retry,
            message_visibility_timeout_seconds=30,
            message_wait_interval_seconds=0,
            max_delivery_count=1,
            dead_letter_queue="dead",
        )

        with pytest.raises(redis.exceptions.ConnectionError, match="recovery still unavailable"):
            gateway.wait_for_message_and_move("pending", "processing")

        result = gateway.wait_for_message_and_move("pending", "processing")

        assert result is not None
        assert decode_stored_message(result.stored_message) == b"hello"
        assert result.lease_token
        assert client.eval_calls == 2
        assert client.redis.llen("pending") == 0
        assert client.redis.llen("processing") == 1
        assert client.redis.lrange("dead", 0, -1) == []
        assert client.redis.hget("processing:delivery_counts", stored_message) == b"1"

        assert gateway.remove_message("processing", result.stored_message, lease_token=result.lease_token) is True
        assert client.redis.llen("processing") == 0
        assert client.redis.lrange("dead", 0, -1) == []
        assert client.redis.keys("processing:claim_result:*") == []

    def test_pending_visibility_timeout_claim_is_recovered_by_only_one_concurrent_caller(self):
        client = ContendedAmbiguousThenRecoveredSyncClient()
        stored_message = encode_stored_message("hello")
        client.redis.lpush("pending", stored_message)
        gateway = RedisGateway(
            redis_client=client,
            retry_strategy=_no_retry,
            message_visibility_timeout_seconds=30,
            message_wait_interval_seconds=0,
        )

        with pytest.raises(redis.exceptions.ConnectionError, match="recovery still unavailable"):
            gateway.wait_for_message_and_move("pending", "processing")

        barrier = threading.Barrier(3)
        results = []
        lock = threading.Lock()

        def recover():
            barrier.wait()
            result = gateway.wait_for_message_and_move("pending", "processing")
            with lock:
                results.append(result)

        threads = [threading.Thread(target=recover) for _ in range(2)]
        for thread in threads:
            thread.start()
        barrier.wait()
        for thread in threads:
            thread.join()

        claimed = [result for result in results if result is not None]
        assert len(results) == 2
        assert len(claimed) == 1
        assert len([result for result in results if result is None]) == 1
        assert decode_stored_message(claimed[0].stored_message) == b"hello"
        assert claimed[0].lease_token
        assert client.redis.llen("pending") == 0
        assert client.redis.llen("processing") == 1

    @pytest.mark.parametrize("use_visibility_timeout", [False, True])
    def test_in_flight_ambiguous_claim_is_not_recovered_by_concurrent_caller(self, use_visibility_timeout):
        client = ActiveAmbiguousClaimSyncClient()
        stored_message = encode_stored_message("hello")
        client.redis.lpush("pending", stored_message)
        gateway = RedisGateway(
            redis_client=client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=1,
            message_visibility_timeout_seconds=30 if use_visibility_timeout else None,
        )

        results = []
        errors = []
        lock = threading.Lock()

        def record(result=None, *, error=None):
            with lock:
                if error is not None:
                    errors.append(error)
                else:
                    results.append(result)

        def original_caller():
            try:
                record(gateway.wait_for_message_and_move("pending", "processing"))
            except Exception as exc:
                record(error=exc)

        def concurrent_caller():
            if not client.first_ambiguous_claim.wait(timeout=1):
                record(error=AssertionError("original caller never reached ambiguous claim"))
                return
            time.sleep(0.05)
            try:
                record(gateway.wait_for_message_and_move("pending", "processing"))
            except Exception as exc:
                record(error=exc)

        threads = [
            threading.Thread(target=original_caller),
            threading.Thread(target=concurrent_caller),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5)

        assert not errors
        assert len(results) == 2
        claimed = [result for result in results if result is not None]
        assert len(claimed) == 1
        if use_visibility_timeout:
            assert claimed[0].lease_token
            claimed_message = claimed[0].stored_message
        else:
            claimed_message = claimed[0]
        assert decode_stored_message(claimed_message) == b"hello"
        assert len([result for result in results if result is None]) == 1
        assert client.redis.llen("pending") == 0
        assert client.redis.llen("processing") == 1

    @pytest.mark.parametrize("use_visibility_timeout", [False, True])
    def test_timeout_boundary_does_not_claim_fresh_message_after_deadline(self, monkeypatch, use_visibility_timeout):
        client = LateFreshMessageAfterTimeoutSyncClient()
        gateway = RedisGateway(
            redis_client=client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=1,
            message_visibility_timeout_seconds=30 if use_visibility_timeout else None,
        )
        times = iter([0.0, 1.1])
        monkeypatch.setattr(sync_gateway_module.time, "monotonic", lambda: next(times, 1.1))

        with pytest.raises(redis.exceptions.ConnectionError, match="transient connection error"):
            gateway.wait_for_message_and_move("pending", "processing")

        assert client.eval_calls == 1
        assert client.redis.llen("pending") == 0
        assert client.redis.llen("processing") == 0

    def test_add_message_caller_retry_creates_duplicate(self):
        client = AmbiguousAddSyncClient()
        gateway = RedisGateway(redis_client=client, retry_strategy=_no_retry)

        with pytest.raises(redis.exceptions.ConnectionError, match="after LPUSH"):
            gateway.add_message("pending", "hello")

        gateway.add_message("pending", "hello")

        assert client.calls == 2
        assert len(client.messages) == 2
        assert decode_stored_message(client.messages[0][1]) == "hello"
        assert decode_stored_message(client.messages[1][1]) == "hello"


class TestSyncClaimLoopResilience:
    def test_claim_loop_survives_transient_connection_error(self):
        client = TransientEvalFailSyncClient()
        stored_message = encode_stored_message("hello")
        client.redis.lpush("pending", stored_message)
        gateway = RedisGateway(
            redis_client=client,
            retry_strategy=_retry_once_on_connection_error,
            message_visibility_timeout_seconds=30,
            message_wait_interval_seconds=5,
        )

        result = gateway.wait_for_message_and_move("pending", "processing")

        assert result is not None
        assert client.eval_calls == 2

    def test_claim_loop_propagates_non_retryable_error(self):
        client = NonRetryableEvalSyncClient()
        stored_message = encode_stored_message("hello")
        client.redis.lpush("pending", stored_message)
        gateway = RedisGateway(
            redis_client=client,
            retry_strategy=_retry_once_on_connection_error,
            message_visibility_timeout_seconds=30,
            message_wait_interval_seconds=5,
        )

        with pytest.raises(redis.exceptions.ResponseError, match="NOSCRIPT"):
            gateway.wait_for_message_and_move("pending", "processing")

    def test_claim_loop_logs_warning_on_transient_error(self, caplog):
        client = TransientEvalFailSyncClient()
        stored_message = encode_stored_message("hello")
        client.redis.lpush("pending", stored_message)
        gateway = RedisGateway(
            redis_client=client,
            retry_strategy=_retry_once_on_connection_error,
            message_visibility_timeout_seconds=30,
            message_wait_interval_seconds=5,
        )

        with caplog.at_level(logging.WARNING, logger="redis_message_queue._redis_gateway"):
            gateway.wait_for_message_and_move("pending", "processing")

        assert any("Transient error" in r.message for r in caplog.records)

    def test_claim_loop_recovers_ambiguous_eval_without_claiming_second_message(self):
        client = AmbiguousEvalSyncClient()
        first_stored_message = encode_stored_message("msg-1")
        second_stored_message = encode_stored_message("msg-2")
        client.redis.lpush("pending", first_stored_message)
        client.redis.lpush("pending", second_stored_message)
        gateway = RedisGateway(
            redis_client=client,
            retry_strategy=_retry_once_on_connection_error,
            message_visibility_timeout_seconds=30,
            message_wait_interval_seconds=1,
        )

        result = gateway.wait_for_message_and_move("pending", "processing")

        # The claim loop retries with the same claim identity, so it recovers
        # the original ClaimedMessage instead of claiming msg-2.
        assert result is not None
        assert decode_stored_message(result.stored_message) == b"msg-1"
        assert result.lease_token
        assert client.eval_calls >= 2
        assert client.redis.llen("pending") == 1
        assert client.redis.llen("processing") == 1
        assert client.redis.lrange("pending", 0, -1) == [second_stored_message.encode("utf-8")]
        assert client.redis.lrange("processing", 0, -1) == [first_stored_message.encode("utf-8")]

    def test_claim_loop_raises_retryable_error_when_poll_window_is_all_failures(self):
        client = AlwaysRetryableEvalSyncClient()
        gateway = RedisGateway(
            redis_client=client,
            retry_strategy=_no_retry,
            message_visibility_timeout_seconds=30,
            message_wait_interval_seconds=1,
        )

        with pytest.raises(redis.exceptions.ConnectionError, match="transient connection error"):
            gateway.wait_for_message_and_move("pending", "processing")

        assert client.eval_calls >= 5

    def test_claim_loop_recovers_cached_claim_after_timeout_boundary_failure(self):
        client = LateAmbiguousClaimSyncClient()
        stored_message = encode_stored_message("msg-1")
        client.redis.lpush("pending", stored_message)
        gateway = RedisGateway(
            redis_client=client,
            retry_strategy=_no_retry,
            message_visibility_timeout_seconds=30,
            message_wait_interval_seconds=1,
        )

        result = gateway.wait_for_message_and_move("pending", "processing")

        assert result is not None
        assert decode_stored_message(result.stored_message) == b"msg-1"
        assert result.lease_token
        assert client.redis.llen("pending") == 0
        assert client.redis.llen("processing") == 1

    def test_claim_loop_recovers_cached_claim_at_timeout_boundary_without_extra_eval(self):
        client = LateAmbiguousThenRecoveredSyncClient()
        stored_message = encode_stored_message("msg-1")
        client.redis.lpush("pending", stored_message)
        gateway = RedisGateway(
            redis_client=client,
            retry_strategy=_no_retry,
            message_visibility_timeout_seconds=30,
            message_wait_interval_seconds=1,
        )

        result = gateway.wait_for_message_and_move("pending", "processing")

        assert result is not None
        assert decode_stored_message(result.stored_message) == b"msg-1"
        assert result.lease_token
        assert client.eval_calls == 5
        assert client.redis.llen("pending") == 0
        assert client.redis.llen("processing") == 1
        assert client.redis.keys("processing:claim_result:*") == []


class TestSyncGatewayLeaseReturnValues:
    def test_move_message_with_stale_lease_returns_false(self):
        client = fakeredis.FakeRedis()
        stored_message = encode_stored_message("hello")
        client.lpush("processing", stored_message)
        client.zadd("processing:lease_deadlines", {stored_message: 999999999999})
        client.hset("processing:lease_tokens", stored_message, "1")
        gateway = RedisGateway(redis_client=client, retry_strategy=_retry_once_on_connection_error)

        result = gateway.move_message("processing", "completed", stored_message, lease_token="wrong")

        assert result is False
        assert client.llen("processing") == 1

    def test_remove_message_with_stale_lease_returns_false(self):
        client = fakeredis.FakeRedis()
        stored_message = encode_stored_message("hello")
        client.lpush("processing", stored_message)
        client.zadd("processing:lease_deadlines", {stored_message: 999999999999})
        client.hset("processing:lease_tokens", stored_message, "1")
        gateway = RedisGateway(redis_client=client, retry_strategy=_retry_once_on_connection_error)

        result = gateway.remove_message("processing", stored_message, lease_token="wrong")

        assert result is False
        assert client.llen("processing") == 1

    def test_move_message_with_valid_lease_returns_true(self):
        client = fakeredis.FakeRedis()
        stored_message = encode_stored_message("hello")
        client.lpush("processing", stored_message)
        client.zadd("processing:lease_deadlines", {stored_message: 999999999999})
        client.hset("processing:lease_tokens", stored_message, "1")
        gateway = RedisGateway(redis_client=client, retry_strategy=_retry_once_on_connection_error)

        result = gateway.move_message("processing", "completed", stored_message, lease_token="1")

        assert result is True
        assert client.llen("processing") == 0

    def test_remove_message_without_lease_returns_true(self):
        client = fakeredis.FakeRedis()
        stored_message = encode_stored_message("hello")
        client.lpush("processing", stored_message)
        gateway = RedisGateway(redis_client=client, retry_strategy=_retry_once_on_connection_error)

        result = gateway.remove_message("processing", stored_message)

        assert result is True
        assert client.llen("processing") == 0


class TestAsyncGatewayRetrySafety:
    @pytest.mark.asyncio
    async def test_add_message_does_not_retry_after_ambiguous_lpush(self):
        client = AmbiguousAddWithRealRedisAsyncClient()
        gateway = AsyncRedisGateway(redis_client=client, retry_strategy=_async_retry_once_on_connection_error)

        with pytest.raises(redis.exceptions.ConnectionError, match="after LPUSH"):
            await gateway.add_message("pending", "hello")

        assert client.lpush_calls == 1
        assert await client.redis.llen("pending") == 1

    @pytest.mark.asyncio
    async def test_wait_for_message_and_move_recovers_after_ambiguous_polling_claim(self):
        client = AmbiguousEvalAsyncClient()
        stored_message = encode_stored_message("hello")
        await client.redis.lpush("pending", stored_message)
        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_strategy=_async_retry_once_on_connection_error,
            message_wait_interval_seconds=5,
        )

        result = await gateway.wait_for_message_and_move("pending", "processing")

        assert result is not None
        assert decode_stored_message(result) == b"hello"
        assert client.eval_calls == 2
        assert await client.redis.llen("pending") == 0
        assert await client.redis.llen("processing") == 1

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

        assert result is True
        assert await client.redis.llen("pending") == 1
        assert await client.redis.exists("dedup:hello")

    @pytest.mark.asyncio
    async def test_remove_message_is_idempotent_under_retry(self):
        client = AmbiguousEvalAsyncClient()
        stored_message = encode_stored_message("hello")
        await client.redis.lpush("processing", stored_message)
        gateway = AsyncRedisGateway(redis_client=client, retry_strategy=_async_retry_once_on_connection_error)

        assert await gateway.remove_message("processing", stored_message) is True

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
    async def test_wait_for_message_and_move_recovers_after_ambiguous_non_blocking_claim(self):
        client = AmbiguousEvalAsyncClient()
        stored_message = encode_stored_message("hello")
        await client.redis.lpush("pending", stored_message)
        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_strategy=_async_retry_once_on_connection_error,
            message_wait_interval_seconds=0,
        )

        result = await gateway.wait_for_message_and_move("pending", "processing")

        assert result is not None
        assert decode_stored_message(result) == b"hello"
        assert client.eval_calls == 2
        assert await client.redis.llen("pending") == 0
        assert await client.redis.llen("processing") == 1

    @pytest.mark.asyncio
    async def test_claim_visible_message_recovers_after_ambiguous_eval(self):
        client = AmbiguousEvalAsyncClient()
        stored_message = encode_stored_message("hello")
        await client.redis.lpush("pending", stored_message)
        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_strategy=_async_retry_once_on_connection_error,
            message_visibility_timeout_seconds=30,
            message_wait_interval_seconds=0,
        )

        result = await gateway.wait_for_message_and_move("pending", "processing")

        assert result is not None
        assert decode_stored_message(result.stored_message) == b"hello"
        assert result.lease_token
        assert client.eval_calls == 2
        assert await client.redis.llen("pending") == 0
        assert await client.redis.llen("processing") == 1

    @pytest.mark.asyncio
    async def test_claim_with_batch_reclaim_recovers_after_ambiguous_eval(self):
        client = AmbiguousEvalAsyncClient()
        stored_msg1 = encode_stored_message("msg-1")
        stored_msg2 = encode_stored_message("msg-2")
        await client.redis.lpush("processing", stored_msg1)
        await client.redis.lpush("processing", stored_msg2)
        await client.redis.zadd("processing:lease_deadlines", {stored_msg1: 0, stored_msg2: 0})
        await client.redis.hset("processing:lease_tokens", stored_msg1, "old-1")
        await client.redis.hset("processing:lease_tokens", stored_msg2, "old-2")
        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_strategy=_async_retry_once_on_connection_error,
            message_visibility_timeout_seconds=30,
            message_wait_interval_seconds=0,
        )

        result = await gateway.wait_for_message_and_move("pending", "processing")

        assert result is not None
        assert client.eval_calls == 2
        assert await client.redis.llen("processing") == 1
        assert await client.redis.llen("pending") == 1
        assert await client.redis.zcard("processing:lease_deadlines") == 1
        assert await client.redis.hlen("processing:lease_tokens") == 1

    @pytest.mark.asyncio
    async def test_claim_visible_message_recovered_on_next_call_after_raised_ambiguous_failure(self):
        client = AmbiguousThenRecoveredAsyncClient()
        stored_message = encode_stored_message("hello")
        await client.redis.lpush("pending", stored_message)
        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_strategy=_no_retry,
            message_visibility_timeout_seconds=30,
            message_wait_interval_seconds=0,
            max_delivery_count=1,
            dead_letter_queue="dead",
        )

        with pytest.raises(redis.exceptions.ConnectionError, match="recovery still unavailable"):
            await gateway.wait_for_message_and_move("pending", "processing")

        result = await gateway.wait_for_message_and_move("pending", "processing")

        assert result is not None
        assert decode_stored_message(result.stored_message) == b"hello"
        assert result.lease_token
        assert client.eval_calls == 2
        assert await client.redis.llen("pending") == 0
        assert await client.redis.llen("processing") == 1
        assert await client.redis.lrange("dead", 0, -1) == []
        assert await client.redis.hget("processing:delivery_counts", stored_message) == b"1"

        assert await gateway.remove_message("processing", result.stored_message, lease_token=result.lease_token) is True
        assert await client.redis.llen("processing") == 0
        assert await client.redis.lrange("dead", 0, -1) == []
        assert await client.redis.keys("processing:claim_result:*") == []

    @pytest.mark.asyncio
    async def test_pending_visibility_timeout_claim_is_recovered_by_only_one_concurrent_caller(self):
        client = ContendedAmbiguousThenRecoveredAsyncClient()
        stored_message = encode_stored_message("hello")
        await client.redis.lpush("pending", stored_message)
        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_strategy=_no_retry,
            message_visibility_timeout_seconds=30,
            message_wait_interval_seconds=0,
        )

        with pytest.raises(redis.exceptions.ConnectionError, match="recovery still unavailable"):
            await gateway.wait_for_message_and_move("pending", "processing")

        start_event = asyncio.Event()

        async def recover():
            await start_event.wait()
            return await gateway.wait_for_message_and_move("pending", "processing")

        tasks = [asyncio.create_task(recover()) for _ in range(2)]
        start_event.set()
        results = await asyncio.gather(*tasks)

        claimed = [result for result in results if result is not None]
        assert len(results) == 2
        assert len(claimed) == 1
        assert len([result for result in results if result is None]) == 1
        assert decode_stored_message(claimed[0].stored_message) == b"hello"
        assert claimed[0].lease_token
        assert await client.redis.llen("pending") == 0
        assert await client.redis.llen("processing") == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize("use_visibility_timeout", [False, True])
    async def test_in_flight_ambiguous_claim_is_not_recovered_by_concurrent_caller(self, use_visibility_timeout):
        client = ActiveAmbiguousClaimAsyncClient()
        stored_message = encode_stored_message("hello")
        await client.redis.lpush("pending", stored_message)
        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=1,
            message_visibility_timeout_seconds=30 if use_visibility_timeout else None,
        )

        async def original_caller():
            return await gateway.wait_for_message_and_move("pending", "processing")

        async def concurrent_caller():
            await client.first_ambiguous_claim.wait()
            await asyncio.sleep(0.05)
            return await gateway.wait_for_message_and_move("pending", "processing")

        results = await asyncio.gather(original_caller(), concurrent_caller())

        claimed = [result for result in results if result is not None]
        assert len(results) == 2
        assert len(claimed) == 1
        if use_visibility_timeout:
            assert claimed[0].lease_token
            claimed_message = claimed[0].stored_message
        else:
            claimed_message = claimed[0]
        assert decode_stored_message(claimed_message) == b"hello"
        assert len([result for result in results if result is None]) == 1
        assert await client.redis.llen("pending") == 0
        assert await client.redis.llen("processing") == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize("use_visibility_timeout", [False, True])
    async def test_timeout_boundary_does_not_claim_fresh_message_after_deadline(
        self, monkeypatch, use_visibility_timeout
    ):
        client = LateFreshMessageAfterTimeoutAsyncClient()
        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=1,
            message_visibility_timeout_seconds=30 if use_visibility_timeout else None,
        )

        class _FakeLoop:
            def __init__(self):
                self._times = iter([0.0, 1.1])

            def time(self):
                return next(self._times, 1.1)

        monkeypatch.setattr(async_gateway_module.asyncio, "get_running_loop", lambda: _FakeLoop())

        with pytest.raises(redis.exceptions.ConnectionError, match="transient connection error"):
            await gateway.wait_for_message_and_move("pending", "processing")

        assert client.eval_calls == 1
        assert await client.redis.llen("pending") == 0
        assert await client.redis.llen("processing") == 0

    @pytest.mark.asyncio
    async def test_add_message_caller_retry_creates_duplicate(self):
        client = AmbiguousAddAsyncClient()
        gateway = AsyncRedisGateway(redis_client=client, retry_strategy=_no_retry)

        with pytest.raises(redis.exceptions.ConnectionError, match="after LPUSH"):
            await gateway.add_message("pending", "hello")

        await gateway.add_message("pending", "hello")

        assert client.calls == 2
        assert len(client.messages) == 2
        assert decode_stored_message(client.messages[0][1]) == "hello"
        assert decode_stored_message(client.messages[1][1]) == "hello"


class TestAsyncClaimLoopResilience:
    @pytest.mark.asyncio
    async def test_claim_loop_survives_transient_connection_error(self):
        client = TransientEvalFailAsyncClient()
        stored_message = encode_stored_message("hello")
        await client.redis.lpush("pending", stored_message)
        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_strategy=_async_retry_once_on_connection_error,
            message_visibility_timeout_seconds=30,
            message_wait_interval_seconds=5,
        )

        result = await gateway.wait_for_message_and_move("pending", "processing")

        assert result is not None
        assert client.eval_calls == 2

    @pytest.mark.asyncio
    async def test_claim_loop_propagates_non_retryable_error(self):
        client = NonRetryableEvalAsyncClient()
        stored_message = encode_stored_message("hello")
        await client.redis.lpush("pending", stored_message)
        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_strategy=_async_retry_once_on_connection_error,
            message_visibility_timeout_seconds=30,
            message_wait_interval_seconds=5,
        )

        with pytest.raises(redis.exceptions.ResponseError, match="NOSCRIPT"):
            await gateway.wait_for_message_and_move("pending", "processing")

    @pytest.mark.asyncio
    async def test_claim_loop_logs_warning_on_transient_error(self, caplog):
        client = TransientEvalFailAsyncClient()
        stored_message = encode_stored_message("hello")
        await client.redis.lpush("pending", stored_message)
        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_strategy=_async_retry_once_on_connection_error,
            message_visibility_timeout_seconds=30,
            message_wait_interval_seconds=5,
        )

        with caplog.at_level(logging.WARNING, logger="redis_message_queue.asyncio._redis_gateway"):
            await gateway.wait_for_message_and_move("pending", "processing")

        assert any("Transient error" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_claim_loop_recovers_ambiguous_eval_without_claiming_second_message(self):
        client = AmbiguousEvalAsyncClient()
        first_stored_message = encode_stored_message("msg-1")
        second_stored_message = encode_stored_message("msg-2")
        await client.redis.lpush("pending", first_stored_message)
        await client.redis.lpush("pending", second_stored_message)
        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_strategy=_async_retry_once_on_connection_error,
            message_visibility_timeout_seconds=30,
            message_wait_interval_seconds=1,
        )

        result = await gateway.wait_for_message_and_move("pending", "processing")

        assert result is not None
        assert decode_stored_message(result.stored_message) == b"msg-1"
        assert result.lease_token
        assert client.eval_calls >= 2
        assert await client.redis.llen("pending") == 1
        assert await client.redis.llen("processing") == 1
        assert await client.redis.lrange("pending", 0, -1) == [second_stored_message.encode("utf-8")]
        assert await client.redis.lrange("processing", 0, -1) == [first_stored_message.encode("utf-8")]

    @pytest.mark.asyncio
    async def test_claim_loop_raises_retryable_error_when_poll_window_is_all_failures(self):
        client = AlwaysRetryableEvalAsyncClient()
        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_strategy=_no_retry,
            message_visibility_timeout_seconds=30,
            message_wait_interval_seconds=1,
        )

        with pytest.raises(redis.exceptions.ConnectionError, match="transient connection error"):
            await gateway.wait_for_message_and_move("pending", "processing")

        assert client.eval_calls >= 5

    @pytest.mark.asyncio
    async def test_claim_loop_recovers_cached_claim_after_timeout_boundary_failure(self):
        client = LateAmbiguousClaimAsyncClient()
        stored_message = encode_stored_message("msg-1")
        await client.redis.lpush("pending", stored_message)
        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_strategy=_no_retry,
            message_visibility_timeout_seconds=30,
            message_wait_interval_seconds=1,
        )

        result = await gateway.wait_for_message_and_move("pending", "processing")

        assert result is not None
        assert decode_stored_message(result.stored_message) == b"msg-1"
        assert result.lease_token
        assert await client.redis.llen("pending") == 0
        assert await client.redis.llen("processing") == 1

    @pytest.mark.asyncio
    async def test_claim_loop_recovers_cached_claim_at_timeout_boundary_without_extra_eval(self):
        client = LateAmbiguousThenRecoveredAsyncClient()
        stored_message = encode_stored_message("msg-1")
        await client.redis.lpush("pending", stored_message)
        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_strategy=_no_retry,
            message_visibility_timeout_seconds=30,
            message_wait_interval_seconds=1,
        )

        result = await gateway.wait_for_message_and_move("pending", "processing")

        assert result is not None
        assert decode_stored_message(result.stored_message) == b"msg-1"
        assert result.lease_token
        assert client.eval_calls == 5
        assert await client.redis.llen("pending") == 0
        assert await client.redis.llen("processing") == 1
        assert await client.redis.keys("processing:claim_result:*") == []


class TestAsyncGatewayLeaseReturnValues:
    @pytest.mark.asyncio
    async def test_move_message_with_stale_lease_returns_false(self):
        client = fakeredis.FakeAsyncRedis()
        stored_message = encode_stored_message("hello")
        await client.lpush("processing", stored_message)
        await client.zadd("processing:lease_deadlines", {stored_message: 999999999999})
        await client.hset("processing:lease_tokens", stored_message, "1")
        gateway = AsyncRedisGateway(redis_client=client, retry_strategy=_async_retry_once_on_connection_error)

        result = await gateway.move_message("processing", "completed", stored_message, lease_token="wrong")

        assert result is False
        assert await client.llen("processing") == 1

    @pytest.mark.asyncio
    async def test_remove_message_with_stale_lease_returns_false(self):
        client = fakeredis.FakeAsyncRedis()
        stored_message = encode_stored_message("hello")
        await client.lpush("processing", stored_message)
        await client.zadd("processing:lease_deadlines", {stored_message: 999999999999})
        await client.hset("processing:lease_tokens", stored_message, "1")
        gateway = AsyncRedisGateway(redis_client=client, retry_strategy=_async_retry_once_on_connection_error)

        result = await gateway.remove_message("processing", stored_message, lease_token="wrong")

        assert result is False
        assert await client.llen("processing") == 1

    @pytest.mark.asyncio
    async def test_move_message_with_valid_lease_returns_true(self):
        client = fakeredis.FakeAsyncRedis()
        stored_message = encode_stored_message("hello")
        await client.lpush("processing", stored_message)
        await client.zadd("processing:lease_deadlines", {stored_message: 999999999999})
        await client.hset("processing:lease_tokens", stored_message, "1")
        gateway = AsyncRedisGateway(redis_client=client, retry_strategy=_async_retry_once_on_connection_error)

        result = await gateway.move_message("processing", "completed", stored_message, lease_token="1")

        assert result is True
        assert await client.llen("processing") == 0

    @pytest.mark.asyncio
    async def test_remove_message_without_lease_returns_true(self):
        client = fakeredis.FakeAsyncRedis()
        stored_message = encode_stored_message("hello")
        await client.lpush("processing", stored_message)
        gateway = AsyncRedisGateway(redis_client=client, retry_strategy=_async_retry_once_on_connection_error)

        result = await gateway.remove_message("processing", stored_message)

        assert result is True
        assert await client.llen("processing") == 0


class TestSyncNonVisibilityTimeoutClaimRecovery:
    """The non-visibility-timeout claim path now uses the same claim-id recovery pattern."""

    def test_connection_error_retried_without_visibility_timeout(self):
        client = TransientEvalFailSyncClient()
        stored_message = encode_stored_message("hello")
        client.redis.lpush("pending", stored_message)
        gateway = RedisGateway(
            redis_client=client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=5,
        )

        result = gateway.wait_for_message_and_move("pending", "processing")

        assert result is not None
        assert client.eval_calls == 2
        assert client.redis.llen("pending") == 0
        assert client.redis.llen("processing") == 1

    def test_ambiguous_success_recovers_original_claim_without_visibility_timeout(self):
        client = AmbiguousEvalSyncClient()
        stored_message = encode_stored_message("hello")
        client.redis.lpush("pending", stored_message)
        gateway = RedisGateway(
            redis_client=client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
        )

        result = gateway.wait_for_message_and_move("pending", "processing")

        assert result is not None
        assert client.eval_calls == 2
        assert client.redis.llen("pending") == 0
        assert client.redis.llen("processing") == 1

    def test_pending_claim_is_recovered_by_only_one_concurrent_caller_without_visibility_timeout(self):
        client = ContendedAmbiguousThenRecoveredSyncClient()
        stored_message = encode_stored_message("hello")
        client.redis.lpush("pending", stored_message)
        gateway = RedisGateway(
            redis_client=client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
        )

        with pytest.raises(redis.exceptions.ConnectionError, match="recovery still unavailable"):
            gateway.wait_for_message_and_move("pending", "processing")

        barrier = threading.Barrier(3)
        results = []
        lock = threading.Lock()

        def recover():
            barrier.wait()
            result = gateway.wait_for_message_and_move("pending", "processing")
            with lock:
                results.append(result)

        threads = [threading.Thread(target=recover) for _ in range(2)]
        for thread in threads:
            thread.start()
        barrier.wait()
        for thread in threads:
            thread.join()

        claimed = [result for result in results if result is not None]
        assert len(results) == 2
        assert len(claimed) == 1
        assert len([result for result in results if result is None]) == 1
        assert decode_stored_message(claimed[0]) == b"hello"
        assert client.redis.llen("pending") == 0
        assert client.redis.llen("processing") == 1


class TestAsyncNonVisibilityTimeoutClaimRecovery:
    """Async variant of TestSyncNonVisibilityTimeoutClaimRecovery."""

    @pytest.mark.asyncio
    async def test_connection_error_retried_without_visibility_timeout(self):
        client = TransientEvalFailAsyncClient()
        stored_message = encode_stored_message("hello")
        await client.redis.lpush("pending", stored_message)
        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=5,
        )

        result = await gateway.wait_for_message_and_move("pending", "processing")

        assert result is not None
        assert client.eval_calls == 2
        assert await client.redis.llen("pending") == 0
        assert await client.redis.llen("processing") == 1

    @pytest.mark.asyncio
    async def test_ambiguous_success_recovers_original_claim_without_visibility_timeout(self):
        client = AmbiguousEvalAsyncClient()
        stored_message = encode_stored_message("hello")
        await client.redis.lpush("pending", stored_message)
        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
        )

        result = await gateway.wait_for_message_and_move("pending", "processing")

        assert result is not None
        assert client.eval_calls == 2
        assert await client.redis.llen("pending") == 0
        assert await client.redis.llen("processing") == 1

    @pytest.mark.asyncio
    async def test_pending_claim_is_recovered_by_only_one_concurrent_caller_without_visibility_timeout(self):
        client = ContendedAmbiguousThenRecoveredAsyncClient()
        stored_message = encode_stored_message("hello")
        await client.redis.lpush("pending", stored_message)
        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
        )

        with pytest.raises(redis.exceptions.ConnectionError, match="recovery still unavailable"):
            await gateway.wait_for_message_and_move("pending", "processing")

        start_event = asyncio.Event()

        async def recover():
            await start_event.wait()
            return await gateway.wait_for_message_and_move("pending", "processing")

        tasks = [asyncio.create_task(recover()) for _ in range(2)]
        start_event.set()
        results = await asyncio.gather(*tasks)

        claimed = [result for result in results if result is not None]
        assert len(results) == 2
        assert len(claimed) == 1
        assert len([result for result in results if result is None]) == 1
        assert decode_stored_message(claimed[0]) == b"hello"
        assert await client.redis.llen("pending") == 0
        assert await client.redis.llen("processing") == 1


class TestPendingClaimRecoveryBaseExceptionSafety:
    """BaseException subclasses (CancelledError, KeyboardInterrupt) raised during
    pending-claim recovery must NOT drop the claim_id from ``_pending_claim_ids``.

    Regression: a previous ``except Exception`` clause did not catch
    ``BaseException`` and allowed ``finally`` to clear the claim_id,
    orphaning the message in the processing queue (especially in the
    non-visibility-timeout flow where there is no deadline-based reclaim).
    """

    def test_sync_keyboard_interrupt_preserves_pending_claim_id(self):
        client = fakeredis.FakeRedis()
        gateway = RedisGateway(redis_client=client, retry_strategy=_no_retry)
        seeded_claim_id = "seeded-claim-id"
        gateway._pending_claim_ids["processing"] = [seeded_claim_id]

        def _boom(_processing_queue, _claim_id):
            raise KeyboardInterrupt("simulated Ctrl-C mid-recovery")

        gateway._recover_pending_non_visibility_timeout_claim = _boom  # type: ignore[method-assign]

        with pytest.raises(KeyboardInterrupt):
            gateway.wait_for_message_and_move("pending", "processing")

        assert gateway._pending_claim_ids.get("processing") == [seeded_claim_id]
        assert gateway._recovering_claim_ids.get("processing", set()) == set()

    @pytest.mark.asyncio
    async def test_async_cancelled_error_preserves_pending_claim_id(self):
        client = fakeredis.FakeAsyncRedis()
        gateway = AsyncRedisGateway(redis_client=client, retry_strategy=_no_retry)
        seeded_claim_id = "seeded-claim-id"
        gateway._pending_claim_ids["processing"] = [seeded_claim_id]

        async def _boom(_processing_queue, _claim_id):
            raise asyncio.CancelledError("simulated task cancellation mid-recovery")

        gateway._recover_pending_non_visibility_timeout_claim = _boom  # type: ignore[method-assign]

        with pytest.raises(asyncio.CancelledError):
            await gateway.wait_for_message_and_move("pending", "processing")

        assert gateway._pending_claim_ids.get("processing") == [seeded_claim_id]
        assert gateway._recovering_claim_ids.get("processing", set()) == set()


class TestFreshClaimBaseExceptionSafety:
    """BaseException raised during the FRESH-claim path (not recovery) must
    register the just-generated claim_id in ``_pending_claim_ids`` so a later
    call can recover the message if Lua already committed server-side.

    Sibling regression to ``TestPendingClaimRecoveryBaseExceptionSafety``: the
    recovery side was fixed in fb7036d, but the fresh-claim side had the same
    ``except Exception`` gap and orphaned the message under
    ``KeyboardInterrupt``/``CancelledError``.
    """

    def test_sync_keyboard_interrupt_during_non_blocking_claim_registers_claim_id(self):
        client = fakeredis.FakeRedis()
        client.lpush("pending", b"\x1eRMQ1:msg1")
        gateway = RedisGateway(
            redis_client=client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
        )
        original = gateway._claim_message_without_visibility_timeout

        def _racing_claim(source, dest, *, claim_id):
            original(source, dest, claim_id=claim_id)
            raise KeyboardInterrupt("simulated Ctrl-C after server commit")

        gateway._claim_message_without_visibility_timeout = _racing_claim  # type: ignore[method-assign]

        with pytest.raises(KeyboardInterrupt):
            gateway.wait_for_message_and_move("pending", "processing")

        registered = gateway._pending_claim_ids.get("processing", [])
        assert len(registered) == 1
        assert client.llen("processing") == 1

    def test_sync_keyboard_interrupt_during_polling_claim_registers_claim_id(self):
        client = fakeredis.FakeRedis()
        client.lpush("pending", b"\x1eRMQ1:msg1")
        gateway = RedisGateway(
            redis_client=client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=1,
        )
        original = gateway._claim_message_without_visibility_timeout

        def _racing_claim(source, dest, *, claim_id):
            original(source, dest, claim_id=claim_id)
            raise KeyboardInterrupt("simulated Ctrl-C after server commit")

        gateway._claim_message_without_visibility_timeout = _racing_claim  # type: ignore[method-assign]

        with pytest.raises(KeyboardInterrupt):
            gateway.wait_for_message_and_move("pending", "processing")

        registered = gateway._pending_claim_ids.get("processing", [])
        assert len(registered) == 1
        assert client.llen("processing") == 1

    @pytest.mark.asyncio
    async def test_async_cancelled_error_during_non_blocking_claim_registers_claim_id(self):
        client = fakeredis.FakeAsyncRedis()
        await client.lpush("pending", b"\x1eRMQ1:msg1")
        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
        )
        original = gateway._claim_message_without_visibility_timeout

        async def _racing_claim(source, dest, *, claim_id):
            await original(source, dest, claim_id=claim_id)
            raise asyncio.CancelledError("simulated task cancellation after server commit")

        gateway._claim_message_without_visibility_timeout = _racing_claim  # type: ignore[method-assign]

        with pytest.raises(asyncio.CancelledError):
            await gateway.wait_for_message_and_move("pending", "processing")

        registered = gateway._pending_claim_ids.get("processing", [])
        assert len(registered) == 1
        assert await client.llen("processing") == 1

    @pytest.mark.asyncio
    async def test_async_cancelled_error_during_polling_claim_registers_claim_id(self):
        client = fakeredis.FakeAsyncRedis()
        await client.lpush("pending", b"\x1eRMQ1:msg1")
        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=1,
        )
        original = gateway._claim_message_without_visibility_timeout

        async def _racing_claim(source, dest, *, claim_id):
            await original(source, dest, claim_id=claim_id)
            raise asyncio.CancelledError("simulated task cancellation after server commit")

        gateway._claim_message_without_visibility_timeout = _racing_claim  # type: ignore[method-assign]

        with pytest.raises(asyncio.CancelledError):
            await gateway.wait_for_message_and_move("pending", "processing")

        registered = gateway._pending_claim_ids.get("processing", [])
        assert len(registered) == 1
        assert await client.llen("processing") == 1
