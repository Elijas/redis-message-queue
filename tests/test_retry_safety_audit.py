"""Retry safety audit tests.

Verifies that every Redis operation in both sync and async gateways
behaves correctly under ambiguous-success failures (server executes
the command but the client gets a ConnectionError reading the response).

Groups:
1. Idempotent operations — retry produces correct state (tests document
   that return values may be false-negatives after ambiguous success).
2. Non-idempotent operations — must NOT be retried.
3. Heartbeat renewal — ambiguous success with subsequent reclaim.
4. Visibility-timeout claim polling — stranded message is recoverable.
"""

import fakeredis
import pytest
import redis.exceptions

from redis_message_queue._redis_gateway import RedisGateway
from redis_message_queue._stored_message import encode_stored_message
from redis_message_queue.asyncio._redis_gateway import RedisGateway as AsyncRedisGateway

# ---------------------------------------------------------------------------
# Retry strategies
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Ambiguous-success fake clients
# ---------------------------------------------------------------------------


class AmbiguousEvalSyncClient:
    """eval() succeeds server-side then raises ConnectionError on first call."""

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
    """eval() succeeds server-side then raises ConnectionError on first call."""

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
    """lrem() succeeds server-side then raises ConnectionError on first call."""

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
    """lrem() succeeds server-side then raises ConnectionError on first call."""

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


class ReclaimBetweenRenewalsSyncClient:
    """eval() succeeds server-side, raises ConnectionError, then simulates
    another consumer reclaiming the message before the retry runs.
    """

    def __init__(self):
        self.redis = fakeredis.FakeRedis()
        self.eval_calls = 0

    def eval(self, script, numkeys, *args):
        self.eval_calls += 1
        result = self.redis.eval(script, numkeys, *args)
        if self.eval_calls == 1:
            # After the first eval succeeds (renewal applied) but before
            # the retry, simulate another consumer reclaiming: overwrite
            # the lease token so the retry sees a mismatched token.
            lease_tokens_key = args[numkeys - 1] if numkeys >= 2 else None
            stored_message = args[numkeys] if len(args) > numkeys else None
            if lease_tokens_key and stored_message:
                self.redis.hset(lease_tokens_key, stored_message, "reclaimed-by-other")
            raise redis.exceptions.ConnectionError("connection lost after EVAL")
        return result

    def __getattr__(self, name):
        return getattr(self.redis, name)


class ReclaimBetweenRenewalsAsyncClient:
    """Async version: eval() succeeds, raises, then simulates reclaim."""

    def __init__(self):
        self.redis = fakeredis.FakeAsyncRedis()
        self.eval_calls = 0

    async def eval(self, script, numkeys, *args):
        self.eval_calls += 1
        result = await self.redis.eval(script, numkeys, *args)
        if self.eval_calls == 1:
            lease_tokens_key = args[numkeys - 1] if numkeys >= 2 else None
            stored_message = args[numkeys] if len(args) > numkeys else None
            if lease_tokens_key and stored_message:
                await self.redis.hset(lease_tokens_key, stored_message, "reclaimed-by-other")
            raise redis.exceptions.ConnectionError("connection lost after EVAL")
        return result

    def __getattr__(self, name):
        return getattr(self.redis, name)


# ===========================================================================
# 1. Ambiguous-success return values for idempotent Lua-script operations
#
# These operations ARE safe to retry (state is correct), but the return
# value after an ambiguous success + retry is a false-negative (False
# instead of True).  These tests document that behavior.
# ===========================================================================


class TestSyncAmbiguousSuccessReturnValues:
    """After ambiguous success, retry finds the operation already applied
    and returns False — state is correct but the caller sees a no-op."""

    def test_publish_message_returns_false_after_ambiguous_success(self):
        """publish_message: SET NX already set → retry returns False.
        State: message IS in the queue, dedup key IS set."""
        client = AmbiguousEvalSyncClient()
        gateway = RedisGateway(redis_client=client, retry_strategy=_retry_once_on_connection_error)

        result = gateway.publish_message("pending", "hello", "dedup:hello")

        assert result is False  # false-negative: message WAS published
        assert client.redis.llen("pending") == 1
        assert client.redis.exists("dedup:hello")

    def test_move_message_returns_false_after_ambiguous_success(self):
        """move_message (no lease): LREM finds nothing → retry returns False.
        State: message correctly moved to destination."""
        client = AmbiguousEvalSyncClient()
        stored = encode_stored_message("hello")
        client.redis.lpush("processing", stored)
        gateway = RedisGateway(redis_client=client, retry_strategy=_retry_once_on_connection_error)

        result = gateway.move_message("processing", "completed", stored)

        assert result is False  # false-negative: move DID happen
        assert client.redis.llen("processing") == 0
        assert client.redis.lrange("completed", 0, -1) == [b"hello"]

    def test_remove_message_returns_false_after_ambiguous_success(self):
        """remove_message (no lease): LREM finds nothing → retry returns False.
        State: message correctly removed."""
        client = AmbiguousLremSyncClient()
        stored = encode_stored_message("hello")
        client.redis.lpush("processing", stored)
        gateway = RedisGateway(redis_client=client, retry_strategy=_retry_once_on_connection_error)

        result = gateway.remove_message("processing", stored)

        assert result is False  # false-negative: remove DID happen
        assert client.redis.llen("processing") == 0

    def test_remove_message_with_lease_returns_false_after_ambiguous_success(self):
        """remove_message (with lease): lease metadata already cleaned → retry returns False."""
        client = AmbiguousEvalSyncClient()
        stored = encode_stored_message("hello")
        client.redis.lpush("processing", stored)
        client.redis.zadd("processing:lease_deadlines", {stored: 999999999999})
        client.redis.hset("processing:lease_tokens", stored, "1")
        gateway = RedisGateway(redis_client=client, retry_strategy=_retry_once_on_connection_error)

        result = gateway.remove_message("processing", stored, lease_token="1")

        assert result is False  # false-negative: remove DID happen
        assert client.redis.llen("processing") == 0
        assert client.redis.zcard("processing:lease_deadlines") == 0
        assert client.redis.hlen("processing:lease_tokens") == 0

    def test_move_message_with_lease_returns_false_after_ambiguous_success(self):
        """move_message (with lease): lease metadata already cleaned → retry returns False."""
        client = AmbiguousEvalSyncClient()
        stored = encode_stored_message("hello")
        client.redis.lpush("processing", stored)
        client.redis.zadd("processing:lease_deadlines", {stored: 999999999999})
        client.redis.hset("processing:lease_tokens", stored, "1")
        gateway = RedisGateway(redis_client=client, retry_strategy=_retry_once_on_connection_error)

        result = gateway.move_message("processing", "completed", stored, lease_token="1")

        assert result is False  # false-negative: move DID happen
        assert client.redis.llen("processing") == 0
        assert client.redis.lrange("completed", 0, -1) == [b"hello"]
        assert client.redis.zcard("processing:lease_deadlines") == 0
        assert client.redis.hlen("processing:lease_tokens") == 0


class TestAsyncAmbiguousSuccessReturnValues:
    """Async mirrors of the false-negative return value tests."""

    @pytest.mark.asyncio
    async def test_publish_message_returns_false_after_ambiguous_success(self):
        client = AmbiguousEvalAsyncClient()
        gateway = AsyncRedisGateway(redis_client=client, retry_strategy=_async_retry_once_on_connection_error)

        result = await gateway.publish_message("pending", "hello", "dedup:hello")

        assert result is False
        assert await client.redis.llen("pending") == 1
        assert await client.redis.exists("dedup:hello")

    @pytest.mark.asyncio
    async def test_move_message_returns_false_after_ambiguous_success(self):
        client = AmbiguousEvalAsyncClient()
        stored = encode_stored_message("hello")
        await client.redis.lpush("processing", stored)
        gateway = AsyncRedisGateway(redis_client=client, retry_strategy=_async_retry_once_on_connection_error)

        result = await gateway.move_message("processing", "completed", stored)

        assert result is False
        assert await client.redis.llen("processing") == 0
        assert await client.redis.lrange("completed", 0, -1) == [b"hello"]

    @pytest.mark.asyncio
    async def test_remove_message_returns_false_after_ambiguous_success(self):
        client = AmbiguousLremAsyncClient()
        stored = encode_stored_message("hello")
        await client.redis.lpush("processing", stored)
        gateway = AsyncRedisGateway(redis_client=client, retry_strategy=_async_retry_once_on_connection_error)

        result = await gateway.remove_message("processing", stored)

        assert result is False
        assert await client.redis.llen("processing") == 0

    @pytest.mark.asyncio
    async def test_remove_message_with_lease_returns_false_after_ambiguous_success(self):
        client = AmbiguousEvalAsyncClient()
        stored = encode_stored_message("hello")
        await client.redis.lpush("processing", stored)
        await client.redis.zadd("processing:lease_deadlines", {stored: 999999999999})
        await client.redis.hset("processing:lease_tokens", stored, "1")
        gateway = AsyncRedisGateway(redis_client=client, retry_strategy=_async_retry_once_on_connection_error)

        result = await gateway.remove_message("processing", stored, lease_token="1")

        assert result is False
        assert await client.redis.llen("processing") == 0
        assert await client.redis.zcard("processing:lease_deadlines") == 0
        assert await client.redis.hlen("processing:lease_tokens") == 0

    @pytest.mark.asyncio
    async def test_move_message_with_lease_returns_false_after_ambiguous_success(self):
        client = AmbiguousEvalAsyncClient()
        stored = encode_stored_message("hello")
        await client.redis.lpush("processing", stored)
        await client.redis.zadd("processing:lease_deadlines", {stored: 999999999999})
        await client.redis.hset("processing:lease_tokens", stored, "1")
        gateway = AsyncRedisGateway(redis_client=client, retry_strategy=_async_retry_once_on_connection_error)

        result = await gateway.move_message("processing", "completed", stored, lease_token="1")

        assert result is False
        assert await client.redis.llen("processing") == 0
        assert await client.redis.lrange("completed", 0, -1) == [b"hello"]
        assert await client.redis.zcard("processing:lease_deadlines") == 0
        assert await client.redis.hlen("processing:lease_tokens") == 0


# ===========================================================================
# 2. Heartbeat renewal — ambiguous success with subsequent reclaim
#
# Scenario: renew_message_lease succeeds server-side (deadline extended),
# connection drops, another consumer reclaims the message (new lease token),
# then the retry runs.  The retry must return False (safe direction) because
# the token no longer matches.
# ===========================================================================


class TestSyncRenewLeaseAmbiguousWithReclaim:
    def test_renew_returns_false_when_reclaimed_between_attempts(self):
        """After ambiguous renewal success, if another consumer reclaims
        the message (new lease token), the retry correctly returns False.
        The heartbeat will stop — this is the safe direction."""
        client = ReclaimBetweenRenewalsSyncClient()
        stored = encode_stored_message("hello")
        client.redis.zadd("processing:lease_deadlines", {stored: 1000})
        client.redis.hset("processing:lease_tokens", stored, "1")
        gateway = RedisGateway(
            redis_client=client,
            retry_strategy=_retry_once_on_connection_error,
            message_visibility_timeout_seconds=30,
        )

        result = gateway.renew_message_lease("processing", stored, "1")

        # The retry sees "reclaimed-by-other" as the token → returns False
        assert result is False
        assert client.eval_calls == 2
        # The deadline WAS extended by the first (successful) attempt
        deadline = client.redis.zscore("processing:lease_deadlines", stored)
        assert deadline > 1000
        # But the token now belongs to the other consumer
        assert client.redis.hget("processing:lease_tokens", stored) == b"reclaimed-by-other"


class TestAsyncRenewLeaseAmbiguousWithReclaim:
    @pytest.mark.asyncio
    async def test_renew_returns_false_when_reclaimed_between_attempts(self):
        client = ReclaimBetweenRenewalsAsyncClient()
        stored = encode_stored_message("hello")
        await client.redis.zadd("processing:lease_deadlines", {stored: 1000})
        await client.redis.hset("processing:lease_tokens", stored, "1")
        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_strategy=_async_retry_once_on_connection_error,
            message_visibility_timeout_seconds=30,
        )

        result = await gateway.renew_message_lease("processing", stored, "1")

        assert result is False
        assert client.eval_calls == 2
        deadline = await client.redis.zscore("processing:lease_deadlines", stored)
        assert deadline > 1000
        assert await client.redis.hget("processing:lease_tokens", stored) == b"reclaimed-by-other"


# ===========================================================================
# 3. Visibility-timeout claim — ambiguous success strands message but it
#    is recoverable via the reclaim mechanism.
# ===========================================================================


class TestSyncClaimAmbiguousSuccessRecovery:
    def test_stranded_claim_is_reclaimable_after_timeout(self):
        """When _claim_visible_message succeeds server-side but the response
        is lost, the message is stranded in processing with a lease.  After
        the visibility timeout expires, the next claim poll reclaims it.

        This proves the system self-heals from ambiguous claim failures."""
        client = AmbiguousEvalSyncClient()
        stored = encode_stored_message("hello")
        client.redis.lpush("pending", stored)
        gateway = RedisGateway(
            redis_client=client,
            retry_strategy=_retry_once_on_connection_error,
            message_visibility_timeout_seconds=30,
            message_wait_interval_seconds=0,
        )

        # First attempt: claim succeeds server-side, ConnectionError on response.
        # _claim_visible_message is NOT retried (correctly) — it raises.
        with pytest.raises(redis.exceptions.ConnectionError, match="after EVAL"):
            gateway.wait_for_message_and_move("pending", "processing")

        # Message is stranded in processing with a lease
        assert client.redis.llen("pending") == 0
        assert client.redis.llen("processing") == 1

        # Simulate the visibility timeout expiring by setting the lease
        # deadline to 0 (well in the past).
        stranded = client.redis.lindex("processing", 0)
        client.redis.zadd("processing:lease_deadlines", {stranded: 0})

        # Second attempt: the claim script's reclaim logic finds the expired
        # message, requeues it to pending, then claims it fresh.
        # Reset eval_calls so the client doesn't raise again.
        client.eval_calls = 99
        result = gateway.wait_for_message_and_move("pending", "processing")

        assert result is not None
        assert result.lease_token  # has a fresh lease


class TestAsyncClaimAmbiguousSuccessRecovery:
    @pytest.mark.asyncio
    async def test_stranded_claim_is_reclaimable_after_timeout(self):
        client = AmbiguousEvalAsyncClient()
        stored = encode_stored_message("hello")
        await client.redis.lpush("pending", stored)
        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_strategy=_async_retry_once_on_connection_error,
            message_visibility_timeout_seconds=30,
            message_wait_interval_seconds=0,
        )

        with pytest.raises(redis.exceptions.ConnectionError, match="after EVAL"):
            await gateway.wait_for_message_and_move("pending", "processing")

        assert await client.redis.llen("pending") == 0
        assert await client.redis.llen("processing") == 1

        # Simulate the visibility timeout expiring
        stranded = await client.redis.lindex("processing", 0)
        await client.redis.zadd("processing:lease_deadlines", {stranded: 0})

        client.eval_calls = 99
        result = await gateway.wait_for_message_and_move("pending", "processing")

        assert result is not None
        assert result.lease_token
