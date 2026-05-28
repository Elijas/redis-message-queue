import fakeredis
import pytest
import redis.exceptions

from redis_message_queue._redis_gateway import RedisGateway
from redis_message_queue._stored_message import encode_stored_message
from redis_message_queue.asyncio._redis_gateway import RedisGateway as AsyncRedisGateway


def _delivery_count(value):
    return 0 if value is None else int(value)


def _poison_branch_command_positions(script):
    branch_start = script.index("if max_delivery_count > 0 and count > max_delivery_count then")
    return {
        "lpush": script.index("redis.call('LPUSH', KEYS[7], dead_letter_value)", branch_start),
        "lrem": script.index("redis.call('LREM', KEYS[2], 1, stored)", branch_start),
        "hdel": script.index("redis.call('HDEL', KEYS[6], stored)", branch_start),
    }


class _DlqPushFailureSyncClient:
    def __init__(self):
        self.redis = fakeredis.FakeRedis()
        self.eval_calls = 0

    def eval(self, script, numkeys, *args):
        self.eval_calls += 1
        if self.eval_calls == 2:
            return self._simulate_dlq_lpush_failure(script, numkeys, *args)
        return self.redis.eval(script, numkeys, *args)

    def _simulate_dlq_lpush_failure(self, script, numkeys, *args):
        assert numkeys == 11
        pending = args[0]
        processing = args[1]
        lease_deadlines = args[2]
        lease_tokens = args[3]
        delivery_counts = args[5]
        claim_result_refs = args[8]
        claim_result_ids = args[9]
        claim_result_backrefs = args[10]

        expired = self.redis.zrangebyscore(lease_deadlines, "-inf", "+inf")
        for stored in expired:
            expired_lease_token = self.redis.hget(lease_tokens, stored)
            self.redis.zrem(lease_deadlines, stored)
            self.redis.hdel(lease_tokens, stored)
            if expired_lease_token is not None:
                claim_result_key = self.redis.hget(claim_result_refs, expired_lease_token)
                if claim_result_key is not None:
                    self.redis.delete(claim_result_key)
                    self.redis.hdel(claim_result_refs, expired_lease_token)
                claim_id = self.redis.hget(claim_result_backrefs, expired_lease_token)
                if claim_id is not None:
                    self.redis.hdel(claim_result_ids, claim_id)
                    self.redis.hdel(claim_result_backrefs, expired_lease_token)
            if self.redis.lrem(processing, 1, stored) == 1:
                self.redis.rpush(pending, stored)

        stored = self.redis.lmove(pending, processing, "RIGHT", "LEFT")
        assert stored is not None
        count = self.redis.hincrby(delivery_counts, stored, 1)
        assert count == 2

        positions = _poison_branch_command_positions(script)
        if positions["lrem"] < positions["lpush"]:
            self.redis.lrem(processing, 1, stored)
        if positions["hdel"] < positions["lpush"]:
            self.redis.hdel(delivery_counts, stored)
        raise redis.exceptions.ResponseError("OOM simulated during DLQ LPUSH")

    def __getattr__(self, name):
        return getattr(self.redis, name)


class _DlqPushFailureAsyncClient:
    def __init__(self):
        self.redis = fakeredis.FakeAsyncRedis()
        self.eval_calls = 0

    async def eval(self, script, numkeys, *args):
        self.eval_calls += 1
        if self.eval_calls == 2:
            return await self._simulate_dlq_lpush_failure(script, numkeys, *args)
        return await self.redis.eval(script, numkeys, *args)

    async def _simulate_dlq_lpush_failure(self, script, numkeys, *args):
        assert numkeys == 11
        pending = args[0]
        processing = args[1]
        lease_deadlines = args[2]
        lease_tokens = args[3]
        delivery_counts = args[5]
        claim_result_refs = args[8]
        claim_result_ids = args[9]
        claim_result_backrefs = args[10]

        expired = await self.redis.zrangebyscore(lease_deadlines, "-inf", "+inf")
        for stored in expired:
            expired_lease_token = await self.redis.hget(lease_tokens, stored)
            await self.redis.zrem(lease_deadlines, stored)
            await self.redis.hdel(lease_tokens, stored)
            if expired_lease_token is not None:
                claim_result_key = await self.redis.hget(claim_result_refs, expired_lease_token)
                if claim_result_key is not None:
                    await self.redis.delete(claim_result_key)
                    await self.redis.hdel(claim_result_refs, expired_lease_token)
                claim_id = await self.redis.hget(claim_result_backrefs, expired_lease_token)
                if claim_id is not None:
                    await self.redis.hdel(claim_result_ids, claim_id)
                    await self.redis.hdel(claim_result_backrefs, expired_lease_token)
            if await self.redis.lrem(processing, 1, stored) == 1:
                await self.redis.rpush(pending, stored)

        stored = await self.redis.lmove(pending, processing, "RIGHT", "LEFT")
        assert stored is not None
        count = await self.redis.hincrby(delivery_counts, stored, 1)
        assert count == 2

        positions = _poison_branch_command_positions(script)
        if positions["lrem"] < positions["lpush"]:
            await self.redis.lrem(processing, 1, stored)
        if positions["hdel"] < positions["lpush"]:
            await self.redis.hdel(delivery_counts, stored)
        raise redis.exceptions.ResponseError("OOM simulated during DLQ LPUSH")

    def __getattr__(self, name):
        return getattr(self.redis, name)


def _sync_gateway(client):
    return RedisGateway(
        redis_client=client,
        retry_budget_seconds=0,
        message_wait_interval_seconds=0,
        message_visibility_timeout_seconds=30,
        max_delivery_count=1,
        dead_letter_queue="dead",
    )


def _async_gateway(client):
    return AsyncRedisGateway(
        redis_client=client,
        retry_budget_seconds=0,
        message_wait_interval_seconds=0,
        message_visibility_timeout_seconds=30,
        max_delivery_count=1,
        dead_letter_queue="dead",
    )


def test_sync_dlq_push_failure_preserves_poison_message_and_delivery_count():
    client = _DlqPushFailureSyncClient()
    gateway = _sync_gateway(client)
    stored = encode_stored_message("poison")
    client.redis.lpush("pending", stored)

    first_claim = gateway.wait_for_message_and_move("pending", "processing")
    assert first_claim is not None
    client.redis.zadd("processing:lease_deadlines", {first_claim.stored_message: 0})

    with pytest.raises(redis.exceptions.ResponseError, match="OOM simulated during DLQ LPUSH"):
        gateway.wait_for_message_and_move("pending", "processing")

    assert client.redis.llen("pending") == 0
    assert client.redis.lrange("processing", 0, -1) == [first_claim.stored_message]
    assert client.redis.llen("dead") == 0
    assert _delivery_count(client.redis.hget("processing:delivery_counts", first_claim.stored_message)) == 2

    assert gateway.wait_for_message_and_move("pending", "processing") is None
    assert client.redis.lrange("processing", 0, -1) == [first_claim.stored_message]
    assert _delivery_count(client.redis.hget("processing:delivery_counts", first_claim.stored_message)) == 2


@pytest.mark.asyncio
async def test_async_dlq_push_failure_preserves_poison_message_and_delivery_count():
    client = _DlqPushFailureAsyncClient()
    gateway = _async_gateway(client)
    stored = encode_stored_message("poison")
    await client.redis.lpush("pending", stored)

    first_claim = await gateway.wait_for_message_and_move("pending", "processing")
    assert first_claim is not None
    await client.redis.zadd("processing:lease_deadlines", {first_claim.stored_message: 0})

    with pytest.raises(redis.exceptions.ResponseError, match="OOM simulated during DLQ LPUSH"):
        await gateway.wait_for_message_and_move("pending", "processing")

    assert await client.redis.llen("pending") == 0
    assert await client.redis.lrange("processing", 0, -1) == [first_claim.stored_message]
    assert await client.redis.llen("dead") == 0
    count = await client.redis.hget("processing:delivery_counts", first_claim.stored_message)
    assert _delivery_count(count) == 2

    assert await gateway.wait_for_message_and_move("pending", "processing") is None
    assert await client.redis.lrange("processing", 0, -1) == [first_claim.stored_message]
    count = await client.redis.hget("processing:delivery_counts", first_claim.stored_message)
    assert _delivery_count(count) == 2
