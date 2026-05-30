import json

import fakeredis
import pytest
import redis.exceptions

from redis_message_queue import ClaimStoreFailedError
from redis_message_queue._config import (
    CLAIM_MESSAGE_WITH_VISIBILITY_TIMEOUT_LUA_SCRIPT,
    CLAIM_STORE_FAILED_LUA_SENTINEL,
)
from redis_message_queue._redis_gateway import RedisGateway
from redis_message_queue._stored_message import encode_stored_message, extract_stored_message_id
from redis_message_queue.asyncio._redis_gateway import RedisGateway as AsyncRedisGateway

pytestmark = pytest.mark.integration


def _keys(queue_name):
    return f"{queue_name}:pending", f"{queue_name}:processing", f"{queue_name}:dlq"


def _stored_bytes(stored):
    return stored.encode("utf-8")


def _delivery_count(value):
    return 0 if value is None else int(value)


def _position_after(script, needle, start):
    try:
        return script.index(needle, start)
    except ValueError:
        return None


def _claim_compensation_command_positions(script):
    branch_start = script.index("if not ok then")
    return {
        "rollback": _position_after(script, "redis.call('HINCRBY', KEYS[6], stored, -1)", branch_start),
        "rpush": script.index("redis.pcall('RPUSH', KEYS[1], stored)", branch_start),
        "lrem": _position_after(script, "redis.call('LREM', KEYS[2], 1, stored)", branch_start),
        "zrem": _position_after(script, "redis.call('ZREM', KEYS[3], stored)", branch_start),
        "hdel_token": _position_after(script, "redis.call('HDEL', KEYS[4], stored)", branch_start),
        "del_claim": _position_after(script, "redis.call('DEL', KEYS[8])", branch_start),
        "hdel_claim_id": _position_after(script, "redis.call('HDEL', KEYS[10], ARGV[4])", branch_start),
        "hdel_ref": _position_after(script, "redis.call('HDEL', KEYS[9], lease_token)", branch_start),
        "hdel_backref": _position_after(script, "redis.call('HDEL', KEYS[11], lease_token)", branch_start),
    }


def _expiry_reclaim_command_positions(script):
    branch_start = script.index("local expired = redis.call('ZRANGEBYSCORE', KEYS[3]")
    branch_end = script.index("local dead_lettered_events = {}", branch_start)
    return {
        "rpush": script.index("redis.call('RPUSH', KEYS[1], stored)", branch_start, branch_end),
        "processing_lrem": script.index("redis.call('LREM', KEYS[2], 1, stored)", branch_start, branch_end),
        "zrem": script.index("redis.call('ZREM', KEYS[3], stored)", branch_start, branch_end),
        "hdel_token": script.index("redis.call('HDEL', KEYS[4], stored)", branch_start, branch_end),
        "pending_lrem": script.index("redis.call('LREM', KEYS[1], 1, stored)", branch_start, branch_end),
    }


class _ClaimCompensationSyncClient:
    def __init__(self, *, fail_return_to_pending):
        self.redis = fakeredis.FakeRedis()
        self.fail_return_to_pending = fail_return_to_pending

    def eval(self, script, numkeys, *args):
        return self._simulate_claim_store_failure(script, numkeys, *args)

    def _simulate_claim_store_failure(self, script, numkeys, *args):
        assert numkeys == 11
        pending = args[0]
        processing = args[1]
        lease_deadlines = args[2]
        lease_tokens = args[3]
        counter = args[4]
        delivery_counts = args[5]
        claim_result_key = args[7]
        claim_result_refs = args[8]
        claim_result_ids = args[9]
        claim_result_backrefs = args[10]
        claim_id = args[numkeys + 3]

        stored = self.redis.lmove(pending, processing, "RIGHT", "LEFT")
        assert stored is not None
        self.redis.hincrby(delivery_counts, stored, 1)
        self.redis.incr(counter)
        lease_token = self.redis.get(counter)
        assert lease_token is not None
        claim_payload = json.dumps([stored.decode("utf-8"), lease_token.decode("utf-8")])
        self.redis.zadd(lease_deadlines, {stored: 9_999_999_999_999})
        self.redis.hset(lease_tokens, stored, lease_token)
        self.redis.set(claim_result_key, claim_payload, px=30_000)
        self.redis.hset(claim_result_refs, lease_token, claim_result_key)
        self.redis.hset(claim_result_ids, claim_id, claim_payload)
        self.redis.hset(claim_result_backrefs, lease_token, claim_id)

        self._simulate_compensation_commands_before_rpush(script, stored, args, lease_token)
        if self.fail_return_to_pending:
            return [
                CLAIM_STORE_FAILED_LUA_SENTINEL,
                "write failed; return-to-pending failed: OOM simulated during compensating RPUSH",
                stored,
            ]
        self.redis.rpush(pending, stored)
        self._simulate_compensation_commands_after_rpush(script, stored, args, lease_token)
        return [CLAIM_STORE_FAILED_LUA_SENTINEL, "write failed after partial metadata", stored]

    def _simulate_compensation_commands_before_rpush(self, script, stored, args, lease_token):
        self._simulate_compensation_commands(script, stored, args, lease_token, before_rpush=True)

    def _simulate_compensation_commands_after_rpush(self, script, stored, args, lease_token):
        self._simulate_compensation_commands(script, stored, args, lease_token, before_rpush=False)

    def _simulate_compensation_commands(self, script, stored, args, lease_token, *, before_rpush):
        positions = _claim_compensation_command_positions(script)
        rpush_pos = positions["rpush"]
        commands = {
            "rollback": lambda: self.redis.hincrby(args[5], stored, -1),
            "lrem": lambda: self.redis.lrem(args[1], 1, stored),
            "zrem": lambda: self.redis.zrem(args[2], stored),
            "hdel_token": lambda: self.redis.hdel(args[3], stored),
            "del_claim": lambda: self.redis.delete(args[7]),
            "hdel_claim_id": lambda: self.redis.hdel(args[9], args[14]),
            "hdel_ref": lambda: self.redis.hdel(args[8], lease_token),
            "hdel_backref": lambda: self.redis.hdel(args[10], lease_token),
        }
        for name, command in commands.items():
            position = positions[name]
            if position is None:
                continue
            if before_rpush == (position < rpush_pos):
                command()

    def __getattr__(self, name):
        return getattr(self.redis, name)


class _ExpiryReclaimRpushFailureSyncClient:
    def __init__(self):
        self.redis = fakeredis.FakeRedis()

    def eval(self, script, numkeys, *args):
        if script == CLAIM_MESSAGE_WITH_VISIBILITY_TIMEOUT_LUA_SCRIPT:
            assert numkeys == 11
            lease_deadlines = args[2]
            expired = self.redis.zrangebyscore(lease_deadlines, "-inf", "+inf")
            if expired:
                positions = _expiry_reclaim_command_positions(script)
                assert positions["rpush"] < positions["processing_lrem"]
                assert positions["rpush"] < positions["zrem"]
                assert positions["rpush"] < positions["hdel_token"]
                raise redis.exceptions.ResponseError("OOM simulated during expiry reclaim RPUSH")
        return self.redis.eval(script, numkeys, *args)

    def __getattr__(self, name):
        return getattr(self.redis, name)


class _ClaimCompensationAsyncClient:
    def __init__(self, *, fail_return_to_pending):
        self.redis = fakeredis.FakeAsyncRedis()
        self.fail_return_to_pending = fail_return_to_pending

    async def eval(self, script, numkeys, *args):
        return await self._simulate_claim_store_failure(script, numkeys, *args)

    async def _simulate_claim_store_failure(self, script, numkeys, *args):
        assert numkeys == 11
        pending = args[0]
        processing = args[1]
        lease_deadlines = args[2]
        lease_tokens = args[3]
        counter = args[4]
        delivery_counts = args[5]
        claim_result_key = args[7]
        claim_result_refs = args[8]
        claim_result_ids = args[9]
        claim_result_backrefs = args[10]
        claim_id = args[numkeys + 3]

        stored = await self.redis.lmove(pending, processing, "RIGHT", "LEFT")
        assert stored is not None
        await self.redis.hincrby(delivery_counts, stored, 1)
        await self.redis.incr(counter)
        lease_token = await self.redis.get(counter)
        assert lease_token is not None
        claim_payload = json.dumps([stored.decode("utf-8"), lease_token.decode("utf-8")])
        await self.redis.zadd(lease_deadlines, {stored: 9_999_999_999_999})
        await self.redis.hset(lease_tokens, stored, lease_token)
        await self.redis.set(claim_result_key, claim_payload, px=30_000)
        await self.redis.hset(claim_result_refs, lease_token, claim_result_key)
        await self.redis.hset(claim_result_ids, claim_id, claim_payload)
        await self.redis.hset(claim_result_backrefs, lease_token, claim_id)

        await self._simulate_compensation_commands_before_rpush(script, stored, args, lease_token)
        if self.fail_return_to_pending:
            return [
                CLAIM_STORE_FAILED_LUA_SENTINEL,
                "write failed; return-to-pending failed: OOM simulated during compensating RPUSH",
                stored,
            ]
        await self.redis.rpush(pending, stored)
        await self._simulate_compensation_commands_after_rpush(script, stored, args, lease_token)
        return [CLAIM_STORE_FAILED_LUA_SENTINEL, "write failed after partial metadata", stored]

    async def _simulate_compensation_commands_before_rpush(self, script, stored, args, lease_token):
        await self._simulate_compensation_commands(script, stored, args, lease_token, before_rpush=True)

    async def _simulate_compensation_commands_after_rpush(self, script, stored, args, lease_token):
        await self._simulate_compensation_commands(script, stored, args, lease_token, before_rpush=False)

    async def _simulate_compensation_commands(self, script, stored, args, lease_token, *, before_rpush):
        positions = _claim_compensation_command_positions(script)
        rpush_pos = positions["rpush"]
        commands = {
            "rollback": lambda: self.redis.hincrby(args[5], stored, -1),
            "lrem": lambda: self.redis.lrem(args[1], 1, stored),
            "zrem": lambda: self.redis.zrem(args[2], stored),
            "hdel_token": lambda: self.redis.hdel(args[3], stored),
            "del_claim": lambda: self.redis.delete(args[7]),
            "hdel_claim_id": lambda: self.redis.hdel(args[9], args[14]),
            "hdel_ref": lambda: self.redis.hdel(args[8], lease_token),
            "hdel_backref": lambda: self.redis.hdel(args[10], lease_token),
        }
        for name, command in commands.items():
            position = positions[name]
            if position is None:
                continue
            if before_rpush == (position < rpush_pos):
                await command()

    def __getattr__(self, name):
        return getattr(self.redis, name)


def _sync_gateway(client, dlq):
    return RedisGateway(
        redis_client=client,
        retry_budget_seconds=0,
        message_wait_interval_seconds=0,
        message_visibility_timeout_seconds=30,
        max_delivery_count=2,
        dead_letter_queue=dlq,
    )


def _async_gateway(client, dlq):
    return AsyncRedisGateway(
        redis_client=client,
        retry_budget_seconds=0,
        message_wait_interval_seconds=0,
        message_visibility_timeout_seconds=30,
        max_delivery_count=2,
        dead_letter_queue=dlq,
    )


def test_sync_claim_store_compensation_rpush_failure_preserves_processing_payload():
    client = _ClaimCompensationSyncClient(fail_return_to_pending=True)
    pending, processing, dlq = "pending", "processing", "dead"
    gateway = _sync_gateway(client, dlq)
    stored = encode_stored_message("hello")
    client.redis.lpush(pending, stored)

    with pytest.raises(ClaimStoreFailedError, match="payload preservation.*return-to-pending failed"):
        gateway.wait_for_message_and_move(pending, processing)

    assert client.redis.llen(pending) == 0
    assert client.redis.lrange(processing, 0, -1) == [_stored_bytes(stored)]
    assert client.redis.llen(dlq) == 0
    assert not (client.redis.llen(pending) == 0 and client.redis.llen(processing) == 0 and client.redis.llen(dlq) == 0)
    assert _delivery_count(client.redis.hget(gateway._delivery_counts_key(processing), stored)) == 0
    assert client.redis.zcard(gateway._lease_deadlines_key(processing)) == 1
    assert client.redis.hlen(gateway._lease_tokens_key(processing)) == 1


def test_sync_claim_store_compensation_success_cleans_partial_metadata():
    client = _ClaimCompensationSyncClient(fail_return_to_pending=False)
    pending, processing, dlq = "pending", "processing", "dead"
    gateway = _sync_gateway(client, dlq)
    stored = encode_stored_message("hello")
    client.redis.lpush(pending, stored)

    with pytest.raises(ClaimStoreFailedError, match="payload preservation"):
        gateway.wait_for_message_and_move(pending, processing)

    assert client.redis.lrange(pending, 0, -1) == [_stored_bytes(stored)]
    assert client.redis.llen(processing) == 0
    assert client.redis.llen(dlq) == 0
    assert _delivery_count(client.redis.hget(gateway._delivery_counts_key(processing), stored)) == 0
    assert client.redis.zcard(gateway._lease_deadlines_key(processing)) == 0
    assert client.redis.hlen(gateway._lease_tokens_key(processing)) == 0
    assert client.redis.hlen(gateway._claim_result_refs_key(processing)) == 0
    assert client.redis.hlen(gateway._claim_result_ids_key(processing)) == 0
    assert client.redis.hlen(gateway._claim_result_backrefs_key(processing)) == 0
    assert client.redis.keys(f"{processing}:claim_result:*") == []


def test_expiry_reclaim_requeues_before_destructive_cleanup():
    positions = _expiry_reclaim_command_positions(CLAIM_MESSAGE_WITH_VISIBILITY_TIMEOUT_LUA_SCRIPT)

    assert positions["rpush"] < positions["processing_lrem"]
    assert positions["rpush"] < positions["zrem"]
    assert positions["rpush"] < positions["hdel_token"]
    assert positions["processing_lrem"] < positions["pending_lrem"]


def test_sync_expiry_reclaim_rpush_failure_preserves_processing_payload_and_lease_metadata():
    client = _ExpiryReclaimRpushFailureSyncClient()
    pending, processing, dlq = "pending", "processing", "dead"
    gateway = _sync_gateway(client, dlq)
    stored = encode_stored_message("reclaim-me")
    stored_bytes = _stored_bytes(stored)
    client.redis.lpush(pending, stored)

    claimed = gateway.wait_for_message_and_move(pending, processing)
    assert claimed is not None
    assert claimed.stored_message == stored_bytes
    client.redis.zadd(gateway._lease_deadlines_key(processing), {stored_bytes: 0})

    with pytest.raises(redis.exceptions.ResponseError, match="OOM simulated during expiry reclaim RPUSH"):
        gateway.wait_for_message_and_move(pending, processing)

    assert client.redis.llen(pending) == 0
    assert client.redis.lrange(processing, 0, -1) == [stored_bytes]
    assert client.redis.zcard(gateway._lease_deadlines_key(processing)) == 1
    assert client.redis.hget(gateway._lease_tokens_key(processing), stored_bytes).decode("utf-8") == claimed.lease_token
    assert _delivery_count(client.redis.hget(gateway._delivery_counts_key(processing), stored)) == 1
    assert client.redis.hlen(gateway._claim_result_refs_key(processing)) == 1
    assert client.redis.hlen(gateway._claim_result_ids_key(processing)) == 1
    assert client.redis.hlen(gateway._claim_result_backrefs_key(processing)) == 1


@pytest.mark.asyncio
async def test_async_claim_store_compensation_rpush_failure_preserves_processing_payload():
    client = _ClaimCompensationAsyncClient(fail_return_to_pending=True)
    pending, processing, dlq = "pending", "processing", "dead"
    gateway = _async_gateway(client, dlq)
    stored = encode_stored_message("hello")
    await client.redis.lpush(pending, stored)

    with pytest.raises(ClaimStoreFailedError, match="payload preservation.*return-to-pending failed"):
        await gateway.wait_for_message_and_move(pending, processing)

    assert await client.redis.llen(pending) == 0
    assert await client.redis.lrange(processing, 0, -1) == [_stored_bytes(stored)]
    assert await client.redis.llen(dlq) == 0
    assert not (
        await client.redis.llen(pending) == 0
        and await client.redis.llen(processing) == 0
        and await client.redis.llen(dlq) == 0
    )
    count = await client.redis.hget(gateway._delivery_counts_key(processing), stored)
    assert _delivery_count(count) == 0
    assert await client.redis.zcard(gateway._lease_deadlines_key(processing)) == 1
    assert await client.redis.hlen(gateway._lease_tokens_key(processing)) == 1


@pytest.mark.asyncio
async def test_async_claim_store_compensation_success_cleans_partial_metadata():
    client = _ClaimCompensationAsyncClient(fail_return_to_pending=False)
    pending, processing, dlq = "pending", "processing", "dead"
    gateway = _async_gateway(client, dlq)
    stored = encode_stored_message("hello")
    await client.redis.lpush(pending, stored)

    with pytest.raises(ClaimStoreFailedError, match="payload preservation"):
        await gateway.wait_for_message_and_move(pending, processing)

    assert await client.redis.lrange(pending, 0, -1) == [_stored_bytes(stored)]
    assert await client.redis.llen(processing) == 0
    assert await client.redis.llen(dlq) == 0
    count = await client.redis.hget(gateway._delivery_counts_key(processing), stored)
    assert _delivery_count(count) == 0
    assert await client.redis.zcard(gateway._lease_deadlines_key(processing)) == 0
    assert await client.redis.hlen(gateway._lease_tokens_key(processing)) == 0
    assert await client.redis.hlen(gateway._claim_result_refs_key(processing)) == 0
    assert await client.redis.hlen(gateway._claim_result_ids_key(processing)) == 0
    assert await client.redis.hlen(gateway._claim_result_backrefs_key(processing)) == 0
    assert await client.redis.keys(f"{processing}:claim_result:*") == []


def test_sync_claim_success_preserves_delivery_count(real_redis_client, queue_name):
    pending, processing, dlq = _keys(queue_name)
    gateway = _sync_gateway(real_redis_client, dlq)
    stored = encode_stored_message("hello")
    real_redis_client.lpush(pending, stored)

    claimed = gateway.wait_for_message_and_move(pending, processing)

    assert claimed is not None
    assert claimed.stored_message == _stored_bytes(stored)
    assert claimed.lease_token
    assert _delivery_count(real_redis_client.hget(gateway._delivery_counts_key(processing), stored)) == 1


def test_sync_claim_store_failure_rolls_back_and_raises(real_redis_client, queue_name):
    pending, processing, dlq = _keys(queue_name)
    gateway = _sync_gateway(real_redis_client, dlq)
    stored = encode_stored_message("hello")
    real_redis_client.lpush(pending, stored)
    real_redis_client.set(gateway._lease_token_counter_key(processing), "not-an-int")

    with pytest.raises(ClaimStoreFailedError) as caught:
        gateway.wait_for_message_and_move(pending, processing)

    assert caught.value.queue == pending
    assert caught.value.message_id == extract_stored_message_id(stored)
    assert caught.value.operation == "claim"
    assert "integer" in str(caught.value)
    assert _delivery_count(real_redis_client.hget(gateway._delivery_counts_key(processing), stored)) == 0
    assert real_redis_client.lrange(pending, 0, -1) == [_stored_bytes(stored)]
    assert real_redis_client.llen(processing) == 0
    assert real_redis_client.lrange(dlq, 0, -1) == []


def test_sync_repeated_store_failures_do_not_consume_delivery_attempts(real_redis_client, queue_name):
    pending, processing, dlq = _keys(queue_name)
    gateway = _sync_gateway(real_redis_client, dlq)
    stored = encode_stored_message("hello")
    real_redis_client.lpush(pending, stored)
    lease_counter = gateway._lease_token_counter_key(processing)
    real_redis_client.set(lease_counter, "not-an-int")

    for _ in range(3):
        with pytest.raises(ClaimStoreFailedError):
            gateway.wait_for_message_and_move(pending, processing)
        assert _delivery_count(real_redis_client.hget(gateway._delivery_counts_key(processing), stored)) == 0
        assert real_redis_client.llen(pending) == 1
        assert real_redis_client.llen(processing) == 0
        assert real_redis_client.lrange(dlq, 0, -1) == []

    real_redis_client.set(lease_counter, "0")

    claimed = gateway.wait_for_message_and_move(pending, processing)

    assert claimed is not None
    assert claimed.stored_message == _stored_bytes(stored)
    assert _delivery_count(real_redis_client.hget(gateway._delivery_counts_key(processing), stored)) == 1
    assert real_redis_client.llen(pending) == 0
    assert real_redis_client.llen(processing) == 1
    assert real_redis_client.lrange(dlq, 0, -1) == []


@pytest.mark.asyncio
async def test_async_claim_success_preserves_delivery_count(real_async_redis_client, queue_name):
    pending, processing, dlq = _keys(queue_name)
    gateway = _async_gateway(real_async_redis_client, dlq)
    stored = encode_stored_message("hello")
    await real_async_redis_client.lpush(pending, stored)

    claimed = await gateway.wait_for_message_and_move(pending, processing)

    assert claimed is not None
    assert claimed.stored_message == _stored_bytes(stored)
    assert claimed.lease_token
    count = await real_async_redis_client.hget(gateway._delivery_counts_key(processing), stored)
    assert _delivery_count(count) == 1


@pytest.mark.asyncio
async def test_async_claim_store_failure_rolls_back_and_raises(real_async_redis_client, queue_name):
    pending, processing, dlq = _keys(queue_name)
    gateway = _async_gateway(real_async_redis_client, dlq)
    stored = encode_stored_message("hello")
    await real_async_redis_client.lpush(pending, stored)
    await real_async_redis_client.set(gateway._lease_token_counter_key(processing), "not-an-int")

    with pytest.raises(ClaimStoreFailedError) as caught:
        await gateway.wait_for_message_and_move(pending, processing)

    assert caught.value.queue == pending
    assert caught.value.message_id == extract_stored_message_id(stored)
    assert caught.value.operation == "claim"
    assert "integer" in str(caught.value)
    count = await real_async_redis_client.hget(gateway._delivery_counts_key(processing), stored)
    assert _delivery_count(count) == 0
    assert await real_async_redis_client.lrange(pending, 0, -1) == [_stored_bytes(stored)]
    assert await real_async_redis_client.llen(processing) == 0
    assert await real_async_redis_client.lrange(dlq, 0, -1) == []


@pytest.mark.asyncio
async def test_async_repeated_store_failures_do_not_consume_delivery_attempts(real_async_redis_client, queue_name):
    pending, processing, dlq = _keys(queue_name)
    gateway = _async_gateway(real_async_redis_client, dlq)
    stored = encode_stored_message("hello")
    await real_async_redis_client.lpush(pending, stored)
    lease_counter = gateway._lease_token_counter_key(processing)
    await real_async_redis_client.set(lease_counter, "not-an-int")

    for _ in range(3):
        with pytest.raises(ClaimStoreFailedError):
            await gateway.wait_for_message_and_move(pending, processing)
        count = await real_async_redis_client.hget(gateway._delivery_counts_key(processing), stored)
        assert _delivery_count(count) == 0
        assert await real_async_redis_client.llen(pending) == 1
        assert await real_async_redis_client.llen(processing) == 0
        assert await real_async_redis_client.lrange(dlq, 0, -1) == []

    await real_async_redis_client.set(lease_counter, "0")

    claimed = await gateway.wait_for_message_and_move(pending, processing)

    assert claimed is not None
    assert claimed.stored_message == _stored_bytes(stored)
    count = await real_async_redis_client.hget(gateway._delivery_counts_key(processing), stored)
    assert _delivery_count(count) == 1
    assert await real_async_redis_client.llen(pending) == 0
    assert await real_async_redis_client.llen(processing) == 1
    assert await real_async_redis_client.lrange(dlq, 0, -1) == []
