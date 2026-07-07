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
from redis_message_queue.redis_message_queue import RedisMessageQueue

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


def _server_now_ms(client):
    seconds, micros = client.time()
    return seconds * 1000 + micros // 1000


async def _async_server_now_ms(client):
    seconds, micros = await client.time()
    return seconds * 1000 + micros // 1000


def _seed_vt_recovery_claim(*, client, gateway, processing, claim_id, lease_token, stored, lease_deadline_score):
    """Seed a cached visibility-timeout claim recoverable via the Python path."""
    claim_payload = json.dumps([stored, lease_token])
    client.rpush(processing, stored)
    client.set(gateway._claim_result_key(processing, claim_id), claim_payload, px=int(gateway._claim_result_ttl_ms()))
    client.hset(
        gateway._claim_result_refs_key(processing), lease_token, gateway._claim_result_key(processing, claim_id)
    )
    client.hset(gateway._lease_tokens_key(processing), stored, lease_token)
    client.zadd(gateway._lease_deadlines_key(processing), {stored: lease_deadline_score})


async def _async_seed_vt_recovery_claim(
    *, client, gateway, processing, claim_id, lease_token, stored, lease_deadline_score
):
    claim_payload = json.dumps([stored, lease_token])
    await client.rpush(processing, stored)
    await client.set(
        gateway._claim_result_key(processing, claim_id), claim_payload, px=int(gateway._claim_result_ttl_ms())
    )
    await client.hset(
        gateway._claim_result_refs_key(processing), lease_token, gateway._claim_result_key(processing, claim_id)
    )
    await client.hset(gateway._lease_tokens_key(processing), stored, lease_token)
    await client.zadd(gateway._lease_deadlines_key(processing), {stored: lease_deadline_score})


def test_sync_visibility_timeout_recovery_rearms_elapsed_lease(real_redis_client, queue_name):
    processing, dlq = f"{queue_name}:processing", f"{queue_name}:dlq"
    gateway = _sync_gateway(real_redis_client, dlq)
    stored = encode_stored_message("recover-me")
    claim_id = "vt-recover-claim"
    lease_token = "7"
    _seed_vt_recovery_claim(
        client=real_redis_client,
        gateway=gateway,
        processing=processing,
        claim_id=claim_id,
        lease_token=lease_token,
        stored=stored,
        lease_deadline_score=1000,  # far in the past: an elapsed lease
    )

    recovered = gateway._recover_pending_visibility_timeout_claim(processing, claim_id)

    assert recovered is not None
    assert recovered.stored_message == _stored_bytes(stored)
    assert recovered.lease_token == lease_token
    # The recovery must ZADD a fresh deadline (~now + visibility_timeout), like
    # the in-Lua replay paths; otherwise the message resumes on a dead lease.
    now_ms = _server_now_ms(real_redis_client)
    score_after = real_redis_client.zscore(gateway._lease_deadlines_key(processing), stored)
    assert score_after is not None
    assert score_after > now_ms
    assert score_after == pytest.approx(now_ms + 30_000, abs=5_000)
    # Cached claim metadata is consumed on a successful recovery.
    assert real_redis_client.get(gateway._claim_result_key(processing, claim_id)) is None
    assert real_redis_client.hget(gateway._claim_result_refs_key(processing), lease_token) is None


def test_sync_visibility_timeout_recovery_miss_when_lease_token_gone(real_redis_client, queue_name):
    processing, dlq = f"{queue_name}:processing", f"{queue_name}:dlq"
    gateway = _sync_gateway(real_redis_client, dlq)
    stored = encode_stored_message("already-reclaimed")
    claim_id = "vt-recover-miss"
    lease_token = "7"
    _seed_vt_recovery_claim(
        client=real_redis_client,
        gateway=gateway,
        processing=processing,
        claim_id=claim_id,
        lease_token=lease_token,
        stored=stored,
        lease_deadline_score=1000,
    )
    # Another worker already reclaimed the message under a fresh lease token, so
    # the token-gated renew reports the recovered token is gone.
    real_redis_client.hset(gateway._lease_tokens_key(processing), stored, "99")

    recovered = gateway._recover_pending_visibility_timeout_claim(processing, claim_id)

    assert recovered is None
    # Renew short-circuited without a ZADD, so the stale deadline is untouched.
    assert real_redis_client.zscore(gateway._lease_deadlines_key(processing), stored) == 1000
    assert real_redis_client.get(gateway._claim_result_key(processing, claim_id)) is None


@pytest.mark.asyncio
async def test_async_visibility_timeout_recovery_rearms_elapsed_lease(real_async_redis_client, queue_name):
    processing, dlq = f"{queue_name}:processing", f"{queue_name}:dlq"
    gateway = _async_gateway(real_async_redis_client, dlq)
    stored = encode_stored_message("recover-me")
    claim_id = "vt-recover-claim"
    lease_token = "7"
    await _async_seed_vt_recovery_claim(
        client=real_async_redis_client,
        gateway=gateway,
        processing=processing,
        claim_id=claim_id,
        lease_token=lease_token,
        stored=stored,
        lease_deadline_score=1000,
    )

    recovered = await gateway._recover_pending_visibility_timeout_claim(processing, claim_id)

    assert recovered is not None
    assert recovered.stored_message == _stored_bytes(stored)
    assert recovered.lease_token == lease_token
    now_ms = await _async_server_now_ms(real_async_redis_client)
    score_after = await real_async_redis_client.zscore(gateway._lease_deadlines_key(processing), stored)
    assert score_after is not None
    assert score_after > now_ms
    assert score_after == pytest.approx(now_ms + 30_000, abs=5_000)
    assert await real_async_redis_client.get(gateway._claim_result_key(processing, claim_id)) is None
    assert await real_async_redis_client.hget(gateway._claim_result_refs_key(processing), lease_token) is None


@pytest.mark.asyncio
async def test_async_visibility_timeout_recovery_miss_when_lease_token_gone(real_async_redis_client, queue_name):
    processing, dlq = f"{queue_name}:processing", f"{queue_name}:dlq"
    gateway = _async_gateway(real_async_redis_client, dlq)
    stored = encode_stored_message("already-reclaimed")
    claim_id = "vt-recover-miss"
    lease_token = "7"
    await _async_seed_vt_recovery_claim(
        client=real_async_redis_client,
        gateway=gateway,
        processing=processing,
        claim_id=claim_id,
        lease_token=lease_token,
        stored=stored,
        lease_deadline_score=1000,
    )
    await real_async_redis_client.hset(gateway._lease_tokens_key(processing), stored, "99")

    recovered = await gateway._recover_pending_visibility_timeout_claim(processing, claim_id)

    assert recovered is None
    assert await real_async_redis_client.zscore(gateway._lease_deadlines_key(processing), stored) == 1000
    assert await real_async_redis_client.get(gateway._claim_result_key(processing, claim_id)) is None


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


# ---------------------------------------------------------------------------
# Structural sentinel classification: a payload equal to the sentinel string
# must not be misread as a store failure (failure replies are exactly 3-tuples)
# ---------------------------------------------------------------------------

_SENTINEL_PAYLOAD = CLAIM_STORE_FAILED_LUA_SENTINEL.encode("utf-8")


def test_sync_claim_delivers_payload_equal_to_sentinel_string(real_redis_client, queue_name):
    pending, processing, dlq = _keys(queue_name)
    gateway = _sync_gateway(real_redis_client, dlq)
    real_redis_client.lpush(pending, _SENTINEL_PAYLOAD)

    claimed = gateway.wait_for_message_and_move(pending, processing)

    assert claimed is not None
    assert claimed.stored_message == _SENTINEL_PAYLOAD
    assert claimed.lease_token
    assert real_redis_client.lrange(processing, 0, -1) == [_SENTINEL_PAYLOAD]


def test_sync_claim_replay_of_sentinel_payload_is_not_misclassified(real_redis_client, queue_name):
    # The Lua cache-replay result is a 2-tuple {stored, lease_token}; a stored
    # payload equal to the sentinel string must not classify as a store failure.
    pending, processing, dlq = _keys(queue_name)
    gateway = _sync_gateway(real_redis_client, dlq)
    real_redis_client.lpush(pending, _SENTINEL_PAYLOAD)
    claim_id = "sentinel-replay-claim"
    first = gateway._claim_visible_message(pending, processing, claim_id=claim_id)
    assert first is not None

    replay = gateway._claim_visible_message(pending, processing, claim_id=claim_id)

    assert replay is not None
    assert replay.stored_message == _SENTINEL_PAYLOAD
    assert replay.lease_token == first.lease_token


@pytest.mark.asyncio
async def test_async_claim_delivers_payload_equal_to_sentinel_string(real_async_redis_client, queue_name):
    pending, processing, dlq = _keys(queue_name)
    gateway = _async_gateway(real_async_redis_client, dlq)
    await real_async_redis_client.lpush(pending, _SENTINEL_PAYLOAD)

    claimed = await gateway.wait_for_message_and_move(pending, processing)

    assert claimed is not None
    assert claimed.stored_message == _SENTINEL_PAYLOAD
    assert claimed.lease_token
    assert await real_async_redis_client.lrange(processing, 0, -1) == [_SENTINEL_PAYLOAD]


# ---------------------------------------------------------------------------
# Non-UTF-8 claim results: the claim Lua stores cjson.encode({stored, token})
# with raw payload bytes, so a foreign binary payload yields a claim result the
# Python recovery cannot parse. It must treat it as corrupt (purge + miss), not
# crash the consumer and drain() with UnicodeDecodeError.
# ---------------------------------------------------------------------------

_INVALID_UTF8_STORED = b"\xff\xfe\x80raw-binary"


def test_sync_vt_recovery_treats_non_utf8_claim_result_as_corrupt(real_redis_client, queue_name):
    processing, dlq = f"{queue_name}:processing", f"{queue_name}:dlq"
    gateway = _sync_gateway(real_redis_client, dlq)
    claim_id = "vt-recover-binary"
    # Exactly what the claim Lua's cjson.encode({stored, lease_token}) stores
    # for a foreign non-UTF-8 payload: raw bytes embedded in the JSON string.
    claim_payload = b'["' + _INVALID_UTF8_STORED + b'","7"]'
    real_redis_client.rpush(processing, _INVALID_UTF8_STORED)
    real_redis_client.set(
        gateway._claim_result_key(processing, claim_id), claim_payload, px=int(gateway._claim_result_ttl_ms())
    )
    real_redis_client.hset(gateway._claim_result_ids_key(processing), claim_id, claim_payload)

    recovered = gateway._recover_pending_visibility_timeout_claim(processing, claim_id)

    assert recovered is None
    assert real_redis_client.get(gateway._claim_result_key(processing, claim_id)) is None
    assert real_redis_client.hget(gateway._claim_result_ids_key(processing), claim_id) is None


def test_sync_lost_claim_response_for_non_utf8_payload_recovers_and_redelivers(real_redis_client, queue_name):
    pending, processing, dlq = _keys(queue_name)
    gateway = _sync_gateway(real_redis_client, dlq)
    real_redis_client.lpush(pending, _INVALID_UTF8_STORED)
    # Commit a claim server-side through the real Lua script, then simulate the
    # response being lost: register the claim id for recovery like the retry
    # path does.
    claim_id = "lost-response-claim"
    assert gateway._claim_visible_message(pending, processing, claim_id=claim_id) is not None
    gateway._set_pending_claim_id(processing, claim_id)

    # Recovery treats the unparseable claim result as corrupt, clears the
    # pending id, and polls normally (nothing else pending).
    assert gateway.wait_for_message_and_move(pending, processing) is None
    assert processing not in gateway._pending_claim_ids
    assert real_redis_client.hget(gateway._claim_result_ids_key(processing), claim_id) is None

    # The message redelivers via lease expiry with its exact original bytes.
    real_redis_client.zadd(gateway._lease_deadlines_key(processing), {_INVALID_UTF8_STORED: 0})
    redelivered = gateway.wait_for_message_and_move(pending, processing)
    assert redelivered is not None
    assert redelivered.stored_message == _INVALID_UTF8_STORED


def test_sync_drain_survives_non_utf8_claim_result(real_redis_client, queue_name):
    queue = RedisMessageQueue(
        queue_name,
        client=real_redis_client,
        deduplication=False,
        visibility_timeout_seconds=30,
    )
    gateway = queue._redis
    real_redis_client.lpush(queue.key.pending, _INVALID_UTF8_STORED)
    claim_id = "drain-binary-claim"
    assert gateway._claim_visible_message(queue.key.pending, queue.key.processing, claim_id=claim_id) is not None
    gateway._set_pending_claim_id(queue.key.processing, claim_id)

    assert queue.drain(timeout=5) is True
    assert queue.key.processing not in gateway._pending_claim_ids


@pytest.mark.asyncio
async def test_async_vt_recovery_treats_non_utf8_claim_result_as_corrupt(real_async_redis_client, queue_name):
    processing, dlq = f"{queue_name}:processing", f"{queue_name}:dlq"
    gateway = _async_gateway(real_async_redis_client, dlq)
    claim_id = "vt-recover-binary"
    claim_payload = b'["' + _INVALID_UTF8_STORED + b'","7"]'
    await real_async_redis_client.rpush(processing, _INVALID_UTF8_STORED)
    await real_async_redis_client.set(
        gateway._claim_result_key(processing, claim_id), claim_payload, px=int(gateway._claim_result_ttl_ms())
    )
    await real_async_redis_client.hset(gateway._claim_result_ids_key(processing), claim_id, claim_payload)

    recovered = await gateway._recover_pending_visibility_timeout_claim(processing, claim_id)

    assert recovered is None
    assert await real_async_redis_client.get(gateway._claim_result_key(processing, claim_id)) is None
    assert await real_async_redis_client.hget(gateway._claim_result_ids_key(processing), claim_id) is None


@pytest.mark.asyncio
async def test_async_lost_claim_response_for_non_utf8_payload_recovers_and_redelivers(
    real_async_redis_client, queue_name
):
    pending, processing, dlq = _keys(queue_name)
    gateway = _async_gateway(real_async_redis_client, dlq)
    await real_async_redis_client.lpush(pending, _INVALID_UTF8_STORED)
    claim_id = "lost-response-claim"
    assert await gateway._claim_visible_message(pending, processing, claim_id=claim_id) is not None
    gateway._set_pending_claim_id(processing, claim_id)

    assert await gateway.wait_for_message_and_move(pending, processing) is None
    assert processing not in gateway._pending_claim_ids
    assert await real_async_redis_client.hget(gateway._claim_result_ids_key(processing), claim_id) is None

    await real_async_redis_client.zadd(gateway._lease_deadlines_key(processing), {_INVALID_UTF8_STORED: 0})
    redelivered = await gateway.wait_for_message_and_move(pending, processing)
    assert redelivered is not None
    assert redelivered.stored_message == _INVALID_UTF8_STORED


def test_sync_vt_recovery_treats_bare_surrogate_escape_claim_result_as_corrupt(real_redis_client, queue_name):
    # A bare \ud800 escape parses to a lone surrogate that cannot be re-encoded
    # to bytes. The claim Lua cannot produce it (cjson doubles backslashes), so
    # it is tampered/corrupt data and must purge-and-miss, not crash.
    processing, dlq = f"{queue_name}:processing", f"{queue_name}:dlq"
    gateway = _sync_gateway(real_redis_client, dlq)
    claim_id = "vt-recover-tampered"
    claim_payload = b'["x\\ud800y","7"]'
    real_redis_client.set(
        gateway._claim_result_key(processing, claim_id), claim_payload, px=int(gateway._claim_result_ttl_ms())
    )
    real_redis_client.hset(gateway._claim_result_ids_key(processing), claim_id, claim_payload)

    recovered = gateway._recover_pending_visibility_timeout_claim(processing, claim_id)

    assert recovered is None
    assert real_redis_client.get(gateway._claim_result_key(processing, claim_id)) is None
    assert real_redis_client.hget(gateway._claim_result_ids_key(processing), claim_id) is None


@pytest.mark.asyncio
async def test_async_vt_recovery_treats_bare_surrogate_escape_claim_result_as_corrupt(
    real_async_redis_client, queue_name
):
    processing, dlq = f"{queue_name}:processing", f"{queue_name}:dlq"
    gateway = _async_gateway(real_async_redis_client, dlq)
    claim_id = "vt-recover-tampered"
    claim_payload = b'["x\\ud800y","7"]'
    await real_async_redis_client.set(
        gateway._claim_result_key(processing, claim_id), claim_payload, px=int(gateway._claim_result_ttl_ms())
    )
    await real_async_redis_client.hset(gateway._claim_result_ids_key(processing), claim_id, claim_payload)

    recovered = await gateway._recover_pending_visibility_timeout_claim(processing, claim_id)

    assert recovered is None
    assert await real_async_redis_client.get(gateway._claim_result_key(processing, claim_id)) is None
    assert await real_async_redis_client.hget(gateway._claim_result_ids_key(processing), claim_id) is None
