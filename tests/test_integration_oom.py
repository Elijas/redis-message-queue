import hashlib
import time

import pytest
import redis.exceptions

from redis_message_queue import ClaimStoreFailedError
from redis_message_queue._redis_gateway import RedisGateway
from redis_message_queue.redis_message_queue import RedisMessageQueue

pytestmark = pytest.mark.integration

_FILLER_PREFIX = "rmq-oom-filler:"
_FILLER_CHUNK = b"x" * (256 * 1024)
_LARGE_MESSAGE = "x" * (1024 * 1024)


@pytest.fixture()
def oom_redis_client(real_redis_client):
    _skip_unless_noeviction_maxmemory(real_redis_client)
    real_redis_client.flushdb()
    yield real_redis_client
    real_redis_client.flushdb()


def _skip_unless_noeviction_maxmemory(client):
    maxmemory = int(client.config_get("maxmemory").get("maxmemory", 0))
    policy = client.config_get("maxmemory-policy").get("maxmemory-policy")
    if maxmemory <= 0 or policy != "noeviction":
        pytest.skip("Redis is not configured with maxmemory and noeviction")


def _fill_redis_to_oom(client):
    keys = []
    for i in range(1_000):
        key = f"{_FILLER_PREFIX}{i}"
        try:
            client.set(key, _FILLER_CHUNK)
        except redis.exceptions.ResponseError as exc:
            if _is_oom_error(exc):
                return keys
            raise
        keys.append(key)
    pytest.fail("Redis did not reach OOM after writing filler keys")


def _is_oom_error(exc):
    return isinstance(exc, redis.exceptions.OutOfMemoryError) or "OOM" in str(exc) or "maxmemory" in str(exc)


def _free_filler_chunks(client, keys, count):
    to_delete = keys[-count:]
    if len(to_delete) != count:
        pytest.fail("Not enough filler keys were created before OOM")
    client.delete(*to_delete)
    del keys[-count:]


def _default_dedup_redis_key(queue, message):
    digest = hashlib.sha256(message.encode("utf-8")).hexdigest()
    return queue.key.deduplication(digest)


def test_publish_oom_clears_dedup_key_for_retry(oom_redis_client, queue_name):
    queue = RedisMessageQueue(queue_name, client=oom_redis_client)
    dedup_key = _default_dedup_redis_key(queue, _LARGE_MESSAGE)

    filler_keys = _fill_redis_to_oom(oom_redis_client)
    _free_filler_chunks(oom_redis_client, filler_keys, count=1)

    with pytest.raises(redis.exceptions.ResponseError, match="dedup key cleared|maxmemory|OOM"):
        queue.publish(_LARGE_MESSAGE)

    assert oom_redis_client.exists(dedup_key) == 0
    assert oom_redis_client.llen(queue.key.pending) == 0


def test_claim_store_compensation_returns_message_to_pending_under_oom_pressure(oom_redis_client, queue_name):
    gateway = RedisGateway(
        redis_client=oom_redis_client,
        retry_budget_seconds=0,
        message_wait_interval_seconds=0,
        message_visibility_timeout_seconds=30,
        max_delivery_count=None,
    )
    queue = RedisMessageQueue(queue_name, gateway=gateway, deduplication=False)
    assert queue.publish("claim-me") is True
    # Force store_claim_and_return's pcall branch after the server is filled.
    # Redis versions differ on whether noeviction OOM is raised before or inside Lua.
    oom_redis_client.set(gateway._lease_token_counter_key(queue.key.processing), "not-an-int")

    filler_keys = _fill_redis_to_oom(oom_redis_client)
    _free_filler_chunks(oom_redis_client, filler_keys, count=1)

    with pytest.raises(ClaimStoreFailedError):
        gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)

    assert oom_redis_client.llen(queue.key.pending) == 1
    assert oom_redis_client.llen(queue.key.processing) == 0


def test_expiry_reclaim_rpush_path_under_maxmemory_pressure(oom_redis_client, queue_name):
    gateway = RedisGateway(
        redis_client=oom_redis_client,
        retry_budget_seconds=0,
        message_wait_interval_seconds=0,
        message_visibility_timeout_seconds=1,
        max_delivery_count=None,
    )
    queue = RedisMessageQueue(queue_name, gateway=gateway, deduplication=False)
    assert queue.publish("reclaim-me") is True

    first = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
    assert first is not None
    time.sleep(1.5)

    filler_keys = _fill_redis_to_oom(oom_redis_client)
    _free_filler_chunks(oom_redis_client, filler_keys, count=2)

    second = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)

    assert second is not None
    assert second.stored_message == first.stored_message
    assert second.lease_token != first.lease_token
