import asyncio
import os
import time

import pytest
import pytest_asyncio
import redis
import redis.asyncio
from redis.cluster import key_slot

from redis_message_queue.asyncio.redis_message_queue import RedisMessageQueue as AsyncRedisMessageQueue
from redis_message_queue.redis_message_queue import RedisMessageQueue
from tests.conftest import close_async_redis_client

pytestmark = pytest.mark.integration


@pytest.fixture(scope="session")
def real_redis_cluster_url():
    url = os.environ.get("REDIS_CLUSTER_URL")
    if not url:
        pytest.skip("Redis Cluster not available")
    return url


@pytest.fixture()
def real_redis_cluster_client(real_redis_cluster_url):
    client = redis.RedisCluster.from_url(real_redis_cluster_url, socket_connect_timeout=2)
    try:
        client.ping()
    except redis.RedisError as exc:
        client.close()
        pytest.skip(f"Redis Cluster not available: {exc}")
    _delete_cluster_test_keys(client)
    yield client
    _delete_cluster_test_keys(client)
    client.close()


@pytest_asyncio.fixture()
async def real_async_redis_cluster_client(real_redis_cluster_url):
    client = redis.asyncio.RedisCluster.from_url(real_redis_cluster_url, socket_connect_timeout=2)
    try:
        await client.ping()
    except redis.RedisError as exc:
        await close_async_redis_client(client)
        pytest.skip(f"Redis Cluster not available: {exc}")
    await _async_delete_cluster_test_keys(client)
    yield client
    await _async_delete_cluster_test_keys(client)
    await close_async_redis_client(client)


def _delete_cluster_test_keys(client):
    for pattern in ("{cluster-test}*", "{cluster-test-b1}*", "wrong-slot::*"):
        for key in list(client.scan_iter(match=pattern)):
            client.delete(key)


async def _async_delete_cluster_test_keys(client):
    for pattern in ("{cluster-test}*", "{cluster-test-b1}*", "wrong-slot::*"):
        async for key in client.scan_iter(match=pattern):
            await client.delete(key)


def _redis_key_slot(key: str) -> int:
    return key_slot(key.encode("utf-8"))


def _cross_slot_key(reference_key: str) -> str:
    reference_slot = _redis_key_slot(reference_key)
    for key in ("wrong-slot::orphan", "{wrong-slot}::orphan", "wrong-slot::orphan::2"):
        if _redis_key_slot(key) != reference_slot:
            return key
    raise AssertionError("test setup could not find a cross-slot key")


def _cluster_b1_queue(client):
    return RedisMessageQueue(
        "{cluster-test-b1}",
        client=client,
        deduplication=False,
        visibility_timeout_seconds=1,
        max_delivery_count=3,
    )


def _publish_and_claim(queue):
    queue.publish("test-payload")
    claimed = queue._redis.wait_for_message_and_move(queue.key.pending, queue.key.processing)
    assert claimed is not None
    return claimed


def _point_claim_result_ref_at_cross_slot_key(client, queue, lease_token: str) -> str:
    refs_key = queue._redis._claim_result_refs_key(queue.key.processing)
    wrong_slot_key = _cross_slot_key(refs_key)
    client.hset(refs_key, lease_token, wrong_slot_key)
    return wrong_slot_key


def _assert_clean_lease_state(client, queue):
    assert client.llen(queue.key.pending) == 0
    assert client.llen(queue.key.processing) == 0
    assert client.zcard(queue._redis._lease_deadlines_key(queue.key.processing)) == 0
    assert client.hlen(queue._redis._lease_tokens_key(queue.key.processing)) == 0
    assert client.hlen(queue._redis._claim_result_refs_key(queue.key.processing)) == 0


def test_hash_tagged_cluster_publish_ack_and_reclaim(real_redis_cluster_client):
    queue = RedisMessageQueue(
        "{cluster-test}",
        client=real_redis_cluster_client,
        deduplication=False,
        visibility_timeout_seconds=1,
        max_delivery_count=None,
    )
    queue.publish("ack-me")

    with queue.process_message() as message:
        assert message == b"ack-me"

    assert real_redis_cluster_client.llen(queue.key.pending) == 0
    assert real_redis_cluster_client.llen(queue.key.processing) == 0

    queue.publish("reclaim-me")
    with pytest.raises(SystemExit, match="simulate crash"):
        with queue.process_message() as message:
            assert message == b"reclaim-me"
            raise SystemExit("simulate crash")

    time.sleep(1.5)

    with queue.process_message() as message:
        assert message == b"reclaim-me"

    assert real_redis_cluster_client.llen(queue.key.pending) == 0
    assert real_redis_cluster_client.llen(queue.key.processing) == 0


def test_b1_pcall_swallows_cluster_rejection_on_data_derived_del_reclaim(real_redis_cluster_client):
    """Regression for B1 (R6-H3 / AB-04 F1).

    VT reclaim reads a claim_result_refs key name from a hash and DELs it. Under
    Redis Cluster that data-derived key can be in a different slot from the
    script's declared KEYS. The pcall wrap must swallow the rejection so ZREM,
    HDEL, LREM, and requeue still complete.
    """
    queue = _cluster_b1_queue(real_redis_cluster_client)

    first = _publish_and_claim(queue)
    _point_claim_result_ref_at_cross_slot_key(real_redis_cluster_client, queue, first.lease_token)

    lease_deadlines_key = queue._redis._lease_deadlines_key(queue.key.processing)
    real_redis_cluster_client.zadd(lease_deadlines_key, {first.stored_message: 0})

    reclaimed = queue._redis.wait_for_message_and_move(queue.key.pending, queue.key.processing)

    assert reclaimed is not None, "Expected reclaim; B1 pcall may be missing"
    assert reclaimed.stored_message == first.stored_message
    assert reclaimed.lease_token != first.lease_token
    assert (
        real_redis_cluster_client.hget(queue._redis._claim_result_refs_key(queue.key.processing), first.lease_token)
        is None
    )
    assert real_redis_cluster_client.llen(queue.key.pending) == 0
    assert real_redis_cluster_client.llen(queue.key.processing) == 1

    assert queue._redis.remove_message(
        queue.key.processing, reclaimed.stored_message, lease_token=reclaimed.lease_token
    )
    _assert_clean_lease_state(real_redis_cluster_client, queue)


def test_b1_pcall_swallows_cluster_rejection_on_ack_data_derived_del(real_redis_cluster_client):
    """Regression coverage for REMOVE_MESSAGE_WITH_LEASE_TOKEN_LUA_SCRIPT."""
    queue = _cluster_b1_queue(real_redis_cluster_client)

    claimed = _publish_and_claim(queue)
    _point_claim_result_ref_at_cross_slot_key(real_redis_cluster_client, queue, claimed.lease_token)

    assert queue._redis.remove_message(queue.key.processing, claimed.stored_message, lease_token=claimed.lease_token)

    _assert_clean_lease_state(real_redis_cluster_client, queue)


def test_b1_pcall_swallows_cluster_rejection_on_move_data_derived_del(real_redis_cluster_client):
    """Regression coverage for MOVE_MESSAGE_WITH_LEASE_TOKEN_LUA_SCRIPT."""
    queue = _cluster_b1_queue(real_redis_cluster_client)

    claimed = _publish_and_claim(queue)
    _point_claim_result_ref_at_cross_slot_key(real_redis_cluster_client, queue, claimed.lease_token)

    assert queue._redis.move_message(
        queue.key.processing,
        queue.key.completed,
        claimed.stored_message,
        lease_token=claimed.lease_token,
    )

    _assert_clean_lease_state(real_redis_cluster_client, queue)
    assert real_redis_cluster_client.llen(queue.key.completed) == 1


@pytest.mark.asyncio
async def test_async_hash_tagged_cluster_publish_ack_and_reclaim(real_async_redis_cluster_client):
    queue = AsyncRedisMessageQueue(
        "{cluster-test}",
        client=real_async_redis_cluster_client,
        deduplication=False,
        visibility_timeout_seconds=1,
        max_delivery_count=None,
    )
    await queue.publish("ack-me")

    async with queue.process_message() as message:
        assert message == b"ack-me"

    assert await real_async_redis_cluster_client.llen(queue.key.pending) == 0
    assert await real_async_redis_cluster_client.llen(queue.key.processing) == 0

    await queue.publish("reclaim-me")
    with pytest.raises(SystemExit, match="simulate crash"):
        async with queue.process_message() as message:
            assert message == b"reclaim-me"
            raise SystemExit("simulate crash")

    await asyncio.sleep(1.5)

    async with queue.process_message() as message:
        assert message == b"reclaim-me"

    assert await real_async_redis_cluster_client.llen(queue.key.pending) == 0
    assert await real_async_redis_cluster_client.llen(queue.key.processing) == 0
