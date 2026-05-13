import asyncio
import os
import time

import pytest
import pytest_asyncio
import redis
import redis.asyncio

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
    keys = list(client.scan_iter(match="{cluster-test}*"))
    if keys:
        client.delete(*keys)


async def _async_delete_cluster_test_keys(client):
    keys = [key async for key in client.scan_iter(match="{cluster-test}*")]
    if keys:
        await client.delete(*keys)


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
