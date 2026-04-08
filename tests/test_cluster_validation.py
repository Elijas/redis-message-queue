import pytest
import redis
import redis.asyncio

from redis_message_queue._redis_gateway import RedisGateway
from redis_message_queue.asyncio._redis_gateway import RedisGateway as AsyncRedisGateway
from redis_message_queue.asyncio.redis_message_queue import RedisMessageQueue as AsyncRedisMessageQueue
from redis_message_queue.redis_message_queue import RedisMessageQueue


def _no_retry(func):
    return func


def _sync_cluster_client():
    return redis.RedisCluster.__new__(redis.RedisCluster)


def _async_cluster_client():
    return redis.asyncio.RedisCluster.__new__(redis.asyncio.RedisCluster)


class TestSyncRedisClusterValidation:
    def test_client_path_requires_hash_tagged_queue_name(self):
        with pytest.raises(ValueError, match="Redis Cluster requires queue keys to share a hash tag"):
            RedisMessageQueue("orders", client=_sync_cluster_client())

    def test_gateway_path_requires_hash_tagged_queue_name(self):
        gateway = RedisGateway(redis_client=_sync_cluster_client(), retry_strategy=_no_retry)
        with pytest.raises(ValueError, match="Redis Cluster requires queue keys to share a hash tag"):
            RedisMessageQueue("orders", gateway=gateway)

    def test_hash_tagged_queue_name_is_accepted(self):
        queue = RedisMessageQueue("{orders}", client=_sync_cluster_client())
        assert queue.key.pending == "{orders}::pending"

    def test_gateway_dead_letter_queue_must_share_hash_tag(self):
        gateway = RedisGateway(
            redis_client=_sync_cluster_client(),
            retry_strategy=_no_retry,
            message_visibility_timeout_seconds=30,
            max_delivery_count=3,
            dead_letter_queue="other::dead_letter",
        )
        with pytest.raises(ValueError, match="dead_letter_queue"):
            RedisMessageQueue("{orders}", gateway=gateway)


class TestAsyncRedisClusterValidation:
    def test_client_path_requires_hash_tagged_queue_name(self):
        with pytest.raises(ValueError, match="Redis Cluster requires queue keys to share a hash tag"):
            AsyncRedisMessageQueue("orders", client=_async_cluster_client())

    def test_gateway_path_requires_hash_tagged_queue_name(self):
        gateway = AsyncRedisGateway(redis_client=_async_cluster_client(), retry_strategy=_no_retry)
        with pytest.raises(ValueError, match="Redis Cluster requires queue keys to share a hash tag"):
            AsyncRedisMessageQueue("orders", gateway=gateway)

    def test_hash_tagged_queue_name_is_accepted(self):
        queue = AsyncRedisMessageQueue("{orders}", client=_async_cluster_client())
        assert queue.key.pending == "{orders}::pending"

    def test_gateway_dead_letter_queue_must_share_hash_tag(self):
        gateway = AsyncRedisGateway(
            redis_client=_async_cluster_client(),
            retry_strategy=_no_retry,
            message_visibility_timeout_seconds=30,
            max_delivery_count=3,
            dead_letter_queue="other::dead_letter",
        )
        with pytest.raises(ValueError, match="dead_letter_queue"):
            AsyncRedisMessageQueue("{orders}", gateway=gateway)
