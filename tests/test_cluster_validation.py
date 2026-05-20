import fakeredis
import pytest
import redis
import redis.asyncio

from redis_message_queue._exceptions import ConfigurationError
from redis_message_queue._redis_gateway import RedisGateway
from redis_message_queue.asyncio._redis_gateway import RedisGateway as AsyncRedisGateway
from redis_message_queue.asyncio.redis_message_queue import RedisMessageQueue as AsyncRedisMessageQueue
from redis_message_queue.redis_message_queue import RedisMessageQueue
from tests.conftest import close_async_redis_client


def _sync_cluster_client():
    return redis.RedisCluster.__new__(redis.RedisCluster)


def _async_cluster_client():
    return redis.asyncio.RedisCluster.__new__(redis.asyncio.RedisCluster)


class TestSyncRedisClusterValidation:
    def test_plain_redis_client_reporting_cluster_mode_raises_configuration_error(self, monkeypatch):
        client = redis.Redis(host="127.0.0.1", port=0)

        def info(section):
            assert section == "cluster"
            return {"cluster_enabled": 1}

        monkeypatch.setattr(client, "info", info)

        with pytest.raises(ConfigurationError, match="plain Redis.*Redis Cluster"):
            RedisMessageQueue("orders", client=client)

    def test_plain_redis_client_reporting_standalone_mode_is_accepted(self, monkeypatch):
        client = redis.Redis(host="127.0.0.1", port=0)
        info_calls = []

        def info(section):
            info_calls.append(section)
            return {"cluster_enabled": 0}

        monkeypatch.setattr(client, "info", info)

        queue = RedisMessageQueue("orders", client=client)

        assert queue.key.pending == "orders::pending"
        assert info_calls == ["cluster"]

    def test_fakeredis_client_skips_cluster_probe(self, monkeypatch):
        client = fakeredis.FakeRedis()

        def info(section):
            raise AssertionError(f"unexpected INFO probe for {section}")

        monkeypatch.setattr(client, "info", info)

        queue = RedisMessageQueue("orders", client=client)

        assert queue.key.pending == "orders::pending"

    def test_client_path_requires_hash_tagged_queue_name(self):
        with pytest.raises(ValueError, match="Redis Cluster requires queue keys to share a hash tag"):
            RedisMessageQueue("orders", client=_sync_cluster_client())

    def test_gateway_path_requires_hash_tagged_queue_name(self):
        gateway = RedisGateway(redis_client=_sync_cluster_client(), retry_budget_seconds=0)
        with pytest.raises(ValueError, match="Redis Cluster requires queue keys to share a hash tag"):
            RedisMessageQueue("orders", gateway=gateway)

    def test_hash_tagged_queue_name_is_accepted(self):
        queue = RedisMessageQueue("{orders}", client=_sync_cluster_client())
        assert queue.key.pending == "{orders}::pending"

    def test_gateway_dead_letter_queue_must_share_hash_tag(self):
        gateway = RedisGateway(
            redis_client=_sync_cluster_client(),
            retry_budget_seconds=0,
            message_visibility_timeout_seconds=30,
            max_delivery_count=3,
            dead_letter_queue="other::dead_letter",
        )
        with pytest.raises(ValueError, match="dead_letter_queue"):
            RedisMessageQueue("{orders}", gateway=gateway)


class TestAsyncRedisClusterValidation:
    @pytest.mark.asyncio
    async def test_plain_redis_client_reporting_cluster_mode_raises_configuration_error(self, monkeypatch):
        client = redis.asyncio.Redis(host="127.0.0.1", port=0)

        async def info(section):
            assert section == "cluster"
            return {"cluster_enabled": 1}

        monkeypatch.setattr(client, "info", info)
        queue = AsyncRedisMessageQueue("orders", client=client, deduplication=False)

        try:
            with pytest.raises(ConfigurationError, match="plain Redis.*Redis Cluster"):
                await queue.publish("payload")
        finally:
            await close_async_redis_client(client)

    @pytest.mark.asyncio
    async def test_plain_redis_client_reporting_standalone_mode_is_accepted(self, monkeypatch):
        client = redis.asyncio.Redis(host="127.0.0.1", port=0)
        info_calls = []
        lpush_calls = []

        async def info(section):
            info_calls.append(section)
            return {"cluster_enabled": 0}

        async def lpush(*args):
            lpush_calls.append(args)
            return 1

        monkeypatch.setattr(client, "info", info)
        monkeypatch.setattr(client, "lpush", lpush)
        queue = AsyncRedisMessageQueue("orders", client=client, deduplication=False)

        try:
            assert await queue.publish("payload")
        finally:
            await close_async_redis_client(client)

        assert info_calls == ["cluster"]
        assert len(lpush_calls) == 1

    @pytest.mark.asyncio
    async def test_fakeredis_client_skips_cluster_probe(self, monkeypatch):
        client = fakeredis.FakeAsyncRedis()

        async def info(section):
            raise AssertionError(f"unexpected INFO probe for {section}")

        monkeypatch.setattr(client, "info", info)
        queue = AsyncRedisMessageQueue("orders", client=client, deduplication=False)

        try:
            assert await queue.publish("payload")
        finally:
            await close_async_redis_client(client)

    def test_client_path_requires_hash_tagged_queue_name(self):
        with pytest.raises(ValueError, match="Redis Cluster requires queue keys to share a hash tag"):
            AsyncRedisMessageQueue("orders", client=_async_cluster_client())

    def test_gateway_path_requires_hash_tagged_queue_name(self):
        gateway = AsyncRedisGateway(redis_client=_async_cluster_client(), retry_budget_seconds=0)
        with pytest.raises(ValueError, match="Redis Cluster requires queue keys to share a hash tag"):
            AsyncRedisMessageQueue("orders", gateway=gateway)

    def test_hash_tagged_queue_name_is_accepted(self):
        queue = AsyncRedisMessageQueue("{orders}", client=_async_cluster_client())
        assert queue.key.pending == "{orders}::pending"

    def test_gateway_dead_letter_queue_must_share_hash_tag(self):
        gateway = AsyncRedisGateway(
            redis_client=_async_cluster_client(),
            retry_budget_seconds=0,
            message_visibility_timeout_seconds=30,
            max_delivery_count=3,
            dead_letter_queue="other::dead_letter",
        )
        with pytest.raises(ValueError, match="dead_letter_queue"):
            AsyncRedisMessageQueue("{orders}", gateway=gateway)
