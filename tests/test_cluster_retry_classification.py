import fakeredis
import pytest
import redis.exceptions
from tenacity import stop_after_attempt, wait_none

import redis_message_queue._config as config
from redis_message_queue._config import is_redis_retryable_exception
from redis_message_queue._redis_gateway import RedisGateway
from redis_message_queue._stored_message import encode_stored_message
from redis_message_queue.asyncio._redis_gateway import RedisGateway as AsyncRedisGateway


class _TtlExhaustedEvalSyncClient:
    def __init__(self):
        self.redis = fakeredis.FakeRedis()
        self.eval_calls = 0

    def eval(self, *args):
        self.eval_calls += 1
        if self.eval_calls == 1:
            raise redis.exceptions.ClusterError("TTL exhausted.")
        return self.redis.eval(*args)

    def __getattr__(self, name):
        return getattr(self.redis, name)


class _TtlExhaustedEvalAsyncClient:
    def __init__(self):
        self.redis = fakeredis.FakeAsyncRedis()
        self.eval_calls = 0

    async def eval(self, *args):
        self.eval_calls += 1
        if self.eval_calls == 1:
            raise redis.exceptions.ClusterError("TTL exhausted.")
        return await self.redis.eval(*args)

    def __getattr__(self, name):
        return getattr(self.redis, name)


def _force_two_attempt_retry_budget(monkeypatch):
    monkeypatch.setattr(config, "stop_after_delay", lambda _seconds: stop_after_attempt(2))
    monkeypatch.setattr(config, "wait_exponential_jitter", lambda **kwargs: wait_none())


def test_retry_classifier_matches_only_ttl_exhausted_cluster_error():
    assert is_redis_retryable_exception(redis.exceptions.ClusterError("TTL exhausted.")) is True
    assert is_redis_retryable_exception(redis.exceptions.ClusterError("no primary node for slot")) is False


def test_sync_publish_retries_ttl_exhausted_cluster_error_from_eval(monkeypatch):
    _force_two_attempt_retry_budget(monkeypatch)
    client = _TtlExhaustedEvalSyncClient()
    gateway = RedisGateway(redis_client=client)

    result = gateway.publish_message("pending", "hello", "dedup:hello")

    assert result is True
    assert client.eval_calls == 2
    assert client.redis.llen("pending") == 1
    assert client.redis.exists("dedup:hello") == 1


@pytest.mark.asyncio
async def test_async_lease_ack_retries_ttl_exhausted_cluster_error_from_eval(monkeypatch):
    _force_two_attempt_retry_budget(monkeypatch)
    client = _TtlExhaustedEvalAsyncClient()
    stored_message = encode_stored_message("hello")
    await client.redis.lpush("processing", stored_message)
    await client.redis.zadd("processing:lease_deadlines", {stored_message: 999999999999})
    await client.redis.hset("processing:lease_tokens", stored_message, "lease-1")
    gateway = AsyncRedisGateway(redis_client=client)

    result = await gateway.remove_message("processing", stored_message, lease_token="lease-1")

    assert result is True
    assert client.eval_calls == 2
    assert await client.redis.llen("processing") == 0
    assert await client.redis.zcard("processing:lease_deadlines") == 0
    assert await client.redis.hlen("processing:lease_tokens") == 0
