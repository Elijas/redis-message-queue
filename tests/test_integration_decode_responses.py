import pytest
import pytest_asyncio
import redis
import redis.asyncio

from redis_message_queue.asyncio.redis_message_queue import RedisMessageQueue as AsyncRedisMessageQueue
from redis_message_queue.redis_message_queue import RedisMessageQueue
from tests.conftest import close_async_redis_client

pytestmark = pytest.mark.integration


@pytest.fixture()
def real_decode_redis_client(real_redis_url):
    client = redis.Redis.from_url(real_redis_url, decode_responses=True)
    try:
        client.ping()
    except redis.ConnectionError:
        pytest.skip("Redis not available")
    yield client
    client.close()


@pytest_asyncio.fixture()
async def real_async_decode_redis_client(real_redis_url):
    client = redis.asyncio.Redis.from_url(real_redis_url, decode_responses=True)
    try:
        await client.ping()
    except redis.ConnectionError:
        pytest.skip("Redis not available")
    yield client
    await close_async_redis_client(client)


class TestSyncDecodeResponsesRealRedis:
    def test_publish_process_ack_yields_str(self, real_decode_redis_client, queue_name):
        queue = RedisMessageQueue(queue_name, client=real_decode_redis_client, deduplication=False)
        queue.publish("hello")

        with queue.process_message() as msg:
            assert msg == "hello"
            assert isinstance(msg, str)

        assert real_decode_redis_client.llen(queue.key.pending) == 0
        assert real_decode_redis_client.llen(queue.key.processing) == 0

    def test_dedup_rejects_duplicate(self, real_decode_redis_client, queue_name):
        queue = RedisMessageQueue(queue_name, client=real_decode_redis_client, deduplication=True)

        assert queue.publish("hello") is True
        assert queue.publish("hello") is False
        assert real_decode_redis_client.llen(queue.key.pending) == 1

    def test_visibility_timeout_lease_and_ack(self, real_decode_redis_client, queue_name):
        queue = RedisMessageQueue(
            queue_name,
            client=real_decode_redis_client,
            deduplication=False,
            visibility_timeout_seconds=30,
            heartbeat_interval_seconds=5,
        )
        queue.publish("hello-vt")

        with queue.process_message() as msg:
            assert msg == "hello-vt"
            assert isinstance(msg, str)

        assert real_decode_redis_client.llen(queue.key.pending) == 0
        assert real_decode_redis_client.llen(queue.key.processing) == 0


class TestAsyncDecodeResponsesRealRedis:
    @pytest.mark.asyncio
    async def test_publish_process_ack_yields_str(self, real_async_decode_redis_client, queue_name):
        queue = AsyncRedisMessageQueue(queue_name, client=real_async_decode_redis_client, deduplication=False)
        await queue.publish("hello")

        async with queue.process_message() as msg:
            assert msg == "hello"
            assert isinstance(msg, str)

        assert await real_async_decode_redis_client.llen(queue.key.pending) == 0
        assert await real_async_decode_redis_client.llen(queue.key.processing) == 0

    @pytest.mark.asyncio
    async def test_dedup_rejects_duplicate(self, real_async_decode_redis_client, queue_name):
        queue = AsyncRedisMessageQueue(queue_name, client=real_async_decode_redis_client, deduplication=True)

        assert await queue.publish("hello") is True
        assert await queue.publish("hello") is False
        assert await real_async_decode_redis_client.llen(queue.key.pending) == 1

    @pytest.mark.asyncio
    async def test_visibility_timeout_lease_and_ack(self, real_async_decode_redis_client, queue_name):
        queue = AsyncRedisMessageQueue(
            queue_name,
            client=real_async_decode_redis_client,
            deduplication=False,
            visibility_timeout_seconds=30,
            heartbeat_interval_seconds=5,
        )
        await queue.publish("hello-vt")

        async with queue.process_message() as msg:
            assert msg == "hello-vt"
            assert isinstance(msg, str)

        assert await real_async_decode_redis_client.llen(queue.key.pending) == 0
        assert await real_async_decode_redis_client.llen(queue.key.processing) == 0
