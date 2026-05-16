import fakeredis
import pytest

from redis_message_queue.asyncio.redis_message_queue import RedisMessageQueue as AsyncRedisMessageQueue
from redis_message_queue.redis_message_queue import RedisMessageQueue


@pytest.fixture
def decode_client():
    return fakeredis.FakeRedis(decode_responses=True)


@pytest.fixture
def async_decode_client():
    return fakeredis.FakeAsyncRedis(decode_responses=True)


class TestSyncDecodeResponses:
    def test_publish_process_ack_yields_str(self, decode_client):
        queue = RedisMessageQueue("test", client=decode_client, deduplication=False)
        queue.publish("hello")

        with queue.process_message() as msg:
            assert msg == "hello"
            assert isinstance(msg, str)

    def test_dedup_rejects_duplicate(self, decode_client):
        queue = RedisMessageQueue(
            "test",
            client=decode_client,
            deduplication=True,
            get_deduplication_key=lambda msg: msg,
        )

        assert queue.publish("hello") is True
        assert queue.publish("hello") is False
        assert decode_client.llen(queue.key.pending) == 1

    def test_visibility_timeout_lease_and_ack(self, decode_client):
        queue = RedisMessageQueue(
            "test",
            client=decode_client,
            deduplication=False,
            visibility_timeout_seconds=30,
            heartbeat_interval_seconds=5,
        )
        queue.publish("hello-vt")

        with queue.process_message() as msg:
            assert msg == "hello-vt"
            assert isinstance(msg, str)

        assert decode_client.llen(queue.key.pending) == 0
        assert decode_client.llen(queue.key.processing) == 0


class TestAsyncDecodeResponses:
    @pytest.mark.asyncio
    async def test_publish_process_ack_yields_str(self, async_decode_client):
        queue = AsyncRedisMessageQueue("test", client=async_decode_client, deduplication=False)
        await queue.publish("hello")

        async with queue.process_message() as msg:
            assert msg == "hello"
            assert isinstance(msg, str)

    @pytest.mark.asyncio
    async def test_dedup_rejects_duplicate(self, async_decode_client):
        queue = AsyncRedisMessageQueue(
            "test",
            client=async_decode_client,
            deduplication=True,
            get_deduplication_key=lambda msg: msg,
        )

        assert await queue.publish("hello") is True
        assert await queue.publish("hello") is False
        assert await async_decode_client.llen(queue.key.pending) == 1

    @pytest.mark.asyncio
    async def test_visibility_timeout_lease_and_ack(self, async_decode_client):
        queue = AsyncRedisMessageQueue(
            "test",
            client=async_decode_client,
            deduplication=False,
            visibility_timeout_seconds=30,
            heartbeat_interval_seconds=5,
        )
        await queue.publish("hello-vt")

        async with queue.process_message() as msg:
            assert msg == "hello-vt"
            assert isinstance(msg, str)

        assert await async_decode_client.llen(queue.key.pending) == 0
        assert await async_decode_client.llen(queue.key.processing) == 0
