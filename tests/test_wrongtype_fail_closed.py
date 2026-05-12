import asyncio
import time

import fakeredis
import pytest
import redis.exceptions

from redis_message_queue import RedisMessageQueue
from redis_message_queue.asyncio import RedisMessageQueue as AsyncRedisMessageQueue


class TestSyncWrongTypeFailClosed:
    def test_publish_does_not_leave_dedup_key_when_pending_queue_has_wrong_type(self):
        client = fakeredis.FakeRedis()
        queue = RedisMessageQueue("test", client=client)
        client.set(queue.key.pending, "not-a-list")

        with pytest.raises(redis.exceptions.ResponseError, match="WRONGTYPE"):
            queue.publish("hello")

        assert client.exists(queue.key.deduplication("hello")) == 0
        assert client.get(queue.key.pending) == b"not-a-list"

    def test_completed_queue_wrong_type_keeps_message_in_processing_without_visibility_timeout(self):
        client = fakeredis.FakeRedis()
        queue = RedisMessageQueue("test", client=client, enable_completed_queue=True)
        assert queue.publish("hello") is True
        client.set(queue.key.completed, "not-a-list")

        with pytest.raises(redis.exceptions.ResponseError, match="WRONGTYPE"):
            with queue.process_message() as message:
                assert message == b"hello"

        assert client.llen(queue.key.pending) == 0
        assert client.llen(queue.key.processing) == 1
        assert client.get(queue.key.completed) == b"not-a-list"

    def test_completed_queue_wrong_type_keeps_message_in_processing_with_visibility_timeout(self):
        client = fakeredis.FakeRedis()
        queue = RedisMessageQueue(
            "test",
            client=client,
            enable_completed_queue=True,
            visibility_timeout_seconds=30,
        )
        assert queue.publish("hello") is True
        client.set(queue.key.completed, "not-a-list")

        with pytest.raises(redis.exceptions.ResponseError, match="WRONGTYPE"):
            with queue.process_message() as message:
                assert message == b"hello"

        assert client.llen(queue.key.pending) == 0
        assert client.llen(queue.key.processing) == 1
        assert client.zcard(f"{queue.key.processing}:lease_deadlines") == 1
        assert client.hlen(f"{queue.key.processing}:lease_tokens") == 1
        assert client.get(queue.key.completed) == b"not-a-list"

    def test_reclaim_wrong_type_pending_queue_keeps_expired_message_in_processing(self):
        client = fakeredis.FakeRedis()
        queue = RedisMessageQueue("test", client=client, visibility_timeout_seconds=1)
        assert queue.publish("hello") is True

        with pytest.raises(SystemExit, match="simulate crash"):
            with queue.process_message() as message:
                assert message == b"hello"
                raise SystemExit("simulate crash")

        client.set(queue.key.pending, "not-a-list")
        time.sleep(1.1)

        with pytest.raises(redis.exceptions.ResponseError, match="WRONGTYPE"):
            with queue.process_message():
                pass

        assert client.get(queue.key.pending) == b"not-a-list"
        assert client.llen(queue.key.processing) == 1
        assert client.zcard(f"{queue.key.processing}:lease_deadlines") == 1
        assert client.hlen(f"{queue.key.processing}:lease_tokens") == 1

    def test_dead_letter_wrong_type_keeps_poison_message_in_processing(self):
        client = fakeredis.FakeRedis()
        queue = RedisMessageQueue(
            "test",
            client=client,
            visibility_timeout_seconds=1,
            max_delivery_count=1,
        )
        assert queue.publish("poison") is True

        with pytest.raises(SystemExit, match="simulate crash"):
            with queue.process_message() as message:
                assert message == b"poison"
                raise SystemExit("simulate crash")

        dead_letter_queue = queue._redis.dead_letter_queue
        client.set(dead_letter_queue, "not-a-list")
        time.sleep(1.1)

        with pytest.raises(redis.exceptions.ResponseError, match="WRONGTYPE"):
            with queue.process_message():
                pass

        assert client.get(dead_letter_queue) == b"not-a-list"
        assert client.llen(queue.key.processing) == 1
        assert client.zcard(f"{queue.key.processing}:lease_deadlines") == 1
        assert client.hlen(f"{queue.key.processing}:lease_tokens") == 1
        assert client.hget(f"{queue.key.processing}:delivery_counts", client.lindex(queue.key.processing, 0)) == b"1"

    def test_claim_wrong_type_processing_queue_keeps_message_in_pending(self):
        client = fakeredis.FakeRedis()
        queue = RedisMessageQueue("test", client=client)
        assert queue.publish("hello") is True
        client.set(queue.key.processing, "not-a-list")

        with pytest.raises(redis.exceptions.ResponseError, match="WRONGTYPE"):
            with queue.process_message():
                pass

        assert client.llen(queue.key.pending) == 1
        assert client.get(queue.key.processing) == b"not-a-list"

    def test_remove_wrong_type_processing_queue_without_visibility_timeout(self):
        client = fakeredis.FakeRedis()
        queue = RedisMessageQueue("test", client=client)
        assert queue.publish("hello") is True

        with pytest.raises(redis.exceptions.ResponseError, match="WRONGTYPE"):
            with queue.process_message() as message:
                assert message == b"hello"
                client.set(queue.key.processing, "not-a-list")

        assert client.get(queue.key.processing) == b"not-a-list"

    def test_remove_wrong_type_processing_queue_with_visibility_timeout(self):
        client = fakeredis.FakeRedis()
        queue = RedisMessageQueue("test", client=client, visibility_timeout_seconds=30)
        assert queue.publish("hello") is True

        with pytest.raises(redis.exceptions.ResponseError, match="WRONGTYPE"):
            with queue.process_message() as message:
                assert message == b"hello"
                client.set(queue.key.processing, "not-a-list")

        assert client.get(queue.key.processing) == b"not-a-list"
        assert client.hlen(f"{queue.key.processing}:lease_tokens") == 1
        assert client.zcard(f"{queue.key.processing}:lease_deadlines") == 1

    def test_renew_lease_wrong_type_lease_deadlines(self):
        client = fakeredis.FakeRedis()
        queue = RedisMessageQueue("test", client=client, visibility_timeout_seconds=30)
        assert queue.publish("hello") is True

        with pytest.raises(SystemExit, match="simulate crash"):
            with queue.process_message() as message:
                assert message == b"hello"
                raise SystemExit("simulate crash")

        assert client.llen(queue.key.processing) == 1
        stored_message = client.lindex(queue.key.processing, 0)
        lease_token = client.hget(f"{queue.key.processing}:lease_tokens", stored_message)
        client.set(f"{queue.key.processing}:lease_deadlines", "not-a-zset")

        with pytest.raises(redis.exceptions.ResponseError, match="WRONGTYPE"):
            queue._redis.renew_message_lease(queue.key.processing, stored_message, lease_token.decode())

        assert client.get(f"{queue.key.processing}:lease_deadlines") == b"not-a-zset"
        assert client.llen(queue.key.processing) == 1
        assert client.hget(f"{queue.key.processing}:lease_tokens", stored_message) == lease_token


class TestAsyncWrongTypeFailClosed:
    @pytest.mark.asyncio
    async def test_publish_does_not_leave_dedup_key_when_pending_queue_has_wrong_type(self):
        client = fakeredis.FakeAsyncRedis()
        queue = AsyncRedisMessageQueue("test", client=client)
        await client.set(queue.key.pending, "not-a-list")

        with pytest.raises(redis.exceptions.ResponseError, match="WRONGTYPE"):
            await queue.publish("hello")

        assert await client.exists(queue.key.deduplication("hello")) == 0
        assert await client.get(queue.key.pending) == b"not-a-list"

    @pytest.mark.asyncio
    async def test_completed_queue_wrong_type_keeps_message_in_processing_without_visibility_timeout(self):
        client = fakeredis.FakeAsyncRedis()
        queue = AsyncRedisMessageQueue("test", client=client, enable_completed_queue=True)
        assert await queue.publish("hello") is True
        await client.set(queue.key.completed, "not-a-list")

        with pytest.raises(redis.exceptions.ResponseError, match="WRONGTYPE"):
            async with queue.process_message() as message:
                assert message == b"hello"

        assert await client.llen(queue.key.pending) == 0
        assert await client.llen(queue.key.processing) == 1
        assert await client.get(queue.key.completed) == b"not-a-list"

    @pytest.mark.asyncio
    async def test_completed_queue_wrong_type_keeps_message_in_processing_with_visibility_timeout(self):
        client = fakeredis.FakeAsyncRedis()
        queue = AsyncRedisMessageQueue(
            "test",
            client=client,
            enable_completed_queue=True,
            visibility_timeout_seconds=30,
        )
        assert await queue.publish("hello") is True
        await client.set(queue.key.completed, "not-a-list")

        with pytest.raises(redis.exceptions.ResponseError, match="WRONGTYPE"):
            async with queue.process_message() as message:
                assert message == b"hello"

        assert await client.llen(queue.key.pending) == 0
        assert await client.llen(queue.key.processing) == 1
        assert await client.zcard(f"{queue.key.processing}:lease_deadlines") == 1
        assert await client.hlen(f"{queue.key.processing}:lease_tokens") == 1
        assert await client.get(queue.key.completed) == b"not-a-list"

    @pytest.mark.asyncio
    async def test_reclaim_wrong_type_pending_queue_keeps_expired_message_in_processing(self):
        client = fakeredis.FakeAsyncRedis()
        queue = AsyncRedisMessageQueue("test", client=client, visibility_timeout_seconds=1)
        assert await queue.publish("hello") is True

        with pytest.raises(SystemExit, match="simulate crash"):
            async with queue.process_message() as message:
                assert message == b"hello"
                raise SystemExit("simulate crash")

        await client.set(queue.key.pending, "not-a-list")
        await asyncio.sleep(1.1)

        with pytest.raises(redis.exceptions.ResponseError, match="WRONGTYPE"):
            async with queue.process_message():
                pass

        assert await client.get(queue.key.pending) == b"not-a-list"
        assert await client.llen(queue.key.processing) == 1
        assert await client.zcard(f"{queue.key.processing}:lease_deadlines") == 1
        assert await client.hlen(f"{queue.key.processing}:lease_tokens") == 1

    @pytest.mark.asyncio
    async def test_dead_letter_wrong_type_keeps_poison_message_in_processing(self):
        client = fakeredis.FakeAsyncRedis()
        queue = AsyncRedisMessageQueue(
            "test",
            client=client,
            visibility_timeout_seconds=1,
            max_delivery_count=1,
        )
        assert await queue.publish("poison") is True

        with pytest.raises(SystemExit, match="simulate crash"):
            async with queue.process_message() as message:
                assert message == b"poison"
                raise SystemExit("simulate crash")

        dead_letter_queue = queue._redis.dead_letter_queue
        await client.set(dead_letter_queue, "not-a-list")
        await asyncio.sleep(1.1)

        with pytest.raises(redis.exceptions.ResponseError, match="WRONGTYPE"):
            async with queue.process_message():
                pass

        assert await client.get(dead_letter_queue) == b"not-a-list"
        assert await client.llen(queue.key.processing) == 1
        assert await client.zcard(f"{queue.key.processing}:lease_deadlines") == 1
        assert await client.hlen(f"{queue.key.processing}:lease_tokens") == 1
        assert (
            await client.hget(
                f"{queue.key.processing}:delivery_counts",
                await client.lindex(queue.key.processing, 0),
            )
            == b"1"
        )

    @pytest.mark.asyncio
    async def test_claim_wrong_type_processing_queue_keeps_message_in_pending(self):
        client = fakeredis.FakeAsyncRedis()
        queue = AsyncRedisMessageQueue("test", client=client)
        assert await queue.publish("hello") is True
        await client.set(queue.key.processing, "not-a-list")

        with pytest.raises(redis.exceptions.ResponseError, match="WRONGTYPE"):
            async with queue.process_message():
                pass

        assert await client.llen(queue.key.pending) == 1
        assert await client.get(queue.key.processing) == b"not-a-list"

    @pytest.mark.asyncio
    async def test_remove_wrong_type_processing_queue_without_visibility_timeout(self):
        client = fakeredis.FakeAsyncRedis()
        queue = AsyncRedisMessageQueue("test", client=client)
        assert await queue.publish("hello") is True

        with pytest.raises(redis.exceptions.ResponseError, match="WRONGTYPE"):
            async with queue.process_message() as message:
                assert message == b"hello"
                await client.set(queue.key.processing, "not-a-list")

        assert await client.get(queue.key.processing) == b"not-a-list"

    @pytest.mark.asyncio
    async def test_remove_wrong_type_processing_queue_with_visibility_timeout(self):
        client = fakeredis.FakeAsyncRedis()
        queue = AsyncRedisMessageQueue("test", client=client, visibility_timeout_seconds=30)
        assert await queue.publish("hello") is True

        with pytest.raises(redis.exceptions.ResponseError, match="WRONGTYPE"):
            async with queue.process_message() as message:
                assert message == b"hello"
                await client.set(queue.key.processing, "not-a-list")

        assert await client.get(queue.key.processing) == b"not-a-list"
        assert await client.hlen(f"{queue.key.processing}:lease_tokens") == 1
        assert await client.zcard(f"{queue.key.processing}:lease_deadlines") == 1

    @pytest.mark.asyncio
    async def test_renew_lease_wrong_type_lease_deadlines(self):
        client = fakeredis.FakeAsyncRedis()
        queue = AsyncRedisMessageQueue("test", client=client, visibility_timeout_seconds=30)
        assert await queue.publish("hello") is True

        with pytest.raises(SystemExit, match="simulate crash"):
            async with queue.process_message() as message:
                assert message == b"hello"
                raise SystemExit("simulate crash")

        assert await client.llen(queue.key.processing) == 1
        stored_message = await client.lindex(queue.key.processing, 0)
        lease_token = await client.hget(f"{queue.key.processing}:lease_tokens", stored_message)
        await client.set(f"{queue.key.processing}:lease_deadlines", "not-a-zset")

        with pytest.raises(redis.exceptions.ResponseError, match="WRONGTYPE"):
            await queue._redis.renew_message_lease(queue.key.processing, stored_message, lease_token.decode())

        assert await client.get(f"{queue.key.processing}:lease_deadlines") == b"not-a-zset"
        assert await client.llen(queue.key.processing) == 1
        assert await client.hget(f"{queue.key.processing}:lease_tokens", stored_message) == lease_token
