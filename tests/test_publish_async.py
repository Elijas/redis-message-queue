import fakeredis
import pytest

from redis_message_queue.asyncio.redis_message_queue import RedisMessageQueue


@pytest.fixture
def redis_client():
    return fakeredis.FakeAsyncRedis()


@pytest.fixture
def queue(redis_client):
    return RedisMessageQueue("test-queue", client=redis_client, deduplication=True)


@pytest.fixture
def queue_no_dedup(redis_client):
    return RedisMessageQueue("test-queue", client=redis_client, deduplication=False)


class TestPublishWithDeduplication:
    @pytest.mark.asyncio
    async def test_publish_enqueues_message(self, queue, redis_client):
        result = await queue.publish("hello")

        assert result is True
        assert await redis_client.llen(queue.key.pending) == 1
        assert await redis_client.lpop(queue.key.pending) == b"hello"

    @pytest.mark.asyncio
    async def test_publish_sets_dedup_key(self, queue, redis_client):
        await queue.publish("hello")

        dedup_key = queue.key.deduplication("hello")
        assert await redis_client.exists(dedup_key)

    @pytest.mark.asyncio
    async def test_publish_rejects_duplicate(self, queue):
        first = await queue.publish("hello")
        second = await queue.publish("hello")

        assert first is True
        assert second is False

    @pytest.mark.asyncio
    async def test_duplicate_not_enqueued(self, queue, redis_client):
        await queue.publish("hello")
        await queue.publish("hello")

        assert await redis_client.llen(queue.key.pending) == 1

    @pytest.mark.asyncio
    async def test_different_messages_both_enqueued(self, queue, redis_client):
        await queue.publish("hello")
        await queue.publish("world")

        assert await redis_client.llen(queue.key.pending) == 2

    @pytest.mark.asyncio
    async def test_publish_dict_message(self, queue, redis_client):
        result = await queue.publish({"key": "value"})

        assert result is True
        assert await redis_client.llen(queue.key.pending) == 1

    @pytest.mark.asyncio
    async def test_publish_dict_dedup_rejects_duplicate(self, queue):
        first = await queue.publish({"key": "value"})
        second = await queue.publish({"key": "value"})

        assert first is True
        assert second is False

    @pytest.mark.asyncio
    async def test_dedup_key_has_ttl(self, queue, redis_client):
        await queue.publish("hello")

        dedup_key = queue.key.deduplication("hello")
        ttl = await redis_client.ttl(dedup_key)
        assert ttl > 0

    @pytest.mark.asyncio
    async def test_atomicity_dedup_key_and_message_consistent(self, queue, redis_client):
        """If dedup key is set, the message must also be in the queue."""
        await queue.publish("hello")

        dedup_key = queue.key.deduplication("hello")
        assert await redis_client.exists(dedup_key)
        assert await redis_client.llen(queue.key.pending) == 1


class TestPublishWithoutDeduplication:
    @pytest.mark.asyncio
    async def test_publish_enqueues_message(self, queue_no_dedup, redis_client):
        result = await queue_no_dedup.publish("hello")

        assert result is True
        assert await redis_client.llen(queue_no_dedup.key.pending) == 1

    @pytest.mark.asyncio
    async def test_publish_allows_duplicates(self, queue_no_dedup, redis_client):
        await queue_no_dedup.publish("hello")
        await queue_no_dedup.publish("hello")

        assert await redis_client.llen(queue_no_dedup.key.pending) == 2

    @pytest.mark.asyncio
    async def test_no_dedup_key_set(self, queue_no_dedup, redis_client):
        await queue_no_dedup.publish("hello")

        dedup_key = queue_no_dedup.key.deduplication("hello")
        assert not await redis_client.exists(dedup_key)


class TestPublishDictKeyOrdering:
    @pytest.mark.asyncio
    async def test_dicts_with_different_key_order_are_deduplicated(self, redis_client):
        """Logically equal dicts must be treated as duplicates regardless of insertion order."""
        queue = RedisMessageQueue("test-queue", client=redis_client, deduplication=True)

        first = await queue.publish({"b": 2, "a": 1})
        second = await queue.publish({"a": 1, "b": 2})

        assert first is True
        assert second is False
        assert await redis_client.llen(queue.key.pending) == 1

    @pytest.mark.asyncio
    async def test_dicts_with_different_key_order_store_canonical_json(self, redis_client):
        """The stored message should be in canonical (sorted-key) JSON form."""
        queue = RedisMessageQueue("test-queue", client=redis_client, deduplication=True)

        await queue.publish({"b": 2, "a": 1})

        stored = await redis_client.lpop(queue.key.pending)
        assert stored == b'{"a": 1, "b": 2}'


class TestPublishDedupDisabledIgnoresCustomKey:
    @pytest.mark.asyncio
    async def test_custom_dedup_function_not_called_when_dedup_disabled(self, redis_client):
        """When deduplication=False, the custom dedup function must not be called at all."""
        def failing_dedup(msg):
            raise RuntimeError("Should not be called")

        queue = RedisMessageQueue(
            "test-queue",
            client=redis_client,
            deduplication=False,
            get_deduplication_key=failing_dedup,
        )

        result = await queue.publish("hello")
        assert result is True
        assert await redis_client.llen(queue.key.pending) == 1


class TestPublishFalsyCustomDedupKey:
    @pytest.mark.asyncio
    async def test_falsy_callable_dedup_function_is_used(self, redis_client):
        """A get_deduplication_key that is falsy (e.g. __bool__=False) but not
        None must still be called — it was explicitly provided."""

        class FalsyDedup:
            def __bool__(self):
                return False

            def __call__(self, msg):
                return msg["id"]

        queue = RedisMessageQueue(
            "test-queue",
            client=redis_client,
            deduplication=True,
            get_deduplication_key=FalsyDedup(),
        )

        first = await queue.publish({"id": "same", "data": "first"})
        second = await queue.publish({"id": "same", "data": "second"})

        assert first is True
        assert second is False
        assert await redis_client.llen(queue.key.pending) == 1


class TestPublishDedupKeyTypeValidation:
    @pytest.mark.asyncio
    async def test_dedup_key_returning_none_raises_type_error(self, redis_client):
        queue = RedisMessageQueue(
            "test-queue",
            client=redis_client,
            deduplication=True,
            get_deduplication_key=lambda msg: msg.get("id"),
        )
        with pytest.raises(TypeError, match="must return a string"):
            await queue.publish({"data": "no id field"})

    @pytest.mark.asyncio
    async def test_dedup_key_returning_int_raises_type_error(self, redis_client):
        queue = RedisMessageQueue(
            "test-queue",
            client=redis_client,
            deduplication=True,
            get_deduplication_key=lambda msg: 42,
        )
        with pytest.raises(TypeError, match="must return a string"):
            await queue.publish({"data": "value"})

    @pytest.mark.asyncio
    async def test_dedup_key_returning_bytes_raises_type_error(self, redis_client):
        queue = RedisMessageQueue(
            "test-queue",
            client=redis_client,
            deduplication=True,
            get_deduplication_key=lambda msg: b"key",
        )
        with pytest.raises(TypeError, match="must return a string"):
            await queue.publish({"data": "value"})

    @pytest.mark.asyncio
    async def test_dedup_key_returning_empty_string_is_accepted(self, redis_client):
        queue = RedisMessageQueue(
            "test-queue",
            client=redis_client,
            deduplication=True,
            get_deduplication_key=lambda msg: "",
        )
        result = await queue.publish({"data": "value"})
        assert result is True

    @pytest.mark.asyncio
    async def test_no_message_enqueued_when_dedup_key_invalid(self, redis_client):
        queue = RedisMessageQueue(
            "test-queue",
            client=redis_client,
            deduplication=True,
            get_deduplication_key=lambda msg: None,
        )
        with pytest.raises(TypeError):
            await queue.publish({"data": "value"})
        assert await redis_client.llen(queue.key.pending) == 0


class TestPublishWithCustomDedupKey:
    @pytest.mark.asyncio
    async def test_custom_dedup_key_used(self, redis_client):
        queue = RedisMessageQueue(
            "test-queue",
            client=redis_client,
            deduplication=True,
            get_deduplication_key=lambda msg: msg["id"],
        )

        result = await queue.publish({"id": "abc", "data": "first"})
        assert result is True

        result = await queue.publish({"id": "abc", "data": "second"})
        assert result is False

        assert await redis_client.llen(queue.key.pending) == 1

    @pytest.mark.asyncio
    async def test_different_custom_keys_both_enqueued(self, redis_client):
        queue = RedisMessageQueue(
            "test-queue",
            client=redis_client,
            deduplication=True,
            get_deduplication_key=lambda msg: msg["id"],
        )

        await queue.publish({"id": "abc", "data": "first"})
        await queue.publish({"id": "def", "data": "second"})

        assert await redis_client.llen(queue.key.pending) == 2
