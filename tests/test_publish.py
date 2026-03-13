import fakeredis
import pytest

from redis_message_queue.redis_message_queue import RedisMessageQueue


@pytest.fixture
def redis_client():
    return fakeredis.FakeRedis()


@pytest.fixture
def queue(redis_client):
    return RedisMessageQueue("test-queue", client=redis_client, deduplication=True)


@pytest.fixture
def queue_no_dedup(redis_client):
    return RedisMessageQueue("test-queue", client=redis_client, deduplication=False)


class TestPublishWithDeduplication:
    def test_publish_enqueues_message(self, queue, redis_client):
        result = queue.publish("hello")

        assert result is True
        assert redis_client.llen(queue.key.pending) == 1
        assert redis_client.lpop(queue.key.pending) == b"hello"

    def test_publish_sets_dedup_key(self, queue, redis_client):
        queue.publish("hello")

        dedup_key = queue.key.deduplication("hello")
        assert redis_client.exists(dedup_key)

    def test_publish_rejects_duplicate(self, queue):
        first = queue.publish("hello")
        second = queue.publish("hello")

        assert first is True
        assert second is False

    def test_duplicate_not_enqueued(self, queue, redis_client):
        queue.publish("hello")
        queue.publish("hello")

        assert redis_client.llen(queue.key.pending) == 1

    def test_different_messages_both_enqueued(self, queue, redis_client):
        queue.publish("hello")
        queue.publish("world")

        assert redis_client.llen(queue.key.pending) == 2

    def test_publish_dict_message(self, queue, redis_client):
        result = queue.publish({"key": "value"})

        assert result is True
        assert redis_client.llen(queue.key.pending) == 1

    def test_publish_dict_dedup_rejects_duplicate(self, queue):
        first = queue.publish({"key": "value"})
        second = queue.publish({"key": "value"})

        assert first is True
        assert second is False

    def test_dedup_key_has_ttl(self, queue, redis_client):
        queue.publish("hello")

        dedup_key = queue.key.deduplication("hello")
        ttl = redis_client.ttl(dedup_key)
        assert ttl > 0

    def test_atomicity_dedup_key_and_message_consistent(self, queue, redis_client):
        """If dedup key is set, the message must also be in the queue."""
        queue.publish("hello")

        dedup_key = queue.key.deduplication("hello")
        assert redis_client.exists(dedup_key)
        assert redis_client.llen(queue.key.pending) == 1


class TestPublishWithoutDeduplication:
    def test_publish_enqueues_message(self, queue_no_dedup, redis_client):
        result = queue_no_dedup.publish("hello")

        assert result is True
        assert redis_client.llen(queue_no_dedup.key.pending) == 1

    def test_publish_allows_duplicates(self, queue_no_dedup, redis_client):
        queue_no_dedup.publish("hello")
        queue_no_dedup.publish("hello")

        assert redis_client.llen(queue_no_dedup.key.pending) == 2

    def test_no_dedup_key_set(self, queue_no_dedup, redis_client):
        queue_no_dedup.publish("hello")

        dedup_key = queue_no_dedup.key.deduplication("hello")
        assert not redis_client.exists(dedup_key)


class TestPublishDictKeyOrdering:
    def test_dicts_with_different_key_order_are_deduplicated(self, redis_client):
        """Logically equal dicts must be treated as duplicates regardless of insertion order."""
        queue = RedisMessageQueue("test-queue", client=redis_client, deduplication=True)

        first = queue.publish({"b": 2, "a": 1})
        second = queue.publish({"a": 1, "b": 2})

        assert first is True
        assert second is False
        assert redis_client.llen(queue.key.pending) == 1

    def test_dicts_with_different_key_order_store_canonical_json(self, redis_client):
        """The stored message should be in canonical (sorted-key) JSON form."""
        queue = RedisMessageQueue("test-queue", client=redis_client, deduplication=True)

        queue.publish({"b": 2, "a": 1})

        stored = redis_client.lpop(queue.key.pending)
        assert stored == b'{"a": 1, "b": 2}'


class TestPublishDedupDisabledIgnoresCustomKey:
    def test_custom_dedup_function_not_called_when_dedup_disabled(self, redis_client):
        """When deduplication=False, the custom dedup function must not be called at all."""
        def failing_dedup(msg):
            raise RuntimeError("Should not be called")

        queue = RedisMessageQueue(
            "test-queue",
            client=redis_client,
            deduplication=False,
            get_deduplication_key=failing_dedup,
        )

        result = queue.publish("hello")
        assert result is True
        assert redis_client.llen(queue.key.pending) == 1


class TestPublishWithCustomDedupKey:
    def test_custom_dedup_key_used(self, redis_client):
        queue = RedisMessageQueue(
            "test-queue",
            client=redis_client,
            deduplication=True,
            get_deduplication_key=lambda msg: msg["id"],
        )

        result = queue.publish({"id": "abc", "data": "first"})
        assert result is True

        result = queue.publish({"id": "abc", "data": "second"})
        assert result is False

        assert redis_client.llen(queue.key.pending) == 1

    def test_different_custom_keys_both_enqueued(self, redis_client):
        queue = RedisMessageQueue(
            "test-queue",
            client=redis_client,
            deduplication=True,
            get_deduplication_key=lambda msg: msg["id"],
        )

        queue.publish({"id": "abc", "data": "first"})
        queue.publish({"id": "def", "data": "second"})

        assert redis_client.llen(queue.key.pending) == 2
