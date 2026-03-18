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
        with queue.process_message() as message:
            assert message == b"hello"

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
        assert ttl == 3600

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

    def test_duplicate_payloads_use_distinct_stored_entries(self, queue_no_dedup, redis_client):
        queue_no_dedup.publish("hello")
        queue_no_dedup.publish("hello")

        raw_entries = redis_client.lrange(queue_no_dedup.key.pending, 0, -1)
        assert len(raw_entries) == 2
        assert raw_entries[0] != raw_entries[1]


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

        with queue.process_message() as message:
            assert message == b'{"a": 1, "b": 2}'


class TestCompletedQueueLogsPayload:
    def test_completed_queue_stores_plain_payload(self, redis_client):
        queue = RedisMessageQueue(
            "test-queue",
            client=redis_client,
            deduplication=False,
            enable_completed_queue=True,
        )

        queue.publish("hello")

        with queue.process_message() as message:
            assert message == b"hello"

        assert redis_client.lpop(queue.key.completed) == b"hello"


class TestPublishDedupDisabledRejectsCustomKey:
    def test_raises_when_dedup_disabled_with_custom_key(self, redis_client):
        """Providing get_deduplication_key when deduplication=False is contradictory and must raise."""

        def failing_dedup(msg):
            raise RuntimeError("Should not be called")

        with pytest.raises(ValueError, match="'get_deduplication_key' cannot be provided"):
            RedisMessageQueue(
                "test-queue",
                client=redis_client,
                deduplication=False,
                get_deduplication_key=failing_dedup,
            )


class TestPublishFalsyCustomDedupKey:
    def test_falsy_callable_dedup_function_is_used(self, redis_client):
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

        first = queue.publish({"id": "same", "data": "first"})
        second = queue.publish({"id": "same", "data": "second"})

        assert first is True
        assert second is False
        assert redis_client.llen(queue.key.pending) == 1


class TestPublishDedupKeyTypeValidation:
    def test_dedup_key_returning_none_raises_type_error(self, redis_client):
        queue = RedisMessageQueue(
            "test-queue",
            client=redis_client,
            deduplication=True,
            get_deduplication_key=lambda msg: msg.get("id"),
        )
        with pytest.raises(TypeError, match="must return a string"):
            queue.publish({"data": "no id field"})

    def test_dedup_key_returning_int_raises_type_error(self, redis_client):
        queue = RedisMessageQueue(
            "test-queue",
            client=redis_client,
            deduplication=True,
            get_deduplication_key=lambda msg: 42,
        )
        with pytest.raises(TypeError, match="must return a string"):
            queue.publish({"data": "value"})

    def test_dedup_key_returning_bytes_raises_type_error(self, redis_client):
        queue = RedisMessageQueue(
            "test-queue",
            client=redis_client,
            deduplication=True,
            get_deduplication_key=lambda msg: b"key",
        )
        with pytest.raises(TypeError, match="must return a string"):
            queue.publish({"data": "value"})

    def test_dedup_key_returning_empty_string_is_accepted(self, redis_client):
        queue = RedisMessageQueue(
            "test-queue",
            client=redis_client,
            deduplication=True,
            get_deduplication_key=lambda msg: "",
        )
        result = queue.publish({"data": "value"})
        assert result is True

    def test_no_message_enqueued_when_dedup_key_invalid(self, redis_client):
        queue = RedisMessageQueue(
            "test-queue",
            client=redis_client,
            deduplication=True,
            get_deduplication_key=lambda msg: None,
        )
        with pytest.raises(TypeError):
            queue.publish({"data": "value"})
        assert redis_client.llen(queue.key.pending) == 0


class TestPublishMessageTypeValidation:
    @pytest.mark.parametrize("invalid_message", [42, b"hello", None, [1, 2], 3.14, True])
    def test_non_str_non_dict_message_raises_type_error(self, queue, invalid_message):
        with pytest.raises(TypeError, match="must be a str or dict"):
            queue.publish(invalid_message)

    @pytest.mark.parametrize("invalid_message", [42, b"hello", None, [1, 2], 3.14, True])
    def test_no_message_enqueued_with_dedup(self, queue, redis_client, invalid_message):
        with pytest.raises(TypeError):
            queue.publish(invalid_message)
        assert redis_client.llen(queue.key.pending) == 0

    @pytest.mark.parametrize("invalid_message", [42, b"hello", None, [1, 2], 3.14, True])
    def test_non_str_non_dict_message_raises_without_dedup(self, queue_no_dedup, invalid_message):
        with pytest.raises(TypeError, match="must be a str or dict"):
            queue_no_dedup.publish(invalid_message)

    @pytest.mark.parametrize("invalid_message", [42, b"hello", None, [1, 2], 3.14, True])
    def test_no_message_enqueued_without_dedup(self, queue_no_dedup, redis_client, invalid_message):
        with pytest.raises(TypeError):
            queue_no_dedup.publish(invalid_message)
        assert redis_client.llen(queue_no_dedup.key.pending) == 0


class TestPublishDedupKeyException:
    def test_exception_in_dedup_function_propagates(self, redis_client):
        queue = RedisMessageQueue(
            "test-queue",
            client=redis_client,
            deduplication=True,
            get_deduplication_key=lambda msg: (_ for _ in ()).throw(RuntimeError("boom")),
        )
        with pytest.raises(RuntimeError, match="boom"):
            queue.publish({"data": "value"})

    def test_no_message_enqueued_when_dedup_function_raises(self, redis_client):
        queue = RedisMessageQueue(
            "test-queue",
            client=redis_client,
            deduplication=True,
            get_deduplication_key=lambda msg: (_ for _ in ()).throw(RuntimeError("boom")),
        )
        with pytest.raises(RuntimeError):
            queue.publish({"data": "value"})
        assert redis_client.llen(queue.key.pending) == 0


class TestPublishSyncRejectsAsyncDedupKey:
    def test_async_dedup_function_raises_type_error(self, redis_client):
        async def async_dedup(msg):
            return msg["id"]

        queue = RedisMessageQueue(
            "test-queue",
            client=redis_client,
            deduplication=True,
            get_deduplication_key=async_dedup,
        )
        with pytest.raises(TypeError, match="returned a coroutine.*async RedisMessageQueue"):
            queue.publish({"id": "abc", "data": "value"})

    def test_async_callable_class_raises_type_error(self, redis_client):
        class AsyncDedup:
            async def __call__(self, msg):
                return msg["id"]

        queue = RedisMessageQueue(
            "test-queue",
            client=redis_client,
            deduplication=True,
            get_deduplication_key=AsyncDedup(),
        )
        with pytest.raises(TypeError, match="returned a coroutine.*async RedisMessageQueue"):
            queue.publish({"id": "abc", "data": "value"})

    def test_no_message_enqueued_when_async_dedup_key_used(self, redis_client):
        async def async_dedup(msg):
            return msg["id"]

        queue = RedisMessageQueue(
            "test-queue",
            client=redis_client,
            deduplication=True,
            get_deduplication_key=async_dedup,
        )
        with pytest.raises(TypeError):
            queue.publish({"id": "abc", "data": "value"})
        assert redis_client.llen(queue.key.pending) == 0

    def test_custom_awaitable_dedup_key_raises_type_error(self, redis_client):
        class CustomAwaitable:
            def __await__(self):
                if False:
                    yield None
                return "abc"

        queue = RedisMessageQueue(
            "test-queue",
            client=redis_client,
            deduplication=True,
            get_deduplication_key=lambda msg: CustomAwaitable(),
        )

        with pytest.raises(TypeError, match="returned an awaitable.*async RedisMessageQueue"):
            queue.publish({"id": "abc", "data": "value"})

        assert redis_client.llen(queue.key.pending) == 0


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
