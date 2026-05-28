import time

import fakeredis
import pytest

from redis_message_queue import ConfigurationError, EventOperation, QueueEvent
from redis_message_queue._redis_gateway import RedisGateway
from redis_message_queue._stored_message import extract_stored_message_id
from redis_message_queue.asyncio._redis_gateway import RedisGateway as AsyncRedisGateway
from redis_message_queue.asyncio.redis_message_queue import RedisMessageQueue as AsyncRedisMessageQueue
from redis_message_queue.redis_message_queue import RedisMessageQueue

# ---------------------------------------------------------------------------
# Constructor validation
# ---------------------------------------------------------------------------


class TestConstructorMaxDeliveryCountValidation:
    """Sync constructor validation for max_delivery_count."""

    @pytest.mark.parametrize("invalid_value", ["3", 1.5, True, False, [3]])
    def test_non_int_raises_type_error(self, invalid_value):
        client = fakeredis.FakeRedis()
        with pytest.raises(TypeError, match="'max_delivery_count' must be an int or None"):
            RedisMessageQueue(
                "test",
                client=client,
                visibility_timeout_seconds=300,
                max_delivery_count=invalid_value,
            )

    @pytest.mark.parametrize("invalid_value", [0, -1, -100])
    def test_non_positive_raises_value_error(self, invalid_value):
        client = fakeredis.FakeRedis()
        with pytest.raises(ValueError, match="'max_delivery_count' must be positive"):
            RedisMessageQueue(
                "test",
                client=client,
                visibility_timeout_seconds=300,
                max_delivery_count=invalid_value,
            )

    def test_without_visibility_timeout_raises_value_error(self):
        client = fakeredis.FakeRedis()
        with pytest.raises(ValueError, match="requires 'visibility_timeout_seconds'"):
            RedisMessageQueue("test", client=client, visibility_timeout_seconds=None, max_delivery_count=3)

    def test_none_is_accepted(self):
        client = fakeredis.FakeRedis()
        q = RedisMessageQueue(
            "test",
            client=client,
            visibility_timeout_seconds=300,
            max_delivery_count=None,
        )
        assert q._max_delivery_count is None

    def test_default_max_delivery_count_uses_auto_derived_dead_letter_queue(self):
        client = fakeredis.FakeRedis()
        q = RedisMessageQueue("test", client=client)
        assert q._max_delivery_count == 10
        assert q._redis._max_delivery_count == 10
        assert q._redis._dead_letter_queue == "test::dlq"

    def test_positive_int_is_accepted(self):
        client = fakeredis.FakeRedis()
        q = RedisMessageQueue(
            "test",
            client=client,
            visibility_timeout_seconds=300,
            max_delivery_count=5,
        )
        assert q._max_delivery_count == 5
        assert q._redis._dead_letter_queue == "test::dlq"

    def test_with_gateway_raises_value_error(self):
        client = fakeredis.FakeRedis()
        gateway = RedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=300,
            max_delivery_count=3,
            dead_letter_queue="test::dead_letter",
        )
        with pytest.raises(ValueError, match="cannot be provided alongside 'gateway'"):
            RedisMessageQueue(
                "test",
                gateway=gateway,
                max_delivery_count=3,
            )

    def test_gateway_without_max_delivery_count_is_accepted(self):
        client = fakeredis.FakeRedis()
        gateway = RedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=300,
            max_delivery_count=3,
            dead_letter_queue="test::dead_letter",
        )
        q = RedisMessageQueue("test", gateway=gateway)
        assert q._max_delivery_count is None

    def test_gateway_with_max_delivery_count_cannot_be_reused_across_queues(self):
        client = fakeredis.FakeRedis()
        gateway = RedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=300,
            max_delivery_count=3,
            dead_letter_queue="shared-dead-letter",
        )

        RedisMessageQueue("queue-a", gateway=gateway)

        with pytest.raises(ValueError, match="cannot be reused across different queues"):
            RedisMessageQueue("queue-b", gateway=gateway)

    @pytest.mark.parametrize("live_key_suffix", ["pending", "processing"])
    def test_gateway_dead_letter_queue_cannot_alias_live_queue_key(self, live_key_suffix):
        client = fakeredis.FakeRedis()
        gateway = RedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=300,
            max_delivery_count=3,
            dead_letter_queue=f"test::{live_key_suffix}",
        )

        with pytest.raises(ConfigurationError, match="must be distinct"):
            RedisMessageQueue("test", gateway=gateway)

    def test_gateway_without_max_delivery_count_can_be_reused_across_queues(self):
        client = fakeredis.FakeRedis()
        gateway = RedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=300,
        )

        RedisMessageQueue("queue-a", gateway=gateway)
        RedisMessageQueue("queue-b", gateway=gateway)


class TestConstructorMaxDeliveryCountValidationAsync:
    """Async constructor validation for max_delivery_count."""

    @pytest.mark.parametrize("invalid_value", ["3", 1.5, True, False, [3]])
    def test_non_int_raises_type_error(self, invalid_value):
        client = fakeredis.FakeAsyncRedis()
        with pytest.raises(TypeError, match="'max_delivery_count' must be an int or None"):
            AsyncRedisMessageQueue(
                "test",
                client=client,
                visibility_timeout_seconds=300,
                max_delivery_count=invalid_value,
            )

    @pytest.mark.parametrize("invalid_value", [0, -1, -100])
    def test_non_positive_raises_value_error(self, invalid_value):
        client = fakeredis.FakeAsyncRedis()
        with pytest.raises(ValueError, match="'max_delivery_count' must be positive"):
            AsyncRedisMessageQueue(
                "test",
                client=client,
                visibility_timeout_seconds=300,
                max_delivery_count=invalid_value,
            )

    def test_without_visibility_timeout_raises_value_error(self):
        client = fakeredis.FakeAsyncRedis()
        with pytest.raises(ValueError, match="requires 'visibility_timeout_seconds'"):
            AsyncRedisMessageQueue("test", client=client, visibility_timeout_seconds=None, max_delivery_count=3)

    def test_none_is_accepted(self):
        client = fakeredis.FakeAsyncRedis()
        q = AsyncRedisMessageQueue(
            "test",
            client=client,
            visibility_timeout_seconds=300,
            max_delivery_count=None,
        )
        assert q._max_delivery_count is None

    def test_default_max_delivery_count_uses_auto_derived_dead_letter_queue(self):
        client = fakeredis.FakeAsyncRedis()
        q = AsyncRedisMessageQueue("test", client=client)
        assert q._max_delivery_count == 10
        assert q._redis._max_delivery_count == 10
        assert q._redis._dead_letter_queue == "test::dlq"

    def test_positive_int_is_accepted(self):
        client = fakeredis.FakeAsyncRedis()
        q = AsyncRedisMessageQueue(
            "test",
            client=client,
            visibility_timeout_seconds=300,
            max_delivery_count=5,
        )
        assert q._max_delivery_count == 5
        assert q._redis._dead_letter_queue == "test::dlq"

    def test_with_gateway_raises_value_error(self):
        client = fakeredis.FakeAsyncRedis()
        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=300,
            max_delivery_count=3,
            dead_letter_queue="test::dead_letter",
        )
        with pytest.raises(ValueError, match="cannot be provided alongside 'gateway'"):
            AsyncRedisMessageQueue(
                "test",
                gateway=gateway,
                max_delivery_count=3,
            )

    def test_gateway_without_max_delivery_count_is_accepted(self):
        client = fakeredis.FakeAsyncRedis()
        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=300,
            max_delivery_count=3,
            dead_letter_queue="test::dead_letter",
        )
        q = AsyncRedisMessageQueue("test", gateway=gateway)
        assert q._max_delivery_count is None

    def test_gateway_with_max_delivery_count_cannot_be_reused_across_queues(self):
        client = fakeredis.FakeAsyncRedis()
        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=300,
            max_delivery_count=3,
            dead_letter_queue="shared-dead-letter",
        )

        AsyncRedisMessageQueue("queue-a", gateway=gateway)

        with pytest.raises(ValueError, match="cannot be reused across different queues"):
            AsyncRedisMessageQueue("queue-b", gateway=gateway)

    @pytest.mark.parametrize("live_key_suffix", ["pending", "processing"])
    def test_gateway_dead_letter_queue_cannot_alias_live_queue_key(self, live_key_suffix):
        client = fakeredis.FakeAsyncRedis()
        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=300,
            max_delivery_count=3,
            dead_letter_queue=f"test::{live_key_suffix}",
        )

        with pytest.raises(ConfigurationError, match="must be distinct"):
            AsyncRedisMessageQueue("test", gateway=gateway)

    def test_gateway_without_max_delivery_count_can_be_reused_across_queues(self):
        client = fakeredis.FakeAsyncRedis()
        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=300,
        )

        AsyncRedisMessageQueue("queue-a", gateway=gateway)
        AsyncRedisMessageQueue("queue-b", gateway=gateway)


# ---------------------------------------------------------------------------
# Gateway constructor: dead_letter_queue / max_delivery_count cross-validation
# ---------------------------------------------------------------------------


class TestGatewayDeadLetterCrossValidationSync:
    """Sync RedisGateway must require dead_letter_queue when max_delivery_count is set."""

    def test_max_delivery_count_without_dead_letter_queue_raises(self):
        client = fakeredis.FakeRedis()
        with pytest.raises(ValueError, match="'dead_letter_queue' is required"):
            RedisGateway(
                redis_client=client,
                retry_budget_seconds=0,
                message_wait_interval_seconds=0,
                message_visibility_timeout_seconds=300,
                max_delivery_count=3,
                dead_letter_queue=None,
            )

    def test_max_delivery_count_with_empty_dead_letter_queue_raises(self):
        client = fakeredis.FakeRedis()
        with pytest.raises(ValueError, match="'dead_letter_queue' is required"):
            RedisGateway(
                redis_client=client,
                retry_budget_seconds=0,
                message_wait_interval_seconds=0,
                message_visibility_timeout_seconds=300,
                max_delivery_count=3,
                dead_letter_queue="",
            )

    def test_dead_letter_queue_without_max_delivery_count_raises(self):
        client = fakeredis.FakeRedis()
        with pytest.raises(ValueError, match="'max_delivery_count' is required"):
            RedisGateway(
                redis_client=client,
                retry_budget_seconds=0,
                message_wait_interval_seconds=0,
                message_visibility_timeout_seconds=300,
                max_delivery_count=None,
                dead_letter_queue="q::dlq",
            )

    @pytest.mark.parametrize("invalid_value", ["3", 1.5, True, False, [3]])
    def test_invalid_max_delivery_count_type_raises(self, invalid_value):
        client = fakeredis.FakeRedis()
        with pytest.raises(TypeError, match="'max_delivery_count' must be an int or None"):
            RedisGateway(
                redis_client=client,
                retry_budget_seconds=0,
                message_wait_interval_seconds=0,
                message_visibility_timeout_seconds=300,
                max_delivery_count=invalid_value,
                dead_letter_queue="q::dlq",
            )

    @pytest.mark.parametrize("invalid_value", [0, -1, -100])
    def test_non_positive_max_delivery_count_raises(self, invalid_value):
        client = fakeredis.FakeRedis()
        with pytest.raises(ValueError, match="'max_delivery_count' must be positive"):
            RedisGateway(
                redis_client=client,
                retry_budget_seconds=0,
                message_wait_interval_seconds=0,
                message_visibility_timeout_seconds=300,
                max_delivery_count=invalid_value,
                dead_letter_queue="q::dlq",
            )

    def test_max_delivery_count_requires_visibility_timeout(self):
        client = fakeredis.FakeRedis()
        with pytest.raises(ValueError, match="requires 'message_visibility_timeout_seconds'"):
            RedisGateway(
                redis_client=client,
                retry_budget_seconds=0,
                message_wait_interval_seconds=0,
                message_visibility_timeout_seconds=None,
                max_delivery_count=3,
                dead_letter_queue="q::dlq",
            )

    @pytest.mark.parametrize("invalid_value", [42, True, 3.14, [1], {"dlq": "x"}])
    def test_dead_letter_queue_type_raises(self, invalid_value):
        client = fakeredis.FakeRedis()
        with pytest.raises(TypeError, match="'dead_letter_queue' must be a str or None"):
            RedisGateway(
                redis_client=client,
                retry_budget_seconds=0,
                message_wait_interval_seconds=0,
                message_visibility_timeout_seconds=300,
                max_delivery_count=3,
                dead_letter_queue=invalid_value,
            )

    def test_both_set_is_accepted(self):
        client = fakeredis.FakeRedis()
        gw = RedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=300,
            max_delivery_count=3,
            dead_letter_queue="q::dlq",
        )
        assert gw._max_delivery_count == 3
        assert gw._dead_letter_queue == "q::dlq"

    def test_neither_set_is_accepted(self):
        client = fakeredis.FakeRedis()
        gw = RedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=300,
        )
        assert gw._max_delivery_count is None
        assert gw._dead_letter_queue is None


class TestGatewayDeadLetterCrossValidationAsync:
    """Async RedisGateway must require dead_letter_queue when max_delivery_count is set."""

    def test_max_delivery_count_without_dead_letter_queue_raises(self):
        client = fakeredis.FakeAsyncRedis()
        with pytest.raises(ValueError, match="'dead_letter_queue' is required"):
            AsyncRedisGateway(
                redis_client=client,
                retry_budget_seconds=0,
                message_wait_interval_seconds=0,
                message_visibility_timeout_seconds=300,
                max_delivery_count=3,
                dead_letter_queue=None,
            )

    def test_max_delivery_count_with_empty_dead_letter_queue_raises(self):
        client = fakeredis.FakeAsyncRedis()
        with pytest.raises(ValueError, match="'dead_letter_queue' is required"):
            AsyncRedisGateway(
                redis_client=client,
                retry_budget_seconds=0,
                message_wait_interval_seconds=0,
                message_visibility_timeout_seconds=300,
                max_delivery_count=3,
                dead_letter_queue="",
            )

    def test_dead_letter_queue_without_max_delivery_count_raises(self):
        client = fakeredis.FakeAsyncRedis()
        with pytest.raises(ValueError, match="'max_delivery_count' is required"):
            AsyncRedisGateway(
                redis_client=client,
                retry_budget_seconds=0,
                message_wait_interval_seconds=0,
                message_visibility_timeout_seconds=300,
                max_delivery_count=None,
                dead_letter_queue="q::dlq",
            )

    @pytest.mark.parametrize("invalid_value", ["3", 1.5, True, False, [3]])
    def test_invalid_max_delivery_count_type_raises(self, invalid_value):
        client = fakeredis.FakeAsyncRedis()
        with pytest.raises(TypeError, match="'max_delivery_count' must be an int or None"):
            AsyncRedisGateway(
                redis_client=client,
                retry_budget_seconds=0,
                message_wait_interval_seconds=0,
                message_visibility_timeout_seconds=300,
                max_delivery_count=invalid_value,
                dead_letter_queue="q::dlq",
            )

    @pytest.mark.parametrize("invalid_value", [0, -1, -100])
    def test_non_positive_max_delivery_count_raises(self, invalid_value):
        client = fakeredis.FakeAsyncRedis()
        with pytest.raises(ValueError, match="'max_delivery_count' must be positive"):
            AsyncRedisGateway(
                redis_client=client,
                retry_budget_seconds=0,
                message_wait_interval_seconds=0,
                message_visibility_timeout_seconds=300,
                max_delivery_count=invalid_value,
                dead_letter_queue="q::dlq",
            )

    def test_max_delivery_count_requires_visibility_timeout(self):
        client = fakeredis.FakeAsyncRedis()
        with pytest.raises(ValueError, match="requires 'message_visibility_timeout_seconds'"):
            AsyncRedisGateway(
                redis_client=client,
                retry_budget_seconds=0,
                message_wait_interval_seconds=0,
                message_visibility_timeout_seconds=None,
                max_delivery_count=3,
                dead_letter_queue="q::dlq",
            )

    @pytest.mark.parametrize("invalid_value", [42, True, 3.14, [1], {"dlq": "x"}])
    def test_dead_letter_queue_type_raises(self, invalid_value):
        client = fakeredis.FakeAsyncRedis()
        with pytest.raises(TypeError, match="'dead_letter_queue' must be a str or None"):
            AsyncRedisGateway(
                redis_client=client,
                retry_budget_seconds=0,
                message_wait_interval_seconds=0,
                message_visibility_timeout_seconds=300,
                max_delivery_count=3,
                dead_letter_queue=invalid_value,
            )

    def test_both_set_is_accepted(self):
        client = fakeredis.FakeAsyncRedis()
        gw = AsyncRedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=300,
            max_delivery_count=3,
            dead_letter_queue="q::dlq",
        )
        assert gw._max_delivery_count == 3
        assert gw._dead_letter_queue == "q::dlq"

    def test_neither_set_is_accepted(self):
        client = fakeredis.FakeAsyncRedis()
        gw = AsyncRedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=300,
        )
        assert gw._max_delivery_count is None
        assert gw._dead_letter_queue is None


# ---------------------------------------------------------------------------
# Functional tests — sync
# ---------------------------------------------------------------------------


class TestDeadLetterQueueSync:
    """Poison messages are routed to the dead-letter queue after max_delivery_count."""

    def test_dlq_events_include_message_id_delivery_count_and_threshold(self):
        client = fakeredis.FakeRedis()
        events: list[QueueEvent] = []
        gateway = RedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=30,
            max_delivery_count=1,
            dead_letter_queue="test::dead_letter",
        )
        queue = RedisMessageQueue("test", gateway=gateway, deduplication=False, on_event=events.append)

        queue.publish("poison-a")
        queue.publish("poison-b")
        claimed = []
        for _ in range(2):
            c = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
            assert c is not None
            claimed.append(c)
        expected_ids = {extract_stored_message_id(c.stored_message) for c in claimed}

        lease_deadlines_key = gateway._lease_deadlines_key(queue.key.processing)
        for c in claimed:
            client.zadd(lease_deadlines_key, {c.stored_message: 0})

        assert gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing) is None

        dlq_events = [event for event in events if event.operation is EventOperation.DLQ]
        assert len(dlq_events) == 2
        assert {event.message_id for event in dlq_events} == expected_ids
        assert {event.delivery_count for event in dlq_events} == {2}
        assert {event.max_delivery_count for event in dlq_events} == {1}
        assert {event.destination_queue for event in dlq_events} == {"test::dead_letter"}

    def test_message_routed_to_dead_letter_after_max_deliveries(self):
        client = fakeredis.FakeRedis()
        vt_seconds = 1
        max_deliveries = 3
        gateway = RedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=vt_seconds,
            max_delivery_count=max_deliveries,
            dead_letter_queue="test::dead_letter",
        )
        queue = RedisMessageQueue(
            "test",
            gateway=gateway,
            deduplication=False,
        )

        queue.publish("poison-message")

        # The message is delivered max_deliveries times (counts 1..max_deliveries).
        # On attempt max_deliveries+1, the count exceeds the limit and the
        # message is routed to the dead-letter queue.
        for i in range(max_deliveries):
            claimed = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
            assert claimed is not None, f"Claim {i + 1} returned None unexpectedly"
            # Simulate crash: don't ack, let lease expire
            time.sleep(vt_seconds + 0.1)

        # This claim attempt exceeds max_delivery_count → dead-lettered
        claimed = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert claimed is None

        # Message should be in dead-letter queue
        assert client.llen(queue.key.dead_letter) == 1
        # Message should not be in pending or processing
        assert client.llen(queue.key.pending) == 0
        assert client.llen(queue.key.processing) == 0

    def test_dead_letter_queue_stores_decoded_payload(self):
        client = fakeredis.FakeRedis()
        vt_seconds = 1
        gateway = RedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=vt_seconds,
            max_delivery_count=1,
            dead_letter_queue="test::dead_letter",
        )
        queue = RedisMessageQueue(
            "test",
            gateway=gateway,
            deduplication=False,
        )
        message = 'poison "snowman" ☃\nslash\\\\'

        queue.publish(message)
        claimed = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert claimed is not None
        time.sleep(vt_seconds + 0.1)

        claimed = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert claimed is None

        dead_letter_message = client.lindex(queue.key.dead_letter, 0)
        assert dead_letter_message.decode("utf-8") == message

    def test_zero_timeout_dead_letter_continues_to_next_fresh_message(self):
        client = fakeredis.FakeRedis()
        gateway = RedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=30,
            max_delivery_count=1,
            dead_letter_queue="test::dead_letter",
        )
        queue = RedisMessageQueue("test", gateway=gateway, deduplication=False)

        queue.publish("poison-message")
        first = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        client.zadd(gateway._lease_deadlines_key(queue.key.processing), {first.stored_message: 0})
        queue.publish("fresh-message")

        second = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)

        assert second is not None
        assert second.stored_message != first.stored_message
        assert client.llen(queue.key.pending) == 0
        assert client.llen(queue.key.processing) == 1
        assert client.lindex(queue.key.processing, 0) == second.stored_message
        assert client.lindex(queue.key.dead_letter, 0) == b"poison-message"

    def test_delivery_count_cleaned_on_successful_ack(self):
        client = fakeredis.FakeRedis()
        vt_seconds = 300
        gateway = RedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=vt_seconds,
            max_delivery_count=5,
            dead_letter_queue="test::dead_letter",
        )
        queue = RedisMessageQueue(
            "test",
            gateway=gateway,
            deduplication=False,
        )

        queue.publish("normal-message")

        with queue.process_message() as msg:
            assert msg is not None

        # Delivery count hash should be cleaned up
        delivery_counts_key = f"{queue.key.processing}:delivery_counts"
        assert client.hlen(delivery_counts_key) == 0
        # No dead-letter
        assert client.llen(queue.key.dead_letter) == 0

    def test_unlimited_redelivery_without_max_delivery_count(self):
        client = fakeredis.FakeRedis()
        vt_seconds = 1
        gateway = RedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=vt_seconds,
        )
        queue = RedisMessageQueue(
            "test",
            gateway=gateway,
            deduplication=False,
        )

        queue.publish("message")

        # Claim and let expire 10 times — should always be reclaimable
        for i in range(10):
            claimed = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
            assert claimed is not None, f"Claim {i + 1} returned None unexpectedly"
            time.sleep(vt_seconds + 0.1)

        # Message is still in the system (pending after reclaim), not dead-lettered
        assert client.llen(queue.key.dead_letter) == 0

    def test_max_delivery_count_1_dead_letters_on_first_reclaim(self):
        """With max_delivery_count=1, the first delivery succeeds but any
        reclaim attempt routes the message to the dead-letter queue."""
        client = fakeredis.FakeRedis()
        vt_seconds = 1
        gateway = RedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=vt_seconds,
            max_delivery_count=1,
            dead_letter_queue="test::dead_letter",
        )
        queue = RedisMessageQueue(
            "test",
            gateway=gateway,
            deduplication=False,
        )

        queue.publish("one-shot")

        # First claim succeeds (delivery count becomes 1 == max, so allowed)
        claimed = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert claimed is not None

        # Simulate crash, let lease expire
        time.sleep(vt_seconds + 0.1)

        # Second claim should route to dead letter (count would become 2 > 1)
        claimed = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert claimed is None
        assert client.llen(queue.key.dead_letter) == 1


# ---------------------------------------------------------------------------
# Functional tests — async
# ---------------------------------------------------------------------------


class TestDeadLetterQueueAsync:
    """Async variant of dead-letter queue tests."""

    @pytest.mark.asyncio
    async def test_dlq_events_include_message_id_delivery_count_and_threshold(self):
        client = fakeredis.FakeAsyncRedis()
        events: list[QueueEvent] = []

        async def observe(event: QueueEvent) -> None:
            events.append(event)

        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=30,
            max_delivery_count=1,
            dead_letter_queue="test::dead_letter",
        )
        queue = AsyncRedisMessageQueue("test", gateway=gateway, deduplication=False, on_event=observe)

        await queue.publish("poison-a")
        await queue.publish("poison-b")
        claimed = []
        for _ in range(2):
            c = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
            assert c is not None
            claimed.append(c)
        expected_ids = {extract_stored_message_id(c.stored_message) for c in claimed}

        lease_deadlines_key = gateway._lease_deadlines_key(queue.key.processing)
        for c in claimed:
            await client.zadd(lease_deadlines_key, {c.stored_message: 0})

        assert await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing) is None

        dlq_events = [event for event in events if event.operation is EventOperation.DLQ]
        assert len(dlq_events) == 2
        assert {event.message_id for event in dlq_events} == expected_ids
        assert {event.delivery_count for event in dlq_events} == {2}
        assert {event.max_delivery_count for event in dlq_events} == {1}
        assert {event.destination_queue for event in dlq_events} == {"test::dead_letter"}

    @pytest.mark.asyncio
    async def test_message_routed_to_dead_letter_after_max_deliveries(self):
        client = fakeredis.FakeAsyncRedis()
        vt_seconds = 1
        max_deliveries = 3
        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=vt_seconds,
            max_delivery_count=max_deliveries,
            dead_letter_queue="test::dead_letter",
        )
        queue = AsyncRedisMessageQueue(
            "test",
            gateway=gateway,
            deduplication=False,
        )

        await queue.publish("poison-message")

        for i in range(max_deliveries):
            claimed = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
            assert claimed is not None, f"Claim {i + 1} returned None unexpectedly"
            time.sleep(vt_seconds + 0.1)

        # This claim attempt exceeds max_delivery_count → dead-lettered
        claimed = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert claimed is None

        assert await client.llen(queue.key.dead_letter) == 1
        assert await client.llen(queue.key.pending) == 0
        assert await client.llen(queue.key.processing) == 0

    @pytest.mark.asyncio
    async def test_dead_letter_queue_stores_decoded_payload(self):
        client = fakeredis.FakeAsyncRedis()
        vt_seconds = 1
        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=vt_seconds,
            max_delivery_count=1,
            dead_letter_queue="test::dead_letter",
        )
        queue = AsyncRedisMessageQueue(
            "test",
            gateway=gateway,
            deduplication=False,
        )
        message = 'poison "snowman" ☃\nslash\\\\'

        await queue.publish(message)
        claimed = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert claimed is not None
        time.sleep(vt_seconds + 0.1)

        claimed = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert claimed is None

        dead_letter_message = await client.lindex(queue.key.dead_letter, 0)
        assert dead_letter_message.decode("utf-8") == message

    @pytest.mark.asyncio
    async def test_zero_timeout_dead_letter_continues_to_next_fresh_message(self):
        client = fakeredis.FakeAsyncRedis()
        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=30,
            max_delivery_count=1,
            dead_letter_queue="test::dead_letter",
        )
        queue = AsyncRedisMessageQueue("test", gateway=gateway, deduplication=False)

        await queue.publish("poison-message")
        first = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        await client.zadd(gateway._lease_deadlines_key(queue.key.processing), {first.stored_message: 0})
        await queue.publish("fresh-message")

        second = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)

        assert second is not None
        assert second.stored_message != first.stored_message
        assert await client.llen(queue.key.pending) == 0
        assert await client.llen(queue.key.processing) == 1
        assert await client.lindex(queue.key.processing, 0) == second.stored_message
        assert await client.lindex(queue.key.dead_letter, 0) == b"poison-message"

    @pytest.mark.asyncio
    async def test_delivery_count_cleaned_on_successful_ack(self):
        client = fakeredis.FakeAsyncRedis()
        vt_seconds = 300
        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=vt_seconds,
            max_delivery_count=5,
            dead_letter_queue="test::dead_letter",
        )
        queue = AsyncRedisMessageQueue(
            "test",
            gateway=gateway,
            deduplication=False,
        )

        await queue.publish("normal-message")

        async with queue.process_message() as msg:
            assert msg is not None

        delivery_counts_key = f"{queue.key.processing}:delivery_counts"
        assert await client.hlen(delivery_counts_key) == 0
        assert await client.llen(queue.key.dead_letter) == 0

    @pytest.mark.asyncio
    async def test_unlimited_redelivery_without_max_delivery_count(self):
        client = fakeredis.FakeAsyncRedis()
        vt_seconds = 1
        gateway = AsyncRedisGateway(
            redis_client=client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=vt_seconds,
        )
        queue = AsyncRedisMessageQueue(
            "test",
            gateway=gateway,
            deduplication=False,
        )

        await queue.publish("message")

        for i in range(10):
            claimed = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
            assert claimed is not None, f"Claim {i + 1} returned None unexpectedly"
            time.sleep(vt_seconds + 0.1)

        assert await client.llen(queue.key.dead_letter) == 0


# ---------------------------------------------------------------------------
# QueueKeyManager dead_letter key
# ---------------------------------------------------------------------------


class TestDeadLetterKey:
    def test_dead_letter_key_format(self):
        client = fakeredis.FakeRedis()
        queue = RedisMessageQueue(
            "myqueue",
            client=client,
            visibility_timeout_seconds=300,
            max_delivery_count=3,
        )
        assert queue.key.dead_letter == "myqueue::dead_letter"

    def test_dead_letter_key_with_hash_tag(self):
        client = fakeredis.FakeRedis()
        queue = RedisMessageQueue(
            "{myqueue}",
            client=client,
            visibility_timeout_seconds=300,
            max_delivery_count=3,
        )
        assert "{myqueue}" in queue.key.dead_letter
