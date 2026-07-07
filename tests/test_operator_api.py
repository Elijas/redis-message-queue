"""Operator API: stats(), peek(), redrive_dead_letters(), purge().

Covers both the sync and async ``RedisMessageQueue`` against fakeredis:
peek does not consume, redrive round-trips a dead-lettered message back into a
reprocessable claim (with a reset delivery count), purge counts and empties,
stats reflects disabled features as ``None``, and every method fails fast on
invalid arguments.
"""

import fakeredis
import pytest

from redis_message_queue import AbstractRedisGateway as SyncAbstractRedisGateway
from redis_message_queue import ClaimedMessage, ConfigurationError, GatewayContractError, QueueStats, ReceivedPayload
from redis_message_queue import QueueStats as SyncQueueStats
from redis_message_queue._redis_gateway import RedisGateway
from redis_message_queue._stored_message import decode_stored_message
from redis_message_queue.asyncio import AbstractRedisGateway as AsyncAbstractRedisGateway
from redis_message_queue.asyncio import QueueStats as AsyncQueueStats
from redis_message_queue.asyncio._redis_gateway import RedisGateway as AsyncRedisGateway
from redis_message_queue.asyncio.redis_message_queue import RedisMessageQueue as AsyncRedisMessageQueue
from redis_message_queue.redis_message_queue import RedisMessageQueue

TRICKY_PAYLOAD = 'poison "snowman" ☃\nslash\\\\'


def _sync_gateway(name, client, *, max_delivery_count=2, vt_seconds=30):
    return RedisGateway(
        redis_client=client,
        retry_budget_seconds=0,
        message_wait_interval_seconds=0,
        message_visibility_timeout_seconds=vt_seconds,
        max_delivery_count=max_delivery_count,
        dead_letter_queue=f"{name}::dlq",
    )


def _async_gateway(name, client, *, max_delivery_count=2, vt_seconds=30):
    return AsyncRedisGateway(
        redis_client=client,
        retry_budget_seconds=0,
        message_wait_interval_seconds=0,
        message_visibility_timeout_seconds=vt_seconds,
        max_delivery_count=max_delivery_count,
        dead_letter_queue=f"{name}::dlq",
    )


def _sync_dead_letter(gateway, queue, message, max_deliveries):
    """Publish ``message`` and drive it into the dead-letter queue."""
    queue.publish(message)
    for _ in range(max_deliveries):
        claimed = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert claimed is not None
        gateway._redis_client.zadd(
            gateway._lease_deadlines_key(queue.key.processing),
            {claimed.stored_message: 0},
        )
    assert gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing) is None


async def _async_dead_letter(gateway, queue, message, max_deliveries):
    await queue.publish(message)
    for _ in range(max_deliveries):
        claimed = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert claimed is not None
        await gateway._redis_client.zadd(
            gateway._lease_deadlines_key(queue.key.processing),
            {claimed.stored_message: 0},
        )
    assert await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing) is None


# ---------------------------------------------------------------------------
# Export / shared dataclass
# ---------------------------------------------------------------------------


def test_queue_stats_is_shared_across_sync_and_async():
    assert SyncQueueStats is AsyncQueueStats is QueueStats


def test_queue_stats_is_frozen():
    stats = QueueStats(pending=1, processing=0, completed=None, failed=None, dead_letter=None)
    with pytest.raises(Exception):
        stats.pending = 5  # type: ignore[misc]


# ---------------------------------------------------------------------------
# stats() — sync
# ---------------------------------------------------------------------------


class TestStatsSync:
    def test_reports_pending_and_processing_depths(self):
        client = fakeredis.FakeRedis()
        queue = RedisMessageQueue("stats", client=client)
        queue.publish("a")
        queue.publish("b")
        stats = queue.stats()
        assert stats.pending == 2
        assert stats.processing == 0

    def test_disabled_features_are_none(self):
        client = fakeredis.FakeRedis()
        queue = RedisMessageQueue(
            "stats",
            client=client,
            visibility_timeout_seconds=None,
            max_delivery_count=None,
        )
        stats = queue.stats()
        assert stats.completed is None
        assert stats.failed is None
        assert stats.dead_letter is None

    def test_enabled_features_are_ints(self):
        client = fakeredis.FakeRedis()
        gateway = _sync_gateway("stats", client, max_delivery_count=1)
        queue = RedisMessageQueue(
            "stats",
            gateway=gateway,
            enable_completed_queue=True,
            enable_failed_queue=True,
        )
        stats = queue.stats()
        assert stats.completed == 0
        assert stats.failed == 0
        assert stats.dead_letter == 0

    def test_dead_letter_depth_tracks_dlq(self):
        client = fakeredis.FakeRedis()
        gateway = _sync_gateway("stats", client, max_delivery_count=1)
        queue = RedisMessageQueue("stats", gateway=gateway)
        _sync_dead_letter(gateway, queue, "poison", max_deliveries=1)
        stats = queue.stats()
        assert stats.dead_letter == 1
        assert stats.pending == 0
        assert stats.processing == 0


# ---------------------------------------------------------------------------
# peek() — sync
# ---------------------------------------------------------------------------


class TestPeekSync:
    def test_does_not_consume(self):
        client = fakeredis.FakeRedis()
        queue = RedisMessageQueue("peek", client=client)
        queue.publish("a")
        queue.publish("b")
        before = queue.stats().pending
        peeked = queue.peek(5)
        assert len(peeked) == 2
        assert queue.stats().pending == before

    def test_returns_decoded_payloads(self):
        client = fakeredis.FakeRedis()
        queue = RedisMessageQueue("peek", client=client)
        queue.publish("hello")
        assert queue.peek() == [b"hello"]

    def test_count_limits_results(self):
        client = fakeredis.FakeRedis()
        queue = RedisMessageQueue("peek", client=client)
        for i in range(4):
            queue.publish(f"m{i}")
        assert len(queue.peek(2)) == 2

    def test_peek_dead_letter_source(self):
        client = fakeredis.FakeRedis()
        gateway = _sync_gateway("peek", client, max_delivery_count=1)
        queue = RedisMessageQueue("peek", gateway=gateway)
        _sync_dead_letter(gateway, queue, TRICKY_PAYLOAD, max_deliveries=1)
        assert queue.peek(source="dead_letter") == [TRICKY_PAYLOAD.encode("utf-8")]

    def test_peek_dead_letter_prefix_colliding_payload_returned_as_is(self):
        # A raw-payload source (dead_letter) stores the user payload verbatim. A
        # payload that merely starts with the RMQ envelope prefix but is not a
        # valid envelope must be returned as-is, not run through envelope
        # decoding (which would raise MalformedStoredMessageError and make the
        # whole DLQ uninspectable via peek()).
        client = fakeredis.FakeRedis()
        gateway = _sync_gateway("peek", client, max_delivery_count=1)
        queue = RedisMessageQueue("peek", gateway=gateway)
        payload = "\x1eRMQ1:this-is-not-json"
        _sync_dead_letter(gateway, queue, payload, max_deliveries=1)
        assert queue.peek(source="dead_letter") == [payload.encode("utf-8")]

    def test_peek_dead_letter_envelope_shaped_payload_not_unwrapped(self):
        # A raw payload that happens to be a well-formed envelope string must be
        # returned as stored, not silently unwrapped to the inner value.
        client = fakeredis.FakeRedis()
        gateway = _sync_gateway("peek", client, max_delivery_count=1)
        queue = RedisMessageQueue("peek", gateway=gateway)
        payload = '\x1eRMQ1:{"id":"innerid","payload":"INNER"}'
        _sync_dead_letter(gateway, queue, payload, max_deliveries=1)
        assert queue.peek(source="dead_letter") == [payload.encode("utf-8")]

    def test_peek_gateway_non_str_bytes_element_raises_contract_error(self):
        gateway = _SyncPeekBadElementsGateway()
        queue = RedisMessageQueue("bare", gateway=gateway)
        with pytest.raises(GatewayContractError, match=r"peek_messages\(\) must return list\[str \| bytes\]"):
            queue.peek(2)

    def test_peek_dead_letter_without_dlq_raises(self):
        client = fakeredis.FakeRedis()
        queue = RedisMessageQueue(
            "peek",
            client=client,
            visibility_timeout_seconds=None,
            max_delivery_count=None,
        )
        with pytest.raises(ConfigurationError, match="no dead-letter queue is configured"):
            queue.peek(source="dead_letter")

    @pytest.mark.parametrize("bad_count", [0, -1])
    def test_count_below_one_raises(self, bad_count):
        client = fakeredis.FakeRedis()
        queue = RedisMessageQueue("peek", client=client)
        with pytest.raises(ConfigurationError, match="'count' must be >= 1"):
            queue.peek(bad_count)

    @pytest.mark.parametrize("bad_count", [True, 1.0, "1"])
    def test_count_non_int_raises(self, bad_count):
        client = fakeredis.FakeRedis()
        queue = RedisMessageQueue("peek", client=client)
        with pytest.raises(TypeError, match="'count' must be an int"):
            queue.peek(bad_count)

    def test_unknown_source_raises(self):
        client = fakeredis.FakeRedis()
        queue = RedisMessageQueue("peek", client=client)
        with pytest.raises(ConfigurationError, match="'source' must be one of"):
            queue.peek(source="bogus")


# ---------------------------------------------------------------------------
# redrive_dead_letters() — sync
# ---------------------------------------------------------------------------


class TestRedriveSync:
    def test_redriven_message_is_reprocessable(self):
        client = fakeredis.FakeRedis()
        gateway = _sync_gateway("redrive", client, max_delivery_count=2)
        queue = RedisMessageQueue("redrive", gateway=gateway)
        _sync_dead_letter(gateway, queue, TRICKY_PAYLOAD, max_deliveries=2)
        assert queue.stats().dead_letter == 1

        moved = queue.redrive_dead_letters()
        assert moved == 1
        assert queue.stats().dead_letter == 0
        assert queue.stats().pending == 1

        claimed = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert claimed is not None
        assert decode_stored_message(claimed.stored_message).decode("utf-8") == TRICKY_PAYLOAD
        assert gateway.remove_message(queue.key.processing, claimed.stored_message, lease_token=claimed.lease_token)

    def test_redrive_resets_delivery_count(self):
        client = fakeredis.FakeRedis()
        max_deliveries = 2
        gateway = _sync_gateway("redrive", client, max_delivery_count=max_deliveries)
        queue = RedisMessageQueue("redrive", gateway=gateway)
        _sync_dead_letter(gateway, queue, "poison", max_deliveries=max_deliveries)

        queue.redrive_dead_letters()
        # A redriven message must survive a full fresh budget of deliveries and
        # only dead-letter again after exceeding max_delivery_count, proving the
        # delivery count was reset rather than carried over.
        for _ in range(max_deliveries):
            claimed = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
            assert claimed is not None
            gateway._redis_client.zadd(
                gateway._lease_deadlines_key(queue.key.processing),
                {claimed.stored_message: 0},
            )
        assert gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing) is None
        assert queue.stats().dead_letter == 1

    def test_redrive_respects_max_messages(self):
        client = fakeredis.FakeRedis()
        gateway = _sync_gateway("redrive", client, max_delivery_count=1)
        queue = RedisMessageQueue("redrive", gateway=gateway)
        for i in range(3):
            _sync_dead_letter(gateway, queue, f"poison-{i}", max_deliveries=1)
        assert queue.stats().dead_letter == 3

        assert queue.redrive_dead_letters(max_messages=2) == 2
        assert queue.stats().dead_letter == 1
        assert queue.stats().pending == 2

    def test_redrive_all_moves_everything(self):
        client = fakeredis.FakeRedis()
        gateway = _sync_gateway("redrive", client, max_delivery_count=1)
        queue = RedisMessageQueue("redrive", gateway=gateway)
        for i in range(3):
            _sync_dead_letter(gateway, queue, f"poison-{i}", max_deliveries=1)
        assert queue.redrive_dead_letters() == 3
        assert queue.stats().dead_letter == 0

    def test_redrive_empty_returns_zero(self):
        client = fakeredis.FakeRedis()
        gateway = _sync_gateway("redrive", client, max_delivery_count=1)
        queue = RedisMessageQueue("redrive", gateway=gateway)
        assert queue.redrive_dead_letters() == 0

    def test_redrive_without_dlq_raises(self):
        client = fakeredis.FakeRedis()
        queue = RedisMessageQueue(
            "redrive",
            client=client,
            visibility_timeout_seconds=None,
            max_delivery_count=None,
        )
        with pytest.raises(ConfigurationError, match="no dead-letter queue is configured"):
            queue.redrive_dead_letters()

    @pytest.mark.parametrize("bad", [0, -5])
    def test_max_messages_below_one_raises(self, bad):
        client = fakeredis.FakeRedis()
        queue = RedisMessageQueue("redrive", client=client)
        with pytest.raises(ConfigurationError, match="'max_messages' must be >= 1"):
            queue.redrive_dead_letters(bad)

    @pytest.mark.parametrize("bad", [True, 1.5, "1"])
    def test_max_messages_non_int_raises(self, bad):
        client = fakeredis.FakeRedis()
        queue = RedisMessageQueue("redrive", client=client)
        with pytest.raises(TypeError, match="'max_messages' must be an int or None"):
            queue.redrive_dead_letters(bad)


# ---------------------------------------------------------------------------
# purge() — sync
# ---------------------------------------------------------------------------


class TestPurgeSync:
    def test_purge_counts_and_empties_pending(self):
        client = fakeredis.FakeRedis()
        queue = RedisMessageQueue("purge", client=client)
        for i in range(3):
            queue.publish(f"m{i}")
        assert queue.purge(target="pending") == 3
        assert queue.stats().pending == 0

    def test_purge_dead_letter(self):
        client = fakeredis.FakeRedis()
        gateway = _sync_gateway("purge", client, max_delivery_count=1)
        queue = RedisMessageQueue("purge", gateway=gateway)
        _sync_dead_letter(gateway, queue, "poison", max_deliveries=1)
        assert queue.purge(target="dead_letter") == 1
        assert queue.stats().dead_letter == 0

    def test_purge_empty_returns_zero(self):
        client = fakeredis.FakeRedis()
        queue = RedisMessageQueue("purge", client=client)
        assert queue.purge(target="pending") == 0

    def test_purge_processing_is_rejected(self):
        client = fakeredis.FakeRedis()
        queue = RedisMessageQueue("purge", client=client)
        with pytest.raises(ConfigurationError, match="refusing to purge 'processing'"):
            queue.purge(target="processing")

    def test_purge_unknown_target_raises(self):
        client = fakeredis.FakeRedis()
        queue = RedisMessageQueue("purge", client=client)
        with pytest.raises(ConfigurationError, match="'target' must be one of"):
            queue.purge(target="bogus")

    def test_purge_requires_named_target(self):
        client = fakeredis.FakeRedis()
        queue = RedisMessageQueue("purge", client=client)
        with pytest.raises(TypeError):
            queue.purge("pending")  # type: ignore[misc]

    def test_purge_dead_letter_without_dlq_raises(self):
        client = fakeredis.FakeRedis()
        queue = RedisMessageQueue(
            "purge",
            client=client,
            visibility_timeout_seconds=None,
            max_delivery_count=None,
        )
        with pytest.raises(ConfigurationError, match="no dead-letter queue is configured"):
            queue.purge(target="dead_letter")


# ---------------------------------------------------------------------------
# stats() — async
# ---------------------------------------------------------------------------


class TestStatsAsync:
    @pytest.mark.asyncio
    async def test_reports_pending_and_processing_depths(self):
        client = fakeredis.FakeAsyncRedis()
        queue = AsyncRedisMessageQueue("stats", client=client)
        await queue.publish("a")
        await queue.publish("b")
        stats = await queue.stats()
        assert stats.pending == 2
        assert stats.processing == 0

    @pytest.mark.asyncio
    async def test_disabled_features_are_none(self):
        client = fakeredis.FakeAsyncRedis()
        queue = AsyncRedisMessageQueue(
            "stats",
            client=client,
            visibility_timeout_seconds=None,
            max_delivery_count=None,
        )
        stats = await queue.stats()
        assert stats.completed is None
        assert stats.failed is None
        assert stats.dead_letter is None

    @pytest.mark.asyncio
    async def test_enabled_features_are_ints(self):
        client = fakeredis.FakeAsyncRedis()
        gateway = _async_gateway("stats", client, max_delivery_count=1)
        queue = AsyncRedisMessageQueue(
            "stats",
            gateway=gateway,
            enable_completed_queue=True,
            enable_failed_queue=True,
        )
        stats = await queue.stats()
        assert stats.completed == 0
        assert stats.failed == 0
        assert stats.dead_letter == 0

    @pytest.mark.asyncio
    async def test_dead_letter_depth_tracks_dlq(self):
        client = fakeredis.FakeAsyncRedis()
        gateway = _async_gateway("stats", client, max_delivery_count=1)
        queue = AsyncRedisMessageQueue("stats", gateway=gateway)
        await _async_dead_letter(gateway, queue, "poison", max_deliveries=1)
        stats = await queue.stats()
        assert stats.dead_letter == 1


# ---------------------------------------------------------------------------
# peek() — async
# ---------------------------------------------------------------------------


class TestPeekAsync:
    @pytest.mark.asyncio
    async def test_does_not_consume(self):
        client = fakeredis.FakeAsyncRedis()
        queue = AsyncRedisMessageQueue("peek", client=client)
        await queue.publish("a")
        await queue.publish("b")
        peeked = await queue.peek(5)
        assert len(peeked) == 2
        assert (await queue.stats()).pending == 2

    @pytest.mark.asyncio
    async def test_returns_decoded_payloads(self):
        client = fakeredis.FakeAsyncRedis()
        queue = AsyncRedisMessageQueue("peek", client=client)
        await queue.publish("hello")
        assert await queue.peek() == [b"hello"]

    @pytest.mark.asyncio
    async def test_peek_dead_letter_source(self):
        client = fakeredis.FakeAsyncRedis()
        gateway = _async_gateway("peek", client, max_delivery_count=1)
        queue = AsyncRedisMessageQueue("peek", gateway=gateway)
        await _async_dead_letter(gateway, queue, TRICKY_PAYLOAD, max_deliveries=1)
        assert await queue.peek(source="dead_letter") == [TRICKY_PAYLOAD.encode("utf-8")]

    @pytest.mark.asyncio
    async def test_peek_dead_letter_prefix_colliding_payload_returned_as_is(self):
        client = fakeredis.FakeAsyncRedis()
        gateway = _async_gateway("peek", client, max_delivery_count=1)
        queue = AsyncRedisMessageQueue("peek", gateway=gateway)
        payload = "\x1eRMQ1:this-is-not-json"
        await _async_dead_letter(gateway, queue, payload, max_deliveries=1)
        assert await queue.peek(source="dead_letter") == [payload.encode("utf-8")]

    @pytest.mark.asyncio
    async def test_peek_dead_letter_envelope_shaped_payload_not_unwrapped(self):
        client = fakeredis.FakeAsyncRedis()
        gateway = _async_gateway("peek", client, max_delivery_count=1)
        queue = AsyncRedisMessageQueue("peek", gateway=gateway)
        payload = '\x1eRMQ1:{"id":"innerid","payload":"INNER"}'
        await _async_dead_letter(gateway, queue, payload, max_deliveries=1)
        assert await queue.peek(source="dead_letter") == [payload.encode("utf-8")]

    @pytest.mark.asyncio
    async def test_peek_gateway_non_str_bytes_element_raises_contract_error(self):
        gateway = _AsyncPeekBadElementsGateway()
        queue = AsyncRedisMessageQueue("bare", gateway=gateway)
        with pytest.raises(GatewayContractError, match=r"peek_messages\(\) must return list\[str \| bytes\]"):
            await queue.peek(2)

    @pytest.mark.asyncio
    async def test_count_below_one_raises(self):
        client = fakeredis.FakeAsyncRedis()
        queue = AsyncRedisMessageQueue("peek", client=client)
        with pytest.raises(ConfigurationError, match="'count' must be >= 1"):
            await queue.peek(0)

    @pytest.mark.asyncio
    async def test_count_non_int_raises(self):
        client = fakeredis.FakeAsyncRedis()
        queue = AsyncRedisMessageQueue("peek", client=client)
        with pytest.raises(TypeError, match="'count' must be an int"):
            await queue.peek(True)

    @pytest.mark.asyncio
    async def test_unknown_source_raises(self):
        client = fakeredis.FakeAsyncRedis()
        queue = AsyncRedisMessageQueue("peek", client=client)
        with pytest.raises(ConfigurationError, match="'source' must be one of"):
            await queue.peek(source="bogus")


# ---------------------------------------------------------------------------
# redrive_dead_letters() — async
# ---------------------------------------------------------------------------


class TestRedriveAsync:
    @pytest.mark.asyncio
    async def test_redriven_message_is_reprocessable(self):
        client = fakeredis.FakeAsyncRedis()
        gateway = _async_gateway("redrive", client, max_delivery_count=2)
        queue = AsyncRedisMessageQueue("redrive", gateway=gateway)
        await _async_dead_letter(gateway, queue, TRICKY_PAYLOAD, max_deliveries=2)

        moved = await queue.redrive_dead_letters()
        assert moved == 1
        assert (await queue.stats()).dead_letter == 0
        assert (await queue.stats()).pending == 1

        claimed = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert claimed is not None
        assert decode_stored_message(claimed.stored_message).decode("utf-8") == TRICKY_PAYLOAD
        assert await gateway.remove_message(
            queue.key.processing, claimed.stored_message, lease_token=claimed.lease_token
        )

    @pytest.mark.asyncio
    async def test_redrive_resets_delivery_count(self):
        client = fakeredis.FakeAsyncRedis()
        max_deliveries = 2
        gateway = _async_gateway("redrive", client, max_delivery_count=max_deliveries)
        queue = AsyncRedisMessageQueue("redrive", gateway=gateway)
        await _async_dead_letter(gateway, queue, "poison", max_deliveries=max_deliveries)

        await queue.redrive_dead_letters()
        for _ in range(max_deliveries):
            claimed = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
            assert claimed is not None
            await gateway._redis_client.zadd(
                gateway._lease_deadlines_key(queue.key.processing),
                {claimed.stored_message: 0},
            )
        assert await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing) is None
        assert (await queue.stats()).dead_letter == 1

    @pytest.mark.asyncio
    async def test_redrive_respects_max_messages(self):
        client = fakeredis.FakeAsyncRedis()
        gateway = _async_gateway("redrive", client, max_delivery_count=1)
        queue = AsyncRedisMessageQueue("redrive", gateway=gateway)
        for i in range(3):
            await _async_dead_letter(gateway, queue, f"poison-{i}", max_deliveries=1)

        assert await queue.redrive_dead_letters(max_messages=2) == 2
        assert (await queue.stats()).dead_letter == 1

    @pytest.mark.asyncio
    async def test_redrive_without_dlq_raises(self):
        client = fakeredis.FakeAsyncRedis()
        queue = AsyncRedisMessageQueue(
            "redrive",
            client=client,
            visibility_timeout_seconds=None,
            max_delivery_count=None,
        )
        with pytest.raises(ConfigurationError, match="no dead-letter queue is configured"):
            await queue.redrive_dead_letters()

    @pytest.mark.asyncio
    async def test_max_messages_non_int_raises(self):
        client = fakeredis.FakeAsyncRedis()
        queue = AsyncRedisMessageQueue("redrive", client=client)
        with pytest.raises(TypeError, match="'max_messages' must be an int or None"):
            await queue.redrive_dead_letters(1.5)


# ---------------------------------------------------------------------------
# purge() — async
# ---------------------------------------------------------------------------


class TestPurgeAsync:
    @pytest.mark.asyncio
    async def test_purge_counts_and_empties_pending(self):
        client = fakeredis.FakeAsyncRedis()
        queue = AsyncRedisMessageQueue("purge", client=client)
        for i in range(3):
            await queue.publish(f"m{i}")
        assert await queue.purge(target="pending") == 3
        assert (await queue.stats()).pending == 0

    @pytest.mark.asyncio
    async def test_purge_dead_letter(self):
        client = fakeredis.FakeAsyncRedis()
        gateway = _async_gateway("purge", client, max_delivery_count=1)
        queue = AsyncRedisMessageQueue("purge", gateway=gateway)
        await _async_dead_letter(gateway, queue, "poison", max_deliveries=1)
        assert await queue.purge(target="dead_letter") == 1
        assert (await queue.stats()).dead_letter == 0

    @pytest.mark.asyncio
    async def test_purge_processing_is_rejected(self):
        client = fakeredis.FakeAsyncRedis()
        queue = AsyncRedisMessageQueue("purge", client=client)
        with pytest.raises(ConfigurationError, match="refusing to purge 'processing'"):
            await queue.purge(target="processing")

    @pytest.mark.asyncio
    async def test_purge_unknown_target_raises(self):
        client = fakeredis.FakeAsyncRedis()
        queue = AsyncRedisMessageQueue("purge", client=client)
        with pytest.raises(ConfigurationError, match="'target' must be one of"):
            await queue.purge(target="bogus")


# ---------------------------------------------------------------------------
# Custom gateways without operator methods fail fast
# ---------------------------------------------------------------------------


class _SyncBareGateway(SyncAbstractRedisGateway):
    """Minimal custom gateway: implements the contract, no operator helpers."""

    def publish_message(self, queue: str, message: str, dedup_key: str) -> bool:
        return True

    def add_message(self, queue: str, message: str) -> None:
        pass

    def move_message(self, from_queue, to_queue, message, *, lease_token=None) -> bool:
        return True

    def remove_message(self, queue, message, *, lease_token=None) -> bool:
        return True

    def renew_message_lease(self, queue, message, lease_token, **_kwargs) -> bool:
        return True

    def wait_for_message_and_move(self, from_queue, to_queue) -> ClaimedMessage | ReceivedPayload | None:
        return None

    def trim_queue(self, queue, max_length) -> None:
        pass


class _AsyncBareGateway(AsyncAbstractRedisGateway):
    async def publish_message(self, queue: str, message: str, dedup_key: str) -> bool:
        return True

    async def add_message(self, queue: str, message: str) -> None:
        pass

    async def move_message(self, from_queue, to_queue, message, *, lease_token=None) -> bool:
        return True

    async def remove_message(self, queue, message, *, lease_token=None) -> bool:
        return True

    async def renew_message_lease(self, queue, message, lease_token, **_kwargs) -> bool:
        return True

    async def wait_for_message_and_move(self, from_queue, to_queue) -> ClaimedMessage | ReceivedPayload | None:
        return None

    async def trim_queue(self, queue, max_length) -> None:
        pass


class _SyncPeekBadElementsGateway(_SyncBareGateway):
    """A gateway whose peek_messages() returns a list of the wrong element type."""

    def peek_messages(self, queue: str, count: int):
        return [123, None]


class _AsyncPeekBadElementsGateway(_AsyncBareGateway):
    async def peek_messages(self, queue: str, count: int):
        return [123, None]


def test_sync_operator_api_requires_supporting_gateway():
    queue = RedisMessageQueue("bare", gateway=_SyncBareGateway())
    with pytest.raises(GatewayContractError, match="does not implement 'queue_length'"):
        queue.stats()
    with pytest.raises(GatewayContractError, match="does not implement 'peek_messages'"):
        queue.peek()
    with pytest.raises(GatewayContractError, match="does not implement 'purge_queue'"):
        queue.purge(target="pending")


@pytest.mark.asyncio
async def test_async_operator_api_requires_supporting_gateway():
    queue = AsyncRedisMessageQueue("bare", gateway=_AsyncBareGateway())
    with pytest.raises(GatewayContractError, match="does not implement 'queue_length'"):
        await queue.stats()
    with pytest.raises(GatewayContractError, match="does not implement 'purge_queue'"):
        await queue.purge(target="pending")
