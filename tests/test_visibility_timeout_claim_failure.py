import pytest

from redis_message_queue import ClaimStoreFailedError
from redis_message_queue._redis_gateway import RedisGateway
from redis_message_queue._stored_message import encode_stored_message, extract_stored_message_id
from redis_message_queue.asyncio._redis_gateway import RedisGateway as AsyncRedisGateway

pytestmark = pytest.mark.integration


def _keys(queue_name):
    return f"{queue_name}:pending", f"{queue_name}:processing", f"{queue_name}:dlq"


def _stored_bytes(stored):
    return stored.encode("utf-8")


def _delivery_count(value):
    return 0 if value is None else int(value)


def _sync_gateway(client, dlq):
    return RedisGateway(
        redis_client=client,
        retry_budget_seconds=0,
        message_wait_interval_seconds=0,
        message_visibility_timeout_seconds=30,
        max_delivery_count=2,
        dead_letter_queue=dlq,
    )


def _async_gateway(client, dlq):
    return AsyncRedisGateway(
        redis_client=client,
        retry_budget_seconds=0,
        message_wait_interval_seconds=0,
        message_visibility_timeout_seconds=30,
        max_delivery_count=2,
        dead_letter_queue=dlq,
    )


def test_sync_claim_success_preserves_delivery_count(real_redis_client, queue_name):
    pending, processing, dlq = _keys(queue_name)
    gateway = _sync_gateway(real_redis_client, dlq)
    stored = encode_stored_message("hello")
    real_redis_client.lpush(pending, stored)

    claimed = gateway.wait_for_message_and_move(pending, processing)

    assert claimed is not None
    assert claimed.stored_message == _stored_bytes(stored)
    assert claimed.lease_token
    assert _delivery_count(real_redis_client.hget(gateway._delivery_counts_key(processing), stored)) == 1


def test_sync_claim_store_failure_rolls_back_and_raises(real_redis_client, queue_name):
    pending, processing, dlq = _keys(queue_name)
    gateway = _sync_gateway(real_redis_client, dlq)
    stored = encode_stored_message("hello")
    real_redis_client.lpush(pending, stored)
    real_redis_client.set(gateway._lease_token_counter_key(processing), "not-an-int")

    with pytest.raises(ClaimStoreFailedError) as caught:
        gateway.wait_for_message_and_move(pending, processing)

    assert caught.value.queue == pending
    assert caught.value.message_id == extract_stored_message_id(stored)
    assert caught.value.operation == "claim"
    assert "integer" in str(caught.value)
    assert _delivery_count(real_redis_client.hget(gateway._delivery_counts_key(processing), stored)) == 0
    assert real_redis_client.lrange(pending, 0, -1) == [_stored_bytes(stored)]
    assert real_redis_client.llen(processing) == 0
    assert real_redis_client.lrange(dlq, 0, -1) == []


def test_sync_repeated_store_failures_do_not_consume_delivery_attempts(real_redis_client, queue_name):
    pending, processing, dlq = _keys(queue_name)
    gateway = _sync_gateway(real_redis_client, dlq)
    stored = encode_stored_message("hello")
    real_redis_client.lpush(pending, stored)
    lease_counter = gateway._lease_token_counter_key(processing)
    real_redis_client.set(lease_counter, "not-an-int")

    for _ in range(3):
        with pytest.raises(ClaimStoreFailedError):
            gateway.wait_for_message_and_move(pending, processing)
        assert _delivery_count(real_redis_client.hget(gateway._delivery_counts_key(processing), stored)) == 0
        assert real_redis_client.llen(pending) == 1
        assert real_redis_client.llen(processing) == 0
        assert real_redis_client.lrange(dlq, 0, -1) == []

    real_redis_client.set(lease_counter, "0")

    claimed = gateway.wait_for_message_and_move(pending, processing)

    assert claimed is not None
    assert claimed.stored_message == _stored_bytes(stored)
    assert _delivery_count(real_redis_client.hget(gateway._delivery_counts_key(processing), stored)) == 1
    assert real_redis_client.llen(pending) == 0
    assert real_redis_client.llen(processing) == 1
    assert real_redis_client.lrange(dlq, 0, -1) == []


@pytest.mark.asyncio
async def test_async_claim_success_preserves_delivery_count(real_async_redis_client, queue_name):
    pending, processing, dlq = _keys(queue_name)
    gateway = _async_gateway(real_async_redis_client, dlq)
    stored = encode_stored_message("hello")
    await real_async_redis_client.lpush(pending, stored)

    claimed = await gateway.wait_for_message_and_move(pending, processing)

    assert claimed is not None
    assert claimed.stored_message == _stored_bytes(stored)
    assert claimed.lease_token
    count = await real_async_redis_client.hget(gateway._delivery_counts_key(processing), stored)
    assert _delivery_count(count) == 1


@pytest.mark.asyncio
async def test_async_claim_store_failure_rolls_back_and_raises(real_async_redis_client, queue_name):
    pending, processing, dlq = _keys(queue_name)
    gateway = _async_gateway(real_async_redis_client, dlq)
    stored = encode_stored_message("hello")
    await real_async_redis_client.lpush(pending, stored)
    await real_async_redis_client.set(gateway._lease_token_counter_key(processing), "not-an-int")

    with pytest.raises(ClaimStoreFailedError) as caught:
        await gateway.wait_for_message_and_move(pending, processing)

    assert caught.value.queue == pending
    assert caught.value.message_id == extract_stored_message_id(stored)
    assert caught.value.operation == "claim"
    assert "integer" in str(caught.value)
    count = await real_async_redis_client.hget(gateway._delivery_counts_key(processing), stored)
    assert _delivery_count(count) == 0
    assert await real_async_redis_client.lrange(pending, 0, -1) == [_stored_bytes(stored)]
    assert await real_async_redis_client.llen(processing) == 0
    assert await real_async_redis_client.lrange(dlq, 0, -1) == []


@pytest.mark.asyncio
async def test_async_repeated_store_failures_do_not_consume_delivery_attempts(real_async_redis_client, queue_name):
    pending, processing, dlq = _keys(queue_name)
    gateway = _async_gateway(real_async_redis_client, dlq)
    stored = encode_stored_message("hello")
    await real_async_redis_client.lpush(pending, stored)
    lease_counter = gateway._lease_token_counter_key(processing)
    await real_async_redis_client.set(lease_counter, "not-an-int")

    for _ in range(3):
        with pytest.raises(ClaimStoreFailedError):
            await gateway.wait_for_message_and_move(pending, processing)
        count = await real_async_redis_client.hget(gateway._delivery_counts_key(processing), stored)
        assert _delivery_count(count) == 0
        assert await real_async_redis_client.llen(pending) == 1
        assert await real_async_redis_client.llen(processing) == 0
        assert await real_async_redis_client.lrange(dlq, 0, -1) == []

    await real_async_redis_client.set(lease_counter, "0")

    claimed = await gateway.wait_for_message_and_move(pending, processing)

    assert claimed is not None
    assert claimed.stored_message == _stored_bytes(stored)
    count = await real_async_redis_client.hget(gateway._delivery_counts_key(processing), stored)
    assert _delivery_count(count) == 1
    assert await real_async_redis_client.llen(pending) == 0
    assert await real_async_redis_client.llen(processing) == 1
    assert await real_async_redis_client.lrange(dlq, 0, -1) == []
