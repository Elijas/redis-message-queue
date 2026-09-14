"""Behavioral contracts for callback deduplication and terminal retention limits."""

import inspect
from contextlib import asynccontextmanager

import fakeredis
import pytest

from redis_message_queue._redis_gateway import RedisGateway
from redis_message_queue.asyncio._redis_gateway import RedisGateway as AsyncRedisGateway
from redis_message_queue.asyncio.redis_message_queue import RedisMessageQueue as AsyncRedisMessageQueue
from redis_message_queue.redis_message_queue import RedisMessageQueue
from tests.test_process_message import FakeGateway
from tests.test_process_message_async import FakeAsyncGateway


async def _resolve(result):
    return await result if inspect.isawaitable(result) else result


@asynccontextmanager
async def _process(queue):
    context = queue.process_message()
    if isinstance(queue, AsyncRedisMessageQueue):
        async with context as message:
            yield message
    else:
        with context as message:
            yield message


class FalseyDedupKey:
    def __bool__(self):
        return False

    def __call__(self, message):
        return message


@pytest.fixture(params=[False, True], ids=["sync", "async"])
def queue_type(request):
    return AsyncRedisMessageQueue if request.param else RedisMessageQueue


@pytest.mark.asyncio
@pytest.mark.parametrize("use_gateway", [False, True], ids=["client", "gateway"])
@pytest.mark.parametrize("terminal", ["completed", "failed"])
@pytest.mark.parametrize("limit", ["omitted", 0, 2, None], ids=["default", "disabled", "bounded", "unlimited"])
async def test_retention_preserves_history_until_a_new_terminal_move(queue_type, use_gateway, terminal, limit):
    async_queue = queue_type is AsyncRedisMessageQueue
    client = fakeredis.FakeAsyncRedis() if async_queue else fakeredis.FakeRedis()
    options = {} if limit == "omitted" else {f"max_{terminal}_length": limit}
    if use_gateway:
        gateway_type = AsyncRedisGateway if async_queue else RedisGateway
        options["gateway"] = gateway_type(redis_client=client, retry_budget_seconds=0)
    else:
        options["client"] = client

    seed = queue_type("retention-history", client=client)
    history_key = getattr(seed.key, terminal)
    other_key = seed.key.failed if terminal == "completed" else seed.key.completed
    await _resolve(client.rpush(history_key, "older-1", "older-2", "older-3"))
    await _resolve(client.rpush(other_key, "unrelated-history"))

    queue = queue_type("retention-history", **options)
    assert await _resolve(client.lrange(history_key, 0, -1)) == [b"older-1", b"older-2", b"older-3"]

    # Omitted deduplication callback allows identical payloads to be processed independently.
    for _ in range(3):
        assert await _resolve(queue.publish("same")) is True
        if terminal == "failed":
            with pytest.raises(RuntimeError, match="handler failed"):
                async with _process(queue) as message:
                    assert message == b"same"
                    raise RuntimeError("handler failed")
        else:
            async with _process(queue) as message:
                assert message == b"same"

    history = await _resolve(client.lrange(history_key, 0, -1))
    if limit in ("omitted", 0):
        assert history == [b"older-1", b"older-2", b"older-3"]
    elif limit is None:
        assert len(history) == 6
        assert history.count(b"same") == 3
        assert all(value in history for value in [b"older-1", b"older-2", b"older-3"])
    else:
        assert history == [b"same", b"same"]
    assert await _resolve(client.lrange(other_key, 0, -1)) == [b"unrelated-history"]
    assert await _resolve(client.llen(queue.key.pending)) == 0
    assert await _resolve(client.llen(queue.key.processing)) == 0


class RecordingGateway(FakeGateway):
    def __init__(self):
        super().__init__()
        self.added = []
        self.dedup_keys = set()

    def add_message(self, queue, message):
        self.added.append(message)

    def publish_message(self, queue, message, dedup_key):
        if dedup_key in self.dedup_keys:
            return False
        self.dedup_keys.add(dedup_key)
        self.added.append(message)
        return True


class RecordingAsyncGateway(FakeAsyncGateway):
    def __init__(self):
        super().__init__()
        self.added = []
        self.dedup_keys = set()

    async def add_message(self, queue, message):
        self.added.append(message)

    async def publish_message(self, queue, message, dedup_key):
        if dedup_key in self.dedup_keys:
            return False
        self.dedup_keys.add(dedup_key)
        self.added.append(message)
        return True


@pytest.mark.asyncio
@pytest.mark.parametrize("callback", ["omitted", None, FalseyDedupKey()], ids=["default", "none", "falsey-callable"])
async def test_custom_gateway_deduplication_uses_callback_presence(queue_type, callback):
    gateway = RecordingAsyncGateway() if queue_type is AsyncRedisMessageQueue else RecordingGateway()
    options = {} if callback == "omitted" else {"get_deduplication_key": callback}
    queue = queue_type("custom-dedup", gateway=gateway, **options)

    assert await _resolve(queue.publish("same")) is True
    dedup_enabled = isinstance(callback, FalseyDedupKey)
    assert await _resolve(queue.publish("same")) is (not dedup_enabled)
    assert len(gateway.added) == (1 if dedup_enabled else 2)
    assert gateway.dedup_keys == ({queue.key.deduplication("same")} if dedup_enabled else set())


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal", ["completed", "failed"])
@pytest.mark.parametrize("limit", [0, 2, None], ids=["disabled", "bounded", "unlimited"])
async def test_custom_gateway_retention_routes_and_trims_only_when_enabled(queue_type, terminal, limit):
    gateway = RecordingAsyncGateway() if queue_type is AsyncRedisMessageQueue else RecordingGateway()
    gateway.message_to_return = b"payload"
    queue = queue_type("custom-retention", gateway=gateway, **{f"max_{terminal}_length": limit})

    if terminal == "failed":
        with pytest.raises(RuntimeError, match="handler failed"):
            async with _process(queue):
                raise RuntimeError("handler failed")
    else:
        async with _process(queue):
            pass

    if limit == 0:
        assert gateway.removed_messages == [(queue.key.processing, b"payload")]
        assert gateway.moved_messages == []
    else:
        assert gateway.removed_messages == []
        assert gateway.moved_messages == [(queue.key.processing, getattr(queue.key, terminal), b"payload")]
    assert gateway.trim_attempts == ([(getattr(queue.key, terminal), limit)] if limit == 2 else [])
