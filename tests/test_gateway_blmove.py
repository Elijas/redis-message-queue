import fakeredis
import fakeredis.aioredis
import pytest

from redis_message_queue._redis_gateway import RedisGateway
from redis_message_queue.asyncio._redis_gateway import (
    RedisGateway as AsyncRedisGateway,
)


def _no_retry(func):
    """Identity decorator that bypasses tenacity retry wrapping."""
    return func


class TestSyncGatewayWaitForMessageAndMove:
    def _make_gateway(self, client, wait_interval=0):
        return RedisGateway(
            redis_client=client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=wait_interval,
        )

    def test_moves_message_from_source_to_destination(self):
        client = fakeredis.FakeRedis()
        gw = self._make_gateway(client)
        client.lpush("src_queue", "msg1")

        result = gw.wait_for_message_and_move("src_queue", "dst_queue")

        assert result == b"msg1"
        assert client.llen("src_queue") == 0
        assert client.llen("dst_queue") == 1
        assert client.lindex("dst_queue", 0) == b"msg1"

    def test_returns_none_on_timeout(self):
        client = fakeredis.FakeRedis()
        gw = self._make_gateway(client, wait_interval=0)

        result = gw.wait_for_message_and_move("empty_queue", "dst_queue")

        assert result is None

    def test_moves_from_right_to_left(self):
        """BLMOVE with src=RIGHT, dest=LEFT gives FIFO order."""
        client = fakeredis.FakeRedis()
        gw = self._make_gateway(client)
        # lpush pushes to the left, so the list is [msg2, msg1]
        # RIGHT pop should yield msg1 first (oldest), then msg2
        client.lpush("src_queue", "msg1")
        client.lpush("src_queue", "msg2")

        first = gw.wait_for_message_and_move("src_queue", "dst_queue")
        second = gw.wait_for_message_and_move("src_queue", "dst_queue")

        assert first == b"msg1"
        assert second == b"msg2"


class TestAsyncGatewayWaitForMessageAndMove:
    def _make_gateway(self, client, wait_interval=0):
        return AsyncRedisGateway(
            redis_client=client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=wait_interval,
        )

    @pytest.mark.asyncio
    async def test_moves_message_from_source_to_destination(self):
        client = fakeredis.aioredis.FakeRedis()
        gw = self._make_gateway(client)
        await client.lpush("src_queue", "msg1")

        result = await gw.wait_for_message_and_move("src_queue", "dst_queue")

        assert result == b"msg1"
        assert await client.llen("src_queue") == 0
        assert await client.llen("dst_queue") == 1
        assert await client.lindex("dst_queue", 0) == b"msg1"

    @pytest.mark.asyncio
    async def test_returns_none_on_timeout(self):
        client = fakeredis.aioredis.FakeRedis()
        gw = self._make_gateway(client, wait_interval=0)

        result = await gw.wait_for_message_and_move("empty_queue", "dst_queue")

        assert result is None

    @pytest.mark.asyncio
    async def test_moves_from_right_to_left(self):
        """BLMOVE with src=RIGHT, dest=LEFT gives FIFO order."""
        client = fakeredis.aioredis.FakeRedis()
        gw = self._make_gateway(client)
        await client.lpush("src_queue", "msg1")
        await client.lpush("src_queue", "msg2")

        first = await gw.wait_for_message_and_move("src_queue", "dst_queue")
        second = await gw.wait_for_message_and_move("src_queue", "dst_queue")

        assert first == b"msg1"
        assert second == b"msg2"
