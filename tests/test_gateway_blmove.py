import fakeredis
import pytest

from redis_message_queue._redis_gateway import RedisGateway
from redis_message_queue.asyncio._redis_gateway import (
    RedisGateway as AsyncRedisGateway,
)


def _no_retry(func):
    """Identity decorator that bypasses tenacity retry wrapping."""
    return func


class RecordingSyncClient:
    def __init__(self, result=b"msg1"):
        self.result = result
        self.calls = []

    def lmove(self, from_queue, to_queue, src, dest):
        self.calls.append(("lmove", from_queue, to_queue, src, dest))
        return self.result

    def blmove(self, from_queue, to_queue, timeout, src, dest):
        self.calls.append(("blmove", from_queue, to_queue, timeout, src, dest))
        return self.result


class RecordingAsyncClient:
    def __init__(self, result=b"msg1"):
        self.result = result
        self.calls = []

    async def lmove(self, from_queue, to_queue, src, dest):
        self.calls.append(("lmove", from_queue, to_queue, src, dest))
        return self.result

    async def blmove(self, from_queue, to_queue, timeout, src, dest):
        self.calls.append(("blmove", from_queue, to_queue, timeout, src, dest))
        return self.result


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

    def test_zero_timeout_uses_non_blocking_lmove(self):
        client = RecordingSyncClient()
        gw = self._make_gateway(client, wait_interval=0)

        result = gw.wait_for_message_and_move("src_queue", "dst_queue")

        assert result == b"msg1"
        assert client.calls == [("lmove", "src_queue", "dst_queue", "RIGHT", "LEFT")]

    def test_positive_timeout_uses_blmove(self):
        client = RecordingSyncClient()
        gw = self._make_gateway(client, wait_interval=5)

        result = gw.wait_for_message_and_move("src_queue", "dst_queue")

        assert result == b"msg1"
        assert client.calls == [("blmove", "src_queue", "dst_queue", 5, "RIGHT", "LEFT")]


class TestAsyncGatewayWaitForMessageAndMove:
    def _make_gateway(self, client, wait_interval=0):
        return AsyncRedisGateway(
            redis_client=client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=wait_interval,
        )

    @pytest.mark.asyncio
    async def test_moves_message_from_source_to_destination(self):
        client = fakeredis.FakeAsyncRedis()
        gw = self._make_gateway(client)
        await client.lpush("src_queue", "msg1")

        result = await gw.wait_for_message_and_move("src_queue", "dst_queue")

        assert result == b"msg1"
        assert await client.llen("src_queue") == 0
        assert await client.llen("dst_queue") == 1
        assert await client.lindex("dst_queue", 0) == b"msg1"

    @pytest.mark.asyncio
    async def test_returns_none_on_timeout(self):
        client = fakeredis.FakeAsyncRedis()
        gw = self._make_gateway(client, wait_interval=0)

        result = await gw.wait_for_message_and_move("empty_queue", "dst_queue")

        assert result is None

    @pytest.mark.asyncio
    async def test_moves_from_right_to_left(self):
        """BLMOVE with src=RIGHT, dest=LEFT gives FIFO order."""
        client = fakeredis.FakeAsyncRedis()
        gw = self._make_gateway(client)
        await client.lpush("src_queue", "msg1")
        await client.lpush("src_queue", "msg2")

        first = await gw.wait_for_message_and_move("src_queue", "dst_queue")
        second = await gw.wait_for_message_and_move("src_queue", "dst_queue")

        assert first == b"msg1"
        assert second == b"msg2"

    @pytest.mark.asyncio
    async def test_zero_timeout_uses_non_blocking_lmove(self):
        client = RecordingAsyncClient()
        gw = self._make_gateway(client, wait_interval=0)

        result = await gw.wait_for_message_and_move("src_queue", "dst_queue")

        assert result == b"msg1"
        assert client.calls == [("lmove", "src_queue", "dst_queue", "RIGHT", "LEFT")]

    @pytest.mark.asyncio
    async def test_positive_timeout_uses_blmove(self):
        client = RecordingAsyncClient()
        gw = self._make_gateway(client, wait_interval=5)

        result = await gw.wait_for_message_and_move("src_queue", "dst_queue")

        assert result == b"msg1"
        assert client.calls == [("blmove", "src_queue", "dst_queue", 5, "RIGHT", "LEFT")]
