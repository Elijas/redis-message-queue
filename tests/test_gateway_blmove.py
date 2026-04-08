import fakeredis
import pytest
import redis.exceptions

from redis_message_queue._redis_gateway import RedisGateway
from redis_message_queue.asyncio._redis_gateway import (
    RedisGateway as AsyncRedisGateway,
)
from redis_message_queue.interrupt_handler._interface import BaseGracefulInterruptHandler


def _no_retry(func):
    """Identity decorator that bypasses tenacity retry wrapping."""

    return func


class RecordingSyncClient:
    def __init__(self, result=b"msg1"):
        self.result = result
        self.calls = []

    def eval(self, script, numkeys, *args):
        self.calls.append(("eval", numkeys, args))
        return self.result

    def delete(self, key):
        self.calls.append(("delete", key))
        return 1


class RecordingAsyncClient:
    def __init__(self, result=b"msg1"):
        self.result = result
        self.calls = []

    async def eval(self, script, numkeys, *args):
        self.calls.append(("eval", numkeys, args))
        return self.result

    async def delete(self, key):
        self.calls.append(("delete", key))
        return 1


class UnsupportedCommandSyncClient:
    def eval(self, script, numkeys, *args):
        raise redis.exceptions.ResponseError("unknown command 'EVAL'")


class UnsupportedCommandAsyncClient:
    async def eval(self, script, numkeys, *args):
        raise redis.exceptions.ResponseError("unknown command 'EVAL'")


class InterruptedHandler(BaseGracefulInterruptHandler):
    def is_interrupted(self) -> bool:
        return True


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
        """The claim script preserves FIFO order by moving RIGHT -> LEFT."""
        client = fakeredis.FakeRedis()
        gw = self._make_gateway(client)
        client.lpush("src_queue", "msg1")
        client.lpush("src_queue", "msg2")

        first = gw.wait_for_message_and_move("src_queue", "dst_queue")
        second = gw.wait_for_message_and_move("src_queue", "dst_queue")

        assert first == b"msg1"
        assert second == b"msg2"

    def test_zero_timeout_uses_eval_claim_script(self):
        client = RecordingSyncClient()
        gw = self._make_gateway(client, wait_interval=0)

        result = gw.wait_for_message_and_move("src_queue", "dst_queue")

        assert result == b"msg1"
        assert len(client.calls) == 2
        assert client.calls[0][0] == "eval"
        assert client.calls[0][1] == 3
        assert client.calls[0][2][0:2] == ("src_queue", "dst_queue")
        assert client.calls[1][0] == "delete"

    def test_positive_timeout_uses_polling_claim_script(self):
        client = RecordingSyncClient()
        gw = self._make_gateway(client, wait_interval=5)

        result = gw.wait_for_message_and_move("src_queue", "dst_queue")

        assert result == b"msg1"
        assert len(client.calls) == 2
        assert client.calls[0][0] == "eval"
        assert client.calls[0][1] == 3
        assert client.calls[1][0] == "delete"

    def test_unsupported_command_error_propagates(self):
        client = UnsupportedCommandSyncClient()
        gw = self._make_gateway(client, wait_interval=5)

        with pytest.raises(redis.exceptions.ResponseError, match="unknown command"):
            gw.wait_for_message_and_move("src_queue", "dst_queue")

    def test_custom_retry_and_interrupt_skip_polling(self):
        client = RecordingSyncClient()
        gw = RedisGateway(
            redis_client=client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=5,
            interrupt=InterruptedHandler(),
        )

        assert gw.wait_for_message_and_move("src_queue", "dst_queue") is None
        assert client.calls == []


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
        """The claim script preserves FIFO order by moving RIGHT -> LEFT."""
        client = fakeredis.FakeAsyncRedis()
        gw = self._make_gateway(client)
        await client.lpush("src_queue", "msg1")
        await client.lpush("src_queue", "msg2")

        first = await gw.wait_for_message_and_move("src_queue", "dst_queue")
        second = await gw.wait_for_message_and_move("src_queue", "dst_queue")

        assert first == b"msg1"
        assert second == b"msg2"

    @pytest.mark.asyncio
    async def test_zero_timeout_uses_eval_claim_script(self):
        client = RecordingAsyncClient()
        gw = self._make_gateway(client, wait_interval=0)

        result = await gw.wait_for_message_and_move("src_queue", "dst_queue")

        assert result == b"msg1"
        assert len(client.calls) == 2
        assert client.calls[0][0] == "eval"
        assert client.calls[0][1] == 3
        assert client.calls[0][2][0:2] == ("src_queue", "dst_queue")
        assert client.calls[1][0] == "delete"

    @pytest.mark.asyncio
    async def test_positive_timeout_uses_polling_claim_script(self):
        client = RecordingAsyncClient()
        gw = self._make_gateway(client, wait_interval=5)

        result = await gw.wait_for_message_and_move("src_queue", "dst_queue")

        assert result == b"msg1"
        assert len(client.calls) == 2
        assert client.calls[0][0] == "eval"
        assert client.calls[0][1] == 3
        assert client.calls[1][0] == "delete"

    @pytest.mark.asyncio
    async def test_unsupported_command_error_propagates(self):
        client = UnsupportedCommandAsyncClient()
        gw = self._make_gateway(client, wait_interval=5)

        with pytest.raises(redis.exceptions.ResponseError, match="unknown command"):
            await gw.wait_for_message_and_move("src_queue", "dst_queue")

    @pytest.mark.asyncio
    async def test_custom_retry_and_interrupt_skip_polling(self):
        client = RecordingAsyncClient()
        gw = AsyncRedisGateway(
            redis_client=client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=5,
            interrupt=InterruptedHandler(),
        )

        assert await gw.wait_for_message_and_move("src_queue", "dst_queue") is None
        assert client.calls == []
