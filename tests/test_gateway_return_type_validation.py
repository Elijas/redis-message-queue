"""Tests for gateway return-type validation and fail-closed behavior.

Ensures that custom gateway subclasses returning wrong types from their
methods cause immediate, clear TypeErrors rather than silent misbehavior
deep in queue internals.

Covers:
- F1: wait_for_message_and_move return-type validation
- F2: renew_message_lease return-type validation in heartbeat
- F3: move_message/remove_message return-type validation
- F4: publish_message return-type validation
- F5: Heartbeat configured with no lease token → warning
- F6: add_message return-type validation
"""

import asyncio
import logging
import time

import pytest

from redis_message_queue._abstract_redis_gateway import (
    AbstractRedisGateway as SyncAbstractRedisGateway,
)
from redis_message_queue._stored_message import ClaimedMessage, MessageData
from redis_message_queue.asyncio._abstract_redis_gateway import (
    AbstractRedisGateway as AsyncAbstractRedisGateway,
)
from redis_message_queue.asyncio.redis_message_queue import (
    RedisMessageQueue as AsyncRedisMessageQueue,
)
from redis_message_queue.redis_message_queue import RedisMessageQueue

_UNSET = object()


# ---------------------------------------------------------------------------
# Configurable gateways for injecting specific return-value violations
# ---------------------------------------------------------------------------


class _SyncConfigurableGateway(SyncAbstractRedisGateway):
    message_visibility_timeout_seconds = 10

    def __init__(
        self,
        *,
        wait_return=_UNSET,
        publish_return=_UNSET,
        add_message_return=_UNSET,
        move_return=_UNSET,
        remove_return=_UNSET,
        renew_return=_UNSET,
        use_claimed_message=True,
    ):
        self._message: str | None = None
        self._wait_return = wait_return
        self._publish_return = publish_return
        self._add_message_return = add_message_return
        self._move_return = move_return
        self._remove_return = remove_return
        self._renew_return = renew_return
        self._use_claimed_message = use_claimed_message

    def publish_message(self, queue: str, message: str, dedup_key: str) -> bool:
        self._message = message
        if self._publish_return is not _UNSET:
            return self._publish_return
        return True

    def add_message(self, queue: str, message: str) -> None:
        self._message = message
        if self._add_message_return is not _UNSET:
            return self._add_message_return

    def move_message(
        self, from_queue: str, to_queue: str, message: MessageData, *, lease_token: str | None = None
    ) -> bool:
        if self._move_return is not _UNSET:
            return self._move_return
        return True

    def remove_message(self, queue: str, message: MessageData, *, lease_token: str | None = None) -> bool:
        if self._remove_return is not _UNSET:
            return self._remove_return
        return True

    def renew_message_lease(self, queue: str, message: MessageData, lease_token: str) -> bool:
        if self._renew_return is not _UNSET:
            return self._renew_return
        return True

    def wait_for_message_and_move(self, from_queue: str, to_queue: str) -> ClaimedMessage | MessageData | None:
        if self._wait_return is not _UNSET:
            self._message = None
            return self._wait_return
        if self._message is None:
            return None
        msg = self._message
        self._message = None
        if self._use_claimed_message:
            return ClaimedMessage(stored_message=msg, lease_token="fake-token")
        return msg


class _AsyncConfigurableGateway(AsyncAbstractRedisGateway):
    message_visibility_timeout_seconds = 10

    def __init__(
        self,
        *,
        wait_return=_UNSET,
        publish_return=_UNSET,
        add_message_return=_UNSET,
        move_return=_UNSET,
        remove_return=_UNSET,
        renew_return=_UNSET,
        use_claimed_message=True,
    ):
        self._message: str | None = None
        self._wait_return = wait_return
        self._publish_return = publish_return
        self._add_message_return = add_message_return
        self._move_return = move_return
        self._remove_return = remove_return
        self._renew_return = renew_return
        self._use_claimed_message = use_claimed_message

    async def publish_message(self, queue: str, message: str, dedup_key: str) -> bool:
        self._message = message
        if self._publish_return is not _UNSET:
            return self._publish_return
        return True

    async def add_message(self, queue: str, message: str) -> None:
        self._message = message
        if self._add_message_return is not _UNSET:
            return self._add_message_return

    async def move_message(
        self, from_queue: str, to_queue: str, message: MessageData, *, lease_token: str | None = None
    ) -> bool:
        if self._move_return is not _UNSET:
            return self._move_return
        return True

    async def remove_message(self, queue: str, message: MessageData, *, lease_token: str | None = None) -> bool:
        if self._remove_return is not _UNSET:
            return self._remove_return
        return True

    async def renew_message_lease(self, queue: str, message: MessageData, lease_token: str) -> bool:
        if self._renew_return is not _UNSET:
            return self._renew_return
        return True

    async def wait_for_message_and_move(self, from_queue: str, to_queue: str) -> ClaimedMessage | MessageData | None:
        if self._wait_return is not _UNSET:
            self._message = None
            return self._wait_return
        if self._message is None:
            return None
        msg = self._message
        self._message = None
        if self._use_claimed_message:
            return ClaimedMessage(stored_message=msg, lease_token="fake-token")
        return msg


# ---------------------------------------------------------------------------
# F1: wait_for_message_and_move return-type validation
# ---------------------------------------------------------------------------


class TestSyncWaitReturnTypeValidation:
    def test_returns_int_raises_type_error(self):
        gateway = _SyncConfigurableGateway(wait_return=42)
        q = RedisMessageQueue("test", gateway=gateway)
        q.publish("hello")
        with pytest.raises(TypeError, match=r"wait_for_message_and_move.*int"):
            with q.process_message() as _msg:
                pass

    def test_returns_true_raises_type_error(self):
        """bool is an int subclass — True is not ClaimedMessage/str/bytes."""
        gateway = _SyncConfigurableGateway(wait_return=True)
        q = RedisMessageQueue("test", gateway=gateway)
        q.publish("hello")
        with pytest.raises(TypeError, match=r"wait_for_message_and_move.*bool"):
            with q.process_message() as _msg:
                pass

    def test_returns_dict_raises_type_error(self):
        gateway = _SyncConfigurableGateway(wait_return={"key": "value"})
        q = RedisMessageQueue("test", gateway=gateway)
        q.publish("hello")
        with pytest.raises(TypeError, match=r"wait_for_message_and_move.*dict"):
            with q.process_message() as _msg:
                pass


class TestAsyncWaitReturnTypeValidation:
    @pytest.mark.asyncio
    async def test_returns_int_raises_type_error(self):
        gateway = _AsyncConfigurableGateway(wait_return=42)
        q = AsyncRedisMessageQueue("test", gateway=gateway)
        await q.publish("hello")
        with pytest.raises(TypeError, match=r"wait_for_message_and_move.*int"):
            async with q.process_message() as _msg:
                pass

    @pytest.mark.asyncio
    async def test_returns_true_raises_type_error(self):
        """bool is an int subclass — True is not ClaimedMessage/str/bytes."""
        gateway = _AsyncConfigurableGateway(wait_return=True)
        q = AsyncRedisMessageQueue("test", gateway=gateway)
        await q.publish("hello")
        with pytest.raises(TypeError, match=r"wait_for_message_and_move.*bool"):
            async with q.process_message() as _msg:
                pass

    @pytest.mark.asyncio
    async def test_returns_dict_raises_type_error(self):
        gateway = _AsyncConfigurableGateway(wait_return={"key": "value"})
        q = AsyncRedisMessageQueue("test", gateway=gateway)
        await q.publish("hello")
        with pytest.raises(TypeError, match=r"wait_for_message_and_move.*dict"):
            async with q.process_message() as _msg:
                pass


# ---------------------------------------------------------------------------
# F2: renew_message_lease return-type validation in heartbeat
# ---------------------------------------------------------------------------


class TestSyncRenewReturnTypeValidation:
    def test_returns_int_heartbeat_stops_with_logged_error(self, caplog):
        gateway = _SyncConfigurableGateway(renew_return=1)
        q = RedisMessageQueue("test", gateway=gateway, heartbeat_interval_seconds=0.02)
        q.publish("hello")
        with caplog.at_level(logging.ERROR):
            with q.process_message() as msg:
                assert msg == "hello"
                time.sleep(0.1)
        assert "gateway.renew_message_lease() must return bool" in caplog.text

    def test_returns_none_heartbeat_stops_with_logged_error(self, caplog):
        gateway = _SyncConfigurableGateway(renew_return=None)
        q = RedisMessageQueue("test", gateway=gateway, heartbeat_interval_seconds=0.02)
        q.publish("hello")
        with caplog.at_level(logging.ERROR):
            with q.process_message() as msg:
                assert msg == "hello"
                time.sleep(0.1)
        assert "gateway.renew_message_lease() must return bool" in caplog.text

    def test_returns_string_heartbeat_stops_with_logged_error(self, caplog):
        gateway = _SyncConfigurableGateway(renew_return="ok")
        q = RedisMessageQueue("test", gateway=gateway, heartbeat_interval_seconds=0.02)
        q.publish("hello")
        with caplog.at_level(logging.ERROR):
            with q.process_message() as msg:
                assert msg == "hello"
                time.sleep(0.1)
        assert "gateway.renew_message_lease() must return bool" in caplog.text


class TestAsyncRenewReturnTypeValidation:
    @pytest.mark.asyncio
    async def test_returns_int_heartbeat_stops_with_logged_error(self, caplog):
        gateway = _AsyncConfigurableGateway(renew_return=1)
        q = AsyncRedisMessageQueue("test", gateway=gateway, heartbeat_interval_seconds=0.02)
        await q.publish("hello")
        with caplog.at_level(logging.ERROR):
            async with q.process_message() as msg:
                assert msg == "hello"
                await asyncio.sleep(0.1)
        assert "gateway.renew_message_lease() must return bool" in caplog.text

    @pytest.mark.asyncio
    async def test_returns_none_heartbeat_stops_with_logged_error(self, caplog):
        gateway = _AsyncConfigurableGateway(renew_return=None)
        q = AsyncRedisMessageQueue("test", gateway=gateway, heartbeat_interval_seconds=0.02)
        await q.publish("hello")
        with caplog.at_level(logging.ERROR):
            async with q.process_message() as msg:
                assert msg == "hello"
                await asyncio.sleep(0.1)
        assert "gateway.renew_message_lease() must return bool" in caplog.text

    @pytest.mark.asyncio
    async def test_returns_string_heartbeat_stops_with_logged_error(self, caplog):
        gateway = _AsyncConfigurableGateway(renew_return="ok")
        q = AsyncRedisMessageQueue("test", gateway=gateway, heartbeat_interval_seconds=0.02)
        await q.publish("hello")
        with caplog.at_level(logging.ERROR):
            async with q.process_message() as msg:
                assert msg == "hello"
                await asyncio.sleep(0.1)
        assert "gateway.renew_message_lease() must return bool" in caplog.text


# ---------------------------------------------------------------------------
# F3: move_message / remove_message return-type validation
# ---------------------------------------------------------------------------


class TestSyncMoveRemoveReturnTypeValidation:
    def test_move_returns_int_on_success_path(self):
        gateway = _SyncConfigurableGateway(move_return=1)
        q = RedisMessageQueue("test", gateway=gateway, enable_completed_queue=True)
        q.publish("hello")
        with pytest.raises(TypeError, match=r"gateway\.move_message\(\) must return bool.*int"):
            with q.process_message() as _msg:
                pass

    def test_remove_returns_none_on_success_path(self):
        gateway = _SyncConfigurableGateway(remove_return=None)
        q = RedisMessageQueue("test", gateway=gateway)
        q.publish("hello")
        with pytest.raises(TypeError, match=r"gateway\.remove_message\(\) must return bool.*NoneType"):
            with q.process_message() as _msg:
                pass

    def test_move_returns_int_on_exception_path_logged_and_user_error_propagates(self, caplog):
        """On exception path, TypeError from validation is caught and logged;
        the original user exception still propagates."""
        gateway = _SyncConfigurableGateway(move_return=1)
        q = RedisMessageQueue("test", gateway=gateway, enable_failed_queue=True)
        q.publish("hello")
        with caplog.at_level(logging.ERROR):
            with pytest.raises(RuntimeError, match="user error"):
                with q.process_message() as _msg:
                    raise RuntimeError("user error")
        assert "gateway.move_message() must return bool" in caplog.text


class TestAsyncMoveRemoveReturnTypeValidation:
    @pytest.mark.asyncio
    async def test_move_returns_int_on_success_path(self):
        gateway = _AsyncConfigurableGateway(move_return=1)
        q = AsyncRedisMessageQueue("test", gateway=gateway, enable_completed_queue=True)
        await q.publish("hello")
        with pytest.raises(TypeError, match=r"gateway\.move_message\(\) must return bool.*int"):
            async with q.process_message() as _msg:
                pass

    @pytest.mark.asyncio
    async def test_remove_returns_none_on_success_path(self):
        gateway = _AsyncConfigurableGateway(remove_return=None)
        q = AsyncRedisMessageQueue("test", gateway=gateway)
        await q.publish("hello")
        with pytest.raises(TypeError, match=r"gateway\.remove_message\(\) must return bool.*NoneType"):
            async with q.process_message() as _msg:
                pass

    @pytest.mark.asyncio
    async def test_move_returns_int_on_exception_path_logged_and_user_error_propagates(self, caplog):
        gateway = _AsyncConfigurableGateway(move_return=1)
        q = AsyncRedisMessageQueue("test", gateway=gateway, enable_failed_queue=True)
        await q.publish("hello")
        with caplog.at_level(logging.ERROR):
            with pytest.raises(RuntimeError, match="user error"):
                async with q.process_message() as _msg:
                    raise RuntimeError("user error")
        assert "gateway.move_message() must return bool" in caplog.text


# ---------------------------------------------------------------------------
# F4: publish_message return-type validation
# ---------------------------------------------------------------------------


class TestSyncPublishReturnTypeValidation:
    def test_returns_int_raises_type_error(self):
        gateway = _SyncConfigurableGateway(publish_return=1)
        q = RedisMessageQueue("test", gateway=gateway)
        with pytest.raises(TypeError, match=r"gateway\.publish_message\(\) must return bool.*int"):
            q.publish("hello")


class TestAsyncPublishReturnTypeValidation:
    @pytest.mark.asyncio
    async def test_returns_int_raises_type_error(self):
        gateway = _AsyncConfigurableGateway(publish_return=1)
        q = AsyncRedisMessageQueue("test", gateway=gateway)
        with pytest.raises(TypeError, match=r"gateway\.publish_message\(\) must return bool.*int"):
            await q.publish("hello")


# ---------------------------------------------------------------------------
# F5: Heartbeat configured + no lease token → warning
# ---------------------------------------------------------------------------


class TestSyncHeartbeatNoLeaseWarning:
    def test_warning_logged_when_plain_message_data_returned(self, caplog):
        gateway = _SyncConfigurableGateway(use_claimed_message=False)
        q = RedisMessageQueue("test", gateway=gateway, heartbeat_interval_seconds=2)
        q.publish("msg1")
        with caplog.at_level(logging.WARNING):
            with q.process_message() as msg:
                assert msg == "msg1"
        assert caplog.text.count("Heartbeat is configured but the gateway returned no lease token") == 1

    def test_warning_logged_only_once_across_messages(self, caplog):
        gateway = _SyncConfigurableGateway(use_claimed_message=False)
        q = RedisMessageQueue("test", gateway=gateway, heartbeat_interval_seconds=2)

        q.publish("msg1")
        with caplog.at_level(logging.WARNING):
            with q.process_message() as msg:
                assert msg == "msg1"

        q.publish("msg2")
        with caplog.at_level(logging.WARNING):
            with q.process_message() as msg:
                assert msg == "msg2"

        assert caplog.text.count("Heartbeat is configured but the gateway returned no lease token") == 1

    def test_no_warning_with_claimed_message(self, caplog):
        gateway = _SyncConfigurableGateway()
        q = RedisMessageQueue("test", gateway=gateway, heartbeat_interval_seconds=2)
        q.publish("hello")
        with caplog.at_level(logging.WARNING):
            with q.process_message() as msg:
                assert msg == "hello"
        assert "Heartbeat is configured but the gateway returned no lease token" not in caplog.text


class TestAsyncHeartbeatNoLeaseWarning:
    @pytest.mark.asyncio
    async def test_warning_logged_when_plain_message_data_returned(self, caplog):
        gateway = _AsyncConfigurableGateway(use_claimed_message=False)
        q = AsyncRedisMessageQueue("test", gateway=gateway, heartbeat_interval_seconds=2)
        await q.publish("msg1")
        with caplog.at_level(logging.WARNING):
            async with q.process_message() as msg:
                assert msg == "msg1"
        assert caplog.text.count("Heartbeat is configured but the gateway returned no lease token") == 1

    @pytest.mark.asyncio
    async def test_warning_logged_only_once_across_messages(self, caplog):
        gateway = _AsyncConfigurableGateway(use_claimed_message=False)
        q = AsyncRedisMessageQueue("test", gateway=gateway, heartbeat_interval_seconds=2)

        await q.publish("msg1")
        with caplog.at_level(logging.WARNING):
            async with q.process_message() as msg:
                assert msg == "msg1"

        await q.publish("msg2")
        with caplog.at_level(logging.WARNING):
            async with q.process_message() as msg:
                assert msg == "msg2"

        assert caplog.text.count("Heartbeat is configured but the gateway returned no lease token") == 1

    @pytest.mark.asyncio
    async def test_no_warning_with_claimed_message(self, caplog):
        gateway = _AsyncConfigurableGateway()
        q = AsyncRedisMessageQueue("test", gateway=gateway, heartbeat_interval_seconds=2)
        await q.publish("hello")
        with caplog.at_level(logging.WARNING):
            async with q.process_message() as msg:
                assert msg == "hello"
        assert "Heartbeat is configured but the gateway returned no lease token" not in caplog.text


# ---------------------------------------------------------------------------
# F6: add_message return-type validation
# ---------------------------------------------------------------------------


class TestSyncAddMessageReturnTypeValidation:
    def test_returns_true_raises_type_error(self):
        gateway = _SyncConfigurableGateway(add_message_return=True)
        q = RedisMessageQueue("test", gateway=gateway, deduplication=False)
        with pytest.raises(TypeError, match=r"gateway\.add_message\(\) must return None.*bool"):
            q.publish("hello")

    def test_returns_string_raises_type_error(self):
        gateway = _SyncConfigurableGateway(add_message_return="ok")
        q = RedisMessageQueue("test", gateway=gateway, deduplication=False)
        with pytest.raises(TypeError, match=r"gateway\.add_message\(\) must return None.*str"):
            q.publish("hello")

    def test_returns_none_succeeds(self):
        gateway = _SyncConfigurableGateway()
        q = RedisMessageQueue("test", gateway=gateway, deduplication=False)
        assert q.publish("hello") is True


class TestAsyncAddMessageReturnTypeValidation:
    @pytest.mark.asyncio
    async def test_returns_true_raises_type_error(self):
        gateway = _AsyncConfigurableGateway(add_message_return=True)
        q = AsyncRedisMessageQueue("test", gateway=gateway, deduplication=False)
        with pytest.raises(TypeError, match=r"gateway\.add_message\(\) must return None.*bool"):
            await q.publish("hello")

    @pytest.mark.asyncio
    async def test_returns_string_raises_type_error(self):
        gateway = _AsyncConfigurableGateway(add_message_return="ok")
        q = AsyncRedisMessageQueue("test", gateway=gateway, deduplication=False)
        with pytest.raises(TypeError, match=r"gateway\.add_message\(\) must return None.*str"):
            await q.publish("hello")

    @pytest.mark.asyncio
    async def test_returns_none_succeeds(self):
        gateway = _AsyncConfigurableGateway()
        q = AsyncRedisMessageQueue("test", gateway=gateway, deduplication=False)
        assert await q.publish("hello") is True
