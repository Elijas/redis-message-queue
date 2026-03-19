"""Tests for custom gateway behavioral contracts and ClaimedMessage validation.

Exercises edge cases that arise when extending AbstractRedisGateway:
- ClaimedMessage construction validation (fail-fast on invalid fields)
- Lease-ignoring gateways (documents the undetectable gap)
- Always-True renewal gateways (heartbeat never self-stops)
- Duck-type check edge cases on message_visibility_timeout_seconds
"""

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

HEARTBEAT_THREAD_NAME = "redis-message-queue-lease-heartbeat"


# ---------------------------------------------------------------------------
# Step 1: ClaimedMessage.__post_init__ validation
# ---------------------------------------------------------------------------


class TestClaimedMessageValidation:
    def test_none_lease_token_raises_type_error(self):
        with pytest.raises(TypeError, match="lease_token"):
            ClaimedMessage(stored_message="msg", lease_token=None)

    def test_int_lease_token_raises_type_error(self):
        with pytest.raises(TypeError, match="lease_token"):
            ClaimedMessage(stored_message="msg", lease_token=42)

    def test_empty_lease_token_raises_value_error(self):
        with pytest.raises(ValueError, match="lease_token"):
            ClaimedMessage(stored_message="msg", lease_token="")

    def test_none_stored_message_raises_type_error(self):
        with pytest.raises(TypeError, match="stored_message"):
            ClaimedMessage(stored_message=None, lease_token="tk1")

    def test_int_stored_message_raises_type_error(self):
        with pytest.raises(TypeError, match="stored_message"):
            ClaimedMessage(stored_message=123, lease_token="tk1")

    def test_valid_str_stored_message(self):
        cm = ClaimedMessage(stored_message="hello", lease_token="tk1")
        assert cm.stored_message == "hello"
        assert cm.lease_token == "tk1"

    def test_valid_bytes_stored_message(self):
        cm = ClaimedMessage(stored_message=b"hello", lease_token="tk1")
        assert cm.stored_message == b"hello"
        assert cm.lease_token == "tk1"


# ---------------------------------------------------------------------------
# Helpers: custom gateways for contract violation tests
# ---------------------------------------------------------------------------


class _SyncLeaseIgnoringGateway(SyncAbstractRedisGateway):
    """Gateway that returns ClaimedMessage but ignores lease_token in ack ops.

    Simulates a custom gateway that structurally supports leases (returns
    ClaimedMessage) but does not actually validate lease_token in
    move_message/remove_message — always returns True regardless.
    """

    message_visibility_timeout_seconds = 10

    def __init__(self) -> None:
        self._message: str | None = None
        self.remove_calls: list[str | None] = []
        self.move_calls: list[str | None] = []

    def publish_message(self, queue: str, message: str, dedup_key: str) -> bool:
        self._message = message
        return True

    def add_message(self, queue: str, message: str) -> None:
        self._message = message

    def move_message(
        self,
        from_queue: str,
        to_queue: str,
        message: MessageData,
        *,
        lease_token: str | None = None,
    ) -> bool:
        self.move_calls.append(lease_token)
        return True  # ignores lease_token

    def remove_message(self, queue: str, message: MessageData, *, lease_token: str | None = None) -> bool:
        self.remove_calls.append(lease_token)
        return True  # ignores lease_token

    def renew_message_lease(self, queue: str, message: MessageData, lease_token: str) -> bool:
        return True

    def wait_for_message_and_move(self, from_queue: str, to_queue: str) -> ClaimedMessage | MessageData | None:
        if self._message is None:
            return None
        msg = self._message
        self._message = None
        return ClaimedMessage(stored_message=msg, lease_token="fake-token")


class _AsyncLeaseIgnoringGateway(AsyncAbstractRedisGateway):
    """Async version of _SyncLeaseIgnoringGateway."""

    message_visibility_timeout_seconds = 10

    def __init__(self) -> None:
        self._message: str | None = None
        self.remove_calls: list[str | None] = []
        self.move_calls: list[str | None] = []

    async def publish_message(self, queue: str, message: str, dedup_key: str) -> bool:
        self._message = message
        return True

    async def add_message(self, queue: str, message: str) -> None:
        self._message = message

    async def move_message(
        self,
        from_queue: str,
        to_queue: str,
        message: MessageData,
        *,
        lease_token: str | None = None,
    ) -> bool:
        self.move_calls.append(lease_token)
        return True

    async def remove_message(self, queue: str, message: MessageData, *, lease_token: str | None = None) -> bool:
        self.remove_calls.append(lease_token)
        return True

    async def renew_message_lease(self, queue: str, message: MessageData, lease_token: str) -> bool:
        return True

    async def wait_for_message_and_move(self, from_queue: str, to_queue: str) -> ClaimedMessage | MessageData | None:
        if self._message is None:
            return None
        msg = self._message
        self._message = None
        return ClaimedMessage(stored_message=msg, lease_token="fake-token")


class _SyncAlwaysTrueRenewalGateway(SyncAbstractRedisGateway):
    """Gateway where renew_message_lease always returns True.

    Demonstrates that the heartbeat never self-stops when renewal is
    unconditionally True, defeating mutual exclusion safety.
    """

    message_visibility_timeout_seconds = 10

    def __init__(self) -> None:
        self._message: str | None = None
        self.renewal_count = 0

    def publish_message(self, queue: str, message: str, dedup_key: str) -> bool:
        self._message = message
        return True

    def add_message(self, queue: str, message: str) -> None:
        self._message = message

    def move_message(
        self,
        from_queue: str,
        to_queue: str,
        message: MessageData,
        *,
        lease_token: str | None = None,
    ) -> bool:
        return True

    def remove_message(self, queue: str, message: MessageData, *, lease_token: str | None = None) -> bool:
        return True

    def renew_message_lease(self, queue: str, message: MessageData, lease_token: str) -> bool:
        self.renewal_count += 1
        return True  # always True — never signals expiry

    def wait_for_message_and_move(self, from_queue: str, to_queue: str) -> ClaimedMessage | MessageData | None:
        if self._message is None:
            return None
        msg = self._message
        self._message = None
        return ClaimedMessage(stored_message=msg, lease_token="fake-token")


# ---------------------------------------------------------------------------
# Step 4a: Lease-ignoring gateway tests
# ---------------------------------------------------------------------------


class TestSyncLeaseIgnoringGateway:
    def test_queue_processes_message_without_error(self):
        """A lease-ignoring gateway processes messages without raising.

        Documents the gap: the queue has no way to detect that the gateway
        ignores lease_token, so processing succeeds even though mutual
        exclusion is not enforced.
        """
        gateway = _SyncLeaseIgnoringGateway()
        q = RedisMessageQueue("test", gateway=gateway, heartbeat_interval_seconds=1)
        q.publish("hello")
        with q.process_message() as msg:
            assert msg == "hello"

    def test_stale_lease_warning_never_fires(self, caplog):
        """Because remove_message always returns True, the stale-lease warning never fires."""
        gateway = _SyncLeaseIgnoringGateway()
        q = RedisMessageQueue("test", gateway=gateway, heartbeat_interval_seconds=1)
        q.publish("hello")
        with caplog.at_level(logging.WARNING):
            with q.process_message() as msg:
                assert msg == "hello"
        assert "lease expired" not in caplog.text

    def test_lease_token_is_passed_to_remove(self):
        """The queue does pass lease_token — the gateway just ignores it."""
        gateway = _SyncLeaseIgnoringGateway()
        q = RedisMessageQueue("test", gateway=gateway, heartbeat_interval_seconds=1)
        q.publish("hello")
        with q.process_message() as msg:
            assert msg is not None
        assert gateway.remove_calls == ["fake-token"]


class TestAsyncLeaseIgnoringGateway:
    @pytest.mark.asyncio
    async def test_queue_processes_message_without_error(self):
        """Async: a lease-ignoring gateway processes messages without raising."""
        gateway = _AsyncLeaseIgnoringGateway()
        q = AsyncRedisMessageQueue("test", gateway=gateway, heartbeat_interval_seconds=1)
        await q.publish("hello")
        async with q.process_message() as msg:
            assert msg == "hello"

    @pytest.mark.asyncio
    async def test_stale_lease_warning_never_fires(self, caplog):
        """Async: because remove_message always returns True, no stale-lease warning."""
        gateway = _AsyncLeaseIgnoringGateway()
        q = AsyncRedisMessageQueue("test", gateway=gateway, heartbeat_interval_seconds=1)
        await q.publish("hello")
        with caplog.at_level(logging.WARNING):
            async with q.process_message() as msg:
                assert msg == "hello"
        assert "lease expired" not in caplog.text

    @pytest.mark.asyncio
    async def test_lease_token_is_passed_to_remove(self):
        """Async: the queue does pass lease_token — the gateway just ignores it."""
        gateway = _AsyncLeaseIgnoringGateway()
        q = AsyncRedisMessageQueue("test", gateway=gateway, heartbeat_interval_seconds=1)
        await q.publish("hello")
        async with q.process_message() as msg:
            assert msg is not None
        assert gateway.remove_calls == ["fake-token"]


# ---------------------------------------------------------------------------
# Step 4b: Always-True renewal gateway tests
# ---------------------------------------------------------------------------


class TestSyncRenewAlwaysTrueGateway:
    def test_heartbeat_never_self_stops(self):
        """When renew_message_lease always returns True, the heartbeat thread
        keeps running indefinitely — it never detects that the lease should
        have expired. This documents the danger of unconditional True renewal.
        """
        gateway = _SyncAlwaysTrueRenewalGateway()
        q = RedisMessageQueue("test", gateway=gateway, heartbeat_interval_seconds=0.02)
        q.publish("hello")

        with q.process_message() as msg:
            assert msg is not None
            # Let the heartbeat fire multiple times
            time.sleep(0.1)

        # The heartbeat renewed multiple times during processing because
        # it never received a False signal to self-stop.
        assert gateway.renewal_count >= 2


# ---------------------------------------------------------------------------
# Step 4c: Duck-type check edge cases on message_visibility_timeout_seconds
# ---------------------------------------------------------------------------


class TestGatewayVisibilityTimeoutDuckType:
    def _make_sync_gateway_class(self, visibility_value):
        """Create a minimal sync gateway class with a given visibility timeout."""

        class _Gateway(SyncAbstractRedisGateway):
            message_visibility_timeout_seconds = visibility_value

            def publish_message(self, queue, message, dedup_key):
                return True

            def add_message(self, queue, message):
                pass

            def move_message(self, from_queue, to_queue, message, *, lease_token=None):
                return True

            def remove_message(self, queue, message, *, lease_token=None):
                return True

            def renew_message_lease(self, queue, message, lease_token):
                return True

            def wait_for_message_and_move(self, from_queue, to_queue):
                return ClaimedMessage(stored_message="msg", lease_token="tk1")

        return _Gateway()

    def _make_async_gateway_class(self, visibility_value):
        """Create a minimal async gateway class with a given visibility timeout."""

        class _Gateway(AsyncAbstractRedisGateway):
            message_visibility_timeout_seconds = visibility_value

            async def publish_message(self, queue, message, dedup_key):
                return True

            async def add_message(self, queue, message):
                pass

            async def move_message(self, from_queue, to_queue, message, *, lease_token=None):
                return True

            async def remove_message(self, queue, message, *, lease_token=None):
                return True

            async def renew_message_lease(self, queue, message, lease_token):
                return True

            async def wait_for_message_and_move(self, from_queue, to_queue):
                return ClaimedMessage(stored_message="msg", lease_token="tk1")

        return _Gateway()

    def test_string_visibility_timeout_raises_type_error(self):
        gateway = self._make_sync_gateway_class("30")
        with pytest.raises(TypeError, match="message_visibility_timeout_seconds"):
            RedisMessageQueue("test", gateway=gateway, heartbeat_interval_seconds=5)

    def test_bool_visibility_timeout_raises_type_error(self):
        gateway = self._make_sync_gateway_class(True)
        with pytest.raises(TypeError, match="message_visibility_timeout_seconds"):
            RedisMessageQueue("test", gateway=gateway, heartbeat_interval_seconds=5)

    def test_zero_visibility_timeout_raises_value_error(self):
        gateway = self._make_sync_gateway_class(0)
        with pytest.raises(ValueError, match="message_visibility_timeout_seconds"):
            RedisMessageQueue("test", gateway=gateway, heartbeat_interval_seconds=5)

    def test_none_visibility_timeout_with_heartbeat_raises_value_error(self):
        gateway = self._make_sync_gateway_class(None)
        with pytest.raises(ValueError, match="heartbeat_interval_seconds"):
            RedisMessageQueue("test", gateway=gateway, heartbeat_interval_seconds=5)

    def test_missing_attribute_with_heartbeat_raises_value_error(self):
        """Gateway without message_visibility_timeout_seconds + heartbeat → ValueError."""

        class _BareGateway(SyncAbstractRedisGateway):
            def publish_message(self, queue, message, dedup_key):
                return True

            def add_message(self, queue, message):
                pass

            def move_message(self, from_queue, to_queue, message, *, lease_token=None):
                return True

            def remove_message(self, queue, message, *, lease_token=None):
                return True

            def renew_message_lease(self, queue, message, lease_token):
                return True

            def wait_for_message_and_move(self, from_queue, to_queue):
                return None

        with pytest.raises(ValueError, match="message_visibility_timeout_seconds"):
            RedisMessageQueue("test", gateway=_BareGateway(), heartbeat_interval_seconds=5)

    def test_claimed_message_without_heartbeat_succeeds(self):
        """A gateway returning ClaimedMessage without heartbeat configured is valid.

        This is a legitimate use case: the gateway manages leases, but the
        consumer opts out of automatic heartbeat renewal.
        """
        gateway = self._make_sync_gateway_class(30)
        q = RedisMessageQueue("test", gateway=gateway)
        # No heartbeat_interval_seconds — should not raise
        assert q._heartbeat_interval_seconds is None

    def test_async_string_visibility_timeout_raises_type_error(self):
        gateway = self._make_async_gateway_class("30")
        with pytest.raises(TypeError, match="message_visibility_timeout_seconds"):
            AsyncRedisMessageQueue("test", gateway=gateway, heartbeat_interval_seconds=5)

    def test_async_bool_visibility_timeout_raises_type_error(self):
        gateway = self._make_async_gateway_class(True)
        with pytest.raises(TypeError, match="message_visibility_timeout_seconds"):
            AsyncRedisMessageQueue("test", gateway=gateway, heartbeat_interval_seconds=5)

    def test_async_zero_visibility_timeout_raises_value_error(self):
        gateway = self._make_async_gateway_class(0)
        with pytest.raises(ValueError, match="message_visibility_timeout_seconds"):
            AsyncRedisMessageQueue("test", gateway=gateway, heartbeat_interval_seconds=5)

    def test_async_none_visibility_timeout_with_heartbeat_raises_value_error(self):
        gateway = self._make_async_gateway_class(None)
        with pytest.raises(ValueError, match="heartbeat_interval_seconds"):
            AsyncRedisMessageQueue("test", gateway=gateway, heartbeat_interval_seconds=5)
