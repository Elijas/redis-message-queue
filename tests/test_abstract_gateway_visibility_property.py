import fakeredis
import pytest

from redis_message_queue._abstract_redis_gateway import (
    AbstractRedisGateway as SyncAbstractRedisGateway,
)
from redis_message_queue._exceptions import ConfigurationError
from redis_message_queue._redis_gateway import RedisGateway as SyncRedisGateway
from redis_message_queue._stored_message import ClaimedMessage, MessageData
from redis_message_queue.asyncio._abstract_redis_gateway import (
    AbstractRedisGateway as AsyncAbstractRedisGateway,
)
from redis_message_queue.asyncio._redis_gateway import RedisGateway as AsyncRedisGateway
from redis_message_queue.asyncio.redis_message_queue import (
    RedisMessageQueue as AsyncRedisMessageQueue,
)
from redis_message_queue.interrupt_handler._interface import BaseGracefulInterruptHandler
from redis_message_queue.redis_message_queue import RedisMessageQueue


class _NonLeaseCustomGateway(SyncAbstractRedisGateway):
    def publish_message(self, queue: str, message: str, dedup_key: str) -> bool:
        return True

    def add_message(self, queue: str, message: str) -> None:
        return None

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

    def renew_message_lease(
        self,
        queue: str,
        message: MessageData,
        lease_token: str,
        *,
        is_interrupted: BaseGracefulInterruptHandler | None = None,
    ) -> bool:
        return True

    def wait_for_message_and_move(self, from_queue: str, to_queue: str) -> ClaimedMessage | MessageData | None:
        return None

    def trim_queue(self, queue: str, max_length: int) -> None:
        return None


class _LeaseCustomGateway(_NonLeaseCustomGateway):
    @property
    def message_visibility_timeout_seconds(self) -> int | None:
        return 300


class _AsyncNonLeaseCustomGateway(AsyncAbstractRedisGateway):
    async def publish_message(self, queue: str, message: str, dedup_key: str) -> bool:
        return True

    async def add_message(self, queue: str, message: str) -> None:
        return None

    async def move_message(
        self,
        from_queue: str,
        to_queue: str,
        message: MessageData,
        *,
        lease_token: str | None = None,
    ) -> bool:
        return True

    async def remove_message(self, queue: str, message: MessageData, *, lease_token: str | None = None) -> bool:
        return True

    async def renew_message_lease(
        self,
        queue: str,
        message: MessageData,
        lease_token: str,
        *,
        is_interrupted: BaseGracefulInterruptHandler | None = None,
    ) -> bool:
        return True

    async def wait_for_message_and_move(self, from_queue: str, to_queue: str) -> ClaimedMessage | MessageData | None:
        return None

    async def trim_queue(self, queue: str, max_length: int) -> None:
        return None


class _AsyncLeaseCustomGateway(_AsyncNonLeaseCustomGateway):
    @property
    def message_visibility_timeout_seconds(self) -> int | None:
        return 300


def test_sync_custom_gateway_inherits_none_visibility_timeout() -> None:
    assert _NonLeaseCustomGateway().message_visibility_timeout_seconds is None


def test_async_custom_gateway_inherits_none_visibility_timeout() -> None:
    assert _AsyncNonLeaseCustomGateway().message_visibility_timeout_seconds is None


def test_sync_custom_gateway_visibility_timeout_override() -> None:
    assert _LeaseCustomGateway().message_visibility_timeout_seconds == 300


def test_async_custom_gateway_visibility_timeout_override() -> None:
    assert _AsyncLeaseCustomGateway().message_visibility_timeout_seconds == 300


def test_sync_builtin_gateway_returns_configured_visibility_timeout() -> None:
    gateway = SyncRedisGateway(
        redis_client=fakeredis.FakeRedis(),
        retry_budget_seconds=0,
        message_visibility_timeout_seconds=300,
    )

    assert gateway.message_visibility_timeout_seconds == 300


def test_async_builtin_gateway_returns_configured_visibility_timeout() -> None:
    gateway = AsyncRedisGateway(
        redis_client=fakeredis.FakeAsyncRedis(),
        retry_budget_seconds=0,
        message_visibility_timeout_seconds=300,
    )

    assert gateway.message_visibility_timeout_seconds == 300


def test_sync_non_lease_custom_gateway_with_heartbeat_still_raises_configuration_error() -> None:
    with pytest.raises(ConfigurationError, match="configured visibility timeout"):
        RedisMessageQueue("test", gateway=_NonLeaseCustomGateway(), heartbeat_interval_seconds=10)


def test_async_non_lease_custom_gateway_with_heartbeat_still_raises_configuration_error() -> None:
    with pytest.raises(ConfigurationError, match="configured visibility timeout"):
        AsyncRedisMessageQueue("test", gateway=_AsyncNonLeaseCustomGateway(), heartbeat_interval_seconds=10)
