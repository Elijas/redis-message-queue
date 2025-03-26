import logging
import typing

import redis
import redis.exceptions
from tenacity import (
    RetryCallState,
    after_log,
    retry,
    retry_base,
    retry_if_exception,
    stop_after_delay,
    wait_exponential_jitter,
)

from redis_message_queue.interrupt_handler._interface import (
    BaseGracefulInterruptHandler,
)

logger = logging.getLogger(__name__)


def is_redis_retryable_exception(exception):
    # 1. Handle ConnectionError hierarchy (retryable except credentials/config issues)
    if isinstance(exception, redis.exceptions.ConnectionError):
        return not isinstance(
            exception,
            (
                redis.exceptions.AuthenticationError,  # Permanent credentials error
                redis.exceptions.AuthorizationError,  # Permanent permissions error
                redis.exceptions.MaxConnectionsError,  # Client-side connection pool exhaustion
            ),
        )

    # 2. Explicit retryable exceptions
    return isinstance(
        exception,
        (
            # Network/availability issues
            redis.exceptions.TimeoutError,  # Socket or server-side timeout
            redis.exceptions.BusyLoadingError,  # Server loading data
            # Cluster transient failures
            redis.exceptions.ClusterDownError,  # Covers ClusterDown + MasterDown
            redis.exceptions.TryAgainError,  # Cluster state requires retry
            # Server-side transient errors
            redis.exceptions.ReadOnlyError,  # Replica might become writable
        ),
    )


class interruptable_retry(retry_base):
    def __init__(
        self,
        interrupt: BaseGracefulInterruptHandler | None,
        get_parent_retry: typing.Callable[[], retry_base],
    ) -> None:
        self._parent_instance = get_parent_retry()
        self.interrupt = interrupt

    def __call__(self, retry_state: RetryCallState) -> bool:
        if self.interrupt and self.interrupt.is_interrupted():
            return False
        return self._parent_instance.__call__(retry_state)


def get_default_redis_connection_retry_strategy(
    *, interrupt: BaseGracefulInterruptHandler | None = None
):
    return retry(
        stop=stop_after_delay(120),
        wait=wait_exponential_jitter(initial=0.01, exp_base=2, max=5, jitter=0.1),
        retry=interruptable_retry(
            interrupt=interrupt,
            get_parent_retry=lambda: retry_if_exception(is_redis_retryable_exception),
        ),
        after=after_log(logger, logging.ERROR),
    )


DEFAULT_MESSAGE_WAIT_INTERVAL_SECONDS = 5
DEFAULT_MESSAGE_DEDUPLICATION_LOG_TTL = 60 * 60  # 1 hour = 60 seconds * 60 minutes
