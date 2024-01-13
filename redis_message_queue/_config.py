import logging

import redis
import redis.exceptions
from tenacity import (
    after_log,
    retry,
    retry_if_exception,
    stop_after_delay,
    wait_exponential_jitter,
)

logger = logging.getLogger(__name__)


def is_redis_retryable_exception(exception):
    return isinstance(
        exception,
        (
            redis.exceptions.ConnectionError,
            redis.exceptions.TimeoutError,
            redis.exceptions.BusyLoadingError,
            redis.exceptions.ClusterDownError,
            redis.exceptions.TryAgainError,
        ),
    )


DEFAULT_REDIS_CONNECTION_RETRY_STRATEGY = retry(
    stop=stop_after_delay(120),
    wait=wait_exponential_jitter(initial=0.01, exp_base=2, max=5, jitter=0.1),
    retry=retry_if_exception(is_redis_retryable_exception),
    after=after_log(logger, logging.ERROR),
)
DEFAULT_MESSAGE_WAIT_INTERVAL_SECONDS = 5
DEFAULT_MESSAGE_DEDUPLICATION_LOG_TTL = 60 * 60  # 1 hour = 60 seconds * 60 minutes
