import pytest
import redis.exceptions

from redis_message_queue._config import is_redis_retryable_exception


def test_noscript_error_is_retryable():
    exception = redis.exceptions.NoScriptError("NOSCRIPT no matching script")

    assert is_redis_retryable_exception(exception) is True


def test_noscript_response_error_fallback_is_retryable():
    exception = redis.exceptions.ResponseError("NOSCRIPT manually constructed")

    assert is_redis_retryable_exception(exception) is True


def test_other_response_error_is_not_retryable():
    exception = redis.exceptions.ResponseError("WRONGTYPE other")

    assert is_redis_retryable_exception(exception) is False


@pytest.mark.parametrize(
    "exception",
    (
        redis.exceptions.ConnectionError("connection refused"),
        redis.exceptions.TimeoutError("timed out"),
        redis.exceptions.ClusterDownError("cluster down"),
        redis.exceptions.TryAgainError("try again"),
        redis.exceptions.ReadOnlyError("readonly"),
        redis.exceptions.ClusterError("TTL exhausted"),
    ),
)
def test_existing_retryable_exceptions_still_retry(exception):
    assert is_redis_retryable_exception(exception) is True


@pytest.mark.parametrize(
    "exception",
    (
        redis.exceptions.AuthenticationError("bad password"),
        redis.exceptions.AuthorizationError("permission denied"),
    ),
)
def test_existing_non_retryable_connection_errors_still_do_not_retry(exception):
    assert is_redis_retryable_exception(exception) is False
