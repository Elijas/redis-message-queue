import redis.exceptions

from redis_message_queue._config import is_redis_retryable_exception


class TestRetryableConnectionErrors:
    """ConnectionError subclasses that ARE transient and should be retried."""

    def test_plain_connection_error_is_retryable(self):
        exc = redis.exceptions.ConnectionError("connection refused")
        assert is_redis_retryable_exception(exc) is True


class TestNonRetryableConnectionErrors:
    """ConnectionError subclasses that are permanent and must NOT be retried."""

    def test_authentication_error_is_not_retryable(self):
        exc = redis.exceptions.AuthenticationError("wrong password")
        assert is_redis_retryable_exception(exc) is False

    def test_authorization_error_is_not_retryable(self):
        exc = redis.exceptions.AuthorizationError("no permission")
        assert is_redis_retryable_exception(exc) is False

    def test_max_connections_error_is_not_retryable(self):
        exc = redis.exceptions.MaxConnectionsError("pool exhausted")
        assert is_redis_retryable_exception(exc) is False


class TestRetryableNonConnectionErrors:
    """Non-ConnectionError exceptions that ARE transient and should be retried."""

    def test_timeout_error_is_retryable(self):
        exc = redis.exceptions.TimeoutError("timed out")
        assert is_redis_retryable_exception(exc) is True

    def test_busy_loading_error_is_retryable(self):
        exc = redis.exceptions.BusyLoadingError("loading dataset")
        assert is_redis_retryable_exception(exc) is True

    def test_cluster_down_error_is_retryable(self):
        exc = redis.exceptions.ClusterDownError("cluster down")
        assert is_redis_retryable_exception(exc) is True

    def test_try_again_error_is_retryable(self):
        exc = redis.exceptions.TryAgainError("try again")
        assert is_redis_retryable_exception(exc) is True

    def test_read_only_error_is_retryable(self):
        exc = redis.exceptions.ReadOnlyError("readonly")
        assert is_redis_retryable_exception(exc) is True


class TestNonRetryableOtherErrors:
    """Exceptions that are not transient and must NOT be retried."""

    def test_response_error_is_not_retryable(self):
        exc = redis.exceptions.ResponseError("wrong number of args")
        assert is_redis_retryable_exception(exc) is False

    def test_data_error_is_not_retryable(self):
        exc = redis.exceptions.DataError("invalid data")
        assert is_redis_retryable_exception(exc) is False

    def test_generic_exception_is_not_retryable(self):
        exc = Exception("something went wrong")
        assert is_redis_retryable_exception(exc) is False

    def test_value_error_is_not_retryable(self):
        exc = ValueError("bad value")
        assert is_redis_retryable_exception(exc) is False
