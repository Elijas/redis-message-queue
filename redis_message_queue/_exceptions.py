import redis.exceptions


class RedisMessageQueueError(Exception):
    """Base class for redis-message-queue specific errors."""


class ConfigurationError(RedisMessageQueueError, ValueError):
    """Bad parameter values or combinations."""


class GatewayContractError(RedisMessageQueueError, TypeError):
    """Custom gateway returned wrong type or violated contract."""


class LuaScriptError(redis.exceptions.ResponseError, RedisMessageQueueError):
    """A Lua script returned an unexpected error_reply."""


class CleanupFailedError(RedisMessageQueueError):
    """Cleanup after handler completion failed."""


class QueueBackpressureError(RedisMessageQueueError):
    """Publish rejected because the pending queue is at its configured limit."""

    _REMEDIATION = (
        "consider increasing `max_pending_length`, switching to "
        "`pending_overload_policy='block'`, or adding consumer capacity."
    )

    def __init__(self, *args: object) -> None:
        message = "Pending queue reached its configured limit" if not args else args[0]
        if not isinstance(message, str) or len(args) > 1:
            super().__init__(*args)
            return
        if self._REMEDIATION not in message:
            message = f"{message}; {self._REMEDIATION}"
        super().__init__(message)


class QueueDrainedError(RedisMessageQueueError):
    """Raised when publish() is called after drain() or aclose()."""


class RetryBudgetExhaustedError(redis.exceptions.RedisError, RedisMessageQueueError):
    """Tenacity retry budget exhausted; underlying redis-py exception is .__cause__."""

    _REMEDIATION = (
        "verify Redis connectivity and consider increasing `retry_budget_seconds` if transient failures are expected."
    )

    def __init__(self, *args: object) -> None:
        message = "Redis retry budget exhausted" if not args else args[0]
        if not isinstance(message, str) or len(args) > 1:
            super().__init__(*args)
            return
        if self._REMEDIATION not in message:
            message = f"{message}; {self._REMEDIATION}"
        super().__init__(message)


def wrap_lua_response_error(exc: redis.exceptions.ResponseError) -> LuaScriptError | None:
    message = str(exc)
    if message.startswith("WRONGTYPE ") or message.startswith("OOM during publish;"):
        return LuaScriptError(message)
    return None
