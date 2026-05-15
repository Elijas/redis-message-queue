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


class QueueDrainedError(RedisMessageQueueError):
    """Raised when publish() is called after drain() or aclose()."""


class RetryBudgetExhaustedError(redis.exceptions.RedisError, RedisMessageQueueError):
    """Tenacity retry budget exhausted; underlying redis-py exception is .__cause__."""


def wrap_lua_response_error(exc: redis.exceptions.ResponseError) -> LuaScriptError | None:
    message = str(exc)
    if message.startswith("WRONGTYPE ") or message.startswith("OOM during publish;"):
        return LuaScriptError(message)
    return None
