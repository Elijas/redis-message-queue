import redis.exceptions


class RedisMessageQueueError(Exception):
    """Base class for redis-message-queue specific errors."""

    def __init__(
        self,
        *args: object,
        queue: str | None = None,
        message_id: str | None = None,
        operation: str | None = None,
    ) -> None:
        super().__init__(*args)
        self.queue = queue
        self.message_id = message_id
        self.operation = operation


class ConfigurationError(RedisMessageQueueError, ValueError):
    """Bad parameter values or combinations."""


class GatewayContractError(RedisMessageQueueError, TypeError):
    """Custom gateway returned wrong type or violated contract."""


class LuaScriptError(redis.exceptions.ResponseError, RedisMessageQueueError):
    """A Lua script returned an unexpected error_reply."""

    def __init__(
        self,
        *args: object,
        queue: str | None = None,
        message_id: str | None = None,
        operation: str | None = None,
    ) -> None:
        RedisMessageQueueError.__init__(
            self,
            *args,
            queue=queue,
            message_id=message_id,
            operation=operation,
        )


class CleanupFailedError(RedisMessageQueueError):
    """Cleanup after handler completion failed."""


class ClaimStoreFailedError(RedisMessageQueueError):
    """Raised when the VT-claim Lua store_claim_and_return pcall failed.

    The script decremented the speculative delivery_count increment and
    compensated by returning the message to pending before surfacing this error.
    """


class DrainFailedError(RedisMessageQueueError):
    """Wraps a non-RMQ exception caught during drain pending-claim recovery.

    drain() returns False as the bool result; this exception carries
    F7 context (queue, operation="drain") into the drain/failure event
    payload so users diagnosing drain incidents via on_event see the
    same structured attrs as elsewhere.
    """


class MalformedStoredMessageError(RedisMessageQueueError):
    """Stored value is not a valid RMQ envelope for the configured decode mode."""


class PayloadTooLargeError(RedisMessageQueueError, ValueError):
    """Publish payload exceeds the configured serialized byte limit."""


class PayloadTooDeepError(RedisMessageQueueError, ValueError):
    """Publish payload exceeds the configured nesting-depth limit."""


class QueueBackpressureError(RedisMessageQueueError):
    """Publish rejected because the pending queue is at its configured limit."""

    _REMEDIATION = (
        "consider increasing `max_pending_length`, switching to "
        "`pending_overload_policy='block'`, or adding consumer capacity."
    )

    def __init__(
        self,
        *args: object,
        queue: str | None = None,
        message_id: str | None = None,
        operation: str | None = None,
    ) -> None:
        message = "Pending queue reached its configured limit" if not args else args[0]
        if not isinstance(message, str) or len(args) > 1:
            super().__init__(*args, queue=queue, message_id=message_id, operation=operation)
            return
        if self._REMEDIATION not in message:
            message = f"{message}; {self._REMEDIATION}"
        super().__init__(message, queue=queue, message_id=message_id, operation=operation)


class QueueDrainedError(RedisMessageQueueError):
    """Raised when publish() is called after drain() or aclose()."""


class RetryBudgetExhaustedError(redis.exceptions.RedisError, RedisMessageQueueError):
    """Tenacity retry budget exhausted; underlying redis-py exception is .__cause__."""

    _REMEDIATION = (
        "verify Redis connectivity and consider increasing `retry_budget_seconds` if transient failures are expected."
    )

    def __init__(
        self,
        *args: object,
        queue: str | None = None,
        message_id: str | None = None,
        operation: str | None = None,
    ) -> None:
        message = "Redis retry budget exhausted" if not args else args[0]
        if not isinstance(message, str) or len(args) > 1:
            RedisMessageQueueError.__init__(
                self,
                *args,
                queue=queue,
                message_id=message_id,
                operation=operation,
            )
            return
        if self._REMEDIATION not in message:
            message = f"{message}; {self._REMEDIATION}"
        RedisMessageQueueError.__init__(
            self,
            message,
            queue=queue,
            message_id=message_id,
            operation=operation,
        )


def _set_exception_context(
    exc: BaseException,
    *,
    queue: str | None = None,
    message_id: str | None = None,
    operation: str | None = None,
) -> None:
    if not isinstance(exc, RedisMessageQueueError):
        return
    if queue is not None:
        exc.queue = queue
    if message_id is not None:
        exc.message_id = message_id
    if operation is not None:
        exc.operation = operation


def wrap_lua_response_error(exc: redis.exceptions.ResponseError) -> LuaScriptError | None:
    message = str(exc)
    if message.startswith("WRONGTYPE ") or message.startswith("OOM during publish;"):
        return LuaScriptError(message)
    return None
