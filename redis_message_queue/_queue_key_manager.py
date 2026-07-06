from redis_message_queue._exceptions import ConfigurationError
from redis_message_queue._stored_message import MessagePayload


def validate_callable_deduplication_key(dedup_key: object, message: MessagePayload) -> str:
    if dedup_key is None:
        raise ConfigurationError(
            f"get_deduplication_key returned None for message {message!r}; the callable must return a non-empty string"
        )
    if not isinstance(dedup_key, str):
        raise TypeError(f"get_deduplication_key must return a str, got {type(dedup_key).__name__}")
    if dedup_key == "":
        raise ConfigurationError(
            f"get_deduplication_key returned an empty string for message {message!r}; "
            "the callable must return a non-empty, high-cardinality key"
        )
    return dedup_key


class QueueKeyManager:
    """Build Redis keys for one rmq queue namespace.

    ``key_separator`` is part of every generated key and rmq has no fixed
    library prefix. Do not choose a separator that overlaps another Redis task
    library's namespace, such as ``":queue:"`` with RQ-style keys; user-chosen
    separators interact with every Redis user on the same DB.
    """

    # Logs message existence to prevent duplication.
    # Messages are marked for the duration of their lifecycle.
    _MESSAGE_DEDUPLICATION_LOG = "deduplication"

    # Queue for messages scheduled for processing but not yet started.
    _PENDING_MESSAGES = "pending"

    # Queue for messages currently being processed.
    _PROCESSING_MESSAGES = "processing"

    # Container for messages that have finished processing successfully.
    _COMPLETED_MESSAGES_LOG = "completed"

    # Container for messages that have failed processing.
    _FAILED_MESSAGES_LOG = "failed"

    # Container for poison messages that exceeded the max delivery count.
    # Must match `_AUTO_DEAD_LETTER_QUEUE_SUFFIX` in redis_message_queue.py, which is
    # what the queue actually writes to when no custom `dead_letter_queue=` is configured.
    _DEAD_LETTER_MESSAGES = "dlq"

    def __init__(self, queue_name: str, key_separator: str):
        if not isinstance(queue_name, str):
            raise TypeError(f"'name' must be a string, got {type(queue_name).__name__}")
        if not queue_name.strip():
            raise ConfigurationError(
                f"'name' must be a non-empty string with non-whitespace characters; got {queue_name!r}"
            )
        if "\x00" in queue_name:
            raise ConfigurationError(
                f"'name' must not contain null bytes; got {queue_name!r}. "
                "Remove null bytes or choose a different queue name."
            )
        if not isinstance(key_separator, str):
            raise TypeError(f"'key_separator' must be a string, got {type(key_separator).__name__}")
        if not key_separator.strip():
            raise ConfigurationError(
                f"'key_separator' must be a non-empty string with non-whitespace characters; got {key_separator!r}"
            )
        # Reject names containing the separator: ``QueueKeyManager('q').deduplication('pending')``
        # and ``QueueKeyManager('q::deduplication').pending`` would both map to
        # ``'q::deduplication::pending'`` — a string key colliding with a list key, producing
        # ``WRONGTYPE`` at runtime. Fail fast at construction instead.
        if key_separator in queue_name:
            raise ConfigurationError(
                f"'name' must not contain the key separator {key_separator!r}; got {queue_name!r}. "
                "Choose a different name or pass a different 'key_separator'."
            )
        self._queue_name = queue_name
        self._key_separator = key_separator

    def deduplication(self, message: str) -> str:
        if not isinstance(message, str):
            raise TypeError(f"'deduplication_key' must be a str, got {type(message).__name__}")
        if message == "":
            raise ConfigurationError("'deduplication_key' must be a non-empty string")
        return f"{self._queue_name}{self._key_separator}{self._MESSAGE_DEDUPLICATION_LOG}{self._key_separator}{message}"

    @property
    def deduplication_prefix(self) -> str:
        return f"{self._queue_name}{self._key_separator}{self._MESSAGE_DEDUPLICATION_LOG}{self._key_separator}"

    @property
    def pending(self) -> str:
        return f"{self._queue_name}{self._key_separator}{self._PENDING_MESSAGES}"

    @property
    def processing(self) -> str:
        return f"{self._queue_name}{self._key_separator}{self._PROCESSING_MESSAGES}"

    @property
    def completed(self) -> str:
        return f"{self._queue_name}{self._key_separator}{self._COMPLETED_MESSAGES_LOG}"

    @property
    def failed(self) -> str:
        return f"{self._queue_name}{self._key_separator}{self._FAILED_MESSAGES_LOG}"

    @property
    def dead_letter(self) -> str:
        """Return the Redis key of the auto-derived default dead-letter queue.

        This is the key the queue writes poison messages to when
        ``max_delivery_count`` is set and no custom ``dead_letter_queue=`` name was
        passed on the gateway. If a custom ``dead_letter_queue=`` name was configured,
        that name takes precedence and messages are written there instead; the
        configured name is available via the gateway's ``dead_letter_queue`` attribute,
        not via this accessor.
        """
        return f"{self._queue_name}{self._key_separator}{self._DEAD_LETTER_MESSAGES}"
