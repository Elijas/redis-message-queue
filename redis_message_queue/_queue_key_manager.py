class QueueKeyManager:
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

    def __init__(self, queue_name: str, key_separator: str):
        if not isinstance(queue_name, str):
            raise TypeError(f"'name' must be a string, got {type(queue_name).__name__}")
        if not queue_name.strip():
            raise ValueError("'name' must be a non-empty string")
        self._queue_name = queue_name
        if not isinstance(key_separator, str):
            raise TypeError(f"'key_separator' must be a string, got {type(key_separator).__name__}")
        if not key_separator.strip():
            raise ValueError("'key_separator' must be a non-empty string")
        self._key_separator = key_separator

    def deduplication(self, message: str) -> str:
        return f"{self._queue_name}{self._key_separator}{self._MESSAGE_DEDUPLICATION_LOG}{self._key_separator}{message}"

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
