class QueueKeyManager:
    # MESSAGE_DEDUPLICATION_LOG: A container that logs the existence of a message to prevent duplication. Messages are marked in this log for the duration of their lifecycle.
    _MESSAGE_DEDUPLICATION_LOG = "deduplication"

    # PENDING_MESSAGES: A queue for messages that are scheduled for processing but have not yet started. Messages are added to this queue before processing begins.
    _PENDING_MESSAGES = "pending"

    # PROCESSING_MESSAGES: A queue for messages that are currently being processed. Messages move to this queue once they start processing.
    _PROCESSING_MESSAGES = "processing"

    # COMPLETED_MESSAGES_LOG: A container for messages that have finished processing. Messages move to this queue once processing is completed.
    _COMPLETED_MESSAGES_LOG = "completed"

    # FAILED_MESSAGES_LOG: A container for messages that have failed processing. Messages move to this queue once processing has failed.
    _FAILED_MESSAGES_LOG = "failed"

    def __init__(self, queue_name: str):
        self._queue_name = queue_name

    def deduplication(self, message: str) -> str:
        return f"{self._queue_name}:{self._MESSAGE_DEDUPLICATION_LOG}:{message}"

    @property
    def pending(self) -> str:
        return f"{self._queue_name}:{self._PENDING_MESSAGES}"

    @property
    def processing(self) -> str:
        return f"{self._queue_name}:{self._PROCESSING_MESSAGES}"

    @property
    def completed(self) -> str:
        return f"{self._queue_name}:{self._COMPLETED_MESSAGES_LOG}"

    @property
    def failed(self) -> str:
        return f"{self._queue_name}:{self._FAILED_MESSAGES_LOG}"
