from dataclasses import dataclass
from enum import StrEnum


class EventOperation(StrEnum):
    """Queue lifecycle operation names emitted via QueueEvent."""

    PUBLISH = "publish"
    PUBLISH_DEDUP_HIT = "publish_dedup_hit"
    CLAIM = "claim"
    CLAIM_EMPTY = "claim_empty"
    CLAIM_RECLAIM = "claim_reclaim"
    ACK = "ack"
    NACK = "nack"
    DLQ = "dlq"
    COMPLETED = "completed"
    FAILED = "failed"
    LEASE_RENEW = "lease_renew"
    LEASE_RENEW_FAILED = "lease_renew_failed"
    HEARTBEAT_STOP_TIMEOUT = "heartbeat_stop_timeout"
    STALE_LEASE_ACK = "stale_lease_ack"
    STALE_LEASE_NACK = "stale_lease_nack"
    CLEANUP_FAILED = "cleanup_failed"
    TRIM_FAILED = "trim_failed"
    RETRY_ATTEMPT = "retry_attempt"
    RETRY_EXHAUSTED = "retry_exhausted"


class EventOutcome(StrEnum):
    """Queue lifecycle operation outcomes emitted via QueueEvent."""

    SUCCESS = "success"
    FAILURE = "failure"
    SKIPPED = "skipped"


@dataclass(frozen=True)
class QueueEvent:
    """Best-effort lifecycle event emitted by RedisMessageQueue."""

    queue: str
    """the queue this event belongs to"""
    operation: EventOperation
    """the operation that produced this event (publish, claim, ack, etc.)"""
    outcome: EventOutcome
    """whether the operation succeeded, failed, was deduplicated, etc."""
    message_id: str | None = None
    """the message ID if the event is bound to one"""
    claim_id: str | None = None
    """the claim attempt ID if the event is bound to one"""
    lease_token_hash: str | None = None
    """a diagnostic hash of the lease token when visibility timeout is enabled"""
    destination_queue: str | None = None
    """the queue a message was moved to, when applicable"""
    delivery_count: int | None = None
    """the number of delivery attempts recorded for this message, when applicable"""
    max_delivery_count: int | None = None
    """the configured delivery-attempt threshold, when applicable"""
    exception_type: str | None = None
    """
    type name of the raised exception for metrics labels (e.g., 'TimeoutError');
    see also `error` for the exception object
    """
    duration_ms: float | None = None
    """wall-clock duration of the operation in milliseconds"""
    error: BaseException | None = None
    """
    the actual exception object when one was raised; pass to OpenTelemetry
    `span.record_exception(...)` for full trace attribution
    """
