from dataclasses import dataclass
from typing import Literal, Optional

EventOperation = Literal[
    "publish",
    "publish_dedup_hit",
    "claim",
    "claim_empty",
    "claim_reclaim",
    "ack",
    "nack",
    "dlq",
    "completed",
    "failed",
    "lease_renew",
    "lease_renew_failed",
    "heartbeat_stop_timeout",
    "stale_lease_ack",
    "stale_lease_nack",
    "cleanup_failed",
    "trim_failed",
    "retry_attempt",
    "retry_exhausted",
]

EventOutcome = Literal["success", "failure", "skipped"]


@dataclass(frozen=True)
class QueueEvent:
    queue: str
    operation: EventOperation
    outcome: EventOutcome
    message_id: Optional[str] = None
    claim_id: Optional[str] = None
    lease_token_hash: Optional[str] = None
    destination_queue: Optional[str] = None
    exception_type: Optional[str] = None
    duration_ms: Optional[float] = None
