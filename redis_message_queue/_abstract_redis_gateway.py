from abc import ABC, abstractmethod

from redis_message_queue._stored_message import ClaimedMessage, MessageData


class AbstractRedisGateway(ABC):
    """Abstract interface for Redis-backed message queue gateway operations.

    Subclass this to provide a custom Redis gateway implementation. The built-in
    ``RedisGateway`` enforces lease-based mutual exclusion via Lua scripts; custom
    gateways MUST uphold the same behavioral contracts documented on each method
    to avoid phantom heartbeats, undetected lease conflicts, or silent data loss.

    Gateways that support visibility timeouts (lease-based claiming) should expose
    a ``message_visibility_timeout_seconds`` property (int or None). This is not
    abstract because it is configuration rather than protocol, but it is required
    when the queue is configured with ``heartbeat_interval_seconds``.

    Concurrency
    -----------
    When the queue is configured with ``heartbeat_interval_seconds``,
    ``renew_message_lease`` is called from a background ``threading.Thread``
    concurrently with the main thread calling ``move_message`` or
    ``remove_message``. Implementations must be safe for concurrent calls
    across these methods. The built-in gateway achieves this via atomic
    Lua scripts.
    """

    @abstractmethod
    def publish_message(self, queue: str, message: str, dedup_key: str) -> bool:
        """Publish a message with deduplication.

        Returns True if the message was enqueued (i.e. ``dedup_key`` was not
        already present). Returns False if the message was deduplicated and
        therefore not enqueued.
        """

    @abstractmethod
    def add_message(self, queue: str, message: str) -> None:
        """Unconditionally enqueue a message. No deduplication is performed."""

    @abstractmethod
    def move_message(
        self,
        from_queue: str,
        to_queue: str,
        message: MessageData,
        *,
        lease_token: str | None = None,
    ) -> bool:
        """Atomically move a message between queues.

        ``message`` is the exact ``stored_message`` value from ``ClaimedMessage``
        (or the raw ``MessageData`` from ``wait_for_message_and_move`` when no
        lease is used). The implementation must be able to locate the message in
        ``from_queue`` using this value.

        When ``lease_token`` is provided, the implementation MUST validate that
        the token matches the current lease holder before moving. If the token
        is stale or the lease has expired, the method MUST return False and
        leave the message in ``from_queue``. Ignoring ``lease_token`` silently
        breaks mutual exclusion.

        Returns True if the message was moved, False otherwise.
        """

    @abstractmethod
    def remove_message(self, queue: str, message: MessageData, *, lease_token: str | None = None) -> bool:
        """Remove a message from a queue.

        ``message`` is the exact ``stored_message`` value from ``ClaimedMessage``
        (or the raw ``MessageData`` from ``wait_for_message_and_move`` when no
        lease is used). The implementation must be able to locate the message in
        ``queue`` using this value.

        When ``lease_token`` is provided, the implementation MUST validate that
        the token matches the current lease holder before removing. If the token
        is stale or the lease has expired, the method MUST return False.
        Ignoring ``lease_token`` silently breaks mutual exclusion.

        Returns True if the message was removed, False otherwise.
        """

    @abstractmethod
    def renew_message_lease(self, queue: str, message: MessageData, lease_token: str) -> bool:
        """Extend the lease for a message currently being processed.

        ``message`` is the exact ``stored_message`` value from ``ClaimedMessage``
        returned by ``wait_for_message_and_move``. The implementation must be
        able to locate the message in ``queue`` using this value.

        MUST return True only if the lease was actually extended server-side.
        MUST return False if the lease has expired or the token is stale.
        Returning True unconditionally defeats the heartbeat's safety role:
        the heartbeat will never self-stop, keeping a consumer alive even after
        another consumer has reclaimed the message.
        """

    @abstractmethod
    def wait_for_message_and_move(self, from_queue: str, to_queue: str) -> ClaimedMessage | MessageData | None:
        """Wait for a message and atomically move it to the processing queue.

        Return ``ClaimedMessage`` when the gateway supports visibility timeouts
        (lease-based claiming). The ``ClaimedMessage.lease_token`` must be a
        non-empty string that uniquely identifies this claim.

        Return plain ``MessageData`` (str or bytes) when the gateway does not
        use leases.

        Return None if no message was available (e.g. timeout or interrupt).
        """
