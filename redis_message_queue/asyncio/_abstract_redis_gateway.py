from abc import ABC, abstractmethod

from redis_message_queue._stored_message import ClaimedMessage, MessageData


class AbstractRedisGateway(ABC):
    """Abstract interface for async Redis-backed message queue gateway operations.

    Subclass this to provide a custom async Redis gateway implementation. The
    built-in ``RedisGateway`` enforces lease-based mutual exclusion via Lua
    scripts; custom gateways MUST uphold the same behavioral contracts
    documented on each method to avoid phantom heartbeats, undetected lease conflicts,
    or silent data loss.

    Gateways that support visibility timeouts (lease-based claiming) MUST expose
    a ``message_visibility_timeout_seconds`` property (int or None). This is not
    abstract because it is configuration rather than protocol, but it is required
    when the queue is configured with ``heartbeat_interval_seconds``.
    Lease-capable custom gateways MUST expose this property; omitting it
    silently disables heartbeat validation and lease-token safety checks,
    causing the queue to treat the gateway as a non-lease implementation.

    The queue also reads ``max_delivery_count`` and ``dead_letter_queue``
    from the gateway. The abstract base provides ``None`` defaults via
    ``@property``; lease + DLQ-enabled custom gateways MUST override both
    to enable poison-message routing. Avoid using these attribute names
    for unrelated purposes on custom gateway implementations.

    Gateways that wrap a Redis Cluster client should override the
    ``is_redis_cluster`` property to return ``True`` so the queue can
    apply hash-tag validation at construction time. The abstract base
    provides ``False`` as the default; non-cluster gateways inherit it.

    Concurrency
    -----------
    When the queue is configured with ``heartbeat_interval_seconds``,
    ``renew_message_lease`` is called from a concurrent ``asyncio.Task``
    while the main task may call ``move_message`` or ``remove_message``.
    Implementations must be safe for concurrent calls across these methods.
    The built-in gateway achieves this via atomic Lua scripts.

    Server-side atomicity (Lua scripts, redis-py transactions, or a
    single-command-per-mutation discipline) is the recommended pattern.
    Python-level locks (``threading.Lock``, ``asyncio.Lock``) shared across
    ``renew_message_lease`` and ``move_message`` / ``remove_message`` are an
    anti-pattern: they serialize without giving Redis-server-side atomicity,
    leave partial-failure orphans, and can deadlock against the heartbeat
    lifecycle (the heartbeat is awaited from the same finally block that
    issues the cleanup move/remove).
    """

    @property
    def is_redis_cluster(self) -> bool:
        return False

    @property
    def max_delivery_count(self) -> int | None:
        return None

    @property
    def dead_letter_queue(self) -> str | None:
        return None

    @abstractmethod
    async def publish_message(self, queue: str, message: str, dedup_key: str) -> bool:
        """Publish a message with deduplication.

        Returns True if the message was enqueued (i.e. ``dedup_key`` was not
        already present). Returns False if the message was deduplicated and
        therefore not enqueued.
        """

    @abstractmethod
    async def add_message(self, queue: str, message: str) -> None:
        """Unconditionally enqueue a message. No deduplication is performed.

        This library deliberately does not wrap the underlying enqueue in a
        retry — retrying after the server may already have executed the
        command can silently duplicate the message. The caller can still
        retry (accepting duplicates).

        Note: a client-level retry policy bypasses this guarantee. If the
        underlying ``redis.Redis`` / ``redis.asyncio.Redis`` client was
        constructed with ``retry=Retry(...)``, redis-py retries on
        ``ConnectionError`` / ``TimeoutError`` below this call and may
        duplicate. Pass ``retry=None`` (the default) when strict at-most-once
        is required for non-deduplicated publishes.
        """

    @abstractmethod
    async def move_message(
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
        no longer matches the current lease holder (i.e. another consumer has
        reclaimed the message), the method MUST return False and leave the
        message in ``from_queue``. Note: the built-in gateway intentionally
        does NOT reject completions whose wall-clock deadline has passed but
        where no other consumer has reclaimed the message — that path keeps
        at-least-once semantics from producing spurious double-processing.
        Ignoring ``lease_token`` entirely silently breaks mutual exclusion.

        Returns True if the message was moved, False otherwise.
        """

    @abstractmethod
    async def remove_message(self, queue: str, message: MessageData, *, lease_token: str | None = None) -> bool:
        """Remove a message from a queue.

        ``message`` is the exact ``stored_message`` value from ``ClaimedMessage``
        (or the raw ``MessageData`` from ``wait_for_message_and_move`` when no
        lease is used). The implementation must be able to locate the message in
        ``queue`` using this value.

        When ``lease_token`` is provided, the implementation MUST validate that
        the token matches the current lease holder before removing. If the token
        no longer matches the current lease holder (i.e. another consumer has
        reclaimed the message), the method MUST return False. Note: the
        built-in gateway intentionally does NOT reject completions whose
        wall-clock deadline has passed but where no other consumer has
        reclaimed the message — that path keeps at-least-once semantics from
        producing spurious double-processing. Ignoring ``lease_token``
        entirely silently breaks mutual exclusion.

        Returns True if the message was removed, False otherwise.
        """

    @abstractmethod
    async def renew_message_lease(self, queue: str, message: MessageData, lease_token: str) -> bool:
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
    async def wait_for_message_and_move(self, from_queue: str, to_queue: str) -> ClaimedMessage | MessageData | None:
        """Wait for a message and atomically move it to the processing queue.

        Return ``ClaimedMessage`` when the gateway supports visibility timeouts
        (lease-based claiming). The ``ClaimedMessage.lease_token`` must be a
        non-empty string that uniquely identifies this claim.

        Return plain ``MessageData`` (str or bytes) when the gateway does not
        use leases.

        Return None if no message was available (e.g. timeout or interrupt).

        Implementations MUST respect a reasonable timeout or return None
        periodically so the consumer can check for interrupts. Blocking
        indefinitely without returning prevents graceful shutdown. As a
        concrete reference, the built-in gateway uses a 5s outer wait
        decomposed into 0.25s polling steps (see
        ``_VISIBILITY_TIMEOUT_POLL_INTERVAL_SECONDS``); custom
        implementations should keep the longest single block below ~5s so
        an interrupt is observed within one polling step.
        """

    @abstractmethod
    async def trim_queue(self, queue: str, max_length: int) -> None:
        """Trim a queue to at most ``max_length`` elements.

        This is a best-effort cleanup operation. Failures are logged but
        do not affect message processing correctness.
        """
