from abc import ABC, abstractmethod

from redis_message_queue._stored_message import ClaimedMessage, ReceivedPayload
from redis_message_queue.interrupt_handler._interface import BaseGracefulInterruptHandler


class AbstractRedisGateway(ABC):
    """Abstract interface for async Redis-backed message queue gateway operations.

    Subclass this to provide a custom async Redis gateway implementation. The
    built-in ``RedisGateway`` enforces lease-based mutual exclusion via Lua
    scripts; custom gateways MUST uphold the same behavioral contracts
    documented on each method to avoid phantom heartbeats, undetected lease conflicts,
    or silent data loss.

    Gateways that support visibility timeouts (lease-based claiming) MUST
    override the ``message_visibility_timeout_seconds`` property with a positive
    int. The abstract base declares this property with a ``None`` default so
    non-lease custom gateways keep the existing behavior, while lease-capable
    custom gateways have a typeable contract to override. A positive value is
    required when the queue is configured with ``heartbeat_interval_seconds``.

    The queue also reads ``max_delivery_count`` and ``dead_letter_queue``
    from the gateway. The abstract base provides ``None`` defaults via
    ``@property``; lease + DLQ-enabled custom gateways MUST override both
    to enable poison-message routing. Avoid using these attribute names
    for unrelated purposes on custom gateway implementations.
    When DLQ routing is enabled, the queue also attaches an internal
    ``_rmq_bound_pending_queue`` attribute to gateway instances to reject
    reusing one DLQ-bound gateway across multiple pending queues.

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

    @property
    def message_visibility_timeout_seconds(self) -> int | None:
        """Visibility timeout (lease duration) in seconds. Override to enable
        lease-based crash recovery; return None to disable. Required when the
        queue is configured with ``heartbeat_interval_seconds``.
        """
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

        Note on retries: redis-py 6.0+ changed the default standalone
        ``Redis()`` / ``redis.asyncio.Redis()`` retry policy from ``None`` (no
        retry) to a multi-attempt ``ExponentialWithJitterBackoff``. The default
        attempt count varies by redis-py version, for example about 3 on
        redis-py 6-7 and about 10 on redis-py 8+. If you need strict
        at-most-once for non-deduplicated publishes, pass ``retry=None``
        explicitly when constructing the redis-py client. This library does
        not configure the redis-py client retry; it only controls its own
        retry budget on top of the client.
        """

    @abstractmethod
    async def move_message(
        self,
        from_queue: str,
        to_queue: str,
        message: ReceivedPayload,
        *,
        lease_token: str | None = None,
    ) -> bool:
        """Atomically move a message between queues.

        ``message`` is the exact ``stored_message`` value from ``ClaimedMessage``
        (or the raw ``ReceivedPayload`` from ``wait_for_message_and_move`` when no
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
    async def remove_message(self, queue: str, message: ReceivedPayload, *, lease_token: str | None = None) -> bool:
        """Remove a message from a queue.

        ``message`` is the exact ``stored_message`` value from ``ClaimedMessage``
        (or the raw ``ReceivedPayload`` from ``wait_for_message_and_move`` when no
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
    async def renew_message_lease(
        self,
        queue: str,
        message: ReceivedPayload,
        lease_token: str,
        *,
        is_interrupted: BaseGracefulInterruptHandler | None = None,
    ) -> bool:
        """Extend the lease for a message currently being processed.

        ``message`` is the exact ``stored_message`` value from ``ClaimedMessage``
        returned by ``wait_for_message_and_move``. The implementation must be
        able to locate the message in ``queue`` using this value.

        MUST return True only if the lease was actually extended server-side.
        MUST return False if the lease has expired or the token is stale.
        Returning True unconditionally defeats the heartbeat's safety role:
        the heartbeat will never self-stop, keeping a consumer alive even after
        another consumer has reclaimed the message.

        ``is_interrupted`` is supplied by ``_LeaseHeartbeat`` so a
        ``stop()`` signal can abort an in-flight retry loop instead of
        waiting out ``retry_budget_seconds`` under pool exhaustion or other
        transient errors (AA-01-F2). Custom gateways SHOULD honor this by
        bailing from their retry loop as soon as
        ``is_interrupted.is_interrupted()`` returns True; ignoring it is
        safe behavior-wise but reintroduces the heartbeat-retention hazard.
        """

    @abstractmethod
    async def wait_for_message_and_move(
        self, from_queue: str, to_queue: str
    ) -> ClaimedMessage | ReceivedPayload | None:
        """Wait for a message and atomically move it to the processing queue.

        Return ``ClaimedMessage`` when the gateway supports visibility timeouts
        (lease-based claiming). The ``ClaimedMessage.lease_token`` must be a
        non-empty string that uniquely identifies this claim.

        Return plain ``ReceivedPayload`` (str or bytes) when the gateway does not
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
