# Upgrading

> Part of the [redis-message-queue](./README.md) documentation.

Version migration guides for redis-message-queue, newest first. For per-release
detail see [CHANGELOG.md](CHANGELOG.md); for the configuration reference see
[docs/configuration.md](docs/configuration.md).

## v7 to v8 migration

v8.0.0 removes implicit dedup key generation. Deduplication is opt-in and
`deduplication=True` now requires `get_deduplication_key`. If you were relying
on v7's automatic content hash, provide the equivalent callable explicitly.

Before:

```python
queue = RedisMessageQueue(
    "orders",
    client=client,
    deduplication=True,
)
```

After, prefer a stable logical ID:

```python
queue = RedisMessageQueue(
    "orders",
    client=client,
    deduplication=True,
    get_deduplication_key=lambda msg: msg["order_id"],
)
```

To preserve v7 content-hash behavior mechanically:

```python
import hashlib
import json

queue = RedisMessageQueue(
    "orders",
    client=client,
    deduplication=True,
    get_deduplication_key=lambda msg: hashlib.sha256(
        json.dumps(msg, sort_keys=True).encode()
    ).hexdigest(),
)
```

If you do not need publish-side deduplication, omit `deduplication` or set
`deduplication=False`.

## v6 to v7 migration

v7.0.0 has four breaking changes to check during upgrade.

**AC-02: queue event operation/outcome types are `StrEnum` members.**
Runtime string comparisons keep working because `StrEnum` subclasses `str`,
but type-strict code should replace old `Literal[...]` annotations and raw
string constants with enum members.

Before:

```python
from typing import Literal

QueueOperation = Literal["publish", "claim", "ack"]

def record(operation: QueueOperation) -> None:
    if operation == "publish":
        print("published")
```

After:

```python
from redis_message_queue import EventOperation

def record(operation: EventOperation) -> None:
    if operation is EventOperation.PUBLISH:
        print("published")
```

**AC-03: drained queue instances refuse new publishes.** After
`queue.drain()` / `queue.close()` (sync) or `await queue.drain()` /
`await queue.aclose()` (async), the same queue instance rejects `publish()`
with `QueueDrainedError("queue is drained")`.

This state is queue-local and process-local; it is not stored in Redis. If a
producer must continue publishing after a worker has drained, use a separate
`RedisMessageQueue(...)` instance for that producer lifecycle. During
shutdown, catch `QueueDrainedError` only at boundaries where late publishes are
expected and safe to drop or reschedule.

```python
from redis_message_queue import QueueDrainedError

try:
    queue.publish("late shutdown audit event")
except QueueDrainedError:
    # The queue instance already began draining; drop or reschedule elsewhere.
    pass
```

**AC-09: unsafe `drop_oldest` combinations now fail at construction.** These
configurations raise `ConfigurationError` before the queue or gateway is
created:

- `pending_overload_policy="drop_oldest"` with `max_pending_length=None`:
  `drop_oldest requires max_pending_length to be set. Use a positive
  max_pending_length to define what can be dropped, or use
  pending_overload_policy='raise' or 'block' for unbounded queues.`
- `pending_overload_policy="drop_oldest"` with deduplication enabled or
  `get_deduplication_key` configured:
  `'pending_overload_policy=drop_oldest' cannot be used with deduplication
  because dropped messages leave their deduplication keys in Redis, causing
  future publishes of the same payload to be silently suppressed. Use 'raise'
  or 'block' for deduplicated queues, or disable deduplication if 'drop_oldest'
  is required.`
- `pending_overload_policy="drop_oldest"` with `max_delivery_count` set:
  `drop_oldest is incompatible with max_delivery_count (set
  max_delivery_count=None or pick another policy to avoid silent loss of
  pending DLQ candidates). Use pending_overload_policy='raise' or 'block' when
  dead-letter handling is required.`

**AC-16: redis-py is capped below 8.0.0.** The package dependency is
`redis>=5.0.1,<8.0.0` until redis-py 8 RESP3-default behavior is verified.
Users on redis-py 7.x and earlier are unaffected. If you installed a redis-py
8.0.0 beta explicitly, downgrade with `pip install "redis<8.0.0"`.

## Configuration changes on live queues

> **Warning:** These changes are destructive on live queues. Drain the queue completely before applying them.

- **Do not change `name` or `key_separator` on a live queue.** Both settings define the Redis key namespace. Existing Redis keys become invisible to the new key scheme. Drain the queue completely before changing either value.
- **Do not rename `dead_letter_queue` on a live queue.** Existing DLQ records stay in the old list, while new failures route to the new list. Inspect or drain the old DLQ manually before switching names.
- **Do not toggle visibility timeout in either direction with messages in processing.** Messages claimed by non-VT consumers have no lease metadata, so VT-enabled consumers cannot reclaim them. Disabling VT later orphans existing lease deadline, lease token, and delivery count metadata and removes crash recovery for those in-flight messages. Drain the processing queue first.
- **Reducing `max_delivery_count` retroactively DLQs messages.** The delivery count hash persists across restarts. Messages whose accumulated count exceeds the new limit are immediately dead-lettered on next claim.
- **Changing `max_delivery_count` from a number to `None` leaves delivery metadata behind.** The delivery count hash continues to exist but is no longer consulted. Use this only after draining or after planning manual cleanup of the delivery-count hash.
- **Changing `get_deduplication_key` changes the dedup keyspace.** Existing dedup records become inert for the duration of their TTL. Drain the queue or clear the old deduplication keys before switching key functions.
- **Disabling `deduplication` has a retention-window overlap.** Existing dedup records remain in Redis until their TTL expires, but new publishes bypass them. Republishes that would have been suppressed under the old setting can enqueue during that window.
- **Disabling `enable_failed_queue` stops recording handler failures.** Existing failed entries remain in Redis, but new failures are removed from `processing` without being appended to the failed queue. If `max_delivery_count=None` is also set, repeated handler failures can be dropped with no DLQ or failed-queue record; see [Dead-letter queue](docs/configuration.md#dead-letter-queue).
- **Lowering `max_completed_length` or `max_failed_length` trims existing history.** The next completed or failed move calls `LTRIM`, so changing `None` to `N` or lowering `N` can immediately reduce historical entries to the new cap.
- **Do not switch sync and async gateway instances mid-process while claims are active.** Redis state is compatible across deploys, but each gateway instance keeps its own pending claim-recovery IDs. In-flight claim recovery state does not transfer between instances.
- **Switching between `gateway=` and `client=` can retarget the DLQ.** The built-in `client=` path derives the DLQ from the queue name. If a custom gateway used a different `dead_letter_queue`, switching paths has the same orphaning impact as renaming the DLQ.

## v5 to v6 migration

v6.0.0 is a non-breaking-defaults release that adds new public APIs. v5 code continues to work; v6 adds opt-in features.

**New APIs (opt in as needed):**

- `max_pending_length=N` caps pending-list depth; with `pending_overload_policy="raise"` (default) producers see `QueueBackpressureError` when the cap is hit; `"block"` waits up to `pending_overload_block_timeout_seconds`; `"drop_oldest"` evicts silently, so use it only when data loss is acceptable.
- `queue.drain(timeout=...)` (sync) and `await queue.aclose(timeout=...)` (async) are explicit graceful-shutdown hooks. They refuse new claims and recover pending claim IDs but do not cancel in-flight handlers; join or await your worker separately.
- `on_event=callback` receives a `QueueEvent` dataclass for publish/claim/ack/reclaim/dedup/cleanup/drain lifecycle events. Use it for metrics, tracing, and structured logging. See [`examples/production/observability.py`](examples/production/observability.py) for the adapter pattern.
- See [`examples/production/backpressure.py`](examples/production/backpressure.py) and [`examples/production/graceful_shutdown.py`](examples/production/graceful_shutdown.py) for sync production patterns, with async siblings under [`examples/production/asyncio/`](examples/production/asyncio/).

> When using a pre-fork app server (gunicorn `--preload`, uvicorn workers that import the app at master startup), call `make_queue()` from your worker startup hook - NOT at module import. See [Fork safety](docs/operations.md#fork-safety-and-pre-fork-servers) for why.

**New constructor rejections:**

- Passing a `redis.sentinel.Sentinel` manager object now raises at construction. Use `sentinel.master_for(name)` instead.
- Passing `redis.Redis(single_connection_client=True)` to the sync built-in gateway now raises. Use a normal pooled `redis.Redis` client.

**Custom gateway migration:**

If you subclass `AbstractRedisGateway` and override `renew_message_lease`, add a keyword argument:

```python
def renew_message_lease(
    self,
    queue,
    message,
    lease_token,
    *,
    is_interrupted=None,  # NEW in v6: heartbeat passes a stop-signal observer
) -> bool:
    ...
```

Async gateways need the same signature on `async def`. Honor `is_interrupted.is_interrupted()` in your retry loops to stop renewing when the queue is shutting down.

**Exception handling:**

- Catch `QueueBackpressureError` around publish if you opt into backpressure.
- A new base class `RedisMessageQueueError` lets you catch all library-owned exceptions in one place. Existing catches for `ValueError`, `TypeError`, `redis.RedisError`, etc. continue to work because the new subclasses preserve those bases.

## v2 to v3 migration

v3.0.0 replaced the `retry_strategy: Callable` constructor parameter with `retry_budget_seconds`, `retry_max_delay_seconds`, and `retry_initial_delay_seconds`. Users with custom retry strategies should subclass `AbstractRedisGateway` instead (see [Custom gateway](docs/configuration.md#custom-gateway)).

