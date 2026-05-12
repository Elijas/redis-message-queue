# redis-message-queue

[![PyPI Version](https://img.shields.io/badge/v5.0.0-version?color=43cd0f&style=flat&label=pypi)](https://pypi.org/project/redis-message-queue)
[![PyPI Downloads](https://img.shields.io/pypi/dm/redis-message-queue?color=43cd0f&style=flat&label=downloads)](https://pypistats.org/packages/redis-message-queue)
[![License: MIT](https://img.shields.io/badge/License-MIT-43cd0f.svg?style=flat&label=license)](LICENSE)
[![Maintained: yes](https://img.shields.io/badge/yes-43cd0f.svg?style=flat&label=maintained)](https://github.com/Elijas/redis-message-queue/issues)
[![CI](https://github.com/Elijas/redis-message-queue/actions/workflows/ci.yml/badge.svg)](https://github.com/Elijas/redis-message-queue/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/Elijas/redis-message-queue/graph/badge.svg)](https://codecov.io/gh/Elijas/redis-message-queue)
[![Linter: Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

**Lightweight Python message queuing with Redis and built-in publish-side deduplication.** Deduplicate publishes within a TTL window, with optional crash recovery — across any number of producers and consumers.

```bash
pip install "redis-message-queue>=3.0.0,<4.0.0"
```

Requires Redis server >= 6.2.

## Quickstart

### Publish messages

```python
from redis import Redis
from redis_message_queue import RedisMessageQueue

client = Redis.from_url("redis://localhost:6379/0", decode_responses=True)
queue = RedisMessageQueue("my_queue", client=client, deduplication=True)

queue.publish("order:1234")           # returns True
queue.publish("order:1234")           # returns False (deduplicated)
queue.publish({"user": "alice"})      # dicts work too
```

### Consume messages

```python
from redis import Redis
from redis_message_queue import RedisMessageQueue

client = Redis.from_url("redis://localhost:6379/0", decode_responses=True)
queue = RedisMessageQueue("my_queue", client=client)

while True:
    with queue.process_message() as message:
        if message is not None:
            print(f"Processing: {message}")
            # Auto-acknowledged on success; cleaned up on exception
```

`RedisMessageQueue` itself is not a context manager. Use
`with queue.process_message() as message:` for each message.

## Why redis-message-queue

**The problem:** You're sending messages between services or workers and need guarantees. Simple Redis LPUSH/BRPOP loses messages on crashes, doesn't deduplicate, and gives you no visibility into what succeeded or failed.

**The solution:** Atomic Lua scripts for publish + dedup, a processing queue for in-flight tracking (with optional crash recovery via visibility timeouts), and optional success/failure logs for observability.

| Feature | Details |
|---------|---------|
| **Deduplicated publish** | Lua-scripted atomic SET NX + LPUSH prevents duplicate enqueues within a configurable TTL window (default: 1 hour), even with producer retries. Supports custom key functions for content-based deduplication. Note: deduplication is publish-side only and does not prevent duplicate *delivery* under at-least-once visibility-timeout reclaim |
| **Visibility-timeout redelivery** | Crashed or stalled consumers' messages are reclaimed and redelivered when a visibility timeout is configured |
| **Success & failure logs** | Optional completed/failed queues for auditing and reprocessing, with configurable max length to prevent unbounded growth |
| **Dead-letter queue** | Poison messages that exceed a configurable delivery count are automatically routed to a dead-letter queue instead of being redelivered indefinitely |
| **Graceful shutdown** | Built-in interrupt handler lets consumers finish current work before stopping |
| **Lease heartbeats** | Optional background lease renewal keeps long-running handlers from being redelivered prematurely |
| **Connection retries** | Exponential backoff with jitter for Redis operations (deduplicated publish, ack, lease renewal). Publish and cleanup paths use replay markers so retryable connection drops preserve the original result within the same call. Message-claim paths use idempotent Lua claim IDs plus persisted claim metadata so retryable errors can recover the original claim safely, either in the same wait call or on the next call from the same gateway instance if the original wait had to give up before Redis became reachable again. Active waits keep their in-flight claim IDs private until they exit, so a concurrent caller on the same gateway instance cannot recover the same claim twice. Timed waits also stay bounded: once the configured wait window expires, the queue only replays persisted state for that same claim attempt and will not claim fresh work after the deadline. If a graceful interrupt arrives during claim recovery, the wait call stops instead of taking fresh work. Non-deduplicated publish is not retried — the exception propagates so the caller can decide whether to retry (accepting potential duplicates) |
| **Async support** | Drop-in async variant with identical API |

All features are optional and can be enabled or disabled as needed.

### Delivery semantics

| Configuration | Delivery guarantee |
|---|---|
| Default (`visibility_timeout_seconds=300`) | **At-least-once** — expired messages are reclaimed and redelivered |
| With `visibility_timeout_seconds=None` | **At-most-once** — a consumer crash loses the in-flight message |

See [Crash recovery with visibility timeout](#crash-recovery-with-visibility-timeout) for details and tradeoffs.

## Configuration

### Deduplication

```python
# Default: deduplicate by SHA-256 hash of canonical message content (1-hour TTL)
queue = RedisMessageQueue("q", client=client, deduplication=True)

# Custom dedup key (e.g., deduplicate by order ID only)
queue = RedisMessageQueue(
    "q", client=client,
    deduplication=True,
    get_deduplication_key=lambda msg: msg["order_id"],
)

# Disable deduplication entirely
queue = RedisMessageQueue("q", client=client, deduplication=False)
```

### Success and failure tracking

```python
queue = RedisMessageQueue(
    "q", client=client,
    enable_completed_queue=True,   # track successful messages
    enable_failed_queue=True,      # track failed messages for reprocessing
)
```

Completed and failed tracking queues are capped at 1,000 entries by default
when enabled. Override the caps when you need a different retention window:

```python
queue = RedisMessageQueue(
    "q", client=client,
    enable_completed_queue=True,
    enable_failed_queue=True,
    max_completed_length=10000,    # keep only the most recent 10,000
    max_failed_length=1000,        # keep only the most recent 1,000
)
```

When set, `LTRIM` is called after each message is moved to the completed/failed queue. This is best-effort cleanup — if the trim fails, the queue is slightly longer until the next successful trim.
Pass `max_completed_length=None` or `max_failed_length=None` explicitly if you
want unbounded tracking queues.

### Crash recovery with visibility timeout

```python
queue = RedisMessageQueue(
    "q",
    client=client,
    visibility_timeout_seconds=300,
    heartbeat_interval_seconds=60,
)
```

This enables lease-based redelivery for messages left in `processing` by a crashed worker and renews the lease while a healthy long-running handler is still working.
Tradeoffs:
- delivery becomes at-least-once after lease expiry
- the timeout must be longer than your normal processing time if you do not use heartbeats
- if you do use heartbeats, the heartbeat interval must be less than half of the visibility timeout
- recovery happens on consumer polling cadence rather than instantly
- heartbeats add background renewal work for active messages
- if a heartbeat fails (network error or stale lease), the heartbeat stops silently; the consumer continues processing but may find at ack time that the message was reclaimed by another consumer

Pass `on_heartbeat_failure` to receive a best-effort callback when the heartbeat stops because renewal failed:

```python
queue = RedisMessageQueue(
    "q", client=client,
    visibility_timeout_seconds=300,
    heartbeat_interval_seconds=60,
    on_heartbeat_failure=lambda: log.warning("heartbeat failed; lease may be stale"),
)
```

The callback is **advisory** — it may fire briefly after a successful `process_message` exit when a final renewal coincided with the success path. Use it for metrics or alerting, not as a correctness signal. For the async queue (`redis_message_queue.asyncio`), the callback may also be `async def`.

Without a visibility timeout, messages already moved to `processing` remain there indefinitely after a consumer crash and are not redelivered, even if the crash happened before your handler started running.

### Dead-letter queue

```python
queue = RedisMessageQueue(
    "q",
    client=client,
    visibility_timeout_seconds=300,
    max_delivery_count=5,
)
```

When a message has been delivered more than `max_delivery_count` times (due to consumer crashes causing visibility-timeout reclaim), it is automatically routed to a dead-letter queue (`{name}::dlq`) instead of being redelivered. `max_delivery_count` defaults to `10` on the built-in `client=` path, with the DLQ name auto-derived from the queue name. This prevents poison messages from cycling indefinitely.

Notes:
- requires `visibility_timeout_seconds` to be set (poison messages are only a concern with VT reclaim)
- the delivery count is tracked per-message in a Redis HASH and cleaned up on successful ack or move to completed/failed
- the delivery count increments when Redis grants the claim/lease, not when your handler begins running. If a process exits after Redis claims a message, that claim still counts toward `max_delivery_count`
- `max_delivery_count=1` means the message is delivered once; any reclaim routes it to the dead-letter queue
- set `max_delivery_count=None` explicitly for unlimited redelivery
- dead-lettered messages contain the **raw payload** only — the internal envelope (which carries a per-delivery UUID) is stripped before pushing to the DLQ, consistent with how completed/failed queues store messages. Two identical payloads dead-lettered separately are indistinguishable in the DLQ

### Graceful shutdown

```python
from redis_message_queue import RedisMessageQueue, GracefulInterruptHandler

interrupt = GracefulInterruptHandler()
queue = RedisMessageQueue("q", client=client, interrupt=interrupt)

while not interrupt.is_interrupted():
    with queue.process_message() as message:
        if message is not None:
            process(message)
# Consumer finishes current message before exiting on Ctrl+C
```

> **Note:** `GracefulInterruptHandler` claims process-global signal handlers for
> its signals (default: SIGINT, SIGTERM, SIGHUP), but only when those signals are
> still using Python's default disposition. If another handler is already installed,
> or if another `GracefulInterruptHandler` already owns the signal, construction raises
> `ValueError`. A repeated owned signal falls back to the default behavior
> (for example, a second Ctrl+C raises `KeyboardInterrupt`). If you need multiple
> shutdown hooks, use a single handler and fan out in your own code.

### Custom gateway

```python
from redis_message_queue import RedisGateway

# Tune retry budget, dedup TTL, or wait interval
gateway = RedisGateway(
    redis_client=client,
    retry_budget_seconds=120,          # total retry window (set 0 to disable retry)
    retry_max_delay_seconds=5.0,       # cap on per-attempt backoff
    retry_initial_delay_seconds=0.01,  # first backoff
    message_deduplication_log_ttl_seconds=3600,
    message_wait_interval_seconds=10,
    message_visibility_timeout_seconds=300,
)
queue = RedisMessageQueue("q", gateway=gateway)
```

The retry knobs configure an internal `tenacity` strategy: exponential
backoff with jitter, retry on transient Redis errors only, capped at
`retry_budget_seconds`. The budget is wall-clock time from the first attempt (including attempt duration), not inter-attempt delay; a single attempt that takes longer than the budget results in zero retries. Setting `retry_budget_seconds=0` disables retry
entirely (single attempt; exceptions propagate). The library uses
`retry_budget_seconds` to size the operation-result cache TTL automatically,
so the previous footgun of an over-long retry budget out-living the cache
and producing misleading "cleanup was a no-op" warnings is now structurally
impossible. Note: tenacity may allow one additional attempt beyond the budget if the budget check passes at attempt start — total wall-clock time can exceed `retry_budget_seconds` by the duration of that final attempt.

To plug in a different retry library (`backoff`, `asyncstdlib.retry`, or your
own logic) or fundamentally different semantics, subclass
`AbstractRedisGateway` from `redis_message_queue` (or
`redis_message_queue.asyncio` for the async sibling) and override the
operation methods directly.

If your custom gateway uses visibility timeouts, it must expose a public
`message_visibility_timeout_seconds` value and return `ClaimedMessage` from
`wait_for_message_and_move()`. The queue now fails closed if a lease-capable
gateway returns plain `str`/`bytes`, because cleanup without a lease token can
ack a message that has already been reclaimed by another consumer.
If a lease-capable custom gateway omits `message_visibility_timeout_seconds`,
the queue cannot detect that lease semantics are in play and will treat the
gateway as a non-lease gateway. In that misconfigured state, lease-token safety
checks and heartbeat validation are bypassed.

When using a custom gateway with dead-letter queue support, configure `max_delivery_count`
and `dead_letter_queue` directly on the gateway — do **not** pass `max_delivery_count` to
`RedisMessageQueue`:

```python
gateway = RedisGateway(
    redis_client=client,
    message_visibility_timeout_seconds=300,
    max_delivery_count=3,
    dead_letter_queue="myqueue::dead_letter",
)
queue = RedisMessageQueue("myqueue", gateway=gateway)
```

Use a separate gateway instance per queue when `max_delivery_count` is enabled.
Dead-letter routing is gateway-scoped, so reusing the same gateway across different
queues is rejected.

If you use Redis Sentinel, pass the Redis client returned by
`sentinel.master_for(name)` to `client=` or `RedisGateway(redis_client=...)`, not
the `sentinel` object itself.

### Connection pool sizing

Each queue with `heartbeat_interval_seconds` set uses up to 2 simultaneous
connections: one for the main operation and one for heartbeat renewal. Size Redis
client pools with `max_connections >= 2 * number_of_queues + headroom`.

## Async API

Replace the import to use the async variant — the API is identical:

```python
from redis_message_queue.asyncio import RedisMessageQueue
```

The sync and async classes intentionally share names. In modules that use both,
alias the imports explicitly, for example
`from redis_message_queue import RedisMessageQueue as SyncRedisMessageQueue` and
`from redis_message_queue.asyncio import RedisMessageQueue as AsyncRedisMessageQueue`.

All examples work the same way. Remember to close the connection when done:

```python
import redis.asyncio as redis

client = redis.Redis()
# ... your code
await client.aclose()
```

For the sync Redis client, call `client.close()` during application shutdown when
you own the client lifecycle.

## Known limitations

- **No metrics or observability hooks.** The library logs warnings (stale leases, heartbeat failures, transient errors) via Python's `logging` module but does not expose callbacks, event hooks, or metric counters. To monitor queue health, inspect the underlying Redis keys directly or parse log output.
- **Timed waits use polling claim loops.** To make claims recoverable after ambiguous connection drops, `wait_for_message_and_move()` uses idempotent Lua claim polling instead of raw blocking list-move commands. This adds a small polling cadence during timed waits.
- **Redis Lua is atomic, not rollback-transactional.** The built-in scripts now preflight queue key types and fail closed on `WRONGTYPE` before mutating queue state, but Redis does not undo earlier writes if a later script command fails for another reason (for example `OOM` under severe memory pressure).
- **Batch reclaim limit of 100.** The visibility-timeout reclaim Lua script processes at most 100 expired messages per consumer poll. Under extreme backlog this may delay recovery, but prevents any single poll from blocking Redis.
- **Claim-attempt loop limit of 100 per poll.** The VT claim Lua script attempts at most 100 LMOVE+delivery-count checks per invocation. Under pathological conditions (>100 consecutive poison messages in pending), a single poll returns no message even though non-poison messages exist deeper in the queue. Subsequent polls drain the poison batch 100 at a time.
- **Cluster detection uses `isinstance(client, RedisCluster)`.** Wrapped or instrumented cluster clients that delegate without inheriting will bypass hash-tag validation. Custom gateways should set `is_redis_cluster = True` explicitly.
- **Redis Cluster requires hash tags.** The built-in queue uses multiple Redis keys per operation. Wrap the queue name in hash tags (for example `{myqueue}`) so every generated key lands in the same slot. When you pass a Redis Cluster client to the built-in queue/gateway path, incompatible names are rejected early.
- **Non-ASCII payloads use ~2x storage.** The default `ensure_ascii=True` in JSON serialization encodes non-ASCII characters as `\uXXXX` escape sequences. This is a deliberate compatibility choice.
- **Client-side `Retry` can duplicate non-deduplicated publishes.** If you construct your `redis.Redis` client with `retry=Retry(...)`, redis-py retries `ConnectionError` / `TimeoutError` at the connection layer — *below* this library. Idempotent operations (deduplicated `publish()`, lease-scoped cleanup) are safe because their Lua scripts replay the original result. `add_message()` (used by `publish()` when `deduplication=False`) is a bare `LPUSH`: this library deliberately does not retry it, but a client-level `Retry` will, and if the server executed the command before the response was lost the message is enqueued twice. Leave `retry=None` (the default) if you need strict at-most-once semantics for non-deduplicated publishes, or accept the duplication risk. More broadly, any non-idempotent `LPUSH` path is vulnerable if the connection drops after server execution but before the client receives the response; all other built-in operations (deduplicated publish, lease-scoped ack/move, lease renewal) use replay markers and are safe under client-level `Retry`.
- **Redis Cluster default retry can stack with this library's retry budget.** In redis-py 6.0+, `RedisCluster()` constructs a default `ExponentialWithJitterBackoff` retry below this library's `retry_budget_seconds`. If you need a single retry surface, pass `retry=Retry(NoBackoff(), 0)` to the cluster client or reduce `retry_budget_seconds` to account for the lower-level retry window.

For a full analysis, see [docs/production-readiness.md](docs/production-readiness.md).

## Upgrading

### Configuration changes on live queues

> **Warning:** These changes are destructive on live queues. Drain the queue completely before applying them.

- **Do not change `name` or `key_separator` on a live queue.** Both settings define the Redis key namespace. Existing Redis keys become invisible to the new key scheme. Drain the queue completely before changing either value.
- **Do not rename `dead_letter_queue` on a live queue.** Existing DLQ records stay in the old list, while new failures route to the new list. Inspect or drain the old DLQ manually before switching names.
- **Do not toggle visibility timeout in either direction with messages in processing.** Messages claimed by non-VT consumers have no lease metadata, so VT-enabled consumers cannot reclaim them. Disabling VT later orphans existing lease deadline, lease token, and delivery count metadata and removes crash recovery for those in-flight messages. Drain the processing queue first.
- **Reducing `max_delivery_count` retroactively DLQs messages.** The delivery count hash persists across restarts. Messages whose accumulated count exceeds the new limit are immediately dead-lettered on next claim.
- **Changing `max_delivery_count` from a number to `None` leaves delivery metadata behind.** The delivery count hash continues to exist but is no longer consulted. Use this only after draining or after planning manual cleanup of the delivery-count hash.
- **Changing `get_deduplication_key` changes the dedup keyspace.** Existing dedup records become inert for the duration of their TTL. Drain the queue or clear the old deduplication keys before switching between the default hash, explicit `None`, or a custom key function.
- **Disabling `deduplication` has a retention-window overlap.** Existing dedup records remain in Redis until their TTL expires, but new publishes bypass them. Republishes that would have been suppressed under the old setting can enqueue during that window.
- **Disabling `enable_failed_queue` stops recording handler failures.** Existing failed entries remain in Redis, but new failures are removed from `processing` without being appended to the failed queue. If `max_delivery_count=None` is also set, repeated handler failures can be dropped with no DLQ or failed-queue record; see [Dead-letter queue](#dead-letter-queue).
- **Lowering `max_completed_length` or `max_failed_length` trims existing history.** The next completed or failed move calls `LTRIM`, so changing `None` to `N` or lowering `N` can immediately reduce historical entries to the new cap.
- **Do not switch sync and async gateway instances mid-process while claims are active.** Redis state is compatible across deploys, but each gateway instance keeps its own pending claim-recovery IDs. In-flight claim recovery state does not transfer between instances.
- **Switching between `gateway=` and `client=` can retarget the DLQ.** The built-in `client=` path derives the DLQ from the queue name. If a custom gateway used a different `dead_letter_queue`, switching paths has the same orphaning impact as renaming the DLQ.

### v2 to v3 migration

v3.0.0 replaced the `retry_strategy: Callable` constructor parameter with `retry_budget_seconds`, `retry_max_delay_seconds`, and `retry_initial_delay_seconds`. Users with custom retry strategies should subclass `AbstractRedisGateway` instead (see [Custom gateway](#custom-gateway)).

## Running locally

You'll need a Redis server:

```bash
docker run -it --rm -p 6379:6379 redis
```

Try the [examples](https://github.com/Elijas/redis-message-queue/tree/main/examples) with multiple terminals:

```bash
# Two publishers
poetry run python -m examples.send_messages
poetry run python -m examples.send_messages

# Three consumers
poetry run python -m examples.receive_messages
poetry run python -m examples.receive_messages
poetry run python -m examples.receive_messages
```

![GitHub Repo stars](https://img.shields.io/github/stars/elijas/redis-message-queue?style=flat&color=fcfcfc&labelColor=white&logo=github&logoColor=black&label=stars)
