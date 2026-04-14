# redis-message-queue

[![PyPI Version](https://img.shields.io/badge/v2.0.0-version?color=43cd0f&style=flat&label=pypi)](https://pypi.org/project/redis-message-queue)
[![PyPI Downloads](https://img.shields.io/pypi/dm/redis-message-queue?color=43cd0f&style=flat&label=downloads)](https://pypistats.org/packages/redis-message-queue)
[![License: MIT](https://img.shields.io/badge/License-MIT-43cd0f.svg?style=flat&label=license)](LICENSE)
[![Maintained: yes](https://img.shields.io/badge/yes-43cd0f.svg?style=flat&label=maintained)](https://github.com/Elijas/redis-message-queue/issues)
[![CI](https://github.com/Elijas/redis-message-queue/actions/workflows/ci.yml/badge.svg)](https://github.com/Elijas/redis-message-queue/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/Elijas/redis-message-queue/graph/badge.svg)](https://codecov.io/gh/Elijas/redis-message-queue)
[![Linter: Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

**Lightweight Python message queuing with Redis and built-in publish-side deduplication.** Deduplicate publishes within a TTL window, with optional crash recovery — across any number of producers and consumers.

```bash
pip install "redis-message-queue>=2.0.0,<3.0.0"
```

Requires Redis server >= 6.2.

## Quickstart

### Publish messages

```python
from redis import Redis
from redis_message_queue import RedisMessageQueue

client = Redis.from_url("redis://localhost:6379/0")
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
| **Connection retries** | Exponential backoff with jitter for Redis operations (deduplicated publish, ack, lease renewal). Publish and cleanup paths use replay markers so retryable connection drops preserve the original result within the same call. Message-claim paths use idempotent Lua claim IDs plus persisted claim metadata so retryable errors can recover the original claim safely, including on the next call from the same gateway instance if the original wait call had to give up before Redis became reachable again. If a graceful interrupt arrives during claim recovery, the wait call stops instead of taking fresh work. Non-deduplicated publish is not retried — the exception propagates so the caller can decide whether to retry (accepting potential duplicates) |
| **Async support** | Drop-in async variant with identical API |

All features are optional and can be enabled or disabled as needed.

### Delivery semantics

| Configuration | Delivery guarantee |
|---|---|
| Default (no visibility timeout) | **At-most-once** — a consumer crash loses the in-flight message |
| With `visibility_timeout_seconds` | **At-least-once** — expired messages are reclaimed and redelivered |

See [Crash recovery with visibility timeout](#crash-recovery-with-visibility-timeout) for details and tradeoffs.

## Configuration

### Deduplication

```python
# Default: deduplicate by full message content (1-hour TTL)
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

To prevent unbounded growth, cap the queue lengths:

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
- if you do use heartbeats, the heartbeat interval must be no more than half of the visibility timeout
- recovery happens on consumer polling cadence rather than instantly
- heartbeats add background renewal work for active messages
- if a heartbeat fails (network error or stale lease), the heartbeat stops silently; the consumer continues processing but may find at ack time that the message was reclaimed by another consumer

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

When a message has been delivered more than `max_delivery_count` times (due to consumer crashes causing visibility-timeout reclaim), it is automatically routed to a dead-letter queue (`{name}::dead_letter`) instead of being redelivered. This prevents poison messages from cycling indefinitely.

Notes:
- requires `visibility_timeout_seconds` to be set (poison messages are only a concern with VT reclaim)
- the delivery count is tracked per-message in a Redis HASH and cleaned up on successful ack or move to completed/failed
- the delivery count increments when Redis grants the claim/lease, not when your handler begins running. If a process exits after Redis claims a message, that claim still counts toward `max_delivery_count`
- `max_delivery_count=1` means the message is delivered once; any reclaim routes it to the dead-letter queue
- without `max_delivery_count`, messages are redelivered indefinitely (existing behavior)

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
from redis_message_queue._redis_gateway import RedisGateway

# Custom retry logic, dedup TTL, or wait interval
gateway = RedisGateway(
    redis_client=client,
    retry_strategy=my_custom_retry,
    message_deduplication_log_ttl_seconds=3600,
    message_wait_interval_seconds=10,
    message_visibility_timeout_seconds=300,
)
queue = RedisMessageQueue("q", gateway=gateway)
```

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

## Async API

Replace the import to use the async variant — the API is identical:

```python
from redis_message_queue.asyncio import RedisMessageQueue
```

All examples work the same way. Remember to close the connection when done:

```python
import redis.asyncio as redis

client = redis.Redis()
# ... your code
await client.aclose()
```

## Known limitations

- **No metrics or observability hooks.** The library logs warnings (stale leases, heartbeat failures, transient errors) via Python's `logging` module but does not expose callbacks, event hooks, or metric counters. To monitor queue health, inspect the underlying Redis keys directly or parse log output.
- **Timed waits use polling claim loops.** To make claims recoverable after ambiguous connection drops, `wait_for_message_and_move()` uses idempotent Lua claim polling instead of raw blocking list-move commands. This adds a small polling cadence during timed waits.
- **Redis Lua is atomic, not rollback-transactional.** The built-in scripts now preflight queue key types and fail closed on `WRONGTYPE` before mutating queue state, but Redis does not undo earlier writes if a later script command fails for another reason (for example `OOM` under severe memory pressure).
- **Batch reclaim limit of 100.** The visibility-timeout reclaim Lua script processes at most 100 expired messages per consumer poll. Under extreme backlog this may delay recovery, but prevents any single poll from blocking Redis.
- **Redis Cluster requires hash tags.** The built-in queue uses multiple Redis keys per operation. Wrap the queue name in hash tags (for example `{myqueue}`) so every generated key lands in the same slot. When you pass a Redis Cluster client to the built-in queue/gateway path, incompatible names are rejected early.

For a full analysis, see [docs/production-readiness.md](docs/production-readiness.md).

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
