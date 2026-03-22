# redis-message-queue

[![PyPI Version](https://img.shields.io/badge/v0.10.1-version?color=43cd0f&style=flat&label=pypi)](https://pypi.org/project/redis-message-queue)
[![PyPI Downloads](https://img.shields.io/pypi/dm/redis-message-queue?color=43cd0f&style=flat&label=downloads)](https://pypistats.org/packages/redis-message-queue)
[![License: MIT](https://img.shields.io/badge/License-MIT-43cd0f.svg?style=flat&label=license)](LICENSE)
[![Maintained: yes](https://img.shields.io/badge/yes-43cd0f.svg?style=flat&label=maintained)](https://github.com/Elijas/redis-message-queue/issues)
[![CI](https://github.com/Elijas/redis-message-queue/actions/workflows/ci.yml/badge.svg)](https://github.com/Elijas/redis-message-queue/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/Elijas/redis-message-queue/graph/badge.svg)](https://codecov.io/gh/Elijas/redis-message-queue)
[![Linter: Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

**Reliable Python message queuing with Redis and built-in deduplication.** Deduplicate publishes within a TTL window, with optional crash recovery — across any number of producers and consumers.

```bash
pip install "redis-message-queue>=0.10.0,<0.11.0"
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
| **Deduplicated publish** | Lua-scripted atomic SET NX + LPUSH prevents duplicate enqueues within a configurable TTL window (default: 1 hour), even with producer retries. Supports custom key functions for content-based deduplication |
| **Visibility-timeout redelivery** | Crashed or stalled consumers' messages are reclaimed and redelivered when a visibility timeout is configured |
| **Success & failure logs** | Optional completed/failed queues for auditing and reprocessing |
| **Graceful shutdown** | Built-in interrupt handler lets consumers finish current work before stopping |
| **Lease heartbeats** | Optional background lease renewal keeps long-running handlers from being redelivered prematurely |
| **Connection retries** | Exponential backoff with jitter for Redis operations (publish, ack, lease renewal); message-claim calls fail fast to prevent double-consumption. Publish without dedup retries but may duplicate on ambiguous failures |
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

Without a visibility timeout, messages that are being processed when a consumer crashes remain in the processing queue indefinitely and are not redelivered.

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

If you pair `gateway=` with `heartbeat_interval_seconds`, the gateway must expose a public
`message_visibility_timeout_seconds` value so the queue can validate the heartbeat safely.

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
