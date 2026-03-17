# redis-message-queue

[![PyPI Version](https://img.shields.io/badge/v0.10.1-version?color=43cd0f&style=flat&label=pypi)](https://pypi.org/project/redis-message-queue)
[![PyPI Downloads](https://img.shields.io/pypi/dm/redis-message-queue?color=43cd0f&style=flat&label=downloads)](https://pypistats.org/packages/redis-message-queue)
[![License: MIT](https://img.shields.io/badge/License-MIT-43cd0f.svg?style=flat&label=license)](LICENSE)
[![Maintained: yes](https://img.shields.io/badge/yes-43cd0f.svg?style=flat&label=maintained)](https://github.com/Elijas/redis-message-queue/issues)
[![CI](https://github.com/Elijas/redis-message-queue/actions/workflows/ci.yml/badge.svg)](https://github.com/Elijas/redis-message-queue/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/Elijas/redis-message-queue/graph/badge.svg)](https://codecov.io/gh/Elijas/redis-message-queue)
[![Linter: Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

**Reliable Python message queuing with Redis and built-in deduplication.** Publish once, process once, recover from crashes — across any number of producers and consumers.

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
            # Auto-acknowledged on success, moved to failed queue on exception
```

## Why redis-message-queue

**The problem:** You're sending messages between services or workers and need guarantees. Simple Redis LPUSH/BRPOP loses messages on crashes, doesn't deduplicate, and gives you no visibility into what succeeded or failed.

**The solution:** Atomic Lua scripts for publish + dedup, a processing queue for crash recovery, and optional success/failure logs for observability.

| Feature | Details |
|---------|---------|
| **Exactly-once publish** | Lua-scripted atomic SET NX + LPUSH prevents duplicate messages even with producer retries |
| **Crash-safe processing** | Messages can be reclaimed from stalled consumers with optional visibility-timeout redelivery |
| **Message deduplication** | Configurable TTL-based dedup with custom key functions for content-based deduplication |
| **Success & failure logs** | Optional completed/failed queues for auditing and reprocessing |
| **Graceful shutdown** | Built-in interrupt handler lets consumers finish current work before stopping |
| **Lease heartbeats** | Optional background lease renewal keeps long-running handlers from being redelivered prematurely |
| **Automatic retries** | Exponential backoff with jitter for idempotent Redis operations; unsafe queue-moving calls fail fast to avoid duplicates or skipped messages |
| **Async support** | Drop-in async variant with identical API |

All features are optional and can be enabled or disabled as needed.

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
