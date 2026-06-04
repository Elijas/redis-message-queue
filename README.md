# redis-message-queue

[![PyPI Version](https://img.shields.io/pypi/v/redis-message-queue?color=43cd0f&style=flat&label=pypi)](https://pypi.org/project/redis-message-queue)
[![PyPI Downloads](https://img.shields.io/pypi/dm/redis-message-queue?color=43cd0f&style=flat&label=downloads)](https://pypistats.org/packages/redis-message-queue)
[![License: MIT](https://img.shields.io/badge/License-MIT-43cd0f.svg?style=flat&label=license)](LICENSE)
[![Maintained: yes](https://img.shields.io/badge/yes-43cd0f.svg?style=flat&label=maintained)](https://github.com/Elijas/redis-message-queue/issues)
[![CI](https://github.com/Elijas/redis-message-queue/actions/workflows/ci.yml/badge.svg)](https://github.com/Elijas/redis-message-queue/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/Elijas/redis-message-queue/graph/badge.svg)](https://codecov.io/gh/Elijas/redis-message-queue)
[![Linter: Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

**Lightweight Python message queuing with Redis and built-in publish-side deduplication.** Deduplicate publishes within a TTL window, with crash recovery (at-least-once) on by default — across any number of producers and consumers.

```bash
pip install "redis-message-queue>=8.3.1,<9.0.0"
```

Requires Python >= 3.12 and Redis server >= 6.2.

**Mental model:** redis-message-queue is a *payload queue, not a task framework*. Producers publish a `str` or `dict`; consumers decide what it means. There is no task registry, result backend, scheduler, or handler-level retry policy — and an ordinary exception raised inside a handler is **terminal, not an automatic retry**. Coming from Celery, RQ, Dramatiq, or taskiq? Read [Migrating from task frameworks](#migrating-from-rq--celery--dramatiq--taskiq) before porting code.

## Quickstart

Redis must be running locally first: use `redis-server` or
`docker run -it --rm -p 6379:6379 redis:7`.

> **Local Redis data:** The sync and async quickstarts below connect to
> `redis://localhost:6379/0` and use the fixed queue namespace `quickstart`.
> Each snippet publishes a message, then claims and removes one message under
> that namespace. If local DB 0 already contains `quickstart` data that matters,
> use a disposable Redis instance, a separate DB/port, or change the URL/queue
> name before running them.

```python
import json
from uuid import uuid4
from redis import Redis
from redis_message_queue import RedisMessageQueue

client = Redis.from_url("redis://localhost:6379/0", decode_responses=True)
queue = RedisMessageQueue(
    "quickstart",
    client=client,
    deduplication=True,
    get_deduplication_key=lambda msg: msg["id"],
)
message = {"id": f"msg-{uuid4().hex}", "text": "hello"}
queue.publish(message)
with queue.process_message() as message:
    if message is not None:
        payload = json.loads(message)
        print(f"got {payload['text']}")
# Expected output: got hello
```

`RedisMessageQueue` itself is not a context manager. Use
`with queue.process_message() as message:` for each message.

> **Important:** In the sync API, work inside `process_message()` must be
> synchronous. If your handler is `async def`, returns a coroutine, or returns
> any other awaitable, use `redis_message_queue.asyncio.RedisMessageQueue`.
> The sync context manager does not inspect the handler's return value; an
> unawaited coroutine can be dropped while the message is acked. For sync
> callback-style handlers, use `process_message_callback(handler)`: it checks
> for awaitable returns before acking and raises `TypeError` if one is returned.

### Async quickstart

```python
import asyncio
import json
from uuid import uuid4
from redis.asyncio import Redis
from redis_message_queue.asyncio import RedisMessageQueue

async def main():
    client = Redis.from_url("redis://localhost:6379/0", decode_responses=True)
    queue = RedisMessageQueue(
        "quickstart",
        client=client,
        deduplication=True,
        get_deduplication_key=lambda msg: msg["id"],
    )
    message = {"id": f"msg-{uuid4().hex}", "text": "hello"}
    await queue.publish(message)
    async with queue.process_message() as message:
        if message is not None:
            payload = json.loads(message)
            print(f"got {payload['text']}")
    await client.aclose()

asyncio.run(main())  # Expected output: got hello
```

## Why redis-message-queue

**The problem:** You're sending messages between services or workers and need guarantees. Simple Redis LPUSH/BRPOP loses messages on crashes, doesn't deduplicate, and gives you no visibility into what succeeded or failed.

**The solution:** Atomic Lua scripts for publish + dedup, a processing queue for in-flight tracking (with optional crash recovery via visibility timeouts), and optional success/failure logs for observability.

| Feature | Details |
|---------|---------|
| **Deduplicated publish** | Lua-scripted atomic SET NX + LPUSH prevents duplicate enqueues within a configurable TTL window (default: 1 hour), even with producer retries. Requires an explicit `get_deduplication_key` callable so your application defines what counts as a duplicate. Note: deduplication is publish-side only and does not prevent duplicate *delivery* under at-least-once visibility-timeout reclaim |
| **Visibility-timeout redelivery** | Crashed or stalled consumers' messages are reclaimed and redelivered when a visibility timeout is configured |
| **Completed & failed queues** | Optional completed/failed queues for auditing, inspection, and application-owned manual reprocessing, with configurable max length to prevent unbounded growth |
| **Dead-letter queue** | Poison messages that exceed a configurable delivery count are automatically routed to a dead-letter queue instead of being redelivered indefinitely |
| **Graceful shutdown** | Built-in interrupt handler lets consumers finish current work before stopping |
| **Lease heartbeats** | Optional background lease renewal keeps long-running handlers from being redelivered prematurely |
| **Connection retries** | Exponential backoff with jitter for Redis ops; idempotent paths (deduplicated publish, ack, lease renewal, claim recovery) replay safely under retries, while non-deduplicated publish is intentionally not retried so the caller decides whether to retry (accepting potential duplicates). See [Custom gateway](docs/configuration.md#custom-gateway) |
| **Async support** | Mirrored async variant — same method and parameter names, but callbacks are not interchangeable: the sync queue rejects async callables, and on the async queue `on_event` must be async (`get_deduplication_key` and `on_heartbeat_failure` may be sync or async) |

All features are optional and can be enabled or disabled as needed.

### Delivery semantics

| Configuration | Delivery guarantee |
|---|---|
| Default (`visibility_timeout_seconds=300`) | **At-least-once** — expired messages are reclaimed and redelivered |
| With `visibility_timeout_seconds=None, max_delivery_count=None` | **At-most-once** — a consumer crash loses the in-flight message |

See [Crash recovery with visibility timeout](docs/configuration.md#crash-recovery-with-visibility-timeout) for details and tradeoffs.
Because delivery-count limits depend on visibility-timeout reclaim, disabling
lease-based crash recovery requires setting both `visibility_timeout_seconds=None`
and `max_delivery_count=None`.

> **Important:** Ordinary `Exception` subclasses raised by handler code are
> terminal. This library is a payload queue, not a task framework: raising an
> ordinary `Exception` inside `process_message()` does not requeue the message.
> With `enable_failed_queue=False`, the message is removed from `processing`;
> with `enable_failed_queue=True`, it is moved to the failed list.
>
> Fatal `BaseException` paths such as `KeyboardInterrupt`, `SystemExit`, and
> externally cancelled async tasks (`asyncio.CancelledError`) are
> shutdown/cancellation paths, not failed handler work. They can leave the
> message in `processing` for visibility-timeout reclaim, or orphan it when
> `visibility_timeout_seconds=None, max_delivery_count=None`; see
> [Graceful shutdown](docs/configuration.md#graceful-shutdown) and
> [Abandoned in-flight messages](docs/configuration.md#abandoned-in-flight-messages).

## Configuration

Every feature is optional and set through constructor arguments. The complete reference — with runnable snippets for each option — lives in **[docs/configuration.md](docs/configuration.md)**:

- **[Deduplication](docs/configuration.md#deduplication)** — publish-side dedup keys, TTL windows, and cardinality guidance
- **[Success and failure tracking](docs/configuration.md#success-and-failure-tracking)** — optional completed/failed audit lists and their caps
- **[Publish backpressure](docs/configuration.md#publish-backpressure)** — `max_pending_length` and the `raise` / `drop_oldest` / `block` overload policies
- **[Crash recovery with visibility timeout](docs/configuration.md#crash-recovery-with-visibility-timeout)** — leases, heartbeats, and redelivery
- **[Ordering and multi-consumer fairness](docs/configuration.md#ordering-and-multi-consumer-fairness)** — the claim-order guarantee and its limits
- **[Dead-letter queue](docs/configuration.md#dead-letter-queue)** — routing poison messages off the redelivery path
- **[Graceful shutdown](docs/configuration.md#graceful-shutdown)** — `drain()` / `aclose()` and the three shutdown shapes
- **[Custom gateway](docs/configuration.md#custom-gateway)** — tuning retries and dedup TTL, or subclassing for new semantics
- **[Connection pool sizing](docs/configuration.md#connection-pool-sizing)** — sizing Redis pools for heartbeat concurrency

## Async API

Replace the import to use the async variant — it mirrors the sync API with the
same method and parameter names (call the awaitable methods with `await`):

```python
from redis_message_queue.asyncio import RedisMessageQueue
```

The sync and async classes intentionally share names. In modules that use both,
alias the imports explicitly, for example
`from redis_message_queue import RedisMessageQueue as SyncRedisMessageQueue` and
`from redis_message_queue.asyncio import RedisMessageQueue as AsyncRedisMessageQueue`.

Callbacks are not interchangeable between the two classes: the sync queue rejects
async callables, and on the async queue `on_event` must be async, while
`get_deduplication_key` and `on_heartbeat_failure` may be sync or async.

The examples otherwise work the same way. Remember to close the connection when
done:

```python
import redis.asyncio as redis

client = redis.Redis()
# ... your code
await client.aclose()
```

For the sync Redis client, call `client.close()` during application shutdown when
you own the client lifecycle.

## Migrating from RQ / Celery / Dramatiq / taskiq

redis-message-queue is a payload queue, not a task framework. It has no task
registry, job object, result backend, scheduler, workflow canvas, callback
graph, or handler-level retry policy. Producers publish a `str` or `dict`
payload, and consumers decide what that payload means.

The most important semantic differences from sibling task libraries are:

- Ordinary `Exception` subclasses raised by handler code are terminal. Raising
  an ordinary `Exception` inside `process_message()` removes the message from
  `processing`, or moves it to the failed list when `enable_failed_queue=True`;
  it does not requeue or retry the message. Fatal `BaseException` shutdown or
  cancellation paths are covered by
  [Graceful shutdown](docs/configuration.md#graceful-shutdown) and
  [Abandoned in-flight messages](docs/configuration.md#abandoned-in-flight-messages).
- `visibility_timeout_seconds` is a crash/stall recovery lease, not a runtime
  limit. Slow handlers are not interrupted; after the lease expires another
  consumer can process the same payload concurrently.
- `on_event` is telemetry only. Callback exceptions are logged and emitted as
  `RuntimeWarning`, but they do not affect ack/nack, failed-queue movement, or
  any other message outcome. Do not use `on_event` for sagas, follow-up writes,
  billing callbacks, or other correctness-critical work.
- Dict payloads are JSON data, not Python call arguments. JSON does not
  preserve every Python type: tuples become lists, and sets or custom objects
  raise unless you encode them into JSON-native values first.
- Process-global signal ownership cannot be safely chained with Celery, RQ, or
  Dramatiq CLI workers. Prefer one top-level owner that calls `queue.drain()`
  or sets an application stop event, and run sibling workers in separate
  processes.

When migrating on the same Redis deployment, prefer separate Redis DBs or hard
namespaces. Do not point a Celery, RQ, Dramatiq, or taskiq worker at an rmq
pending key. A sibling worker can pop the rmq stored message, fail its own
decoder, and leave the rmq queue without that message. Also avoid custom
`key_separator` values that synthesize another library's key namespace, such as
using `":queue:"` with a queue name that overlaps RQ keys. rmq has no fixed
library prefix; generated keys share the Redis DB namespace with every other
Redis user.

Set `strict_envelope_decoding=True` if this Redis is shared with sibling task
libraries (Celery, RQ, Dramatiq) to fail-fast on foreign payloads. With the
default `False`, non-rmq values that do not start with the rmq envelope prefix
remain backward-compatible raw messages and are yielded to the handler.

## Production notes

Deploying to production? See **[docs/operations.md](docs/operations.md)** for [fork safety and pre-fork servers](docs/operations.md#fork-safety-and-pre-fork-servers) (gunicorn `--preload`, `multiprocessing`, `ProcessPoolExecutor`) and [Redis memory sizing for deduplication and replay metadata](docs/operations.md#redis-memory-sizing-for-deduplication-and-replay-metadata).

## Observability

redis-message-queue emits lifecycle events through an optional `on_event` callback — publish, dedup hits, claims, reclaims, ack/nack, DLQ moves, heartbeats, drain, and retries — and exposes a typed exception hierarchy rooted at `RedisMessageQueueError`. The full guide (event catalog, dispatch context, timing versus Redis commit, intentionally silent paths, secret-safety for `event.error`, and the exception tree) is in **[docs/observability.md](docs/observability.md)**.

## Known limitations

Known limitations and edge cases — timed-wait polling, Lua atomicity, batch-reclaim bounds, Redis Cluster hash-tag requirements, non-ASCII payload sizing, and client-side `Retry` interactions — are catalogued in **[docs/operations.md#known-limitations](docs/operations.md#known-limitations)**. For the full residual-risk register, see **[docs/production-readiness.md](docs/production-readiness.md)**.

## Upgrading

Version migration guides — v7→v8, v6→v7, v5→v6, v2→v3, and the destructive-on-live-queues configuration changes — are in **[UPGRADING.md](UPGRADING.md)**. Per-release detail lives in **[CHANGELOG.md](CHANGELOG.md)**.

## Running locally

These examples ship in the GitHub repo, not the PyPI package — clone the repo and
run `uv sync` first.

Start a local Redis server with `redis-server`, or with Docker:

```bash
docker run -it --rm -p 6379:6379 redis:7
```

Try the [examples](https://github.com/Elijas/redis-message-queue/tree/main/examples) with multiple terminals:

These examples connect to `REDIS_URL` when it is set; otherwise they use
`redis://localhost:6379/0` (database 0). The send and receive examples use the
fixed queue name `my_message_queue`: publishers write queue data under that
namespace, including pending and deduplication keys, and consumers can claim and
remove messages from it. If existing local Redis data in `localhost:6379/0`
matters, run a disposable Redis instance or select a separate Redis database
before running the commands.

```bash
# Two publishers
uv run python -m examples.send_messages
uv run python -m examples.send_messages

# Three consumers
uv run python -m examples.receive_messages
uv run python -m examples.receive_messages
uv run python -m examples.receive_messages
```

These publisher and consumer examples are long-running; stop them with Ctrl+C or
another interrupt when you are done. Publishers print `Success: Sent message ...`
or `Duplicate: Message ...`. Consumers print `Received Message: ...` before
simulated `time.sleep(...)` work and `Finished processing message ...`
afterward. On a clean single handled shutdown, the consumer prints `Exiting...`
and the signal handler prints `Received signal: ...` on stderr.

When examples run through wrappers such as `uv run`, terminal interrupts may
reach the process group more than once. If an interrupt lands during the
consumer's simulated `time.sleep(...)` work, you can see a `KeyboardInterrupt`
traceback instead of the clean `Exiting...` line.

![GitHub Repo stars](https://img.shields.io/github/stars/elijas/redis-message-queue?style=flat&color=fcfcfc&labelColor=white&logo=github&logoColor=black&label=stars)
