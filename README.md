# redis-message-queue

[![PyPI Version](https://img.shields.io/pypi/v/redis-message-queue?color=43cd0f&style=flat&label=pypi)](https://pypi.org/project/redis-message-queue)
[![PyPI Downloads](https://img.shields.io/pypi/dm/redis-message-queue?color=43cd0f&style=flat&label=downloads)](https://pypistats.org/packages/redis-message-queue)
[![License: MIT](https://img.shields.io/badge/License-MIT-43cd0f.svg?style=flat&label=license)](LICENSE)
[![Maintained: yes](https://img.shields.io/badge/yes-43cd0f.svg?style=flat&label=maintained)](https://github.com/Elijas/redis-message-queue/issues)
[![CI](https://github.com/Elijas/redis-message-queue/actions/workflows/ci.yml/badge.svg)](https://github.com/Elijas/redis-message-queue/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/Elijas/redis-message-queue/graph/badge.svg)](https://codecov.io/gh/Elijas/redis-message-queue)
[![Linter: Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

**Lightweight Python message queuing with Redis and built-in publish-side deduplication.** Deduplicate publishes within a TTL window, with optional crash recovery — across any number of producers and consumers.

```bash
pip install "redis-message-queue>=8.2.7,<9.0.0"
```

Requires Redis server >= 6.2.

## Quickstart

Redis must be running locally first: use `redis-server` or
`docker run -p 6379:6379 redis:7`.

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

> **Important:** Handler exceptions are terminal. This library is a payload
> queue, not a task framework: raising inside `process_message()` does not
> requeue the message. With `enable_failed_queue=False`, the message is removed
> from `processing`; with `enable_failed_queue=True`, it is moved to the failed
> list.

## Configuration

### Deduplication

```python
# Deduplicate by order ID for a 1-hour TTL
queue = RedisMessageQueue(
    "q", client=client,
    deduplication=True,
    get_deduplication_key=lambda msg: msg["order_id"],
)

# Disable deduplication entirely
queue = RedisMessageQueue("q", client=client, deduplication=False)
```

#### Dedup key callable must return a non-empty, high-cardinality, tenant-scoped string

When `deduplication=True`, `get_deduplication_key` is required. The callable is
called once per publish and must return a `str` that uniquely represents the
deduplication scope for that message. Returning `None` or `""` raises
`ConfigurationError` at publish time; returning a non-`str` value raises
`TypeError`.

Use stable, high-cardinality keys that include any tenant or account boundary
needed by your system:

```python
queue = RedisMessageQueue(
    "orders",
    client=client,
    deduplication=True,
    get_deduplication_key=lambda msg: f"{msg['tenant_id']}:{msg['order_id']}",
)
```

Avoid fallback patterns such as `lambda msg: msg.get("order_id", "")`.
Missing fields should fail loudly instead of collapsing unrelated messages into
one deduplication key.

Deduplication markers and publish retry-safety markers are Redis TTL keys. A
large forward step in the Redis server expiration clock during an in-call retry
window can expire those markers before the Python-side monotonic retry budget
elapses, allowing a duplicate publish. This is an extreme anomaly, mainly
relevant under cluster-wide NTP step corrections while a producer is retrying
after an ambiguous Redis write.

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

### Publish backpressure

By default, the pending queue is unbounded (`max_pending_length=None`), matching
the v5 behavior. Set `max_pending_length` when producers can outrun consumers
and Redis memory must fail closed before the broker is exhausted:

```python
queue = RedisMessageQueue(
    "q",
    client=client,
    max_pending_length=100_000,
    pending_overload_policy="raise",  # "raise", "drop_oldest", or "block"
)
```

The built-in Redis path checks pending depth and enqueues in the same Lua script,
so concurrent publishers cannot race above the configured cap. Overload policies:

- `raise` raises `QueueBackpressureError` and leaves the pending list unchanged.
- `drop_oldest` removes the oldest pending message (`RPOP`) before enqueueing the
  new message. This is silent data loss by design; deduplication markers for
  dropped messages are not removed, so a dropped duplicate may still be
  suppressed until its dedup TTL expires. The current event contract emits
  `publish/success` for the new message, but no separate `on_event` signal for
  the dropped message.
- `block` retries the atomic check until space opens or
  `pending_overload_block_timeout_seconds` elapses (default: 1.0), then raises
  `QueueBackpressureError`.

These limits apply only to the pending list at publish time. They do not cap
messages already in `processing`, dead-letter queues, deduplication keys, or
replay metadata. `max_completed_length` and `max_failed_length` only bound the
completed/failed history lists. Size pending payload memory separately from the
dedup/replay metadata described in
[Redis memory sizing](#redis-memory-sizing-for-deduplication-and-replay-metadata).

When using `gateway=`, configure backpressure on the gateway directly, for
example `RedisGateway(redis_client=client, max_pending_length=100_000)`.

### Crash recovery with visibility timeout

```python
queue = RedisMessageQueue(
    "q",
    client=client,
    visibility_timeout_seconds=300,
    heartbeat_interval_seconds=60,
)
```

> **Important:** `visibility_timeout_seconds` is a lease, not a handler runtime
> cap. rmq never interrupts a long-running handler. If the lease expires while
> the handler continues, another consumer can reclaim and process the same
> message concurrently.

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

Visibility deadlines use Redis server time (`TIME`), not Python process time.
A forward step in the Redis server clock can make a live lease appear expired
and allow premature redelivery while the original consumer is still processing;
a backward step can delay reclaim of truly abandoned messages. Treat NTP step
corrections on Redis hosts as a deployment risk. Prefer time-synchronization
discipline that slews corrections rather than stepping the Redis clock.

### Ordering and multi-consumer fairness

The built-in queue is a shared-pull Redis list. Successful publishes push to the
left side of the pending list, and claims pop from the right side, so Redis
grants claims in enqueue order in the no-failure path.

This is a claim-order guarantee only. It is not a completion-order guarantee:
multiple consumers process concurrently, handlers can run for different
durations, and younger messages can finish before older messages.

With `visibility_timeout_seconds` enabled, expired messages from `processing`
are reclaimed before fresh pending work on the next consumer poll. A reclaimed
message may be delivered after younger messages were already processed, and may
be processed concurrently with a stale original handler if that handler keeps
running after its lease expires.

Expired reclaims are ordered by lease deadline within one reclaim batch.
`CLAIM_MESSAGE_WITH_VISIBILITY_TIMEOUT_LUA_SCRIPT` selects expired leases with
`ZRANGEBYSCORE ... LIMIT 0, 100` to bound Redis Lua execution time. When more
than 100 messages expire together, the next poll can append a later reclaim
batch at the claimable end of the pending list ahead of leftovers from the
previous batch, so cross-batch redelivery order is not guaranteed.

`max_delivery_count` can skip over poison messages during a claim poll by moving
over-limit messages to the dead-letter queue and returning a later pending
message. Deduplication is publish-side only: duplicate publishes are not
enqueued and therefore do not occupy a queue position.

Handler exceptions are not retries: the default behavior removes the message
from `processing`, or moves it to the failed queue when enabled. Redelivery is
for crash, stall, or stale-lease paths where cleanup does not complete.

Multiple consumers contend for the same queue. The next message goes to the
consumer whose claim request Redis executes next. There is no round-robin,
equal-share, or starvation-freedom guarantee; faster consumers can receive more
than 1/N of messages.

### If you need stronger ordering or fairness guarantees

- **Strict queue-wide processing order** — use a single consumer per queue.
  Multiple consumers will interleave handler completions.
- **Per-key processing order** — partition by key into multiple queues
  (`queue_<hash(key) % N>`), and consume each partition with a single consumer.
- **Equal-share / round-robin fairness across consumers** — choose a different
  scheduler. This queue does not guarantee that any individual consumer makes
  forward progress at any specific rate.
- **Cross-batch ordering after reclaim** — accept that reclaimed messages will
  reappear after newer un-reclaimed messages have been consumed. If your handler
  must observe original publish order, persist that order in the payload (for
  example, a sequence number set by the producer). For clock-related operator
  detail behind reclaim behavior, see
  [production readiness R11](docs/production-readiness.md#r11-redis-clock-dependencies).

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
>
> Process-global signal ownership cannot be safely chained with task-worker
> CLIs such as Celery, RQ, or Dramatiq. Run sibling workers in separate
> processes, or install one top-level signal owner that calls `queue.drain()`
> / `queue.aclose()` or sets an application stop event.

If another library owns SIGTERM/SIGINT in the same process, adapt its shutdown
signal to rmq with a user-owned event instead of installing rmq signal handlers:

```python
import threading

from redis_message_queue import EventDrivenInterruptHandler, RedisMessageQueue

stop_event = threading.Event()
interrupt = EventDrivenInterruptHandler(stop_event)
queue = RedisMessageQueue("q", client=client, interrupt=interrupt)

while not interrupt.is_interrupted():
    with queue.process_message() as message:
        if message is not None:
            process(message)

# In the sibling library's shutdown hook:
stop_event.set()
queue.drain(timeout=25)
```

The caller MUST set `stop_event` before exiting. rmq observes
`is_interrupted()` and exits cooperatively; it does not call `sys.exit()` or
otherwise force process shutdown.

There are three distinct shutdown shapes; pick the one that matches your runtime:

| Shape | Trigger | In-flight handler | Pending claim IDs |
|---|---|---|---|
| **Flag-based soft drain** (`GracefulInterruptHandler`) | First SIGINT/SIGTERM flips a flag | Runs to completion | Drained on the next claim call, not on signal arrival |
| **Async task cancellation** (`asyncio.CancelledError`) | Framework cancels the worker task (Uvicorn/K8s SIGTERM in many setups) | **Hard abort** — message stays in `processing`; with VT it is reclaimed at deadline expiry, without VT it is orphaned | Not drained |
| **Explicit drain** (`drain()` / `aclose()`) | You call the method | Caller's responsibility to let it finish (drain does **not** cancel) | Drained synchronously via the gateway recovery path; new publishes are refused |

Use `drain()` / `aclose()` to bridge K8s `preStop` / SIGTERM grace windows without
relying on signal interception:

```python
# sync — in your SIGTERM handler or preStop hook
queue.drain(timeout=25)   # refuses new publishes/claims, recovers pending claim IDs
worker_thread.join()      # wait for in-flight process_message to finish

# async — same shape
await queue.aclose(timeout=25)
await worker_task         # task observes ``_draining`` and exits its loop
```

`drain()` / `aclose()` set a queue-local flag so subsequent `process_message()`
calls yield `None` immediately and subsequent `publish()` calls raise
`QueueDrainedError("queue is drained")`. Drain also gates the publish path:
if a publish is already inside the queue instance's publish path, drain waits
for that publish to finish before it returns; publishes that arrive after the
drained flag is set are rejected. The drained state is local to that Python
queue object and is not written to Redis, so constructing a fresh
`RedisMessageQueue(...)` over the same keys remains usable. A separate process
or separate queue instance against the same Redis keys is not marked drained by
this call. For multi-process graceful shutdown, each process must drain its own
queue instances.

Drain does not cancel in-flight handlers — the caller must arrange handler
exit through normal thread/task coordination. Returns `True` if all in-memory
pending claim IDs were recovered within the timeout; `False` if the deadline
fired or transient Redis errors left claim IDs pending (call again to retry).
`timeout=0` reports current state without attempting recovery.

#### Abandoned in-flight messages

Abandoned in-flight messages are recovered lazily. Async tasks cancelled
without `aclose()`, or sync processes killed mid-handler, can leave the message
and its processing/lease metadata in Redis until a later consumer claim path
triggers visibility-timeout reclaim. With visibility timeouts enabled, this is
the designed at-least-once recovery path: the message is delayed by the lease,
not lost. With `visibility_timeout_seconds=None`, there is no automatic reclaim
path. For low-visibility-timeout workloads, prefer an explicit `drain()` /
`aclose()` during shutdown so local pending claim IDs are recovered before
process exit.

`drain()` / `aclose()` timeouts are measured with Python monotonic clocks, but
any lease deadlines they recover were created from Redis server time. The same
Redis-clock step caveats from
[Crash recovery with visibility timeout](#crash-recovery-with-visibility-timeout)
apply to when abandoned work becomes reclaimable.

> **Heartbeat caveat (best-effort stop):** when `heartbeat_interval_seconds` is
> set, the heartbeat sidecar's `stop()` is bounded but not strictly quiescent —
> a slow renewal in flight when `process_message` exits may still write to
> Redis after the caller believes shutdown is complete. The renewal is bounded
> by the configured visibility timeout and the lease token check on the Redis
> side, but plan for a small post-shutdown overlap rather than instant quiesce.

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

When `gateway=` is supplied, queue-level constructor defaults are not copied
into the gateway. For example, `RedisMessageQueue(..., gateway=gateway)`
leaves visibility timeout and dead-letter routing disabled unless
`message_visibility_timeout_seconds` and `max_delivery_count` are configured on
the gateway itself. Passing the queue-level default values
`visibility_timeout_seconds=300` or `max_delivery_count=10` with `gateway=`
does not transfer those settings to the gateway.

The retry knobs configure an internal `tenacity` strategy: exponential
backoff with jitter, retry on transient Redis errors only, capped at
`retry_budget_seconds`. The budget is monotonic elapsed time from the first attempt (including attempt duration), not inter-attempt delay; it is unaffected by Python-host NTP jumps. A single attempt that takes longer than the budget results in zero retries. Setting `retry_budget_seconds=0` disables retry
entirely (single attempt; exceptions propagate). The library uses
`retry_budget_seconds` to size the operation-result cache TTL automatically,
so the previous footgun of an over-long retry budget out-living the cache
and producing misleading "cleanup was a no-op" warnings is now structurally
impossible. Note: tenacity may allow one additional attempt beyond the budget if the budget check passes at attempt start, so total monotonic elapsed time can exceed `retry_budget_seconds` by the duration of that final attempt.

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

## Migrating from RQ / Celery / Dramatiq / taskiq

redis-message-queue is a payload queue, not a task framework. It has no task
registry, job object, result backend, scheduler, workflow canvas, callback
graph, or handler-level retry policy. Producers publish a `str` or `dict`
payload, and consumers decide what that payload means.

The most important semantic differences from sibling task libraries are:

- Handler exceptions are terminal. Raising inside `process_message()` removes
  the message from `processing`, or moves it to the failed list when
  `enable_failed_queue=True`; it does not requeue or retry the message.
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

### Fork safety and pre-fork servers

Construct Redis clients and `RedisMessageQueue` instances after a process forks.
This is the recommended pattern for `multiprocessing`, `ProcessPoolExecutor`,
and pre-fork servers such as gunicorn with `--preload`.

```python
def worker_main():
    client = redis.Redis()
    queue = RedisMessageQueue("jobs", client=client)
    ...
```

Avoid constructing a queue/client in a parent process and then using that same
object in forked children, especially if the parent has already run any Redis
command. The queue stores the user-provided Redis client and process-local
claim-recovery state. Inherited Redis sockets can corrupt the Redis protocol if
two processes use the same file descriptor.

Notes:

- The sync redis-py pooled client attempts to reset its connection pool after
  fork, but this does not apply to every client shape.
- The built-in sync gateway rejects `redis.Redis(single_connection_client=True)`
  because that mode pins one socket instead of using the pool.
- Do not share `redis.asyncio.Redis` or async queues across fork; create or
  reconnect them in the child process.
- If you use `GracefulInterruptHandler`, create it in the worker process after
  fork so signal ownership is local to that worker.
- The heartbeat sidecar is lazy and starts only while processing a leased
  message. Do not call `fork()` from inside active message handlers unless the
  child exits without using the inherited queue/client.

#### Forking after constructing GracefulInterruptHandler

If your application constructed `GracefulInterruptHandler` in the parent process
before `os.fork()` (for example, via module import in a pre-fork app server),
forked children cannot construct a fresh handler for the same signal because the
inherited signal table still routes to the parent-process handler.

In each child process, call `parent_handler.reset()` before constructing a fresh
handler:

```python
def worker_main():
    # Inherited handler from parent - reset it.
    if shared.interrupt_handler is not None:
        shared.interrupt_handler.reset()

    # Now safe to construct a fresh handler for this child.
    interrupt = GracefulInterruptHandler()
    queue = RedisMessageQueue("jobs", client=redis.Redis(), interrupt=interrupt)
    ...
```

Alternatively, defer all construction (handler and queue) to inside
`worker_main()` and pass `--no-preload` (or equivalent) to your app server. That
avoids the parent-construct hazard entirely.

### Redis memory sizing for deduplication and replay metadata

When deduplication is enabled, each distinct dedup key creates one Redis string
for `message_deduplication_log_ttl_seconds` (default: 3600 seconds). The dedup
key is whatever your `get_deduplication_key` callable returns, so choose a
short, stable logical ID and size Redis for:

```text
peak_unique_publish_rate_per_second
* message_deduplication_log_ttl_seconds
* bytes_per_dedup_key
```

Use 200 bytes per dedup key as a conservative starting point for short queue
names, then validate with `MEMORY USAGE` in your Redis version. Example:
1,000 unique messages/s * 3,600s * 200 B ~= 720 MB for dedup markers alone.
A 24h dedup window at the same rate is 86.4M keys, or roughly 17 GB before
message payload lists, lease metadata, completed/failed queues, and allocator
fragmentation.

Operation-result replay keys are normally deleted after a successful call, but
may live until their TTL after ambiguous connection drops or failed cleanup
deletes. With visibility timeouts, active claims also store replay metadata
until ack or reclaim. Without visibility timeouts, abandoned claims leave
`claim_result_ids` and `claim_result_backrefs` fields until the message is
acked or manually cleaned.

`max_completed_length` and `max_failed_length` only bound the completed/failed
lists. They do not bound deduplication keys or replay metadata.

Avoid sharing queue Redis DBs with unrelated high-cardinality workloads. If
idempotency matters, prefer explicit capacity planning and `noeviction` with
alerts over LRU/random eviction policies: evicting dedup/replay keys before
their TTL can weaken duplicate suppression and retry result replay.

## Observability

Queue instances accept an optional `on_event` callback for metrics, tracing, or
structured logging. The sync queue expects a regular callable; the async queue
expects an async callable:

```python
from redis_message_queue import QueueEvent, RedisMessageQueue

def on_event(event: QueueEvent) -> None:
    ...

queue = RedisMessageQueue("jobs", client=client, on_event=on_event)
```

Events cover publish, dedup hits, claim/empty polls, reclaim, ack/nack,
completed/failed cleanup, DLQ moves, heartbeat renewal, stale leases, drain,
cleanup and trim failures, and retry attempts. Callback exceptions are logged and
reported with `RuntimeWarning`, but never propagate into queue operations.
`on_event` is telemetry only: use it for metrics, tracing, and logging, not for
sagas, follow-up writes, billing callbacks, or other correctness-critical
work. Package logs remain diagnostic; use `on_event` rather than log parsing
for metrics.

```python
from opentelemetry import trace
from prometheus_client import Counter
from redis_message_queue import QueueEvent, RedisMessageQueue

events_total = Counter(
    "rmq_events_total",
    "redis-message-queue lifecycle events",
    ["queue", "operation", "outcome", "exception_type"],
)
SPAN_SINK_TRUSTED = False

def observe(event: QueueEvent) -> None:
    events_total.labels(
        event.queue, event.operation, event.outcome, event.exception_type or ""
    ).inc()
    if event.error is not None and SPAN_SINK_TRUSTED:
        trace.get_current_span().record_exception(event.error)

queue = RedisMessageQueue("jobs", client=client, on_event=observe)
```

#### ⚠ Secrets in `event.error`

`event.error` is the actual exception object — it retains the exception
message, `__cause__` chain, and traceback. These can contain sensitive content:
Redis credentials in connection-error messages, message payloads in handler
exceptions, environment values in stack-frame locals.

When exporting to telemetry sinks (OpenTelemetry, Sentry, Datadog), prefer the
redaction-friendly `event.exception_type` for metrics and labels. Use
`event.error` for full structured error data ONLY if your sink is
trust-equivalent to your application logs and is access-controlled.

Recommended pattern:

```python
def on_event(event: QueueEvent) -> None:
    # Metric labels — always safe (just the exception class name)
    metric_counter.labels(
        operation=event.operation,
        outcome=event.outcome,
        exception_type=event.exception_type or "none",
    ).inc()

    # Full exception — only if your span sink is trusted
    if event.error is not None and SPAN_SINK_TRUSTED:
        span.record_exception(event.error)
```

#### Event dispatch context

Callbacks fire inline:

- **Sync queue:** the callback runs in the caller's thread. It sees
  contextvars, the OpenTelemetry current span, and structlog contextvars bound
  by the caller.
- **Async queue:** the callback is awaited in the current asyncio task. It has
  the same contextvars, span, and structlog visibility.
- **Sync heartbeat:** heartbeat events fire from a separate
  `threading.Thread`. That thread does not inherit caller contextvars or the
  caller's OpenTelemetry current span. Use `event.message_id` and
  `event.lease_token_hash` for correlation.
- **Async heartbeat:** heartbeat events fire from an asyncio task. The task
  copies the context present when the heartbeat was started, so contextvars and
  OpenTelemetry spans bound at handler entry are visible.

#### Event timing vs. Redis commit

Most events are post-commit, emitted after the Redis command or Lua script
returned: `publish/success`, `publish_dedup_hit`, `claim/success`,
`claim_empty`, `claim_reclaim`, `ack`, `nack`, `completed`, `dlq`,
`lease_renew`, `trim_failed`, and `stale_lease_*`.

Pre-commit and mid-flight exceptions:

- `failed/failure` fires after the handler raises but before failed-queue
  cleanup completes. Use `nack` for cleanup-commit metrics; use `failed` for
  handler-exception attribution.
- `retry_attempt/failure` and `retry_exhausted` fire on the claim-loop retry
  path. The first Redis attempt may or may not have committed.
- `publish/failure`, `claim/failure`, and `cleanup_failed/failure` follow
  exceptions. Under an ambiguous lost response, Redis may have committed
  despite the exception. Treat them as "operation did not succeed from the
  caller's perspective", not "Redis did not commit".

#### Drain events

`drain()` and `close()` on the sync queue, and `drain()` and `aclose()` on the
async queue, emit `drain` events:

- `drain/start` when the queue-local drain flag is set.
- `drain/success` when pending claim IDs were recovered or no gateway drain
  hook is present.
- `drain/skipped` when the queue was already drained and the cached successful
  result is returned.
- `drain/failure` when pending claim recovery times out or otherwise leaves
  unresolved claim IDs.

Drain events use `timeout_seconds` for the caller-supplied timeout,
`pending_claim_ids` for the number of unresolved local claim IDs when known,
and `exception_type` / `error` on failure.

#### Intentionally silent paths

The following operations have no `on_event` surface by design:

- **B1 Cluster `pcall` cleanup failure:** three lease-aware Lua scripts wrap a
  data-derived `DEL` in `redis.pcall(...)` and ignore the result. This
  preserves queue safety on Cluster `CROSSSLOT` rejection but cannot be
  observed through `on_event`. Operators watching key-TTL behavior or Redis
  slow logs can detect orphans.
- **VT claim-store OOM compensation:** if the visibility-timeout Lua script
  cannot store the claim result, it removes the message from processing, pushes
  it back to pending, and returns `false`. Python translates that into
  `claim_empty/skipped`, the same shape as an empty poll. This is intentional
  fail-safe behavior; the message is not lost.
- **`drop_oldest` evictions:** when publish backpressure uses
  `pending_overload_policy="drop_oldest"`, the oldest pending message is
  discarded before the new message is enqueued. The successful enqueue emits
  `publish/success`, but there is no separate drop event for the discarded
  message in the current feature set.
- **Non-claim-loop retry attempts:** tenacity retries in deduplicated publish,
  ack/remove, move-to-completed/failed, and lease renewal collapse into the
  terminal operation's failure event. There is no per-attempt event for those
  paths.

The public exception hierarchy is rooted at `RedisMessageQueueError`.
Configuration value/combinations raise `ConfigurationError` (also a
`ValueError`), custom gateway contract violations raise `GatewayContractError`
(also a `TypeError`), and Lua `redis.error_reply(...)` failures raise
`LuaScriptError` (also a redis-py `ResponseError`). Publish overload raises
`QueueBackpressureError`; publish after explicit drain raises
`QueueDrainedError`. `CleanupFailedError` and `RetryBudgetExhaustedError` are
reserved categories for cleanup and retry surfaces.

## Known limitations

- **Timed waits use polling claim loops.** To make claims recoverable after ambiguous connection drops, `wait_for_message_and_move()` uses idempotent Lua claim polling instead of raw blocking list-move commands. This adds a small polling cadence during timed waits.
- **Redis Lua is atomic, not rollback-transactional.** The built-in scripts now preflight queue key types and fail closed on `WRONGTYPE` before mutating queue state, but Redis does not undo earlier writes if a later script command fails for another reason (for example `OOM` under severe memory pressure).
- **Batch reclaim limit of 100.** The visibility-timeout reclaim Lua script processes at most 100 expired messages per consumer poll. Under extreme backlog this may delay recovery, but prevents any single poll from blocking Redis.
- **Claim-attempt loop limit of 100 per poll.** The VT claim Lua script attempts at most 100 LMOVE+delivery-count checks per invocation. Under pathological conditions (>100 consecutive poison messages in pending), a single poll returns no message even though non-poison messages exist deeper in the queue. Subsequent polls drain the poison batch 100 at a time.
- **Cluster detection uses `isinstance(client, RedisCluster)`.** Wrapped or instrumented cluster clients that delegate without inheriting will bypass hash-tag validation. Custom gateways should set `is_redis_cluster = True` explicitly.
- **Redis Cluster requires hash tags.** The built-in queue uses multiple Redis keys per operation. Wrap the queue name in hash tags (for example `{myqueue}`) so every generated key lands in the same slot. When you pass a Redis Cluster client to the built-in queue/gateway path, incompatible names are rejected early.
- **Non-ASCII payloads use ~2x storage.** The default `ensure_ascii=True` in JSON serialization encodes non-ASCII characters as `\uXXXX` escape sequences. This is a deliberate compatibility choice.
- **Client-side `Retry` can duplicate non-deduplicated publishes.** If you construct your `redis.Redis` or `redis.asyncio.Redis` client with `retry=Retry(...)`, redis-py retries `ConnectionError` / `TimeoutError` at the connection layer — *below* this library. Idempotent operations (deduplicated `publish()`, lease-scoped cleanup) are safe because their Lua scripts replay the original result. `add_message()` (used by `publish()` when `deduplication=False`) is a bare `LPUSH` by default, or a single non-idempotent Lua enqueue when `max_pending_length` is set: this library deliberately does not retry it, but a client-level `Retry` will, and if the server executed the command before the response was lost the message is enqueued twice. redis-py 6.0+ changed the default standalone `Redis()` / `redis.asyncio.Redis()` retry policy from `None` (no retry) to a 3-attempt `ExponentialWithJitterBackoff`; pass `retry=None` explicitly if you need strict at-most-once semantics for non-deduplicated publishes, or accept the duplication risk. More broadly, any non-idempotent enqueue path is vulnerable if the connection drops after server execution but before the client receives the response; all other built-in operations (deduplicated publish, lease-scoped ack/move, lease renewal) use replay markers and are safe under client-level `Retry`.

  ```python
  import redis
  from redis_message_queue import RedisMessageQueue

  # Strict at-most-once for non-dedup messages: disable redis-py's
  # default 3-retry policy explicitly.
  client = redis.Redis(retry=None)
  queue = RedisMessageQueue("jobs", client=client)
  ```

  ```python
  import redis.asyncio as redis
  from redis_message_queue.asyncio import RedisMessageQueue

  # Strict at-most-once for non-dedup messages: disable redis-py's
  # default 3-retry policy explicitly.
  client = redis.Redis(retry=None)
  queue = RedisMessageQueue("jobs", client=client)
  ```
- **Redis Cluster default retry can stack with this library's retry budget.** In redis-py 6.0+, `RedisCluster()` constructs a default `ExponentialWithJitterBackoff` retry below this library's `retry_budget_seconds`. If you need a single retry surface, pass `retry=Retry(NoBackoff(), 0)` to the cluster client or reduce `retry_budget_seconds` to account for the lower-level retry window.

For a full analysis, see [docs/production-readiness.md](docs/production-readiness.md).

## Upgrading

### v7 to v8 migration

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

### v6 to v7 migration

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

### Configuration changes on live queues

> **Warning:** These changes are destructive on live queues. Drain the queue completely before applying them.

- **Do not change `name` or `key_separator` on a live queue.** Both settings define the Redis key namespace. Existing Redis keys become invisible to the new key scheme. Drain the queue completely before changing either value.
- **Do not rename `dead_letter_queue` on a live queue.** Existing DLQ records stay in the old list, while new failures route to the new list. Inspect or drain the old DLQ manually before switching names.
- **Do not toggle visibility timeout in either direction with messages in processing.** Messages claimed by non-VT consumers have no lease metadata, so VT-enabled consumers cannot reclaim them. Disabling VT later orphans existing lease deadline, lease token, and delivery count metadata and removes crash recovery for those in-flight messages. Drain the processing queue first.
- **Reducing `max_delivery_count` retroactively DLQs messages.** The delivery count hash persists across restarts. Messages whose accumulated count exceeds the new limit are immediately dead-lettered on next claim.
- **Changing `max_delivery_count` from a number to `None` leaves delivery metadata behind.** The delivery count hash continues to exist but is no longer consulted. Use this only after draining or after planning manual cleanup of the delivery-count hash.
- **Changing `get_deduplication_key` changes the dedup keyspace.** Existing dedup records become inert for the duration of their TTL. Drain the queue or clear the old deduplication keys before switching key functions.
- **Disabling `deduplication` has a retention-window overlap.** Existing dedup records remain in Redis until their TTL expires, but new publishes bypass them. Republishes that would have been suppressed under the old setting can enqueue during that window.
- **Disabling `enable_failed_queue` stops recording handler failures.** Existing failed entries remain in Redis, but new failures are removed from `processing` without being appended to the failed queue. If `max_delivery_count=None` is also set, repeated handler failures can be dropped with no DLQ or failed-queue record; see [Dead-letter queue](#dead-letter-queue).
- **Lowering `max_completed_length` or `max_failed_length` trims existing history.** The next completed or failed move calls `LTRIM`, so changing `None` to `N` or lowering `N` can immediately reduce historical entries to the new cap.
- **Do not switch sync and async gateway instances mid-process while claims are active.** Redis state is compatible across deploys, but each gateway instance keeps its own pending claim-recovery IDs. In-flight claim recovery state does not transfer between instances.
- **Switching between `gateway=` and `client=` can retarget the DLQ.** The built-in `client=` path derives the DLQ from the queue name. If a custom gateway used a different `dead_letter_queue`, switching paths has the same orphaning impact as renaming the DLQ.

### v5 to v6 migration

v6.0.0 is a non-breaking-defaults release that adds new public APIs. v5 code continues to work; v6 adds opt-in features.

**New APIs (opt in as needed):**

- `max_pending_length=N` caps pending-list depth; with `pending_overload_policy="raise"` (default) producers see `QueueBackpressureError` when the cap is hit; `"block"` waits up to `pending_overload_block_timeout_seconds`; `"drop_oldest"` evicts silently, so use it only when data loss is acceptable.
- `queue.drain(timeout=...)` (sync) and `await queue.aclose(timeout=...)` (async) are explicit graceful-shutdown hooks. They refuse new claims and recover pending claim IDs but do not cancel in-flight handlers; join or await your worker separately.
- `on_event=callback` receives a `QueueEvent` dataclass for publish/claim/ack/reclaim/dedup/cleanup/drain lifecycle events. Use it for metrics, tracing, and structured logging. See [`examples/production/observability.py`](examples/production/observability.py) for the adapter pattern.
- See [`examples/production/backpressure.py`](examples/production/backpressure.py) and [`examples/production/graceful_shutdown.py`](examples/production/graceful_shutdown.py) for sync production patterns, with async siblings under [`examples/production/asyncio/`](examples/production/asyncio/).

> When using a pre-fork app server (gunicorn `--preload`, uvicorn workers that import the app at master startup), call `make_queue()` from your worker startup hook - NOT at module import. See [Fork safety](#fork-safety-and-pre-fork-servers) for why.

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
uv run python -m examples.send_messages
uv run python -m examples.send_messages

# Three consumers
uv run python -m examples.receive_messages
uv run python -m examples.receive_messages
uv run python -m examples.receive_messages
```

![GitHub Repo stars](https://img.shields.io/github/stars/elijas/redis-message-queue?style=flat&color=fcfcfc&labelColor=white&logo=github&logoColor=black&label=stars)
