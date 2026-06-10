# Configuration

> Part of the [redis-message-queue](../README.md) documentation.

Complete configuration reference for redis-message-queue: every constructor
option, grouped by feature. New here? Start with the [README](../README.md)
quickstart, then come back to tune behavior. Every feature is optional and can
be enabled independently.

## Deduplication

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

### Dedup key callable must return a non-empty, high-cardinality, tenant-scoped string

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

## Success and failure tracking

```python
queue = RedisMessageQueue(
    "q", client=client,
    enable_completed_queue=True,   # track successful messages
    enable_failed_queue=True,      # retain failed messages for inspection/manual repair
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

Completed queue entries are terminal audit/inspection records, not a result
backend. The generated completed list is available as `queue.key.completed` and
defaults to `{name}::completed` when `key_separator="::"`. Completed records
store raw payload bytes only, without result values, timestamps, delivery
counts, exception metadata, deduplication keys, or the internal delivery
envelope. Operators can inspect them conservatively, for example with
`LRANGE queue.key.completed 0 -1` or application-owned tooling. Archive,
export, or trim completed records according to the application's retention
policy; the library does not assign completed records automatic replay or retry
semantics.

Failed queue entries are retained for inspection and application-owned manual
reprocessing; they are not automatically retried. The generated failed list is
available as `queue.key.failed` and defaults to `{name}::failed` when
`key_separator="::"`. Terminal lists (`completed`, `failed`, and `dlq`) store
raw payload bytes only, not exception metadata, timestamps, delivery counts, or
the internal delivery envelope.

Operators should inspect first, for example with `LRANGE queue.key.failed 0 -1`
or application-owned tooling, then deliberately republish or move records to a
separate repair queue according to the application's idempotency and
deduplication policy. Replaying by `publish()` can be suppressed by publish-side
deduplication while the original dedup key is live unless the application
changes the replay key, waits for TTL expiry, disables deduplication for that
path, or otherwise owns the policy. Do not treat blind `LPUSH` or `RPUSH` of
failed-list records back to pending as a universal safe replay workflow.

## Publish backpressure

By default, the pending queue is unbounded (`max_pending_length=None`), matching
the v5 behavior. Set `max_pending_length` when producers can outrun consumers
and Redis memory must fail closed before the broker is exhausted:

```python
queue = RedisMessageQueue(
    "q",
    client=client,
    max_pending_length=100_000,
    pending_overload_policy="raise",  # "raise" | "block" | "drop_oldest"; see requirements below
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

Only the default `"raise"` operates on an unbounded queue. Both `"block"` and
`"drop_oldest"` require `max_pending_length` to be set (a threshold to block on
or to drop from) and raise `ConfigurationError` at construction otherwise.

`"drop_oldest"` carries two extra constraints and is therefore **not** a drop-in
swap on the default `client=` path: it requires `max_delivery_count=None`, and
it is incompatible with deduplication. The built-in `client=` path defaults
`max_delivery_count=10`, so `pending_overload_policy="drop_oldest"` raises
`ConfigurationError` unless you also pass `max_delivery_count=None` (which
disables dead-letter routing — dropping pending DLQ candidates silently is the
loss `drop_oldest` accepts). With deduplication enabled, `drop_oldest` is
rejected because dropped messages leave their dedup keys in Redis, so future
publishes of the same payload would be silently suppressed; use `"raise"` or
`"block"` for deduplicated queues.

These limits apply only to the pending list at publish time. They do not cap
messages already in `processing`, dead-letter queues, deduplication keys, or
replay metadata. `max_completed_length` and `max_failed_length` only bound the
completed/failed history lists. Size pending payload memory separately from the
dedup/replay metadata described in
[Redis memory sizing](operations.md#redis-memory-sizing-for-deduplication-and-replay-metadata).

When using `gateway=`, configure backpressure on the gateway directly, for
example `RedisGateway(redis_client=client, max_pending_length=100_000)`.

## Payload validation and limits

All three payload guards default to **no validation**. Enable them to fail a bad
publish loudly before it is enqueued instead of discovering the problem on the
consumer side:

```python
queue = RedisMessageQueue(
    "q",
    client=client,
    strict_payload_types=True,   # reject lossy/Python-only JSON types
    max_payload_bytes=65536,     # reject serialized payloads over 64 KiB
    max_payload_depth=20,        # reject dict/list nesting deeper than 20
)
```

- `strict_payload_types` (`bool`, default `False`) validates the values of a
  **dict** payload before publish and raises a path-aware `TypeError` for types
  that JSON cannot round-trip losslessly — tuples (silently become lists),
  `set`/`frozenset`, `bytes`, `datetime`, and any other custom object. Only
  JSON-native values (`None`, `bool`, `int`, `float`, `str`, and nested
  `list`/`dict`) are allowed. The error names the offending path, e.g.
  `value at message['items'][0] is a tuple`. The default `False` preserves the
  existing `json.dumps` behavior, where tuples become lists and unsupported
  values raise at serialization time instead.
- `max_payload_bytes` (default `None`, unbounded) raises `PayloadTooLargeError`
  when the serialized payload exceeds the limit. This applies to both dict
  payloads (measured on the UTF-8 JSON encoding) and `str` payloads (measured on
  the UTF-8 bytes), so it bounds pending-list memory per message.
- `max_payload_depth` (default `None`, unbounded) raises `PayloadTooDeepError`
  when a **dict** payload nests dicts/lists deeper than the limit, guarding
  against pathological or hostile structures before they reach Redis.

Each limit must be a positive integer when set; `0` or negative values raise
`ConfigurationError` at construction. `PayloadTooLargeError` and
`PayloadTooDeepError` both subclass `RedisMessageQueueError` (and `ValueError`);
they are listed in the
[public exception hierarchy](observability.md#public-exception-hierarchy), and
this section describes how to trigger them.

## Crash recovery with visibility timeout

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
- heartbeats renew the lease indefinitely for a hung (non-crashed) handler: there is no processing-time cap, so a handler that never returns keeps the message locked forever. rmq cannot detect this; alert on processing-queue dwell time externally (see [production readiness: hung handlers with heartbeat](production-readiness.md#residual-risks))

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

With `visibility_timeout_seconds=None, max_delivery_count=None`, messages
already moved to `processing` remain there indefinitely after a consumer crash
and are not redelivered, even if the crash happened before your handler
started running.

Visibility deadlines use Redis server time (`TIME`), not Python process time.
A forward step in the Redis server clock can make a live lease appear expired
and allow premature redelivery while the original consumer is still processing;
a backward step can delay reclaim of truly abandoned messages. Treat NTP step
corrections on Redis hosts as a deployment risk. Prefer time-synchronization
discipline that slews corrections rather than stepping the Redis clock.

## Ordering and multi-consumer fairness

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

Ordinary `Exception` subclasses raised by handler code are not retries: the
default behavior removes the message from `processing`, or moves it to the
failed queue when enabled. Redelivery is for crash, stall, stale-lease, or fatal
`BaseException` shutdown/cancellation paths where cleanup does not complete; see
[Abandoned in-flight messages](#abandoned-in-flight-messages).

Multiple consumers contend for the same queue. The next message goes to the
consumer whose claim request Redis executes next. There is no round-robin,
equal-share, or starvation-freedom guarantee; faster consumers can receive more
than 1/N of messages.

## If you need stronger ordering or fairness guarantees

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
  [production readiness: Redis clock dependencies](production-readiness.md#redis-clock-dependencies).

## Dead-letter queue

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
- dead-lettered messages contain the **raw payload** only. They are raw payload bytes only, without exception metadata, final delivery-count metadata, timestamps, deduplication keys, or the internal delivery envelope. The internal envelope, which carries a per-delivery UUID, is stripped before pushing to the DLQ, consistent with how completed/failed queues store messages. Two identical payloads dead-lettered separately are indistinguishable in the DLQ

Manual DLQ handling: DLQ entries are terminal retained records; they are not
automatically retried or moved back to pending by the library. For built-in
`client=` queues, inspect `{name}::dlq`; for custom gateway queues, inspect the
configured `dead_letter_queue`. Do not use `queue.key.dead_letter` to inspect
the built-in DLQ: that accessor currently formats `{name}::dead_letter`, which
is not the built-in default DLQ list.

Operators should inspect first, for example with `LLEN` / `LRANGE` on the
configured DLQ key or application-owned tooling, then intentionally republish,
move records to a separate repair queue, trim/archive, or discard according to
the application's idempotency and deduplication policy. Replaying by `publish()`
can be suppressed by publish-side deduplication while the original dedup key is
live unless the application changes the replay key, waits for TTL expiry,
disables deduplication for the repair path, or otherwise owns the policy. Do not
treat blind `LPUSH` or `RPUSH` of DLQ records back to pending as a universal
safe replay workflow.

## Graceful shutdown

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

Both built-in handlers subclass `BaseGracefulInterruptHandler`, the abstract
base class (exported from `redis_message_queue`) you subclass to wire in a
custom interrupt source; it has a single abstract method,
`is_interrupted() -> bool`, that the polling waits call between claims.

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

### Callback-style consuming

`process_message_callback(handler)` is the callback-shaped sibling to the
`with queue.process_message()` context manager. It claims at most one message,
invokes `handler(message)`, acks on a normal return, and returns a `bool`:
`True` when a message was claimed, handled, and acked; `False` for an empty
poll (no message available). Pair it with the interrupt loop from above:

```python
while not interrupt.is_interrupted():
    claimed = queue.process_message_callback(handler)  # True=claimed+acked, False=empty poll
```

On the sync queue, if `handler` returns an awaitable, `process_message_callback`
raises `TypeError` before acking and leaves the message in `processing` for
visibility-timeout reclaim, rather than silently dropping an un-awaited
coroutine. Use the async queue (`redis_message_queue.asyncio`) for `async def`
handlers: its `process_message_callback` accepts both plain and `async def`
handlers and awaits the result before acking.

### Abandoned in-flight messages

Abandoned in-flight messages are recovered lazily. Async tasks cancelled
without `aclose()`, or sync processes killed mid-handler, can leave the message
and its processing/lease metadata in Redis until a later consumer claim path
triggers visibility-timeout reclaim. With visibility timeouts enabled, this is
the designed at-least-once recovery path: the message is delayed by the lease,
not lost. With `visibility_timeout_seconds=None, max_delivery_count=None`,
there is no automatic reclaim path. For low-visibility-timeout workloads,
prefer an explicit `drain()` / `aclose()` during shutdown so local pending claim
IDs are recovered before process exit.

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

### Cancellation observability on the async failure path

When a handler inside `async with queue.process_message()` raises, the queue
runs failure-path cleanup (move-to-failed or remove-from-processing) before the
handler error propagates. To finish that cleanup atomically, the async queue
suppresses a single cancellation or `asyncio.timeout` deadline that lands in
this window. Message state stays correct — the cleanup completes and the
handler exception is what you see — but the cancellation signal itself can
become hard to observe. Plan for these three effects when you cancel or
time-box a failing consumer:

- **An expired `asyncio.timeout` may surface as the handler error, with no
  `TimeoutError`.** If the deadline fires while failure-path cleanup is running
  and the cleanup then succeeds, the suppressed cancellation is consumed and the
  block raises the original handler exception. The `asyncio.timeout` context
  manager finds no live cancellation to convert, so no `TimeoutError` is raised
  or spliced into the chain. Do not rely on catching `TimeoutError` to detect
  that the deadline expired here.

- **On Python 3.12, a deadline that expires mid-ack whose ack also fails is not
  discoverable from the exception chain.** When the timeout fires during
  post-success cleanup and that cleanup also fails, the queue raises
  `CleanupFailedError`. On Python 3.13+ a `TimeoutError` is chained into that
  exception's context (so you can detect the deadline); on Python 3.12 there is
  no such chaining and the deadline expiry leaves no trace in the raised
  exception. If you run on 3.12 and need to distinguish "ack failed" from "ack
  failed because my deadline expired", enforce the deadline outside the
  `process_message` block.

- **A task cancelled during failure-path cleanup completes with the handler
  exception, not as cancelled.** No number of `task.cancel()` calls landing in
  this window makes the task end `cancelled()` — the handler error always wins.
  Shutdown logic of the form `task.cancel(); await task; assert
  task.cancelled()` will misclassify the exit. Detect shutdown by other means
  (your own flag, `aclose()`, or by inspecting the raised exception) rather than
  gating on `task.cancelled()`.

## Custom gateway

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
    dead_letter_queue="myqueue::dlq",
)
queue = RedisMessageQueue("myqueue", gateway=gateway)
```

Custom DLQ records follow the same manual inspection, repair, archive, trim,
and replay contract described in [Dead-letter queue](#dead-letter-queue); they
are terminal retained raw payloads and are not automatically retried.

Use a separate gateway instance per queue when `max_delivery_count` is enabled.
Dead-letter routing is gateway-scoped, so reusing the same gateway across different
queues is rejected.

If you use Redis Sentinel, pass the Redis client returned by
`sentinel.master_for(name)` to `client=` or `RedisGateway(redis_client=...)`, not
the `sentinel` object itself.

## Connection pool sizing

Each queue with `heartbeat_interval_seconds` set uses up to 2 simultaneous
connections: one for the main operation and one for heartbeat renewal.

redis-py's default standalone `max_connections=None` resolves differently across
the versions this library supports (`redis>=5.0.1,<9.0.0`). Before redis-py 8.0 it
resolves to `2**31` connections — effectively unbounded: the pool grows on demand
and a concurrency spike retains every socket it created until you call
`client.close()`, never shrinking back down, so an accidental fan-out leaves a
large idle socket footprint for the lifetime of the client. redis-py 8.0+ resolves
it to `100` and raises `ConnectionError` once the pool is exhausted. Neither
default is what you want under real concurrency.

Pass an explicit finite `max_connections` to `redis.Redis(...)` sized for your
real concurrency: the number of consumer threads/tasks calling
`process_message` concurrently, plus one extra connection per message currently
holding a lease (active heartbeat renewal), plus headroom. A single serial
consumer with heartbeats needs `max_connections >= 2`; N concurrent consumers
with heartbeats need `max_connections >= 2 * N + headroom`. Sizing purely as
`2 * number_of_queues` undercounts when one queue object is polled by many
concurrent callers. See
[production readiness: connection pool sizing](production-readiness.md#residual-risks) for the
operator summary.
