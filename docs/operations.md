# Operations and production notes

> Part of the [redis-message-queue](../README.md) documentation.

Running redis-message-queue in production: fork safety and pre-fork servers,
Redis memory sizing for deduplication and replay metadata, and a catalog of
known limitations. See the [README](../README.md) for the quickstart and
[production-readiness.md](production-readiness.md) for the full residual-risk
register.

## Fork safety and pre-fork servers

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

### Forking after constructing GracefulInterruptHandler

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

## Redis memory sizing for deduplication and replay metadata

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

The Redis client's `decode_responses` setting determines the type your handler
receives from `process_message()`: with `decode_responses=True` the yielded
payload is `str`; omit it (the default) and the handler receives `bytes`. Match
the client setting to what your handler expects so payload deserialization
(for example `json.loads`) is not handed the wrong type.

## Making consumers idempotent

The default configuration (`visibility_timeout_seconds=300`) is **at-least-once**:
the same payload can be delivered to a consumer more than once. A consumer that
crashes after its side effect but before it acks, a slow-but-healthy handler
whose lease expires and gets reclaimed, and a Redis clock jump can all produce a
duplicate delivery. Publish-side deduplication does not prevent this — it only
suppresses duplicate *enqueues*, not duplicate *delivery* of a single enqueued
message. The forward/backward clock-jump and reclaim scenarios are catalogued in
[Redis clock dependencies](production-readiness.md#redis-clock-dependencies).

Because duplicates are expected, make the side effect idempotent: running it
twice must be indistinguishable from running it once. `process_message()` yields
the raw payload (`str`/`bytes`), not a message-id object, so the deduplication
key for the consumer has to come from the payload. Publish messages that carry a
stable business id, then guard the side effect with an atomic Redis
`SET <key> NX EX <ttl>` on that id:

```python
import json

def handle(client, message):
    payload = json.loads(message)
    key = f"idempotency:jobs:{payload['id']}"
    # SET NX EX is atomic: exactly one delivery wins the key.
    if not client.set(key, "1", nx=True, ex=24 * 60 * 60):
        return  # duplicate delivery — side effect already ran; skip and ack
    try:
        do_side_effect(payload)
    except Exception:
        client.delete(key)  # side effect did not happen; allow a redelivery to retry
        raise
```

The first delivery of a given id wins the key and runs the side effect; a
duplicate finds the key already set and skips. On failure the example deletes the
marker so a redelivery can retry from scratch (**release** semantics); to instead
suppress all retries after one attempt, **keep** the marker and delete nothing.

Choosing the TTL is a trade-off. The marker must outlive the widest window in
which a duplicate can still arrive — visibility-timeout reclaim plus
`max_delivery_count` retries, plus slack for clock jumps. Too short and a late
duplicate re-runs the side effect; too long and the markers accumulate and cost
more Redis memory (they share the DB namespace, so size them alongside dedup and
replay metadata — see [Redis memory sizing](#redis-memory-sizing-for-deduplication-and-replay-metadata)).

For side effects that write to a database, prefer a database-native guarantee
over a separate Redis marker: a **unique constraint** on the business id (let the
duplicate insert fail and swallow the conflict), or a **transactional outbox**
that commits the side effect and the processed-id record in one transaction.
These keep the idempotency check and the side effect in the same atomic unit, so
there is no window where one commits without the other.

Runnable end-to-end versions of the `SET NX EX` pattern are in
[`examples/production/idempotent_consumer.py`](../examples/production/idempotent_consumer.py)
and its [async sibling](../examples/production/asyncio/idempotent_consumer.py).

## Inspecting and managing queues

`RedisMessageQueue` exposes four operator helpers so you can inspect and manage
queue contents through the same envelope-aware key layout the library uses,
instead of reaching for raw `LRANGE` / `LLEN` against internal keys. All four
exist on both the sync and async queues (await the async ones).

### `stats()` — queue depths

Returns a `QueueStats` snapshot of the Redis list depths:

```python
from redis_message_queue import RedisMessageQueue, QueueStats

queue = RedisMessageQueue("jobs", client=client)
s = queue.stats()
print(s.pending, s.processing, s.completed, s.failed, s.dead_letter)
```

`pending` and `processing` are always integers. `completed`, `failed`, and
`dead_letter` are `None` when that feature is disabled for the queue (no
completed queue, no failed queue, or no dead-letter routing) and an integer
depth otherwise. Each depth is a separate `LLEN`, so the result is a best-effort
snapshot, not a single point-in-time-consistent view across every list.

### `peek(count=1, *, source="pending")` — look without consuming

Returns up to `count` messages from the head of a list without removing them.
`source` is one of `"pending"`, `"processing"`, `"completed"`, `"failed"`, or
`"dead_letter"`. Payloads are decoded back to what you published: pending and
processing envelopes are unwrapped, and completed/failed/dead-letter entries
(stored as raw payloads) are returned as-is.

```python
next_up = queue.peek(10)                       # 10 pending payloads
poisoned = queue.peek(5, source="dead_letter")  # inspect poison messages
```

### `redrive_dead_letters(max_messages=None)` — retry poison messages

Moves messages from the dead-letter queue back to pending and returns how many
moved. `max_messages=None` redrives everything currently dead-lettered; a
positive integer caps the batch. Each message is re-wrapped in a fresh envelope,
which **resets its delivery count**, so a redriven message is redelivered up to
`max_delivery_count` times again rather than being dead-lettered immediately on
its next claim. Oldest dead-letter entries are moved first. Requires a configured
dead-letter queue.

```python
moved = queue.redrive_dead_letters()          # retry all poison messages
moved = queue.redrive_dead_letters(max_messages=100)
```

### `purge(*, target)` — delete a list

Destructive and irreversible: deletes every message in `target` and returns how
many were removed. `target` must be named explicitly (there is no default and no
"all") and is one of `"pending"`, `"completed"`, `"failed"`, or `"dead_letter"`.
Purging `"processing"` is rejected because it holds in-flight message leases that
purging would corrupt. Only the target list is removed; deduplication markers and
lease metadata are left untouched.

```python
removed = queue.purge(target="dead_letter")   # drop poison messages
removed = queue.purge(target="failed")        # clear the failed log
```

These helpers require the built-in gateway (the `client=` constructor) or a
custom gateway that implements the same operator methods.

## Known limitations

- **Timed waits use polling claim loops.** To make claims recoverable after ambiguous connection drops, `wait_for_message_and_move()` uses idempotent Lua claim polling instead of raw blocking list-move commands. This adds a small polling cadence during timed waits.
- **Redis Lua is atomic, not rollback-transactional.** The built-in scripts now preflight queue key types and fail closed on `WRONGTYPE` before mutating queue state, but Redis does not undo earlier writes if a later script command fails for another reason (for example `OOM` under severe memory pressure). The visibility-timeout expiry-reclaim path is ordered durable-before-destructive: it `RPUSH`es the message back to pending *before* removing it from `processing` and deleting its lease metadata, so a failed reclaim `RPUSH` under memory pressure leaves the in-flight message and its lease intact in `processing` for a later reclaim attempt rather than losing it. Provision Redis `maxmemory` headroom and prefer the `noeviction` policy so write-side scripts fail closed instead of having keys evicted mid-operation.
- **Message durability is bounded by your Redis persistence and failover setup.** redis-message-queue provides *atomic* Redis state transitions, not disk-durable or replica-acknowledged broker durability: it issues ordinary Redis writes and never calls `WAIT` or waits for an fsync or replica acknowledgement. A `publish()` that returns success can still be lost if Redis crashes before the write reaches an AOF/RDB snapshot, or if a replica that had not yet received the write is promoted during failover; eviction under `maxmemory` can likewise drop queue, dedup, or claim keys. Treat message durability as exactly as strong as your Redis durability configuration: enable AOF (with an `appendfsync` policy matching your loss tolerance), prefer `noeviction` for queue databases, understand your replication/failover loss window, and keep consumers idempotent so a replayed or duplicate delivery is safe.
- **Batch reclaim limit of 100.** The visibility-timeout reclaim Lua script processes at most 100 expired messages per consumer poll. Under extreme backlog this may delay recovery, but prevents any single poll from blocking Redis.
- **Claim-attempt loop limit of 100 per poll.** The VT claim Lua script attempts at most 100 LMOVE+delivery-count checks per invocation. Under pathological conditions (>100 consecutive poison messages in pending), a single poll returns no message even though non-poison messages exist deeper in the queue. Subsequent polls drain the poison batch 100 at a time.
- **Cluster detection uses `isinstance(client, RedisCluster)`.** Wrapped or instrumented cluster clients that delegate without inheriting will bypass hash-tag validation. Custom gateways should set `is_redis_cluster = True` explicitly.
- **Redis Cluster requires hash tags.** The built-in queue uses multiple Redis keys per operation. Wrap the queue name in hash tags (for example `{myqueue}`) so every generated key lands in the same slot. When you pass a Redis Cluster client to the built-in queue/gateway path, incompatible names are rejected early.
- **Non-ASCII payloads use ~2x storage.** The default `ensure_ascii=True` in JSON serialization encodes non-ASCII characters as `\uXXXX` escape sequences. This is a deliberate compatibility choice.
- **Client-side `Retry` can duplicate non-deduplicated publishes.** If you construct your `redis.Redis` or `redis.asyncio.Redis` client with `retry=Retry(...)`, redis-py retries `ConnectionError` / `TimeoutError` at the connection layer — *below* this library. Idempotent operations (deduplicated `publish()`, lease-scoped cleanup) are safe because their Lua scripts replay the original result. `add_message()` (used by `publish()` when `deduplication=False`) is a bare `LPUSH` by default, or a single non-idempotent Lua enqueue when `max_pending_length` is set: this library deliberately does not retry it, but a client-level `Retry` will, and if the server executed the command before the response was lost the message is enqueued twice. redis-py 6.0+ changed the default standalone `Redis()` / `redis.asyncio.Redis()` retry policy from `None` (no retry) to a multi-attempt `ExponentialWithJitterBackoff`; the default attempt count varies by redis-py version, for example about 3 on redis-py 6-7 and about 10 on redis-py 8+. Pass `retry=None` explicitly if you need strict at-most-once semantics for non-deduplicated publishes, or accept the duplication risk. More broadly, any non-idempotent enqueue path is vulnerable if the connection drops after server execution but before the client receives the response; all other built-in operations (deduplicated publish, lease-scoped ack/move, lease renewal) use replay markers and are safe under client-level `Retry`.

  ```python
  import redis
  from redis_message_queue import RedisMessageQueue

  # Strict at-most-once for non-dedup messages: disable redis-py's
  # default client retry policy explicitly.
  client = redis.Redis(retry=None)
  queue = RedisMessageQueue("jobs", client=client)
  ```

  ```python
  import redis.asyncio as redis
  from redis_message_queue.asyncio import RedisMessageQueue

  # Strict at-most-once for non-dedup messages: disable redis-py's
  # default client retry policy explicitly.
  client = redis.Redis(retry=None)
  queue = RedisMessageQueue("jobs", client=client)
  ```
- **Redis Cluster default retry can stack with this library's retry budget.** In redis-py 6.0+, `RedisCluster()` constructs a default multi-attempt `ExponentialWithJitterBackoff` retry below this library's `retry_budget_seconds`. If you need a single retry surface, pass `retry=Retry(NoBackoff(), 0)` to the cluster client or reduce `retry_budget_seconds` to account for the lower-level retry window.

For a full analysis, see [production-readiness.md](production-readiness.md).
