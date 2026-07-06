# Troubleshooting

> Part of the [redis-message-queue](../README.md) documentation.

Symptom-keyed index for diagnosing redis-message-queue at runtime. Each entry
is a pointer into the deeper reference docs — this page does not duplicate
that prose, it tells you where to look and what to check first.

## Redis down / `RetryBudgetExhaustedError`

**Likely cause:** Redis is unreachable, overloaded, or the client's own retry
budget ran out before a command could complete.

**What to check:**

- Is Redis reachable at all from this process (`redis-cli -u <url> PING`)?
- Is `retry_budget_seconds` (default on the `client=` path) too tight for your
  network's actual latency/outage profile?
- Is a client-level `redis.Redis(retry=Retry(...))` stacking its own retry
  window underneath this library's budget, extending total wall-clock time
  before you see the failure?

**Details:** [docs/observability.md — Public exception hierarchy](observability.md#public-exception-hierarchy)
for what `RetryBudgetExhaustedError` means and its `redis.RedisError`
compatibility; [docs/configuration.md — Custom gateway](configuration.md#custom-gateway)
for tuning the retry budget; [docs/operations.md — known limitations](operations.md#known-limitations)
for how client-side `Retry` interacts with this library's retries.

## Publish rejected or blocking / `QueueBackpressureError`

**Likely cause:** `max_pending_length` is set and the pending list is full,
with `pending_overload_policy="raise"` (rejects immediately) or `"block"`
(waits, then raises on timeout).

**What to check:**

- Current pending depth: `queue.stats().pending` (or `LLEN <name>::pending`).
- Which `pending_overload_policy` is configured, and whether the consumer
  side is keeping up with the producer rate.
- If using `"block"`, whether `publish(..., timeout=...)` is set to what you
  expect — it's an absolute bound on the call, not a per-attempt one.

**Details:** [docs/configuration.md — Publish backpressure](configuration.md#publish-backpressure)
for the three overload policies and their tradeoffs; [docs/observability.md — Public exception hierarchy](observability.md#public-exception-hierarchy)
for the exception itself.

## `WRONGTYPE` errors on startup or mid-run

**Likely cause:** A queue-name collision — something else (another rmq queue
with an overlapping `key_separator`, or an unrelated Redis client) already
holds one of this queue's keys as the wrong Redis type. This library's Lua
scripts preflight key types and fail closed rather than silently corrupting
state, but the preflight check surfaces as a `WRONGTYPE`-flavored error.

**What to check:**

- `TYPE <name>::pending` / `TYPE <name>::processing` / etc. against what the
  library expects (all lists, except the dedup keys which are strings and the
  lease/delivery-count metadata which are hashes/sorted sets — see
  [docs/operations.md — Redis key layout](operations.md#redis-key-layout)).
  A collision usually means a shared `key_separator` with another task
  library, or two rmq queues with the same name pointed at the same Redis DB.
- Whether `key_separator` was chosen to avoid overlapping another library's
  namespace (Celery, RQ, Dramatiq, etc.) on the same Redis DB.

**Details:** [docs/operations.md — Known limitations](operations.md#known-limitations)
(Lua atomicity / `WRONGTYPE` fail-closed) and
[docs/operations.md — Redis key layout](operations.md#redis-key-layout) for
the full list of keys and their expected types.

## Messages stuck in `processing`

**Likely cause:** either this queue has no `visibility_timeout_seconds`
configured (so there is no automatic reclaim path and an abandoned claim is
permanent until you intervene), or a consumer crashed/hung mid-handler and the
message is waiting out its lease before reclaim.

**What to check:**

- Is `visibility_timeout_seconds` (or `message_visibility_timeout_seconds` on
  a custom gateway) set at all? Without it, delivery is at-most-once and a
  crashed consumer's claim never comes back automatically.
- `queue.stats().processing` / `queue.peek(source="processing")` to see what's
  stuck, without consuming it.
- Whether the lease deadline has actually passed yet — reclaim only happens on
  the *next consumer poll* after expiry, not on a background timer.

**Details:** [docs/configuration.md — Crash recovery with visibility timeout](configuration.md#crash-recovery-with-visibility-timeout)
and [docs/configuration.md — Abandoned in-flight messages](configuration.md#abandoned-in-flight-messages)
for reclaim mechanics and the at-most-once/at-least-once tradeoff;
[docs/operations.md — Inspecting and managing queues](operations.md#inspecting-and-managing-queues)
for `stats()`/`peek()`.

## Duplicate deliveries

**Likely cause:** this is expected at-least-once behavior under
`visibility_timeout_seconds` (a handler that ran past its lease, or a
consumer that crashed after claiming but before acking, gets its message
redelivered) — not a bug. Deduplication (`get_deduplication_key`) only
prevents duplicate *publishes* within a TTL window; it does not prevent
duplicate *delivery* of an already-published message.

**What to check:**

- Is the handler idempotent? At-least-once delivery requires it.
- Is `visibility_timeout_seconds` sized realistically against actual handler
  runtime (including GC pauses, cold starts, etc.)? Too-short a timeout causes
  reclaim-while-still-processing.
- Client-side `retry=Retry(...)` on the Redis client can also duplicate
  *non-deduplicated* `publish()` calls at the connection layer, independent of
  visibility timeouts.

**Details:** [docs/configuration.md — Crash recovery with visibility timeout](configuration.md#crash-recovery-with-visibility-timeout)
for the at-least-once contract; [docs/operations.md — known limitations](operations.md#known-limitations)
(client-side `Retry` duplicating non-deduplicated publishes) for the publish-side case.

## DLQ filling up

**Likely cause:** messages are exceeding `max_delivery_count` — usually a
poison message (handler always raises for this payload) or a
`visibility_timeout_seconds` set too short relative to real handler runtime,
causing legitimate in-flight work to be reclaimed and redelivered until it
dead-letters.

**What to check:**

- `queue.stats().dead_letter` for the current depth, `queue.peek(source="dead_letter")`
  to inspect entries without consuming them.
- Whether `visibility_timeout_seconds` is realistic for handler runtime — a
  too-short timeout dead-letters otherwise-healthy messages.
- After fixing the root cause, `queue.redrive_dead_letters()` moves messages
  back to pending (resetting their delivery count) instead of requiring a
  manual `LMOVE`/re-publish.

**Details:** [docs/configuration.md — Dead-letter queue](configuration.md#dead-letter-queue)
for `max_delivery_count` semantics; [docs/operations.md — Inspecting and managing queues](operations.md#inspecting-and-managing-queues)
for `stats()`/`peek()`/`redrive_dead_letters()`/`purge()`.

## `CROSSSLOT` / cluster key errors

**Likely cause:** a Redis Cluster client was passed a queue name that doesn't
put all of this queue's keys in the same hash slot, so multi-key Lua scripts
get rejected, or a wrapped/instrumented cluster client isn't being detected as
a cluster client at all.

**What to check:**

- Is the queue name wrapped in a hash tag, e.g. `{myqueue}`, so every key this
  library derives from it (`{myqueue}::pending`, `{myqueue}::processing`, ...)
  lands in the same slot?
- If using a wrapped/subclassed cluster client that doesn't inherit from
  `redis.cluster.RedisCluster`, did you set `is_redis_cluster = True`
  explicitly on your custom gateway? Cluster detection is
  `isinstance(client, RedisCluster)`, which wrapped clients can silently
  bypass.

**Details:** [docs/operations.md — Known limitations](operations.md#known-limitations)
(cluster detection and hash-tag requirements) and
[docs/observability.md — Intentionally silent paths](observability.md#intentionally-silent-paths)
for the cluster `pcall` cleanup behavior on `CROSSSLOT` rejection.
