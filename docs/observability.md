# Observability

> Part of the [redis-message-queue](../README.md) documentation.

How to observe redis-message-queue in production: the `on_event` callback, the
full event catalog, dispatch context, event timing versus Redis commit,
intentionally silent paths, secret-safety for `event.error`, and the public
exception hierarchy. See the [README](../README.md) for the quickstart.

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
completed/failed cleanup, DLQ moves, heartbeat renewal and its failures, stale
leases, drain, cleanup and trim failures, and retry attempts. Callback
exceptions are logged and reported with `RuntimeWarning`, but never propagate
into queue operations.
`on_event` is telemetry only: use it for metrics, tracing, and logging, not for
sagas, follow-up writes, billing callbacks, or other correctness-critical
work. Package logs remain diagnostic; use `on_event` rather than log parsing
for metrics.

```python
from redis_message_queue import QueueEvent, RedisMessageQueue

try:
    from opentelemetry import trace
except ImportError:
    trace = None

try:
    from prometheus_client import Counter
except ImportError:
    Counter = None

events_total = (
    Counter(
        "rmq_events_total",
        "redis-message-queue lifecycle events",
        ["queue", "operation", "outcome", "exception_type"],
    )
    if Counter is not None
    else None
)
SPAN_SINK_TRUSTED = False

def observe(event: QueueEvent) -> None:
    if events_total is not None:
        events_total.labels(
            event.queue, event.operation, event.outcome, event.exception_type or ""
        ).inc()
    if event.error is not None and SPAN_SINK_TRUSTED and trace is not None:
        trace.get_current_span().record_exception(event.error)

queue = RedisMessageQueue("jobs", client=client, on_event=observe)
```

## ⚠ Secrets in `event.error`

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

## Event dispatch context

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

> **Warning:** Because callbacks fire inline and may run while an internal
> publish/drain lock is held, an `on_event` callback must not call back into the
> same queue instance's `publish()`, `drain()`, `close()` (sync), or `aclose()`
> (async). Those locks are non-reentrant, so re-entering deadlocks and wedges
> the caller permanently. Re-entering a *different* queue instance, or scheduling
> the follow-up work outside the callback, is safe.

## Event timing vs. Redis commit

Most events are post-commit, emitted after the Redis command or Lua script
returned: `publish/success`, `publish_dedup_hit`, `claim/success`,
`claim_empty`, `claim_reclaim`, `ack`, `nack`, `completed`, `dlq`,
`lease_renew`, `trim_failed`, and `stale_lease_*`.

Pre-commit and mid-flight exceptions:

- `failed/failure` fires after the handler raises but before failed-queue
  cleanup completes. Use `nack` for cleanup-commit metrics; use `failed` for
  handler-exception attribution.
- `lease_renew_failed/failure` fires from the heartbeat after a renewal attempt
  raised (the renewal command may or may not have committed);
  `lease_renew_failed/skipped` fires when the gateway reports the lease is no
  longer renewable. Both terminate the heartbeat, and the message is reclaimed
  once the visibility timeout expires.
- `heartbeat_stop_timeout/failure` fires when the heartbeat thread (sync) or
  task (async) does not stop within its join timeout, so it may briefly renew a
  stale lease before exiting.
- `retry_attempt/failure` and `retry_exhausted` fire on the claim-loop retry
  path. The first Redis attempt may or may not have committed.
- `publish/failure`, `claim/failure`, and `cleanup_failed/failure` follow
  exceptions. Under an ambiguous lost response, Redis may have committed
  despite the exception. Treat them as "operation did not succeed from the
  caller's perspective", not "Redis did not commit".
- Visibility-timeout claim-store write failures raise
  `ClaimStoreFailedError` and emit `claim/failure`. When the compensating
  return-to-pending write succeeds, the payload is back in pending; if
  return-to-pending also fails, the payload remains in processing so there is
  still a live queue copy.

## Drain events

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

## Intentionally silent paths

The following operations have no `on_event` surface by design:

- **Cluster `pcall` cleanup failure:** three lease-aware Lua scripts wrap a
  data-derived `DEL` in `redis.pcall(...)` and ignore the result. This
  preserves queue safety on Cluster `CROSSSLOT` rejection but cannot be
  observed through `on_event`. Operators watching key-TTL behavior or Redis
  slow logs can detect orphans.
- **`drop_oldest` evictions:** when publish backpressure uses
  `pending_overload_policy="drop_oldest"`, the oldest pending message is
  discarded before the new message is enqueued. The successful enqueue emits
  `publish/success`, but there is no separate drop event for the discarded
  message in the current feature set.
- **Non-claim-loop retry attempts:** tenacity retries in deduplicated publish,
  ack/remove, move-to-completed/failed, and lease renewal collapse into the
  terminal operation's failure event. There is no per-attempt event for those
  paths.
- **Claim cache-replay after a lost reply:** a visibility-timeout claim can
  commit server-side — dead-lettering a poison message (`dlq`) or reclaiming an
  expired lease (`claim_reclaim`) before claiming the next live message — and
  then lose its reply (for example, a dropped connection). The claim loop
  retries the same claim ID and hits the `claim_result` cache-replay, which
  re-asserts the lease and returns the stored claim but does not re-run those
  side effects, so their `claim_reclaim` / `dlq` event payloads are not
  re-emitted. Queue state stays correct (the poison message stays
  dead-lettered, the live message is claimed exactly once); only telemetry for
  the lost-reply attempt is dropped. Reconcile poison-message alerting against
  `LLEN {name}::dlq` rather than the `on_event` stream alone.

## Public exception hierarchy

The public exception hierarchy is rooted at `RedisMessageQueueError`. The
current exported queue-owned exception classes are:

- `RedisMessageQueueError` (base)
  - `ClaimStoreFailedError` - visibility-timeout claim metadata could not be stored
  - `ConfigurationError` - invalid constructor args; also a `ValueError`
  - `DrainFailedError` - drain pending-claim recovery failed
  - `GatewayContractError` - custom gateway protocol violation; also a `TypeError`
  - `LuaScriptError` - Lua `redis.error_reply(...)`; also a redis-py `ResponseError`
  - `MalformedStoredMessageError` - stored value is not a valid RMQ envelope
  - `PayloadTooLargeError` - serialized payload exceeds `max_payload_bytes`; also a `ValueError`
  - `PayloadTooDeepError` - payload nesting exceeds `max_payload_depth`; also a `ValueError`
  - `QueueBackpressureError` - `pending_overload_policy="raise"` rejected enqueue
  - `QueueDrainedError` - `publish()` called after explicit drain/aclose
  - `CleanupFailedError` - cleanup after handler completion failed
  - `RetryBudgetExhaustedError` - Redis retry budget exhausted; also a redis-py `RedisError`
