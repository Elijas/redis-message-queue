# Changelog

## v7.0.1

### Bug Fix

- **R8-AD-04:** Custom `get_deduplication_key` callables now fail at
  publish time when they return `None` or `""`, preventing empty dedup keys
  from creating a bare-prefix Redis marker that silently suppresses unrelated
  messages. Non-`str` callable returns continue to raise `TypeError`, now with
  the explicit `get_deduplication_key must return a str, got <type>` message.

## v7.0.0

R7 (Round 7) audit follow-up — major release fixing footguns and tightening
contracts identified by auditing the v6.0.1 surface under realistic
operational pressure (long-running, multi-process, pinned-redis-py-version,
full-instrumentation).

### Breaking Changes

- **R7-AC-02 (M5):** `EventOperation` and `EventOutcome` are now `StrEnum`
  classes instead of `typing.Literal` aliases. Runtime-compatible because
  `StrEnum` is a `str` subclass; type-strict callers using the previous
  `Literal[...]` annotations should switch to the enum types.
- **R7-AC-03 (M7):** `drain()` / `close()` (sync) and `drain()` / `aclose()`
  (async) now put the queue instance into a queue-local drained state
  that rejects all subsequent `publish()` calls with
  `QueueDrainedError("queue is drained")`. This makes explicit drain a
  coherent shutdown boundary: once drain starts, no new messages can
  enter that queue instance's publish path, and drain waits for any
  publish already inside that path before returning. The drained state
  is local to the Python queue object and is not persisted to Redis; a
  fresh `RedisMessageQueue(...)` over the same Redis keys can still
  publish.
- **R7-AC-09 (H-R7-1 + H5-rewire):** Three previously-accepted-but-unsafe
  configurations now raise `ConfigurationError` at construction:
  `pending_overload_policy="drop_oldest"` with `max_pending_length=None`
  (silent no-op); `pending_overload_policy="drop_oldest"` with
  `max_delivery_count` set (silently discarded pending DLQ candidates,
  caught by AC-11/F1); and the existing `drop_oldest`+deduplication
  rejection is now enforced through the centralized
  `validate_pending_backpressure_parameters` validator (queue ctors and
  direct `RedisGateway` ctors both rely on the same single source of
  truth).
- **R7-AC-16 (H-R7-2):** Capped `redis<8.0.0` in `pyproject.toml` until
  RESP3-default compatibility is verified. Users on redis-py 7.x and
  earlier are unaffected.

### New API

- **R7-AC-02 (AC-14/F2):** Added `QueueEvent.error: BaseException | None`
  so adapters can call `span.record_exception(ex)` from the event
  payload without relying on ambient exception state. The label-friendly
  `exception_type: str | None` field remains for metrics.
- **R7-AC-03 (M7):** Added `QueueDrainedError`, a subclass of
  `RedisMessageQueueError`, exported from both `redis_message_queue`
  and `redis_message_queue.asyncio`.
- **R7-AC-18 (M-R7-2):** Added `GracefulInterruptHandler.reset()` to
  release signal-handler ownership and restore saved handlers. Idempotent.
  Lets a forked child reset the parent-installed handler before
  constructing its own. README documents the pre-fork constraint.

### Bug Fix

- **R7-AC-01 (M1):** Heartbeat tenacity inter-attempt sleep is now
  interruptible by the stop event. Previously, a longer retry delay
  could let the daemon heartbeat thread outlive `stop()`'s short join
  timeout; with the interrupt-aware sleep hook the thread observes the
  stop signal in bounded ~50ms intervals.
- **R7-AC-04 (M8):** Concurrent async `aclose()` callers no longer race
  through the gateway drain path. An `asyncio.Lock` serializes the
  drain body; later concurrent callers wait for the first drain and
  return its cached boolean result.
- **R7-AC-05 (M9):** `pending_overload_policy="block"` now uses
  exponential backoff with jitter (start 10ms, double on each overload
  sentinel, cap `min(500ms, pending_overload_block_timeout_seconds/10)`,
  jitter `base * (0.8 + 0.4 * random.random())`) instead of a fixed
  10ms poll. Each sleep remains bounded by the remaining timeout budget.

### Documentation

- **R7-AC-06 (AB-08/F2 + AC-12/F1 + AC-10/F3):** Added production-readiness
  catalog rows R14-R20 — B2 pending-backpressure caveats, B5 explicit
  drain behavior, B10 callback/exception-hierarchy notes, redis-py
  `max_connections=<finite>` pool-cap recommendation, and the fork-after-
  construct residual-risk row.
- **R7-AC-08 (AB-08/F5):** Added prescriptive "If you need..." subsections
  to README ordering (strict queue-wide order, per-key order, fairness,
  cross-batch reclaim) and operator guidance to production-readiness R11
  (Redis clock stability, skew alerting, visibility-timeout tuning).
- **R7-AC-16 (H-R7-2):** Gateway docstring and README now state that
  redis-py 6.0+ changed default standalone retry from `None` to a
  3-attempt `ExponentialWithJitterBackoff`. Pass `retry=None` to
  redis-py when strict at-most-once is required for non-deduplicated
  publishes.
- **R7-AC-19 (M-R7-5 + M-R7-7 + M-R7-8 + L-R7-2):** Added README
  observability subsections — *Event dispatch context* (sync-heartbeat
  contextvars/OTel/structlog boundary), *Event timing vs. Redis commit*
  (`failed/failure` pre-cleanup; other events post-commit), and
  *Intentionally silent paths* (Cluster `pcall` cleanup, VT claim-store
  OOM compensation, drain/close/aclose lifecycle, non-claim-loop retry
  attempts). Production-readiness R21 summarizes these boundaries.

### Examples

- **R7-AC-07 (AB-08/F4):** Added `examples/production/backpressure.py`,
  `examples/production/graceful_shutdown.py`, and async siblings under
  `examples/production/asyncio/`. Backpressure examples use
  `max_pending_length` + `pending_overload_policy="raise"` and catch
  `QueueBackpressureError`; graceful-shutdown examples install signal
  handlers and call `drain()` / `aclose()` with a timeout.
- **R7-AC-17 (M-R7-1):** Observability examples now construct queue +
  Redis client inside a factory function instead of at module import
  time. The previous shape was a fork hazard under pre-fork servers
  (uWSGI, gunicorn, Celery prefork).

## v6.0.1

R6 (Round 6) audit follow-up — patch release fixing contract gaps and
documentation drift found by auditing the v6.0.0 surface.

### Bug Fix

- **R6-H1 + R6-H2 (AB-03):** Wire `RetryBudgetExhaustedError` and
  `CleanupFailedError` raise paths. Both classes were exported in
  v6.0.0 but never raised; `except RedisMessageQueueError` missed
  retry-budget exhaustion (which raised raw redis-py exceptions) and
  cleanup-after-success failures (which bare-re-raised the cleanup
  exception). v6.0.1 wraps retry exhaustion in
  `RetryBudgetExhaustedError(...) from <orig>` across the four
  affected gateway paths (sync + async, blocking + non-blocking), and
  wraps cleanup-after-successful-processing in
  `CleanupFailedError(...) from <orig>`. Cleanup-after-handler-error
  preserves the handler exception as before. The MRO layering keeps
  existing `except redis.RedisError` catches working.
- **R6-H4 (AB-05/F2):** `aclose()` preserves cleanup on task
  cancellation. The async drain now routes through
  `_await_preserving_cancellation(...)` (a helper already in the
  module) so `_pending_claim_ids` is fully drained before
  `CancelledError` propagates.
- **R6-H5 (AB-06/F1):** Reject `pending_overload_policy="drop_oldest"`
  combined with deduplication at construction. The previous behavior
  evicted the oldest pending message but did NOT delete its dedup
  key, silently suppressing future publishes of the same payload.
  Users needing overload protection on a deduplicated queue should
  pick `"raise"` or `"block"`.
- **R6-H6 (AB-07/F1):** `on_event` callback failures no longer escape
  under `warnings.filterwarnings("error", RuntimeWarning)` or
  `PYTHONWARNINGS=error`. `_emit_event` now wraps the
  `warnings.warn(...)` call in a `warnings.catch_warnings()` block
  with a local `always` filter so callback exceptions do not crash
  queue operations under hardened warning policies.

### New API

- **R6-M2 (AB-Y05 / AB-01 / AB-05):** Add sync `close(timeout=None)`
  as a documented alias of `drain(timeout=None)` on `RedisMessageQueue`.
  Async remains `aclose()`. Restores parity with redis-py shutdown
  naming (`close()` sync / `aclose()` async).

### Tests / CI

- **R6-H3 (AB-04/F1):** Add Cluster regression tests covering all three
  lease-aware Lua scripts under cross-slot `claim_result_refs` keys
  (VT reclaim, lease ack, lease move-to-completed). Confirms the B1
  `pcall` wrap correctly swallows Cluster rejection so the surrounding
  mutations complete. Skipped when `REDIS_CLUSTER_URL` is unset;
  runs in the `real-redis-cluster` CI job.

### Documentation

- **R6 doc-drift (AB-Y05 / AB-08/F1+F3+F6 / AB-09/F1+F3+F5):**
  - `docs/production-readiness.md` R6 row rewritten from stale
    "no observability hooks" to mitigated-by-`on_event` with caveats.
  - `CHANGELOG.md` v6.0.0 B2 entry corrected: `drop_oldest` uses
    `RPOP` (not `LPOP`).
  - `README.md` "Upgrading" section now has a v5 → v6 migration
    subsection covering new APIs, new constructor rejections, the
    custom-gateway `renew_message_lease(..., *, is_interrupted=None)`
    signature change, and exception-hierarchy guidance.

## v6.0.0

### Bug Fix

- **B1:** Wrap data-derived `DEL` in `pcall` across the three lease-aware
  Lua scripts (`CLAIM_MESSAGE_WITH_VISIBILITY_TIMEOUT`,
  `REMOVE_MESSAGE_WITH_LEASE_TOKEN`, `MOVE_MESSAGE_WITH_LEASE_TOKEN`).
  Under Redis Cluster, the prior code could strand a message in
  `processing` if the data-derived key was rejected by Cluster's
  key-routing contract; the `pcall` lets the surrounding mutations
  complete and relies on the per-claim TTL to self-heal orphaned
  claim-result entries.
- **B3:** Make heartbeat-renewal retry interruptible by the stop signal.
  Previously, a tenacity retry could outlast a `drain()` / `aclose()`
  call and continue renewing a lease the queue intended to release.
- **B9:** Fail fast in the gateway constructor when passed a
  `redis.sentinel.Sentinel` (sync or async) directly, instead of
  silently misusing it as a client. Pass `sentinel.master_for(name)`
  instead.

### New API

- **B2:** `max_pending_length` + `pending_overload_policy` constructor
  parameters add a publish-side backpressure surface. Policies:
  `raise` (default — raises `QueueBackpressureError`),
  `block` (waits up to `pending_overload_block_timeout_seconds`,
  default 1.0s, then raises if still over the limit), or
  `drop_oldest` (Lua-side RPOP of the oldest pending message —
  accept silent eviction trade-off; use with care).
- **B5:** `drain()` (sync) / `aclose()` (async) graceful-shutdown API
  on the queue surfaces. Refuses new claims, drains in-flight
  `_pending_claim_ids` via the recovery cycle, and bounds heartbeat
  stop with documented best-effort semantics. Returns `True` if
  drain completed within the optional `timeout` window, `False`
  otherwise.
- **B10:** `on_event` constructor callback receives a stable
  `QueueEvent` dataclass for every publish/consume/cleanup state
  transition. The exception hierarchy now roots at
  `RedisMessageQueueError`, with subclasses `ConfigurationError`,
  `GatewayContractError`, `LuaScriptError`, `CleanupFailedError`,
  `RetryBudgetExhaustedError`, and `QueueBackpressureError`. Where
  redis-py exceptions wrap a queue surface, the new subclasses
  inherit from BOTH `RedisMessageQueueError` AND the matching redis-py
  base, preserving downstream `except redis.RedisError` catches.

### Breaking Changes

- **B10 exception hierarchy:** new base class `RedisMessageQueueError`
  inserts into the MRO of the existing exception classes. Code that
  catches specific subclasses continues to work; code that relied on
  the absence of a common base may now catch broader exception
  classes than before. Audit `except` chains that combined
  `redis.RedisError` with a per-class handler.
- **B9 Sentinel guard:** code that passed a `redis.sentinel.Sentinel`
  object directly to the queue (instead of `Sentinel.master_for(...)`)
  now raises at construction. Previously, this silently misbehaved.

### CI Hardening

- **B11:** Add four real-Redis CI jobs covering OOM, Redis Cluster,
  `WRONGTYPE`, and `decode_responses=True` paths. The Cluster job
  validates B1.

### Documentation

- **B4 / B6:** Add README sections on Fork safety and Redis memory
  sizing for deduplication and replay metadata.
- **B7:** Clarify that the retry budget uses monotonic elapsed time
  (not wall-clock); expand `docs/production-readiness.md` R11 with
  three Redis-clock-jump scenarios.
- **B8:** Add Ordering & multi-consumer fairness contract section
  documenting cross-batch reordering under the 100-message reclaim cap.

### Notes

- Style follow-up: line-length fixes after the B5 merge dropped CI lint
  red (`8ed033f`).

## v5.0.0

### Bug Fix

- Guard deduplicated publish Lua against OOM partial commits by using `pcall`
  around `LPUSH`.
- Tighten channel-rule warnings and follow-up filter behavior so user-actionable
  failures are emitted predictably.
- De-flake lease, deduplication, and interleaved-expiry tests.

### Breaking Changes

- **M1:** `visibility_timeout_seconds` now defaults to `300` instead of `None`.
  Pass `visibility_timeout_seconds=None` to keep the old at-most-once behavior.
- **M2:** `max_delivery_count` now defaults to `10` when visibility timeouts are
  enabled, with an auto-derived dead-letter queue.
- **M3:** `max_completed_length` and `max_failed_length` now default to `1000`
  instead of unbounded retention.
- **M4:** `get_deduplication_key` now defaults to a SHA-256 hash of the canonical
  message string instead of storing the literal message in the dedup key.

### New Defaults

- Add production examples showing the hardened v5 queue shape.
- Re-export `GracefulInterruptHandler` from the async package.
- Add `py.typed` for PEP 561 inline typing support.

### Error Message Tightening

- Improve gateway mismatch, validation, and configuration error messages.
- Lift duck-typed gateway attributes to `@property` defaults on the abstract
  gateway base classes.

### API and Typing

- Add a Redis version-skew CI matrix and use the canonical `redis.crc.key_slot`
  import.
- Harmonize public README imports and publisher examples around supported public
  paths and `decode_responses=True`.

### Doc Sweep

- Add production examples and README pointers from minimal examples.
- Document production readiness risks, upgrade hazards, and remaining operator
  caveats for the v5 defaults.
