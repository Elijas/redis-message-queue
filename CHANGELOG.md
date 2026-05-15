# Changelog

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
