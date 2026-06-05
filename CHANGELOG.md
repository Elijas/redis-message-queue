# Changelog

## v8.4.0

### Compatibility

- redis-py 8.x is now supported. The dependency cap is lifted to
  `redis>=5.0.1,<9.0.0` after verification against real Redis with RESP3.
- redis-py 8 changes the default standalone client retry policy to about 10
  attempts. This matters for at-most-once or non-idempotent publish paths:
  client-level retry can duplicate non-deduplicated publishes if Redis executed
  the command before the client lost the response. See
  [operations.md](docs/operations.md#known-limitations) for the `retry=None`
  guidance.

## v8.3.0

Minor release with visibility-timeout and gateway correctness fixes, fail-fast configuration
validation, and new property/contract test coverage. No public API was removed. Three new
validations reject previously-accepted-but-unsafe configurations — see Behavior Changes before
upgrading. v8.2.2–v8.2.10 were maintenance and hardening releases focused on documentation
updates.

### Behavior Changes

- `pending_overload_policy="block"` now requires `max_pending_length` to be set. Constructing a
  queue with `block` and no cap now raises `ConfigurationError` instead of silently behaving as an
  unbounded queue that never blocks.
- Passing non-default backpressure parameters (`pending_overload_policy`,
  `pending_overload_block_timeout_seconds`) alongside `gateway=` now raises `ConfigurationError`
  instead of silently ignoring them; configure publish backpressure on the gateway directly.
- `RedisGateway.publish_message` (the deduplicated publish path) now rejects a gateway configured
  with `pending_overload_policy="drop_oldest"` with `ConfigurationError`, instead of dropping a
  pending message and leaving its deduplication key behind to silently suppress later republishes.
  Non-deduplicated `add_message` is unaffected.

### Bug Fixes

- Visibility-timeout expiry reclaim now requeues a message to pending (`RPUSH`) before removing it
  from the processing list or deleting lease metadata (durable-before-destructive, mirroring
  `RETURN_MESSAGE_TO_PENDING`). If the requeue fails, the message stays in processing for a later
  reclaim attempt rather than being lost in the reclaim window.
- The async `aclose()` idempotency guard now compares with `is True`, so a non-bool truthy value
  cannot be misread as already-closed.
- The lease heartbeat is now started inside the `process_message` try/finally on sync and async
  queues, so an exception between claim and heartbeat start can no longer orphan a heartbeat
  thread/task.

### Documentation

- Constructor docstrings and the README now warn that an `on_event` callback must not re-enter the
  same queue instance's `publish()`/`drain()`/`close()`/`aclose()`: it runs while an internal
  operation lock is held, so same-instance re-entry deadlocks. The telemetry-only contract is
  reinforced.
- Constructor docstrings now warn that `on_heartbeat_failure` must not block the async event loop
  (it runs on the event loop for async queues; blocking starves other leases and stalls shutdown).
- Documented that under claim cache-replay, `dlq`/`claim_reclaim` events can be silently dropped,
  with a contract test pinning the behavior.

### Tests

- Added a bounded property/fuzz harness for the visibility-timeout `delivery_count`→DLQ state
  machine.

## v8.2.1

Maintenance release with no library-code changes. This release exercised the
normalized release pipeline end-to-end after the v8.2.0 ship.

### Documentation

- Normalized the uv-based packaging and release workflow, updated release
  instructions to publish from tagged releases, adjusted the CI Redis client
  floor, and refreshed the 8.2.1 version metadata in `pyproject.toml`,
  `README.md`, and `uv.lock`.

## v8.2.0

Minor release with security, Lua-correctness, and typing fixes.

### Bug Fixes

- Added opt-in `max_payload_bytes` and `max_payload_depth` publish guards for
  sync and async queues, with explicit `PayloadTooLargeError` and
  `PayloadTooDeepError` failures before Redis I/O.
- Visibility-timeout claim Lua now rolls back speculative `delivery_count`
  increments when claim-result storage fails, and sync and async gateways raise
  `ClaimStoreFailedError` instead of silently risking DLQ of a never-delivered
  message.
- Visibility-timeout claim Lua now reads lease-token counters back from Redis
  after `INCR`, preserving exact integer strings above Lua's `2^53` precision
  boundary.
- `NOSCRIPT` errors are now classified as retryable for future `EVALSHA` and
  proxy/script-cache paths.
- Mypy-dependent tests now skip cleanly when `mypy` is not available in the
  test environment.

### Documentation

- Sync and async `AbstractRedisGateway` now declare
  `message_visibility_timeout_seconds`, making the custom-gateway visibility
  timeout contract visible to strict typing.
- Async deduplication callback typing now accepts `str | Awaitable[str]`,
  matching runtime behavior.
- Added public `MessagePayload = str | dict[str, object]` and applied it to
  publish and deduplication callback signatures, so type checkers reject
  payload shapes that runtime validation already rejects.

## v8.1.0

Minor release with API and ergonomics improvements.

### Bug Fixes

- Added drain lifecycle events on sync and async drain/close paths:
  `drain/start`, `drain/success`, `drain/skipped`, and `drain/failure`.
  Events include additive timeout and pending-claim context where available.
- RMQ-owned exceptions now carry structured context attributes where available:
  `queue`, `message_id`, and `operation`.
- Added `process_message_callback()` for sync and async queues. Sync queues now
  detect awaitable handler returns and fail with an actionable `TypeError`
  while leaving the message visible for reclaim; async queues accept sync
  handlers and await async handlers.
- Added opt-in `strict_envelope_decoding=False` on sync and async queues so
  foreign Redis list payloads can fail fast with `MalformedStoredMessageError`
  instead of being treated as legacy raw messages.
- Added `EventDrivenInterruptHandler` for cooperative shutdown when another
  runtime owns process-global signal handling.
- Added opt-in `strict_payload_types=False` on sync and async queues with
  path-aware validation that rejects JSON-eroding payload values before
  publish.
- Added `DrainFailedError` so third-party drain failures are wrapped in an
  RMQ exception that preserves the original exception as `__cause__` while
  carrying structured drain context.

## v8.0.3

Patch release with cluster/runtime fixes and a documentation batch covering
operational guidance.

### Bug Fixes

- `ClusterError("TTL exhausted.")` during Redis Cluster slot migration is now
  treated as retryable, allowing the existing retry path to handle in-flight
  slot movement.
- Plain `redis.Redis` clients pointed at cluster nodes now fail fast instead
  of bypassing cluster-client validation. Sync construction probes cluster
  mode; async clients probe lazily.

### Documentation

- Added documentation updates for migration notes, visibility-timeout
  lease semantics versus handler runtime caps, handler-exception retry
  expectations, and the `on_event` telemetry contract.

## v8.0.2

Patch release with five observability and lifecycle bug fixes.

### Bug Fixes

- Publish preflight failures now emit `publish/failure` events on the
  `on_event` surface.
- Gateway contract failures now emit failure events, and `claim/success` is
  emitted only after lease validation succeeds.
- Malformed RMQ envelopes now raise `MalformedStoredMessageError` and emit
  `claim/failure` instead of silently yielding raw payloads.
- Reclaim and DLQ events now carry `message_id` and `delivery_count` so
  consumers can diagnose retries and terminal failures.
- `drain()` now cleans up `lease_token_counter` metadata for transient queues,
  preventing per-queue metadata leaks.

## v8.0.1

Patch release with three runtime hardening bug fixes.

### Bug Fixes

- `drain()` now honors its barrier for already-blocked consumers.
  Previously, a consumer that had already entered the gateway claim
  poll loop could still claim a post-drain message before stopping.
  Sync and async claim loops now observe the drain signal inside the
  blocking poll.
- `drain(timeout)` and async `aclose(timeout)` now hard-bound under
  slow Redis: the pending-claim recovery path checks the deadline
  before and around each Redis read, and leaves the pending claim id
  uncleared if the deadline is exceeded. Previously, a drain with
  `timeout=0.1` could run ~2.4s under 1.2s-per-call Redis latency.
- `publish()` now rejects non-string dict keys at any nesting depth.
  Previously, top-level non-string keys were rejected but nested
  ones were silently stringified by `json.dumps()`, delivering a
  mutated payload.

## v8.0.0

Major release removing implicit deduplication key generation.

### Breaking Changes

- `deduplication=True` now requires `get_deduplication_key`. Queue
  construction raises `ConfigurationError` when deduplication is enabled
  without a callable, with guidance to pass a stable logical ID function or set
  `deduplication=False`.
- The full-payload/key-generation fallback has been removed. Deduplication keys
  are always derived from the user-provided callable.
- Deduplication is now opt-in by constructor default. Queues constructed
  without `deduplication=True` use the non-deduplicated publish path.

## v7.0.1

Patch release closing the v7.0.0 empty/None dedup-key footgun, fixing two
v7.0.0 drain/aclose return-value regressions, hardening
observability-secrets guidance, and polishing first-90-minute adoption.

### Bug Fix

- Custom `get_deduplication_key` callables now fail at
  publish time when they return `None` or `""`, preventing empty dedup keys
  from creating a bare-prefix Redis marker that silently suppresses unrelated
  messages. Non-`str` callable returns continue to raise `TypeError`, now with
  the explicit `get_deduplication_key must return a str, got <type>` message.
  Direct `RedisGateway.publish_message(..., dedup_key=...)` callers get the
  same validation: empty or non-`str` `dedup_key` raises immediately rather
  than writing a bare-prefix marker.
- Concurrent sync `drain()` calls now
  serialize and return the successful drain result consistently instead of
  allowing one caller to observe another caller's in-progress pending-claim
  recovery as a terminal `False`. Sync `drain()` and async `aclose()` now cache
  only successful `True` drains; a `False` timeout/transient result is retryable
  with a fresh timeout budget, matching the README contract.

### Documentation

- Added an observability warning that `QueueEvent.error`
  retains exception messages, causes, tracebacks, and frame locals that may
  contain Redis credentials, payload data, or environment values. README now
  recommends `event.exception_type` for metrics and labels, and limits
  `event.error` export to trust-equivalent, access-controlled telemetry sinks.
  The production-readiness report cross-references the warning.
  `examples/production/observability.py` and its async sibling now gate
  `event.error` export behind a `SPAN_SINK_TRUSTED` flag mirroring the
  README pattern.
- README install
  instructions now allow the published 7.x line. Added top-of-README sync and
  async quickstarts that publish and consume in one paste, with local Redis
  prerequisites and expected output. Expanded the v6 to v7 migration section
  to cover all four v7 breaking changes: `StrEnum` event types, post-drain
  publish refusal, `drop_oldest` construction rejections, and the
  `redis<8.0.0` dependency cap.

### Examples

- Every example reads Redis from `REDIS_URL`
  with a localhost fallback and documents the environment variable at the
  top of each file. The fallback uses `os.getenv("REDIS_URL") or
  "redis://..."` so an explicitly empty `REDIS_URL=""` is treated the same
  as unset rather than passing `""` to `Redis.from_url` (which raises a
  confusing `ValueError`). Sync and async graceful-shutdown examples show
  `QueueDrainedError` handling around late publishes after drain begins.

### Error Messages

- `ConfigurationError` raised by
  `validate_gateway_parameters`, `validate_pending_backpressure_parameters`,
  and `validate_dead_letter_parameters` now includes "Use X to ..."
  remediation guidance. `QueueBackpressureError` and
  `RetryBudgetExhaustedError` append capacity / connectivity remediation
  hints to their messages.

## v7.0.0

Major release fixing footguns and tightening
contracts identified by auditing the v6.0.1 surface under realistic
operational pressure (long-running, multi-process, pinned-redis-py-version,
full-instrumentation).

### Breaking Changes

- `EventOperation` and `EventOutcome` are now `StrEnum`
  classes instead of `typing.Literal` aliases. Runtime-compatible because
  `StrEnum` is a `str` subclass; type-strict callers using the previous
  `Literal[...]` annotations should switch to the enum types.
- `drain()` / `close()` (sync) and `drain()` / `aclose()`
  (async) now put the queue instance into a queue-local drained state
  that rejects all subsequent `publish()` calls with
  `QueueDrainedError("queue is drained")`. This makes explicit drain a
  coherent shutdown boundary: once drain starts, no new messages can
  enter that queue instance's publish path, and drain waits for any
  publish already inside that path before returning. The drained state
  is local to the Python queue object and is not persisted to Redis; a
  fresh `RedisMessageQueue(...)` over the same Redis keys can still
  publish.
- Three previously-accepted-but-unsafe
  configurations now raise `ConfigurationError` at construction:
  `pending_overload_policy="drop_oldest"` with `max_pending_length=None`
  (silent no-op); `pending_overload_policy="drop_oldest"` with
  `max_delivery_count` set (silently discarded pending DLQ candidates);
  and the existing `drop_oldest`+deduplication
  rejection is now enforced through the centralized
  `validate_pending_backpressure_parameters` validator (queue ctors and
  direct `RedisGateway` ctors both rely on the same single source of
  truth).
- Capped `redis<8.0.0` in `pyproject.toml` until
  RESP3-default compatibility is verified. Users on redis-py 7.x and
  earlier are unaffected.

### New API

- Added `QueueEvent.error: BaseException | None`
  so adapters can call `span.record_exception(ex)` from the event
  payload without relying on ambient exception state. The label-friendly
  `exception_type: str | None` field remains for metrics.
- Added `QueueDrainedError`, a subclass of
  `RedisMessageQueueError`, exported from both `redis_message_queue`
  and `redis_message_queue.asyncio`.
- Added `GracefulInterruptHandler.reset()` to
  release signal-handler ownership and restore saved handlers. Idempotent.
  Lets a forked child reset the parent-installed handler before
  constructing its own. README documents the pre-fork constraint.

### Bug Fix

- Heartbeat tenacity inter-attempt sleep is now
  interruptible by the stop event. Previously, a longer retry delay
  could let the daemon heartbeat thread outlive `stop()`'s short join
  timeout; with the interrupt-aware sleep hook the thread observes the
  stop signal in bounded ~50ms intervals.
- Concurrent async `aclose()` callers no longer race
  through the gateway drain path. An `asyncio.Lock` serializes the
  drain body; later concurrent callers wait for the first drain and
  return its cached boolean result.
- `pending_overload_policy="block"` now uses
  exponential backoff with jitter (start 10ms, double on each overload
  sentinel, cap `min(500ms, pending_overload_block_timeout_seconds/10)`,
  jitter `base * (0.8 + 0.4 * random.random())`) instead of a fixed
  10ms poll. Each sleep remains bounded by the remaining timeout budget.

### Documentation

- Added production-readiness catalog rows covering pending-backpressure caveats,
  explicit drain behavior, callback/exception-hierarchy notes, redis-py
  `max_connections=<finite>` pool-cap recommendation, and the fork-after-
  construct residual-risk row.
- Added prescriptive "If you need..." subsections
  to README ordering (strict queue-wide order, per-key order, fairness,
  cross-batch reclaim) and operator guidance to production-readiness Redis clock dependencies
  (Redis clock stability, skew alerting, visibility-timeout tuning).
- Gateway docstring and README now state that
  redis-py 6.0+ changed default standalone retry from `None` to a
  3-attempt `ExponentialWithJitterBackoff`. Pass `retry=None` to
  redis-py when strict at-most-once is required for non-deduplicated
  publishes.
- Added README
  observability subsections — *Event dispatch context* (sync-heartbeat
  contextvars/OTel/structlog boundary), *Event timing vs. Redis commit*
  (`failed/failure` pre-cleanup; other events post-commit), and
  *Intentionally silent paths* (Cluster `pcall` cleanup, VT claim-store
  OOM compensation, drain/close/aclose lifecycle, non-claim-loop retry
  attempts). Production-readiness observability guidance summarizes these boundaries.

### Examples

- Added `examples/production/backpressure.py`,
  `examples/production/graceful_shutdown.py`, and async siblings under
  `examples/production/asyncio/`. Backpressure examples use
  `max_pending_length` + `pending_overload_policy="raise"` and catch
  `QueueBackpressureError`; graceful-shutdown examples install signal
  handlers and call `drain()` / `aclose()` with a timeout.
- Observability examples now construct queue +
  Redis client inside a factory function instead of at module import
  time. The previous shape was a fork hazard under pre-fork servers
  (uWSGI, gunicorn, Celery prefork).

## v6.0.1

Patch release fixing contract gaps and documentation drift in the v6.0.0
surface.

### Bug Fix

- Wire `RetryBudgetExhaustedError` and
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
- `aclose()` preserves cleanup on task
  cancellation. The async drain now routes through
  `_await_preserving_cancellation(...)` (a helper already in the
  module) so `_pending_claim_ids` is fully drained before
  `CancelledError` propagates.
- Reject `pending_overload_policy="drop_oldest"`
  combined with deduplication at construction. The previous behavior
  evicted the oldest pending message but did NOT delete its dedup
  key, silently suppressing future publishes of the same payload.
  Users needing overload protection on a deduplicated queue should
  pick `"raise"` or `"block"`.
- `on_event` callback failures no longer escape
  under `warnings.filterwarnings("error", RuntimeWarning)` or
  `PYTHONWARNINGS=error`. `_emit_event` now wraps the
  `warnings.warn(...)` call in a `warnings.catch_warnings()` block
  with a local `always` filter so callback exceptions do not crash
  queue operations under hardened warning policies.

### New API

- Add sync `close(timeout=None)`
  as a documented alias of `drain(timeout=None)` on `RedisMessageQueue`.
  Async remains `aclose()`. Restores parity with redis-py shutdown
  naming (`close()` sync / `aclose()` async).

### Tests / CI

- Add Cluster regression tests covering all three
  lease-aware Lua scripts under cross-slot `claim_result_refs` keys
  (VT reclaim, lease ack, lease move-to-completed). Confirms the
  `pcall` wrap correctly swallows Cluster rejection so the surrounding
  mutations complete. Skipped when `REDIS_CLUSTER_URL` is unset;
  runs in the `real-redis-cluster` CI job.

### Documentation

- Documentation drift cleanup:
  - `docs/production-readiness.md` observability row rewritten from stale
    "no observability hooks" to mitigated-by-`on_event` with caveats.
  - `CHANGELOG.md` v6.0.0 backpressure entry corrected: `drop_oldest` uses
    `RPOP` (not `LPOP`).
  - `README.md` "Upgrading" section now has a v5 → v6 migration
    subsection covering new APIs, new constructor rejections, the
    custom-gateway `renew_message_lease(..., *, is_interrupted=None)`
    signature change, and exception-hierarchy guidance.

## v6.0.0

### Bug Fix

- Wrap data-derived `DEL` in `pcall` across the three lease-aware
  Lua scripts (`CLAIM_MESSAGE_WITH_VISIBILITY_TIMEOUT`,
  `REMOVE_MESSAGE_WITH_LEASE_TOKEN`, `MOVE_MESSAGE_WITH_LEASE_TOKEN`).
  Under Redis Cluster, the prior code could strand a message in
  `processing` if the data-derived key was rejected by Cluster's
  key-routing contract; the `pcall` lets the surrounding mutations
  complete and relies on the per-claim TTL to self-heal orphaned
  claim-result entries.
- Make heartbeat-renewal retry interruptible by the stop signal.
  Previously, a tenacity retry could outlast a `drain()` / `aclose()`
  call and continue renewing a lease the queue intended to release.
- Fail fast in the gateway constructor when passed a
  `redis.sentinel.Sentinel` (sync or async) directly, instead of
  silently misusing it as a client. Pass `sentinel.master_for(name)`
  instead.

### New API

- `max_pending_length` + `pending_overload_policy` constructor
  parameters add a publish-side backpressure surface. Policies:
  `raise` (default — raises `QueueBackpressureError`),
  `block` (waits up to `pending_overload_block_timeout_seconds`,
  default 1.0s, then raises if still over the limit), or
  `drop_oldest` (Lua-side RPOP of the oldest pending message —
  accept silent eviction trade-off; use with care).
- `drain()` (sync) / `aclose()` (async) graceful-shutdown API
  on the queue surfaces. Refuses new claims, drains in-flight
  `_pending_claim_ids` via the recovery cycle, and bounds heartbeat
  stop with documented best-effort semantics. Returns `True` if
  drain completed within the optional `timeout` window, `False`
  otherwise.
- `on_event` constructor callback receives a stable
  `QueueEvent` dataclass for every publish/consume/cleanup state
  transition. The exception hierarchy now roots at
  `RedisMessageQueueError`, with subclasses `ConfigurationError`,
  `GatewayContractError`, `LuaScriptError`, `CleanupFailedError`,
  `RetryBudgetExhaustedError`, and `QueueBackpressureError`. Where
  redis-py exceptions wrap a queue surface, the new subclasses
  inherit from BOTH `RedisMessageQueueError` AND the matching redis-py
  base, preserving downstream `except redis.RedisError` catches.

### Breaking Changes

- New base class `RedisMessageQueueError`
  inserts into the MRO of the existing exception classes. Code that
  catches specific subclasses continues to work; code that relied on
  the absence of a common base may now catch broader exception
  classes than before. Audit `except` chains that combined
  `redis.RedisError` with a per-class handler.
- Code that passed a `redis.sentinel.Sentinel`
  object directly to the queue (instead of `Sentinel.master_for(...)`)
  now raises at construction. Previously, this silently misbehaved.

### CI Hardening

- Add four real-Redis CI jobs covering OOM, Redis Cluster,
  `WRONGTYPE`, and `decode_responses=True` paths. The Cluster job
  validates the cluster cleanup behavior.

### Documentation

- Add README sections on Fork safety and Redis memory
  sizing for deduplication and replay metadata.
- Clarify that the retry budget uses monotonic elapsed time
  (not wall-clock); expand `docs/production-readiness.md` Redis clock dependencies with
  three Redis-clock-jump scenarios.
- Add Ordering & multi-consumer fairness contract section
  documenting cross-batch reordering under the 100-message reclaim cap.

## v5.0.0

### Bug Fix

- Guard deduplicated publish Lua against OOM partial commits by using `pcall`
  around `LPUSH`.
- Tighten channel-rule warnings and follow-up filter behavior so user-actionable
  failures are emitted predictably.
- De-flake lease, deduplication, and interleaved-expiry tests.

### Breaking Changes

- `visibility_timeout_seconds` now defaults to `300` instead of `None`.
  Pass `visibility_timeout_seconds=None` to keep the old at-most-once behavior.
- `max_delivery_count` now defaults to `10` when visibility timeouts are
  enabled, with an auto-derived dead-letter queue.
- `max_completed_length` and `max_failed_length` now default to `1000`
  instead of unbounded retention.
- `get_deduplication_key` now defaults to a SHA-256 hash of the canonical
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
