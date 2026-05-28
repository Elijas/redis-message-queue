# Production Readiness Report

Consolidated reference for residual risks, known limitations, and design tradeoffs
in `redis-message-queue`. Each item is independently tested; this document collects
them in one place.

Applicable version: 8.0.0

## Residual Risks

Before changing constructor parameters on a live queue, see the
[Upgrading section in README](../README.md#upgrading) — several parameter
changes are destructive on populated queues.

| ID | Severity | Description | Where Tested / Documented |
|----|----------|-------------|---------------------------|
| R1 | MEDIUM | Heartbeat failure is invisible during processing — if a heartbeat renewal fails (network error or stale lease), the heartbeat stops silently; the consumer continues processing but may find at ack time that the message was reclaimed by another consumer | README (crash recovery tradeoffs), `test_heartbeat_lifecycle.py` (stale lease warning tests) |
| R2 | MITIGATED | ~~No retry limit or dead-letter queue for poison messages~~ — **mitigated** by `max_delivery_count` parameter: messages exceeding the delivery limit are routed to a dead-letter queue. Without `max_delivery_count`, the original unlimited-redelivery behavior is preserved. | `test_dead_letter.py`, `test_lease_stress.py:TestPoisonMessageIsolation` |
| R3 | MITIGATED | ~~Completed/failed queues grow without bound~~ — **mitigated** by `max_completed_length` and `max_failed_length` parameters: LTRIM is called after each move to cap queue size. Without these parameters, the original unbounded behavior is preserved. | `test_process_message.py:TestBoundedCompletedQueue`, `test_process_message.py:TestBoundedFailedQueue` |
| R4 | LOW | Batch reclaim limit of 100 — the visibility-timeout reclaim Lua script processes at most 100 expired messages per consumer poll, which may delay recovery under extreme backlog | `CLAIM_MESSAGE_WITH_VISIBILITY_TIMEOUT_LUA_SCRIPT`, `test_visibility_timeout.py`, `_model_based.py:225-230`, `test_lease_stress.py` |
| R5 | LOW | At-most-once delivery without visibility timeout (by design) — once Redis has moved a message to `processing`, a consumer crash can orphan it there permanently, even if application code never started handling the payload | README (delivery semantics table), `test_process_message.py:TestAtMostOnceMessageLoss` (label F1) |
| R6 | MITIGATED | ~~No metrics or observability hooks~~ — **mitigated** by the `on_event` constructor callback, which receives a `QueueEvent` dataclass for publish/claim/ack/reclaim/dedup/cleanup/drain lifecycle events. Remaining caveats: callbacks are best-effort; callback exceptions are logged/warned but do not crash queue operations; the library does not ship metrics exporters. Use `examples/production/observability.py` as the adapter pattern. | [README observability](../README.md#observability), `examples/production/observability.py` |
| R7 | LOW | Redis Lua is atomic but not rollback-transactional — built-in scripts now preflight queue key types and fail closed on `WRONGTYPE`, but Redis does not undo earlier writes if a later command fails for another reason such as `OOM` under severe memory pressure. This includes the expiry-reclaim RPUSH path: if RPUSH fails after messages have already been removed from tracking structures, those messages are permanently lost. | README (known limitations section), `test_wrongtype_fail_closed.py` |
| R8 | LOW | Non-VT claim recovery hashes (`claim_result_ids`, `claim_result_backrefs`) leak two fields per orphaned message on consumer crash — proportional to R5 orphan count. With visibility timeout enabled, the expiry-reclaim Lua cleans these automatically. Without VT, manual cleanup of the processing queue also needs to clean these hash keys. At 1k abandoned claims/s for 24h, the two hashes alone can require roughly 29 GB before processing-list payloads. | `_redis_gateway.py:_claim_result_ids_key`, `_claim_result_backrefs_key`; README Redis memory sizing section |
| R9 | LOW | Dead-letter queue grows without bound — no `max_dead_letter_length` parameter exists. Under sustained poison-message load, monitor DLQ length via `LLEN {name}::dead_letter` and trim manually if needed. | `test_dead_letter.py` |
| R10 | MEDIUM | Consumer hang with heartbeat keeps message locked forever — when handler code hangs, heartbeat renews the lease indefinitely. No processing-time cap exists. Monitor processing-queue dwell time externally. | `_LeaseHeartbeat._run` loop, README (heartbeat tradeoffs) |
| R11 | MEDIUM | Redis clock jumps can move lease, deduplication, and replay windows because lease deadlines use server-side `TIME` and Redis key expirations. Python production code uses monotonic/relative timers for retry, polling, and heartbeat waits, so Python-host wall-clock jumps do not directly move those budgets. | `CLAIM_MESSAGE_WITH_VISIBILITY_TIMEOUT_LUA_SCRIPT` (ZADD with server TIME), `RENEW_MESSAGE_LEASE_LUA_SCRIPT`, retry TTL sizing |
| R12 | LOW | Co-tenant Redis applications can manipulate queue internals via predictable auxiliary keys. The library does not authenticate or isolate keys beyond the queue name prefix. | `_queue_key_manager.py` (key naming scheme) |
| R13 | LOW | Queue names containing ANSI escape sequences or newline characters can corrupt structured log output. The library does not sanitize queue names beyond checking for the key separator. | — |
| R14 | LOW | B2 `max_pending_length` does not cap completed/failed lists; those have separate caps via `max_completed_length` / `max_failed_length`. | [README publish backpressure](../README.md#publish-backpressure) |
| R15 | MEDIUM | B2 `drop_oldest` policy is intentional data loss — the dropped message is discarded silently from the queue. The current frozen feature set emits `publish/success` for the new message, but no separate `on_event` signal for the dropped message. | [README publish backpressure](../README.md#publish-backpressure) |
| R16 | LOW | B5/AC-03 `drain()` / `aclose()` do not cancel in-flight handlers that started before drain; handlers must reach natural completion. Explicit drain now refuses new publishes on the same queue instance with `QueueDrainedError`. | [README graceful shutdown](../README.md#graceful-shutdown) |
| R17 | LOW | B10 callback exceptions are caught, logged, and emitted as a queue warning; they do not interrupt queue operations. | [README observability](../README.md#observability) |
| R18 | LOW | B10/AC-03 queue-specific failures share `RedisMessageQueueError`; publish after explicit drain raises `QueueDrainedError`. See `redis_message_queue._exceptions` for the active hierarchy. | [README observability](../README.md#observability) |
| R19 | MEDIUM | **redis-py default standalone client `max_connections=None` resolves to `2**31`. A concurrency spike retains spike-created sockets until client close.** Pass `max_connections=<finite>` to `redis.Redis()` sized to expected worker + heartbeat concurrency. (Source: R7 AC-12 F1) | [README connection pool sizing](../README.md#connection-pool-sizing) |
| R20 | LOW | **Fork after construct is unsupported for queue/client objects.** Construct queue + Redis client after fork in worker processes. Sync pooled Redis recovers via redis-py PID-reset, but async clients do not. If a parent already installed `GracefulInterruptHandler`, a child can call `reset()` on the inherited handler before constructing its own handler. (Source: R7 AC-10) | [README fork safety](../README.md#fork-safety-and-pre-fork-servers) |
| R21 | LOW | Observability event semantics have intentional boundaries: sync heartbeat events run in a background thread without caller contextvars/span context; `failed/failure` is pre-cleanup; Cluster `pcall` cleanup, VT claim-store OOM compensation, `drop_oldest` evictions, and non-claim-loop retry attempts are intentionally silent or collapsed into terminal events. `QueueEvent.error` is the actual exception object and can retain sensitive data in messages, causes, tracebacks, and frame locals; use `event.exception_type` for labels and export `event.error` only to trust-equivalent, access-controlled sinks. | [README event dispatch context](../README.md#event-dispatch-context), [README event timing vs. Redis commit](../README.md#event-timing-vs-redis-commit), [README intentionally silent paths](../README.md#intentionally-silent-paths), [README secrets in `event.error`](../README.md#secrets-in-eventerror) |
| R22 | MITIGATED | ~~Implicit deduplication key generation could create an accidental "every distinct payload" dedup keyspace.~~ **Mitigated in v8.0.0** by making deduplication opt-in and requiring `get_deduplication_key` whenever `deduplication=True`. The callable must return a non-empty string; `None`, `""`, and non-`str` returns fail before enqueue. | [README deduplication](../README.md#deduplication), `test_dedup_empty_key.py` |

### R11: Redis Clock Dependencies

Visibility-timeout lease deadlines are written and compared with Redis
`TIME`. Redis-side replay, deduplication, and claim-result keys also expire
on Redis key TTLs. The library does not compensate for non-monotonic Redis
server time.

- **Redis host forward clock jump:** existing lease deadlines and Redis TTLs
  can become due immediately. Under the default `visibility_timeout_seconds=300`,
  a forward step beyond an active lease's remaining time can make the next
  visibility-timeout claim reclaim a message that a healthy handler is still
  processing. The original handler's later ack/move can then fail as stale
  because the lease token was replaced, creating duplicate processing and
  incrementing delivery counts without a real processing overrun. A large
  forward jump can also expire replay/dedup keys while Python is still inside
  its retry budget.
- **Redis host backward clock jump:** existing lease scores and Redis TTL
  expirations move farther into the future relative to Redis `now`. Crash
  recovery can be delayed by the jump magnitude plus the remaining visibility
  timeout, and dedup/replay keys can suppress operations longer than their
  configured real-time windows.
- **Static Python/Redis clock skew:** no direct production hazard was found
  from absolute skew alone. Production code does not compare Python wall-clock
  time with Redis `TIME`, and no production callsite uses `time.time()`,
  `datetime.utcnow()`, or `datetime.now(...)` for retry, polling, or heartbeat
  math. Cross-side coherence depends instead on Redis TTL countdowns advancing
  roughly in line with Python monotonic retry windows.

**Operator guidance:**

- Run Redis hosts with stable, slewed time (NTP `slew` mode, not `step`).
  Avoid clock jumps >100ms during normal operation.
- Alert on `time` skew between Python application hosts and Redis hosts. >1s
  skew should page; >100ms should warn.
- Treat duplicate-delivery and stale-lease spikes immediately after a Redis
  clock event as expected. They are the at-least-once property doing its job.
- If you cannot guarantee stable time on Redis hosts (for example, shared cloud
  Redis where the host time is opaque), increase `visibility_timeout_seconds` to
  absorb expected skew without triggering reclaim storms.

## Design Decisions

### Atomic Lua Scripts

All critical state transitions use Lua scripts to guarantee atomicity:

| Operation | Script | Atomicity Guarantee |
|-----------|--------|---------------------|
| Publish with dedup | `PUBLISH_MESSAGE_LUA_SCRIPT` | SET NX + LPUSH + replay marker so ambiguous-success retries preserve the original boolean result |
| Claim without VT | `CLAIM_MESSAGE_LUA_SCRIPT` | Replayable claim ID + LMOVE + persisted claim metadata so recovery survives loss of the short-lived claim-result key |
| Claim with VT | `CLAIM_MESSAGE_WITH_VISIBILITY_TIMEOUT_LUA_SCRIPT` | Requeue expired + replayable claim ID + LMOVE + HINCRBY delivery count + dead-letter check + INCR + ZADD + pcall-guarded HSET (OOM compensation on metadata writes) + persisted claim metadata |
| Move without lease | `MOVE_MESSAGE_LUA_SCRIPT` | LPUSH + LREM + replay marker + non-VT claim metadata cleanup |
| Remove without lease | `REMOVE_MESSAGE_LUA_SCRIPT` | LREM + replay marker + non-VT claim metadata cleanup |
| Move with lease | `MOVE_MESSAGE_WITH_LEASE_TOKEN_LUA_SCRIPT` | HGET token check + LPUSH + LREM + replay marker + claim metadata cleanup + HDEL delivery count |
| Remove with lease | `REMOVE_MESSAGE_WITH_LEASE_TOKEN_LUA_SCRIPT` | HGET token check + LREM + replay marker + claim metadata cleanup + HDEL delivery count |
| Renew lease | `RENEW_MESSAGE_LEASE_LUA_SCRIPT` | HGET token check + ZADD |

`max_delivery_count` counts successful Redis-side claims (leases granted), not confirmed handler starts. A process that dies after Redis grants a claim still consumes one delivery attempt.

Built-in scripts also preflight expected key types before the first mutating
command. This turns stray `WRONGTYPE` key collisions into fail-closed errors
instead of partial queue mutations.

### Generic Retry Wrapper Is Reserved For Safe Operations

These operations intentionally avoid the generic tenacity retry wrapper:

- `add_message()` — raw LPUSH, retry could enqueue twice
- `_claim_message_without_visibility_timeout()` — single Lua eval, recovered in the outer polling loop via claim IDs plus persisted claim replay metadata
- `_claim_visible_message()` — single Lua eval, recovered in the outer polling loop via claim IDs plus persisted claim replay metadata

Active wait calls keep their claim IDs local while they are still retrying.
Only orphaned claim IDs from an earlier failed or interrupted wait are published to shared recovery state, which prevents a concurrent caller on the same gateway instance from recovering the same in-flight claim twice. Timed waits also remain bounded: once the configured wait window has expired, the claim loop only replays persisted state for that same claim attempt and does not claim fresh work after the timeout boundary.

### Exception handling design

All queue exceptions descend from `RedisMessageQueueError`. The active hierarchy
as of v7.0.0 is:

- `RedisMessageQueueError` (base)
  - `ConfigurationError` — invalid constructor args
  - `GatewayContractError` — gateway protocol violation
  - `LuaScriptError` — Lua script execution failure
  - `QueueBackpressureError` — `pending_overload_policy="raise"` triggered
  - `QueueDrainedError` — `publish()` called after explicit drain/aclose
  - `CleanupFailedError` — cleanup-after-success failed
  - `RetryBudgetExhaustedError` — also subclass of `redis.RedisError` for backward-compat

Catch `RedisMessageQueueError` to handle all queue-specific failures. Catch
`redis.RedisError` to handle Redis-layer failures (which includes
`RetryBudgetExhaustedError`).

## Test Coverage Summary

The test suite includes 1,933 tests across 33 files:

| Category | Files | What It Covers |
|----------|-------|----------------|
| Model-based | `_model_based.py`, `test_model_based.py`, `test_model_no_vt.py`, `test_model_scenarios.py` | 16 invariants checked after every step, 400+ randomized seeds |
| Retry safety | `test_retry_safety_audit.py` | Ambiguous-success replay for publish, cleanup, lease renewal, and claim recovery, sync + async |
| Wrong-type fail-closed | `test_wrongtype_fail_closed.py` | Guarded `WRONGTYPE` failures for publish, ack/move, VT reclaim, and dead-letter paths |
| Heartbeat lifecycle | `test_heartbeat_lifecycle.py` | Start/stop/failure/double-stop/slow-renewal/stale-lease |
| Lease stress | `test_lease_stress.py` | Mass expiry (100-2000 messages), poison messages, multi-consumer drain |
| Gateway contracts | `test_gateway_contract.py`, `test_gateway_return_type_validation.py` | Return type validation (F1-F6), lease enforcement, duck-type checks |
| Dead-letter queue | `test_dead_letter.py` | Delivery counting, dead-letter routing, cleanup on ack, constructor validation |
| Constructor validation | `test_gateway_constructor.py` | Every parameter combination, type/value errors |
| Integration | `test_integration_*.py` | Real Redis (sync, async, blocking, non-blocking), concurrency (20 threads/tasks) |

### Known Untested Territory

These are not bugs, but areas without dedicated test coverage:

- Sustained Redis connection failures / prolonged pool exhaustion under load
- Large message payloads / memory pressure
- Clock skew between Redis servers
- Model-based tests do not exercise dead-letter / delivery-count paths
- VT reclaim LREM is O(expired x processing_queue_depth) — benchmarked at 73ms for 50K messages with 100 expired
- Double external `CancelledError` during async `process_message` finally block can replace original exception
- Async deduplication callable — cancellation during dedup-key computation, nested coroutines, and event-loop-cross interaction remain untested
- Runtime Redis Cluster coverage — construction-time hash-tag validation is tested, but no integration test exercises sharded multi-key Lua evaluation, MOVED/ASK redirects, or cross-slot key access at runtime. A 3-node cluster fixture is needed to close this gap.
- Heartbeat lease renewal timing against real Redis — lifecycle tests use fakeredis; renewal latency, thread scheduling jitter, and real network delays are not exercised.
- Model-based randomized testing covers sync gateway only — async gateway and non-VT claim path at integration level remain unexercised by the model harness.

## Test Label Index

Tests that document known limitations use short labels for cross-referencing:

| Label | File | Description |
|-------|------|-------------|
| F1 | `test_process_message.py:TestAtMostOnceMessageLoss` | At-most-once message loss without VT (R5) |
| F2 | `test_process_message.py:TestCompletedQueueGrowth` | Unbounded completed/failed queue growth (R3) |
| F3 | `test_process_message.py:TestClusterHashTagCompatibility` | Redis Cluster hash-tag requirement |
| F1-F6 | `test_gateway_return_type_validation.py` | Gateway return-type validation (separate numbering scheme for validation-specific findings) |
