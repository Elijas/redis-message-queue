# Production Readiness Report

Consolidated reference for residual risks, known limitations, and design tradeoffs
in `redis-message-queue`. Each item is independently tested; this document collects
them in one place.

Applicable version: 1.0.0

## Residual Risks

| ID | Severity | Description | Where Tested / Documented |
|----|----------|-------------|---------------------------|
| R1 | MEDIUM | Heartbeat failure is invisible during processing — if a heartbeat renewal fails (network error or stale lease), the heartbeat stops silently; the consumer continues processing but may find at ack time that the message was reclaimed by another consumer | README (crash recovery tradeoffs), `test_heartbeat_lifecycle.py` (stale lease warning tests) |
| R2 | MITIGATED | ~~No retry limit or dead-letter queue for poison messages~~ — **mitigated** by `max_delivery_count` parameter: messages exceeding the delivery limit are routed to a dead-letter queue. Without `max_delivery_count`, the original unlimited-redelivery behavior is preserved. | `test_dead_letter.py`, `test_lease_stress.py:TestPoisonMessageIsolation` |
| R3 | MITIGATED | ~~Completed/failed queues grow without bound~~ — **mitigated** by `max_completed_length` and `max_failed_length` parameters: LTRIM is called after each move to cap queue size. Without these parameters, the original unbounded behavior is preserved. | `test_process_message.py:TestBoundedCompletedQueue`, `test_process_message.py:TestBoundedFailedQueue` |
| R4 | LOW | Batch reclaim limit of 100 — the visibility-timeout reclaim Lua script processes at most 100 expired messages per consumer poll, which may delay recovery under extreme backlog | `_config.py:141` (Lua LIMIT), `test_visibility_timeout.py`, `_model_based.py:225-230`, `test_lease_stress.py` |
| R5 | LOW | At-most-once delivery without visibility timeout (by design) — a consumer crash orphans the in-flight message permanently | README (delivery semantics table), `test_process_message.py:TestAtMostOnceMessageLoss` (label F1) |
| R6 | LOW | False-negative returns after ambiguous-success retries — idempotent operations produce correct Redis state but may return False when the operation actually succeeded | `test_retry_safety_audit.py` (12 test cases, 467 lines) |
| R7 | LOW | No metrics or observability hooks — the library logs via Python `logging` but exposes no callbacks, event hooks, or metric counters for programmatic monitoring | README (known limitations section) |

## Design Decisions

### Atomic Lua Scripts

All critical state transitions use Lua scripts to guarantee atomicity:

| Operation | Script | Atomicity Guarantee |
|-----------|--------|---------------------|
| Publish with dedup | `PUBLISH_MESSAGE_LUA_SCRIPT` | SET NX + LPUSH |
| Claim with VT | `CLAIM_MESSAGE_WITH_VISIBILITY_TIMEOUT_LUA_SCRIPT` | Requeue expired + LMOVE + HINCRBY delivery count + dead-letter check + INCR + ZADD + HSET |
| Move with lease | `MOVE_MESSAGE_WITH_LEASE_TOKEN_LUA_SCRIPT` | HGET token check + LREM + LPUSH + metadata cleanup + HDEL delivery count |
| Remove with lease | `REMOVE_MESSAGE_WITH_LEASE_TOKEN_LUA_SCRIPT` | HGET token check + LREM + metadata cleanup + HDEL delivery count |
| Renew lease | `RENEW_MESSAGE_LEASE_LUA_SCRIPT` | HGET token check + ZADD |

### Non-Idempotent Operations Are Not Retried

These operations are deliberately excluded from the retry wrapper to prevent duplication:

- `add_message()` — raw LPUSH, retry could enqueue twice
- `wait_for_message_and_move()` without VT — LMOVE/BLMOVE, retry could claim twice
- `_claim_visible_message()` — single Lua eval, no retry wrapper

### Exception Handling Preserves Original Errors

In `process_message()`, cleanup exceptions during the `except` path are caught and logged,
then the original exception is always re-raised via bare `raise` (see `redis_message_queue.py:281-295`).

## Test Coverage Summary

The test suite includes 1,660+ tests across 31 files:

| Category | Files | What It Covers |
|----------|-------|----------------|
| Model-based | `_model_based.py`, `test_model_based.py`, `test_model_no_vt.py`, `test_model_scenarios.py` | 16 invariants checked after every step, 400+ randomized seeds |
| Retry safety | `test_retry_safety_audit.py` | Ambiguous-success for all operations, sync + async |
| Heartbeat lifecycle | `test_heartbeat_lifecycle.py` | Start/stop/failure/double-stop/slow-renewal/stale-lease |
| Lease stress | `test_lease_stress.py` | Mass expiry (100-2000 messages), poison messages, multi-consumer drain |
| Gateway contracts | `test_gateway_contract.py`, `test_gateway_return_type_validation.py` | Return type validation (F1-F6), lease enforcement, duck-type checks |
| Dead-letter queue | `test_dead_letter.py` | Delivery counting, dead-letter routing, cleanup on ack, constructor validation |
| Constructor validation | `test_gateway_constructor.py` | Every parameter combination, type/value errors |
| Integration | `test_integration_*.py` | Real Redis (sync, async, blocking, non-blocking), concurrency (20 threads/tasks) |

### Known Untested Territory

These are not bugs, but areas without dedicated test coverage:

- Sustained Redis connection failures / pool exhaustion
- Large message payloads / memory pressure
- Clock skew between Redis servers
- Async deduplication callable edge cases (construction validation IS tested)

## Test Label Index

Tests that document known limitations use short labels for cross-referencing:

| Label | File | Description |
|-------|------|-------------|
| F1 | `test_process_message.py:TestAtMostOnceMessageLoss` | At-most-once message loss without VT (R5) |
| F2 | `test_process_message.py:TestCompletedQueueGrowth` | Unbounded completed/failed queue growth (R3) |
| F3 | `test_process_message.py:TestClusterHashTagCompatibility` | Redis Cluster CROSSSLOT incompatibility |
| F4 | `test_gateway_retry_safety.py` | Message loss on transient LMOVE/BLMOVE failure without VT |
| F1-F6 | `test_gateway_return_type_validation.py` | Gateway return-type validation (separate numbering scheme for validation-specific findings) |
