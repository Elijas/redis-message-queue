# Production-Readiness Audit

**Date**: 2026-03-23
**Scope**: Correctness under failure, recovery guarantees, observability gaps, API footguns, test coverage blind spots
**Verdict**: No actionable bugs found. No patches or new tests required.

---

## What Was Verified

| Area | Evidence |
|---|---|
| **Lua script correctness** | All 6 scripts verified: KEYS/ARGV mappings match call sites in both sync (`_redis_gateway.py`) and async (`asyncio/_redis_gateway.py`) gateways. Atomicity guarantees hold. |
| **Lease token protocol** | Claim, renew, ack, and stale-token rejection are correct. Model-based tests verify 16 invariants including conservation, token monotonicity, token accounting completeness, and metadata consistency (`tests/_model_based.py:54-170`). |
| **Race conditions** | Atomic Lua scripts prevent all TOCTOU races. Cross-message token acks rejected. Double-acks rejected. Stale acks/renewals rejected. |
| **Exception handling in `process_message`** | Original exception always re-raised on failure path. Cleanup exceptions logged but never mask user exceptions. Heartbeat always stopped in `finally`. Success-path cleanup failures propagate correctly. All tested in `test_process_message.py` and `test_heartbeat_lifecycle.py`. |
| **Retry safety** | Ambiguous-success scenarios (server executes, client loses connection) exhaustively tested for all gateway operations in `test_retry_safety_audit.py` and `test_gateway_retry_safety.py`. Non-idempotent operations (`add_message`, `wait_for_message_and_move` without VT) correctly NOT retried. |
| **Sync/async parity** | Both gateways are structural mirrors. Same Lua scripts, same KEYS/ARGV mappings, same validation logic. Async heartbeat uses `asyncio.Task` + cancellation instead of `threading.Thread` + daemon. |

---

## Residual Risks

These are design limitations, not bugs. Each is documented and tested in the test suite.

### R1 (MEDIUM): Heartbeat failure is invisible to user code during processing

- **Location**: `redis_message_queue.py:114-116`
- When the heartbeat thread/task fails, it logs via `logger.exception()` and exits silently.
- Consumer continues processing. Lease may expire. Another consumer reclaims.
- User only discovers this at ack time via a WARNING log ("was a no-op").
- No callback, flag, or exception to signal the consumer mid-processing.
- **Impact**: Double-processing of non-idempotent work under VT.
- **Mitigation**: This IS the at-least-once guarantee working as designed. Users processing non-idempotent work should design for idempotency.

### R2 (MEDIUM): No retry limit for VT redelivery (poison messages)

- **Tests**: `test_lease_stress.py::TestPoisonMessageIsolation`
- Messages that consistently crash consumers are reclaimed indefinitely.
- Good messages DO make progress between reclaim cycles (tested).
- **Mitigation**: Users must implement their own DLQ/retry-count logic at the application layer.

### R3 (LOW-MEDIUM): Completed/failed queues grow without bounds

- **Tests**: `test_process_message.py::TestCompletedQueueGrowth`
- Every processed message accumulates with no LTRIM, TTL, or max-length.
- **Mitigation**: Users must implement periodic cleanup. Consider adding `LTRIM` or `EXPIRE` in a future version.

### R4 (LOW): Reap batch limit of 100 in claim Lua script

- **Location**: `_config.py:139`
- `CLAIM_MESSAGE_WITH_VISIBILITY_TIMEOUT_LUA_SCRIPT` reaps at most 100 expired messages per invocation.
- Under mass crash (1000+ workers), recovery takes multiple claim cycles.
- **Mitigation**: The limit exists to prevent blocking Redis with long Lua scripts. Each subsequent claim reaps another batch. Recovery is eventual, not instant.

### R5 (LOW): At-most-once message loss without VT (by design)

- **Tests**: `test_process_message.py::TestAtMostOnceMessageLoss`
- Default configuration has no visibility timeout. Crashed consumer orphans message permanently.
- **Mitigation**: Use `visibility_timeout_seconds` for at-least-once delivery.

### R6 (LOW): False-negative return values after ambiguous-success retries

- **Tests**: `test_retry_safety_audit.py::TestSyncAmbiguousSuccessReturnValues`
- When an operation succeeds server-side but the client loses the response, the retry sees an already-applied state and returns `False`.
- Queue state is correct, but the caller sees a misleading return value.
- **Mitigation**: Callers should treat `False` as "operation may or may not have been applied" rather than "definitely not applied."

### R7 (LOW): No metrics or observability hooks

- No callbacks, counters, or hooks for message throughput, processing latency, or queue depth.
- Users must inspect Redis keys manually or rely on log output.
- **Mitigation**: Could add optional queue-depth inspection methods in a future version.

---

## Test Coverage Summary

| Area | Coverage | Key test files |
|---|---|---|
| Model-based invariant checking | 16 invariants, 300+ seeds, up to 2000 messages | `_model_based.py`, `test_model_based.py`, `test_model_scenarios.py` |
| Retry safety (ambiguous success) | All gateway operations, sync + async | `test_retry_safety_audit.py`, `test_gateway_retry_safety.py` |
| Heartbeat lifecycle | Start, stop, failure, double-stop, slow-renewal, CancelledError, stale lease | `test_heartbeat_lifecycle.py`, `test_lease_heartbeat.py` |
| Constructor validation | Types, values, conflicts, edge cases | `test_process_message.py`, `test_gateway_constructor.py` |
| Gateway return type validation | All methods, wrong-type rejection | `test_gateway_return_type_validation.py` |
| Lease stress | Mass expiry (100-2000), metadata cleanup, multi-consumer drain, poison messages | `test_lease_stress.py` |
| Integration (real Redis) | Sync + async, blocking + non-blocking | `test_integration_sync.py`, `test_integration_async.py`, `test_integration_blocking_*.py` |

---

## Minor Observations

These are not actionable but worth noting for future maintainers.

1. **Tenacity logs at ERROR level on every retry attempt** (`_config.py:74`) -- could cause alert fatigue under transient Redis connectivity issues. Consider WARNING for transient retries.
2. **`_warned_no_lease_for_heartbeat` flag is not thread-safe** (`redis_message_queue.py:261`) -- harmless (worst case: duplicate warning log), but assumes single-threaded `process_message` usage per queue instance.
3. **`stop_after_delay(120)` is a long retry window** (`_config.py:68`) -- consumers stall up to 2 minutes on a broken Redis before failing. This is intentional for resilience but may surprise users expecting faster failure.
