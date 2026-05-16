# FIX-AD-05 - Drain/aclose return-value regressions

## Summary

Fixed both v7.0.0 drain/aclose return-value regressions for v7.0.1.

- M-R8-1: sync `drain()` now serializes concurrent callers with a queue-local
  `threading.Lock` and caches only successful `True` results. The gateway drain
  helper is also serialized so shared-gateway callers cannot see another
  caller's `_recovering_claim_ids` state as a terminal false drain.
- M-R8-2: async `aclose()` continues to serialize concurrent callers with its
  existing `asyncio.Lock`, but now caches only successful `True` results. A
  `False` timeout/transient result is discarded so the next caller can retry.

KNOWN UNKNOWN: the requested prior reports
`capsules/260515-round-7/fixes/FIX-AC-03.md` and `FIX-AC-04.md` were not present
in this worktree or current git history, so this patch was based on the v7
changelog entries and current implementation.

## Code Changes

- `redis_message_queue/redis_message_queue.py:569` adds `_drain_lock`; `:570`
  adds success-only `_drain_result`.
- `redis_message_queue/redis_message_queue.py:1085` serializes sync `drain()`;
  `:1086` returns cached success; `:1098` caches only `True`; `:1101` discards
  false results for retry.
- `redis_message_queue/_redis_gateway.py:203` adds sync gateway drain
  serialization; `:965` wraps `_drain_pending_claim_ids()` with that lock.
- `redis_message_queue/asyncio/redis_message_queue.py:1126` keeps the async
  `aclose()` serialization; `:1142` caches only `True`; `:1145` discards false
  results for retry.
- `redis_message_queue/asyncio/_redis_gateway.py:194` adds async gateway drain
  serialization; `:962` wraps `_drain_pending_claim_ids()` with that lock.
- `tests/test_graceful_drain.py:258` adds
  `test_sync_concurrent_drain_both_return_true`.
- `tests/test_graceful_drain.py:505` adds
  `test_async_aclose_after_timeout_can_retry`.
- `CHANGELOG.md:3` documents the v7.0.1 bug fix.

## Repro Output

Before patch, with only the new regression tests applied and the source edits
temporarily reversed:

```text
$ poetry run pytest tests/test_graceful_drain.py::test_sync_concurrent_drain_both_return_true tests/test_graceful_drain.py::test_async_aclose_after_timeout_can_retry -q
FF                                                                       [100%]
FAILED tests/test_graceful_drain.py::test_sync_concurrent_drain_both_return_true
FAILED tests/test_graceful_drain.py::test_async_aclose_after_timeout_can_retry
2 failed in 0.09s
```

The sync failure was `assert second_waited_for_first is True` because the second
caller returned before the first completed. The async failure was
`assert False is True` on the second `await queue.aclose(timeout=10)`.

After patch:

```text
$ poetry run pytest tests/test_graceful_drain.py::test_sync_concurrent_drain_both_return_true tests/test_graceful_drain.py::test_async_aclose_after_timeout_can_retry -q
..                                                                       [100%]
2 passed in 0.11s
```

## Verification

```text
$ poetry run pytest tests/test_graceful_drain.py -q
27 passed in 0.21s

$ poetry run pytest
2061 passed, 8 skipped, 8 warnings in 290.07s (0:04:50)

$ poetry run ruff check .
All checks passed!

$ poetry run ruff format --check .
78 files already formatted
```
