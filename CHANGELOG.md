# Changelog

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
