# FIX-AD-06-v8

## Validator Added

`redis_message_queue/_config.py:35`:

```python
DEDUPLICATION_REQUIRES_KEY_MESSAGE = (
    "deduplication=True requires get_deduplication_key (callable returning a non-empty str). "
    "Pass a callable like `lambda msg: msg['id']` (recommended: a stable logical ID), "
    "or set deduplication=False."
)
```

`redis_message_queue/_config.py:309`:

```python
def validate_dedup_configuration(
    *,
    deduplication: bool,
    get_deduplication_key: object,
) -> None:
    if deduplication and get_deduplication_key is None:
        raise ConfigurationError(DEDUPLICATION_REQUIRES_KEY_MESSAGE)
```

Invoked from:

- `redis_message_queue/redis_message_queue.py:520`
- `redis_message_queue/asyncio/redis_message_queue.py:575`

## Error Message

```text
deduplication=True requires get_deduplication_key (callable returning a non-empty str). Pass a callable like `lambda msg: msg['id']` (recommended: a stable logical ID), or set deduplication=False.
```

## Full-Payload Fallback Removal

- Sync publish now derives the dedup suffix only from `self._get_deduplication_key(message)` and validates that callable result before namespacing it: `redis_message_queue/redis_message_queue.py:761`.
- Async publish mirrors the same callable-only path: `redis_message_queue/asyncio/redis_message_queue.py:807`.
- The prior default SHA-256 helper and `get_deduplication_key is None` payload fallback branches were removed from both queue implementations.

## Test Files Updated

- `tests/test_dedup_empty_key.py`: 2 new constructor-regression tests, sync and async.
- `tests/test_publish.py`: 13 dedup publish tests updated to provide explicit v7-compatible key function; obsolete explicit-`None` fallback test replaced.
- `tests/test_publish_async.py`: 13 async dedup publish tests updated similarly; obsolete explicit-`None` fallback test replaced.
- `tests/test_process_message.py`: 5 constructor/cluster validation tests updated or added.
- `tests/test_process_message_async.py`: 5 async constructor/cluster validation tests updated or added.
- `tests/test_decode_responses.py`: 2 duplicate-rejection tests updated.
- `tests/test_integration_decode_responses.py`: 2 real-Redis duplicate-rejection tests updated.
- `tests/test_integration_sync.py`: 8 real-Redis dedup publish tests updated.
- `tests/test_integration_async.py`: 8 async real-Redis dedup publish tests updated.
- `tests/test_integration_advanced_sync.py`: 2 dedup/visibility-timeout interaction tests updated.
- `tests/test_integration_advanced_async.py`: 2 async dedup/visibility-timeout interaction tests updated.
- `tests/test_gateway_return_type_validation.py`: 2 publish-message return-type tests updated to exercise the dedup path explicitly.
- `tests/test_publish_backpressure.py`: 6 test functions / 10 parameterized cases updated for explicit dedup keys.
- `tests/test_observability.py`: 2 observability/exception hierarchy tests updated.
- `tests/test_model_scenarios.py`: 4 scenario harness constructors updated for explicit dedup keys.
- `tests/_model_based.py`: model harness updated to use explicit callable-derived dedup keys.
- `tests/_model_no_vt.py`: non-VT model harness updated to use explicit callable-derived dedup keys.

## Version Bump

```diff
 [tool.poetry]
 name = "redis-message-queue"
-version = "7.0.1"
+version = "8.0.0"
 description = "Python message queuing with Redis and message deduplication"
```

## README v7 to v8 Migration Section

````markdown
### v7 to v8 migration

v8.0.0 removes implicit dedup key generation. Deduplication is opt-in and
`deduplication=True` now requires `get_deduplication_key`. If you were relying
on v7's automatic content hash, provide the equivalent callable explicitly.

Before:

```python
queue = RedisMessageQueue(
    "orders",
    client=client,
    deduplication=True,
)
```

After, prefer a stable logical ID:

```python
queue = RedisMessageQueue(
    "orders",
    client=client,
    deduplication=True,
    get_deduplication_key=lambda msg: msg["order_id"],
)
```

To preserve v7 content-hash behavior mechanically:

```python
import hashlib
import json

queue = RedisMessageQueue(
    "orders",
    client=client,
    deduplication=True,
    get_deduplication_key=lambda msg: hashlib.sha256(
        json.dumps(msg, sort_keys=True).encode()
    ).hexdigest(),
)
```

If you do not need publish-side deduplication, omit `deduplication` or set
`deduplication=False`.
````

## CHANGELOG v8.0.0 Entry

```markdown
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
```

## Verification

- `poetry run pytest` -> `2071 passed, 8 skipped, 8 warnings`
- `uvx ruff check .` -> passed
- `uvx ruff format --check .` -> passed
