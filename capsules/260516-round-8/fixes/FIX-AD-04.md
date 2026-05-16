# FIX-AD-04 - Reject empty/None dedup keys at publish time

## Summary

Custom `get_deduplication_key` callable returns are now validated during
publish before Redis key construction. `None` and `""` fail loudly with
`ConfigurationError`, non-`str` values fail with `TypeError`, and no message or
bare-prefix dedup marker is written.

## Validation Code Added

`redis_message_queue/_queue_key_manager.py:4`

```python
def validate_callable_deduplication_key(dedup_key: object, message: str | dict) -> str:
    if dedup_key is None:
        raise ConfigurationError(
            f"get_deduplication_key returned None for message {message!r}; the callable must return a non-empty string"
        )
    if not isinstance(dedup_key, str):
        raise TypeError(f"get_deduplication_key must return a str, got {type(dedup_key).__name__}")
    if dedup_key == "":
        raise ConfigurationError(
            f"get_deduplication_key returned an empty string for message {message!r}; "
            "the callable must return a non-empty, high-cardinality key"
        )
    return dedup_key
```

Call sites:

- `redis_message_queue/redis_message_queue.py:780`
- `redis_message_queue/asyncio/redis_message_queue.py:819`

Final key-construction guard:

`redis_message_queue/_queue_key_manager.py:69`

```python
def deduplication(self, message: str) -> str:
    if not isinstance(message, str):
        raise TypeError(f"'deduplication_key' must be a str, got {type(message).__name__}")
    if message == "":
        raise ConfigurationError("'deduplication_key' must be a non-empty string")
    return f"{self._queue_name}{self._key_separator}{self._MESSAGE_DEDUPLICATION_LOG}{self._key_separator}{message}"
```

`redis_message_queue/_queue_key_manager.py:76` adds `deduplication_prefix` for
Redis Cluster namespace validation without constructing an empty dedup key.

## Error Message Text

- `get_deduplication_key returned None for message <repr>; the callable must return a non-empty string`
- `get_deduplication_key returned an empty string for message <repr>; the callable must return a non-empty, high-cardinality key`
- `get_deduplication_key must return a str, got <type>`
- Final key-construction guard: `'deduplication_key' must be a non-empty string`

## README Addition Text

`README.md:101`

````markdown
#### Custom dedup key callable must return a non-empty, high-cardinality, tenant-scoped string

When `get_deduplication_key` is a callable, it is called once per publish and
must return a `str` that uniquely represents the deduplication scope for that
message. Returning `None` or `""` raises `ConfigurationError` at publish time;
returning a non-`str` value raises `TypeError`.

Use stable, high-cardinality keys that include any tenant or account boundary
needed by your system:

```python
queue = RedisMessageQueue(
    "orders",
    client=client,
    deduplication=True,
    get_deduplication_key=lambda msg: f"{msg['tenant_id']}:{msg['order_id']}",
)
```

Avoid fallback patterns such as `lambda msg: msg.get("order_id", "")`.
Missing fields should fail loudly instead of collapsing unrelated messages into
one deduplication key.
````

## CHANGELOG v7.0.1 Entry Text

`CHANGELOG.md:3`

```markdown
## v7.0.1

### Bug Fix

- **R8-AD-04:** Custom `get_deduplication_key` callables now fail at
  publish time when they return `None` or `""`, preventing empty dedup keys
  from creating a bare-prefix Redis marker that silently suppresses unrelated
  messages. Non-`str` callable returns continue to raise `TypeError`, now with
  the explicit `get_deduplication_key must return a str, got <type>` message.
```

## Tests

- Added `tests/test_dedup_empty_key.py` covering the AD-02/P2 empty-key repro,
  the `None` callable return, the non-`str` callable return, and the async
  empty-key path.
- Updated existing publish type-validation tests to assert the new
  `ConfigurationError` path for `None` and `""`.

## KNOWN UNKNOWN

`ConfigurationError` is used for `None`/empty callable returns per the
assignment's conservative choice. A dedicated runtime `DeduplicationKeyError`
could distinguish a single bad message from constructor configuration errors,
but was not added to keep the patch narrow.
