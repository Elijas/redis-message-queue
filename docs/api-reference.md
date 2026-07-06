# API reference

A single-page index of `RedisMessageQueue`'s constructor parameters, public
methods/properties, and exported exceptions/types. This page is deliberately
terse — it links to [configuration.md](configuration.md) for the runnable
snippets and full tradeoff discussion of each feature.

The sync (`redis_message_queue`) and async (`redis_message_queue.asyncio`)
queues share the same constructor signature and almost all method names.
Divergences are called out explicitly below.

## Constructor: `RedisMessageQueue(name, *, ...)`

`name` is positional and required: the queue's base name, used to derive all
Redis keys (see [`queue.key` accessor family](#queuekey-accessor-family)
below).

| Parameter | Type | Default | Meaning | Docs |
|---|---|---|---|---|
| `gateway` | `AbstractRedisGateway \| None` | `None` | Use a custom/tuned gateway instead of the built-in `client=` path; mutually exclusive with `client`, `interrupt`, and most queue-level defaults below | [Custom gateway](configuration.md#custom-gateway) |
| `client` | `redis.Redis` (sync) / `redis.asyncio.Redis` (async) `\| None` | `None` | The Redis client backing the built-in gateway; required unless `gateway` is given | [Configuration](../README.md#configuration) |
| `deduplication` | `bool` | `False` | Enable publish-side deduplication; requires `get_deduplication_key` | [Deduplication](configuration.md#deduplication) |
| `enable_completed_queue` | `bool` | `False` | Keep an audit list of successfully processed messages | [Success and failure tracking](configuration.md#success-and-failure-tracking) |
| `enable_failed_queue` | `bool` | `False` | Move messages whose handler raised into a `failed` list instead of discarding them | [Success and failure tracking](configuration.md#success-and-failure-tracking) |
| `strict_envelope_decoding` | `bool` | `False` | Fail-fast on non-rmq payloads when this Redis is shared with sibling task libraries | [Graceful shutdown](configuration.md#graceful-shutdown) |
| `visibility_timeout_seconds` | `int \| None` | `300` | Lease duration for crash recovery; `None` (with `max_delivery_count=None`) disables lease-based reclaim | [Crash recovery with visibility timeout](configuration.md#crash-recovery-with-visibility-timeout) |
| `heartbeat_interval_seconds` | `int \| float \| None` | `None` | Background lease renewal interval for long-running handlers; must be `< visibility_timeout_seconds / 2` | [Crash recovery with visibility timeout](configuration.md#crash-recovery-with-visibility-timeout) |
| `max_completed_length` | `int \| None` | `1000` | Cap on the completed list length; requires `enable_completed_queue=True` to override | [Success and failure tracking](configuration.md#success-and-failure-tracking) |
| `max_failed_length` | `int \| None` | `1000` | Cap on the failed list length; requires `enable_failed_queue=True` to override | [Success and failure tracking](configuration.md#success-and-failure-tracking) |
| `max_delivery_count` | `int \| None` | `10` | Redeliveries allowed before a message is routed to the auto-derived dead-letter queue; `None` disables dead-lettering; cannot be combined with `gateway` | [Dead-letter queue](configuration.md#dead-letter-queue) |
| `max_pending_length` | `int \| None` | `None` | Cap on pending-list depth during publish; unbounded by default; cannot be combined with `gateway` | [Publish backpressure](configuration.md#publish-backpressure) |
| `pending_overload_policy` | `Literal["raise", "drop_oldest", "block"]` | `"raise"` | What `publish()` does when the pending list is full | [Publish backpressure](configuration.md#publish-backpressure) |
| `pending_overload_block_timeout_seconds` | `float` | `1.0` | How long `"block"` waits for capacity before raising `QueueBackpressureError`; `0` is a single immediate check | [Publish backpressure](configuration.md#publish-backpressure) |
| `key_separator` | `str` | `"::"` | Separator used in generated Redis key names; rmq has no fixed library prefix | [Configuration](../README.md#configuration) |
| `get_deduplication_key` | `Callable[[MessagePayload], str]` (sync) / `Callable[[MessagePayload], str \| Awaitable[str]]` (async) `\| None` | `None` | Derives the dedup key from a message; required when `deduplication=True` | [Deduplication](configuration.md#deduplication) |
| `strict_payload_types` | `bool` | `False` | Reject Python-only/lossy JSON types (tuples, sets, bytes, datetimes, ...) in dict payloads before publish | [Payload validation and limits](configuration.md#payload-validation-and-limits) |
| `max_payload_bytes` | `int \| None` | `None` | Reject serialized payloads larger than this many bytes; unbounded by default | [Payload validation and limits](configuration.md#payload-validation-and-limits) |
| `max_payload_depth` | `int \| None` | `None` | Reject dict/list payloads nested deeper than this; unbounded by default | [Payload validation and limits](configuration.md#payload-validation-and-limits) |
| `interrupt` | `BaseGracefulInterruptHandler \| None` | `None` | Handler for prompt Ctrl-C / termination handling in polling waits; only valid on the `client=` path | [Graceful shutdown](configuration.md#graceful-shutdown) |
| `on_heartbeat_failure` | `Callable[[], None]` (sync) / `Callable[[], Awaitable[None] \| None]` (async) `\| None` | `None` | Zero-arg callback invoked when lease renewal fails; requires `heartbeat_interval_seconds`; must not block in the async queue | [Crash recovery with visibility timeout](configuration.md#crash-recovery-with-visibility-timeout) |
| `on_event` | `Callable[[QueueEvent], None]` (sync) / `Callable[[QueueEvent], Awaitable[None]]` (async) `\| None` | `None` | Best-effort telemetry callback for lifecycle events; never influences ack/nack outcomes | [Observability](observability.md) |

**Divergences between sync and async constructors:**
- `client` type differs (`redis.Redis` vs `redis.asyncio.Redis`), as does `gateway` (sync vs async `AbstractRedisGateway`).
- `get_deduplication_key`, `on_heartbeat_failure`, and `on_event` accept plain sync callables on both queues, but the async queue also accepts (and the sync queue rejects with `TypeError`) an async callable / one returning an awaitable.
- `interrupt` is the same type (`BaseGracefulInterruptHandler`) on both.

## Public methods and properties

| Sync | Async | Behavior | Docs |
|---|---|---|---|
| `publish(message: MessagePayload) -> bool` | `async publish(message) -> bool` | Enqueue a `str` or `dict` payload; returns `True` unless deduplication skipped a duplicate | [Deduplication](configuration.md#deduplication) |
| `process_message() -> ContextManager[MessageData \| None]` | `process_message() -> AsyncContextManager[MessageData \| None]` | Claim and process one message as a `with`/`async with` block; yields `None` when nothing is available or the queue is draining; an exception raised inside the block is terminal (no requeue) | [Graceful shutdown](configuration.md#graceful-shutdown) |
| `process_message_callback(handler) -> bool` | `async process_message_callback(handler) -> bool` | Callback-shaped sibling of `process_message()`; returns `False` when no message was claimed, `True` after the handler ran and the message was acked. The sync queue raises `TypeError` if the handler returns an awaitable instead of acking; the async queue awaits an awaitable handler result and also accepts a plain sync handler | [Callback-style consuming](configuration.md#callback-style-consuming) |
| `drain(timeout: float \| None = None) -> bool` | `async drain(timeout=None) -> bool` (alias of `aclose`) | Stop accepting new claims/publishes and recover in-flight claim ids; returns `True` if recovery completed (or nothing was pending) | [Graceful shutdown](configuration.md#graceful-shutdown) |
| `close(timeout=None) -> bool` (alias of `drain`) | `async aclose(timeout=None) -> bool` | Same semantics as `drain()`, named to match the convention of the underlying client (`close` sync, `aclose` async) | [Graceful shutdown](configuration.md#graceful-shutdown) |
| `is_draining -> bool` (property) | `is_draining -> bool` (property) | `True` once `drain()`/`close()`/`aclose()` has set the drain flag, even if pending-claim recovery is still running | [Graceful shutdown](configuration.md#graceful-shutdown) |
| `is_drained -> bool` (property) | `is_drained -> bool` (property) | `True` once the publish lock has committed the drain flag, guaranteeing no `publish()` is still mid-flight; does not imply recovery succeeded | [Graceful shutdown](configuration.md#graceful-shutdown) |
| `stats() -> QueueStats` | `async stats() -> QueueStats` | Best-effort snapshot of `pending`/`processing`/`completed`/`failed`/`dead_letter` list depths; `None` for disabled features; requires the built-in gateway or a custom gateway implementing the operator methods | [Operations](operations.md) |
| `peek(count: int = 1, *, source: str = "pending") -> list[MessageData]` | `async peek(count=1, *, source="pending") -> list[MessageData]` | Read up to `count` messages from `source` (`"pending"`, `"processing"`, `"completed"`, `"failed"`, `"dead_letter"`) without consuming them | [Operations](operations.md) |
| `redrive_dead_letters(max_messages: int \| None = None) -> int` | `async redrive_dead_letters(max_messages=None) -> int` | Move dead-lettered messages back to pending (resetting delivery count) and return how many moved; requires a configured dead-letter queue | [Dead-letter queue](configuration.md#dead-letter-queue) |
| `purge(*, target: str) -> int` | `async purge(*, target: str) -> int` | Delete every message in `target` (`"pending"`, `"completed"`, `"failed"`, `"dead_letter"`; `"processing"` is rejected) and return how many were removed; destructive and irreversible | [Operations](operations.md) |
| `key` (attribute, `QueueKeyManager`) | `key` (attribute, `QueueKeyManager`) | See [`queue.key` accessor family](#queuekey-accessor-family) below | — |

Both queues also support `repr(queue)`, which reports the queue name and
whether it has been drained.

### `queue.key` accessor family

`queue.key` is a `QueueKeyManager` exposing the Redis key names this queue
instance reads and writes, all derived from `name` and `key_separator`:

| Accessor | Returns |
|---|---|
| `key.pending` | Redis key of the pending list |
| `key.processing` | Redis key of the processing list |
| `key.completed` | Redis key of the completed list (used only when `enable_completed_queue=True`) |
| `key.failed` | Redis key of the failed list (used only when `enable_failed_queue=True`) |
| `key.dead_letter` | Redis key of the **auto-derived default** dead-letter queue (`f"{name}{key_separator}dlq"`). If a custom `dead_letter_queue=` was configured on the gateway, that name is used instead and is available via the gateway's `dead_letter_queue` attribute, not this accessor |
| `key.deduplication(message: str) -> str` | Redis key for a given deduplication key value |
| `key.deduplication_prefix` | Common prefix of all deduplication keys for this queue |

## Exported exceptions and types

From `redis_message_queue` (and mirrored by `redis_message_queue.asyncio`,
which re-exports the same exception/type classes):

| Name | One-liner |
|---|---|
| `RedisMessageQueueError` | Base class for all redis-message-queue-specific errors; carries `.queue`, `.message_id`, `.operation` context |
| `ConfigurationError` (also a `ValueError`) | Bad parameter values or combinations |
| `GatewayContractError` (also a `TypeError`) | A custom gateway returned the wrong type or violated its contract |
| `LuaScriptError` (also a `redis.exceptions.ResponseError`) | A Lua script returned an unexpected `error_reply` |
| `ClaimStoreFailedError` | The visibility-timeout claim Lua script's `store_claim_and_return` call failed |
| `CleanupFailedError` | Cleanup (ack/nack) after handler completion failed |
| `DrainFailedError` | Wraps a non-rmq exception caught during `drain()`/`aclose()` pending-claim recovery |
| `MalformedStoredMessageError` | The stored value is not a valid rmq envelope for the configured decode mode |
| `PayloadTooLargeError` (also a `ValueError`) | Publish payload exceeds the configured `max_payload_bytes` limit |
| `PayloadTooDeepError` (also a `ValueError`) | Publish payload exceeds the configured `max_payload_depth` limit |
| `QueueBackpressureError` | Publish rejected because the pending queue is at its configured `max_pending_length` |
| `QueueDrainedError` | `publish()` was called after `drain()`/`close()`/`aclose()` |
| `RetryBudgetExhaustedError` (also a `redis.exceptions.RedisError`) | The gateway's tenacity retry budget was exhausted; the underlying redis-py exception is `.__cause__` |

Also exported (not exceptions):

| Name | One-liner |
|---|---|
| `RedisMessageQueue` | The queue class documented above |
| `RedisGateway` | The built-in gateway used by the `client=` constructor path |
| `AbstractRedisGateway` | Base class for writing a custom gateway |
| `ClaimedMessage` | Stored-message-plus-lease-token wrapper returned by lease-aware gateways |
| `MessageData` | Type alias for the raw claimed message (`str` or `bytes`, depending on client `decode_responses`) |
| `MessagePayload` | Type alias for a publishable message (`str` or `dict`) |
| `QueueStats` | Return type of `stats()` |
| `QueueEvent` | Lifecycle event object passed to `on_event` |
| `EventOperation` | Enum of `QueueEvent.operation` values (e.g. `publish`, `claim`, `drain`) |
| `EventOutcome` | Enum of `QueueEvent.outcome` values (e.g. `success`, `failure`, `skipped`) |
| `BaseGracefulInterruptHandler` | Base class for the `interrupt=` protocol |
| `GracefulInterruptHandler` | Built-in signal-based interrupt handler |
| `EventDrivenInterruptHandler` | Built-in `threading.Event`/`asyncio.Event`-based interrupt handler |
