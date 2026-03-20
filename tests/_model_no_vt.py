"""Shared model-based testing helpers for the non-VT code path.

When message_visibility_timeout_seconds=None, the gateway uses plain LMOVE,
MOVE_MESSAGE_LUA_SCRIPT, and direct LREM — entirely different code from the
visibility-timeout path. This module provides a state tracker, invariant
checker, and command generators for testing this code path.
"""

import random
from collections import Counter
from dataclasses import dataclass, field

from redis_message_queue._redis_gateway import RedisGateway
from redis_message_queue._stored_message import ClaimedMessage
from redis_message_queue.redis_message_queue import RedisMessageQueue


def _no_retry(func):
    return func


# -- State tracker -----------------------------------------------------


@dataclass
class NoVtProcessingEntry:
    stored_message: bytes
    payload: str


@dataclass
class NoVtQueueTracker:
    published_count: int = 0
    removed_count: int = 0
    processing: list[NoVtProcessingEntry] = field(default_factory=list)
    dedup_keys_used: set[str] = field(default_factory=set)
    completed_payloads: list[str] = field(default_factory=list)
    failed_payloads: list[str] = field(default_factory=list)
    all_published_payloads: dict[bytes, str] = field(default_factory=dict)


# -- Invariant checker --------------------------------------------------


def _check_no_vt_invariants(client, gateway, queue, tracker, step_desc):
    pending_len = client.llen(queue.key.pending)
    processing_len = client.llen(queue.key.processing)
    completed_len = client.llen(queue.key.completed)
    failed_len = client.llen(queue.key.failed)

    # 1. Conservation: total messages across all queues = published - removed
    total = pending_len + processing_len + completed_len + failed_len
    expected = tracker.published_count - tracker.removed_count
    assert total == expected, (
        f"Conservation: P={pending_len}+Pr={processing_len}"
        f"+C={completed_len}+F={failed_len}={total} "
        f"!= pub({tracker.published_count})-rem({tracker.removed_count})={expected}"
    )

    # 2. No duplicates in pending + processing
    all_active = client.lrange(queue.key.pending, 0, -1) + client.lrange(queue.key.processing, 0, -1)
    if len(all_active) != len(set(all_active)):
        dupes = {k: v for k, v in Counter(all_active).items() if v > 1}
        raise AssertionError(f"Duplicate envelopes: {len(dupes)} duplicated")

    # 3. Tracker processing count matches Redis
    assert len(tracker.processing) == processing_len, (
        f"Tracker processing={len(tracker.processing)} != redis={processing_len}"
    )

    # 4. No lease metadata exists (verifies non-VT path never creates VT metadata)
    lease_tokens_key = gateway._lease_tokens_key(queue.key.processing)
    lease_deadlines_key = gateway._lease_deadlines_key(queue.key.processing)
    counter_key = gateway._lease_token_counter_key(queue.key.processing)
    assert client.hlen(lease_tokens_key) == 0, (
        f"lease_tokens HASH should not exist in non-VT mode, has {client.hlen(lease_tokens_key)} entries"
    )
    assert client.zcard(lease_deadlines_key) == 0, (
        f"lease_deadlines ZSET should not exist in non-VT mode, has {client.zcard(lease_deadlines_key)} members"
    )
    counter_raw = client.get(counter_key)
    assert counter_raw is None, f"lease_token_counter should not exist in non-VT mode, has value {counter_raw!r}"

    # 5. Completed content exact match
    redis_completed = client.lrange(queue.key.completed, 0, -1)
    expected_completed = [p.encode("utf-8") for p in tracker.completed_payloads]
    assert redis_completed == expected_completed, (
        f"Completed mismatch: redis={redis_completed!r} vs expected={expected_completed!r}"
    )

    # 6. Failed content exact match
    redis_failed = client.lrange(queue.key.failed, 0, -1)
    expected_failed = [p.encode("utf-8") for p in tracker.failed_payloads]
    assert redis_failed == expected_failed, f"Failed mismatch: redis={redis_failed!r} vs expected={expected_failed!r}"

    # 7. All terminal payloads are known published payloads
    known_payloads = set(tracker.all_published_payloads.values())
    for item in redis_completed:
        payload = item.decode("utf-8")
        assert payload in known_payloads, f"Completed payload {payload!r} not in known published payloads"
    for item in redis_failed:
        payload = item.decode("utf-8")
        assert payload in known_payloads, f"Failed payload {payload!r} not in known published payloads"

    # 8. Every pending member is a known published envelope
    pending_members = client.lrange(queue.key.pending, 0, -1)
    for member in pending_members:
        assert member in tracker.all_published_payloads, f"Pending member {member!r} not in known published envelopes"

    # 9. Every processing member is a known published envelope
    processing_members = client.lrange(queue.key.processing, 0, -1)
    for member in processing_members:
        assert member in tracker.all_published_payloads, (
            f"Processing member {member!r} not in known published envelopes"
        )


# -- Command implementations -------------------------------------------


def _cmd_publish(rng, client, queue, tracker, payload_pool_size):
    payload = f"msg-{rng.randint(0, payload_pool_size - 1)}"
    dedup_redis_key = queue.key.deduplication(payload)
    is_duplicate = dedup_redis_key in tracker.dedup_keys_used

    pending_before = client.llen(queue.key.pending)
    accepted = queue.publish(payload)

    if is_duplicate:
        assert not accepted, f"Duplicate {payload!r} was accepted"
        assert client.llen(queue.key.pending) == pending_before
        return f"DuplicatePublish({payload!r})"

    assert accepted, f"Fresh {payload!r} was rejected"
    envelope = client.lindex(queue.key.pending, 0)
    tracker.all_published_payloads[envelope] = payload
    tracker.published_count += 1
    tracker.dedup_keys_used.add(dedup_redis_key)
    return f"Publish({payload!r})"


def _cmd_publish_no_dedup(rng, client, gateway, queue, tracker, payload_pool_size):
    payload = f"nd-{rng.randint(0, payload_pool_size - 1)}"
    gateway.add_message(queue.key.pending, payload)
    envelope = client.lindex(queue.key.pending, 0)
    tracker.all_published_payloads[envelope] = payload
    tracker.published_count += 1
    return f"PublishNoDedup({payload!r})"


def _cmd_claim(client, gateway, queue, tracker):
    # FIFO check: snapshot the rightmost pending element (oldest) before claim
    expected_next = client.lindex(queue.key.pending, -1)

    result = gateway.wait_for_message_and_move(
        queue.key.pending,
        queue.key.processing,
    )

    if result is None:
        assert expected_next is None, f"Pending had element {expected_next!r} but claim returned None"
        return "Claim() -> None"

    # Non-VT path must NOT return ClaimedMessage
    assert not isinstance(result, ClaimedMessage), "Non-VT claim returned ClaimedMessage, expected bytes"

    # FIFO: claimed message must be the rightmost pending element
    assert result == expected_next, f"FIFO violation: expected {expected_next!r}, got {result!r}"

    payload = tracker.all_published_payloads[result]
    tracker.processing.append(
        NoVtProcessingEntry(stored_message=result, payload=payload),
    )
    return f"Claim() -> {payload!r}"


def _cmd_ack_success(rng, client, gateway, queue, tracker, enable_completed):
    idx = rng.randint(0, len(tracker.processing) - 1)
    entry = tracker.processing[idx]

    if enable_completed:
        applied = gateway.move_message(
            queue.key.processing,
            queue.key.completed,
            entry.stored_message,
        )
        assert applied is True, f"AckSuccess move_message returned {applied!r}, expected True"
        tracker.completed_payloads.insert(0, entry.payload)
    else:
        applied = gateway.remove_message(
            queue.key.processing,
            entry.stored_message,
        )
        assert applied is True, f"AckSuccess remove_message returned {applied!r}, expected True"
        tracker.removed_count += 1

    tracker.processing.pop(idx)
    return f"AckSuccess(idx={idx})"


def _cmd_ack_fail(rng, client, gateway, queue, tracker, enable_failed):
    idx = rng.randint(0, len(tracker.processing) - 1)
    entry = tracker.processing[idx]

    if enable_failed:
        applied = gateway.move_message(
            queue.key.processing,
            queue.key.failed,
            entry.stored_message,
        )
        assert applied is True, f"AckFail move_message returned {applied!r}, expected True"
        tracker.failed_payloads.insert(0, entry.payload)
    else:
        applied = gateway.remove_message(
            queue.key.processing,
            entry.stored_message,
        )
        assert applied is True, f"AckFail remove_message returned {applied!r}, expected True"
        tracker.removed_count += 1

    tracker.processing.pop(idx)
    return f"AckFail(idx={idx})"


def _cmd_double_ack(rng, client, gateway, queue, tracker, enable_completed):
    idx = rng.randint(0, len(tracker.processing) - 1)
    entry = tracker.processing[idx]

    # First ack — should succeed
    if enable_completed:
        first_result = gateway.move_message(
            queue.key.processing,
            queue.key.completed,
            entry.stored_message,
        )
        assert first_result is True, f"DoubleAck first move_message returned {first_result!r}, expected True"
        tracker.completed_payloads.insert(0, entry.payload)
    else:
        first_result = gateway.remove_message(
            queue.key.processing,
            entry.stored_message,
        )
        assert first_result is True, f"DoubleAck first remove_message returned {first_result!r}, expected True"
        tracker.removed_count += 1

    tracker.processing.pop(idx)

    # Second ack with same message — should be rejected (LREM finds nothing)
    if enable_completed:
        second_result = gateway.move_message(
            queue.key.processing,
            queue.key.completed,
            entry.stored_message,
        )
    else:
        second_result = gateway.remove_message(
            queue.key.processing,
            entry.stored_message,
        )
    assert second_result is False, f"DoubleAck second ack returned {second_result!r}, expected False"

    return f"DoubleAck(idx={idx})"


def _cmd_expire_dedup_key(rng, client, queue, tracker):
    dedup_key = rng.choice(list(tracker.dedup_keys_used))
    client.delete(dedup_key)
    tracker.dedup_keys_used.discard(dedup_key)
    return f"ExpireDedupKey({dedup_key!r})"


# -- Command generator & dispatcher ------------------------------------


def _pick_command(rng, tracker, dedup_expire_weight):
    choices = []
    weights = []

    # Always available
    choices.append("publish")
    weights.append(30)
    choices.append("publish_no_dedup")
    weights.append(15)
    choices.append("claim")
    weights.append(25)

    # Require processing entries
    if tracker.processing:
        choices.append("ack_success")
        weights.append(15)
        choices.append("ack_fail")
        weights.append(10)
        choices.append("double_ack")
        weights.append(3)

    # Require dedup keys
    if tracker.dedup_keys_used:
        choices.append("expire_dedup_key")
        weights.append(dedup_expire_weight)

    return rng.choices(choices, weights=weights, k=1)[0]


def _execute_command(
    cmd_name,
    rng,
    client,
    gateway,
    queue,
    tracker,
    *,
    enable_completed,
    enable_failed,
    payload_pool_size,
):
    if cmd_name == "publish":
        return _cmd_publish(rng, client, queue, tracker, payload_pool_size)
    elif cmd_name == "publish_no_dedup":
        return _cmd_publish_no_dedup(rng, client, gateway, queue, tracker, payload_pool_size)
    elif cmd_name == "claim":
        return _cmd_claim(client, gateway, queue, tracker)
    elif cmd_name == "ack_success":
        return _cmd_ack_success(rng, client, gateway, queue, tracker, enable_completed)
    elif cmd_name == "ack_fail":
        return _cmd_ack_fail(rng, client, gateway, queue, tracker, enable_failed)
    elif cmd_name == "double_ack":
        return _cmd_double_ack(rng, client, gateway, queue, tracker, enable_completed)
    elif cmd_name == "expire_dedup_key":
        return _cmd_expire_dedup_key(rng, client, queue, tracker)
    else:
        raise ValueError(f"Unknown command: {cmd_name}")


# -- Drain epilogue -----------------------------------------------------


def _drain_epilogue_no_vt(client, gateway, queue, tracker, enable_completed, enable_failed):
    """Drain all remaining messages after a randomized run and verify clean state.

    Acks all processing entries, then claims and acks all pending entries.
    Asserts that the queue is fully empty with no orphaned metadata.
    """
    # Ack all current processing entries
    while tracker.processing:
        entry = tracker.processing.pop(0)
        if enable_completed:
            applied = gateway.move_message(
                queue.key.processing,
                queue.key.completed,
                entry.stored_message,
            )
            assert applied is True, f"Drain ack returned {applied!r}"
            tracker.completed_payloads.insert(0, entry.payload)
        else:
            applied = gateway.remove_message(
                queue.key.processing,
                entry.stored_message,
            )
            assert applied is True, f"Drain remove returned {applied!r}"
            tracker.removed_count += 1

    # Claim and ack all remaining pending messages
    safety_limit = tracker.published_count - tracker.removed_count + 10
    drained = 0
    while drained < safety_limit:
        result = gateway.wait_for_message_and_move(
            queue.key.pending,
            queue.key.processing,
        )
        if result is None:
            break

        assert not isinstance(result, ClaimedMessage)
        payload = tracker.all_published_payloads[result]

        if enable_completed:
            applied = gateway.move_message(
                queue.key.processing,
                queue.key.completed,
                result,
            )
            assert applied is True, f"Drain ack returned {applied!r}"
            tracker.completed_payloads.insert(0, payload)
        else:
            applied = gateway.remove_message(
                queue.key.processing,
                result,
            )
            assert applied is True, f"Drain remove returned {applied!r}"
            tracker.removed_count += 1
        drained += 1

    # Verify completely empty state
    assert client.llen(queue.key.pending) == 0, "Drain: pending not empty"
    assert client.llen(queue.key.processing) == 0, "Drain: processing not empty"

    # No lease metadata
    lease_tokens_key = gateway._lease_tokens_key(queue.key.processing)
    lease_deadlines_key = gateway._lease_deadlines_key(queue.key.processing)
    counter_key = gateway._lease_token_counter_key(queue.key.processing)
    assert client.hlen(lease_tokens_key) == 0, "Drain: lease_tokens HASH not empty"
    assert client.zcard(lease_deadlines_key) == 0, "Drain: lease_deadlines ZSET not empty"
    assert client.get(counter_key) is None, "Drain: lease_token_counter exists"

    # Conservation: completed + failed + removed == published
    completed_len = client.llen(queue.key.completed) if enable_completed else 0
    failed_len = client.llen(queue.key.failed) if enable_failed else 0
    terminal_total = completed_len + failed_len + tracker.removed_count
    assert terminal_total == tracker.published_count, (
        f"Drain conservation: completed({completed_len}) + failed({failed_len}) "
        f"+ removed({tracker.removed_count}) = {terminal_total} "
        f"!= published({tracker.published_count})"
    )


# -- Main test driver ---------------------------------------------------


def _run_model_test_no_vt(
    seed,
    n=150,
    *,
    client,
    queue_name="test-no-vt",
    enable_completed=True,
    enable_failed=True,
    payload_pool_size=20,
    dedup_expire_weight=5,
    drain_epilogue=False,
):
    rng = random.Random(seed)
    gateway = RedisGateway(
        redis_client=client,
        retry_strategy=_no_retry,
        message_wait_interval_seconds=0,
        message_visibility_timeout_seconds=None,
    )
    queue = RedisMessageQueue(
        queue_name,
        gateway=gateway,
        enable_completed_queue=enable_completed,
        enable_failed_queue=enable_failed,
    )
    tracker = NoVtQueueTracker()
    history = []

    for step in range(n):
        cmd_name = _pick_command(rng, tracker, dedup_expire_weight)
        desc = _execute_command(
            cmd_name,
            rng,
            client,
            gateway,
            queue,
            tracker,
            enable_completed=enable_completed,
            enable_failed=enable_failed,
            payload_pool_size=payload_pool_size,
        )
        history.append(desc)

        try:
            _check_no_vt_invariants(client, gateway, queue, tracker, desc)
        except AssertionError as exc:
            lines = [
                f"Invariant violation at step {step} (seed={seed})",
                f"Command: {desc}",
                f"Error: {exc}",
                "",
                f"Full history ({len(history)} commands):",
            ]
            for i, h in enumerate(history):
                marker = " >>>" if i == step else "    "
                lines.append(f"{marker} {i:4d}: {h}")
            raise AssertionError("\n".join(lines)) from None

    if drain_epilogue:
        _drain_epilogue_no_vt(client, gateway, queue, tracker, enable_completed, enable_failed)
