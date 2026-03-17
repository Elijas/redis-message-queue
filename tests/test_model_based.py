"""Model-based randomized tests for queue correctness.

Generates arbitrary sequences of queue operations (publish, claim, ack, fail,
lease renewal, lease expiry) against fakeredis, then checks structural
invariants after every step. Each test is parameterized by a seed for
reproducibility.
"""

import random
from collections import Counter
from dataclasses import dataclass, field

import fakeredis
import pytest

from redis_message_queue._redis_gateway import RedisGateway
from redis_message_queue._stored_message import ClaimedMessage
from redis_message_queue.redis_message_queue import RedisMessageQueue


def _no_retry(func):
    return func


# ── State tracker ─────────────────────────────────────────────────


@dataclass
class ProcessingEntry:
    stored_message: bytes
    lease_token: str
    expired: bool = False


@dataclass
class QueueTracker:
    published_count: int = 0
    removed_count: int = 0
    processing: list[ProcessingEntry] = field(default_factory=list)
    stale_tokens: list[str] = field(default_factory=list)
    dedup_keys_used: set[str] = field(default_factory=set)


# ── Invariant checker ─────────────────────────────────────────────


def _check_invariants(client, gateway, queue, tracker, step_desc):
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

    # 2. Lease token hash keys == processing list members
    lease_tokens_key = gateway._lease_tokens_key(queue.key.processing)
    token_hash_members = set(client.hkeys(lease_tokens_key))
    processing_set = set(client.lrange(queue.key.processing, 0, -1))
    assert token_hash_members == processing_set, (
        f"Token hash mismatch: |hash|={len(token_hash_members)}, |list|={len(processing_set)}"
    )

    # 3. Lease deadline members == processing list members
    lease_deadlines_key = gateway._lease_deadlines_key(queue.key.processing)
    deadline_members = set(client.zrange(lease_deadlines_key, 0, -1))
    assert deadline_members == processing_set, (
        f"Deadline mismatch: |deadlines|={len(deadline_members)}, |list|={len(processing_set)}"
    )

    # 4. No duplicate envelopes in pending + processing
    all_active = client.lrange(queue.key.pending, 0, -1) + client.lrange(queue.key.processing, 0, -1)
    if len(all_active) != len(set(all_active)):
        dupes = {k: v for k, v in Counter(all_active).items() if v > 1}
        raise AssertionError(f"Duplicate envelopes: {len(dupes)} duplicated")

    # 5. Tracker processing count matches Redis
    assert len(tracker.processing) == processing_len, (
        f"Tracker processing={len(tracker.processing)} != redis={processing_len}"
    )


# ── Command implementations ──────────────────────────────────────


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
    tracker.published_count += 1
    tracker.dedup_keys_used.add(dedup_redis_key)
    return f"Publish({payload!r})"


def _cmd_publish_no_dedup(rng, client, gateway, queue, tracker, payload_pool_size):
    payload = f"nd-{rng.randint(0, payload_pool_size - 1)}"
    gateway.add_message(queue.key.pending, payload)
    tracker.published_count += 1
    return f"PublishNoDedup({payload!r})"


def _cmd_claim(client, gateway, queue, tracker):
    result = gateway.wait_for_message_and_move(
        queue.key.pending,
        queue.key.processing,
    )
    if result is None:
        return "Claim() -> None"

    assert isinstance(result, ClaimedMessage)
    stored = result.stored_message
    token = result.lease_token

    # Check if this is a reclaim of an expired message
    for entry in tracker.processing:
        if entry.stored_message == stored:
            assert entry.expired, "Reclaimed a non-expired processing message"
            tracker.stale_tokens.append(entry.lease_token)
            entry.lease_token = token
            entry.expired = False
            return f"Claim() -> reclaim token={token}"

    tracker.processing.append(
        ProcessingEntry(stored_message=stored, lease_token=token),
    )
    return f"Claim() -> new token={token}"


def _cmd_ack_success(rng, client, gateway, queue, tracker, enable_completed):
    idx = rng.randint(0, len(tracker.processing) - 1)
    entry = tracker.processing[idx]

    if enable_completed:
        gateway.move_message(
            queue.key.processing,
            queue.key.completed,
            entry.stored_message,
            lease_token=entry.lease_token,
        )
    else:
        gateway.remove_message(
            queue.key.processing,
            entry.stored_message,
            lease_token=entry.lease_token,
        )
        tracker.removed_count += 1

    tracker.stale_tokens.append(entry.lease_token)
    tracker.processing.pop(idx)
    return f"AckSuccess(idx={idx})"


def _cmd_ack_fail(rng, client, gateway, queue, tracker, enable_failed):
    idx = rng.randint(0, len(tracker.processing) - 1)
    entry = tracker.processing[idx]

    if enable_failed:
        gateway.move_message(
            queue.key.processing,
            queue.key.failed,
            entry.stored_message,
            lease_token=entry.lease_token,
        )
    else:
        gateway.remove_message(
            queue.key.processing,
            entry.stored_message,
            lease_token=entry.lease_token,
        )
        tracker.removed_count += 1

    tracker.stale_tokens.append(entry.lease_token)
    tracker.processing.pop(idx)
    return f"AckFail(idx={idx})"


def _cmd_stale_ack(rng, client, gateway, queue, tracker, enable_completed):
    idx = rng.randint(0, len(tracker.processing) - 1)
    entry = tracker.processing[idx]
    stale_token = rng.choice(tracker.stale_tokens)

    processing_before = client.llen(queue.key.processing)
    if enable_completed:
        gateway.move_message(
            queue.key.processing,
            queue.key.completed,
            entry.stored_message,
            lease_token=stale_token,
        )
    else:
        gateway.remove_message(
            queue.key.processing,
            entry.stored_message,
            lease_token=stale_token,
        )
    assert client.llen(queue.key.processing) == processing_before, "Stale ack modified processing"
    return f"StaleAck(idx={idx}, stale={stale_token})"


def _cmd_stale_fail(rng, client, gateway, queue, tracker, enable_failed):
    idx = rng.randint(0, len(tracker.processing) - 1)
    entry = tracker.processing[idx]
    stale_token = rng.choice(tracker.stale_tokens)

    processing_before = client.llen(queue.key.processing)
    if enable_failed:
        gateway.move_message(
            queue.key.processing,
            queue.key.failed,
            entry.stored_message,
            lease_token=stale_token,
        )
    else:
        gateway.remove_message(
            queue.key.processing,
            entry.stored_message,
            lease_token=stale_token,
        )
    assert client.llen(queue.key.processing) == processing_before, "Stale fail modified processing"
    return f"StaleFail(idx={idx}, stale={stale_token})"


def _cmd_expire_lease(rng, client, gateway, queue, tracker):
    non_expired = [(i, e) for i, e in enumerate(tracker.processing) if not e.expired]
    idx, entry = rng.choice(non_expired)

    lease_deadlines_key = gateway._lease_deadlines_key(queue.key.processing)
    client.zadd(lease_deadlines_key, {entry.stored_message: 0})
    entry.expired = True
    return f"ExpireLease(idx={idx})"


def _cmd_renew_lease(rng, client, gateway, queue, tracker):
    idx = rng.randint(0, len(tracker.processing) - 1)
    entry = tracker.processing[idx]

    result = gateway.renew_message_lease(
        queue.key.processing,
        entry.stored_message,
        entry.lease_token,
    )
    assert result is True, f"Valid renew returned {result}"
    entry.expired = False
    return f"RenewLease(idx={idx})"


def _cmd_stale_renew(rng, client, gateway, queue, tracker):
    idx = rng.randint(0, len(tracker.processing) - 1)
    entry = tracker.processing[idx]
    stale_token = rng.choice(tracker.stale_tokens)

    result = gateway.renew_message_lease(
        queue.key.processing,
        entry.stored_message,
        stale_token,
    )
    assert result is False, f"Stale renew returned {result}"
    return f"StaleRenew(idx={idx}, stale={stale_token})"


# ── Command generator & dispatcher ───────────────────────────────


def _pick_command(rng, tracker, expire_weight):
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
        choices.append("renew_lease")
        weights.append(8)
        if any(not e.expired for e in tracker.processing):
            choices.append("expire_lease")
            weights.append(expire_weight)

    # Require stale tokens AND processing entries
    if tracker.stale_tokens and tracker.processing:
        choices.append("stale_ack")
        weights.append(5)
        choices.append("stale_fail")
        weights.append(5)
        choices.append("stale_renew")
        weights.append(5)

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
        return _cmd_publish_no_dedup(
            rng,
            client,
            gateway,
            queue,
            tracker,
            payload_pool_size,
        )
    elif cmd_name == "claim":
        return _cmd_claim(client, gateway, queue, tracker)
    elif cmd_name == "ack_success":
        return _cmd_ack_success(
            rng,
            client,
            gateway,
            queue,
            tracker,
            enable_completed,
        )
    elif cmd_name == "ack_fail":
        return _cmd_ack_fail(
            rng,
            client,
            gateway,
            queue,
            tracker,
            enable_failed,
        )
    elif cmd_name == "stale_ack":
        return _cmd_stale_ack(
            rng,
            client,
            gateway,
            queue,
            tracker,
            enable_completed,
        )
    elif cmd_name == "stale_fail":
        return _cmd_stale_fail(
            rng,
            client,
            gateway,
            queue,
            tracker,
            enable_failed,
        )
    elif cmd_name == "expire_lease":
        return _cmd_expire_lease(rng, client, gateway, queue, tracker)
    elif cmd_name == "renew_lease":
        return _cmd_renew_lease(rng, client, gateway, queue, tracker)
    elif cmd_name == "stale_renew":
        return _cmd_stale_renew(rng, client, gateway, queue, tracker)
    else:
        raise ValueError(f"Unknown command: {cmd_name}")


# ── Main test driver ──────────────────────────────────────────────


def _run_model_test(
    seed,
    n=150,
    *,
    enable_completed=True,
    enable_failed=True,
    payload_pool_size=20,
    expire_weight=10,
):
    rng = random.Random(seed)
    client = fakeredis.FakeRedis()
    gateway = RedisGateway(
        redis_client=client,
        retry_strategy=_no_retry,
        message_wait_interval_seconds=0,
        message_visibility_timeout_seconds=30,
    )
    queue = RedisMessageQueue(
        "test",
        gateway=gateway,
        enable_completed_queue=enable_completed,
        enable_failed_queue=enable_failed,
    )
    tracker = QueueTracker()
    history = []

    for step in range(n):
        cmd_name = _pick_command(rng, tracker, expire_weight)
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
            _check_invariants(client, gateway, queue, tracker, desc)
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


# ── Test parameterization ─────────────────────────────────────────


class TestModelBased:
    @pytest.mark.parametrize("seed", range(50))
    def test_with_completed_and_failed_queues(self, seed):
        _run_model_test(
            seed,
            n=150,
            enable_completed=True,
            enable_failed=True,
        )

    @pytest.mark.parametrize("seed", range(50))
    def test_without_destination_queues(self, seed):
        _run_model_test(
            seed,
            n=150,
            enable_completed=False,
            enable_failed=False,
        )

    @pytest.mark.parametrize("seed", range(20))
    def test_dedup_heavy(self, seed):
        _run_model_test(
            seed,
            n=200,
            enable_completed=True,
            enable_failed=True,
            payload_pool_size=3,
        )

    @pytest.mark.parametrize("seed", range(20))
    def test_expire_heavy(self, seed):
        _run_model_test(
            seed,
            n=200,
            enable_completed=True,
            enable_failed=True,
            expire_weight=40,
        )
