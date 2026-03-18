"""Targeted scenario tests and randomized expired-entry interaction tests.

Exercises specific edge-case paths not covered by the randomized model tests:
- Ack/fail/renew on expired-but-not-yet-reclaimed messages
- Dedup independence from redelivery
- Multi-reclaim token monotonicity
- process_message context manager success/failure paths

Also includes a sequence minimizer for debugging randomized test failures.
"""

import random

import fakeredis
import pytest

from redis_message_queue._redis_gateway import RedisGateway
from redis_message_queue._stored_message import decode_stored_message
from redis_message_queue.redis_message_queue import RedisMessageQueue
from tests._model_based import (
    QueueTracker,
    _check_invariants,
    _cmd_claim,
    _cmd_expire_dedup_key,
    _run_model_test,
)


def _no_retry(func):
    return func


def _make_queue(client, *, enable_completed=True, enable_failed=True, queue_name="scenario"):
    gateway = RedisGateway(
        redis_client=client,
        retry_strategy=_no_retry,
        message_wait_interval_seconds=0,
        message_visibility_timeout_seconds=30,
    )
    queue = RedisMessageQueue(
        queue_name,
        gateway=gateway,
        enable_completed_queue=enable_completed,
        enable_failed_queue=enable_failed,
    )
    return gateway, queue


def _publish_one(client, queue, tracker, payload="test-msg"):
    """Publish a single message and update tracker. Returns the stored envelope."""
    accepted = queue.publish(payload)
    assert accepted
    envelope = client.lindex(queue.key.pending, 0)
    tracker.all_published_payloads[envelope] = payload
    tracker.published_count += 1
    dedup_key = queue.key.deduplication(payload)
    tracker.dedup_keys_used.add(dedup_key)
    return envelope


def _claim_one(client, gateway, queue, tracker):
    """Claim one message from pending into processing. Returns the ProcessingEntry."""
    _cmd_claim(client, gateway, queue, tracker)
    assert tracker.processing, "Expected a message in processing after claim"
    return tracker.processing[-1]


def _expire_entry(client, gateway, queue, entry):
    """Force-expire a processing entry by setting its deadline to 0."""
    lease_deadlines_key = gateway._lease_deadlines_key(queue.key.processing)
    client.zadd(lease_deadlines_key, {entry.stored_message: 0})
    entry.expired = True


# ---------------------------------------------------------------------------
# Targeted deterministic scenario tests
# ---------------------------------------------------------------------------


class TestTargetedScenarios:

    def test_multi_reclaim_cycle(self):
        """publish -> claim -> expire -> claim(reclaim) -> expire -> claim(reclaim).

        Verifies token monotonicity across multiple reclaims and that stale
        tokens accumulate correctly.
        """
        client = fakeredis.FakeRedis()
        gateway, queue = _make_queue(client)
        tracker = QueueTracker()

        _publish_one(client, queue, tracker)
        _check_invariants(client, gateway, queue, tracker, "after publish")

        entry1 = _claim_one(client, gateway, queue, tracker)
        token1 = entry1.lease_token
        _check_invariants(client, gateway, queue, tracker, "after claim 1")

        # Expire and reclaim
        _expire_entry(client, gateway, queue, entry1)
        entry2 = _claim_one(client, gateway, queue, tracker)
        token2 = entry2.lease_token
        assert int(token2) > int(token1), "Token must increase after reclaim"
        assert token1 in tracker.stale_tokens
        _check_invariants(client, gateway, queue, tracker, "after reclaim 1")

        # Expire and reclaim again
        _expire_entry(client, gateway, queue, entry2)
        entry3 = _claim_one(client, gateway, queue, tracker)
        token3 = entry3.lease_token
        assert int(token3) > int(token2), "Token must increase after second reclaim"
        assert token2 in tracker.stale_tokens
        _check_invariants(client, gateway, queue, tracker, "after reclaim 2")

    def test_ack_before_reclaim(self):
        """publish -> claim(T1) -> expire -> ack(T1) -> claim -> None.

        A valid token should still work after expiry (before reclaim).
        The acked message should not be reclaimable.
        """
        client = fakeredis.FakeRedis()
        gateway, queue = _make_queue(client)
        tracker = QueueTracker()

        _publish_one(client, queue, tracker)
        entry = _claim_one(client, gateway, queue, tracker)

        # Expire but don't reclaim yet
        _expire_entry(client, gateway, queue, entry)
        _check_invariants(client, gateway, queue, tracker, "after expire")

        # Ack with the valid token — should succeed
        gateway.move_message(
            queue.key.processing,
            queue.key.completed,
            entry.stored_message,
            lease_token=entry.lease_token,
        )
        payload = tracker.all_published_payloads[entry.stored_message]
        tracker.completed_payloads.insert(0, payload)
        tracker.stale_tokens.append(entry.lease_token)
        tracker.processing.remove(entry)
        _check_invariants(client, gateway, queue, tracker, "after ack expired")

        # Claim again — nothing should be available
        result = gateway.wait_for_message_and_move(
            queue.key.pending, queue.key.processing,
        )
        assert result is None, "Acked message should not be reclaimable"

    def test_renew_prevents_reclaim(self):
        """publish -> claim(T1) -> expire -> renew(T1) -> claim -> None.

        Renewal resets the deadline; the message stays in processing and
        is not reclaimable.
        """
        client = fakeredis.FakeRedis()
        gateway, queue = _make_queue(client)
        tracker = QueueTracker()

        _publish_one(client, queue, tracker)
        entry = _claim_one(client, gateway, queue, tracker)

        # Expire then renew
        _expire_entry(client, gateway, queue, entry)
        result = gateway.renew_message_lease(
            queue.key.processing,
            entry.stored_message,
            entry.lease_token,
        )
        assert result is True, "Renewal of expired-but-valid lease should succeed"
        entry.expired = False
        _check_invariants(client, gateway, queue, tracker, "after renew")

        # Claim again — nothing pending, and the renewed message shouldn't reclaim
        result = gateway.wait_for_message_and_move(
            queue.key.pending, queue.key.processing,
        )
        assert result is None, "Renewed message should not be reclaimable"

    def test_dedup_independent_from_reclaim(self):
        """publish("x") -> claim -> expire -> claim(reclaim "x").

        Dedup key is still active but must not block reclaim of the same payload.
        """
        client = fakeredis.FakeRedis()
        gateway, queue = _make_queue(client)
        tracker = QueueTracker()

        _publish_one(client, queue, tracker, payload="x")
        entry = _claim_one(client, gateway, queue, tracker)
        _check_invariants(client, gateway, queue, tracker, "after claim")

        # Dedup key for "x" is still active
        dedup_key = queue.key.deduplication("x")
        assert client.exists(dedup_key), "Dedup key should still exist"

        # Expire and reclaim — dedup should NOT block this
        _expire_entry(client, gateway, queue, entry)
        reclaimed_entry = _claim_one(client, gateway, queue, tracker)
        assert reclaimed_entry.stored_message == entry.stored_message
        _check_invariants(client, gateway, queue, tracker, "after dedup-independent reclaim")

    def test_stale_after_multi_reclaim(self):
        """publish -> claim(T1) -> expire -> claim(T2) -> stale_ack(T1) -> ack(T2).

        Only the winning lease holder (T2) should be able to move the message.
        Stale token T1 should be a no-op.
        """
        client = fakeredis.FakeRedis()
        gateway, queue = _make_queue(client)
        tracker = QueueTracker()

        _publish_one(client, queue, tracker)
        entry1 = _claim_one(client, gateway, queue, tracker)
        token1 = entry1.lease_token

        # Expire and reclaim
        _expire_entry(client, gateway, queue, entry1)
        entry2 = _claim_one(client, gateway, queue, tracker)
        token2 = entry2.lease_token

        # Stale ack with T1 — should be a no-op
        processing_before = client.llen(queue.key.processing)
        completed_before = client.llen(queue.key.completed)
        gateway.move_message(
            queue.key.processing,
            queue.key.completed,
            entry2.stored_message,
            lease_token=token1,
        )
        assert client.llen(queue.key.processing) == processing_before
        assert client.llen(queue.key.completed) == completed_before

        # Valid ack with T2 — should succeed
        gateway.move_message(
            queue.key.processing,
            queue.key.completed,
            entry2.stored_message,
            lease_token=token2,
        )
        payload = tracker.all_published_payloads[entry2.stored_message]
        tracker.completed_payloads.insert(0, payload)
        tracker.stale_tokens.append(token2)
        tracker.processing.remove(entry2)
        _check_invariants(client, gateway, queue, tracker, "after valid ack")

    def test_dedup_expiry_creates_second_copy(self):
        """publish("x") -> claim -> expire_dedup("x") -> publish("x") -> claim.

        Conservation holds with two envelopes for the same payload.
        """
        client = fakeredis.FakeRedis()
        gateway, queue = _make_queue(client)
        tracker = QueueTracker()
        rng = random.Random(42)

        _publish_one(client, queue, tracker, payload="x")
        entry1 = _claim_one(client, gateway, queue, tracker)
        _check_invariants(client, gateway, queue, tracker, "after first claim")

        # Expire the dedup key
        _cmd_expire_dedup_key(rng, client, queue, tracker)

        # Publish "x" again — should be accepted as a new envelope
        _publish_one(client, queue, tracker, payload="x")
        _check_invariants(client, gateway, queue, tracker, "after second publish")

        # Claim the second copy
        entry2 = _claim_one(client, gateway, queue, tracker)
        assert entry2.stored_message != entry1.stored_message, "Should be a different envelope"
        _check_invariants(client, gateway, queue, tracker, "after second claim")

    def test_process_message_with_exception(self):
        """publish -> process_message raising ValueError.

        Message should land in failed queue; completed should be empty.
        """
        client = fakeredis.FakeRedis()
        gateway, queue = _make_queue(client, enable_completed=True, enable_failed=True)

        queue.publish("fail-me")

        with pytest.raises(ValueError, match="intentional"):
            with queue.process_message() as msg:
                assert msg is not None
                raise ValueError("intentional error")

        assert client.llen(queue.key.processing) == 0
        assert client.llen(queue.key.completed) == 0
        assert client.llen(queue.key.failed) == 1
        failed_payload = decode_stored_message(client.lindex(queue.key.failed, 0))
        if isinstance(failed_payload, bytes):
            failed_payload = failed_payload.decode("utf-8")
        assert failed_payload == "fail-me"

    def test_process_message_success(self):
        """publish -> process_message success.

        Message should land in completed queue.
        """
        client = fakeredis.FakeRedis()
        gateway, queue = _make_queue(client, enable_completed=True, enable_failed=True)

        queue.publish("succeed-me")

        with queue.process_message() as msg:
            assert msg is not None

        assert client.llen(queue.key.processing) == 0
        assert client.llen(queue.key.failed) == 0
        assert client.llen(queue.key.completed) == 1
        completed_payload = decode_stored_message(client.lindex(queue.key.completed, 0))
        if isinstance(completed_payload, bytes):
            completed_payload = completed_payload.decode("utf-8")
        assert completed_payload == "succeed-me"


# ---------------------------------------------------------------------------
# Randomized tests with elevated expired-entry weights
# ---------------------------------------------------------------------------


class TestExpiredEntryInteractions:

    @pytest.mark.parametrize("seed", range(30))
    def test_expired_ack_heavy(self, seed):
        _run_model_test(
            seed,
            n=200,
            client=fakeredis.FakeRedis(),
            enable_completed=True,
            enable_failed=True,
            expire_weight=30,
            expired_ack_weight=20,
        )

    @pytest.mark.parametrize("seed", range(30))
    def test_renew_expired_heavy(self, seed):
        _run_model_test(
            seed,
            n=200,
            client=fakeredis.FakeRedis(),
            enable_completed=True,
            enable_failed=True,
            expire_weight=30,
            expired_renew_weight=20,
        )

    @pytest.mark.parametrize("seed", range(20))
    def test_mixed_expired_interactions(self, seed):
        _run_model_test(
            seed,
            n=300,
            client=fakeredis.FakeRedis(),
            enable_completed=True,
            enable_failed=True,
            expire_weight=35,
            expired_ack_weight=15,
            expired_renew_weight=15,
        )


# ---------------------------------------------------------------------------
# Sequence minimizer for debugging randomized test failures
# ---------------------------------------------------------------------------


def _run_model_test_recorded(
    seed,
    n=150,
    *,
    client_factory,
    queue_name="test",
    enable_completed=True,
    enable_failed=True,
    payload_pool_size=20,
    expire_weight=10,
    dedup_expire_weight=5,
    expired_ack_weight=0,
    expired_renew_weight=0,
):
    """Run a model test and record (step_index, rng_state) for minimization.

    Returns (history, rng_states, error_or_None).
    """
    from tests._model_based import (
        _check_invariants,
        _execute_command,
        _pick_command,
    )

    rng = random.Random(seed)
    client = client_factory()
    gateway = RedisGateway(
        redis_client=client,
        retry_strategy=_no_retry,
        message_wait_interval_seconds=0,
        message_visibility_timeout_seconds=30,
    )
    queue = RedisMessageQueue(
        queue_name,
        gateway=gateway,
        enable_completed_queue=enable_completed,
        enable_failed_queue=enable_failed,
    )
    tracker = QueueTracker()
    history = []
    rng_states = []

    for step in range(n):
        rng_states.append(rng.getstate())
        cmd_name = _pick_command(
            rng,
            tracker,
            expire_weight,
            dedup_expire_weight,
            expired_ack_weight,
            expired_renew_weight,
        )
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
        except AssertionError:
            return history, rng_states, step

    return history, rng_states, None


def _replay_subset(
    steps_to_include,
    rng_states,
    n,
    *,
    seed,
    client_factory,
    queue_name="test",
    enable_completed=True,
    enable_failed=True,
    payload_pool_size=20,
    expire_weight=10,
    dedup_expire_weight=5,
    expired_ack_weight=0,
    expired_renew_weight=0,
):
    """Replay only the steps in steps_to_include. Returns True if it fails."""
    from tests._model_based import (
        _check_invariants,
        _execute_command,
        _pick_command,
    )

    rng = random.Random(seed)
    client = client_factory()
    gateway = RedisGateway(
        redis_client=client,
        retry_strategy=_no_retry,
        message_wait_interval_seconds=0,
        message_visibility_timeout_seconds=30,
    )
    queue = RedisMessageQueue(
        queue_name,
        gateway=gateway,
        enable_completed_queue=enable_completed,
        enable_failed_queue=enable_failed,
    )
    tracker = QueueTracker()

    for step in range(n):
        rng.setstate(rng_states[step])
        cmd_name = _pick_command(
            rng,
            tracker,
            expire_weight,
            dedup_expire_weight,
            expired_ack_weight,
            expired_renew_weight,
        )

        if step not in steps_to_include:
            continue

        try:
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
        except (AssertionError, IndexError, KeyError):
            # Command itself failed due to missing preconditions from skipped steps
            return False

        try:
            _check_invariants(client, gateway, queue, tracker, desc)
        except AssertionError:
            return True

    return False


def _minimize_sequence(
    failing_step,
    rng_states,
    **replay_kwargs,
):
    """Delta-debugging minimizer: removes chunks then individual steps.

    Returns the minimal set of step indices that reproduce the failure.
    """
    n = failing_step + 1
    all_steps = set(range(n))

    # Phase 1: remove chunks, halving
    chunk_size = max(1, n // 2)
    while chunk_size >= 1:
        steps = sorted(all_steps)
        i = 0
        while i < len(steps):
            chunk = set(steps[i : i + chunk_size])
            candidate = all_steps - chunk
            if candidate and _replay_subset(
                candidate, rng_states, n, **replay_kwargs
            ):
                all_steps = candidate
                steps = sorted(all_steps)
                # Don't advance i — the list shifted
            else:
                i += chunk_size
        chunk_size //= 2

    return sorted(all_steps)


def minimize_failing_test(
    seed,
    n=150,
    *,
    client_factory=fakeredis.FakeRedis,
    **kwargs,
):
    """Run a model test; if it fails, minimize and return the minimal sequence.

    Returns (minimal_steps, full_history) or None if the test passes.
    """
    history, rng_states, failing_step = _run_model_test_recorded(
        seed, n, client_factory=client_factory, **kwargs,
    )
    if failing_step is None:
        return None

    replay_kwargs = dict(
        seed=seed,
        client_factory=client_factory,
        **kwargs,
    )
    minimal_steps = _minimize_sequence(
        failing_step, rng_states, **replay_kwargs,
    )
    return minimal_steps, [history[i] for i in minimal_steps]
