"""Lease / visibility-timeout stress tests.

Stress-tests the batch-reclaim Lua script (CLAIM_MESSAGE_WITH_VISIBILITY_TIMEOUT)
for scalability, fairness, and metadata cleanup correctness under pressure.

Tests cover:
- Mass expiry recovery across batch boundaries (100, 500, 750 messages)
- Fairness ordering between reclaimed and fresh messages
- Poison message isolation behavior
- Metadata cleanup under high-frequency churn
- Multi-consumer drain convergence
"""

import random

import fakeredis
import pytest

from redis_message_queue._redis_gateway import RedisGateway
from redis_message_queue.redis_message_queue import RedisMessageQueue
from tests._model_based import QueueTracker, _check_invariants, _cmd_claim

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _no_retry(func):
    return func


def _make_queue(client, *, queue_name="stress"):
    gateway = RedisGateway(
        redis_client=client,
        retry_strategy=_no_retry,
        message_wait_interval_seconds=0,
        message_visibility_timeout_seconds=30,
    )
    queue = RedisMessageQueue(
        queue_name,
        gateway=gateway,
        deduplication=False,
    )
    return gateway, queue


def _publish_one(client, queue, tracker, payload):
    queue.publish(payload)
    envelope = client.lindex(queue.key.pending, 0)
    tracker.all_published_payloads[envelope] = payload
    tracker.published_count += 1
    return envelope


def _claim_one(client, gateway, queue, tracker):
    _cmd_claim(client, gateway, queue, tracker)
    assert tracker.processing, "Expected a message in processing after claim"
    return tracker.processing[-1]


def _expire_entry(client, gateway, queue, entry):
    lease_deadlines_key = gateway._lease_deadlines_key(queue.key.processing)
    client.zadd(lease_deadlines_key, {entry.stored_message: 0})
    entry.expired = True


def _expire_all(client, gateway, queue, entries):
    """Expire entries with unique, monotonically increasing deadlines.

    Assigning unique scores (0, 1, 2, ...) makes ZRANGEBYSCORE return entries
    in tracker list order. The model's _cmd_claim sorts by (expire_score,
    stored_message) to match Redis's composite ordering.
    """
    lease_deadlines_key = gateway._lease_deadlines_key(queue.key.processing)
    for i, entry in enumerate(entries):
        client.zadd(lease_deadlines_key, {entry.stored_message: i})
        entry.expired = True
        entry.expire_score = i


def _ack_entry(client, gateway, queue, tracker, entry):
    applied = gateway.remove_message(queue.key.processing, entry.stored_message, lease_token=entry.lease_token)
    assert applied is True
    tracker.stale_tokens.append(entry.lease_token)
    tracker.processing.remove(entry)
    tracker.removed_count += 1


# ---------------------------------------------------------------------------
# Test 1: Mass Expiry Recovery
# ---------------------------------------------------------------------------


class TestMassExpiryRecovery:
    """All N expired messages are fully recovered with correct invariants throughout."""

    @pytest.mark.parametrize("n_messages", [100, 500, 750])
    def test_mass_expiry_full_recovery(self, n_messages):
        client = fakeredis.FakeRedis()
        gateway, queue = _make_queue(client)
        tracker = QueueTracker()

        for i in range(n_messages):
            _publish_one(client, queue, tracker, payload=f"msg-{i}")

        for _ in range(n_messages):
            _claim_one(client, gateway, queue, tracker)
        assert len(tracker.processing) == n_messages

        _expire_all(client, gateway, queue, list(tracker.processing))

        recovered = set()
        for step in range(n_messages):
            entry = _claim_one(client, gateway, queue, tracker)
            payload = tracker.all_published_payloads[entry.stored_message]
            recovered.add(payload)
            _ack_entry(client, gateway, queue, tracker, entry)

            if (step + 1) % 50 == 0:
                _check_invariants(client, gateway, queue, tracker, f"recovery step {step + 1}")

        assert len(recovered) == n_messages
        assert recovered == {f"msg-{i}" for i in range(n_messages)}
        assert client.llen(queue.key.pending) == 0
        assert client.llen(queue.key.processing) == 0

        lease_deadlines_key = gateway._lease_deadlines_key(queue.key.processing)
        lease_tokens_key = gateway._lease_tokens_key(queue.key.processing)
        assert client.zcard(lease_deadlines_key) == 0
        assert client.hlen(lease_tokens_key) == 0


# ---------------------------------------------------------------------------
# Test 2: Fairness Under Recovery
# ---------------------------------------------------------------------------


class TestFairnessUnderRecovery:
    """Reclaimed messages get priority, but fresh messages are eventually claimed."""

    def test_reclaimed_before_fresh(self):
        client = fakeredis.FakeRedis()
        gateway, queue = _make_queue(client)
        tracker = QueueTracker()

        for i in range(10):
            _publish_one(client, queue, tracker, payload=f"reclaim-{i}")
        for _ in range(10):
            _claim_one(client, gateway, queue, tracker)
        _expire_all(client, gateway, queue, list(tracker.processing))

        for i in range(5):
            _publish_one(client, queue, tracker, payload=f"fresh-{i}")

        claim_order = []
        for _ in range(15):
            entry = _claim_one(client, gateway, queue, tracker)
            payload = tracker.all_published_payloads[entry.stored_message]
            claim_order.append(payload)
            _ack_entry(client, gateway, queue, tracker, entry)

        reclaimed_payloads = {f"reclaim-{i}" for i in range(10)}
        fresh_payloads = {f"fresh-{i}" for i in range(5)}

        assert set(claim_order[:10]) == reclaimed_payloads
        assert set(claim_order[10:]) == fresh_payloads
        assert len(claim_order) == 15

        # Conservation: all 15 recovered
        assert client.llen(queue.key.pending) == 0
        assert client.llen(queue.key.processing) == 0


# ---------------------------------------------------------------------------
# Test 3: Poison Message Isolation
# ---------------------------------------------------------------------------


class TestPoisonMessageIsolation:
    """Documents poison message behavior under pathological and realistic conditions."""

    def test_instant_expire_documents_reclaim_priority(self):
        """With instant expiry, poison is always reclaimed before fresh messages."""
        client = fakeredis.FakeRedis()
        gateway, queue = _make_queue(client)
        tracker = QueueTracker()

        _publish_one(client, queue, tracker, payload="poison")
        _publish_one(client, queue, tracker, payload="good-1")
        _publish_one(client, queue, tracker, payload="good-2")

        # First claim: LMOVE RIGHT takes "poison" (rightmost, first published)
        entry = _claim_one(client, gateway, queue, tracker)
        assert tracker.all_published_payloads[entry.stored_message] == "poison"

        claims = ["poison"]
        for _ in range(9):
            _expire_entry(client, gateway, queue, entry)
            entry = _claim_one(client, gateway, queue, tracker)
            claims.append(tracker.all_published_payloads[entry.stored_message])

        assert all(c == "poison" for c in claims), f"Expected all poison, got {claims}"
        assert len(claims) == 10

        # Good messages remain in pending throughout — not lost, just delayed
        assert client.llen(queue.key.pending) == 2

    def test_good_messages_progress_between_reclaim_cycles(self):
        """Between each poison reclaim cycle, one good message can be claimed."""
        client = fakeredis.FakeRedis()
        gateway, queue = _make_queue(client)
        tracker = QueueTracker()

        for payload in ["poison", "good-1", "good-2", "good-3"]:
            _publish_one(client, queue, tracker, payload=payload)

        # Claim poison (rightmost = first published)
        poison_entry = _claim_one(client, gateway, queue, tracker)
        assert tracker.all_published_payloads[poison_entry.stored_message] == "poison"

        # Claim good-1 (next rightmost); poison still in processing, not expired
        good1_entry = _claim_one(client, gateway, queue, tracker)
        assert tracker.all_published_payloads[good1_entry.stored_message] == "good-1"
        _ack_to_completed(client, gateway, queue, tracker, good1_entry)

        # Expire poison → reclaim claims poison again
        _expire_entry(client, gateway, queue, poison_entry)
        poison_entry = _claim_one(client, gateway, queue, tracker)
        assert tracker.all_published_payloads[poison_entry.stored_message] == "poison"

        # Claim good-2 between cycles
        good2_entry = _claim_one(client, gateway, queue, tracker)
        assert tracker.all_published_payloads[good2_entry.stored_message] == "good-2"
        _ack_to_completed(client, gateway, queue, tracker, good2_entry)

        # Expire poison again → reclaim
        _expire_entry(client, gateway, queue, poison_entry)
        poison_entry = _claim_one(client, gateway, queue, tracker)
        assert tracker.all_published_payloads[poison_entry.stored_message] == "poison"

        # Claim good-3
        good3_entry = _claim_one(client, gateway, queue, tracker)
        assert tracker.all_published_payloads[good3_entry.stored_message] == "good-3"
        _ack_to_completed(client, gateway, queue, tracker, good3_entry)

        # All good messages reached completed queue
        assert client.llen(queue.key.completed) == 3
        # Only poison remains in processing
        assert len(tracker.processing) == 1
        assert tracker.all_published_payloads[tracker.processing[0].stored_message] == "poison"


def _ack_to_completed(client, gateway, queue, tracker, entry):
    """Move entry from processing to completed queue."""
    applied = gateway.move_message(
        queue.key.processing,
        queue.key.completed,
        entry.stored_message,
        lease_token=entry.lease_token,
    )
    assert applied is True
    payload = tracker.all_published_payloads[entry.stored_message]
    tracker.completed_payloads.insert(0, payload)
    tracker.stale_tokens.append(entry.lease_token)
    tracker.processing.remove(entry)


# ---------------------------------------------------------------------------
# Test 4: Metadata Cleanup Under Churn
# ---------------------------------------------------------------------------


class TestMetadataCleanupUnderChurn:
    """After high-frequency publish/claim/expire/ack cycles, all lease metadata is clean."""

    def test_metadata_clean_after_churn(self):
        client = fakeredis.FakeRedis()
        gateway, queue = _make_queue(client)
        tracker = QueueTracker()
        rng = random.Random(42)

        total_claims = 0
        for step in range(200):
            _publish_one(client, queue, tracker, payload=f"churn-{step}")
            entry = _claim_one(client, gateway, queue, tracker)
            total_claims += 1

            if rng.random() < 0.5:
                _expire_entry(client, gateway, queue, entry)
                entry = _claim_one(client, gateway, queue, tracker)
                total_claims += 1

            _ack_entry(client, gateway, queue, tracker, entry)

            if (step + 1) % 20 == 0:
                _check_invariants(client, gateway, queue, tracker, f"churn step {step + 1}")

        assert client.llen(queue.key.pending) == 0
        assert client.llen(queue.key.processing) == 0

        lease_deadlines_key = gateway._lease_deadlines_key(queue.key.processing)
        lease_tokens_key = gateway._lease_tokens_key(queue.key.processing)
        assert client.zcard(lease_deadlines_key) == 0
        assert client.hlen(lease_tokens_key) == 0

        counter_key = gateway._lease_token_counter_key(queue.key.processing)
        assert int(client.get(counter_key)) == total_claims


# ---------------------------------------------------------------------------
# Test 5: Multi-Consumer Drain Convergence
# ---------------------------------------------------------------------------


class TestMultiConsumerDrainConvergence:
    """Simulated multiple consumers fully drain expired backlog."""

    @pytest.mark.parametrize("n_consumers", [1, 3, 5])
    def test_multi_consumer_drain(self, n_consumers):
        client = fakeredis.FakeRedis()
        gateway, queue = _make_queue(client)
        tracker = QueueTracker()
        n_messages = 200

        for i in range(n_messages):
            _publish_one(client, queue, tracker, payload=f"drain-{i}")

        for _ in range(n_messages):
            _claim_one(client, gateway, queue, tracker)
        assert len(tracker.processing) == n_messages

        _expire_all(client, gateway, queue, list(tracker.processing))

        recovered = set()
        total_polls = 0
        remaining = n_messages
        while remaining > 0:
            for _ in range(min(n_consumers, remaining)):
                entry = _claim_one(client, gateway, queue, tracker)
                payload = tracker.all_published_payloads[entry.stored_message]
                recovered.add(payload)
                _ack_entry(client, gateway, queue, tracker, entry)
                total_polls += 1
                remaining -= 1

        assert len(recovered) == n_messages
        assert recovered == {f"drain-{i}" for i in range(n_messages)}
        assert total_polls == n_messages

        assert client.llen(queue.key.pending) == 0
        assert client.llen(queue.key.processing) == 0

        lease_deadlines_key = gateway._lease_deadlines_key(queue.key.processing)
        lease_tokens_key = gateway._lease_tokens_key(queue.key.processing)
        assert client.zcard(lease_deadlines_key) == 0
        assert client.hlen(lease_tokens_key) == 0


# ---------------------------------------------------------------------------
# Test 6: Sustained Reclaim Backpressure
# ---------------------------------------------------------------------------


class TestSustainedReclaimBackpressure:
    """Documents that fresh messages are delayed (not lost) during sustained poison reclaim."""

    def test_fresh_delayed_during_sustained_reclaim(self):
        """Under sustained poison reclaim, fresh messages wait in pending.

        Scenario: 20 poison messages cycle through reclaim continuously while
        10 fresh messages sit in pending. After poison is drained, all fresh
        messages become available.
        """
        client = fakeredis.FakeRedis()
        gateway, queue = _make_queue(client)
        tracker = QueueTracker()

        # Publish and claim 20 "poison" messages into processing
        poison_entries = []
        for i in range(20):
            _publish_one(client, queue, tracker, payload=f"poison-{i}")
        for _ in range(20):
            poison_entries.append(_claim_one(client, gateway, queue, tracker))

        # Publish 10 "fresh" messages — these sit at LEFT of pending
        for i in range(10):
            _publish_one(client, queue, tracker, payload=f"fresh-{i}")
        assert client.llen(queue.key.pending) == 10

        # Run 10 sustained-reclaim cycles:
        # expire all poison → claim one → verify it's always poison → leave in processing
        #
        # After reclaim, pending holds both reclaimed-but-unclaimed poison messages
        # (RPUSH'd by the Lua script) and the 10 fresh messages. The key invariant
        # is that the LMOVE RIGHT always picks a poison message (rightmost = most
        # recently RPUSH'd reclaim), never a fresh one.
        for cycle in range(10):
            _expire_all(client, gateway, queue, poison_entries)
            entry = _claim_one(client, gateway, queue, tracker)
            payload = tracker.all_published_payloads[entry.stored_message]
            assert payload.startswith("poison-"), f"Cycle {cycle}: expected poison, got {payload!r}"
            # Re-build poison_entries from current tracker state for next expire
            poison_entries = [
                e for e in tracker.processing if tracker.all_published_payloads[e.stored_message].startswith("poison-")
            ]
            _check_invariants(client, gateway, queue, tracker, f"reclaim cycle {cycle}")

        # Ack all poison entries still in processing
        for entry in list(tracker.processing):
            payload = tracker.all_published_payloads[entry.stored_message]
            if payload.startswith("poison-"):
                _ack_entry(client, gateway, queue, tracker, entry)

        # Drain all remaining (unclaimed poison in pending + fresh messages).
        # Poison messages (RPUSH'd by reclaim) sit at RIGHT of pending,
        # fresh messages (LPUSH'd by publish) sit at LEFT. LMOVE RIGHT
        # takes rightmost first, so poison drains before fresh.
        remaining_poison = []
        fresh_claimed = []
        while client.llen(queue.key.pending) > 0:
            entry = _claim_one(client, gateway, queue, tracker)
            payload = tracker.all_published_payloads[entry.stored_message]
            if payload.startswith("poison-"):
                remaining_poison.append(payload)
            else:
                fresh_claimed.append(payload)
            _ack_entry(client, gateway, queue, tracker, entry)

        # All 10 fresh messages must have been recovered
        assert set(fresh_claimed) == {f"fresh-{i}" for i in range(10)}
        # Total accounted: 20 poison + 10 fresh = 30 published
        assert tracker.published_count == 30

        # Terminal: everything drained, metadata clean
        assert client.llen(queue.key.pending) == 0
        assert client.llen(queue.key.processing) == 0
        _check_invariants(client, gateway, queue, tracker, "terminal")


# ---------------------------------------------------------------------------
# Test 7: Lease Metadata Lifecycle
# ---------------------------------------------------------------------------


class TestLeaseMetadataLifecycle:
    """Proves the lease_token_counter is the sole residual key after full drain."""

    def test_counter_is_sole_residual_after_drain(self):
        """After publishing, claiming, expiring, and acking 50 messages,
        the counter key is the only Redis key remaining.
        """
        client = fakeredis.FakeRedis()
        gateway, queue = _make_queue(client)
        tracker = QueueTracker()

        # Publish and claim 50 messages
        entries = []
        for i in range(50):
            _publish_one(client, queue, tracker, payload=f"meta-{i}")
        for _ in range(50):
            entries.append(_claim_one(client, gateway, queue, tracker))

        # Expire first 25, then reclaim+ack all 50
        first_25 = entries[:25]
        _expire_all(client, gateway, queue, first_25)

        # Reclaim the 25 expired (they get new tokens on claim)
        reclaimed = []
        for _ in range(25):
            entry = _claim_one(client, gateway, queue, tracker)
            reclaimed.append(entry)
            _ack_entry(client, gateway, queue, tracker, entry)

        # Ack the remaining 25 that were never expired
        for entry in list(tracker.processing):
            _ack_entry(client, gateway, queue, tracker, entry)

        # Only the counter key should remain
        counter_key = gateway._lease_token_counter_key(queue.key.processing)
        all_keys = [k.decode("utf-8") if isinstance(k, bytes) else k for k in client.keys("*")]
        assert all_keys == [counter_key], f"Expected only counter key, got: {all_keys}"

        # Counter value = 50 initial claims + 25 reclaims = 75
        assert int(client.get(counter_key)) == 75

    def test_no_orphaned_metadata_after_mixed_lifecycle(self):
        """Interleaved publish/claim/expire/ack over 100 messages leaves only
        the counter key after full drain.
        """
        client = fakeredis.FakeRedis()
        gateway, queue = _make_queue(client)
        tracker = QueueTracker()
        rng = random.Random(99)

        total_claims = 0

        # Phase 1: Publish and claim 40, ack 20, expire 20
        for i in range(40):
            _publish_one(client, queue, tracker, payload=f"mixed-{i}")
        for _ in range(40):
            _claim_one(client, gateway, queue, tracker)
            total_claims += 1

        # Ack first 20
        for _ in range(20):
            idx = rng.randint(0, len(tracker.processing) - 1)
            entry = tracker.processing[idx]
            _ack_entry(client, gateway, queue, tracker, entry)

        # Expire remaining 20
        _expire_all(client, gateway, queue, list(tracker.processing))
        _check_invariants(client, gateway, queue, tracker, "phase 1 done")

        # Phase 2: Publish 30 more, reclaim+ack interleaved
        for i in range(30):
            _publish_one(client, queue, tracker, payload=f"mixed-{40 + i}")
            entry = _claim_one(client, gateway, queue, tracker)
            total_claims += 1
            _ack_entry(client, gateway, queue, tracker, entry)

        _check_invariants(client, gateway, queue, tracker, "phase 2 done")

        # Phase 3: Publish 30 more, claim all, expire half, reclaim, drain
        for i in range(30):
            _publish_one(client, queue, tracker, payload=f"mixed-{70 + i}")
        for _ in range(30):
            _claim_one(client, gateway, queue, tracker)
            total_claims += 1

        half = list(tracker.processing)[:15]
        _expire_all(client, gateway, queue, half)

        # Reclaim the expired ones
        for _ in range(15):
            entry = _claim_one(client, gateway, queue, tracker)
            total_claims += 1
            _ack_entry(client, gateway, queue, tracker, entry)

        # Ack everything remaining in processing
        for entry in list(tracker.processing):
            _ack_entry(client, gateway, queue, tracker, entry)

        # Drain any residual pending messages (reclaimed entries that were
        # RPUSH'd to pending by the Lua script but never claimed)
        while client.llen(queue.key.pending) > 0:
            entry = _claim_one(client, gateway, queue, tracker)
            total_claims += 1
            _ack_entry(client, gateway, queue, tracker, entry)

        _check_invariants(client, gateway, queue, tracker, "phase 3 done")

        # Only counter key should remain
        counter_key = gateway._lease_token_counter_key(queue.key.processing)
        all_keys = [k.decode("utf-8") if isinstance(k, bytes) else k for k in client.keys("*")]
        assert all_keys == [counter_key], f"Expected only counter key, got: {all_keys}"
        assert int(client.get(counter_key)) == total_claims


# ---------------------------------------------------------------------------
# Test 8: Large Scale Recovery
# ---------------------------------------------------------------------------


class TestLargeScaleRecovery:
    """Extends mass-expiry testing to 2000 messages (20 batch boundaries)."""

    def test_mass_expiry_2000_messages(self):
        """2000 messages expired and fully recovered, invariants checked every 200 steps."""
        client = fakeredis.FakeRedis()
        gateway, queue = _make_queue(client)
        tracker = QueueTracker()
        n_messages = 2000

        for i in range(n_messages):
            _publish_one(client, queue, tracker, payload=f"large-{i}")

        for _ in range(n_messages):
            _claim_one(client, gateway, queue, tracker)
        assert len(tracker.processing) == n_messages

        _expire_all(client, gateway, queue, list(tracker.processing))

        recovered = set()
        for step in range(n_messages):
            entry = _claim_one(client, gateway, queue, tracker)
            payload = tracker.all_published_payloads[entry.stored_message]
            recovered.add(payload)
            _ack_entry(client, gateway, queue, tracker, entry)

            if (step + 1) % 200 == 0:
                _check_invariants(client, gateway, queue, tracker, f"recovery step {step + 1}")

        assert len(recovered) == n_messages
        assert recovered == {f"large-{i}" for i in range(n_messages)}
        assert client.llen(queue.key.pending) == 0
        assert client.llen(queue.key.processing) == 0

        lease_deadlines_key = gateway._lease_deadlines_key(queue.key.processing)
        lease_tokens_key = gateway._lease_tokens_key(queue.key.processing)
        assert client.zcard(lease_deadlines_key) == 0
        assert client.hlen(lease_tokens_key) == 0

    def test_2000_with_interleaved_fresh(self):
        """2000 expired + 100 fresh: reclaimed messages served first, then fresh."""
        client = fakeredis.FakeRedis()
        gateway, queue = _make_queue(client)
        tracker = QueueTracker()

        # Publish and claim 2000
        for i in range(2000):
            _publish_one(client, queue, tracker, payload=f"expired-{i}")
        for _ in range(2000):
            _claim_one(client, gateway, queue, tracker)

        # Expire all 2000
        _expire_all(client, gateway, queue, list(tracker.processing))

        # Publish 100 fresh (sit at LEFT of pending; reclaimed go to RIGHT)
        for i in range(100):
            _publish_one(client, queue, tracker, payload=f"fresh-{i}")

        # Claim and ack all 2100
        claim_order = []
        for step in range(2100):
            entry = _claim_one(client, gateway, queue, tracker)
            payload = tracker.all_published_payloads[entry.stored_message]
            claim_order.append(payload)
            _ack_entry(client, gateway, queue, tracker, entry)

            if (step + 1) % 300 == 0:
                _check_invariants(client, gateway, queue, tracker, f"step {step + 1}")

        # First ~2000 should be reclaimed (expired-*), last ~100 should be fresh
        reclaimed_payloads = {f"expired-{i}" for i in range(2000)}
        fresh_payloads = {f"fresh-{i}" for i in range(100)}
        assert set(claim_order[:2000]) == reclaimed_payloads
        assert set(claim_order[2000:]) == fresh_payloads

        # Terminal state
        assert client.llen(queue.key.pending) == 0
        assert client.llen(queue.key.processing) == 0
