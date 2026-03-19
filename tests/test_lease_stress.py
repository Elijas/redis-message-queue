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


def _ack_entry(client, gateway, queue, tracker, entry):
    applied = gateway.remove_message(
        queue.key.processing, entry.stored_message, lease_token=entry.lease_token
    )
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

        for entry in tracker.processing:
            _expire_entry(client, gateway, queue, entry)

        recovered = set()
        for step in range(n_messages):
            entry = _claim_one(client, gateway, queue, tracker)
            payload = tracker.all_published_payloads[entry.stored_message]
            recovered.add(payload)
            _ack_entry(client, gateway, queue, tracker, entry)

            if (step + 1) % 50 == 0:
                _check_invariants(
                    client, gateway, queue, tracker, f"recovery step {step + 1}"
                )

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
        for entry in tracker.processing:
            _expire_entry(client, gateway, queue, entry)

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
                _check_invariants(
                    client, gateway, queue, tracker, f"churn step {step + 1}"
                )

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

        for entry in tracker.processing:
            _expire_entry(client, gateway, queue, entry)

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
