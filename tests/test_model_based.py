"""Model-based randomized tests for queue correctness.

Generates arbitrary sequences of queue operations (publish, claim, ack, fail,
lease renewal, lease expiry) against fakeredis, then checks structural
invariants after every step. Each test is parameterized by a seed for
reproducibility.
"""

import fakeredis
import pytest

from tests._model_based import _run_model_test


class TestModelBased:
    @pytest.mark.parametrize("seed", range(50))
    def test_with_completed_and_failed_queues(self, seed):
        _run_model_test(
            seed,
            n=150,
            client=fakeredis.FakeRedis(),
            enable_completed=True,
            enable_failed=True,
        )

    @pytest.mark.parametrize("seed", range(50))
    def test_without_destination_queues(self, seed):
        _run_model_test(
            seed,
            n=150,
            client=fakeredis.FakeRedis(),
            enable_completed=False,
            enable_failed=False,
        )

    @pytest.mark.parametrize("seed", range(20))
    def test_dedup_heavy(self, seed):
        _run_model_test(
            seed,
            n=200,
            client=fakeredis.FakeRedis(),
            enable_completed=True,
            enable_failed=True,
            payload_pool_size=3,
        )

    @pytest.mark.parametrize("seed", range(20))
    def test_expire_heavy(self, seed):
        _run_model_test(
            seed,
            n=200,
            client=fakeredis.FakeRedis(),
            enable_completed=True,
            enable_failed=True,
            expire_weight=40,
        )

    @pytest.mark.parametrize("seed", range(30))
    def test_content_audit_heavy(self, seed):
        """Heavy ack/fail with content verification via invariants 6-8."""
        _run_model_test(
            seed,
            n=250,
            client=fakeredis.FakeRedis(),
            enable_completed=True,
            enable_failed=True,
            payload_pool_size=5,
            expire_weight=15,
        )

    @pytest.mark.parametrize("seed", range(20))
    def test_dedup_expire_republish(self, seed):
        """Dedup key expiry + re-publish cycle. Small payload pool forces collisions."""
        _run_model_test(
            seed,
            n=200,
            client=fakeredis.FakeRedis(),
            enable_completed=True,
            enable_failed=True,
            payload_pool_size=3,
            dedup_expire_weight=20,
        )

    @pytest.mark.parametrize("seed", range(20))
    def test_rapid_expire_reclaim_cycle(self, seed):
        """Same messages expired and reclaimed repeatedly. Stress-tests token monotonicity."""
        _run_model_test(
            seed,
            n=300,
            client=fakeredis.FakeRedis(),
            enable_completed=True,
            enable_failed=True,
            payload_pool_size=5,
            expire_weight=50,
        )
