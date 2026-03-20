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

    @pytest.mark.parametrize("seed", range(30))
    def test_return_value_heavy(self, seed):
        """Exercises return value assertions under randomized conditions.

        Small payload pool forces reuse; elevated expire weight creates stale
        tokens; double_ack (weight=3) fires ~5-6 times per run.
        """
        _run_model_test(
            seed,
            n=200,
            client=fakeredis.FakeRedis(),
            enable_completed=True,
            enable_failed=True,
            payload_pool_size=5,
            expire_weight=15,
            expired_ack_weight=10,
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

    @pytest.mark.parametrize("seed", range(30))
    def test_expired_ack_and_renew_race(self, seed):
        """Ack/fail/renew an expired-but-not-yet-reclaimed message. Exercises the race window."""
        _run_model_test(
            seed,
            n=200,
            client=fakeredis.FakeRedis(),
            enable_completed=True,
            enable_failed=True,
            expired_ack_weight=15,
            expired_renew_weight=10,
        )

    @pytest.mark.parametrize("seed", range(20))
    def test_single_payload_dedup_saturation(self, seed):
        """Single payload pool: every publish after the first hits dedup until key expires."""
        _run_model_test(
            seed,
            n=200,
            client=fakeredis.FakeRedis(),
            enable_completed=True,
            enable_failed=True,
            payload_pool_size=1,
            dedup_expire_weight=15,
        )

    @pytest.mark.parametrize("seed", range(20))
    def test_dedup_expiry_with_heavy_reclaim(self, seed):
        """Multiple envelopes for the same payload via dedup expiry + aggressive lease cycling."""
        _run_model_test(
            seed,
            n=250,
            client=fakeredis.FakeRedis(),
            enable_completed=True,
            enable_failed=True,
            payload_pool_size=3,
            expire_weight=35,
            dedup_expire_weight=20,
            expired_ack_weight=10,
        )

    @pytest.mark.parametrize("seed", range(20))
    def test_no_dedup_flood(self, seed):
        """High volume publish_no_dedup: many distinct envelopes stress conservation tracking."""
        _run_model_test(
            seed,
            n=300,
            client=fakeredis.FakeRedis(),
            enable_completed=True,
            enable_failed=True,
            payload_pool_size=5,
            expire_weight=20,
            expired_ack_weight=10,
        )

    @pytest.mark.parametrize("seed", range(30))
    def test_all_operations_balanced(self, seed):
        """All commands fire at reasonable frequency — no command is starved."""
        _run_model_test(
            seed,
            n=300,
            client=fakeredis.FakeRedis(),
            enable_completed=True,
            enable_failed=True,
            payload_pool_size=8,
            expire_weight=15,
            dedup_expire_weight=10,
            expired_ack_weight=10,
            expired_renew_weight=10,
        )


class TestModelBasedWithDrainEpilogue:
    """Run randomized sequences then drain all messages and verify clean state.

    After N random steps, the drain epilogue processes every remaining message
    and asserts: pending=0, processing=0, all lease metadata empty, and
    completed + failed + removed == published.
    """

    @pytest.mark.parametrize("seed", range(20))
    def test_drain_after_balanced_run(self, seed):
        _run_model_test(
            seed,
            n=200,
            client=fakeredis.FakeRedis(),
            enable_completed=True,
            enable_failed=True,
            payload_pool_size=8,
            expire_weight=15,
            dedup_expire_weight=10,
            expired_ack_weight=10,
            expired_renew_weight=10,
            drain_epilogue=True,
        )

    @pytest.mark.parametrize("seed", range(20))
    def test_drain_after_expire_heavy_run(self, seed):
        _run_model_test(
            seed,
            n=200,
            client=fakeredis.FakeRedis(),
            enable_completed=True,
            enable_failed=True,
            payload_pool_size=5,
            expire_weight=40,
            expired_ack_weight=15,
            drain_epilogue=True,
        )
