"""Model-based randomized tests for the non-VT code path.

When message_visibility_timeout_seconds=None, the gateway uses plain LMOVE
and direct LREM. These tests verify conservation, FIFO ordering, and the
critical invariant that no lease metadata is ever created.
"""

import fakeredis
import pytest

from tests._model_no_vt import _run_model_test_no_vt


class TestModelNoVt:
    @pytest.mark.parametrize("seed", range(50))
    def test_with_completed_and_failed_queues(self, seed):
        _run_model_test_no_vt(
            seed,
            n=150,
            client=fakeredis.FakeRedis(),
            enable_completed=True,
            enable_failed=True,
        )

    @pytest.mark.parametrize("seed", range(50))
    def test_without_destination_queues(self, seed):
        _run_model_test_no_vt(
            seed,
            n=150,
            client=fakeredis.FakeRedis(),
            enable_completed=False,
            enable_failed=False,
        )

    @pytest.mark.parametrize("seed", range(20))
    def test_dedup_heavy(self, seed):
        _run_model_test_no_vt(
            seed,
            n=200,
            client=fakeredis.FakeRedis(),
            enable_completed=True,
            enable_failed=True,
            payload_pool_size=3,
        )

    @pytest.mark.parametrize("seed", range(30))
    def test_content_audit_heavy(self, seed):
        """Heavy ack/fail with content verification via non-VT invariants 5-7."""
        _run_model_test_no_vt(
            seed,
            n=250,
            client=fakeredis.FakeRedis(),
            enable_completed=True,
            enable_failed=True,
            payload_pool_size=5,
        )

    @pytest.mark.parametrize("seed", range(20))
    def test_dedup_expire_republish(self, seed):
        """Dedup key expiry + re-publish cycle. Small payload pool forces collisions."""
        _run_model_test_no_vt(
            seed,
            n=200,
            client=fakeredis.FakeRedis(),
            enable_completed=True,
            enable_failed=True,
            payload_pool_size=3,
            dedup_expire_weight=20,
        )

    @pytest.mark.parametrize("seed", range(20))
    def test_single_payload_dedup_saturation(self, seed):
        """Single payload pool: every publish after the first hits dedup until key expires."""
        _run_model_test_no_vt(
            seed,
            n=200,
            client=fakeredis.FakeRedis(),
            enable_completed=True,
            enable_failed=True,
            payload_pool_size=1,
            dedup_expire_weight=15,
        )

    @pytest.mark.parametrize("seed", range(20))
    def test_no_dedup_flood(self, seed):
        """High volume publish_no_dedup: many distinct envelopes stress conservation tracking."""
        _run_model_test_no_vt(
            seed,
            n=300,
            client=fakeredis.FakeRedis(),
            enable_completed=True,
            enable_failed=True,
            payload_pool_size=5,
        )


class TestModelNoVtWithDrainEpilogue:
    @pytest.mark.parametrize("seed", range(20))
    def test_drain_after_balanced_run(self, seed):
        _run_model_test_no_vt(
            seed,
            n=200,
            client=fakeredis.FakeRedis(),
            enable_completed=True,
            enable_failed=True,
            drain_epilogue=True,
        )

    @pytest.mark.parametrize("seed", range(20))
    def test_drain_after_dedup_heavy_run(self, seed):
        _run_model_test_no_vt(
            seed,
            n=200,
            client=fakeredis.FakeRedis(),
            enable_completed=True,
            enable_failed=True,
            payload_pool_size=3,
            dedup_expire_weight=20,
            drain_epilogue=True,
        )
