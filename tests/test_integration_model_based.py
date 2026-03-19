"""Model-based randomized tests against real Redis.

Mirrors all scenarios from test_model_based.py but runs against a real Redis
instance. Validates that Lua scripts, TIME-based deadline arithmetic, INCR
monotonicity, and atomic batch reclaim all behave correctly under the real
Redis engine (not fakeredis's Python emulation).
"""

import pytest

from tests._model_based import _run_model_test

pytestmark = pytest.mark.integration


class TestIntegrationModelBased:
    @pytest.mark.parametrize("seed", range(3))
    def test_with_completed_and_failed_queues(self, seed, real_redis_client, queue_name):
        _run_model_test(
            seed,
            n=100,
            client=real_redis_client,
            queue_name=queue_name,
            enable_completed=True,
            enable_failed=True,
        )

    @pytest.mark.parametrize("seed", range(3))
    def test_without_destination_queues(self, seed, real_redis_client, queue_name):
        _run_model_test(
            seed,
            n=100,
            client=real_redis_client,
            queue_name=queue_name,
            enable_completed=False,
            enable_failed=False,
        )

    @pytest.mark.parametrize("seed", range(3))
    def test_dedup_heavy(self, seed, real_redis_client, queue_name):
        _run_model_test(
            seed,
            n=100,
            client=real_redis_client,
            queue_name=queue_name,
            enable_completed=True,
            enable_failed=True,
            payload_pool_size=3,
        )

    @pytest.mark.parametrize("seed", range(3))
    def test_expire_heavy(self, seed, real_redis_client, queue_name):
        _run_model_test(
            seed,
            n=100,
            client=real_redis_client,
            queue_name=queue_name,
            enable_completed=True,
            enable_failed=True,
            expire_weight=40,
        )

    @pytest.mark.parametrize("seed", range(3))
    def test_content_audit_heavy(self, seed, real_redis_client, queue_name):
        """Heavy ack/fail with content verification via invariants 6-8."""
        _run_model_test(
            seed,
            n=100,
            client=real_redis_client,
            queue_name=queue_name,
            enable_completed=True,
            enable_failed=True,
            payload_pool_size=5,
            expire_weight=15,
        )

    @pytest.mark.parametrize("seed", range(3))
    def test_dedup_expire_republish(self, seed, real_redis_client, queue_name):
        """Dedup key expiry + re-publish cycle. Small payload pool forces collisions."""
        _run_model_test(
            seed,
            n=100,
            client=real_redis_client,
            queue_name=queue_name,
            enable_completed=True,
            enable_failed=True,
            payload_pool_size=3,
            dedup_expire_weight=20,
        )

    @pytest.mark.parametrize("seed", range(3))
    def test_rapid_expire_reclaim_cycle(self, seed, real_redis_client, queue_name):
        """Same messages expired and reclaimed repeatedly. Stress-tests token monotonicity."""
        _run_model_test(
            seed,
            n=100,
            client=real_redis_client,
            queue_name=queue_name,
            enable_completed=True,
            enable_failed=True,
            payload_pool_size=5,
            expire_weight=50,
        )

    @pytest.mark.parametrize("seed", range(3))
    def test_single_payload_dedup_saturation(self, seed, real_redis_client, queue_name):
        """Single payload pool: every publish after the first hits dedup until key expires."""
        _run_model_test(
            seed,
            n=100,
            client=real_redis_client,
            queue_name=queue_name,
            enable_completed=True,
            enable_failed=True,
            payload_pool_size=1,
            dedup_expire_weight=15,
        )

    @pytest.mark.parametrize("seed", range(3))
    def test_all_operations_balanced(self, seed, real_redis_client, queue_name):
        """All commands fire at reasonable frequency — no command is starved."""
        _run_model_test(
            seed,
            n=100,
            client=real_redis_client,
            queue_name=queue_name,
            enable_completed=True,
            enable_failed=True,
            payload_pool_size=8,
            expire_weight=15,
            dedup_expire_weight=10,
            expired_ack_weight=10,
            expired_renew_weight=10,
        )
