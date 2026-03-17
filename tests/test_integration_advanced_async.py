import asyncio

import pytest

from redis_message_queue.asyncio._redis_gateway import RedisGateway
from redis_message_queue.asyncio.redis_message_queue import RedisMessageQueue


def _no_retry(func):
    return func


pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# 1. Multi-Message Expiry Ordering
# ---------------------------------------------------------------------------


class TestMultiMessageExpiryOrdering:
    @pytest.mark.asyncio
    async def test_multiple_expired_reclaimed_oldest_first(self, real_async_redis_client, queue_name):
        """Expired messages are reclaimed in oldest-deadline-first order."""
        gateway = RedisGateway(
            redis_client=real_async_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=1,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway, deduplication=False)
        await queue.publish("msg-a")
        await queue.publish("msg-b")
        await queue.publish("msg-c")

        claims = []
        for _ in range(3):
            claimed = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
            assert claimed is not None
            claims.append(claimed)
            await asyncio.sleep(0.1)  # stagger to ensure distinct deadlines

        await asyncio.sleep(1.5)

        reclaims = []
        for _ in range(3):
            reclaimed = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
            assert reclaimed is not None
            reclaims.append(reclaimed)

        # Reclaim order matches original claim order (oldest deadline first)
        for original, reclaimed in zip(claims, reclaims):
            assert original.stored_message == reclaimed.stored_message

        # Each reclaim gets a new, distinct lease token
        reclaim_tokens = [r.lease_token for r in reclaims]
        assert len(set(reclaim_tokens)) == 3
        for original, reclaimed in zip(claims, reclaims):
            assert original.lease_token != reclaimed.lease_token

    @pytest.mark.asyncio
    async def test_mixed_expired_and_fresh_messages(self, real_async_redis_client, queue_name):
        """Expired reclaims are served before fresh pending messages."""
        gateway = RedisGateway(
            redis_client=real_async_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=1,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway, deduplication=False)
        await queue.publish("msg-a")
        await queue.publish("msg-b")

        claim_a = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        await asyncio.sleep(0.01)
        claim_b = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert claim_a is not None and claim_b is not None

        await asyncio.sleep(1.5)

        # Publish a fresh message while two are expired in processing
        await queue.publish("msg-c")

        # First two claims should be reclaims (oldest deadline first)
        first = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        second = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert first is not None and second is not None
        assert first.stored_message == claim_a.stored_message
        assert second.stored_message == claim_b.stored_message

        # Third claim gets the fresh message from pending
        third = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert third is not None
        assert third.stored_message != claim_a.stored_message
        assert third.stored_message != claim_b.stored_message

    @pytest.mark.asyncio
    async def test_interleaved_publish_and_partial_expiry(self, real_async_redis_client, queue_name):
        """Only expired messages are reclaimed; unexpired ones wait."""
        gateway = RedisGateway(
            redis_client=real_async_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=1,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway, deduplication=False)

        await queue.publish("msg-a")
        claim_a = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert claim_a is not None

        await asyncio.sleep(0.5)

        await queue.publish("msg-b")
        claim_b = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert claim_b is not None

        # msg-a expires ~1.0s after claim, msg-b ~1.5s after start
        await asyncio.sleep(0.8)

        reclaimed = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert reclaimed is not None
        assert reclaimed.stored_message == claim_a.stored_message

        # msg-b should not be reclaimable yet
        nothing = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert nothing is None

        await asyncio.sleep(0.8)

        reclaimed_b = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert reclaimed_b is not None
        assert reclaimed_b.stored_message == claim_b.stored_message


# ---------------------------------------------------------------------------
# 2. Full Lifecycle Pipeline
# ---------------------------------------------------------------------------


class TestFullLifecyclePipeline:
    @pytest.mark.asyncio
    async def test_publish_fail_reclaim_succeed(self, real_async_redis_client, queue_name):
        """A failed message goes to failed; a reclaimed message completes."""
        gateway = RedisGateway(
            redis_client=real_async_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=1,
        )
        queue = RedisMessageQueue(
            queue_name,
            gateway=gateway,
            deduplication=False,
            enable_completed_queue=True,
            enable_failed_queue=True,
        )

        # job-1: process and fail
        await queue.publish("job-1")
        with pytest.raises(RuntimeError):
            async with queue.process_message() as msg:
                assert msg == b"job-1"
                raise RuntimeError("boom")

        # job-2: claim via gateway, let lease expire
        await queue.publish("job-2")
        claimed = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert claimed is not None

        await asyncio.sleep(1.5)

        # Reclaim job-2 via process_message, succeed
        async with queue.process_message() as msg:
            assert msg == b"job-2"

        assert await real_async_redis_client.llen(queue.key.completed) == 1
        assert await real_async_redis_client.llen(queue.key.failed) == 1
        assert await real_async_redis_client.llen(queue.key.pending) == 0
        assert await real_async_redis_client.llen(queue.key.processing) == 0

    @pytest.mark.asyncio
    async def test_multi_message_mixed_outcomes(self, real_async_redis_client, queue_name):
        """5 messages with mixed outcomes: succeed, fail, expire+reclaim."""
        gateway = RedisGateway(
            redis_client=real_async_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=1,
        )
        queue = RedisMessageQueue(
            queue_name,
            gateway=gateway,
            deduplication=False,
            enable_completed_queue=True,
            enable_failed_queue=True,
        )

        for i in range(5):
            await queue.publish(f"msg-{i}")

        # msg-0: succeed
        async with queue.process_message() as msg:
            assert msg == b"msg-0"

        # msg-1: fail
        with pytest.raises(RuntimeError):
            async with queue.process_message() as msg:
                assert msg == b"msg-1"
                raise RuntimeError("fail")

        # msg-2: claim via gateway only, don't complete (let expire)
        claimed_2 = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert claimed_2 is not None

        # msg-3: succeed
        async with queue.process_message() as msg:
            assert msg == b"msg-3"

        # msg-4: fail
        with pytest.raises(RuntimeError):
            async with queue.process_message() as msg:
                assert msg == b"msg-4"
                raise RuntimeError("fail")

        # Wait for msg-2 to expire, then reclaim and succeed
        await asyncio.sleep(1.5)

        async with queue.process_message() as msg:
            assert msg == b"msg-2"

        assert await real_async_redis_client.llen(queue.key.completed) == 3
        assert await real_async_redis_client.llen(queue.key.failed) == 2
        assert await real_async_redis_client.llen(queue.key.pending) == 0
        assert await real_async_redis_client.llen(queue.key.processing) == 0


# ---------------------------------------------------------------------------
# 3. Dedup + Visibility Timeout Interaction
# ---------------------------------------------------------------------------


class TestDedupVisibilityTimeoutInteraction:
    @pytest.mark.asyncio
    async def test_dedup_blocks_republish_after_reclaim(self, real_async_redis_client, queue_name):
        """Reclaiming a message does NOT clear the dedup key."""
        gateway = RedisGateway(
            redis_client=real_async_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=1,
            message_deduplication_log_ttl_seconds=3,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway)

        assert await queue.publish("hello") is True
        claimed = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert claimed is not None

        await asyncio.sleep(1.5)

        # Reclaim the expired message
        reclaimed = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert reclaimed is not None

        # Dedup key still alive (~1.5s into a 3s TTL) — republish should be blocked
        assert await queue.publish("hello") is False

    @pytest.mark.asyncio
    async def test_dedup_ttl_expiry_allows_republish_while_processing(self, real_async_redis_client, queue_name):
        """Dedup key and lease are independent: dedup can expire while lease is active."""
        gateway = RedisGateway(
            redis_client=real_async_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=10,
            message_deduplication_log_ttl_seconds=1,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway)

        assert await queue.publish("hello") is True
        claimed = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert claimed is not None

        # Dedup key expires (1s TTL), but lease still valid (10s)
        await asyncio.sleep(1.5)

        # Republish succeeds because dedup key expired
        assert await queue.publish("hello") is True

        # Both copies exist: new one in pending, original still in processing
        assert await real_async_redis_client.llen(queue.key.pending) == 1
        assert await real_async_redis_client.llen(queue.key.processing) == 1


# ---------------------------------------------------------------------------
# 4. Heartbeat Failure Leads to Reclaim
# ---------------------------------------------------------------------------


class TestHeartbeatFailureLeadsToReclaim:
    @pytest.mark.asyncio
    async def test_stopped_renewal_allows_reclaim(self, real_async_redis_client, queue_name):
        """When manual renewal stops, the message becomes reclaimable."""
        gateway = RedisGateway(
            redis_client=real_async_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=2,
        )
        rival_gateway = RedisGateway(
            redis_client=real_async_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=2,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway)
        await queue.publish("hello")

        claimed = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert claimed is not None

        # Renew twice at 0.5s intervals (extends deadline each time)
        await asyncio.sleep(0.5)
        assert (
            await gateway.renew_message_lease(queue.key.processing, claimed.stored_message, claimed.lease_token) is True
        )
        await asyncio.sleep(0.5)
        assert (
            await gateway.renew_message_lease(queue.key.processing, claimed.stored_message, claimed.lease_token) is True
        )

        # Stop renewing. Last renewal at ~t=1.0, deadline at ~t=3.0.
        await asyncio.sleep(2.5)

        # Rival reclaims the expired message
        reclaimed = await rival_gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert reclaimed is not None
        assert reclaimed.stored_message == claimed.stored_message
        assert reclaimed.lease_token != claimed.lease_token

        # Stale renewal with first token should fail
        assert (
            await gateway.renew_message_lease(queue.key.processing, claimed.stored_message, claimed.lease_token)
            is False
        )

    @pytest.mark.asyncio
    async def test_heartbeat_keeps_message_alive_across_multiple_timeouts(self, real_async_redis_client, queue_name):
        """Automatic heartbeat prevents reclaim across multiple timeout periods."""
        gateway = RedisGateway(
            redis_client=real_async_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=1,
        )
        rival_gateway = RedisGateway(
            redis_client=real_async_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=1,
        )
        queue = RedisMessageQueue(
            queue_name,
            gateway=gateway,
            heartbeat_interval_seconds=0.3,
            enable_completed_queue=True,
        )
        rival_queue = RedisMessageQueue(queue_name, gateway=rival_gateway)

        await queue.publish("hello")

        async with queue.process_message() as msg:
            assert msg == b"hello"
            # Sleep across 3 full timeout periods — heartbeat keeps it alive
            await asyncio.sleep(3)
            rival = await rival_gateway.wait_for_message_and_move(rival_queue.key.pending, rival_queue.key.processing)
            assert rival is None

        assert await real_async_redis_client.llen(queue.key.completed) == 1
        assert await real_async_redis_client.llen(queue.key.processing) == 0


# ---------------------------------------------------------------------------
# 5. Concurrent Consumers with Visibility Timeout
# ---------------------------------------------------------------------------


class TestConcurrentConsumersWithVisibilityTimeout:
    @pytest.mark.asyncio
    async def test_concurrent_process_message_no_double_delivery(self, real_async_redis_client, queue_name):
        """20 coroutines processing 20 messages: no duplicates, all delivered."""
        gateway = RedisGateway(
            redis_client=real_async_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=10,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway, deduplication=False)
        n = 20
        for i in range(n):
            await queue.publish(f"msg-{i}")

        consumed = []

        async def consume():
            async with queue.process_message() as msg:
                if msg is not None:
                    consumed.append(msg)

        await asyncio.gather(*[consume() for _ in range(n)])

        assert len(consumed) == n
        assert len(set(consumed)) == n
        assert await real_async_redis_client.llen(queue.key.processing) == 0

    @pytest.mark.asyncio
    async def test_more_consumers_than_messages_with_visibility_timeout(self, real_async_redis_client, queue_name):
        """10 coroutines for 5 messages: 5 get messages, 5 get None, metadata cleaned."""
        gateway = RedisGateway(
            redis_client=real_async_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=10,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway, deduplication=False)
        for i in range(5):
            await queue.publish(f"msg-{i}")

        consumed = []
        nones = []

        async def consume():
            async with queue.process_message() as msg:
                if msg is not None:
                    consumed.append(msg)
                else:
                    nones.append(None)

        await asyncio.gather(*[consume() for _ in range(10)])

        assert len(consumed) == 5
        assert len(set(consumed)) == 5
        assert len(nones) == 5

        # All lease metadata cleaned up
        lease_deadlines_key = f"{queue.key.processing}:lease_deadlines"
        lease_tokens_key = f"{queue.key.processing}:lease_tokens"
        assert await real_async_redis_client.llen(queue.key.processing) == 0
        assert await real_async_redis_client.zcard(lease_deadlines_key) == 0
        assert await real_async_redis_client.hlen(lease_tokens_key) == 0


# ---------------------------------------------------------------------------
# 6. Renewal/Reclaim Boundary Race
# ---------------------------------------------------------------------------


class TestRenewalReclaimBoundaryRace:
    @pytest.mark.asyncio
    async def test_concurrent_renewal_and_reclaim_at_boundary(self, real_async_redis_client, queue_name):
        """At the expiry boundary, either renewal or reclaim wins — never both."""
        gateway = RedisGateway(
            redis_client=real_async_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=1,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway)
        await queue.publish("hello")

        first = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert first is not None

        # Sleep to just before the expiry boundary
        await asyncio.sleep(0.9)

        async def try_renew():
            return await gateway.renew_message_lease(queue.key.processing, first.stored_message, first.lease_token)

        async def try_claim():
            return await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)

        tasks = []
        for _ in range(5):
            tasks.append(try_renew())
            tasks.append(try_claim())

        results = await asyncio.gather(*tasks)
        renewal_results = [results[i] for i in range(0, 10, 2)]
        claim_results = [results[i] for i in range(1, 10, 2)]

        winners = [r for r in claim_results if r is not None]

        # Exactly one of two outcomes due to Lua atomicity:
        if len(winners) == 0:
            # Renewal won: no one reclaimed, at least one renewal succeeded
            assert any(r is True for r in renewal_results)
        else:
            # Reclaim won: exactly one claimer got it, all renewals failed (stale token)
            assert len(winners) == 1
            assert all(r is False for r in renewal_results)

        # Invariant: exactly 1 message in processing with consistent metadata
        lease_deadlines_key = f"{queue.key.processing}:lease_deadlines"
        lease_tokens_key = f"{queue.key.processing}:lease_tokens"
        assert await real_async_redis_client.llen(queue.key.processing) == 1
        assert await real_async_redis_client.zcard(lease_deadlines_key) == 1
        assert await real_async_redis_client.hlen(lease_tokens_key) == 1


# ---------------------------------------------------------------------------
# 7. Redis TIME Fidelity
# ---------------------------------------------------------------------------


class TestRedisTimeFidelity:
    @pytest.mark.asyncio
    async def test_sequential_claims_get_increasing_deadlines(self, real_async_redis_client, queue_name):
        """Deadlines from sequential claims strictly increase (real Redis TIME)."""
        gateway = RedisGateway(
            redis_client=real_async_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=30,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway, deduplication=False)
        await queue.publish("msg-a")
        await queue.publish("msg-b")

        claim_a = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        await asyncio.sleep(0.1)
        claim_b = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert claim_a is not None and claim_b is not None

        lease_deadlines_key = f"{queue.key.processing}:lease_deadlines"
        deadline_a = await real_async_redis_client.zscore(lease_deadlines_key, claim_a.stored_message)
        deadline_b = await real_async_redis_client.zscore(lease_deadlines_key, claim_b.stored_message)

        assert deadline_b > deadline_a

    @pytest.mark.asyncio
    async def test_renewal_deadline_advances_from_current_time(self, real_async_redis_client, queue_name):
        """Renewal sets deadline to current Redis TIME + visibility_timeout."""
        timeout_seconds = 30
        gateway = RedisGateway(
            redis_client=real_async_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=timeout_seconds,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway)
        await queue.publish("hello")

        claimed = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert claimed is not None

        lease_deadlines_key = f"{queue.key.processing}:lease_deadlines"
        original_deadline = await real_async_redis_client.zscore(lease_deadlines_key, claimed.stored_message)

        await asyncio.sleep(0.2)

        assert (
            await gateway.renew_message_lease(queue.key.processing, claimed.stored_message, claimed.lease_token) is True
        )
        new_deadline = await real_async_redis_client.zscore(lease_deadlines_key, claimed.stored_message)

        assert new_deadline > original_deadline

        # New deadline should be close to redis_server_time + timeout
        redis_time = await real_async_redis_client.time()
        now_ms = redis_time[0] * 1000 + redis_time[1] // 1000
        expected_ms = now_ms + timeout_seconds * 1000
        assert abs(new_deadline - expected_ms) < 500


# ---------------------------------------------------------------------------
# 8. Lease Token Monotonicity
# ---------------------------------------------------------------------------


class TestLeaseTokenMonotonicity:
    @pytest.mark.asyncio
    async def test_tokens_strictly_increasing_across_claims_and_reclaims(self, real_async_redis_client, queue_name):
        """INCR counter produces strictly monotonic tokens across claim/reclaim cycles."""
        gateway = RedisGateway(
            redis_client=real_async_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=1,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway, deduplication=False)
        await queue.publish("msg-a")
        await queue.publish("msg-b")
        await queue.publish("msg-c")

        # Claim all three
        claims = []
        for _ in range(3):
            claimed = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
            assert claimed is not None
            claims.append(claimed)

        t1, t2, t3 = [int(c.lease_token) for c in claims]
        assert t1 < t2 < t3

        # Let all expire
        await asyncio.sleep(1.5)

        # Reclaim all three
        reclaims = []
        for _ in range(3):
            reclaimed = await gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
            assert reclaimed is not None
            reclaims.append(reclaimed)

        t4, t5, t6 = [int(r.lease_token) for r in reclaims]
        assert t3 < t4 < t5 < t6
