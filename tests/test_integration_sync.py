import threading
import time

import pytest

from redis_message_queue._redis_gateway import RedisGateway
from redis_message_queue.redis_message_queue import RedisMessageQueue


def _no_retry(func):
    return func


pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# 3A. Publish Deduplication
# ---------------------------------------------------------------------------


class TestPublishDeduplication:
    def test_dedup_rejects_duplicate(self, real_redis_client, queue_name):
        queue = RedisMessageQueue(queue_name, client=real_redis_client)
        assert queue.publish("hello") is True
        assert queue.publish("hello") is False
        assert real_redis_client.llen(queue.key.pending) == 1

    def test_dedup_key_has_real_ttl(self, real_redis_client, queue_name):
        queue = RedisMessageQueue(queue_name, client=real_redis_client)
        queue.publish("hello")
        dedup_key = queue.key.deduplication("hello")
        ttl = real_redis_client.ttl(dedup_key)
        assert 3500 < ttl <= 3600

    def test_dedup_atomicity_key_and_queue_consistent(self, real_redis_client, queue_name):
        queue = RedisMessageQueue(queue_name, client=real_redis_client)
        queue.publish("hello")
        dedup_key = queue.key.deduplication("hello")
        assert real_redis_client.exists(dedup_key) == 1
        assert real_redis_client.llen(queue.key.pending) == 1

    def test_different_messages_both_enqueued(self, real_redis_client, queue_name):
        queue = RedisMessageQueue(queue_name, client=real_redis_client)
        assert queue.publish("msg-a") is True
        assert queue.publish("msg-b") is True
        assert real_redis_client.llen(queue.key.pending) == 2

    def test_no_dedup_allows_duplicates(self, real_redis_client, queue_name):
        queue = RedisMessageQueue(queue_name, client=real_redis_client, deduplication=False)
        queue.publish("hello")
        queue.publish("hello")
        assert real_redis_client.llen(queue.key.pending) == 2


# ---------------------------------------------------------------------------
# 3B. Queue Ordering (Multiple Producers/Consumers)
# ---------------------------------------------------------------------------


class TestQueueOrdering:
    def test_fifo_ordering_single_producer(self, real_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway, deduplication=False)
        for msg in ["a", "b", "c"]:
            queue.publish(msg)

        consumed = []
        for _ in range(3):
            with queue.process_message() as msg:
                consumed.append(msg)

        assert consumed == [b"a", b"b", b"c"]

    def test_concurrent_publish_all_delivered(self, real_redis_client, queue_name):
        queue = RedisMessageQueue(queue_name, client=real_redis_client, deduplication=False)
        n = 20
        barrier = threading.Barrier(n)

        def publish(i):
            barrier.wait()
            queue.publish(f"msg-{i}")

        threads = [threading.Thread(target=publish, args=(i,)) for i in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert real_redis_client.llen(queue.key.pending) == n

    def test_concurrent_consume_no_double_delivery(self, real_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway, deduplication=False)
        n = 20
        for i in range(n):
            queue.publish(f"msg-{i}")

        consumed = []
        lock = threading.Lock()

        def consume():
            with queue.process_message() as msg:
                if msg is not None:
                    with lock:
                        consumed.append(msg)

        threads = [threading.Thread(target=consume) for _ in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(consumed) == n
        assert len(set(consumed)) == n


# ---------------------------------------------------------------------------
# 3C. Processing -> Completed / Failed Transitions
# ---------------------------------------------------------------------------


class TestProcessingTransitions:
    def test_success_moves_to_completed(self, real_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway, enable_completed_queue=True)
        queue.publish("hello")

        with queue.process_message() as msg:
            assert msg == b"hello"

        assert real_redis_client.llen(queue.key.processing) == 0
        assert real_redis_client.llen(queue.key.completed) == 1
        assert real_redis_client.lpop(queue.key.completed) == b"hello"

    def test_failure_moves_to_failed(self, real_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway, enable_failed_queue=True)
        queue.publish("hello")

        with pytest.raises(ValueError):
            with queue.process_message() as msg:
                assert msg == b"hello"
                raise ValueError("boom")

        assert real_redis_client.llen(queue.key.processing) == 0
        assert real_redis_client.llen(queue.key.failed) == 1
        assert real_redis_client.lpop(queue.key.failed) == b"hello"

    def test_success_without_completed_removes(self, real_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway, enable_completed_queue=False)
        queue.publish("hello")

        with queue.process_message() as msg:
            assert msg == b"hello"

        assert real_redis_client.llen(queue.key.pending) == 0
        assert real_redis_client.llen(queue.key.processing) == 0
        assert real_redis_client.llen(queue.key.completed) == 0

    def test_failure_without_failed_removes(self, real_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway, enable_failed_queue=False)
        queue.publish("hello")

        with pytest.raises(ValueError):
            with queue.process_message() as msg:
                assert msg == b"hello"
                raise ValueError("boom")

        assert real_redis_client.llen(queue.key.pending) == 0
        assert real_redis_client.llen(queue.key.processing) == 0
        assert real_redis_client.llen(queue.key.failed) == 0

    def test_completed_stores_decoded_payload(self, real_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway, enable_completed_queue=True)
        queue.publish("hello")

        with queue.process_message():
            pass

        stored = real_redis_client.lpop(queue.key.completed)
        assert stored == b"hello"
        assert not stored.startswith(b"\x1eRMQ1:")


# ---------------------------------------------------------------------------
# 3D. Visibility-Timeout Reclaim
# ---------------------------------------------------------------------------


class TestVisibilityTimeoutReclaim:
    def test_expired_message_reclaimed(self, real_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=1,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway)
        queue.publish("hello")

        first = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert first is not None

        time.sleep(1.5)

        second = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert second is not None
        assert first.stored_message == second.stored_message
        assert first.lease_token != second.lease_token

    def test_not_reclaimed_before_expiry(self, real_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=1,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway)
        queue.publish("hello")

        first = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert first is not None

        second = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert second is None

    def test_real_time_used_for_lease_deadlines(self, real_redis_client, queue_name):
        timeout_seconds = 2
        gateway = RedisGateway(
            redis_client=real_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=timeout_seconds,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway)
        queue.publish("hello")

        gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)

        processing_key = queue.key.processing
        lease_deadlines_key = f"{processing_key}:lease_deadlines"
        stored_message = real_redis_client.lindex(processing_key, 0)
        deadline_ms = real_redis_client.zscore(lease_deadlines_key, stored_message)

        redis_time = real_redis_client.time()
        now_ms = redis_time[0] * 1000 + redis_time[1] // 1000
        expected_ms = now_ms + timeout_seconds * 1000

        # The deadline was set moments ago, so it should be close to now + timeout.
        # Allow 500ms tolerance for CI jitter.
        assert abs(deadline_ms - expected_ms) < 500


# ---------------------------------------------------------------------------
# 3E. Heartbeat Lease Renewal
# ---------------------------------------------------------------------------


class TestHeartbeatLeaseRenewal:
    def test_heartbeat_prevents_redelivery(self, real_redis_client, queue_name):
        queue_gateway = RedisGateway(
            redis_client=real_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=1,
        )
        rival_gateway = RedisGateway(
            redis_client=real_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=1,
        )
        queue = RedisMessageQueue(
            queue_name,
            gateway=queue_gateway,
            heartbeat_interval_seconds=0.3,
        )
        queue.publish("hello")

        with queue.process_message() as msg:
            assert msg == b"hello"
            time.sleep(2)
            rival = rival_gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
            assert rival is None

    def test_manual_renewal_extends_deadline(self, real_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=2,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway)
        queue.publish("hello")

        claimed = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        lease_deadlines_key = f"{queue.key.processing}:lease_deadlines"
        original_deadline = real_redis_client.zscore(lease_deadlines_key, claimed.stored_message)

        time.sleep(0.1)

        assert gateway.renew_message_lease(queue.key.processing, claimed.stored_message, claimed.lease_token) is True
        new_deadline = real_redis_client.zscore(lease_deadlines_key, claimed.stored_message)
        assert new_deadline > original_deadline

    def test_stale_renewal_rejected_after_redelivery(self, real_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=1,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway)
        queue.publish("hello")

        first = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        time.sleep(1.5)
        second = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert second is not None

        assert gateway.renew_message_lease(queue.key.processing, first.stored_message, first.lease_token) is False
        assert gateway.renew_message_lease(queue.key.processing, second.stored_message, second.lease_token) is True


# ---------------------------------------------------------------------------
# 3F. Stale Worker Ack/Fail Rejection
# ---------------------------------------------------------------------------


class TestStaleWorkerRejection:
    def test_stale_remove_ignored_after_redelivery(self, real_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=1,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway)
        queue.publish("hello")

        first = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        time.sleep(1.5)
        second = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert second is not None

        gateway.remove_message(queue.key.processing, first.stored_message, lease_token=first.lease_token)
        assert real_redis_client.llen(queue.key.processing) == 1

        gateway.remove_message(queue.key.processing, second.stored_message, lease_token=second.lease_token)
        assert real_redis_client.llen(queue.key.processing) == 0

    def test_stale_complete_ignored_after_redelivery(self, real_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=1,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway, enable_completed_queue=True)
        queue.publish("hello")

        first_ctx = queue.process_message()
        first_msg = first_ctx.__enter__()
        assert first_msg == b"hello"

        time.sleep(1.5)

        second_ctx = queue.process_message()
        second_msg = second_ctx.__enter__()
        assert second_msg == b"hello"

        # Old consumer exits -- stale token, should NOT move to completed
        first_ctx.__exit__(None, None, None)
        assert real_redis_client.llen(queue.key.processing) == 1
        assert real_redis_client.llen(queue.key.completed) == 0

        # New consumer exits -- valid token, should move to completed
        second_ctx.__exit__(None, None, None)
        assert real_redis_client.llen(queue.key.processing) == 0
        assert real_redis_client.llen(queue.key.completed) == 1

    def test_stale_fail_ignored_after_redelivery(self, real_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=1,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway, enable_failed_queue=True)
        queue.publish("hello")

        first_ctx = queue.process_message()
        first_msg = first_ctx.__enter__()
        assert first_msg == b"hello"

        time.sleep(1.5)

        second_ctx = queue.process_message()
        second_msg = second_ctx.__enter__()
        assert second_msg == b"hello"

        # Old consumer fails -- stale token, should NOT move to failed
        try:
            first_ctx.__exit__(ValueError, ValueError("old boom"), None)
        except ValueError:
            pass
        assert real_redis_client.llen(queue.key.processing) == 1
        assert real_redis_client.llen(queue.key.failed) == 0

        # New consumer fails -- valid token, should move to failed
        try:
            second_ctx.__exit__(ValueError, ValueError("new boom"), None)
        except ValueError:
            pass
        assert real_redis_client.llen(queue.key.processing) == 0
        assert real_redis_client.llen(queue.key.failed) == 1
