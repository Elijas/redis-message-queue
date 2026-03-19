"""Integration tests for blocking BLMOVE and VT polling loop paths against real Redis.

These tests target the real Redis code paths that are *not* exercised by the
existing integration tests (which all use ``message_wait_interval_seconds=0``,
triggering non-blocking ``LMOVE``).

fakeredis vs Real Redis behavioral gaps covered:

1. BLMOVE blocking: fakeredis returns immediately from blmove(); real Redis
   blocks the connection until data arrives or timeout expires.
2. Redis TIME in Lua: fakeredis emulates redis.call('TIME') via Python's
   time.time(); real Redis uses the server's clock, making lease deadlines
   immune to client clock skew.
3. EVAL return types: Real Redis returns Lua integers as int and Lua strings
   as bytes. fakeredis matches simple cases but may differ for nested returns
   (the claim script returns {stored_message, lease_token} as a list).
"""

import json
import threading
import time
from uuid import uuid4

import pytest
import redis

from redis_message_queue._redis_gateway import RedisGateway
from redis_message_queue.redis_message_queue import RedisMessageQueue


def _no_retry(func):
    return func


def _cleanup_keys(redis_client, prefix):
    cursor = 0
    while True:
        cursor, keys = redis_client.scan(cursor=cursor, match=f"{prefix}*", count=100)
        if keys:
            redis_client.delete(*keys)
        if cursor == 0:
            break


pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Blocking BLMOVE path (_redis_gateway.py:210-216)
# ---------------------------------------------------------------------------


class TestBlmoveBlockingPath:
    """Tests for the blmove() branch: message_wait_interval > 0, no visibility timeout."""

    def test_blmove_returns_message_when_already_available(self, real_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=1,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway, deduplication=False)
        queue.publish("hello")

        start = time.monotonic()
        result = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        elapsed = time.monotonic() - start

        assert result is not None
        assert elapsed < 0.5

    def test_blmove_blocks_then_receives_late_publish(self, real_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=3,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway, deduplication=False)

        result = [None]

        def consumer():
            result[0] = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)

        t = threading.Thread(target=consumer)
        start = time.monotonic()
        t.start()
        time.sleep(0.5)
        queue.publish("late-msg")
        t.join(timeout=5)
        elapsed = time.monotonic() - start

        assert result[0] is not None
        assert elapsed < 2.0

    def test_blmove_returns_none_on_timeout(self, real_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=1,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway)

        start = time.monotonic()
        result = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        elapsed = time.monotonic() - start

        assert result is None
        assert elapsed >= 0.8

    def test_blmove_fifo_ordering_preserved(self, real_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=1,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway, deduplication=False)
        for msg in ["a", "b", "c"]:
            queue.publish(msg)

        consumed = []
        for _ in range(3):
            with queue.process_message() as msg:
                consumed.append(msg)

        assert consumed == [b"a", b"b", b"c"]

    def test_blmove_multiple_consumers_no_double_delivery(self, real_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=1,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway, deduplication=False)
        n = 10
        for i in range(n):
            queue.publish(f"msg-{i}")

        results = []
        lock = threading.Lock()
        barrier = threading.Barrier(n)

        def consumer():
            barrier.wait()
            r = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
            if r is not None:
                with lock:
                    results.append(r)

        threads = [threading.Thread(target=consumer) for _ in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)

        assert len(results) == n
        assert len(set(results)) == n


# ---------------------------------------------------------------------------
# Visibility-timeout polling loop (_redis_gateway.py:218-237)
# ---------------------------------------------------------------------------


class TestVisibilityTimeoutPollingLoop:
    """Tests for the 0.25s poll loop: message_wait_interval > 0 AND visibility_timeout > 0."""

    def test_vt_polling_returns_message_when_available(self, real_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=1,
            message_visibility_timeout_seconds=2,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway)
        queue.publish("hello")

        start = time.monotonic()
        result = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        elapsed = time.monotonic() - start

        assert result is not None
        assert elapsed < 0.5

    def test_vt_polling_detects_late_publish(self, real_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=3,
            message_visibility_timeout_seconds=2,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway, deduplication=False)

        result = [None]

        def consumer():
            result[0] = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)

        t = threading.Thread(target=consumer)
        start = time.monotonic()
        t.start()
        time.sleep(0.5)
        queue.publish("late-msg")
        t.join(timeout=5)
        elapsed = time.monotonic() - start

        assert result[0] is not None
        assert elapsed < 2.0

    def test_vt_polling_reclaims_expired_during_wait(self, real_redis_client, queue_name):
        first_gateway = RedisGateway(
            redis_client=real_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=1,
        )
        queue = RedisMessageQueue(queue_name, gateway=first_gateway)
        queue.publish("hello")

        first = first_gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert first is not None

        time.sleep(1.5)

        second_gateway = RedisGateway(
            redis_client=real_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=3,
            message_visibility_timeout_seconds=1,
        )
        start = time.monotonic()
        second = second_gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        elapsed = time.monotonic() - start

        assert second is not None
        assert second.stored_message == first.stored_message
        assert second.lease_token != first.lease_token
        assert elapsed < 1.0

    def test_vt_polling_returns_none_on_timeout(self, real_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=1,
            message_visibility_timeout_seconds=2,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway)

        start = time.monotonic()
        result = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        elapsed = time.monotonic() - start

        assert result is None
        assert elapsed >= 0.8

    def test_vt_polling_multiple_consumers_no_double_delivery(self, real_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=1,
            message_visibility_timeout_seconds=10,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway, deduplication=False)
        n = 10
        for i in range(n):
            queue.publish(f"msg-{i}")

        results = []
        lock = threading.Lock()
        barrier = threading.Barrier(n)

        def consumer():
            barrier.wait()
            r = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
            if r is not None:
                with lock:
                    results.append(r.stored_message)

        threads = [threading.Thread(target=consumer) for _ in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)

        assert len(results) == n
        assert len(set(results)) == n


# ---------------------------------------------------------------------------
# Dict message JSON round-trip through real Redis bytes
# ---------------------------------------------------------------------------


class TestDictMessageRoundTrip:
    def test_dict_publish_consume_round_trip(self, real_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway)
        original = {"key": "value", "count": 42}
        queue.publish(original)

        with queue.process_message() as msg:
            assert msg is not None
            parsed = json.loads(msg)
            assert parsed == original

    def test_nested_dict_round_trip(self, real_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway)
        original = {"outer": {"inner": [1, 2, 3]}, "list": [{"a": 1}, {"b": 2}]}
        queue.publish(original)

        with queue.process_message() as msg:
            assert msg is not None
            parsed = json.loads(msg)
            assert parsed == original

    def test_unicode_dict_round_trip(self, real_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
        )
        queue = RedisMessageQueue(queue_name, gateway=gateway)
        original = {"emoji": "\U0001f389", "japanese": "\u65e5\u672c\u8a9e", "math": "\u2211\u222b\u2202"}
        queue.publish(original)

        with queue.process_message() as msg:
            assert msg is not None
            parsed = json.loads(msg)
            assert parsed == original


# ---------------------------------------------------------------------------
# Cross-queue isolation (two queues sharing one Redis)
# ---------------------------------------------------------------------------


class TestCrossQueueIsolation:
    def test_two_queues_do_not_interfere(self, real_redis_client, queue_name):
        second_name = f"rmq-integ-{uuid4().hex[:12]}"
        gateway = RedisGateway(
            redis_client=real_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
        )
        queue_a = RedisMessageQueue(queue_name, gateway=gateway, deduplication=False)
        queue_b = RedisMessageQueue(second_name, gateway=gateway, deduplication=False)

        try:
            queue_a.publish("msg-a")
            queue_b.publish("msg-b")

            with queue_a.process_message() as msg_a:
                assert msg_a == b"msg-a"
            with queue_b.process_message() as msg_b:
                assert msg_b == b"msg-b"

            with queue_a.process_message() as msg:
                assert msg is None
            with queue_b.process_message() as msg:
                assert msg is None
        finally:
            _cleanup_keys(real_redis_client, second_name)

    def test_cross_queue_isolation_with_visibility_timeout(self, real_redis_client, queue_name):
        second_name = f"rmq-integ-{uuid4().hex[:12]}"
        gateway = RedisGateway(
            redis_client=real_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=10,
        )
        queue_a = RedisMessageQueue(queue_name, gateway=gateway)
        queue_b = RedisMessageQueue(second_name, gateway=gateway)

        try:
            queue_a.publish("msg-a")
            queue_b.publish("msg-b")

            claimed_a = gateway.wait_for_message_and_move(queue_a.key.pending, queue_a.key.processing)
            claimed_b = gateway.wait_for_message_and_move(queue_b.key.pending, queue_b.key.processing)
            assert claimed_a is not None and claimed_b is not None

            lease_deadlines_a = f"{queue_a.key.processing}:lease_deadlines"
            lease_deadlines_b = f"{queue_b.key.processing}:lease_deadlines"
            assert real_redis_client.zcard(lease_deadlines_a) == 1
            assert real_redis_client.zcard(lease_deadlines_b) == 1

            assert real_redis_client.llen(queue_a.key.processing) == 1
            assert real_redis_client.llen(queue_b.key.processing) == 1
        finally:
            _cleanup_keys(real_redis_client, second_name)


# ---------------------------------------------------------------------------
# Heartbeat daemon thread lifecycle with real Redis
# ---------------------------------------------------------------------------


class TestHeartbeatThreadLifecycle:
    def test_heartbeat_thread_terminates_after_success(self, real_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=2,
        )
        queue = RedisMessageQueue(
            queue_name,
            gateway=gateway,
            heartbeat_interval_seconds=0.3,
        )
        queue.publish("hello")

        with queue.process_message() as msg:
            assert msg == b"hello"
            heartbeat_threads = [t for t in threading.enumerate() if t.name == "redis-message-queue-lease-heartbeat"]
            assert len(heartbeat_threads) == 1

        time.sleep(0.1)
        heartbeat_threads = [t for t in threading.enumerate() if t.name == "redis-message-queue-lease-heartbeat"]
        assert len(heartbeat_threads) == 0

    def test_heartbeat_thread_terminates_after_failure(self, real_redis_client, queue_name):
        gateway = RedisGateway(
            redis_client=real_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=2,
        )
        queue = RedisMessageQueue(
            queue_name,
            gateway=gateway,
            heartbeat_interval_seconds=0.3,
        )
        queue.publish("hello")

        with pytest.raises(ValueError):
            with queue.process_message() as msg:
                assert msg == b"hello"
                heartbeat_threads = [
                    t for t in threading.enumerate() if t.name == "redis-message-queue-lease-heartbeat"
                ]
                assert len(heartbeat_threads) == 1
                raise ValueError("boom")

        time.sleep(0.1)
        heartbeat_threads = [t for t in threading.enumerate() if t.name == "redis-message-queue-lease-heartbeat"]
        assert len(heartbeat_threads) == 0


# ---------------------------------------------------------------------------
# Queue state persistence across client reconnection
# ---------------------------------------------------------------------------


class TestStatePersistenceAcrossReconnection:
    def test_queue_state_survives_reconnect(self, real_redis_client, real_redis_url, queue_name):
        gateway_a = RedisGateway(
            redis_client=real_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
        )
        queue_a = RedisMessageQueue(queue_name, gateway=gateway_a, deduplication=False)
        queue_a.publish("hello")

        client_b = redis.Redis.from_url(real_redis_url)
        try:
            gateway_b = RedisGateway(
                redis_client=client_b,
                retry_strategy=_no_retry,
                message_wait_interval_seconds=0,
            )
            queue_b = RedisMessageQueue(queue_name, gateway=gateway_b, deduplication=False)
            with queue_b.process_message() as msg:
                assert msg == b"hello"
        finally:
            client_b.close()

    def test_lease_metadata_survives_reconnect(self, real_redis_client, real_redis_url, queue_name):
        gateway_a = RedisGateway(
            redis_client=real_redis_client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=10,
        )
        queue_a = RedisMessageQueue(queue_name, gateway=gateway_a)
        queue_a.publish("hello")

        claimed = gateway_a.wait_for_message_and_move(queue_a.key.pending, queue_a.key.processing)
        assert claimed is not None

        client_b = redis.Redis.from_url(real_redis_url)
        try:
            gateway_b = RedisGateway(
                redis_client=client_b,
                retry_strategy=_no_retry,
                message_wait_interval_seconds=0,
                message_visibility_timeout_seconds=10,
            )
            renewed = gateway_b.renew_message_lease(queue_a.key.processing, claimed.stored_message, claimed.lease_token)
            assert renewed is True
        finally:
            client_b.close()
