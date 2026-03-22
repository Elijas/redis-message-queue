import logging
import re

import fakeredis
import pytest

from redis_message_queue._abstract_redis_gateway import AbstractRedisGateway
from redis_message_queue._redis_gateway import RedisGateway
from redis_message_queue._stored_message import ClaimedMessage
from redis_message_queue.redis_message_queue import RedisMessageQueue


def _no_retry(func):
    return func


class TestConstructorClientValidation:
    def test_falsy_non_none_client_is_accepted(self):
        """A client that is falsy (e.g. __bool__=False) but not None must not be
        rejected by the constructor — it should fail later at point of use, not
        with a misleading 'must be provided' error."""

        class FalsyClient:
            """A mock client that evaluates to falsy."""

            def __bool__(self):
                return False

        # Should NOT raise ValueError — the client was provided, it's just falsy.
        # Any error should come later when actually using the client, not here.
        try:
            RedisMessageQueue("test", client=FalsyClient())
        except ValueError as e:
            if "must be provided" in str(e):
                pytest.fail(f"Constructor rejected a falsy-but-provided client: {e}")


class TestConstructorNameValidation:
    def test_empty_name_raises_value_error(self):
        gateway = FakeGateway()
        with pytest.raises(ValueError, match="non-empty"):
            RedisMessageQueue("", gateway=gateway)

    def test_whitespace_only_name_raises_value_error(self):
        gateway = FakeGateway()
        with pytest.raises(ValueError, match="non-empty"):
            RedisMessageQueue("   ", gateway=gateway)

    def test_none_name_raises_type_error(self):
        gateway = FakeGateway()
        with pytest.raises(TypeError, match="must be a string"):
            RedisMessageQueue(None, gateway=gateway)


class TestConstructorKeySeparatorValidation:
    def test_none_key_separator_raises_type_error(self):
        gateway = FakeGateway()
        with pytest.raises(TypeError, match="must be a string"):
            RedisMessageQueue("test", gateway=gateway, key_separator=None)

    def test_int_key_separator_raises_type_error(self):
        gateway = FakeGateway()
        with pytest.raises(TypeError, match="must be a string"):
            RedisMessageQueue("test", gateway=gateway, key_separator=123)

    def test_bool_key_separator_raises_type_error(self):
        gateway = FakeGateway()
        with pytest.raises(TypeError, match="must be a string"):
            RedisMessageQueue("test", gateway=gateway, key_separator=True)

    def test_empty_key_separator_raises_value_error(self):
        gateway = FakeGateway()
        with pytest.raises(ValueError, match="non-empty"):
            RedisMessageQueue("test", gateway=gateway, key_separator="")

    def test_whitespace_only_key_separator_raises_value_error(self):
        gateway = FakeGateway()
        with pytest.raises(ValueError, match="non-empty"):
            RedisMessageQueue("test", gateway=gateway, key_separator="   ")


class TestConstructorConflictingParameters:
    """When gateway is provided, client and interrupt must not be silently ignored."""

    def test_gateway_and_client_raises(self):
        gateway = FakeGateway()
        with pytest.raises(ValueError, match="cannot be provided alongside"):
            RedisMessageQueue("test", gateway=gateway, client=object())

    def test_gateway_and_interrupt_raises(self):
        gateway = FakeGateway()
        with pytest.raises(ValueError, match="cannot be provided alongside"):
            RedisMessageQueue("test", gateway=gateway, interrupt=object())

    def test_gateway_alone_is_accepted(self):
        gateway = FakeGateway()
        queue = RedisMessageQueue("test", gateway=gateway)
        assert queue._redis is gateway

    def test_client_alone_is_accepted(self):
        class FakeClient:
            pass

        try:
            RedisMessageQueue("test", client=FakeClient())
        except ValueError as e:
            if "cannot be provided alongside" in str(e):
                pytest.fail(f"Constructor rejected client-only usage: {e}")


class TestConstructorGatewayValidation:
    def test_non_gateway_object_raises_type_error(self):
        with pytest.raises(TypeError, match="'gateway' must be an AbstractRedisGateway"):
            RedisMessageQueue("test", gateway=object())


class FakeGateway(AbstractRedisGateway):
    def __init__(self):
        self.message_to_return = None
        self.fail_on_move = False
        self.fail_on_remove = False
        self.move_exception = ConnectionError("Redis connection lost")
        self.remove_exception = ConnectionError("Redis connection lost")
        self.removed_messages = []
        self.moved_messages = []
        self.move_attempts = []
        self.remove_attempts = []
        self.move_lease_tokens = []
        self.remove_lease_tokens = []
        self.move_return_value = True
        self.remove_return_value = True

    def publish_message(self, queue, message, dedup_key):
        return True

    def add_message(self, queue, message):
        pass

    def move_message(self, from_queue, to_queue, message, *, lease_token=None):
        self.move_attempts.append((from_queue, to_queue, message))
        self.move_lease_tokens.append(lease_token)
        if self.fail_on_move:
            raise self.move_exception
        self.moved_messages.append((from_queue, to_queue, message))
        return self.move_return_value

    def remove_message(self, queue, message, *, lease_token=None):
        self.remove_attempts.append((queue, message))
        self.remove_lease_tokens.append(lease_token)
        if self.fail_on_remove:
            raise self.remove_exception
        self.removed_messages.append((queue, message))
        return self.remove_return_value

    def renew_message_lease(self, queue, message, lease_token):
        return True

    def wait_for_message_and_move(self, from_queue, to_queue):
        return self.message_to_return


class TestConstructorBooleanParameterValidation:
    @pytest.mark.parametrize("invalid_value", ["yes", "false", 1, 0, None, 2.0, []])
    def test_non_bool_deduplication_raises_type_error(self, invalid_value):
        gateway = FakeGateway()
        with pytest.raises(TypeError, match="'deduplication' must be a bool"):
            RedisMessageQueue("test", gateway=gateway, deduplication=invalid_value)

    @pytest.mark.parametrize("invalid_value", ["yes", "false", 1, 0, None, 2.0, []])
    def test_non_bool_enable_completed_queue_raises_type_error(self, invalid_value):
        gateway = FakeGateway()
        with pytest.raises(TypeError, match="'enable_completed_queue' must be a bool"):
            RedisMessageQueue("test", gateway=gateway, enable_completed_queue=invalid_value)

    @pytest.mark.parametrize("invalid_value", ["yes", "false", 1, 0, None, 2.0, []])
    def test_non_bool_enable_failed_queue_raises_type_error(self, invalid_value):
        gateway = FakeGateway()
        with pytest.raises(TypeError, match="'enable_failed_queue' must be a bool"):
            RedisMessageQueue("test", gateway=gateway, enable_failed_queue=invalid_value)

    def test_true_is_accepted(self):
        gateway = FakeGateway()
        q = RedisMessageQueue(
            "test",
            gateway=gateway,
            deduplication=True,
            enable_completed_queue=True,
            enable_failed_queue=True,
        )
        assert q._deduplication is True
        assert q._enable_completed_queue is True
        assert q._enable_failed_queue is True

    def test_false_is_accepted(self):
        gateway = FakeGateway()
        q = RedisMessageQueue(
            "test",
            gateway=gateway,
            deduplication=False,
            enable_completed_queue=False,
            enable_failed_queue=False,
        )
        assert q._deduplication is False
        assert q._enable_completed_queue is False
        assert q._enable_failed_queue is False


class TestConstructorGetDeduplicationKeyValidation:
    @pytest.mark.parametrize("invalid_value", [42, "not_callable", True, 3.14, [1, 2], {"a": 1}])
    def test_non_callable_get_deduplication_key_raises_type_error(self, invalid_value):
        gateway = FakeGateway()
        with pytest.raises(TypeError, match="'get_deduplication_key' must be callable"):
            RedisMessageQueue("test", gateway=gateway, get_deduplication_key=invalid_value)

    def test_none_is_accepted(self):
        gateway = FakeGateway()
        q = RedisMessageQueue("test", gateway=gateway, get_deduplication_key=None)
        assert q._get_deduplication_key is None

    def test_lambda_is_accepted(self):
        gateway = FakeGateway()
        fn = lambda msg: msg
        q = RedisMessageQueue("test", gateway=gateway, get_deduplication_key=fn)
        assert q._get_deduplication_key is fn

    def test_callable_object_is_accepted(self):
        class MyCallable:
            def __call__(self, msg):
                return msg

        gateway = FakeGateway()
        obj = MyCallable()
        q = RedisMessageQueue("test", gateway=gateway, get_deduplication_key=obj)
        assert q._get_deduplication_key is obj


class TestConstructorDeduplicationContradiction:
    def test_dedup_disabled_with_callback_raises_value_error(self):
        gateway = FakeGateway()
        with pytest.raises(ValueError, match="cannot be provided when 'deduplication' is disabled"):
            RedisMessageQueue(
                "test",
                gateway=gateway,
                deduplication=False,
                get_deduplication_key=lambda msg: msg,
            )

    def test_dedup_enabled_with_callback_is_accepted(self):
        gateway = FakeGateway()
        q = RedisMessageQueue(
            "test",
            gateway=gateway,
            deduplication=True,
            get_deduplication_key=lambda msg: msg,
        )
        assert q._deduplication is True
        assert q._get_deduplication_key is not None

    def test_dedup_disabled_without_callback_is_accepted(self):
        gateway = FakeGateway()
        q = RedisMessageQueue(
            "test",
            gateway=gateway,
            deduplication=False,
            get_deduplication_key=None,
        )
        assert q._deduplication is False
        assert q._get_deduplication_key is None


class TestProcessMessageExceptionPropagation:
    def test_user_exception_propagates_when_remove_fails(self):
        gateway = FakeGateway()
        gateway.message_to_return = b"test-message"
        gateway.fail_on_remove = True
        queue = RedisMessageQueue("test", gateway=gateway)

        with pytest.raises(ValueError, match="original error"):
            with queue.process_message() as _msg:
                raise ValueError("original error")

    def test_user_exception_propagates_when_move_to_failed_fails(self):
        gateway = FakeGateway()
        gateway.message_to_return = b"test-message"
        gateway.fail_on_move = True
        queue = RedisMessageQueue("test", gateway=gateway, enable_failed_queue=True)

        with pytest.raises(ValueError, match="original error"):
            with queue.process_message() as _msg:
                raise ValueError("original error")

    def test_keyboard_interrupt_propagates_when_cleanup_fails(self):
        gateway = FakeGateway()
        gateway.message_to_return = b"test-message"
        gateway.fail_on_remove = True
        queue = RedisMessageQueue("test", gateway=gateway)

        with pytest.raises(KeyboardInterrupt):
            with queue.process_message() as _msg:
                raise KeyboardInterrupt()

    def test_user_exception_still_cleans_up_when_possible(self):
        gateway = FakeGateway()
        gateway.message_to_return = b"test-message"
        queue = RedisMessageQueue("test", gateway=gateway)

        with pytest.raises(ValueError):
            with queue.process_message() as _msg:
                raise ValueError("boom")

        assert len(gateway.removed_messages) == 1

    def test_user_exception_moves_to_failed_queue_when_enabled(self):
        gateway = FakeGateway()
        gateway.message_to_return = b"test-message"
        queue = RedisMessageQueue("test", gateway=gateway, enable_failed_queue=True)

        with pytest.raises(ValueError):
            with queue.process_message() as _msg:
                raise ValueError("boom")

        assert len(gateway.moved_messages) == 1
        _, to_queue, _ = gateway.moved_messages[0]
        assert to_queue == queue.key.failed


class TestProcessMessageCleanupBaseException:
    """When cleanup raises a non-Exception BaseException (e.g. KeyboardInterrupt),
    the original user exception must still propagate."""

    def test_user_exception_propagates_when_remove_raises_base_exception(self):
        gateway = FakeGateway()
        gateway.message_to_return = b"test-message"
        gateway.fail_on_remove = True
        gateway.remove_exception = KeyboardInterrupt()
        queue = RedisMessageQueue("test", gateway=gateway)

        with pytest.raises(ValueError, match="original error"):
            with queue.process_message() as _msg:
                raise ValueError("original error")

    def test_user_exception_propagates_when_move_to_failed_raises_base_exception(self):
        gateway = FakeGateway()
        gateway.message_to_return = b"test-message"
        gateway.fail_on_move = True
        gateway.move_exception = KeyboardInterrupt()
        queue = RedisMessageQueue("test", gateway=gateway, enable_failed_queue=True)

        with pytest.raises(ValueError, match="original error"):
            with queue.process_message() as _msg:
                raise ValueError("original error")


class TestProcessMessageNoneHandling:
    def test_timeout_yields_none(self):
        gateway = FakeGateway()
        gateway.message_to_return = None
        queue = RedisMessageQueue("test", gateway=gateway)

        with queue.process_message() as msg:
            assert msg is None

    def test_empty_bytes_message_is_not_swallowed(self):
        gateway = FakeGateway()
        gateway.message_to_return = b""
        queue = RedisMessageQueue("test", gateway=gateway)

        with queue.process_message() as msg:
            assert msg == b""

    def test_empty_bytes_message_is_cleaned_up_on_success(self):
        gateway = FakeGateway()
        gateway.message_to_return = b""
        queue = RedisMessageQueue("test", gateway=gateway)

        with queue.process_message() as _msg:
            pass

        assert len(gateway.removed_messages) == 1


class TestProcessMessageSuccessPath:
    def test_success_removes_from_processing(self):
        gateway = FakeGateway()
        gateway.message_to_return = b"test-message"
        queue = RedisMessageQueue("test", gateway=gateway)

        with queue.process_message() as msg:
            assert msg == b"test-message"

        assert len(gateway.removed_messages) == 1
        removed_queue, _ = gateway.removed_messages[0]
        assert removed_queue == queue.key.processing

    def test_success_moves_to_completed_when_enabled(self):
        gateway = FakeGateway()
        gateway.message_to_return = b"test-message"
        queue = RedisMessageQueue("test", gateway=gateway, enable_completed_queue=True)

        with queue.process_message() as _msg:
            pass

        assert len(gateway.moved_messages) == 1
        _, to_queue, _ = gateway.moved_messages[0]
        assert to_queue == queue.key.completed

    def test_timeout_does_not_clean_up(self):
        gateway = FakeGateway()
        gateway.message_to_return = None
        queue = RedisMessageQueue("test", gateway=gateway)

        with queue.process_message() as _msg:
            pass

        assert len(gateway.removed_messages) == 0
        assert len(gateway.moved_messages) == 0


class TestProcessMessageSuccessCleanupFailure:
    """When user code succeeds but post-success Redis cleanup fails,
    the message must NOT be routed to the failed queue."""

    def test_move_to_completed_failure_does_not_move_to_failed(self):
        gateway = FakeGateway()
        gateway.message_to_return = b"test-message"
        gateway.fail_on_move = True
        queue = RedisMessageQueue(
            "test",
            gateway=gateway,
            enable_completed_queue=True,
            enable_failed_queue=True,
        )

        with pytest.raises(ConnectionError):
            with queue.process_message() as _msg:
                pass

        assert len(gateway.move_attempts) == 1
        assert gateway.move_attempts[0][1] == queue.key.completed

    def test_remove_failure_does_not_move_to_failed(self):
        gateway = FakeGateway()
        gateway.message_to_return = b"test-message"
        gateway.fail_on_remove = True
        queue = RedisMessageQueue(
            "test",
            gateway=gateway,
            enable_completed_queue=False,
            enable_failed_queue=True,
        )

        with pytest.raises(ConnectionError):
            with queue.process_message() as _msg:
                pass

        assert len(gateway.remove_attempts) == 1
        assert len(gateway.move_attempts) == 0


class TestProcessMessageWithLeaseToken:
    def test_success_passes_lease_token_to_remove(self):
        gateway = FakeGateway()
        gateway.message_to_return = ClaimedMessage(stored_message=b"msg", lease_token="tk1")
        queue = RedisMessageQueue("test", gateway=gateway)

        with queue.process_message() as _msg:
            pass

        assert len(gateway.removed_messages) == 1
        assert gateway.remove_lease_tokens == ["tk1"]

    def test_success_passes_lease_token_to_move_completed(self):
        gateway = FakeGateway()
        gateway.message_to_return = ClaimedMessage(stored_message=b"msg", lease_token="tk1")
        queue = RedisMessageQueue("test", gateway=gateway, enable_completed_queue=True)

        with queue.process_message() as _msg:
            pass

        assert len(gateway.moved_messages) == 1
        assert gateway.move_lease_tokens == ["tk1"]

    def test_failure_passes_lease_token_to_move_failed(self):
        gateway = FakeGateway()
        gateway.message_to_return = ClaimedMessage(stored_message=b"msg", lease_token="tk1")
        queue = RedisMessageQueue("test", gateway=gateway, enable_failed_queue=True)

        with pytest.raises(ValueError):
            with queue.process_message() as _msg:
                raise ValueError("boom")

        assert len(gateway.moved_messages) == 1
        _, to_queue, _ = gateway.moved_messages[0]
        assert to_queue == queue.key.failed
        assert gateway.move_lease_tokens == ["tk1"]

    def test_failure_passes_lease_token_to_remove(self):
        gateway = FakeGateway()
        gateway.message_to_return = ClaimedMessage(stored_message=b"msg", lease_token="tk1")
        queue = RedisMessageQueue("test", gateway=gateway)

        with pytest.raises(ValueError):
            with queue.process_message() as _msg:
                raise ValueError("boom")

        assert len(gateway.removed_messages) == 1
        assert gateway.remove_lease_tokens == ["tk1"]

    def test_yields_decoded_stored_message_not_claimed_message(self):
        gateway = FakeGateway()
        gateway.message_to_return = ClaimedMessage(stored_message=b"msg", lease_token="tk1")
        queue = RedisMessageQueue("test", gateway=gateway)

        with queue.process_message() as msg:
            assert msg == b"msg"
            assert not isinstance(msg, ClaimedMessage)


class TestProcessMessageStaleLease:
    def test_success_path_logs_warning_when_remove_returns_false(self, caplog):
        gateway = FakeGateway()
        gateway.message_to_return = ClaimedMessage(stored_message=b"msg", lease_token="tk1")
        gateway.remove_return_value = False
        queue = RedisMessageQueue("test", gateway=gateway)

        with caplog.at_level(logging.WARNING, logger="redis_message_queue.redis_message_queue"):
            with queue.process_message() as _msg:
                pass

        assert any("lease expired" in r.message for r in caplog.records)

    def test_success_path_logs_warning_when_move_to_completed_returns_false(self, caplog):
        gateway = FakeGateway()
        gateway.message_to_return = ClaimedMessage(stored_message=b"msg", lease_token="tk1")
        gateway.move_return_value = False
        queue = RedisMessageQueue("test", gateway=gateway, enable_completed_queue=True)

        with caplog.at_level(logging.WARNING, logger="redis_message_queue.redis_message_queue"):
            with queue.process_message() as _msg:
                pass

        assert any("lease expired" in r.message for r in caplog.records)

    def test_success_path_no_warning_without_lease_token(self, caplog):
        gateway = FakeGateway()
        gateway.message_to_return = b"msg"
        gateway.remove_return_value = False
        queue = RedisMessageQueue("test", gateway=gateway)

        with caplog.at_level(logging.WARNING, logger="redis_message_queue.redis_message_queue"):
            with queue.process_message() as _msg:
                pass

        assert not any("lease expired" in r.message for r in caplog.records)

    def test_failure_path_stale_lease_does_not_mask_user_exception(self):
        gateway = FakeGateway()
        gateway.message_to_return = ClaimedMessage(stored_message=b"msg", lease_token="tk1")
        gateway.remove_return_value = False
        queue = RedisMessageQueue("test", gateway=gateway)

        with pytest.raises(ValueError, match="original error"):
            with queue.process_message() as _msg:
                raise ValueError("original error")


class TestAtMostOnceMessageLoss:
    """F1: Without visibility_timeout_seconds (the default), a consumer crash
    after claiming a message leaves it permanently orphaned in the processing
    queue. There is no reclaim mechanism, no TTL, no warning. This is
    at-most-once delivery by design."""

    def test_crashed_consumer_orphans_message_permanently(self):
        client = fakeredis.FakeRedis()
        gateway = RedisGateway(
            redis_client=client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
        )
        queue = RedisMessageQueue("test", gateway=gateway, deduplication=False)

        queue.publish("important-message")
        assert client.llen(queue.key.pending) == 1

        # Simulate consumer claiming the message (what process_message does internally).
        claimed = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert claimed is not None

        # Consumer crashes here — never calls remove_message or move_message.
        # The message is now stuck in processing with no recovery path.
        assert client.llen(queue.key.pending) == 0
        assert client.llen(queue.key.processing) == 1

        # No reclaim mechanism exists without visibility timeout.
        # Another consumer trying to claim gets nothing from pending.
        next_claim = gateway.wait_for_message_and_move(queue.key.pending, queue.key.processing)
        assert next_claim is None

        # The message remains permanently orphaned in processing.
        assert client.llen(queue.key.pending) == 0
        assert client.llen(queue.key.processing) == 1


class TestCompletedQueueGrowth:
    """F2: Completed/failed queues grow without bound. Every processed message
    is LPUSH'd with no LTRIM, TTL, or max-length cap. In sustained workloads,
    these lists grow linearly with throughput, consuming Redis memory
    indefinitely."""

    def test_completed_queue_grows_linearly_with_throughput(self):
        client = fakeredis.FakeRedis()
        gateway = RedisGateway(
            redis_client=client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
        )
        queue = RedisMessageQueue(
            "test",
            gateway=gateway,
            deduplication=False,
            enable_completed_queue=True,
        )

        n_messages = 50
        for i in range(n_messages):
            queue.publish(f"message-{i}")

        for _ in range(n_messages):
            with queue.process_message() as msg:
                assert msg is not None

        # Every successfully processed message accumulates in the completed queue.
        # There is no trimming — the list grows without bound.
        assert client.llen(queue.key.completed) == n_messages
        assert client.llen(queue.key.processing) == 0
        assert client.llen(queue.key.pending) == 0

    def test_failed_queue_grows_linearly_with_throughput(self):
        client = fakeredis.FakeRedis()
        gateway = RedisGateway(
            redis_client=client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
        )
        queue = RedisMessageQueue(
            "test",
            gateway=gateway,
            deduplication=False,
            enable_failed_queue=True,
        )

        n_messages = 50
        for i in range(n_messages):
            queue.publish(f"message-{i}")

        for _ in range(n_messages):
            with pytest.raises(RuntimeError):
                with queue.process_message() as msg:
                    assert msg is not None
                    raise RuntimeError("processing failed")

        # Every failed message accumulates in the failed queue.
        # There is no trimming — the list grows without bound.
        assert client.llen(queue.key.failed) == n_messages
        assert client.llen(queue.key.processing) == 0
        assert client.llen(queue.key.pending) == 0


class TestClusterHashTagCompatibility:
    """F3: Redis Cluster CROSSSLOT incompatibility. The key scheme uses keys
    that hash to different slots in Cluster mode. The workaround is wrapping
    the queue name in hash tags: '{myqueue}'. This ensures all keys share the
    same hash tag and map to the same Redis Cluster slot."""

    def test_hash_tagged_name_produces_colocated_keys(self):
        """All keys generated for a hash-tagged queue name must share the
        same hash tag, ensuring they map to the same Redis Cluster slot."""
        gateway = FakeGateway()
        queue = RedisMessageQueue("{myqueue}", gateway=gateway, deduplication=True)

        # All QueueKeyManager keys
        queue_keys = [
            queue.key.pending,
            queue.key.processing,
            queue.key.completed,
            queue.key.failed,
            queue.key.deduplication("some-message"),
        ]

        # Gateway-derived lease metadata keys (suffixed to the processing queue key)
        processing = queue.key.processing
        lease_keys = [
            f"{processing}:lease_deadlines",
            f"{processing}:lease_tokens",
            f"{processing}:lease_token_counter",
        ]

        all_keys = queue_keys + lease_keys

        # Redis Cluster computes hash slot from content between first { and first } after it.
        hash_tag_pattern = re.compile(r"\{([^}]+)\}")
        for key in all_keys:
            match = hash_tag_pattern.search(key)
            assert match is not None, f"Key {key!r} has no hash tag"
            assert match.group(1) == "myqueue", (
                f"Key {key!r} has hash tag {match.group(1)!r}, expected 'myqueue'"
            )

    def test_hash_tagged_queue_round_trip(self):
        """Publish and process a message using a hash-tagged queue name to
        verify functional correctness with the workaround."""
        client = fakeredis.FakeRedis()
        gateway = RedisGateway(
            redis_client=client,
            retry_strategy=_no_retry,
            message_wait_interval_seconds=0,
        )
        queue = RedisMessageQueue(
            "{myqueue}",
            gateway=gateway,
            deduplication=False,
        )

        queue.publish("hello-cluster")

        with queue.process_message() as msg:
            assert msg is not None
            if isinstance(msg, bytes):
                msg = msg.decode("utf-8")
            assert msg == "hello-cluster"

        assert client.llen(queue.key.pending) == 0
        assert client.llen(queue.key.processing) == 0
