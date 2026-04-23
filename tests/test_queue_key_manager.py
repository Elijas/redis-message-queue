import pytest

from redis_message_queue._queue_key_manager import QueueKeyManager


class TestQueueKeyManagerValidation:
    def test_empty_name_raises_value_error(self):
        with pytest.raises(ValueError, match="'name' must be a non-empty string"):
            QueueKeyManager("", key_separator="::")

    def test_whitespace_name_raises_value_error(self):
        with pytest.raises(ValueError, match="'name' must be a non-empty string"):
            QueueKeyManager("   ", key_separator="::")

    def test_non_string_name_raises_type_error(self):
        with pytest.raises(TypeError, match="'name' must be a string"):
            QueueKeyManager(42, key_separator="::")  # type: ignore[arg-type]

    def test_empty_separator_raises_value_error(self):
        with pytest.raises(ValueError, match="'key_separator' must be a non-empty string"):
            QueueKeyManager("q", key_separator="")

    def test_non_string_separator_raises_type_error(self):
        with pytest.raises(TypeError, match="'key_separator' must be a string"):
            QueueKeyManager("q", key_separator=42)  # type: ignore[arg-type]

    def test_name_containing_default_separator_raises_value_error(self):
        """Regression for a key-collision bug:
        ``QueueKeyManager('q').deduplication('pending')`` produces
        ``'q::deduplication::pending'`` and
        ``QueueKeyManager('q::deduplication').pending`` produces the same string.
        The first is a string key (dedup marker), the second a list key (pending queue).
        Accepting such a name lets the two collide in Redis and causes WRONGTYPE at runtime.
        """
        with pytest.raises(ValueError, match="must not contain the key separator"):
            QueueKeyManager("q::deduplication", key_separator="::")

    def test_name_containing_custom_separator_raises_value_error(self):
        with pytest.raises(ValueError, match="must not contain the key separator"):
            QueueKeyManager("q/sub", key_separator="/")

    def test_name_not_containing_separator_is_accepted(self):
        km = QueueKeyManager("q", key_separator="::")
        assert km.pending == "q::pending"
        assert km.deduplication("msg") == "q::deduplication::msg"

    def test_cluster_hashtag_name_is_accepted(self):
        km = QueueKeyManager("{queue}", key_separator="::")
        assert km.pending == "{queue}::pending"
