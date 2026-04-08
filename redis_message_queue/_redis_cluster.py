import re

from redis.cluster import key_slot

from redis_message_queue._queue_key_manager import QueueKeyManager

_HASH_TAG_PATTERN = re.compile(r"\{([^{}]+)\}")


def _redis_cluster_key_slot(key: str) -> int:
    return key_slot(key.encode("utf-8"))


def validate_queue_keys_for_redis_cluster(
    key_manager: QueueKeyManager,
    *,
    dead_letter_queue: str | None = None,
) -> None:
    deduplication_prefix = key_manager.deduplication("")
    if _HASH_TAG_PATTERN.search(deduplication_prefix) is None:
        raise ValueError(
            "Redis Cluster requires queue keys to share a hash tag. "
            "Wrap the queue name in braces, for example '{myqueue}'."
        )

    queue_keys = [
        key_manager.pending,
        key_manager.processing,
        key_manager.completed,
        key_manager.failed,
        key_manager.dead_letter,
        deduplication_prefix,
    ]
    slots = {_redis_cluster_key_slot(key) for key in queue_keys}
    if len(slots) != 1:
        raise ValueError(
            "Queue keys do not map to a single Redis Cluster slot. "
            "Wrap the queue name in braces, for example '{myqueue}'."
        )

    if dead_letter_queue is not None:
        queue_slot = next(iter(slots))
        dead_letter_slot = _redis_cluster_key_slot(dead_letter_queue)
        if dead_letter_slot != queue_slot:
            raise ValueError("'dead_letter_queue' must share the same Redis Cluster hash tag as the queue keys.")
