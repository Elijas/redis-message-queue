import re

from redis.crc import key_slot

from redis_message_queue._exceptions import ConfigurationError
from redis_message_queue._queue_key_manager import QueueKeyManager

_HASH_TAG_PATTERN = re.compile(r"\{([^{}]+)\}")


def _redis_cluster_key_slot(key: str) -> int:
    return key_slot(key.encode("utf-8"))


def validate_queue_keys_for_redis_cluster(
    key_manager: QueueKeyManager,
    *,
    dead_letter_queue: str | None = None,
) -> None:
    queue_name = getattr(key_manager, "_queue_name", "<unknown>")
    deduplication_prefix = key_manager.deduplication("")
    if _HASH_TAG_PATTERN.search(deduplication_prefix) is None:
        raise ConfigurationError(
            "Redis Cluster requires queue keys to share a hash tag; "
            f"'name'={queue_name!r} produced key {deduplication_prefix!r} without one. "
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
    key_slots = {key: _redis_cluster_key_slot(key) for key in queue_keys}
    slots = set(key_slots.values())
    if len(slots) != 1:
        slot_summary = ", ".join(f"{key!r}:{slot}" for key, slot in sorted(key_slots.items()))
        raise ConfigurationError(
            "Queue keys do not map to a single Redis Cluster slot. "
            f"'name'={queue_name!r} produced slots {{{slot_summary}}}. "
            "Wrap the queue name in braces, for example '{myqueue}'."
        )

    if dead_letter_queue is not None:
        queue_slot = next(iter(slots))
        dead_letter_slot = _redis_cluster_key_slot(dead_letter_queue)
        if dead_letter_slot != queue_slot:
            raise ConfigurationError(
                "'dead_letter_queue' must share the same Redis Cluster hash tag as the queue keys."
                f" Queue slot is {queue_slot}; dead_letter_queue={dead_letter_queue!r} maps to {dead_letter_slot}."
                " For example, both '{myqueue}::pending' and '{myqueue}::dlq' share the '{myqueue}' tag —"
                " give your DLQ a name with the same braces."
            )
