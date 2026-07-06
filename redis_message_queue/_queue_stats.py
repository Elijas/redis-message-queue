from dataclasses import dataclass


@dataclass(frozen=True)
class QueueStats:
    """Immutable snapshot of a queue's Redis list depths.

    ``pending`` and ``processing`` are always integers. ``completed``,
    ``failed``, and ``dead_letter`` are ``None`` when the corresponding feature
    is disabled for the queue (no completed queue, no failed queue, or no
    dead-letter routing) and an integer depth otherwise. Depths are read with
    independent ``LLEN`` calls, so a ``QueueStats`` is a best-effort snapshot
    rather than a single point-in-time-consistent view of every list.
    """

    pending: int
    processing: int
    completed: int | None
    failed: int | None
    dead_letter: int | None
