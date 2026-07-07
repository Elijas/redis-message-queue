"""Filter-guarded warning emission shared by the sync and async queue flavors.

``warnings.catch_warnings`` saves, mutates, and restores the process-global
``warnings.filters`` list and is not thread-safe. The queue emits diagnostics
from per-message heartbeat threads, so during a renewal-failure storm several
threads can run their ``catch_warnings`` blocks near-simultaneously; an
interleaved ``__exit__`` would then restore a snapshot containing a peer's
temporary ``"always"`` filter, silently overriding the application's warning
policy for the rest of the process (or, the other way around, restore the
application's ``"error"`` filter mid-emission and promote the guarded warn to
an exception). Every guarded emission therefore serializes on one
process-wide lock.
"""

import threading
import warnings

_warning_filters_lock = threading.Lock()


def warn_runtime_warning(message: str, *, stacklevel: int) -> None:
    """Emit a RuntimeWarning that survives app-level warning filters.

    The temporary ``"always"`` filter keeps failure-site diagnostics from
    being promoted to exceptions (``python -W error`` / pytest
    ``filterwarnings = ["error"]``) at call sites where raising would corrupt
    the caller's control flow.
    """

    with _warning_filters_lock:
        with warnings.catch_warnings():
            warnings.simplefilter("always", RuntimeWarning)
            warnings.warn(message, RuntimeWarning, stacklevel=stacklevel + 1)
