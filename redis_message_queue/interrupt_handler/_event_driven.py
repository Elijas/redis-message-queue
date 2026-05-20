import threading

from redis_message_queue.interrupt_handler._interface import (
    BaseGracefulInterruptHandler,
)


class EventDrivenInterruptHandler(BaseGracefulInterruptHandler):
    """Interrupt handler driven by a user-owned ``threading.Event``.

    Unlike ``GracefulInterruptHandler``, this does not install signal handlers.
    Use it when rmq runs in the same process as another library that owns
    SIGTERM/SIGINT, such as a Celery worker, RQ SimpleWorker, or Dramatiq CLI.

    The caller MUST set ``stop_event`` before exiting. rmq observes
    ``is_interrupted()`` in wait/drain paths and exits cooperatively; it will
    not call ``sys.exit`` or otherwise force shutdown.
    """

    def __init__(self, stop_event: threading.Event) -> None:
        self._stop_event = stop_event

    def is_interrupted(self) -> bool:
        return self._stop_event.is_set()
