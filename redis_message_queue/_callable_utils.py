import inspect


def is_async_callable(value: object) -> bool:
    """Return True for async functions and callable objects with async __call__."""

    if inspect.iscoroutinefunction(value):
        return True
    return inspect.iscoroutinefunction(getattr(value, "__call__", None))
