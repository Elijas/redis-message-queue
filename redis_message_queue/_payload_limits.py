import json

from redis_message_queue._exceptions import ConfigurationError, PayloadTooDeepError, PayloadTooLargeError


def validate_payload_limit_parameter(name: str, value: int | None) -> int | None:
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool):
        bool_hint = " (use a positive int or None, not True/False)" if isinstance(value, bool) else ""
        raise TypeError(f"'{name}' must be an int or None, got {type(value).__name__}{bool_hint}")
    if value <= 0:
        raise ConfigurationError(f"'{name}' must be positive when provided, got {value}")
    return value


def validate_max_payload_depth(message: dict, max_payload_depth: int | None) -> None:
    if max_payload_depth is None:
        return

    stack: list[tuple[object, str, int]] = [(message, "message", 0)]
    seen: set[int] = set()
    while stack:
        value, path, depth = stack.pop()
        if depth > max_payload_depth:
            raise PayloadTooDeepError(
                f"max_payload_depth={max_payload_depth} exceeded: depth {depth} reached at {path}"
            )
        if isinstance(value, dict):
            current_id = id(value)
            if current_id in seen:
                continue
            seen.add(current_id)
            children = list(value.items())
            for key, child in reversed(children):
                stack.append((child, f"{path}[{key!r}]", depth + 1))
        elif isinstance(value, (list, tuple)):
            current_id = id(value)
            if current_id in seen:
                continue
            seen.add(current_id)
            for index in range(len(value) - 1, -1, -1):
                stack.append((value[index], f"{path}[{index}]", depth + 1))


def serialize_dict_payload_with_limit(message: dict, max_payload_bytes: int | None) -> str:
    message_str = json.dumps(message, sort_keys=True, allow_nan=False)
    if max_payload_bytes is not None:
        validate_max_payload_bytes(
            len(message_str.encode("utf-8")),
            max_payload_bytes,
            payload_type="dict message",
        )
    return message_str


def validate_str_payload_size(message: str, max_payload_bytes: int | None) -> None:
    if max_payload_bytes is None:
        return
    validate_max_payload_bytes(
        len(message.encode("utf-8")),
        max_payload_bytes,
        payload_type="str message",
    )


def validate_max_payload_bytes(size_bytes: int, max_payload_bytes: int | None, *, payload_type: str) -> None:
    if max_payload_bytes is None or size_bytes <= max_payload_bytes:
        return
    raise PayloadTooLargeError(
        f"max_payload_bytes={max_payload_bytes} exceeded: payload is {size_bytes} bytes ({payload_type})"
    )
