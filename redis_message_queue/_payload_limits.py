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

    # json.dumps expands aliased/shared sub-objects into independent copies, so depth must be
    # measured along every mount path, not just the first one a global visited-set happens to
    # reach. Cycles are tracked by ancestors on the current path only (not a global set), so an
    # alias mounted at two non-nested paths is still measured twice. A cycle is infinitely deep
    # once expanded, so it raises PayloadTooDeepError. Recursion depth is bounded by
    # max_payload_depth + 1 because we raise as soon as depth exceeds the limit.
    def walk(value: object, path: str, depth: int, ancestors: frozenset[int]) -> None:
        if depth > max_payload_depth:
            raise PayloadTooDeepError(
                f"max_payload_depth={max_payload_depth} exceeded: depth {depth} reached at {path}"
            )
        if isinstance(value, dict):
            if id(value) in ancestors:
                raise PayloadTooDeepError(
                    f"max_payload_depth={max_payload_depth} exceeded: cycle reached at {path} (infinite depth)"
                )
            child_ancestors = ancestors | {id(value)}
            for key, child in value.items():
                walk(child, f"{path}[{key!r}]", depth + 1, child_ancestors)
        elif isinstance(value, (list, tuple)):
            if id(value) in ancestors:
                raise PayloadTooDeepError(
                    f"max_payload_depth={max_payload_depth} exceeded: cycle reached at {path} (infinite depth)"
                )
            child_ancestors = ancestors | {id(value)}
            for index, child in enumerate(value):
                walk(child, f"{path}[{index}]", depth + 1, child_ancestors)

    walk(message, "message", 0, frozenset())


def serialize_dict_payload_with_limit(message: dict, max_payload_bytes: int | None) -> str:
    message_str = json.dumps(message, sort_keys=True, allow_nan=False)
    if max_payload_bytes is not None:
        validate_max_payload_bytes(
            len(message_str.encode("utf-8")),
            max_payload_bytes,
            payload_type="dict message",
        )
    return message_str


def validate_str_payload_utf8_encodable(message: str) -> None:
    # The RMQ envelope is UTF-8 JSON text; a lone surrogate survives the
    # ensure_ascii publish serialization only to poison every downstream decode
    # (consume, peek, dead-letter). Reject it at the boundary instead. Dict
    # payloads never need this check: json.dumps escapes surrogates to ASCII.
    try:
        message.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ValueError(
            f"'message' str is not UTF-8-encodable ({exc.reason} at index {exc.start}); "
            "str payloads must be valid Unicode text. Lone surrogates typically come from "
            "surrogateescape decoding (e.g. os.fsdecode); repair the value before publishing."
        ) from exc


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
