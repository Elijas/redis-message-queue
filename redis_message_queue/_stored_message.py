import json
import uuid
from dataclasses import dataclass

from redis_message_queue._exceptions import MalformedStoredMessageError

# What your consumer receives from process_message(): a str (already-decoded
# text) or bytes (raw wire payload). If you published a dict, use
# json.loads(...) on it to recover the dict.
ReceivedPayload = str | bytes
# What you pass to publish(): a str (must be UTF-8-encodable; lone surrogates
# are rejected) or a dict (JSON-serialized before storage).
PublishPayload = str | dict[str, object]

_STORED_MESSAGE_PREFIX = "\x1eRMQ1:"
_STORED_MESSAGE_PREFIX_BYTES = _STORED_MESSAGE_PREFIX.encode("utf-8")
_NON_ENVELOPE_STRICT_ERROR = "value does not start with RMQ envelope prefix; expected an rmq-published message"


@dataclass(frozen=True)
class ClaimedMessage:
    stored_message: ReceivedPayload
    lease_token: str

    def __post_init__(self) -> None:
        if not isinstance(self.lease_token, str):
            raise TypeError(
                "ClaimedMessage.lease_token must be a str"
                " (typically uuid4().hex returned by the claim Lua); got "
                f"{type(self.lease_token).__name__}"
            )
        if not self.lease_token:
            raise ValueError(
                "ClaimedMessage.lease_token must be a non-empty string"
                " (typically uuid4().hex returned by the claim Lua); got empty string"
            )
        if not isinstance(self.stored_message, (str, bytes)):
            raise TypeError(
                "ClaimedMessage.stored_message must be str or bytes"
                " (the envelope written by encode_stored_message); got "
                f"{type(self.stored_message).__name__}"
            )


def encode_stored_message(message: str) -> str:
    envelope = {
        "id": uuid.uuid4().hex,
        "payload": message,
    }
    return f"{_STORED_MESSAGE_PREFIX}{json.dumps(envelope, separators=(',', ':'))}"


def decode_stored_message(message: ReceivedPayload, *, strict_envelope_decoding: bool = False) -> ReceivedPayload:
    """Strip the stored-message envelope and return the original payload.

    Designed to consume values produced by ``encode_stored_message`` (or the
    redrive script's binary-safe ``payload_hex`` envelopes) only. Calling this
    on a raw user-supplied string that happens to look like a valid envelope
    (matches the prefix and parses as a payload-bearing JSON object) will
    return the inner ``payload`` field — round-trip is preserved only when
    input came through ``encode_stored_message`` first. Built-in
    publish/consume always re-wraps so this footgun cannot fire end-to-end;
    custom gateways feeding raw input must wrap before decoding.

    Raises ``MalformedStoredMessageError`` when the value starts with the RMQ
    envelope prefix but is not a valid payload-bearing envelope. When
    ``strict_envelope_decoding=True``, also raises for values that do not start
    with the RMQ envelope prefix.
    """
    envelope = _decode_envelope(message, strict_envelope_decoding=strict_envelope_decoding)
    if envelope is None:
        return message
    _message_id, payload = envelope
    if isinstance(payload, bytes):
        # Binary-safe ``payload_hex`` envelope (redrive of non-UTF-8 foreign
        # bytes): the exact original bytes, regardless of client decode mode.
        return payload
    if isinstance(message, bytes):
        try:
            return payload.encode("utf-8")
        except UnicodeEncodeError as exc:
            # publish() rejects lone-surrogate str payloads, so such an
            # envelope can only come from a foreign or tampering writer; it
            # must fail with the library's typed error, not a builtin
            # UnicodeEncodeError.
            raise MalformedStoredMessageError(
                "Stored RMQ envelope 'payload' field is not UTF-8-encodable (contains a lone surrogate)"
            ) from exc
    # str-mode (decode_responses=True) round-trips the payload losslessly even
    # when it contains lone surrogates; only the bytes wire form cannot.
    return payload


def extract_stored_message_id(message: ReceivedPayload, *, strict_envelope_decoding: bool = False) -> str | None:
    """Return the RMQ envelope id, or None for values that are not RMQ envelopes.

    Raises ``MalformedStoredMessageError`` when the value starts with the RMQ
    envelope prefix but is not a valid payload-bearing envelope. When
    ``strict_envelope_decoding=True``, also raises for values that do not start
    with the RMQ envelope prefix.
    """
    envelope = _decode_envelope(message, strict_envelope_decoding=strict_envelope_decoding)
    if envelope is None:
        return None
    message_id, _payload = envelope
    return message_id


def _decode_envelope(
    message: ReceivedPayload, *, strict_envelope_decoding: bool = False
) -> tuple[str, str | bytes] | None:
    if isinstance(message, bytes):
        if not message.startswith(_STORED_MESSAGE_PREFIX_BYTES):
            if strict_envelope_decoding:
                raise MalformedStoredMessageError(_NON_ENVELOPE_STRICT_ERROR)
            return None
        try:
            message = message.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise MalformedStoredMessageError(
                "Stored message starts with the RMQ envelope prefix but is not valid UTF-8"
            ) from exc
    elif not message.startswith(_STORED_MESSAGE_PREFIX):
        if strict_envelope_decoding:
            raise MalformedStoredMessageError(_NON_ENVELOPE_STRICT_ERROR)
        return None

    envelope_body = message[len(_STORED_MESSAGE_PREFIX) :]

    try:
        envelope = json.loads(envelope_body)
    except json.JSONDecodeError as exc:
        raise MalformedStoredMessageError(
            "Stored message starts with the RMQ envelope prefix but does not contain valid JSON"
        ) from exc

    if not isinstance(envelope, dict):
        raise MalformedStoredMessageError(
            "Stored message starts with the RMQ envelope prefix but does not contain a JSON object"
        )

    if "id" not in envelope:
        raise MalformedStoredMessageError("Stored RMQ envelope is missing required 'id' field")
    # ``payload_hex`` is the binary-safe alternative written by the redrive
    # script for dead-letter payloads whose bytes are not valid UTF-8; exactly
    # one of the two payload fields must be present.
    has_payload = "payload" in envelope
    has_payload_hex = "payload_hex" in envelope
    if has_payload and has_payload_hex:
        raise MalformedStoredMessageError("Stored RMQ envelope must not contain both 'payload' and 'payload_hex'")
    if not has_payload and not has_payload_hex:
        raise MalformedStoredMessageError("Stored RMQ envelope is missing required 'payload' field")

    envelope_id = envelope["id"]
    if not isinstance(envelope_id, str):
        raise MalformedStoredMessageError("Stored RMQ envelope 'id' field must be a string")
    if has_payload_hex:
        payload_hex = envelope["payload_hex"]
        if not isinstance(payload_hex, str):
            raise MalformedStoredMessageError("Stored RMQ envelope 'payload_hex' field must be a string")
        try:
            payload_bytes = bytes.fromhex(payload_hex)
        except ValueError as exc:
            raise MalformedStoredMessageError(
                "Stored RMQ envelope 'payload_hex' field is not a valid hex byte string"
            ) from exc
        return envelope_id, payload_bytes
    payload = envelope["payload"]
    if not isinstance(payload, str):
        raise MalformedStoredMessageError("Stored RMQ envelope 'payload' field must be a string")
    return envelope_id, payload
