import json
import uuid
from dataclasses import dataclass

MessageData = str | bytes

_STORED_MESSAGE_PREFIX = "\x1eRMQ1:"


@dataclass(frozen=True)
class ClaimedMessage:
    stored_message: MessageData
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


def decode_stored_message(message: MessageData) -> MessageData:
    """Strip the stored-message envelope and return the original payload.

    Designed to consume values produced by ``encode_stored_message`` only.
    Calling this on a raw user-supplied string that happens to look like a
    valid envelope (matches the prefix and parses as a payload-bearing JSON
    object) will return the inner ``payload`` field — round-trip is preserved
    only when input came through ``encode_stored_message`` first. Built-in
    publish/consume always re-wraps so this footgun cannot fire end-to-end;
    custom gateways feeding raw input must wrap before decoding.
    """
    payload = _extract_payload(message)
    if payload is None:
        return message
    if isinstance(message, bytes):
        return payload.encode("utf-8")
    return payload


def extract_stored_message_id(message: MessageData) -> str | None:
    if isinstance(message, bytes):
        try:
            message = message.decode("utf-8")
        except UnicodeDecodeError:
            return None
    if not message.startswith(_STORED_MESSAGE_PREFIX):
        return None
    try:
        envelope = json.loads(message[len(_STORED_MESSAGE_PREFIX) :])
    except json.JSONDecodeError:
        return None
    if isinstance(envelope, dict) and isinstance(envelope.get("id"), str):
        return envelope["id"]
    return None


def _extract_payload(message: MessageData) -> str | None:
    if isinstance(message, bytes):
        try:
            message = message.decode("utf-8")
        except UnicodeDecodeError:
            return None

    if not message.startswith(_STORED_MESSAGE_PREFIX):
        return None

    try:
        envelope = json.loads(message[len(_STORED_MESSAGE_PREFIX) :])
    except json.JSONDecodeError:
        return None

    if not isinstance(envelope, dict):
        return None

    payload = envelope.get("payload")
    envelope_id = envelope.get("id")
    if not isinstance(payload, str) or not isinstance(envelope_id, str):
        return None
    return payload
