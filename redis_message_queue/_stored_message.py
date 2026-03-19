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
                f"'lease_token' must be a str, got {type(self.lease_token).__name__}"
            )
        if not self.lease_token:
            raise ValueError("'lease_token' must be a non-empty string")
        if not isinstance(self.stored_message, (str, bytes)):
            raise TypeError(
                f"'stored_message' must be str or bytes, got {type(self.stored_message).__name__}"
            )


def encode_stored_message(message: str) -> str:
    envelope = {
        "id": uuid.uuid4().hex,
        "payload": message,
    }
    return f"{_STORED_MESSAGE_PREFIX}{json.dumps(envelope, separators=(',', ':'))}"


def decode_stored_message(message: MessageData) -> MessageData:
    payload = _extract_payload(message)
    if payload is None:
        return message
    if isinstance(message, bytes):
        return payload.encode("utf-8")
    return payload


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
