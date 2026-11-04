"""A custom codec, to show that the wire format is yours to choose."""

import base64
import binascii

from asynk import DecodeError, Envelope, JsonCodec


class Base64JsonCodec:
    """JSON wrapped in base64, so messages on the socket are not human-readable.

    A codec only has to turn an Envelope into bytes and back. Producer and
    consumer must use the same one. The unix transport also needs the bytes to
    contain no newline, since a newline ends a message; base64 guarantees that.
    """

    def __init__(self) -> None:
        self._json = JsonCodec()

    def encode(self, envelope: Envelope) -> bytes:
        return base64.b64encode(self._json.encode(envelope))

    def decode(self, data: bytes) -> Envelope:
        try:
            raw = base64.b64decode(data, validate=True)
        except binascii.Error as exc:
            # Raise DecodeError for bad input; the consumer drops such messages.
            raise DecodeError(f"invalid base64: {exc}") from exc
        return self._json.decode(raw)
