from __future__ import annotations

from typing import Protocol

from asynk.envelope import Envelope


class DecodeError(ValueError):
    """Raised when bytes cannot be decoded into a valid Envelope."""


class EncodeError(ValueError):
    """Raised when an Envelope holds a value the codec cannot encode."""


class Codec(Protocol):
    """Converts envelopes to and from the bytes a transport carries."""

    def validate(self, envelope: Envelope) -> None:
        """Checks that `encode` would accept an envelope, without encoding it.

        Args:
            envelope: The envelope to check.

        Raises:
            EncodeError: If the envelope holds a value this codec cannot encode.
        """
        ...

    def encode(self, envelope: Envelope) -> bytes:
        """Serializes an envelope.

        Args:
            envelope: The envelope to serialize.

        Returns:
            The encoded envelope.

        Raises:
            EncodeError: If the envelope holds a value this codec cannot encode.
        """
        ...

    def decode(self, data: bytes) -> Envelope:
        """Deserializes an envelope.

        Args:
            data: Bytes previously produced by `encode`.

        Returns:
            The decoded envelope.

        Raises:
            DecodeError: If `data` is not a valid encoded envelope.
        """
        ...
