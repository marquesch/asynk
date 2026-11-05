from __future__ import annotations

from typing import Protocol


class DecodeError(ValueError):
    """Raised when bytes cannot be decoded into a valid value."""


class EncodeError(ValueError):
    """Raised when a value holds something the codec cannot encode."""


class Codec(Protocol):
    """Converts values to and from bytes."""

    def validate(self, value: object) -> None:
        """Checks that `encode` would accept a value, without encoding it.

        Args:
            value: The value to check.

        Raises:
            EncodeError: If this codec cannot encode `value`.
        """
        ...

    def encode(self, value: object) -> bytes:
        """Serializes a value.

        Args:
            value: The value to serialize.

        Returns:
            The encoded value.

        Raises:
            EncodeError: If this codec cannot encode `value`.
        """
        ...

    def decode(self, data: bytes) -> object:
        """Deserializes a value.

        Args:
            data: Bytes previously produced by `encode`.

        Returns:
            The decoded value.

        Raises:
            DecodeError: If `data` is not a valid encoded value.
        """
        ...
