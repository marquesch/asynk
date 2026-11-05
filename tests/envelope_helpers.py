"""Test helpers that move envelopes through byte-moving transports."""

from __future__ import annotations

from typing import Any

from asynk.codec import Codec
from asynk.codec.json import JsonCodec
from asynk.envelope import Envelope
from asynk.transport import Delivery, Producer


def encode_envelope(envelope: Envelope, codec: Codec | None = None) -> bytes:
    """Encodes an envelope with `codec`, a `JsonCodec` by default."""
    return (codec or JsonCodec()).encode(envelope.to_dict())


def decode_delivery(delivery: Delivery, codec: Codec | None = None) -> Envelope:
    """Decodes the envelope a delivery carries with `codec`, a `JsonCodec` by default."""
    return Envelope.from_dict((codec or JsonCodec()).decode(delivery.body))


async def publish_envelope(
    producer: Producer, envelope: Envelope, codec: Codec | None = None
) -> Any:
    """Encodes an envelope and publishes it, returning the producer's receipt."""
    return await producer.publish(envelope, encode_envelope(envelope, codec))
