"""Offline SmartStream packet generator.

Encodes real SmartStream V2 binary packets, so the full ingestion path can be
exercised with no broker, no credentials and no network. The encoder is the
inverse of `protocol.decode_packet`; a round-trip test over both is the strongest
check that the byte offsets are right.

This is also how a captured production packet should be turned into a regression
test: dump the bytes once, keep them as a fixture, and assert the decoded values
forever after.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass

from .protocol import (
    DEFAULT_PRICE_SCALE,
    MIN_PACKET_LENGTH,
    ExchangeType,
    SubscriptionMode,
    DepthLevel,
)

_LE = "<"


@dataclass(slots=True)
class TickSpec:
    """Input to the encoder. Prices are real prices, not scaled integers."""

    mode: SubscriptionMode
    exchange_type: ExchangeType
    token: str
    exchange_timestamp_ms: int
    last_traded_price: float
    sequence_number: int = 1
    last_traded_quantity: int = 0
    volume_trade_for_the_day: int = 0
    total_buy_quantity: float = 0.0
    total_sell_quantity: float = 0.0
    open_price: float = 0.0
    high_price: float = 0.0
    low_price: float = 0.0
    closed_price: float = 0.0
    last_traded_timestamp_ms: int = 0
    open_interest: int = 0
    open_interest_change_percentage: int = 0
    upper_circuit_limit: float = 0.0
    lower_circuit_limit: float = 0.0
    week_52_high: float = 0.0
    week_52_low: float = 0.0
    bids: tuple[tuple[float, int], ...] = ()
    asks: tuple[tuple[float, int], ...] = ()
    scale: int = DEFAULT_PRICE_SCALE


def _scale(price: float, scale: int) -> int:
    # Use string-based rounding so 24000.05 * 100 is 2400005, not 2400004.99...
    from decimal import Decimal

    return int((Decimal(str(price)) * scale).to_integral_value())


def _token_bytes(token: str) -> bytes:
    raw = token.encode("ascii")
    if len(raw) > 25:
        raise ValueError("token longer than 25 bytes")
    return raw + b"\x00" * (25 - len(raw))


def _best_five_block(spec: TickSpec) -> bytes:
    """10 levels x 20 bytes at offset 147: flag H, qty q, price q, orders H.

    flag 0 marks an ASK and flag 1 marks a BID, which is inverted from the
    naming. Settled on a live NIFTY packet: the flag==0 levels sat above the
    flag==1 levels, and a bid is always below an ask. The decoder and the
    official SDK both read it this way.
    """
    block = bytearray()
    for price, qty in spec.asks:
        block += struct.pack(_LE + "HqqH", 0, qty, _scale(price, spec.scale), 1)
    for price, qty in spec.bids:
        block += struct.pack(_LE + "HqqH", 1, qty, _scale(price, spec.scale), 1)
    block += b"\x00" * (200 - len(block))
    return bytes(block)


def encode_tick(spec: TickSpec) -> bytes:
    """Encode one SmartStream V2 packet."""
    mode = spec.mode
    buf = bytearray(MIN_PACKET_LENGTH[mode])
    struct.pack_into(_LE + "B", buf, 0, int(mode))
    struct.pack_into(_LE + "B", buf, 1, int(spec.exchange_type))
    buf[2:27] = _token_bytes(spec.token)

    if mode is SubscriptionMode.DEPTH:
        struct.pack_into(_LE + "q", buf, 35, spec.exchange_timestamp_ms)
        for i, (price, qty) in enumerate(spec.bids[:20]):
            o = 43 + i * 10
            struct.pack_into(_LE + "iih", buf, o, qty, _scale(price, spec.scale), 1)
        for i, (price, qty) in enumerate(spec.asks[:20]):
            o = 243 + i * 10
            struct.pack_into(_LE + "iih", buf, o, qty, _scale(price, spec.scale), 1)
        return bytes(buf)

    struct.pack_into(_LE + "q", buf, 27, spec.sequence_number)
    struct.pack_into(_LE + "q", buf, 35, spec.exchange_timestamp_ms)
    struct.pack_into(_LE + "q", buf, 43, _scale(spec.last_traded_price, spec.scale))

    if mode in (SubscriptionMode.QUOTE, SubscriptionMode.SNAP_QUOTE):
        struct.pack_into(_LE + "q", buf, 51, spec.last_traded_quantity)
        struct.pack_into(_LE + "q", buf, 59, _scale(spec.last_traded_price, spec.scale))
        struct.pack_into(_LE + "q", buf, 67, spec.volume_trade_for_the_day)
        struct.pack_into(_LE + "d", buf, 75, spec.total_buy_quantity)
        struct.pack_into(_LE + "d", buf, 83, spec.total_sell_quantity)
        struct.pack_into(_LE + "q", buf, 91, _scale(spec.open_price, spec.scale))
        struct.pack_into(_LE + "q", buf, 99, _scale(spec.high_price, spec.scale))
        struct.pack_into(_LE + "q", buf, 107, _scale(spec.low_price, spec.scale))
        struct.pack_into(_LE + "q", buf, 115, _scale(spec.closed_price, spec.scale))
        buf[147:347] = _best_five_block(spec)

    if mode is SubscriptionMode.SNAP_QUOTE:
        struct.pack_into(_LE + "q", buf, 123, spec.last_traded_timestamp_ms)
        struct.pack_into(_LE + "q", buf, 131, spec.open_interest)
        struct.pack_into(_LE + "q", buf, 139, spec.open_interest_change_percentage)
        struct.pack_into(_LE + "q", buf, 347, _scale(spec.upper_circuit_limit, spec.scale))
        struct.pack_into(_LE + "q", buf, 355, _scale(spec.lower_circuit_limit, spec.scale))
        struct.pack_into(_LE + "q", buf, 363, _scale(spec.week_52_high, spec.scale))
        struct.pack_into(_LE + "q", buf, 371, _scale(spec.week_52_low, spec.scale))

    return bytes(buf)


def encode_stream(specs) -> list[bytes]:
    """Encode several specs, one frame each (the feed sends one per message)."""
    return [encode_tick(s) for s in specs]
