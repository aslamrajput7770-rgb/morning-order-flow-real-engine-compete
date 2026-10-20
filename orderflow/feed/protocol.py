"""Angel One SmartStream V2 binary protocol decoder.

Pure standard library: no network, no SDK, no dependencies. The decoder takes
raw WebSocket frame bytes and returns a structured tick. That split is what makes
the whole ingestion layer testable offline.

Protocol facts that matter
--------------------------
* One binary packet per WebSocket message. Every multi-byte field is
  LITTLE-ENDIAN.
* Prices are SCALED INTEGERS. NSE, BSE and MCX use a scale of 100, so the raw
  value 2400005 means 24000.05. Currency derivatives (CDE) use 10^7.
  Forgetting to divide is the single most common ingestion bug: every price
  lands on 2400005.00, no price is on the 0.05 tick grid, and the footprint is
  nonsense. `TickGrid.off_grid_count` catches this immediately.
* The token is a 25-byte NUL-terminated ASCII string, not an integer.

Layout, byte offsets within one packet:

  common header
    0   :  1    subscription_mode       B
    1   :  2    exchange_type           B
    2   :  27   token                   25-byte ASCII, NUL-terminated
    27  :  35   sequence_number         q
    35  :  43   exchange_timestamp      q   (epoch milliseconds)
    43  :  51   last_traded_price       q   (scaled integer)

  QUOTE (2) and SNAP_QUOTE (3) add
    51  :  59   last_traded_quantity    q
    59  :  67   average_traded_price    q
    67  :  75   volume_trade_for_the_day q
    75  :  83   total_buy_quantity      d
    83  :  91   total_sell_quantity     d
    91  :  99   open_price_of_the_day   q
    99  :  107  high_price_of_the_day   q
    107 :  115  low_price_of_the_day    q
    115 :  123  closed_price            q
    147 :  347  best_5_buy_and_sell     10 x 20 bytes

  SNAP_QUOTE (3) additionally
    123 :  131  last_traded_timestamp   q
    131 :  139  open_interest           q
    139 :  147  open_interest_change_percentage q
    347 :  355  upper_circuit_limit     q
    355 :  363  lower_circuit_limit     q
    363 :  371  52_week_high_price      q
    371 :  379  52_week_low_price       q

  best_5 entry (20 bytes): flag H, quantity q, price q, no_of_orders H

  DEPTH (4) replaces the payload:
    35  :  43   packet_received_time    q
    43  :  243  20 buy levels, 10 bytes each
    243 :  443  20 sell levels, 10 bytes each
    level (10 bytes): quantity i, price i, no_of_orders h
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from enum import IntEnum

_LE = "<"

#: Scale applied to every price field for NSE / BSE / MCX.
DEFAULT_PRICE_SCALE = 100
#: Scale for currency derivatives (CDE).
CDS_PRICE_SCALE = 10_000_000


class ProtocolError(ValueError):
    """Malformed or unsupported SmartStream packet."""


class PriceScaleError(ValueError):
    """A price is implausible for the instrument's price scale.

    The classic failure this catches: the scaled integer is never divided, so a
    print at 24000.05 arrives as 2400005.0. Note that the tick grid does NOT
    catch this: 2400005 is an exact multiple of 0.05, so it looks perfectly
    aligned and every level simply lands 100x too high. The footprint renders,
    the delta is computed, and all of it is wrong.
    """


class SubscriptionMode(IntEnum):
    LTP = 1
    QUOTE = 2
    SNAP_QUOTE = 3
    DEPTH = 4


class ExchangeType(IntEnum):
    NSE_CM = 1
    NSE_FO = 2
    BSE_CM = 3
    BSE_FO = 4
    MCX_FO = 5
    NCX_FO = 7
    CDE_FO = 13


#: Minimum packet length per mode. Anything shorter is a truncated frame.
MIN_PACKET_LENGTH: dict[SubscriptionMode, int] = {
    SubscriptionMode.LTP: 51,
    SubscriptionMode.QUOTE: 347,
    SubscriptionMode.SNAP_QUOTE: 379,
    SubscriptionMode.DEPTH: 443,
}

SUBSCRIBE_ACTION = 1
UNSUBSCRIBE_ACTION = 0


def price_scale(exchange_type: ExchangeType) -> int:
    """Integer scale applied to price fields for this exchange segment."""
    return CDS_PRICE_SCALE if exchange_type is ExchangeType.CDE_FO else DEFAULT_PRICE_SCALE


@dataclass(frozen=True, slots=True)
class DepthLevel:
    """One resting level. Price is still the raw scaled integer."""

    price_raw: int
    quantity: int
    orders: int


@dataclass(frozen=True, slots=True)
class RawTick:
    """A decoded packet. All prices are raw scaled integers, unscaled."""

    mode: SubscriptionMode
    exchange_type: ExchangeType
    token: str
    sequence_number: int | None = None
    exchange_timestamp: int | None = None  # epoch milliseconds
    last_traded_price: int | None = None
    last_traded_quantity: int | None = None
    average_traded_price: int | None = None
    volume_trade_for_the_day: int | None = None
    total_buy_quantity: float | None = None
    total_sell_quantity: float | None = None
    open_price_of_the_day: int | None = None
    high_price_of_the_day: int | None = None
    low_price_of_the_day: int | None = None
    closed_price: int | None = None
    last_traded_timestamp: int | None = None
    open_interest: int | None = None
    open_interest_change_percentage: int | None = None
    upper_circuit_limit: int | None = None
    lower_circuit_limit: int | None = None
    week_52_high_price: int | None = None
    week_52_low_price: int | None = None
    bids: tuple[DepthLevel, ...] = ()
    asks: tuple[DepthLevel, ...] = ()

    @property
    def scale(self) -> int:
        return price_scale(self.exchange_type)


def _unpack(fmt: str, data: bytes, start: int):
    return struct.unpack_from(_LE + fmt, data, start)[0]


def _parse_token(data: bytes) -> str:
    raw = data[2:27]
    nul = raw.find(b"\x00")
    if nul >= 0:
        raw = raw[:nul]
    return raw.decode("ascii", errors="replace")


def decode_packet(data: bytes) -> RawTick:
    """Decode one SmartStream V2 packet.

    Raises `ProtocolError` on a truncated frame or an unknown mode/exchange
    rather than guessing, because a guessed field offset produces plausible
    numbers that are silently wrong.
    """
    if len(data) < 3:
        raise ProtocolError(f"packet too short: {len(data)} bytes")

    try:
        mode = SubscriptionMode(data[0])
    except ValueError:
        raise ProtocolError(f"unknown subscription mode {data[0]}") from None
    try:
        exchange_type = ExchangeType(data[1])
    except ValueError:
        raise ProtocolError(f"unknown exchange type {data[1]}") from None

    need = MIN_PACKET_LENGTH[mode]
    if len(data) < need:
        raise ProtocolError(
            f"{mode.name} packet truncated: got {len(data)} bytes, need {need}"
        )

    token = _parse_token(data)

    if mode is SubscriptionMode.DEPTH:
        return _decode_depth(data, exchange_type, token)

    fields: dict = {
        "mode": mode,
        "exchange_type": exchange_type,
        "token": token,
        "sequence_number": _unpack("q", data, 27),
        "exchange_timestamp": _unpack("q", data, 35),
        "last_traded_price": _unpack("q", data, 43),
    }

    if mode in (SubscriptionMode.QUOTE, SubscriptionMode.SNAP_QUOTE):
        fields.update(
            last_traded_quantity=_unpack("q", data, 51),
            average_traded_price=_unpack("q", data, 59),
            volume_trade_for_the_day=_unpack("q", data, 67),
            total_buy_quantity=_unpack("d", data, 75),
            total_sell_quantity=_unpack("d", data, 83),
            open_price_of_the_day=_unpack("q", data, 91),
            high_price_of_the_day=_unpack("q", data, 99),
            low_price_of_the_day=_unpack("q", data, 107),
            closed_price=_unpack("q", data, 115),
        )
        bids, asks = _parse_best_five(data)
        fields["bids"] = bids
        fields["asks"] = asks

    if mode is SubscriptionMode.SNAP_QUOTE:
        fields.update(
            last_traded_timestamp=_unpack("q", data, 123),
            open_interest=_unpack("q", data, 131),
            open_interest_change_percentage=_unpack("q", data, 139),
            upper_circuit_limit=_unpack("q", data, 347),
            lower_circuit_limit=_unpack("q", data, 355),
            week_52_high_price=_unpack("q", data, 363),
            week_52_low_price=_unpack("q", data, 371),
        )

    return RawTick(**fields)


def _parse_best_five(data: bytes) -> tuple[tuple[DepthLevel, ...], tuple[DepthLevel, ...]]:
    """Parse the 10 x 20-byte best-five block at offset 147.

    The level flag is 0 for an ASK and 1 for a BID. That is the opposite of
    what the field name suggests, and it was settled on a live NIFTY packet,
    not from documentation. In that packet the flag==0 levels ran
    22904.8 -> 22907.0 and the flag==1 levels ran 22893.1 -> 22892.0. A bid is
    always below an ask, so the only uncrossed reading is flag==0 = ask.

    This is why the official `smartapi-python` SDK looks like it swaps the two
    lists: `_parse_best_5_buy_and_sell_data` files flag==0 under
    `best_5_buy_data`, then `_parse_binary_data` assigns `best_5_buy_data` from
    `best_5_sell_data` and vice versa. The swap is deliberate, not a copy/paste
    error, and the SDK ends up correct.

    Getting this wrong inverts every inferred trade side, because the quote
    rule compares each print against the best bid and the best ask.
    """
    bids: list[DepthLevel] = []
    asks: list[DepthLevel] = []
    for i in range(10):
        o = 147 + i * 20
        flag = _unpack("H", data, o)
        price_raw = _unpack("q", data, o + 10)
        # The block is fixed at 10 levels and padded with zeros when the book
        # is short. A price of zero is not a real level, and keeping it sets
        # best_ask to 0, which makes every print look like a buy at the ask.
        if price_raw <= 0:
            continue
        level = DepthLevel(
            price_raw=price_raw,
            quantity=_unpack("q", data, o + 2),
            orders=_unpack("H", data, o + 18),
        )
        (asks if flag == 0 else bids).append(level)
    return tuple(bids), tuple(asks)


def _decode_depth(data: bytes, exchange_type: ExchangeType, token: str) -> RawTick:
    """Depth-20 payload: 20 bid levels then 20 ask levels, 10 bytes each."""
    bids = []
    asks = []
    for i in range(20):
        o = 43 + i * 10
        bids.append(
            DepthLevel(
                price_raw=_unpack("i", data, o + 4),
                quantity=_unpack("i", data, o),
                orders=_unpack("h", data, o + 8),
            )
        )
    for i in range(20):
        o = 243 + i * 10
        asks.append(
            DepthLevel(
                price_raw=_unpack("i", data, o + 4),
                quantity=_unpack("i", data, o),
                orders=_unpack("h", data, o + 8),
            )
        )
    return RawTick(
        mode=SubscriptionMode.DEPTH,
        exchange_type=exchange_type,
        token=token,
        exchange_timestamp=_unpack("q", data, 35),
        bids=tuple(bids),
        asks=tuple(asks),
    )


def decode_stream(data: bytes) -> list[RawTick]:
    """Decode several concatenated packets from one frame.

    The V2 feed normally sends one packet per message, but a recorded buffer or
    a batched frame can carry several. This walks the buffer using the known
    packet length for the leading mode.
    """
    out: list[RawTick] = []
    offset = 0
    while offset < len(data):
        if len(data) - offset < 3:
            break
        try:
            mode = SubscriptionMode(data[offset])
        except ValueError:
            raise ProtocolError(
                f"unknown subscription mode {data[offset]} at offset {offset}"
            ) from None
        length = MIN_PACKET_LENGTH[mode]
        if len(data) - offset < length:
            raise ProtocolError(
                f"trailing {mode.name} packet truncated at offset {offset}"
            )
        out.append(decode_packet(data[offset : offset + length]))
        offset += length
    return out


def is_heartbeat(message: object) -> bool:
    """True for the non-binary heartbeat the server interleaves.

    The client sends the literal ``ping`` every 10 seconds; anything that is not
    a binary frame is not a tick and must be skipped, not parsed.
    """
    if isinstance(message, (bytes, bytearray)):
        return len(message) < 3
    return True
