"""Core data contracts for the order-flow engine.

Prices are snapped onto an integer tick grid before aggregation. Floats are
never used as dictionary keys here: 0.1 + 0.2 != 0.3, and a footprint that
silently merges two adjacent price levels is worse than no footprint at all.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class Side(str, Enum):
    """Aggressor side of a print."""

    BUY = "BUY"  # buyer lifted the offer -> volume lands on the ask
    SELL = "SELL"  # seller hit the bid -> volume lands on the bid
    UNKNOWN = "UNK"  # no aggressor info (auction print, late report)


@dataclass(frozen=True, slots=True)
class Trade:
    """A single tick / time-and-sales print."""

    ts: float
    price: float
    size: float
    side: Side = Side.UNKNOWN

    def __post_init__(self) -> None:
        if self.size <= 0:
            raise ValueError(f"trade size must be > 0, got {self.size}")
        if self.price <= 0:
            raise ValueError(f"trade price must be > 0, got {self.price}")


class TickGrid:
    """Maps float prices onto a discrete integer index at a fixed tick size."""

    __slots__ = ("tick_size", "_scale")

    def __init__(self, tick_size: float) -> None:
        if tick_size <= 0:
            raise ValueError("tick_size must be > 0")
        self.tick_size = float(tick_size)
        self._scale = 1.0 / self.tick_size

    def to_index(self, price: float) -> int:
        return int(round(price * self._scale))

    def to_price(self, index: int) -> float:
        return index / self._scale

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"TickGrid(tick_size={self.tick_size})"


@dataclass(slots=True)
class PriceLevel:
    """Aggregated bid/ask volume resting at one price inside one bar."""

    price: float
    bid_vol: float = 0.0  # sell-aggressor volume (hit the bid)
    ask_vol: float = 0.0  # buy-aggressor volume (lifted the offer)
    prints: int = 0

    @property
    def total_vol(self) -> float:
        return self.bid_vol + self.ask_vol

    @property
    def delta(self) -> float:
        return self.ask_vol - self.bid_vol

    @property
    def delta_pct(self) -> float:
        total = self.total_vol
        return 0.0 if total == 0 else 100.0 * self.delta / total

    def add(self, size: float, side: Side) -> None:
        if side is Side.BUY:
            self.ask_vol += size
        elif side is Side.SELL:
            self.bid_vol += size
        else:
            # No aggressor information: split evenly. Any other choice invents
            # signal that the feed did not provide.
            self.bid_vol += size / 2.0
            self.ask_vol += size / 2.0
        self.prints += 1


class ImbalanceSide(str, Enum):
    BID = "BID"  # bid side dominant (sellers getting absorbed)
    ASK = "ASK"  # ask side dominant (buyers getting absorbed)


@dataclass(frozen=True, slots=True)
class Imbalance:
    """A diagonal imbalance at a single price level."""

    index: int
    price: float
    side: ImbalanceSide
    ratio: float
    dominant_vol: float
    weak_vol: float
    stacked: bool = False


@dataclass(frozen=True, slots=True)
class Divergence:
    """Price vs cumulative-delta divergence between two pivots."""

    kind: str  # "BEARISH" | "BULLISH"
    from_ts: float
    to_ts: float
    from_price: float
    to_price: float
    from_delta: float
    to_delta: float

    @property
    def detail(self) -> str:
        return (
            f"{self.kind}: price {self.from_price:g}->{self.to_price:g} "
            f"while cum delta {self.from_delta:g}->{self.to_delta:g}"
        )


@dataclass(slots=True)
class ValueArea:
    """Volume value area computed over a set of price levels."""

    poc: float
    vah: float
    val: float
    total_vol: float
    poc_vol: float
    levels: tuple[PriceLevel, ...] = field(default_factory=tuple)

    @property
    def width(self) -> float:
        return self.vah - self.val
