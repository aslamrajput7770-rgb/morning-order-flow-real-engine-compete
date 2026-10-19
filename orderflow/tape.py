"""Tape reading: time & sales, big trades, tick/volume counters."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field

from .core import Side, Trade


@dataclass(frozen=True, slots=True)
class TapePrint:
    ts: float
    price: float
    size: float
    side: Side
    cumulative: float


@dataclass(slots=True)
class Tape:
    """Time & Sales with a size filter and running counters.

    `min_size` is the "hide the noise" filter. Trades below it are counted but
    not emitted to the display, because on a liquid instrument 90% of prints are
    algos slicing 1-2 lots and they only add latency to the eye.
    """

    min_size: float = 0.0
    prints: list[TapePrint] = field(default_factory=list)
    _cum: float = 0.0
    buy_volume: float = 0.0
    sell_volume: float = 0.0
    tick_count: int = 0

    def __post_init__(self) -> None:
        if self.min_size < 0:
            raise ValueError("min_size must be >= 0")

    def add(self, trade: Trade) -> TapePrint | None:
        self._cum += trade.size
        self.tick_count += 1
        if trade.side is Side.BUY:
            self.buy_volume += trade.size
        elif trade.side is Side.SELL:
            self.sell_volume += trade.size

        if trade.size < self.min_size:
            return None
        p = TapePrint(
            ts=trade.ts,
            price=trade.price,
            size=trade.size,
            side=trade.side,
            cumulative=self._cum,
        )
        self.prints.append(p)
        return p

    def add_many(self, trades: Iterable[Trade]) -> None:
        for t in trades:
            self.add(t)

    @property
    def total_volume(self) -> float:
        return self.buy_volume + self.sell_volume

    @property
    def delta(self) -> float:
        return self.buy_volume - self.sell_volume

    @property
    def buy_pressure(self) -> float:
        """Buy volume as a percentage of total (50 = balanced)."""
        total = self.total_volume
        return 0.0 if total == 0 else 100.0 * self.buy_volume / total

    def big_trades(self, min_size: float) -> list[TapePrint]:
        """Prints at or above `min_size` — the iceberg/block candidates."""
        return [p for p in self.prints if p.size >= min_size]

    def largest(self, n: int = 10) -> list[TapePrint]:
        return sorted(self.prints, key=lambda p: p.size, reverse=True)[:n]


def big_trade_summary(prints: Sequence[TapePrint], min_size: float) -> dict[str, float]:
    """Aggregate large prints into a buy/sell summary."""
    big = [p for p in prints if p.size >= min_size]
    buy = sum(p.size for p in big if p.side is Side.BUY)
    sell = sum(p.size for p in big if p.side is Side.SELL)
    return {
        "count": len(big),
        "buy_volume": buy,
        "sell_volume": sell,
        "net": buy - sell,
        "total": buy + sell,
    }


@dataclass(slots=True)
class Counter:
    """Bar timer + tick/volume counter for the current bar."""

    bar_seconds: float
    _start: float | None = None
    ticks: int = 0
    volume: float = 0.0

    def __post_init__(self) -> None:
        if self.bar_seconds <= 0:
            raise ValueError("bar_seconds must be > 0")

    def add(self, trade: Trade) -> bool:
        """Returns True when this trade rolled over into a new bar."""
        bucket = self.bar_seconds * (trade.ts // self.bar_seconds)
        rolled = bucket != self._start
        if rolled:
            self._start = bucket
            self.ticks = 0
            self.volume = 0.0
        self.ticks += 1
        self.volume += trade.size
        return rolled

    def elapsed(self, now: float) -> float:
        if self._start is None:
            return 0.0
        return max(0.0, now - self._start)

    def remaining(self, now: float) -> float:
        return max(0.0, self.bar_seconds - self.elapsed(now))
