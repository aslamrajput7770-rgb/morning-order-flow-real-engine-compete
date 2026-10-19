"""Support indicators: order-flow VWAP with bands, pressure, bar timer, scanner."""

from __future__ import annotations

import math
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field

from .core import Side, Trade
from .footprint import FootprintBar


@dataclass(frozen=True, slots=True)
class VWAPBand:
    mult: float
    upper: float
    lower: float


@dataclass(frozen=True, slots=True)
class VWAPPoint:
    ts: float
    vwap: float
    sd: float
    bands: tuple[VWAPBand, ...]
    total_volume: float

    @property
    def upper(self) -> float:
        """Outer upper band (largest multiplier)."""
        return max(self.bands, key=lambda b: b.mult).upper if self.bands else self.vwap

    @property
    def lower(self) -> float:
        """Outer lower band (largest multiplier)."""
        return max(self.bands, key=lambda b: b.mult).lower if self.bands else self.vwap


def order_flow_vwap(
    bars: Sequence[FootprintBar],
    bands: tuple[float, ...] = (1.0, 2.0),
    anchor_ts: float | None = None,
) -> list[VWAPPoint]:
    """VWAP with volume-weighted standard-deviation bands.

    `anchor_ts` anchors the VWAP at a given time (a news print, a session open).
    Without it, the VWAP anchors at the first bar in `bars`.

    Deviation is measured about the *volume-weighted* mean using price levels,
    so a 2-sigma tag means price is stretched relative to where volume actually
    traded. A plain price standard deviation would ignore volume and produce
    bands that are too tight on thin tape and too wide on thick tape.

    Bands are symmetric: each multiplier yields one upper and one lower band.
    """
    if not bands:
        raise ValueError("at least one band multiplier is required")
    if any(m <= 0 for m in bands):
        raise ValueError("band multipliers must be > 0")

    out: list[VWAPPoint] = []
    cum_pv = cum_pv2 = cum_vol = 0.0

    for bar in bars:
        if anchor_ts is not None and bar.start_ts < anchor_ts:
            continue
        for lvl in bar.levels.values():
            v = lvl.total_vol
            if v == 0:
                continue
            cum_pv += lvl.price * v
            cum_pv2 += lvl.price * lvl.price * v
            cum_vol += v
        if cum_vol == 0:
            continue
        vwap = cum_pv / cum_vol
        var = max(0.0, cum_pv2 / cum_vol - vwap * vwap)
        sd = math.sqrt(var)
        out.append(
            VWAPPoint(
                ts=bar.end_ts,
                vwap=vwap,
                sd=sd,
                bands=tuple(
                    VWAPBand(mult=m, upper=vwap + m * sd, lower=vwap - m * sd)
                    for m in sorted(set(bands))
                ),
                total_volume=cum_vol,
            )
        )
    return out


@dataclass(slots=True)
class PressureGauge:
    """Buy/sell pressure over a rolling window of the most recent trades.

    The window is a single stream of the last `window` prints, not a separate
    window per side. Per-side windows are a subtle bug: in a burst of 100 buys
    and 1 sell they would let a stale sell from long ago keep polluting the
    reading, and the ratio would stop responding to current flow.
    """

    window: int = 50
    _sizes: list[float] = field(default_factory=list)
    buy_volume: float = 0.0
    sell_volume: float = 0.0

    def __post_init__(self) -> None:
        if self.window <= 0:
            raise ValueError("window must be > 0")

    def add(self, trade: Trade) -> None:
        if trade.side is Side.BUY:
            self.buy_volume += trade.size
        elif trade.side is Side.SELL:
            self.sell_volume += trade.size
        self._sizes.append(trade.size if trade.side is Side.BUY else -trade.size)
        if len(self._sizes) > self.window:
            self._sizes.pop(0)

    def add_many(self, trades: Iterable[Trade]) -> None:
        for t in trades:
            self.add(t)

    @property
    def window_buy(self) -> float:
        return sum(s for s in self._sizes if s > 0)

    @property
    def window_sell(self) -> float:
        return -sum(s for s in self._sizes if s < 0)

    @property
    def ratio(self) -> float:
        """Window buy/sell ratio. 1.0 means balanced; inf means one-sided buying."""
        buy = self.window_buy
        sell = self.window_sell
        if sell == 0:
            return float("inf") if buy > 0 else 1.0
        return buy / sell

    @property
    def oscillator(self) -> float:
        """-100 (pure selling) .. +100 (pure buying) in the window."""
        buy = self.window_buy
        sell = self.window_sell
        total = buy + sell
        return 0.0 if total == 0 else 100.0 * (buy - sell) / total


@dataclass(frozen=True, slots=True)
class InstrumentSignal:
    """One row of the Market Analyzer scan."""

    symbol: str
    delta: float
    cumulative_delta: float
    total_volume: float
    poc: float | None
    last: float | None
    imbalance_count: int
    stacked_count: int

    @property
    def bias(self) -> str:
        if self.cumulative_delta > 0 and self.imbalance_count > 0:
            return "LONG"
        if self.cumulative_delta < 0 and self.imbalance_count > 0:
            return "SHORT"
        return "NEUTRAL"


def market_analyzer(
    instruments: dict[str, Sequence[FootprintBar]],
    ratio: float = 3.0,
    stack_min: int = 3,
) -> list[InstrumentSignal]:
    """Scan many instruments and rank by absolute cumulative delta.

    Ranking is on cumulative delta, not price change: the quote board already
    shows price. Delta tells you whether a move has real aggression behind it.
    """
    rows: list[InstrumentSignal] = []
    for symbol, bars in instruments.items():
        if not bars:
            continue
        cum = sum(b.delta for b in bars)
        last_bar = bars[-1]
        imbs = last_bar.imbalances(ratio=ratio, stack_min=stack_min)
        rows.append(
            InstrumentSignal(
                symbol=symbol,
                delta=last_bar.delta,
                cumulative_delta=cum,
                total_volume=sum(b.total_volume for b in bars),
                poc=last_bar.poc,
                last=last_bar.close,
                imbalance_count=len(imbs),
                stacked_count=sum(1 for i in imbs if i.stacked),
            )
        )
    rows.sort(key=lambda r: abs(r.cumulative_delta), reverse=True)
    return rows


def buy_sell_volume(bars: Sequence[FootprintBar]) -> list[tuple[float, float, float]]:
    """[(ts, buy_volume, sell_volume)] per bar."""
    return [(b.end_ts, b.ask_volume, b.bid_volume) for b in bars]
