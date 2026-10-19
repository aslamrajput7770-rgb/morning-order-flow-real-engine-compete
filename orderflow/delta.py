"""Delta suite: bar delta, cumulative delta, divergence, delta profile.

Divergence detection is deliberately pivot-based rather than "last two bars".
Comparing adjacent bars produces a signal on every other tick and is useless in
practice; real divergences are measured between swing points.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field

from .core import Divergence, TickGrid, ValueArea
from .footprint import FootprintBar
from .profile import Profile


@dataclass(frozen=True, slots=True)
class Candle:
    """One bar of cumulative delta, ready to be plotted as a candle."""

    ts: float
    open: float
    high: float
    low: float
    close: float
    delta: float


def cumulative_delta_series(bars: Sequence[FootprintBar]) -> list[Candle]:
    """Cumulative delta as OHLC candles.

    Each bar's candle opens at the previous running total and closes at the new
    running total; high/low track the extremes reached *inside* the bar. Without
    intra-bar data we approximate with the two endpoints plus the bar's own
    min/max cumulative excursions, which is what most platforms do.
    """
    out: list[Candle] = []
    running = 0.0
    for bar in bars:
        o = running
        running += bar.delta
        c = running
        hi = max(o, c)
        lo = min(o, c)
        out.append(Candle(ts=bar.end_ts, open=o, high=hi, low=lo, close=c, delta=bar.delta))
    return out


def bar_delta_series(bars: Sequence[FootprintBar]) -> list[tuple[float, float]]:
    return [(b.end_ts, b.delta) for b in bars]


def delta_profile(bars: Sequence[FootprintBar], grid: TickGrid) -> Profile:
    """Delta aggregated by price across bars (delta histogram)."""
    prof = Profile(start_ts=bars[0].start_ts, end_ts=bars[-1].end_ts, grid=grid) if bars else Profile(0.0, 0.0, grid)
    for bar in bars:
        for lvl in bar.levels.values():
            prof.add(lvl)
    return prof


def _pivots(bars: Sequence[FootprintBar], left: int, right: int) -> list[tuple[int, str]]:
    """Swing highs/lows using `left`/`right` bars on each side."""
    out: list[tuple[int, str]] = []
    for i in range(left, len(bars) - right):
        window = bars[i - left : i + right + 1]
        hi = bars[i].high
        lo = bars[i].low
        if hi is None or lo is None:
            continue
        others = [b for b in window if b is not bars[i]]
        if all((o.high is not None and hi >= o.high) for o in others):
            out.append((i, "H"))
        if all((o.low is not None and lo <= o.low) for o in others):
            out.append((i, "L"))
    return out


def detect_divergence(
    bars: Sequence[FootprintBar],
    left: int = 3,
    right: int = 3,
) -> list[Divergence]:
    """Regular price/delta divergence between consecutive same-type pivots.

    * bearish: price makes a higher high, cumulative delta makes a lower high
    * bullish: price makes a lower low, cumulative delta makes a higher low

    Delta used is cumulative delta at the pivot bar, which is the reading a
    trader actually sees on the indicator pane.
    """
    if len(bars) < left + right + 2:
        return []

    cum = {c.ts: c.close for c in cumulative_delta_series(bars)}
    pivots = _pivots(bars, left, right)
    out: list[Divergence] = []

    for kind in ("H", "L"):
        seq = [p for p in pivots if p[1] == kind]
        for (i1, _), (i2, _) in zip(seq, seq[1:]):
            b1, b2 = bars[i1], bars[i2]
            d1 = cum[b1.end_ts]
            d2 = cum[b2.end_ts]
            if kind == "H":
                if b2.high > b1.high and d2 < d1:  # type: ignore[operator]
                    out.append(_mk("BEARISH", b1, b2, d1, d2, b1.high, b2.high))
            else:
                if b2.low < b1.low and d2 > d1:  # type: ignore[operator]
                    out.append(_mk("BULLISH", b1, b2, d1, d2, b1.low, b2.low))
    return out


def _mk(kind, b1, b2, d1, d2, p1, p2) -> Divergence:
    return Divergence(
        kind=kind,
        from_ts=b1.end_ts,
        to_ts=b2.end_ts,
        from_price=float(p1),
        to_price=float(p2),
        from_delta=d1,
        to_delta=d2,
    )


@dataclass(slots=True)
class DeltaState:
    """Rolling delta state, handy for a live indicator pane."""

    cumulative: float = 0.0
    history: list[float] = field(default_factory=list)

    def update(self, bar: FootprintBar) -> float:
        self.cumulative += bar.delta
        self.history.append(self.cumulative)
        return self.cumulative

    def reset(self) -> None:
        self.cumulative = 0.0
        self.history.clear()


def value_area_of_delta(bars: Sequence[FootprintBar], grid: TickGrid, pct: float = 70.0) -> ValueArea:
    return delta_profile(bars, grid).value_area(pct)
