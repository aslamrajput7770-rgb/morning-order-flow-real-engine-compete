"""Footprint aggregation: bid/ask volume per price level, per bar.

This is the heart of the tool. Everything else (delta, profile, imbalance,
absorption) is derived from the output of this module, so it is written to be
boring, deterministic and cheap.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field

from .core import (
    Imbalance,
    ImbalanceSide,
    PriceLevel,
    Side,
    TickGrid,
    Trade,
)


@dataclass(slots=True)
class FootprintBar:
    """One time bucket, with bid/ask volume split at every traded price."""

    start_ts: float
    end_ts: float
    grid: TickGrid
    levels: dict[int, PriceLevel] = field(default_factory=dict)
    open: float | None = None
    high: float | None = None
    low: float | None = None
    close: float | None = None
    trade_count: int = 0

    # ---------------------------------------------------------------- mutate
    def add(self, trade: Trade) -> None:
        idx = self.grid.to_index(trade.price)
        price = self.grid.to_price(idx)
        lvl = self.levels.get(idx)
        if lvl is None:
            lvl = self.levels[idx] = PriceLevel(price=price)
        lvl.add(trade.size, trade.side)

        self.trade_count += 1
        if self.open is None:
            self.open = price
            self.high = price
            self.low = price
        else:
            if price > self.high:  # type: ignore[operator]
                self.high = price
            if price < self.low:  # type: ignore[operator]
                self.low = price
        self.close = price

    # --------------------------------------------------------------- derived
    @property
    def sorted_levels(self) -> list[PriceLevel]:
        return [self.levels[i] for i in sorted(self.levels)]

    @property
    def poc(self) -> float | None:
        """Point of control: price with the highest total volume.

        Ties are broken toward the level closest to the bar's mid price, and
        finally toward the lower price. The final tie-break on price matters:
        iterating a dict returns insertion order, so without it the POC of two
        identical bars could differ depending on the order trades arrived.
        """
        if not self.levels:
            return None
        mid = ((self.high or 0.0) + (self.low or 0.0)) / 2.0
        best_idx = min(
            self.levels,
            key=lambda i: (
                -self.levels[i].total_vol,
                abs(self.levels[i].price - mid),
                self.levels[i].price,
            ),
        )
        return self.levels[best_idx].price

    @property
    def bid_volume(self) -> float:
        return sum(l.bid_vol for l in self.levels.values())

    @property
    def ask_volume(self) -> float:
        return sum(l.ask_vol for l in self.levels.values())

    @property
    def total_volume(self) -> float:
        return self.bid_volume + self.ask_volume

    @property
    def delta(self) -> float:
        return self.ask_volume - self.bid_volume

    @property
    def delta_pct(self) -> float:
        total = self.total_volume
        return 0.0 if total == 0 else 100.0 * self.delta / total

    @property
    def max_delta_level(self) -> PriceLevel | None:
        """Level with the highest absolute delta (buying/selling extreme)."""
        if not self.levels:
            return None
        return max(self.levels.values(), key=lambda l: abs(l.delta))

    @property
    def cumulative_delta(self) -> float:
        """Bar delta, expressed cumulatively by the caller. Alias kept explicit."""
        return self.delta

    # ------------------------------------------------------------- imbalance
    def imbalances(
        self,
        ratio: float = 3.0,
        min_volume: float = 0.0,
        stack_min: int = 3,
    ) -> list[Imbalance]:
        """Diagonal imbalance detection (the "3:1" footprint rule).

        A level is imbalanced when one side's volume is at least `ratio` times
        the opposite side at the *diagonally adjacent* price:

          * ask-side (buy) imbalance at level i: ask_vol[i] >= ratio * bid_vol[i-1]
          * bid-side (sell) imbalance at level i: bid_vol[i] >= ratio * ask_vol[i+1]

        The diagonal comparison (not same-level) is what distinguishes a real
        footprint imbalance from a plain delta print: it measures aggression
        against the liquidity that was available one tick away.

        Runs of >= `stack_min` consecutive same-side imbalances are flagged
        `stacked=True`, and those are the levels that matter for entries.
        """
        if ratio <= 1.0:
            raise ValueError("imbalance ratio must be > 1")

        idxs = sorted(self.levels)
        out: list[Imbalance] = []
        for pos, i in enumerate(idxs):
            lvl = self.levels[i]
            if lvl.total_vol < min_volume:
                continue

            if pos > 0:
                below = self.levels[idxs[pos - 1]]
                if lvl.ask_vol >= ratio * below.bid_vol and below.bid_vol > 0:
                    out.append(
                        Imbalance(
                            index=i,
                            price=lvl.price,
                            side=ImbalanceSide.ASK,
                            ratio=lvl.ask_vol / below.bid_vol,
                            dominant_vol=lvl.ask_vol,
                            weak_vol=below.bid_vol,
                        )
                    )
            if pos < len(idxs) - 1:
                above = self.levels[idxs[pos + 1]]
                if lvl.bid_vol >= ratio * above.ask_vol and above.ask_vol > 0:
                    out.append(
                        Imbalance(
                            index=i,
                            price=lvl.price,
                            side=ImbalanceSide.BID,
                            ratio=lvl.bid_vol / above.ask_vol,
                            dominant_vol=lvl.bid_vol,
                            weak_vol=above.ask_vol,
                        )
                    )

        return _flag_stacked(out, stack_min)

    def delta_flip(self) -> float | None:
        """Price where bar delta changes sign, scanning low -> high."""
        prev: PriceLevel | None = None
        for lvl in self.sorted_levels:
            if prev is not None and prev.delta != 0 and lvl.delta != 0:
                if (prev.delta > 0) != (lvl.delta > 0):
                    return lvl.price
            prev = lvl
        return None

    def render(self, mode: str = "bidask", width: int = 7) -> str:
        """ASCII footprint for one bar. Modes: bidask|volume|delta|deltapct."""
        rows = []
        poc = self.poc
        for lvl in reversed(self.sorted_levels):
            marker = "*" if lvl.price == poc else " "
            if mode == "bidask":
                body = f"{_fmt(lvl.bid_vol, width)} x {_fmt(lvl.ask_vol, width)}"
            elif mode == "volume":
                body = f"{_fmt(lvl.total_vol, 2 * width + 3):>{2 * width + 3}}"
            elif mode == "delta":
                body = f"{_fmt(lvl.delta, 2 * width + 3):>{2 * width + 3}}"
            elif mode == "deltapct":
                body = f"{lvl.delta_pct:>{2 * width + 3}.1f}"
            else:
                raise ValueError(f"unknown render mode: {mode}")
            rows.append(f"{lvl.price:>10.4f}{marker}{body}")
        head = (
            f"bar {self.start_ts:g}-{self.end_ts:g}  "
            f"O{self.open} H{self.high} L{self.low} C{self.close}  "
            f"vol={self.total_volume:g} delta={self.delta:g}"
        )
        return "\n".join([head, *rows])


def _fmt(v: float, width: int) -> str:
    """Compact, fixed-width number for the ASCII footprint."""
    n = int(round(v))
    s = f"{n:,}"
    return s if len(s) <= width else f"{v:.1e}"


def _flag_stacked(imbs: list[Imbalance], stack_min: int) -> list[Imbalance]:
    """Mark runs of >= stack_min consecutive same-side imbalances as stacked."""
    if stack_min < 2 or not imbs:
        return imbs

    # Group by side, preserving price order, then find contiguous runs.
    by_side: dict[ImbalanceSide, list[int]] = {}
    for pos, imb in enumerate(imbs):
        by_side.setdefault(imb.side, []).append(pos)

    stacked: set[int] = set()
    for positions in by_side.values():
        positions.sort(key=lambda p: imbs[p].index)
        run = [positions[0]]
        for p in positions[1:]:
            # Compare price indices, not list positions: run[-1] is an index
            # into `imbs`, so it must be dereferenced before the comparison.
            if abs(imbs[p].index - imbs[run[-1]].index) == 1:
                run.append(p)
            else:
                if len(run) >= stack_min:
                    stacked.update(run)
                run = [p]
        if len(run) >= stack_min:
            stacked.update(run)

    if not stacked:
        return imbs
    return [
        Imbalance(
            index=i.index,
            price=i.price,
            side=i.side,
            ratio=i.ratio,
            dominant_vol=i.dominant_vol,
            weak_vol=i.weak_vol,
            stacked=(pos in stacked),
        )
        for pos, i in enumerate(imbs)
    ]


@dataclass(slots=True)
class FootprintChart:
    """Streaming aggregator: trades in, footprint bars out."""

    grid: TickGrid
    bar_seconds: float = 60.0
    bars: list[FootprintBar] = field(default_factory=list)
    _current: FootprintBar | None = None

    def __post_init__(self) -> None:
        if self.bar_seconds <= 0:
            raise ValueError("bar_seconds must be > 0")

    def add(self, trade: Trade) -> FootprintBar:
        bucket = self.bar_seconds * (trade.ts // self.bar_seconds)
        if self._current is None or bucket != self._current.start_ts:
            self._current = FootprintBar(
                start_ts=bucket,
                end_ts=bucket + self.bar_seconds,
                grid=self.grid,
            )
            self.bars.append(self._current)
        self._current.add(trade)
        return self._current

    def add_many(self, trades: Iterable[Trade]) -> None:
        for t in trades:
            self.add(t)

    @property
    def current(self) -> FootprintBar | None:
        return self._current

    @property
    def cumulative_delta(self) -> list[tuple[float, float]]:
        """[(bar_end_ts, running_delta)] across all bars."""
        out = []
        running = 0.0
        for bar in self.bars:
            running += bar.delta
            out.append((bar.end_ts, running))
        return out
