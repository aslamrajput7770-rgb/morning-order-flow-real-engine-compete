"""DOM / market-depth: ladder, liquidity tracking, absorption detection.

A full SuperDOM needs order routing, which is broker-specific. What is modelled
here is the *analytical* part that is actually reusable: book snapshots,
liquidity-at-price, and absorption — the signature where size keeps trading
without price moving.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field

from .core import Side, TickGrid, Trade


@dataclass(frozen=True, slots=True)
class BookSnapshot:
    """Top-of-book ladder snapshot. Quantities are resting size at that price."""

    ts: float
    bids: tuple[tuple[float, float], ...]  # (price, size), best bid first
    asks: tuple[tuple[float, float], ...]  # (price, size), best ask first

    def __post_init__(self) -> None:
        if self.bids and self.asks:
            if self.bids[0][0] >= self.asks[0][0]:
                raise ValueError("crossed book: best bid >= best ask")

    @property
    def best_bid(self) -> float | None:
        return self.bids[0][0] if self.bids else None

    @property
    def best_ask(self) -> float | None:
        return self.asks[0][0] if self.asks else None

    @property
    def spread(self) -> float | None:
        if self.best_bid is None or self.best_ask is None:
            return None
        return self.best_ask - self.best_bid

    @property
    def mid(self) -> float | None:
        if self.best_bid is None or self.best_ask is None:
            return None
        return (self.best_bid + self.best_ask) / 2.0

    def bid_size(self, price: float) -> float:
        return next((s for p, s in self.bids if p == price), 0.0)

    def ask_size(self, price: float) -> float:
        return next((s for p, s in self.asks if p == price), 0.0)


@dataclass(slots=True)
class LiquidityMap:
    """Tracks resting liquidity per price from a stream of snapshots."""

    grid: TickGrid
    bid_liquidity: dict[int, float] = field(default_factory=dict)
    ask_liquidity: dict[int, float] = field(default_factory=dict)
    snapshots: int = 0

    def update(self, snap: BookSnapshot) -> None:
        self.snapshots += 1
        for price, size in snap.bids:
            i = self.grid.to_index(price)
            self.bid_liquidity[i] = size
        for price, size in snap.asks:
            i = self.grid.to_index(price)
            self.ask_liquidity[i] = size

    def largest_bid_wall(self) -> tuple[float, float] | None:
        if not self.bid_liquidity:
            return None
        i = max(self.bid_liquidity, key=lambda k: self.bid_liquidity[k])
        return self.grid.to_price(i), self.bid_liquidity[i]

    def largest_ask_wall(self) -> tuple[float, float] | None:
        if not self.ask_liquidity:
            return None
        i = max(self.ask_liquidity, key=lambda k: self.ask_liquidity[k])
        return self.grid.to_price(i), self.ask_liquidity[i]


@dataclass(frozen=True, slots=True)
class Absorption:
    """A price where heavy aggression failed to move the market."""

    price: float
    start_ts: float
    end_ts: float
    volume: float
    absorbed_side: str  # side of the aggressor that got absorbed
    range_ticks: float

    @property
    def detail(self) -> str:
        return (
            f"{self.absorbed_side} absorbed {self.volume:g} @ {self.price:g} "
            f"over {self.range_ticks:g} ticks"
        )


def detect_absorption(
    trades: Sequence[Trade],
    grid: TickGrid,
    min_volume: float = 500.0,
    max_range_ticks: float = 2.0,
    max_seconds: float = 30.0,
) -> list[Absorption]:
    """Find clusters of aggressive volume that failed to move price.

    A cluster is anchored at a price and keeps absorbing while two conditions
    hold: price stays within `max_range_ticks` of the anchor, and the cluster
    lasts no longer than `max_seconds`. Both bounds are required. Without the
    time bound, a session that chops inside two ticks for an hour collapses into
    one giant fake "absorption" event, which is exactly what a naive
    price-only implementation reports.
    """
    if not trades:
        return []
    if min_volume <= 0:
        raise ValueError("min_volume must be > 0")
    if max_seconds <= 0:
        raise ValueError("max_seconds must be > 0")

    out: list[Absorption] = []
    anchor_idx = grid.to_index(trades[0].price)
    vol = 0.0
    start = end = trades[0].ts
    lo_idx = hi_idx = anchor_idx
    agg_buy = agg_sell = 0.0

    def flush() -> None:
        if vol >= min_volume:
            out.append(
                Absorption(
                    price=grid.to_price(anchor_idx),
                    start_ts=start,
                    end_ts=end,
                    volume=vol,
                    absorbed_side="BUY" if agg_buy >= agg_sell else "SELL",
                    range_ticks=float(hi_idx - lo_idx),
                )
            )

    for t in trades:
        idx = grid.to_index(t.price)
        too_far = abs(idx - anchor_idx) > max_range_ticks
        too_long = (t.ts - start) > max_seconds
        if too_far or too_long:
            flush()
            anchor_idx = idx
            vol = 0.0
            start = t.ts
            lo_idx = hi_idx = idx
            agg_buy = agg_sell = 0.0
        vol += t.size
        if t.side is Side.BUY:
            agg_buy += t.size
        elif t.side is Side.SELL:
            agg_sell += t.size
        lo_idx = min(lo_idx, idx)
        hi_idx = max(hi_idx, idx)
        end = t.ts
    flush()
    return out


@dataclass(slots=True)
class SuperDOM:
    """Ladder view: price column with working orders and one-click actions.

    Order routing is intentionally left to the adapter layer; what is modelled
    here is the ladder state a UI needs to render and the "one-click" intent
    objects that a broker adapter would consume.
    """

    grid: TickGrid
    working: dict[int, float] = field(default_factory=dict)  # price idx -> signed qty
    position: float = 0.0
    realized_pnl: float = 0.0

    def place(self, price: float, qty: float) -> None:
        if qty == 0:
            raise ValueError("qty must be non-zero")
        i = self.grid.to_index(price)
        self.working[i] = self.working.get(i, 0.0) + qty

    def cancel(self, price: float) -> None:
        self.working.pop(self.grid.to_index(price), None)

    def ladder(self, depth: int = 5, around: float | None = None) -> list[tuple[float, float, float]]:
        """[(price, working_qty, position_at_price)] around a reference price."""
        if not self.working:
            return []
        ref = self.grid.to_index(around) if around is not None else min(self.working)
        idxs = range(ref - depth, ref + depth + 1)
        return [(self.grid.to_price(i), self.working.get(i, 0.0), 0.0) for i in idxs]

    def fill(self, price: float, qty: float) -> None:
        """Apply a fill: reduce working order and move position."""
        i = self.grid.to_index(price)
        if i in self.working:
            self.working[i] -= qty
            if abs(self.working[i]) < 1e-9:
                del self.working[i]
        self.position += qty


def liquidity_shift(snaps: Sequence[BookSnapshot], price: float) -> list[tuple[float, float]]:
    """Resting size at one price over time, for spotting pulls/stacks."""
    return [(s.ts, s.bid_size(price) + s.ask_size(price)) for s in snaps]
