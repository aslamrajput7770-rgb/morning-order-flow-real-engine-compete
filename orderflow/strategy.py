"""Turn the order-flow analytics into explicit, evidence-backed trade signals.

The engine already computes delta, footprint imbalances, absorption, VWAP and
divergence. This module adds no new indicator. It composes the ones that exist
into one decision, and states the reason for every point of that decision's
score, so a trader can see *why* a signal fired and disagree with it.

Two rules shape the design:

* **No invented data.** A signal is only produced when the inputs it needs are
  present and unambiguous. A bar whose aggressor side is mostly `UNKNOWN`
  cannot produce a delta-confirmed entry, because the delta would be noise.
* **Every point has a reason.** `Signal.reasons` lists the evidence in plain
  words and `Signal.strength` is the weighted sum of the components. A signal
  below the configured strength is not returned at all.

What this module will not do is claim accuracy. It produces *disciplined*
entries: each one rests on named order-flow evidence, carries a stop and a
target, and refuses to fire on a bar that does not qualify. Nothing about a
signal is a prediction of profit.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Sequence

from .core import ImbalanceSide, Side
from .delta import detect_divergence
from .dom import Absorption, detect_absorption
from .footprint import FootprintBar
from .indicators import order_flow_vwap
from .ticks import TickGrid


@dataclass(frozen=True, slots=True)
class Signal:
    """One entry decision, with the evidence that produced it."""

    symbol: str
    side: Side
    ts: float
    price: float
    strength: float
    stop: float
    target: float
    reasons: tuple[str, ...]

    @property
    def is_long(self) -> bool:
        return self.side is Side.BUY

    @property
    def risk(self) -> float:
        """Distance from entry to stop, in price."""
        return abs(self.price - self.stop)

    @property
    def reward(self) -> float:
        """Distance from entry to target, in price."""
        return abs(self.target - self.price)

    @property
    def reward_risk(self) -> float:
        return self.reward / self.risk if self.risk > 0 else float("inf")

    def detail(self) -> str:
        return (
            f"{self.side.value} {self.symbol} @ {self.price:g} "
            f"stop {self.stop:g} target {self.target:g} "
            f"strength {self.strength:.2f} | " + "; ".join(self.reasons)
        )


@dataclass(slots=True)
class StrategyConfig:
    """Thresholds for the signal engine. Every default is deliberately strict.

    A permissive default is how a backtest looks brilliant and a live account
    bleeds: if the engine fires on weak evidence, the fills are real and the
    edge is not. Tighten rather than loosen until a capture proves otherwise.
    """

    imbalance_ratio: float = 3.0
    stack_min: int = 3
    #: Stacked levels required on one side before that side counts as aggression.
    min_stacked: int = 2
    #: Weights. They are summed and clamped to 1.0; a signal must clear
    #: `min_strength` to be returned.
    w_stacked: float = 0.35
    w_delta: float = 0.25
    w_vwap: float = 0.15
    w_absorption: float = 0.25
    w_divergence: float = 0.15
    min_strength: float = 0.60
    #: A delta-confirmed entry is refused when more than this fraction of the
    #: bar's volume has an unknown aggressor, because the delta is then noise.
    max_unknown_frac: float = 0.40
    #: Stops and targets are sized off the instrument's own recent range, not a
    #: fixed tick count. A fixed count is wrong across instruments: 20 ticks is
    #: 1.0 point on NIFTY at 23000 (0.004%, inside the spread) and 20.0 points
    #: on CRUDEOIL at 5700 (0.35%). The first is a guaranteed stop-out. Sizing
    #: off the average bar range makes the stop scale with what the instrument
    #: actually does.
    stop_atr_mult: float = 1.5
    target_atr_mult: float = 3.0
    atr_bars: int = 14
    #: Floor for the stop, in ticks, so a dead-quiet stretch cannot produce a
    #: stop smaller than the spread.
    min_stop_ticks: int = 8
    #: Used only when there is too little history for an ATR and the fixed path
    #: is explicitly requested.
    stop_ticks: int = 20
    target_ticks: int = 40
    #: Absorption only counts as evidence when it is at least this big.
    min_absorption_volume: float = 500.0
    #: A divergence older than this many bars is stale and is not used.
    divergence_max_bars: int = 6


class SignalEngine:
    """Evaluate completed bars and return at most one signal.

    The engine is stateless with respect to a decision: everything it needs is
    in the bars and trades it is handed. That keeps it identical in replay and
    live, which is the only way a backtest says anything about the live path.
    """

    def __init__(self, config: StrategyConfig | None = None) -> None:
        self.config = config or StrategyConfig()

    def evaluate(
        self,
        bars: Sequence[FootprintBar],
        grid: TickGrid,
        symbol: str = "",
        trades: Sequence | None = None,
    ) -> Signal | None:
        """Return the signal for the most recent bar, or None.

        `bars` must be in time order and end at the bar being judged. `trades`
        is optional; when given it enables the absorption component.
        """
        cfg = self.config
        if len(bars) < 2:
            return None
        bar = bars[-1]
        if bar.close is None or bar.total_volume <= 0:
            return None

        cum_delta = sum(b.delta for b in bars)
        stacked_ask, stacked_bid = self._stacked_counts(bar)
        unknown_frac = self._unknown_fraction(bar)

        vwap = self._last_vwap(bars)
        absorption = self._absorption(trades, grid)
        divergence = self._divergence(bars)

        long_pts, long_reasons = self._score(
            Side.BUY, bar, cum_delta, vwap, stacked_ask, stacked_bid,
            unknown_frac, absorption, divergence,
        )
        short_pts, short_reasons = self._score(
            Side.SELL, bar, cum_delta, vwap, stacked_bid, stacked_ask,
            unknown_frac, absorption, divergence,
        )

        if long_pts >= short_pts:
            side, pts, reasons = Side.BUY, long_pts, long_reasons
        else:
            side, pts, reasons = Side.SELL, short_pts, short_reasons

        if pts < cfg.min_strength:
            return None
        return self._make(side, bar, grid, symbol, min(pts, 1.0), reasons, bars=bars)

    # ------------------------------------------------------------- components
    def _stacked_counts(self, bar: FootprintBar) -> tuple[int, int]:
        cfg = self.config
        imbs = bar.imbalances(ratio=cfg.imbalance_ratio, stack_min=cfg.stack_min)
        ask = sum(1 for i in imbs if i.stacked and i.side is ImbalanceSide.ASK)
        bid = sum(1 for i in imbs if i.stacked and i.side is ImbalanceSide.BID)
        return ask, bid

    @staticmethod
    def _unknown_fraction(bar: FootprintBar) -> float:
        """Share of the bar's volume with no aggressor side.

        The reader marks a print UNKNOWN when price is unchanged and there is no
        book to lean on. When that share is large the delta is built on a 50/50
        split, not on real aggression, and it must not confirm an entry.
        """
        total = bar.total_volume
        if total <= 0:
            return 1.0
        # buy + sell are the sided volumes; the remainder is unknown.
        sided = bar.buy_volume + bar.sell_volume
        return max(0.0, 1.0 - sided / total)

    @staticmethod
    def _last_vwap(bars: Sequence[FootprintBar]) -> float | None:
        pts = order_flow_vwap(bars)
        return pts[-1].vwap if pts else None

    def _absorption(self, trades, grid) -> Absorption | None:
        if not trades:
            return None
        found = detect_absorption(
            trades, grid, min_volume=self.config.min_absorption_volume,
        )
        return found[-1] if found else None

    def _divergence(self, bars: Sequence[FootprintBar]):
        cfg = self.config
        divs = detect_divergence(bars)
        if not divs:
            return None
        recent = divs[-1]
        # Stale divergences are not evidence about the current bar.
        age = sum(1 for b in bars if b.end_ts > recent.to_ts)
        return recent if age <= cfg.divergence_max_bars else None

    def _score(
        self,
        side: Side,
        bar: FootprintBar,
        cum_delta: float,
        vwap: float | None,
        stacked_favourable: int,
        stacked_adverse: int,
        unknown_frac: float,
        absorption: Absorption | None,
        divergence,
    ) -> tuple[float, list[str]]:
        """Score one direction. `stacked_favourable` is the side we would trade.

        For a long, favourable stacked imbalances are ASK-side (buyers lifting
        offers). For a short they are BID-side.
        """
        cfg = self.config
        pts = 0.0
        reasons: list[str] = []
        close = bar.close
        assert close is not None

        if stacked_favourable >= cfg.min_stacked:
            pts += cfg.w_stacked
            reasons.append(
                f"{stacked_favourable} stacked {self._side_word(side)} imbalances"
            )
        if stacked_adverse >= cfg.min_stacked:
            # The other side is stacked too: this is a two-sided bar and the
            # signal is not clean. Penalise it rather than pretend.
            pts -= cfg.w_stacked
            reasons.append(
                f"opposing {stacked_adverse} stacked imbalances (conflict)"
            )

        bar_delta = bar.delta
        if unknown_frac <= cfg.max_unknown_frac:
            if side is Side.BUY and bar_delta > 0 and cum_delta > 0:
                pts += cfg.w_delta
                reasons.append(f"delta +{bar_delta:g} with cum delta +{cum_delta:g}")
            elif side is Side.SELL and bar_delta < 0 and cum_delta < 0:
                pts += cfg.w_delta
                reasons.append(f"delta {bar_delta:g} with cum delta {cum_delta:g}")

        if vwap is not None:
            if side is Side.BUY and close >= vwap:
                pts += cfg.w_vwap
                reasons.append(f"price {close:g} >= VWAP {vwap:.4f}")
            elif side is Side.SELL and close <= vwap:
                pts += cfg.w_vwap
                reasons.append(f"price {close:g} <= VWAP {vwap:.4f}")

        if absorption is not None:
            # Sellers absorbed at the low -> buyers are winning -> long.
            if side is Side.BUY and absorption.absorbed_side.upper() == "SELL":
                pts += cfg.w_absorption
                reasons.append(
                    f"sellers absorbed {absorption.volume:g} @ {absorption.price:g}"
                )
            elif side is Side.SELL and absorption.absorbed_side.upper() == "BUY":
                pts += cfg.w_absorption
                reasons.append(
                    f"buyers absorbed {absorption.volume:g} @ {absorption.price:g}"
                )

        if divergence is not None:
            if side is Side.BUY and divergence.kind == "BULLISH":
                pts += cfg.w_divergence
                reasons.append("bullish price/delta divergence")
            elif side is Side.SELL and divergence.kind == "BEARISH":
                pts += cfg.w_divergence
                reasons.append("bearish price/delta divergence")

        return pts, reasons

    @staticmethod
    def _side_word(side: Side) -> str:
        return "ask-side (buy)" if side is Side.BUY else "bid-side (sell)"

    def _make(
        self,
        side: Side,
        bar: FootprintBar,
        grid: TickGrid,
        symbol: str,
        strength: float,
        reasons: list[str],
        bars: Sequence[FootprintBar] | None = None,
    ) -> Signal:
        cfg = self.config
        assert bar.close is not None
        entry = bar.close
        step = grid.tick_size
        stop_dist, target_dist = self._stop_distances(bars, bar, grid)
        if side is Side.BUY:
            stop = entry - stop_dist
            target = entry + target_dist
        else:
            stop = entry + stop_dist
            target = entry - target_dist
        return Signal(
            symbol=symbol,
            side=side,
            ts=bar.end_ts,
            price=entry,
            strength=strength,
            stop=round(stop, 6),
            target=round(target, 6),
            reasons=tuple(reasons),
        )

    def _stop_distances(
        self,
        bars: Sequence[FootprintBar] | None,
        bar: FootprintBar,
        grid: TickGrid,
    ) -> tuple[float, float]:
        """Stop and target distances, sized off the average bar range.

        The average true range is computed from the bars handed in, which
        include the signal bar. It is a plain mean of the bar ranges, which for
        footprint bars is the honest measure of "how far does this instrument
        move in one bar". The stop is floored at `min_stop_ticks` so a quiet
        stretch cannot set a stop inside the spread, and the target keeps the
        configured reward:risk ratio.
        """
        cfg = self.config
        floor = cfg.min_stop_ticks * grid.tick_size
        atr = self._atr(bars or [], bar)
        if atr <= 0:
            # No history: fall back to the fixed ticks, which is what a caller
            # who set them expects.
            return (max(cfg.stop_ticks * grid.tick_size, floor),
                    max(cfg.target_ticks * grid.tick_size, floor * cfg.target_atr_mult / cfg.stop_atr_mult))
        stop = max(atr * cfg.stop_atr_mult, floor)
        target = stop * (cfg.target_atr_mult / cfg.stop_atr_mult)
        return stop, target

    def _atr(self, bars: Sequence[FootprintBar], fallback: FootprintBar) -> float:
        cfg = self.config
        window = list(bars)[-cfg.atr_bars:] if bars else [fallback]
        ranges = [
            (b.high - b.low) for b in window
            if b.high is not None and b.low is not None
        ]
        if not ranges:
            if fallback.high is None or fallback.low is None:
                return 0.0
            ranges = [fallback.high - fallback.low]
        return sum(ranges) / len(ranges) if ranges else 0.0


def evaluate_latest(
    bars: Sequence[FootprintBar],
    grid: TickGrid,
    symbol: str = "",
    config: StrategyConfig | None = None,
    trades: Sequence | None = None,
) -> Signal | None:
    """Convenience wrapper for a one-off evaluation."""
    return SignalEngine(config).evaluate(bars, grid, symbol=symbol, trades=trades)
