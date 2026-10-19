"""Turn SmartStream ticks into engine `Trade`s.

The hard part, and the reason this module exists
------------------------------------------------
Angel One SmartStream does NOT send order-by-order trades. In QUOTE and
SNAP_QUOTE mode you get `last_traded_price`, `last_traded_quantity` and a
cumulative `volume_trade_for_the_day`. There is no aggressor flag.

A footprint needs two things per print:

1. **Incremental size.** The feed gives a cumulative day volume. Feeding that
   straight into a footprint puts the whole day's volume into every bar. The
   incremental size is ``volume - previous_volume``.

2. **Aggressor side.** The feed does not say. It must be inferred. Two
   mechanisms are available, and this module uses both:

   * *Tick rule* (Lee-Ready style): a print at a higher price than the previous
     print is buyer-initiated; a lower price is seller-initiated. This is the
     fallback and needs no order book.
   * *Quote rule*: compare the print price against the prevailing best bid and
     best ask. A print at or above the best ask is a buy; at or below the best
     bid is a sell. This is more accurate and is used whenever a book snapshot
     is available.

   When both agree, confidence is high. When they disagree, or when the price is
   unchanged and there is no book, the side is genuinely unknown and is marked
   `Side.UNKNOWN`, which the engine splits 50/50 rather than inventing a
   direction. **Do not silently guess**: a footprint whose delta is built on
   invented sides looks fine and is worthless.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from ..core import Side, Trade
from .protocol import PriceScaleError, RawTick, SubscriptionMode


class SideSource(str, Enum):
    QUOTE_RULE = "quote_rule"
    TICK_RULE = "tick_rule"
    BOTH = "both"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class TickTrade:
    """A `Trade` plus provenance for the inferred side."""

    trade: Trade
    source: SideSource
    price_change: int  # ticks vs previous print
    incremental_volume: float


@dataclass(slots=True)
class TickReaderState:
    """Per-token running state. One per instrument, never shared."""

    last_volume: float = 0.0
    last_price_raw: int | None = None
    best_bid_raw: int | None = None
    best_ask_raw: int | None = None
    prints: int = 0
    volume_resets: int = 0


class TickReader:
    """Converts a stream of `RawTick` for one token into `Trade`s.

    Usage:

        reader = TickReader(instrument, mapper)
        for raw in decoded_ticks:
            tt = reader.read(raw)
            if tt is not None:
                session_chart.add(tt.trade)

    Volume resets
    -------------
    At the session open the day volume restarts at zero. If a new tick's
    cumulative volume is *below* the previous one, the session rolled (or the
    feed restarted) and the delta must be taken as the new absolute value, not
    as a negative increment. Getting this wrong injects a large negative print
    exactly at the open, which is where the profile is most sensitive.
    """

    def __init__(self, token: str, scale: int = 100) -> None:
        self.token = token
        self.scale = scale
        self.state = TickReaderState()
        self._price_checked = False
        # Upper bound on a plausible *unscaled* price. The most expensive NSE
        # equity is around 150,000; no Indian instrument trades near 1,000,000.
        # Keeping this tight is the whole point: a 100x scale error yields
        # 2400005, which must be rejected.
        self.max_plausible_price = 1_000_000.0
        self.min_plausible_price = 0.01

    # ------------------------------------------------------------- side logic
    def _infer_side(
        self,
        price_raw: int,
        state: TickReaderState,
    ) -> tuple[Side, SideSource]:
        quote_side: Side | None = None
        if state.best_ask_raw is not None and price_raw >= state.best_ask_raw:
            quote_side = Side.BUY
        elif state.best_bid_raw is not None and price_raw <= state.best_bid_raw:
            quote_side = Side.SELL

        tick_side: Side | None = None
        if state.last_price_raw is not None:
            if price_raw > state.last_price_raw:
                tick_side = Side.BUY
            elif price_raw < state.last_price_raw:
                tick_side = Side.SELL

        if quote_side is not None and tick_side is not None:
            if quote_side is tick_side:
                return quote_side, SideSource.BOTH
            # Book and tick rule disagree: the tick may be a stale reprint.
            # Trust the book, which is the stronger signal.
            return quote_side, SideSource.QUOTE_RULE
        if quote_side is not None:
            return quote_side, SideSource.QUOTE_RULE
        if tick_side is not None:
            return tick_side, SideSource.TICK_RULE
        return Side.UNKNOWN, SideSource.UNKNOWN

    # ------------------------------------------------------------------ read
    def read(self, raw: RawTick) -> TickTrade | None:
        """Convert one tick. Returns None when there is no new trade.

        A tick carries no new trade when the cumulative volume is unchanged:
        that is a quote update (the book moved, no contract changed hands) and
        must not be added to the footprint as a zero or repeated print.
        """
        if raw.token != self.token:
            raise ValueError(f"tick for token {raw.token!r}, reader owns {self.token!r}")
        if raw.last_traded_price is None:
            return None

        state = self.state
        price_raw = raw.last_traded_price
        scale = raw.scale

        # Guard against the un-divided scaled price. This runs once per reader,
        # because a wrong scale is a configuration error, not a transient one.
        # `self.scale` must agree with the packet's own scale: if they differ,
        # the reader was configured for the wrong instrument.
        if not self._price_checked:
            if self.scale != scale:
                raise PriceScaleError(
                    f"token {self.token}: reader configured for scale {self.scale} "
                    f"but the packet reports {scale}"
                )
            price = price_raw / scale
            if not (self.min_plausible_price <= price <= self.max_plausible_price):
                raise PriceScaleError(
                    f"token {self.token}: price {price_raw} at scale {scale} gives "
                    f"{price}, which is implausible. The price field is probably "
                    "not being divided by the exchange scale."
                )
            self._price_checked = True

        cumulative = float(raw.volume_trade_for_the_day or 0.0)
        if cumulative < state.last_volume:
            # Session rollover or feed restart: treat the new value as the first
            # increment rather than a negative one.
            state.volume_resets += 1
            increment = cumulative
        else:
            increment = cumulative - state.last_volume

        price_change = (
            0
            if state.last_price_raw is None
            else price_raw - state.last_price_raw
        )

        # Refresh the book context from THIS tick before inferring the side, so
        # the quote rule compares the print against the book it traded into
        # rather than the previous tick's book.
        if raw.bids:
            state.best_bid_raw = raw.bids[0].price_raw
        if raw.asks:
            state.best_ask_raw = raw.asks[0].price_raw

        side, source = self._infer_side(price_raw, state)

        if increment <= 0:
            # No contract changed hands; quote update only. Still refresh the
            # last price, but emit nothing.
            state.last_price_raw = price_raw
            state.last_volume = cumulative
            return None

        trade = Trade(
            ts=(raw.exchange_timestamp or 0) / 1000.0,
            price=price_raw / scale,
            size=increment,
            side=side,
        )
        state.last_volume = cumulative
        state.last_price_raw = price_raw
        state.prints += 1
        return TickTrade(
            trade=trade,
            source=source,
            price_change=price_change,
            incremental_volume=increment,
        )

    def read_many(self, ticks) -> list[TickTrade]:
        out = []
        for t in ticks:
            tt = self.read(t)
            if tt is not None:
                out.append(tt)
        return out


class SnapshotModeUnsupported(Exception):
    """Raised when LTP mode is used, which carries no volume to aggregate."""


def require_volume_capable_mode(mode: SubscriptionMode) -> None:
    """LTP mode has no volume field, so it cannot build a footprint.

    This is a guard, not a nicety: subscribing in LTP mode and feeding the
    engine produces an empty chart with no error, which reads as "the market is
    quiet" instead of "the subscription is wrong".
    """
    if mode is SubscriptionMode.LTP:
        raise SnapshotModeUnsupported(
            "LTP mode carries no volume; subscribe in QUOTE or SNAP_QUOTE mode "
            "to build volumetric bars"
        )
