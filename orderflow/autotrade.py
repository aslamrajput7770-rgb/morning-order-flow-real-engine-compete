"""The autonomous loop: bars in, decisions out, positions managed.

`AutoTrader` is the piece that makes the platform hands-off. It owns, per
instrument, a session chart, a rolling window of recent prints, and the paper
broker. On every print it:

1. folds the print into the session chart (so bars, delta, profile stay live);
2. lets the broker fill anything resting and marks the open position;
3. checks an open position against its stop and target, and exits if hit;
4. when a bar *completes*, asks the signal engine whether to enter.

Two invariants keep this honest:

* **One position per instrument.** A second signal while a position is open is
  ignored, not averaged into. Averaging into a losing order-flow trade is how a
  small loss becomes a large one.
* **A completed bar, not the forming one.** A signal is only ever computed on a
  bar whose window has closed. Judging a forming bar means the last print can
  flip the decision, and the entry you backtest is not the entry you get.

The same object drives a replay and a live socket, because it consumes prints
and nothing else. That is what lets a capture validate the live path.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field

from .core import Side, Trade
from .footprint import FootprintBar
from .paper import Fill, PaperBroker
from .session import Market, MarketCalendar
from .session_chart import SessionChart
from .strategy import Signal, SignalEngine, StrategyConfig
from .ticks import TickGrid


@dataclass(slots=True)
class InstrumentState:
    """Everything the auto-trader tracks for one instrument."""

    symbol: str
    grid: TickGrid
    chart: SessionChart
    broker: PaperBroker
    engine: SignalEngine
    #: Recent prints, for the absorption detector. Bounded, so a long session
    #: cannot grow this without limit.
    recent: deque = field(default_factory=lambda: deque(maxlen=4000))
    #: end_ts of the last bar already judged, so a bar is judged once.
    last_judged_end: float = 0.0
    #: Symbol is not entered again until this timestamp.
    cooldown_until: float = 0.0
    #: The stop and target of the currently open position.
    stop: float | None = None
    target: float | None = None
    entries: int = 0
    exits: int = 0


@dataclass(slots=True)
class AutoTraderConfig:
    strategy: StrategyConfig = field(default_factory=StrategyConfig)
    #: Seconds to wait after an exit before a new entry on the same instrument.
    cooldown_seconds: float = 300.0
    #: Lots per entry. Kept at one by default; scaling is a separate decision.
    lots: int = 1
    #: Stop and target in ticks, overriding the strategy's defaults when set.
    stop_ticks: int | None = None
    target_ticks: int | None = None
    #: Bar length. Declared here rather than set on the instance because this
    #: dataclass uses slots: an undeclared attribute would raise.
    bar_seconds: float = 300.0
    #: Starting paper cash, per instrument. Used by the broker factory.
    cash: float = 1_000_000.0


class AutoTrader:
    """Feed it prints; it manages positions and emits decisions."""

    def __init__(
        self,
        calendar_for,
        config: AutoTraderConfig | None = None,
        broker_factory=None,
        bar_seconds: float = 300.0,
        on_signal=None,
        on_exit=None,
    ) -> None:
        """`calendar_for(symbol, market)` returns a MarketCalendar.

        `broker_factory()` returns a fresh PaperBroker per instrument, so each
        instrument's exposure is accounted for separately.
        """
        self.calendar_for = calendar_for
        self.config = config or AutoTraderConfig()
        self.broker_factory = broker_factory or (lambda: PaperBroker())
        self.bar_seconds = bar_seconds
        self.on_signal = on_signal
        self.on_exit = on_exit
        self.instruments: dict[str, InstrumentState] = {}

    # ------------------------------------------------------------ registration
    def register(self, symbol: str, market: Market, tick_size: float,
                 lot_size: int, segment: str | None = None) -> InstrumentState:
        grid = TickGrid(tick_size)
        calendar = self.calendar_for(symbol, market, segment)
        broker = self.broker_factory()
        broker.register(symbol, lot_size=lot_size, tick_size=tick_size)
        engine = SignalEngine(self.config.strategy)
        state = InstrumentState(
            symbol=symbol, grid=grid, broker=broker, engine=engine,
            chart=SessionChart(grid, calendar, bar_seconds=self.bar_seconds),
        )
        self.instruments[symbol] = state
        return state

    # ------------------------------------------------------------------- feed
    def on_trade(self, symbol: str, trade: Trade) -> list[Fill]:
        """One print. Fills orders, manages the position, judges new bars."""
        state = self.instruments.get(symbol)
        if state is None:
            return []

        fills: list[Fill] = []
        state.chart.add(trade)
        state.recent.append(trade)
        state.broker.mark(symbol, trade.price)
        fills.extend(state.broker.on_trade(symbol, trade.price, trade.ts, trade.size))

        if state.chart.state is None:
            # Off-session print. The chart already counted and dropped it; do
            # not trade on a print the session does not recognise.
            return fills

        self._manage_position(state, trade)
        self._maybe_enter(state, trade)
        return fills

    # ------------------------------------------------------------ position mgmt
    def _manage_position(self, state: InstrumentState, trade: Trade) -> None:
        pos = state.broker.positions[state.symbol]
        if pos.is_flat or state.stop is None or state.target is None:
            return
        price = trade.price
        if pos.qty > 0:
            hit_stop = price <= state.stop
            hit_target = price >= state.target
        else:
            hit_stop = price >= state.stop
            hit_target = price <= state.target
        if not (hit_stop or hit_target):
            return

        # Exit at the trigger, not at the print that blew past it: in a fast
        # market the fill is at least as bad as the level, never better.
        exit_side = Side.SELL if pos.qty > 0 else Side.BUY
        order = state.broker.submit_market(
            state.symbol, exit_side, self._lots_for(state),
            trade.ts, reason="stop" if hit_stop else "target",
        )
        # A market exit must not rest: fill it on this print directly, priced at
        # the worse of the trigger and the print.
        trigger = state.stop if hit_stop else state.target
        fill_price = state.broker._worse(exit_side, trigger, state.symbol)
        if exit_side is Side.BUY:
            fill_price = max(fill_price, price)
        else:
            fill_price = min(fill_price, price)
        state.broker._fill(order, fill_price, trade.ts)

        state.exits += 1
        state.stop = None
        state.target = None
        state.cooldown_until = trade.ts + self.config.cooldown_seconds
        if self.on_exit is not None:
            self.on_exit(state, "stop" if hit_stop else "target")

    # ------------------------------------------------------------------ entries
    def _maybe_enter(self, state: InstrumentState, trade: Trade) -> None:
        pos = state.broker.positions[state.symbol]
        if not pos.is_flat:
            return
        if trade.ts < state.cooldown_until:
            return

        bars = state.chart.bars
        if len(bars) < 2:
            return
        # Judge the last *completed* bar. The final element is the forming one,
        # so the completed bar is the second from the end.
        completed = bars[-2]
        if completed.end_ts <= state.last_judged_end:
            return
        state.last_judged_end = completed.end_ts

        signal = state.engine.evaluate(
            bars[:-1], state.grid, symbol=state.symbol, trades=list(state.recent),
        )
        if signal is None:
            return
        self._enter(state, signal, trade.ts, trade.price)

    def _enter(self, state: InstrumentState, signal: Signal, ts: float,
               live_price: float) -> None:
        """Enter at the live print, not at the signal's bar close.

        The signal is computed from the completed bar, but the fill happens on
        the next print, which can be several ticks away. Filling at the bar
        close would be look-ahead: it books an entry at a price that is no
        longer available. The honest reference is the print we are standing on.
        """
        order = state.broker.submit_market(
            state.symbol, signal.side, self._lots_for(state), ts,
            reason=f"signal strength {signal.strength:.2f}",
        )
        fill_price = state.broker._worse(signal.side, live_price, state.symbol)
        state.broker._fill(order, fill_price, ts)

        # Keep the signal's own stop/target distance, but measure it from the
        # actual fill. The signal sized them off the signal bar's close; the
        # fill can be a tick or two away, and the risk must be measured from
        # where we actually got in, not from where the signal fired.
        stop_dist = signal.risk
        target_dist = signal.reward
        if self.config.stop_ticks is not None:
            stop_dist = self.config.stop_ticks * state.grid.tick_size
        if self.config.target_ticks is not None:
            target_dist = self.config.target_ticks * state.grid.tick_size
        if signal.side is Side.BUY:
            state.stop = fill_price - stop_dist
            state.target = fill_price + target_dist
        else:
            state.stop = fill_price + stop_dist
            state.target = fill_price - target_dist
        state.entries += 1
        if self.on_signal is not None:
            self.on_signal(state, signal, fill_price)

    def _lots_for(self, state: InstrumentState) -> int:
        return self.config.lots

    # ------------------------------------------------------------------- views
    def all_positions(self) -> list[dict]:
        """Open positions only. A flat instrument is not a position."""
        out: list[dict] = []
        for state in self.instruments.values():
            pos = state.broker.positions[state.symbol]
            if pos.is_flat:
                continue
            out.append({
                "symbol": state.symbol,
                "qty": pos.qty,
                "avg_price": round(pos.avg_price, 4),
                "last_price": round(pos.last_price, 4),
                "unrealized": round(pos.unrealized_pnl, 2),
                "stop": state.stop,
                "target": state.target,
            })
        return out

    def account(self) -> dict:
        cash = sum(s.broker.cash for s in self.instruments.values())
        realized = sum(s.broker.realized_pnl for s in self.instruments.values())
        unrealized = sum(s.broker.unrealized_pnl for s in self.instruments.values())
        costs = sum(s.broker.total_costs for s in self.instruments.values())
        return {
            "cash": round(cash, 2),
            "equity": round(cash + unrealized, 2),
            "realized_pnl": round(realized, 2),
            "unrealized_pnl": round(unrealized, 2),
            "costs": round(costs, 2),
            "net_pnl": round(realized + unrealized - costs, 2),
            "entries": sum(s.entries for s in self.instruments.values()),
            "exits": sum(s.exits for s in self.instruments.values()),
            "round_trips": [
                {
                    "symbol": r.symbol, "side": r.side.value, "qty": r.qty,
                    "entry_price": round(r.entry_price, 4),
                    "exit_price": round(r.exit_price, 4),
                    "gross": round(r.gross, 2), "costs": round(r.costs, 2),
                    "net": round(r.net, 2),
                }
                for s in self.instruments.values()
                for r in s.broker.round_trips
            ],
        }

    def bars_for(self, symbol: str) -> list[FootprintBar]:
        state = self.instruments.get(symbol)
        return state.chart.bars if state else []
