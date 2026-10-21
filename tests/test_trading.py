"""Tests for the strategy signal layer, paper broker, auto-trader, supervisor.

These exercise real code paths, not mocks: bars are built by the real
FootprintChart, fills are driven by real Trade objects, and the supervisor is
driven by a fake clock but a real connector-shaped object.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from orderflow.core import Side, TickGrid, Trade
from orderflow.footprint import FootprintChart
from orderflow.paper import CostModel, OrderStatus, OrderType, PaperBroker
from orderflow.strategy import SignalEngine, StrategyConfig
from orderflow.session import IST, Market, MarketCalendar


# --------------------------------------------------------------- helpers
def stacked_buy_bar(grid, base=100.0, ts=0.0, levels=4, ask=1000, bid=10):
    """One bar with stacked ask-side (buy) imbalances across `levels` prices.

    Each level holds heavy buy volume against a thin bid at the level below, so
    the diagonal ask_vol[i] >= 3 * bid_vol[i-1] holds at every step and the run
    is flagged stacked. The bar closes at its high, which is what a bar built on
    sustained buying actually looks like.
    """
    chart = FootprintChart(grid, bar_seconds=60)
    lo = grid.to_index(base)
    for i in range(levels):
        price = grid.to_price(lo + i)
        for _ in range(int(bid)):
            chart.add(Trade(ts=ts, price=price, size=1, side=Side.SELL))
        for _ in range(int(ask)):
            chart.add(Trade(ts=ts, price=price, size=1, side=Side.BUY))
    return chart.bars[0]


# --------------------------------------------------------------- strategy
class TestSignalEngine:
    def test_no_signal_without_enough_bars(self):
        grid = TickGrid(0.05)
        engine = SignalEngine()
        assert engine.evaluate([], grid) is None

    def test_strong_buy_stack_produces_a_long_with_reasons(self):
        """Stacked ask-side imbalances plus positive delta and VWAP -> LONG."""
        grid = TickGrid(0.05)
        bars = [
            stacked_buy_bar(grid, base=100.0 + n * 0.05, ts=n * 60.0)
            for n in range(3)
        ]
        engine = SignalEngine(StrategyConfig(min_strength=0.5))
        sig = engine.evaluate(bars, grid, symbol="TEST")
        assert sig is not None
        assert sig.side is Side.BUY
        assert sig.reasons, "a signal must carry its evidence"
        assert sig.stop < sig.price < sig.target
        assert sig.reward_risk == pytest.approx(2.0, rel=1e-6)

    def test_two_sided_bar_is_not_a_clean_signal(self):
        """When both sides are stacked, the conflict must lower the score."""
        grid = TickGrid(0.05)
        bars = []
        for n in range(3):
            chart = FootprintChart(grid, bar_seconds=60)
            lo = grid.to_index(100.0)
            for i in range(4):
                price = grid.to_price(lo + i)
                for _ in range(1000):
                    chart.add(Trade(ts=n * 60.0, price=price, size=1, side=Side.BUY))
                    chart.add(Trade(ts=n * 60.0, price=price, size=1, side=Side.SELL))
            bars.append(chart.bars[0])
        engine = SignalEngine(StrategyConfig(min_strength=0.6))
        sig = engine.evaluate(bars, grid)
        # A perfectly balanced book should not clear a strict bar.
        assert sig is None

    def test_unknown_heavy_bar_cannot_confirm_on_delta(self):
        """Delta built on UNKNOWN sides is noise and must not score."""
        grid = TickGrid(0.05)
        chart = FootprintChart(grid, bar_seconds=60)
        for n in range(3):
            for _ in range(200):
                # UNKNOWN side: the reader had no book and no price change.
                chart.add(Trade(ts=n * 60.0, price=100.0, size=1, side=Side.UNKNOWN))
        bars = chart.bars
        assert len(bars) == 3
        engine = SignalEngine(StrategyConfig(min_strength=0.1))
        sig = engine.evaluate(bars, grid)
        if sig is not None:
            assert not any("delta" in r for r in sig.reasons), (
                "delta must not appear when the aggressor side is unknown")

    def test_stop_scales_with_the_instrument_not_a_fixed_tick_count(self):
        """A high-priced instrument must not get a stop inside its spread.

        The bug this guards: a fixed 20-tick stop on NIFTY at 23000 is 1.0
        point, which the spread alone will hit. Sizing off the bar range makes
        the stop scale with the instrument.
        """
        grid = TickGrid(0.05)
        # Bars with a ~50-point range, as a real NIFTY 5-minute bar has.
        bars = []
        for n in range(6):
            chart = FootprintChart(grid, bar_seconds=60)
            base = 23000.0 + n * 0.05
            lo = grid.to_index(base)
            for i in range(1000):
                price = grid.to_price(lo + i)
                for _ in range(20):
                    chart.add(Trade(ts=n * 60.0, price=price, size=1, side=Side.BUY))
                for _ in range(1):
                    chart.add(Trade(ts=n * 60.0, price=price, size=1, side=Side.SELL))
            bars.append(chart.bars[0])
        engine = SignalEngine(StrategyConfig(min_strength=0.5))
        sig = engine.evaluate(bars, grid, symbol="NIFTY")
        assert sig is not None
        # The stop must be far wider than the old fixed 20 ticks (1.0 point).
        assert sig.risk > 1.0, "a fixed-tick stop would be inside the spread"
        assert sig.risk >= 8 * 0.05, "the stop must clear the configured floor"


# --------------------------------------------------------------- paper broker
class TestPaperBroker:
    def test_market_buy_fills_worse_than_the_print(self):
        broker = PaperBroker(cash=1_000_000.0, slippage_ticks=1)
        broker.register("X", lot_size=75, tick_size=0.05)
        broker.submit_market("X", Side.BUY, 1, ts=0.0)
        fills = broker.on_trade("X", 100.0, 0.0)
        assert len(fills) == 1
        assert fills[0].price == pytest.approx(100.05), "buy must pay up one tick"

    def test_market_sell_fills_worse_than_the_print(self):
        broker = PaperBroker(cash=1_000_000.0, slippage_ticks=1)
        broker.register("X", lot_size=75, tick_size=0.05)
        broker.submit_market("X", Side.SELL, 1, ts=0.0)
        fills = broker.on_trade("X", 100.0, 0.0)
        assert fills[0].price == pytest.approx(99.95), "sell must hit down one tick"

    def test_costs_are_charged_and_never_zero(self):
        broker = PaperBroker(cash=1_000_000.0)
        broker.register("X", lot_size=75, tick_size=0.05)
        broker.submit_market("X", Side.BUY, 1, ts=0.0)
        broker.on_trade("X", 100.0, 0.0)
        assert broker.total_costs > 0, "a real account always pays something"

    def test_round_trip_pnl_is_net_of_costs(self):
        broker = PaperBroker(cash=1_000_000.0, slippage_ticks=0)
        broker.register("X", lot_size=10, tick_size=0.05)
        broker.submit_market("X", Side.BUY, 1, ts=0.0)
        broker.on_trade("X", 100.0, 0.0)
        broker.submit_market("X", Side.SELL, 1, ts=1.0)
        broker.on_trade("X", 110.0, 1.0)
        rt = broker.round_trips[-1]
        assert rt.gross == pytest.approx(100.0)  # (110-100)*10
        assert rt.net == pytest.approx(rt.gross - rt.costs)
        assert rt.net < rt.gross, "costs must reduce the round trip"

    def test_limit_order_only_fills_when_price_reaches_it(self):
        broker = PaperBroker(cash=1_000_000.0)
        broker.register("X", lot_size=1, tick_size=0.05)
        broker.submit_limit("X", Side.BUY, 1, price=99.0, ts=0.0)
        assert broker.on_trade("X", 100.0, 0.0) == []  # not reached
        fills = broker.on_trade("X", 98.95, 1.0)
        assert len(fills) == 1 and fills[0].price == pytest.approx(99.0)

    def test_stop_order_arms_then_fills(self):
        broker = PaperBroker(cash=1_000_000.0)
        broker.register("X", lot_size=1, tick_size=0.05)
        broker.submit_stop("X", Side.BUY, 1, price=101.0, ts=0.0)
        assert broker.on_trade("X", 100.0, 0.0) == []
        fills = broker.on_trade("X", 101.0, 1.0)
        assert len(fills) == 1

    def test_zero_lots_is_rejected_not_silently_ignored(self):
        broker = PaperBroker(cash=1_000_000.0)
        broker.register("X", lot_size=75, tick_size=0.05)
        order = broker.submit_market("X", Side.BUY, 0, ts=0.0)
        assert order.status is OrderStatus.REJECTED
        assert broker.rejected == 1

    def test_limit_without_price_is_rejected(self):
        broker = PaperBroker(cash=1_000_000.0)
        broker.register("X", lot_size=1, tick_size=0.05)
        order = broker.submit_limit("X", Side.BUY, 1, price=0.0, ts=0.0)
        assert order.status is OrderStatus.REJECTED

    def test_position_marks_and_unrealized(self):
        broker = PaperBroker(cash=1_000_000.0, slippage_ticks=0)
        broker.register("X", lot_size=10, tick_size=0.05)
        broker.submit_market("X", Side.BUY, 1, ts=0.0)
        broker.on_trade("X", 100.0, 0.0)
        broker.mark("X", 105.0)
        pos = broker.positions["X"]
        assert pos.qty == 10
        assert pos.unrealized_pnl == pytest.approx(50.0)

    def test_cash_and_equity_track_fills(self):
        broker = PaperBroker(cash=1_000_000.0, slippage_ticks=0)
        broker.register("X", lot_size=10, tick_size=0.05)
        broker.submit_market("X", Side.BUY, 1, ts=0.0)
        broker.on_trade("X", 100.0, 0.0)
        broker.mark("X", 110.0)
        assert broker.equity == pytest.approx(1_000_000.0 - broker.total_costs + 100.0)

    def test_cost_breakdown_sums_to_total(self):
        cm = CostModel()
        parts = cm.breakdown(Side.SELL, 100.0, 75.0)
        assert parts["total"] == pytest.approx(sum(
            v for k, v in parts.items() if k != "total"))


# --------------------------------------------------------------- auto-trader
class TestAutoTrader:
    def _trader(self):
        from orderflow.autotrade import AutoTrader, AutoTraderConfig
        from orderflow.paper import PaperBroker as PB

        def calendar_for(symbol, market, segment=None):
            return MarketCalendar(market, segment=segment or "NSE_FNO")

        cfg = AutoTraderConfig()
        cfg.strategy.min_strength = 0.5
        cfg.cooldown_seconds = 0.0
        return AutoTrader(calendar_for, cfg, broker_factory=lambda: PB(cash=1e6),
                          bar_seconds=60.0)

    def _session_ts(self, minutes=0):
        d = datetime(2026, 10, 21, 10, 0, tzinfo=IST)
        return d + timedelta(minutes=minutes)

    def test_registers_and_creates_a_session_chart(self):
        at = self._trader()
        st = at.register("NIFTY", Market.NSE, tick_size=0.05, lot_size=75)
        assert st.grid.tick_size == 0.05
        assert st.broker.lot_size("NIFTY") == 75

    def test_off_session_print_does_not_trade(self):
        at = self._trader()
        at.register("NIFTY", Market.NSE, tick_size=0.05, lot_size=75)
        # 04:00 IST is well outside the NSE session.
        early = datetime(2026, 10, 21, 4, 0, tzinfo=IST).timestamp()
        at.on_trade("NIFTY", Trade(ts=early, price=23000.0, size=75, side=Side.BUY))
        assert at.account()["entries"] == 0

    def test_stop_loss_exits_at_the_stop(self):
        at = self._trader()
        st = at.register("NIFTY", Market.NSE, tick_size=0.05, lot_size=75)
        # Open a long directly, then drive price through the stop.
        st.broker.submit_market("NIFTY", Side.BUY, 1, ts=self._session_ts().timestamp())
        ts = self._session_ts().timestamp()
        st.broker._fill(st.broker.orders[-1], 23000.0, ts)
        st.stop = 22990.0
        st.target = 23020.0
        at.on_trade("NIFTY", Trade(ts=ts + 1, price=22985.0, size=75, side=Side.SELL))
        assert at.account()["exits"] == 1
        assert st.broker.positions["NIFTY"].is_flat

    def test_one_position_per_instrument(self):
        """A second entry while a position is open must be refused."""
        at = self._trader()
        st = at.register("NIFTY", Market.NSE, tick_size=0.05, lot_size=75)
        ts = self._session_ts().timestamp()
        st.broker.submit_market("NIFTY", Side.BUY, 1, ts=ts)
        st.broker._fill(st.broker.orders[-1], 23000.0, ts)
        # Now drive a strong bar; the trader must not add.
        for i in range(120):
            at.on_trade("NIFTY", Trade(
                ts=ts + 60 + i * 0.1, price=23000.0 + i * 0.05, size=75, side=Side.BUY))
        assert at.account()["entries"] == 0, "must not average into an open position"

    def test_account_snapshot_is_json_safe(self):
        import json

        at = self._trader()
        at.register("NIFTY", Market.NSE, tick_size=0.05, lot_size=75)
        json.dumps(at.account())  # must not raise


# --------------------------------------------------------------- supervisor
class TestAlwaysOnFeed:
    def _calendar(self):
        return MarketCalendar(Market.NSE, segment="NSE_FNO")

    #: A moment NSE F&O is open on a Wednesday, so a test that pins its clock
    #: here is not at the mercy of when the suite happens to run. The two tests
    #: below drive a fake connector that calls `stop()` on its first run, which
    #: can only happen if the supervisor believes the market is open.
    OPEN_AT = datetime(2026, 10, 21, 11, 0, tzinfo=IST).timestamp()

    def _open_clock(self):
        return lambda: self.OPEN_AT

    def test_stop_is_the_only_way_out(self):
        from orderflow.supervisor import AlwaysOnFeed

        calls = {"n": 0}

        class FakeConn:
            def run(self, on_trade=None, max_seconds=None, reconnect=True):
                calls["n"] += 1
                feed.stop()
                return type("S", (), {"frames": 0, "reconnects": 0})()

        feed = AlwaysOnFeed(self._calendar(), lambda: FakeConn(),
                            clock=self._open_clock())
        feed.run()  # must return only because the fake called stop()
        assert calls["n"] == 1
        assert feed.health.stopped

    def test_connector_exception_is_survived_and_retried(self):
        from orderflow.supervisor import AlwaysOnFeed, SupervisorConfig

        calls = {"n": 0}

        class FlakyConn:
            def run(self, on_trade=None, max_seconds=None, reconnect=True):
                calls["n"] += 1
                if calls["n"] >= 3:
                    feed.stop()
                raise RuntimeError("boom")

        cfg = SupervisorConfig(backoff_min=0.0, backoff_max=0.0)
        feed = AlwaysOnFeed(self._calendar(), lambda: FlakyConn(), cfg,
                            clock=self._open_clock(), sleep=lambda s: None)
        feed.run()
        assert calls["n"] >= 3, "the supervisor must keep trying after a failure"

    def test_closed_market_waits_instead_of_exiting(self):
        from orderflow.supervisor import AlwaysOnFeed, SupervisorConfig

        # A clock pinned to 03:00 IST, before the NSE open, that advances so the
        # wait loop can see the open and then stop.
        base = datetime(2026, 10, 21, 3, 0, tzinfo=IST).timestamp()
        state = {"t": base}

        def clock():
            return state["t"]

        def fake_sleep(seconds):
            state["t"] += max(seconds, 60.0)
            if state["t"] > base + 24 * 3600:
                feed.stop()

        class FakeConn:
            def run(self, on_trade=None, max_seconds=None, reconnect=True):
                feed.stop()
                return type("S", (), {"frames": 0, "reconnects": 0})()

        cfg = SupervisorConfig(closed_poll_seconds=60.0)
        feed = AlwaysOnFeed(self._calendar(), lambda: FakeConn(), cfg,
                            clock=clock, sleep=fake_sleep)
        feed.run()
        assert feed.health.stopped
        # It must have been in a waiting state at some point, not streaming.
        assert feed.health.attempts >= 1 or feed.health.next_open_at is not None

    def test_snapshot_is_json_safe(self):
        import json

        from orderflow.supervisor import AlwaysOnFeed

        feed = AlwaysOnFeed(self._calendar(), lambda: None)
        json.dumps(feed.snapshot())
