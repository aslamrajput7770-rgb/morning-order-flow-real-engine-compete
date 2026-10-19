"""Tests for the order-flow engine. Real code paths only, no mocks.

Numbers are asserted exactly wherever the engine's contract is exact
(bid/ask split, delta, POC, value area, imbalance ratios). Loose tolerances are
used only where the definition itself is a convention (value-area width).
"""

from __future__ import annotations

import pytest

from orderflow import (
    BookSnapshot,
    FootprintChart,
    ImbalanceSide,
    LiquidityMap,
    PressureGauge,
    ProfileBuilder,
    ProfileMode,
    Side,
    SuperDOM,
    Tape,
    TickGrid,
    Trade,
    big_trade_summary,
    cumulative_delta_series,
    detect_absorption,
    detect_divergence,
    footprint_html,
    heatmap_color,
    market_analyzer,
    naked_pocs,
    order_flow_vwap,
    ansi_footprint,
)

TICK = 0.25


@pytest.fixture
def grid() -> TickGrid:
    return TickGrid(TICK)


def bar_of(grid, trades):
    chart = FootprintChart(grid, bar_seconds=60.0)
    chart.add_many(trades)
    return chart.bars[0]


# --------------------------------------------------------------------- core
class TestTickGrid:
    def test_round_trip_snaps_to_tick(self):
        g = TickGrid(0.25)
        assert g.to_price(g.to_index(100.10)) == 100.0
        assert g.to_price(g.to_index(100.13)) == 100.25

    def test_float_keys_do_not_collide(self):
        """0.1 + 0.2 must land on the same index as 0.3 at tick 0.1."""
        g = TickGrid(0.1)
        assert g.to_index(0.1 + 0.2) == g.to_index(0.3)

    def test_rejects_bad_tick(self):
        with pytest.raises(ValueError):
            TickGrid(0)

    def test_trade_validates(self):
        with pytest.raises(ValueError):
            Trade(0.0, 100.0, 0)
        with pytest.raises(ValueError):
            Trade(0.0, -1.0, 5)


# ---------------------------------------------------------------- footprint
class TestFootprint:
    def test_bid_ask_split_and_delta(self, grid):
        bar = bar_of(
            grid,
            [
                Trade(0.0, 100.0, 10, Side.BUY),
                Trade(1.0, 100.0, 4, Side.SELL),
                Trade(2.0, 99.75, 7, Side.SELL),
            ],
        )
        lvl = bar.levels[grid.to_index(100.0)]
        assert lvl.ask_vol == 10
        assert lvl.bid_vol == 4
        assert lvl.delta == 6
        assert lvl.total_vol == 14
        assert bar.ask_volume == 10
        assert bar.bid_volume == 11
        assert bar.delta == -1

    def test_unknown_side_splits_evenly(self, grid):
        bar = bar_of(grid, [Trade(0.0, 100.0, 10, Side.UNKNOWN)])
        lvl = bar.levels[grid.to_index(100.0)]
        assert lvl.bid_vol == 5
        assert lvl.ask_vol == 5
        assert lvl.delta == 0

    def test_delta_pct(self, grid):
        bar = bar_of(
            grid,
            [Trade(0.0, 100.0, 15, Side.BUY), Trade(1.0, 100.0, 5, Side.SELL)],
        )
        lvl = bar.levels[grid.to_index(100.0)]
        assert lvl.delta_pct == pytest.approx(50.0)
        assert bar.delta_pct == pytest.approx(50.0)

    def test_ohlc(self, grid):
        bar = bar_of(
            grid,
            [
                Trade(0.0, 100.0, 1, Side.BUY),
                Trade(1.0, 100.5, 1, Side.BUY),
                Trade(2.0, 99.75, 1, Side.SELL),
                Trade(3.0, 100.25, 1, Side.BUY),
            ],
        )
        assert (bar.open, bar.high, bar.low, bar.close) == (100.0, 100.5, 99.75, 100.25)

    def test_poc_is_highest_volume_price(self, grid):
        bar = bar_of(
            grid,
            [
                Trade(0.0, 100.0, 3, Side.BUY),
                Trade(1.0, 100.25, 20, Side.BUY),
                Trade(2.0, 100.5, 5, Side.SELL),
            ],
        )
        assert bar.poc == 100.25

    def test_poc_tie_breaks_toward_mid(self, grid):
        bar = bar_of(
            grid,
            [
                Trade(0.0, 100.0, 10, Side.BUY),
                Trade(1.0, 100.5, 10, Side.BUY),
            ],
        )
        # Equal volume at 100.00 and 100.50; both are 1 tick from the mid. The
        # final tie-break is the lower price, so the result is deterministic
        # regardless of arrival order.
        assert bar.poc == 100.0
        assert (bar.high + bar.low) / 2 == 100.25

    def test_poc_tie_break_is_arrival_order_independent(self, grid):
        a = bar_of(grid, [Trade(0.0, 100.0, 10, Side.BUY), Trade(1.0, 100.5, 10, Side.BUY)])
        b = bar_of(grid, [Trade(0.0, 100.5, 10, Side.BUY), Trade(1.0, 100.0, 10, Side.BUY)])
        assert a.poc == b.poc

    def test_max_delta_level(self, grid):
        bar = bar_of(
            grid,
            [
                Trade(0.0, 100.0, 10, Side.BUY),
                Trade(1.0, 100.25, 20, Side.SELL),
            ],
        )
        assert bar.max_delta_level.price == 100.25
        assert bar.max_delta_level.delta == -20

    def test_bar_bucketing(self, grid):
        chart = FootprintChart(grid, bar_seconds=60.0)
        chart.add_many(
            [
                Trade(0.0, 100.0, 1, Side.BUY),
                Trade(59.9, 100.0, 1, Side.BUY),
                Trade(60.0, 100.0, 1, Side.BUY),
                Trade(120.0, 100.0, 1, Side.BUY),
            ]
        )
        assert [b.start_ts for b in chart.bars] == [0.0, 60.0, 120.0]
        assert [b.trade_count for b in chart.bars] == [2, 1, 1]

    def test_cumulative_delta_series(self, grid):
        chart = FootprintChart(grid, bar_seconds=60.0)
        chart.add_many(
            [
                Trade(0.0, 100.0, 10, Side.BUY),
                Trade(60.0, 100.0, 4, Side.SELL),
                Trade(120.0, 100.0, 2, Side.SELL),
            ]
        )
        assert chart.cumulative_delta == [(60.0, 10.0), (120.0, 6.0), (180.0, 4.0)]

    def test_render_modes(self, grid):
        bar = bar_of(grid, [Trade(0.0, 100.0, 10, Side.BUY)])
        for mode in ("bidask", "volume", "delta", "deltapct"):
            assert "100.0000" in bar.render(mode)
        with pytest.raises(ValueError):
            bar.render("nope")


# --------------------------------------------------------------- imbalances
class TestImbalances:
    def test_diagonal_ask_imbalance(self, grid):
        """ask[100.25] >= 3 x bid[100.00] -> ASK imbalance at 100.25."""
        bar = bar_of(
            grid,
            [
                Trade(0.0, 100.00, 2, Side.SELL),  # weak bid below
                Trade(1.0, 100.25, 10, Side.BUY),  # strong ask
            ],
        )
        imbs = bar.imbalances(ratio=3.0)
        assert len(imbs) == 1
        assert imbs[0].side is ImbalanceSide.ASK
        assert imbs[0].price == 100.25
        assert imbs[0].ratio == pytest.approx(5.0)

    def test_diagonal_bid_imbalance(self, grid):
        """bid[100.00] >= 3 x ask[100.25] -> BID imbalance at 100.00."""
        bar = bar_of(
            grid,
            [
                Trade(0.0, 100.00, 10, Side.SELL),
                Trade(1.0, 100.25, 2, Side.BUY),
            ],
        )
        imbs = bar.imbalances(ratio=3.0)
        assert len(imbs) == 1
        assert imbs[0].side is ImbalanceSide.BID
        assert imbs[0].price == 100.00

    def test_no_imbalance_when_balanced(self, grid):
        bar = bar_of(
            grid,
            [
                Trade(0.0, 100.00, 5, Side.SELL),
                Trade(1.0, 100.25, 5, Side.BUY),
            ],
        )
        assert bar.imbalances(ratio=3.0) == []

    def test_stacked_imbalance_flagged(self, grid):
        """Four consecutive buy imbalances -> all flagged stacked."""
        trades = []
        for i in range(4):
            trades.append(Trade(float(i), 100.00 + i * TICK, 1, Side.SELL))  # weak
        for i in range(4):
            trades.append(Trade(10.0 + i, 100.25 + i * TICK, 10, Side.BUY))  # strong
        bar = bar_of(grid, trades)
        imbs = bar.imbalances(ratio=3.0, stack_min=3)
        ask = [i for i in imbs if i.side is ImbalanceSide.ASK]
        assert len(ask) == 4
        assert all(i.stacked for i in ask)

    def test_stack_min_not_reached(self, grid):
        trades = [
            Trade(0.0, 100.00, 1, Side.SELL),
            Trade(1.0, 100.25, 10, Side.BUY),
            Trade(2.0, 100.50, 1, Side.SELL),
        ]
        bar = bar_of(grid, trades)
        imbs = bar.imbalances(ratio=3.0, stack_min=3)
        assert not any(i.stacked for i in imbs)

    def test_ratio_must_exceed_one(self, grid):
        bar = bar_of(grid, [Trade(0.0, 100.0, 1, Side.BUY)])
        with pytest.raises(ValueError):
            bar.imbalances(ratio=1.0)

    def test_min_volume_filter(self, grid):
        bar = bar_of(
            grid,
            [
                Trade(0.0, 100.00, 1, Side.SELL),
                Trade(1.0, 100.25, 5, Side.BUY),
            ],
        )
        assert bar.imbalances(ratio=3.0, min_volume=100.0) == []

    def test_delta_flip(self, grid):
        bar = bar_of(
            grid,
            [
                Trade(0.0, 100.00, 10, Side.SELL),
                Trade(1.0, 100.25, 10, Side.BUY),
            ],
        )
        assert bar.delta_flip() == 100.25


# ------------------------------------------------------------------ profile
class TestProfile:
    def _profile(self, grid, trades, **kw):
        chart = FootprintChart(grid, bar_seconds=60.0)
        chart.add_many(trades)
        b = ProfileBuilder(grid, ProfileMode.SESSION, session_seconds=1e9, **kw)
        return b.build(chart.bars)[0]

    def test_poc_and_value_area_known_distribution(self, grid):
        """Volumes 10,40,30,20,5 across 5 prices.

        POC = 100.25 (40). Value area target = 70% of 105 = 73.5.
        Start at POC (40). Below 10, above 30 -> take above (70).
        Below 10, above 20 -> take above (90 >= 73.5) -> stop.
        VAH = 100.75, VAL = 100.25.
        """
        sizes = [10, 40, 30, 20, 5]
        trades = []
        for i, s in enumerate(sizes):
            trades.append(Trade(float(i), 100.0 + i * TICK, s, Side.BUY))
        prof = self._profile(grid, trades)
        va = prof.value_area(70.0)
        assert va.poc == 100.25
        assert va.vah == 100.75
        assert va.val == 100.25
        assert va.total_vol == 105
        assert va.poc_vol == 40
        assert va.width == pytest.approx(0.50)

    def test_value_area_takes_pairs_downward_when_below_is_bigger(self, grid):
        """POC high, most volume below -> value area must extend downward."""
        trades = [
            Trade(0.0, 100.00, 50, Side.BUY),  # POC
            Trade(1.0, 99.75, 40, Side.BUY),
            Trade(2.0, 99.50, 30, Side.BUY),
            Trade(3.0, 99.25, 1, Side.BUY),
        ]
        prof = self._profile(grid, trades)
        va = prof.value_area(70.0)
        assert va.poc == 100.00
        assert va.val < va.poc
        assert va.vah == 100.00

    def test_value_area_full_coverage(self, grid):
        trades = [Trade(0.0, 100.0, 5, Side.BUY), Trade(1.0, 99.75, 5, Side.BUY)]
        prof = self._profile(grid, trades)
        va = prof.value_area(100.0)
        assert va.vah == 100.0 and va.val == 99.75

    def test_value_area_pct_validation(self, grid):
        prof = self._profile(grid, [Trade(0.0, 100.0, 5, Side.BUY)])
        with pytest.raises(ValueError):
            prof.value_area(0)
        with pytest.raises(ValueError):
            prof.value_area(101)

    def test_empty_profile_raises(self, grid):
        from orderflow import Profile

        with pytest.raises(ValueError):
            Profile(0.0, 60.0, grid).value_area()

    def test_profile_sums_match_footprint_totals(self, grid):
        """Profile and footprint must never disagree on total volume."""
        trades = [
            Trade(0.0, 100.00, 7, Side.BUY),
            Trade(1.0, 100.25, 13, Side.SELL),
            Trade(70.0, 99.75, 4, Side.BUY),
        ]
        chart = FootprintChart(grid, bar_seconds=60.0)
        chart.add_many(trades)
        prof = ProfileBuilder(grid, ProfileMode.SESSION, session_seconds=1e9).build(chart.bars)[0]
        assert prof.total_volume == sum(b.total_volume for b in chart.bars)

    def test_fixed_mode_groups_bars(self, grid):
        chart = FootprintChart(grid, bar_seconds=60.0)
        chart.add_many([Trade(i * 60.0, 100.0, 1, Side.BUY) for i in range(5)])
        profs = ProfileBuilder(grid, ProfileMode.FIXED, fixed_bars=2).build(chart.bars)
        assert len(profs) == 3  # 2 + 2 + 1

    def test_rolling_mode_window(self, grid):
        chart = FootprintChart(grid, bar_seconds=60.0)
        chart.add_many([Trade(i * 60.0, 100.0, 1, Side.BUY) for i in range(5)])
        profs = ProfileBuilder(grid, ProfileMode.ROLLING, rolling_bars=3).build(chart.bars)
        assert len(profs) == 5
        assert profs[0].total_volume == 1
        assert profs[2].total_volume == 3
        assert profs[4].total_volume == 3

    def test_naked_poc_virgin_then_touched(self, grid):
        chart = FootprintChart(grid, bar_seconds=60.0)
        chart.add_many(
            [
                # session 1: POC at 100.50
                Trade(0.0, 100.00, 1, Side.BUY),
                Trade(1.0, 100.50, 50, Side.BUY),
                # session 2: trades back through 100.50
                Trade(86400.0, 100.50, 1, Side.BUY),
            ]
        )
        builder = ProfileBuilder(grid, ProfileMode.SESSION, session_seconds=86400.0)
        profs = builder.build(chart.bars)
        pocs = naked_pocs(profs, chart.bars)
        first = next(p for p in pocs if p.price == 100.50)
        assert first.is_virgin is False
        assert first.first_touch_ts == 86400.0


# -------------------------------------------------------------------- delta
class TestDelta:
    def _bars(self, grid, trades):
        chart = FootprintChart(grid, bar_seconds=60.0)
        chart.add_many(trades)
        return chart.bars

    def test_cumulative_delta_candles(self, grid):
        bars = self._bars(
            grid,
            [
                Trade(0.0, 100.0, 10, Side.BUY),  # +10
                Trade(60.0, 100.0, 4, Side.SELL),  # -4
                Trade(120.0, 100.0, 1, Side.BUY),  # +1
            ],
        )
        candles = cumulative_delta_series(bars)
        assert [c.close for c in candles] == [10.0, 6.0, 7.0]
        assert candles[0].open == 0.0
        assert candles[1].open == 10.0

    def test_divergence_bearish(self, grid):
        """Two swing highs; the second is higher in price, lower in cum delta.

        Bars are one trade each so the pivots are unambiguous. The swing highs
        sit at indices 4 and 6 (never on the first/last bar, which the pivot
        scan cannot classify).
        """
        trades = [
            Trade(0.0, 100.5, 30, Side.BUY),  # cum +30
            Trade(60.0, 100.0, 1, Side.SELL),
            Trade(120.0, 99.5, 1, Side.SELL),  # swing low
            Trade(180.0, 100.0, 20, Side.BUY),
            Trade(240.0, 101.0, 1, Side.SELL),  # swing high 1 (cum +47)
            Trade(300.0, 100.5, 1, Side.SELL),
            Trade(360.0, 102.0, 2, Side.SELL),  # swing high 2 (cum +44)
            Trade(420.0, 101.0, 1, Side.SELL),
            Trade(480.0, 100.0, 1, Side.SELL),
        ]
        bars = self._bars(grid, trades)
        divs = detect_divergence(bars, left=1, right=1)
        bearish = [d for d in divs if d.kind == "BEARISH"]
        assert bearish, f"expected a bearish divergence, got {divs}"
        d = bearish[0]
        assert d.to_price > d.from_price
        assert d.to_delta < d.from_delta

    def test_divergence_needs_enough_bars(self, grid):
        bars = self._bars(grid, [Trade(0.0, 100.0, 1, Side.BUY)])
        assert detect_divergence(bars, left=3, right=3) == []


# --------------------------------------------------------------------- tape
class TestTape:
    def test_size_filter_and_counters(self):
        tape = Tape(min_size=5.0)
        tape.add(Trade(0.0, 100.0, 1, Side.BUY))
        tape.add(Trade(1.0, 100.0, 10, Side.SELL))
        tape.add(Trade(2.0, 100.0, 3, Side.BUY))
        assert tape.tick_count == 3
        assert tape.total_volume == 14
        assert len(tape.prints) == 1  # only the 10-lot
        assert tape.buy_volume == 4
        assert tape.sell_volume == 10
        assert tape.delta == -6

    def test_buy_pressure(self):
        tape = Tape()
        tape.add_many([Trade(0.0, 100.0, 3, Side.BUY), Trade(1.0, 100.0, 1, Side.SELL)])
        assert tape.buy_pressure == pytest.approx(75.0)

    def test_big_trades(self):
        tape = Tape(min_size=0)
        tape.add_many(
            [
                Trade(0.0, 100.0, 5, Side.BUY),
                Trade(1.0, 100.0, 50, Side.SELL),
                Trade(2.0, 100.0, 25, Side.BUY),
            ]
        )
        big = tape.big_trades(25.0)
        assert len(big) == 2
        s = big_trade_summary(tape.prints, min_size=25.0)
        assert s["count"] == 2
        assert s["net"] == pytest.approx(-25.0)

    def test_counter_rollover(self):
        from orderflow import Counter

        c = Counter(bar_seconds=60.0)
        assert c.add(Trade(0.0, 100.0, 5, Side.BUY)) is True
        assert c.add(Trade(1.0, 100.0, 5, Side.BUY)) is False
        assert c.volume == 10
        assert c.add(Trade(60.0, 100.0, 2, Side.BUY)) is True
        assert c.volume == 2
        assert c.elapsed(90.0) == pytest.approx(30.0)
        assert c.remaining(90.0) == pytest.approx(30.0)


# ---------------------------------------------------------------------- dom
class TestDOM:
    def test_book_snapshot(self):
        s = BookSnapshot(
            ts=0.0,
            bids=((100.0, 50), (99.75, 20)),
            asks=((100.25, 30), (100.5, 10)),
        )
        assert s.best_bid == 100.0
        assert s.best_ask == 100.25
        assert s.spread == pytest.approx(0.25)
        assert s.mid == pytest.approx(100.125)
        assert s.bid_size(99.75) == 20
        assert s.ask_size(100.5) == 10

    def test_crossed_book_rejected(self):
        with pytest.raises(ValueError):
            BookSnapshot(ts=0.0, bids=((100.25, 1),), asks=((100.0, 1),))

    def test_liquidity_map_walls(self, grid):
        lm = LiquidityMap(grid)
        lm.update(BookSnapshot(0.0, ((100.0, 500), (99.75, 10)), ((100.25, 20),)))
        lm.update(BookSnapshot(1.0, ((100.0, 50),), ((100.25, 900),)))
        assert lm.largest_bid_wall() == (100.0, 50.0)
        assert lm.largest_ask_wall() == (100.25, 900.0)

    def test_absorption_detects_pinned_size(self, grid):
        trades = [Trade(float(i), 100.0, 100, Side.SELL) for i in range(10)]
        out = detect_absorption(trades, grid, min_volume=500.0, max_seconds=1e9)
        assert len(out) == 1
        assert out[0].volume == 1000
        assert out[0].absorbed_side == "SELL"
        assert out[0].range_ticks == 0.0

    def test_absorption_splits_on_price_move(self, grid):
        trades = [Trade(0.0, 100.0, 100, Side.BUY)] * 5 + [
            Trade(5.0, 105.0, 100, Side.BUY)
        ] * 5
        out = detect_absorption(trades, grid, min_volume=400.0, max_seconds=1e9)
        assert len(out) == 2
        assert {a.price for a in out} == {100.0, 105.0}

    def test_absorption_splits_on_time(self, grid):
        trades = [Trade(0.0, 100.0, 300, Side.BUY), Trade(100.0, 100.0, 300, Side.BUY)]
        out = detect_absorption(trades, grid, min_volume=200.0, max_seconds=10.0)
        assert len(out) == 2

    def test_absorption_below_threshold_ignored(self, grid):
        trades = [Trade(0.0, 100.0, 10, Side.BUY)]
        assert detect_absorption(trades, grid, min_volume=500.0) == []

    def test_absorption_validates_min_volume(self, grid):
        with pytest.raises(ValueError):
            detect_absorption([Trade(0.0, 100.0, 1, Side.BUY)], grid, min_volume=0)

    def test_superdom_place_fill_position(self, grid):
        dom = SuperDOM(grid)
        dom.place(100.0, 5)
        assert dom.working[grid.to_index(100.0)] == 5
        dom.fill(100.0, 5)
        assert dom.position == 5
        assert grid.to_index(100.0) not in dom.working

    def test_superdom_rejects_zero_qty(self, grid):
        with pytest.raises(ValueError):
            SuperDOM(grid).place(100.0, 0)


# --------------------------------------------------------------- indicators
class TestIndicators:
    def _bars(self, grid, trades):
        chart = FootprintChart(grid, bar_seconds=60.0)
        chart.add_many(trades)
        return chart.bars

    def test_vwap_single_price(self, grid):
        bars = self._bars(grid, [Trade(0.0, 100.0, 10, Side.BUY)])
        v = order_flow_vwap(bars)[0]
        assert v.vwap == pytest.approx(100.0)
        assert v.sd == pytest.approx(0.0)
        assert v.upper == pytest.approx(100.0)
        assert v.lower == pytest.approx(100.0)

    def test_vwap_volume_weighted(self, grid):
        """90 lots at 100.00 and 10 lots at 101.00 -> VWAP = 100.10."""
        bars = self._bars(
            grid,
            [Trade(0.0, 100.0, 90, Side.BUY), Trade(1.0, 101.0, 10, Side.BUY)],
        )
        v = order_flow_vwap(bars)[0]
        assert v.vwap == pytest.approx(100.10)

    def test_vwap_bands_symmetric(self, grid):
        bars = self._bars(
            grid,
            [Trade(0.0, 100.0, 50, Side.BUY), Trade(1.0, 101.0, 50, Side.BUY)],
        )
        v = order_flow_vwap(bars, bands=(1.0, 2.0))[0]
        assert len(v.bands) == 2
        for b in v.bands:
            assert (b.upper - v.vwap) == pytest.approx(v.vwap - b.lower)
        assert v.bands[1].upper > v.bands[0].upper

    def test_vwap_anchor_skips_earlier_bars(self, grid):
        bars = self._bars(
            grid,
            [
                Trade(0.0, 100.0, 100, Side.BUY),
                Trade(60.0, 200.0, 100, Side.BUY),
            ],
        )
        anchored = order_flow_vwap(bars, anchor_ts=60.0)
        assert len(anchored) == 1
        assert anchored[0].vwap == pytest.approx(200.0)

    def test_vwap_rejects_bad_bands(self, grid):
        bars = self._bars(grid, [Trade(0.0, 100.0, 1, Side.BUY)])
        with pytest.raises(ValueError):
            order_flow_vwap(bars, bands=())
        with pytest.raises(ValueError):
            order_flow_vwap(bars, bands=(0.0,))

    def test_pressure_gauge_window(self):
        g = PressureGauge(window=2)
        g.add(Trade(0.0, 100.0, 10, Side.BUY))
        g.add(Trade(1.0, 100.0, 10, Side.BUY))
        g.add(Trade(2.0, 100.0, 10, Side.SELL))  # evicts the first buy
        assert g.window_buy == 10
        assert g.window_sell == 10
        assert g.oscillator == pytest.approx(0.0)
        # Lifetime totals keep accumulating, independent of the window.
        assert g.buy_volume == 20
        assert g.sell_volume == 10

    def test_pressure_oscillator_extremes(self):
        g = PressureGauge(window=10)
        g.add_many([Trade(float(i), 100.0, 1, Side.BUY) for i in range(5)])
        assert g.oscillator == pytest.approx(100.0)
        assert g.ratio == float("inf")

    def test_market_analyzer_ranking_and_bias(self, grid):
        chart = FootprintChart(grid, bar_seconds=60.0)
        chart.add_many([Trade(0.0, 100.0, 10, Side.BUY)])
        up = chart.bars
        chart2 = FootprintChart(grid, bar_seconds=60.0)
        chart2.add_many([Trade(0.0, 100.0, 10, Side.SELL)])
        down = chart2.bars
        rows = market_analyzer({"UP": up, "DOWN": down})
        assert rows[0].cumulative_delta != 0
        assert {r.symbol for r in rows} == {"UP", "DOWN"}


# --------------------------------------------------------- integration
class TestRender:
    def test_heatmap_volume_scale_endpoints(self):
        assert heatmap_color(0.0, 0.0, 10.0, "volume") == "#182030"
        assert heatmap_color(10.0, 0.0, 10.0, "volume") == "#3498db"

    def test_heatmap_delta_is_diverging(self):
        """Negative delta must be reddish, positive greenish, zero neutral."""
        lo = heatmap_color(-5.0, -5.0, 5.0, "delta")
        mid = heatmap_color(0.0, -5.0, 5.0, "delta")
        hi = heatmap_color(5.0, -5.0, 5.0, "delta")
        assert mid == "#282830"
        r_lo, g_lo = int(lo[1:3], 16), int(lo[3:5], 16)
        r_hi, g_hi = int(hi[1:3], 16), int(hi[3:5], 16)
        assert r_lo > g_lo  # red-dominant
        assert g_hi > r_hi  # green-dominant

    def test_heatmap_flat_range_does_not_crash(self):
        assert heatmap_color(5.0, 5.0, 5.0, "volume").startswith("#")

    def test_heatmap_clamps_out_of_range(self):
        assert heatmap_color(-100.0, 0.0, 10.0, "volume") == heatmap_color(0.0, 0.0, 10.0, "volume")
        assert heatmap_color(100.0, 0.0, 10.0, "volume") == heatmap_color(10.0, 0.0, 10.0, "volume")

    def test_heatmap_unknown_mode(self):
        with pytest.raises(ValueError):
            heatmap_color(1.0, 0.0, 2.0, "nope")

    def test_html_contains_bar_data(self, grid):
        chart = FootprintChart(grid, bar_seconds=60.0)
        chart.add_many(
            [Trade(0.0, 100.0, 10, Side.BUY), Trade(1.0, 99.75, 5, Side.SELL)]
        )
        html = footprint_html(chart.bars, mode="delta")
        assert html.startswith("<!doctype html>")
        assert "100.0000" in html
        assert "99.7500" in html
        assert html.count("<table>") == 1

    def test_html_rejects_bad_mode(self, grid):
        with pytest.raises(ValueError):
            footprint_html([], mode="rainbow")

    def test_ansi_footprint_has_colour_codes(self, grid):
        bar = bar_of(grid, [Trade(0.0, 100.0, 10, Side.BUY)])
        out = ansi_footprint(bar, mode="volume")
        assert "\033[48;2;" in out
        assert "100.0000" in out


class TestIntegration:
    def test_end_to_end_consistency(self, grid):
        """Every derived number must reconcile with the raw trade list."""
        trades = [
            Trade(0.0, 100.00, 10, Side.BUY),
            Trade(10.0, 100.25, 20, Side.SELL),
            Trade(20.0, 100.00, 5, Side.BUY),
            Trade(70.0, 99.75, 15, Side.SELL),
            Trade(80.0, 100.00, 25, Side.BUY),
        ]
        chart = FootprintChart(grid, bar_seconds=60.0)
        chart.add_many(trades)

        raw_vol = sum(t.size for t in trades)
        raw_delta = sum(
            t.size if t.side is Side.BUY else -t.size if t.side is Side.SELL else 0
            for t in trades
        )
        assert sum(b.total_volume for b in chart.bars) == pytest.approx(raw_vol)
        assert sum(b.delta for b in chart.bars) == pytest.approx(raw_delta)

        prof = ProfileBuilder(grid, ProfileMode.SESSION, session_seconds=1e9).build(chart.bars)[0]
        assert prof.total_volume == pytest.approx(raw_vol)

        tape = Tape(min_size=0)
        tape.add_many(trades)
        assert tape.total_volume == pytest.approx(raw_vol)
        assert tape.delta == pytest.approx(raw_delta)

    def test_round_trip_price_grid_stability(self, grid):
        """Snapping a price twice must be idempotent, or levels fragment."""
        for p in (99.75, 100.0, 100.123, 100.249, 100.251):
            once = grid.to_price(grid.to_index(p))
            twice = grid.to_price(grid.to_index(once))
            assert once == twice
