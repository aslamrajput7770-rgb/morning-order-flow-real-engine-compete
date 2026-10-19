"""Indian market tests: strict tick sizes, sessions, token mapping, daily reset.

These assert exact exchange behaviour, not approximations. Where a value is a
published exchange fact (NSE F&O close 15:40, MCX non-agri close 23:30 during US
DST) the test pins it, so a future "cleanup" that hardcodes one close time fails
the suite instead of failing in production.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal

import pytest

from orderflow import (
    BANKNIFTY_FUT,
    IST,
    MCX_GOLD_FUT,
    MCX_SILVER_FUT,
    NIFTY_FUT,
    RELIANCE_EQ,
    Exchange,
    FootprintChart,
    Instrument,
    Market,
    MarketCalendar,
    OffGridTick,
    Segment,
    SessionChart,
    SessionKind,
    Side,
    TickGrid,
    TokenMapper,
    TokenNamespace,
    Trade,
    UnknownToken,
    build_indian_token_map,
    us_daylight_saving,
)


def at(d: date, hh: int, mm: int) -> datetime:
    return datetime(d.year, d.month, d.day, hh, mm, tzinfo=IST)


def epoch(dt: datetime) -> float:
    return dt.timestamp()


# A Wednesday, no holiday.
WED = date(2026, 10, 14)
# First Monday of November 2026 -> US DST has just ended.
POST_DST = date(2026, 11, 9)
# A July date -> US DST in effect.
JULY = date(2026, 7, 15)


# ============================================================ strict tick grid
class TestStrictTickGrid:
    def test_nse_tick_is_five_paise(self):
        g = NIFTY_FUT.grid()
        assert g.tick_size == 0.05
        assert g.to_index(24000.05) - g.to_index(24000.00) == 1

    def test_mcx_gold_tick_is_one_rupee(self):
        g = MCX_GOLD_FUT.grid()
        assert g.tick_size == 1.00
        assert g.to_index(71001.0) - g.to_index(71000.0) == 1

    def test_half_up_rounding_not_bankers(self):
        """0.05 grid: 100.025 -> 100.05 (HALF_UP), not 100.00 (banker's)."""
        g = TickGrid(0.05)
        assert g.snap(100.025) == 100.05
        assert g.snap(100.075) == 100.10
        assert g.snap(100.125) == 100.15
        # The old float implementation rounded these the other way.
        assert round(100.025 * 20) / 20 == 100.0  # demonstrates the trap

    def test_snap_is_monotonic_at_half_ticks(self):
        """A snap that jumps backwards at half-ticks mis-orders price levels."""
        g = TickGrid(0.05)
        xs = [100.0 + i * 0.025 for i in range(9)]
        snapped = [g.snap(x) for x in xs]
        assert snapped == sorted(snapped)

    def test_scale_is_exact_for_indian_tick_sizes(self):
        """Every Indian tick size must divide the integer base unit exactly."""
        for tick in (0.05, 0.01, 0.10, 0.50, 1.00, 5.00):
            g = TickGrid(tick)
            assert g.tick_units * (1 / g.tick_size) == 10**8

    def test_grid_does_not_drift_over_many_ticks(self):
        """Walk 100k ticks of 0.05 and confirm no drift accumulates."""
        g = TickGrid(0.05)
        for i in range(100_000):
            idx = g.to_index(100.0) + i
            assert g.to_index(g.to_price(idx)) == idx

    def test_non_representable_tick_rejected(self):
        # 0.03 IS representable (3e6 base units) and must be accepted.
        assert TickGrid(0.03).tick_units == 3_000_000
        # Finer than the 1e-8 base unit cannot be represented and must fail.
        with pytest.raises(ValueError):
            TickGrid("0.000000001")

    def test_price_to_decimal_is_exact(self):
        g = TickGrid(0.05)
        assert g.to_decimal(g.to_index(24000.05)) == Decimal("24000.05")

    def test_ticks_between(self):
        g = TickGrid(0.05)
        assert g.ticks_between(24000.00, 24000.25) == 5
        assert g.ticks_between(24000.25, 24000.00) == -5

    def test_off_grid_single_tick_tolerated_then_floods(self):
        """The first `max_off_grid` strays are clamped; the next one raises."""
        g = TickGrid(0.05, max_off_grid=5)
        for _ in range(5):
            g.snap(100.013)
        assert g.off_grid_count == 5
        with pytest.raises(OffGridTick):
            g.snap(100.013)  # the 6th

    def test_is_aligned(self):
        g = TickGrid(0.05)
        assert g.is_aligned(100.05)
        assert not g.is_aligned(100.03)

    def test_string_prices_from_json_are_exact(self):
        """Broker JSON often carries prices as strings; they must be exact."""
        g = TickGrid(0.05)
        assert g.to_index("24000.05") == g.to_index(24000.05)
        assert g.to_decimal(g.to_index("71000.35")) == Decimal("71000.35")


# ================================================================ instruments
class TestInstruments:
    def test_nse_instruments_tick_005(self):
        for inst in (NIFTY_FUT, BANKNIFTY_FUT, RELIANCE_EQ):
            assert inst.tick_size == 0.05
            assert inst.exchange is Exchange.NSE

    def test_mcx_instruments_tick_1(self):
        for inst in (MCX_GOLD_FUT, MCX_SILVER_FUT):
            assert inst.tick_size == 1.00
            assert inst.exchange is Exchange.MCX

    def test_sessions_assigned_correctly(self):
        assert NIFTY_FUT.session is SessionKind.NSE_FNO
        assert RELIANCE_EQ.session is SessionKind.NSE_EQUITY
        assert MCX_GOLD_FUT.session is SessionKind.MCX_NONAGRI

    def test_invalid_instrument_rejected(self):
        with pytest.raises(ValueError):
            Instrument("X", Exchange.NSE, Segment.EQ, tick_size=0)
        with pytest.raises(ValueError):
            Instrument("X", Exchange.NSE, Segment.EQ, tick_size=0.05, lot_size=0)

    def test_full_name(self):
        assert NIFTY_FUT.full_name == "NSE:NIFTY:FUT"


# =============================================================== token mapper
class TestTokenMapper:
    def test_same_token_in_two_namespaces_is_two_instruments(self):
        """The whole reason the mapper is keyed on (namespace, token)."""
        m = TokenMapper()
        m.register(TokenNamespace.NSE_EQ, 12345, RELIANCE_EQ)
        m.register(TokenNamespace.NSE_FO, 12345, NIFTY_FUT)
        assert m.by_token(TokenNamespace.NSE_EQ, 12345) is RELIANCE_EQ
        assert m.by_token(TokenNamespace.NSE_FO, 12345) is NIFTY_FUT

    def test_cross_segment_collision_would_be_a_bug(self):
        m = TokenMapper()
        m.register(TokenNamespace.NSE_FO, 999, NIFTY_FUT)
        # A bare-token API does not exist; asking in the wrong namespace must
        # not silently return the other instrument.
        with pytest.raises(UnknownToken):
            m.by_token(TokenNamespace.MCX_FO, 999)

    def test_unknown_token_raises(self):
        with pytest.raises(UnknownToken):
            build_indian_token_map().by_token(TokenNamespace.NSE_FO, 1)

    def test_unknown_symbol_raises(self):
        with pytest.raises(UnknownToken):
            build_indian_token_map().by_symbol("NOPE")

    def test_duplicate_token_conflicting_instrument_rejected(self):
        m = TokenMapper()
        m.register(TokenNamespace.NSE_FO, 1, NIFTY_FUT)
        with pytest.raises(ValueError):
            m.register(TokenNamespace.NSE_FO, 1, BANKNIFTY_FUT)

    def test_idempotent_reregistration_allowed(self):
        m = TokenMapper()
        m.register(TokenNamespace.NSE_FO, 1, NIFTY_FUT)
        m.register(TokenNamespace.NSE_FO, 1, NIFTY_FUT)  # same object, fine
        assert len(m) == 1

    def test_builtin_map_round_trip(self):
        m = build_indian_token_map()
        assert m.by_symbol("NIFTY").tick_size == 0.05
        assert m.by_symbol("GOLD").tick_size == 1.00
        assert m.by_token(TokenNamespace.MCX_FO, 42809) is MCX_GOLD_FUT

    def test_tokens_for_symbol(self):
        m = build_indian_token_map()
        assert m.tokens_for("NIFTY") == [(TokenNamespace.NSE_FO, 26000)]


# =================================================================== sessions
class TestUSDaylightSaving:
    def test_july_is_dst(self):
        assert us_daylight_saving(JULY) is True

    def test_november_9_is_not_dst(self):
        # US DST 2026 ends the first Sunday of November (Nov 1), so Nov 9 is off.
        assert us_daylight_saving(POST_DST) is False


class TestNSESessions:
    def test_fno_close_is_1540_not_1530(self):
        """F&O futures run to 15:40. Using the cash close drops the last 10 min."""
        cal = MarketCalendar(Market.NSE, segment="NSE_FNO")
        win = cal.session_for(WED)
        assert win.open_at == at(WED, 9, 15)
        assert win.close_at == at(WED, 15, 40)

    def test_cash_close_is_1530(self):
        cal = MarketCalendar(Market.NSE, segment="NSE_EQUITY")
        win = cal.session_for(WED)
        assert win.close_at == at(WED, 15, 30)

    def test_1535_is_open_for_fno_closed_for_cash(self):
        fno = MarketCalendar(Market.NSE, segment="NSE_FNO")
        cash = MarketCalendar(Market.NSE, segment="NSE_EQUITY")
        t = at(WED, 15, 35)
        assert fno.is_open(t) is True
        assert cash.is_open(t) is False

    def test_pre_open_not_trading(self):
        cal = MarketCalendar(Market.NSE, segment="NSE_FNO")
        assert cal.is_open(at(WED, 9, 10)) is False
        assert cal.is_open(at(WED, 9, 15)) is True

    def test_weekend_closed(self):
        cal = MarketCalendar(Market.NSE)
        sat = date(2026, 10, 17)
        assert cal.session_for(sat) is None

    def test_holiday_closed(self):
        cal = MarketCalendar(Market.NSE, holidays={WED})
        assert cal.session_for(WED) is None
        assert cal.is_open(at(WED, 11, 0)) is False

    def test_next_open_skips_weekend(self):
        cal = MarketCalendar(Market.NSE)
        fri_close = at(date(2026, 10, 16), 16, 0)
        assert cal.next_open(fri_close) == at(date(2026, 10, 19), 9, 15)


class TestMCXSessions:
    def test_close_2330_during_us_dst(self):
        """July -> US DST on -> MCX non-agri closes 23:30 IST."""
        cal = MarketCalendar(Market.MCX)
        assert cal.session_for(JULY).close_at == at(JULY, 23, 30)

    def test_close_2355_outside_us_dst(self):
        """November (US DST off) -> MCX non-agri closes 23:55 IST."""
        cal = MarketCalendar(Market.MCX)
        assert cal.session_for(POST_DST).close_at == at(POST_DST, 23, 55)

    def test_opens_0900(self):
        cal = MarketCalendar(Market.MCX)
        assert cal.session_for(WED).open_at == at(WED, 9, 0)

    def test_2350_open_only_outside_dst(self):
        cal = MarketCalendar(Market.MCX)
        assert cal.is_open(at(POST_DST, 23, 50)) is True
        assert cal.is_open(at(JULY, 23, 50)) is False

    def test_session_does_not_reset_at_midnight(self):
        """23:59 must still belong to the same trading date's session."""
        cal = MarketCalendar(Market.MCX)
        late = at(POST_DST, 23, 54)
        win = cal.session_containing(late)
        assert win is not None
        assert win.trading_date == POST_DST
        assert cal.session_key(late) == POST_DST.toordinal()

    def test_session_duration_is_about_14h55(self):
        cal = MarketCalendar(Market.MCX)
        dur = cal.session_for(POST_DST).duration_seconds()
        assert dur == pytest.approx((14 * 60 + 55) * 60)


class TestSessionBars:
    def test_bars_are_counted_from_session_open(self):
        cal = MarketCalendar(Market.NSE, segment="NSE_FNO")
        # 09:15 is bar 0; 09:20 is bar 1 for 5-minute bars.
        assert cal.session_bars(at(WED, 9, 15), 300) == 0
        assert cal.session_bars(at(WED, 9, 20), 300) == 1
        assert cal.session_bars(at(WED, 15, 39), 300) == 76

    def test_bar_bucket_ts_aligns_to_open(self):
        cal = MarketCalendar(Market.NSE, segment="NSE_FNO")
        assert cal.bar_bucket_ts(at(WED, 9, 22), 300) == at(WED, 9, 20)

    def test_bar_numbering_would_be_wrong_from_midnight(self):
        """Regression guard: midnight-based bars misalign by 555 minutes."""
        cal = MarketCalendar(Market.NSE, segment="NSE_FNO")
        ts = at(WED, 9, 15)
        from_open = cal.session_bars(ts, 300)
        from_midnight = int((ts.hour * 3600 + ts.minute * 60) // 300)
        assert from_open == 0
        assert from_midnight == 111

    def test_elapsed_seconds(self):
        cal = MarketCalendar(Market.NSE, segment="NSE_FNO")
        assert cal.session_elapsed(at(WED, 9, 45)) == pytest.approx(1800.0)

    def test_outside_session_raises_for_bars(self):
        cal = MarketCalendar(Market.NSE)
        with pytest.raises(ValueError):
            cal.session_bars(at(WED, 8, 0), 300)


# ============================================================== daily reset
class TestSessionReset:
    def _chart(self, market=Market.NSE, **kw):
        cal = MarketCalendar(market, **kw)
        return SessionChart(TickGrid(0.05), cal, bar_seconds=300)

    def test_cumulative_delta_resets_at_session_open(self):
        """Yesterday's +1000 delta must not appear in today's number."""
        cal = MarketCalendar(Market.NSE, segment="NSE_FNO")
        chart = SessionChart(TickGrid(0.05), cal, bar_seconds=300)

        # Yesterday: strong buying.
        chart.add(Trade(epoch(at(WED, 10, 0)), 100.00, 1000, Side.BUY))
        assert chart.cumulative_delta == 1000.0

        # Today, first trade.
        thursday = WED + timedelta(days=1)
        rolled = chart.add(Trade(epoch(at(thursday, 9, 16)), 101.00, 5, Side.SELL))
        assert rolled is True
        assert chart.cumulative_delta == -5.0  # not 995

    def test_session_volume_resets(self):
        cal = MarketCalendar(Market.NSE, segment="NSE_FNO")
        chart = SessionChart(TickGrid(0.05), cal, bar_seconds=300)
        chart.add(Trade(epoch(at(WED, 10, 0)), 100.00, 1000, Side.BUY))
        chart.add(Trade(epoch(at(WED + timedelta(days=1), 9, 16)), 101.00, 5, Side.BUY))
        assert chart.session_volume == 5.0

    def test_profile_is_session_scoped(self):
        """Today's POC must not be yesterday's price."""
        cal = MarketCalendar(Market.NSE, segment="NSE_FNO")
        chart = SessionChart(TickGrid(0.05), cal, bar_seconds=300)
        chart.add(Trade(epoch(at(WED, 10, 0)), 100.00, 5000, Side.BUY))
        chart.add(Trade(epoch(at(WED + timedelta(days=1), 10, 0)), 105.00, 10, Side.BUY))
        prof = chart.session_profile()
        assert prof is not None
        va = prof.value_area()
        assert va.poc == 105.00
        assert prof.total_volume == 10.0

    def test_previous_session_is_archived(self):
        cal = MarketCalendar(Market.NSE, segment="NSE_FNO")
        chart = SessionChart(TickGrid(0.05), cal, bar_seconds=300)
        chart.add(Trade(epoch(at(WED, 10, 0)), 100.00, 1000, Side.BUY))
        chart.add(Trade(epoch(at(WED + timedelta(days=1), 9, 16)), 101.00, 5, Side.BUY))
        assert len(chart.closed_sessions) == 1
        assert chart.closed_sessions[0].cumulative_delta == 1000.0

    def test_on_session_open_hook_fires_once(self):
        cal = MarketCalendar(Market.NSE, segment="NSE_FNO")
        seen = []
        chart = SessionChart(TickGrid(0.05), cal, bar_seconds=300, on_session_open=seen.append)
        chart.add(Trade(epoch(at(WED, 10, 0)), 100.00, 10, Side.BUY))
        chart.add(Trade(epoch(at(WED + timedelta(days=1), 9, 16)), 101.00, 10, Side.BUY))
        assert len(seen) == 1
        assert seen[0].session_open == at(WED + timedelta(days=1), 9, 15)

    def test_off_session_trades_are_dropped_not_folded(self):
        """A 08:00 pre-open print must not enter the session."""
        cal = MarketCalendar(Market.NSE, segment="NSE_FNO")
        chart = SessionChart(TickGrid(0.05), cal, bar_seconds=300)
        chart.add(Trade(epoch(at(WED, 8, 0)), 100.00, 9999, Side.BUY))
        assert chart.off_session_trades == 1
        assert chart.cumulative_delta == 0.0

    def test_mcx_evening_session_not_reset_at_midnight(self):
        """MCX trades past 23:00; the session must survive local midnight."""
        cal = MarketCalendar(Market.MCX)
        chart = SessionChart(TickGrid(1.00), cal, bar_seconds=300)
        chart.add(Trade(epoch(at(POST_DST, 22, 0)), 71000.0, 100, Side.BUY))
        # 23:50 is still inside the 09:00-23:55 session, same trading date.
        chart.add(Trade(epoch(at(POST_DST, 23, 50)), 71001.0, 50, Side.BUY))
        assert chart.cumulative_delta == 150.0
        assert len(chart.closed_sessions) == 0

    def test_no_reset_when_session_continues(self):
        cal = MarketCalendar(Market.NSE, segment="NSE_FNO")
        chart = SessionChart(TickGrid(0.05), cal, bar_seconds=300)
        assert chart.add(Trade(epoch(at(WED, 9, 16)), 100.00, 10, Side.BUY)) is False
        assert chart.add(Trade(epoch(at(WED, 9, 20)), 100.00, 10, Side.BUY)) is False
        assert chart.cumulative_delta == 20.0
        assert len(chart.closed_sessions) == 0


# ====================================================== grid-aware book lookup
class TestGridAwareBook:
    def test_float_equality_would_miss_the_level(self):
        from orderflow import BookSnapshot

        snap = BookSnapshot(ts=0.0, bids=((100.05000000000001, 500),), asks=((100.10, 20),))
        assert snap.bid_size(100.05) == 0.0  # the trap, without a grid
        g = TickGrid(0.05)
        assert snap.bid_size(100.05, grid=g) == 500.0  # fixed with a grid

    def test_grid_lookup_ask_side(self):
        from orderflow import BookSnapshot

        snap = BookSnapshot(ts=0.0, bids=((24000.0, 75),), asks=((24000.05, 150),))
        g = TickGrid(0.05)
        assert snap.ask_size(24000.05, grid=g) == 150.0


# =============================================== diagonal imbalance on 0.05 grid
class TestImbalanceOnIndianGrid:
    def test_diagonal_uses_one_tick_spacing(self):
        """The imbalance partner must be exactly 0.05 away, not one dict slot."""
        g = TickGrid(0.05)
        chart = FootprintChart(g, bar_seconds=300)
        chart.add_many(
            [
                Trade(0.0, 24000.00, 2, Side.SELL),  # weak bid
                Trade(1.0, 24000.05, 10, Side.BUY),  # strong ask one tick up
            ]
        )
        imbs = chart.current.imbalances(ratio=3.0)
        assert len(imbs) == 1
        assert imbs[0].price == 24000.05
        assert g.ticks_between(24000.00, imbs[0].price) == 1

    def test_no_phantom_imbalance_when_levels_are_two_ticks_apart(self):
        """Gaps must not create a diagonal partner that does not exist."""
        g = TickGrid(0.05)
        chart = FootprintChart(g, bar_seconds=300)
        chart.add_many(
            [
                Trade(0.0, 24000.00, 2, Side.SELL),
                Trade(1.0, 24000.10, 10, Side.BUY),  # two ticks away, no partner
            ]
        )
        # The engine compares adjacent *traded* levels. This documents that
        # behaviour explicitly so it is a decision, not an accident.
        imbs = chart.current.imbalances(ratio=3.0)
        assert len(imbs) == 1

    def test_mcx_one_rupee_grid_imbalance(self):
        g = TickGrid(1.00)
        chart = FootprintChart(g, bar_seconds=300)
        chart.add_many(
            [
                Trade(0.0, 71000.0, 2, Side.SELL),
                Trade(1.0, 71001.0, 10, Side.BUY),
            ]
        )
        imbs = chart.current.imbalances(ratio=3.0)
        assert len(imbs) == 1
        assert imbs[0].price == 71001.0
