"""Instrument master loader tests.

The master file is 36 MB and changes daily, so these tests run against a small
hand-built fixture that reproduces the shapes that matter. The real file is
checked separately by `fetch_instruments.py`, which prints loader stats.
"""

from __future__ import annotations

import json
from datetime import date

import pytest

from orderflow.instrument_master import (
    _parse_expiry,
    _tick_for,
    find_instrument,
    load_master,
    parse_master,
    upcoming_futures,
)
from orderflow.instruments import Segment, TokenNamespace


def rec(token, symbol, name="", expiry="", strike="0", lotsize="1",
        instrumenttype="", exch_seg="NSE", tick_size="0"):
    """One master record, in the real file's shape (all values are strings)."""
    return {
        "token": token, "symbol": symbol, "name": name, "expiry": expiry,
        "strike": strike, "lotsize": lotsize, "instrumenttype": instrumenttype,
        "exch_seg": exch_seg, "tick_size": tick_size, "freeze_qty": "0",
        "is_cas_enabled": False,
    }


FIXTURE = [
    rec("99926000", "Nifty 50", "NIFTY", instrumenttype="AMXIDX"),
    rec("2885", "RELIANCE-EQ", "RELIANCE", tick_size="10.000000"),
    rec("1594", "INFY-EQ", "INFY", tick_size="5.000000"),
    # Nifty family: prefix collision is the trap.
    rec("48704", "NIFTY27OCT26FUT", "NIFTY", "27OCT2026",
        "0", "65", "FUTIDX", "NFO", "10.000000"),
    rec("48705", "NIFTYNXT5027OCT26FUT", "NIFTYNXT50", "27OCT2026",
        "0", "25", "FUTIDX", "NFO", "10.000000"),
    rec("48706", "NIFTYFPI27OCT26FUT", "NIFTYFPI", "27OCT2026",
        "0", "25", "FUTIDX", "NFO", "10.000000"),
    rec("48699", "BANKNIFTY27OCT26FUT", "BANKNIFTY", "27OCT2026",
        "0", "30", "FUTIDX", "NFO", "20.000000"),
    # An expired contract, which must not be offered.
    rec("11111", "NIFTY25SEP25FUT", "NIFTY", "25SEP2025",
        "0", "75", "FUTIDX", "NFO", "10.000000"),
    # MCX bullion.
    rec("495213", "GOLD04DEC26FUT", "GOLD", "04DEC2026",
        "0", "1", "FUTCOM", "MCX", "100.000000"),
    rec("42809", "SILVER04DEC26FUT", "SILVER", "04DEC2026",
        "0", "1", "FUTCOM", "MCX", "100.000000"),
    # Currency derivatives: 1e7 scale segment.
    rec("10008", "USDINR26OCT26FUT", "USDINR", "26OCT2026",
        "0", "1", "FUTCUR", "CDS", "25000.000000"),
    # Junk that must be skipped, each for a different reason.
    rec("abc", "BROKEN", "", "", "0", "1", "", "NFO"),
    rec("99999", "NOSEG", "", "", "0", "1", "", "WHO"),
]


@pytest.fixture(scope="module")
def mapper():
    m, _ = parse_master(FIXTURE)
    return m


class TestLoader:
    def test_loads_every_known_segment(self, mapper):
        assert len(mapper) == 11

    def test_skip_reasons_are_counted_separately(self):
        """Lumping a bad token under 'unknown segment' hides a data problem."""
        _, stats = parse_master(FIXTURE)
        assert stats.skipped_bad_token == 1
        assert stats.skipped_unknown_segment == 1
        assert stats.loaded == 11
        assert stats.total_records == len(FIXTURE)

    def test_currency_segment_is_mapped(self, mapper):
        inst = mapper.by_token(TokenNamespace.CDS_FO, 10008)
        assert inst.segment is Segment.FUT
        assert inst.underlying == "USDINR"

    def test_mcx_tokens_load(self, mapper):
        inst = mapper.by_token(TokenNamespace.MCX_FO, 495213)
        assert inst.underlying == "GOLD"


class TestTickSizeIsNotTrusted:
    """The master's tick_size is not the traded tick, so it must be ignored."""

    def test_reliance_reports_10_but_really_ticks_at_005(self, mapper):
        inst = mapper.by_token(TokenNamespace.NSE_EQ, 2885)
        assert inst.tick_size == 0.05

    def test_nifty_future_reports_10_but_really_ticks_at_005(self, mapper):
        inst = mapper.by_token(TokenNamespace.NSE_FO, 48704)
        assert inst.tick_size == 0.05

    def test_mcx_gold_uses_a_one_rupee_tick(self, mapper):
        inst = mapper.by_token(TokenNamespace.MCX_FO, 495213)
        assert inst.tick_size == 1.0

    def test_master_values_would_collapse_a_footprint(self, mapper):
        """The failure this prevents: a 10-rupee grid instead of 0.05.

        Every price level in a Nifty footprint lands on the same 10-rupee bar,
        the whole ladder collapses, and the number came from the broker's own
        file so it looks authoritative.
        """
        inst = mapper.by_token(TokenNamespace.NSE_FO, 48704)
        grid = inst.grid()
        assert grid.tick_size == 0.05
        # Two prices one tick apart must land on different grid steps.
        assert grid.to_index(24000.05) != grid.to_index(24000.10)
        assert grid.ticks_between(24000.05, 24000.10) == 1

    def test_mcx_tick_lookup_by_commodity(self):
        assert _tick_for("MCX", "GOLD", "FUTCOM") == 1.0
        assert _tick_for("MCX", "CRUDEOIL", "FUTCOM") == 1.0
        assert _tick_for("MCX", "SILVERMIC", "FUTCOM") == 0.1
        assert _tick_for("MCX", "SOMETHINGNEW", "FUTCOM") == 1.0


class TestUnderlyingMatching:
    """Symbol-prefix matching is wrong; the `name` field is the underlying."""

    def test_nifty_does_not_return_nifty_next_50(self, mapper):
        got = upcoming_futures(mapper, "NIFTY", today=date(2026, 10, 1))
        assert len(got) == 1
        _, token, inst = got[0]
        assert token == 48704
        assert inst.symbol == "NIFTY27OCT26FUT"
        assert "NXT50" not in inst.symbol

    def test_nifty_next_50_is_still_reachable_by_its_own_name(self, mapper):
        got = upcoming_futures(mapper, "NIFTYNXT50", today=date(2026, 10, 1))
        assert [t for _, t, _ in got] == [48705]

    def test_nifty_fpi_is_reachable_and_distinct(self, mapper):
        got = upcoming_futures(mapper, "NIFTYFPI", today=date(2026, 10, 1))
        assert [t for _, t, _ in got] == [48706]

    def test_banknifty_resolves(self, mapper):
        got = upcoming_futures(mapper, "BANKNIFTY", today=date(2026, 10, 1))
        assert [t for _, t, _ in got] == [48699]

    def test_expired_contracts_are_excluded(self, mapper):
        got = upcoming_futures(mapper, "NIFTY", today=date(2026, 10, 1))
        assert 11111 not in [t for _, t, _ in got]

    def test_nearest_expiry_comes_first(self, mapper):
        got = upcoming_futures(mapper, "NIFTY", today=date(2026, 10, 1), limit=5)
        exps = [inst.expiry for _, _, inst in got]
        assert exps == sorted(exps)


class TestExpiryParsing:
    def test_master_format(self):
        assert _parse_expiry("27OCT2026") == "2026-10-27"

    def test_blank_is_none(self):
        assert _parse_expiry("") is None
        assert _parse_expiry("   ") is None

    def test_garbage_is_none_not_an_exception(self):
        assert _parse_expiry("not-a-date") is None


class TestRealMasterFile:
    """Runs only when a downloaded master is present."""

    def test_real_file_loads_if_present(self, tmp_path):
        import pathlib

        p = pathlib.Path("instruments.json")
        if not p.exists():
            pytest.skip("instruments.json not downloaded")
        mapper, stats = load_master(p)
        assert stats.loaded > 100_000
        assert len(mapper) == stats.loaded
        # A real Nifty future must be findable.
        got = upcoming_futures(mapper, "NIFTY", limit=1)
        assert got, "no unexpired NIFTY future in the master"
        _, _, inst = got[0]
        assert inst.tick_size == 0.05
