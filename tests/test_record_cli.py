"""Tests for the record and replay command-line guards.

These guards exist to turn a silent failure into a loud one. An accepted
subscription outside its session streams nothing, and an unregistered token is
dropped without complaint; both look exactly like a quiet market, so the tools
have to say otherwise. That behaviour is what is tested here.
"""

from __future__ import annotations

import datetime as dt

import pytest

import record_ticks
import replay_backtest
from orderflow.session import IST, Market
from orderflow.watchlist import parse_instruments

CRED_KEYS = ("ANGEL_API_KEY", "ANGEL_CLIENT_CODE", "ANGEL_PIN", "ANGEL_TOTP_SECRET")


@pytest.fixture
def no_credentials(monkeypatch):
    for key in CRED_KEYS:
        monkeypatch.delenv(key, raising=False)


class TestCredentialGuard:
    def test_missing_credentials_are_named(self, no_credentials):
        with pytest.raises(record_ticks.CredentialError) as exc:
            record_ticks.credentials_from_env()
        for key in CRED_KEYS:
            assert key in str(exc.value)

    def test_credentials_come_from_the_environment(self, monkeypatch):
        for i, key in enumerate(CRED_KEYS):
            monkeypatch.setenv(key, f"v{i}")
        creds = record_ticks.credentials_from_env()
        assert creds["api_key"] == "v0"
        assert creds["totp_secret"] == "v3"

    def test_no_credentials_exits_two(self, no_credentials, tmp_path):
        rc = record_ticks.main(["--out", str(tmp_path), "--minutes", "1"])
        assert rc == 2

    def test_dry_run_needs_no_credentials(self, no_credentials, tmp_path):
        rc = record_ticks.main(["--out", str(tmp_path), "--dry-run"])
        assert rc == 0


class TestSessionReporting:
    def test_reports_a_closed_window_before_the_open(self):
        specs = parse_instruments("NSE_FO:48704:0.05")
        before = dt.datetime(2026, 10, 21, 8, 0, tzinfo=IST)
        assert record_ticks.session_state(specs, before) == {
            "NSE_FO:48704": "closed"
        }

    def test_reports_the_window_during_the_session(self):
        specs = parse_instruments("NSE_FO:48704:0.05")
        mid = dt.datetime(2026, 10, 21, 11, 0, tzinfo=IST)
        state = record_ticks.session_state(specs, mid)
        assert state["NSE_FO:48704"] == "09:15-15:40"


class TestReplayMapper:
    def test_registers_every_token(self):
        specs = parse_instruments("NSE_FO:48704:0.05,MCX_FO:495213:1.0")
        mapper = replay_backtest.build_mapper(specs)
        nse = mapper.by_token(specs[0].namespace, 48704)
        mcx = mapper.by_token(specs[1].namespace, 495213)
        assert nse.tick_size == 0.05
        assert mcx.tick_size == 1.0

    def test_mapper_matches_the_record_path(self):
        """Both CLIs must name a token identically or replay finds no trades."""
        text = "NSE_FO:48704:0.05,MCX_FO:495213:1.0"
        specs = parse_instruments(text)
        record_mapper = record_ticks._mapper_for_tokens(specs)
        replay_mapper = replay_backtest.build_mapper(specs)
        for spec in specs:
            a = record_mapper.by_token(spec.namespace, spec.token)
            b = replay_mapper.by_token(spec.namespace, spec.token)
            assert a.symbol == b.symbol
            assert a.tick_size == b.tick_size

    def test_replay_with_no_shards_exits_two(self, tmp_path):
        rc = replay_backtest.main([
            "--rec", str(tmp_path), "--key", "conn0",
            "--instruments", "NSE_FO:48704:0.05",
        ])
        assert rc == 2
