"""Live-runner wiring: real tokens in, silent failures refused.

The runner is tested through its argument resolution, which is where the
dangerous behaviour lives. Opening a socket needs real credentials and is out
of scope; deciding *what* to subscribe to does not.
"""

from __future__ import annotations

import argparse
import json

import pytest

import live_feed


def rec(token, symbol, name="", expiry="", instrumenttype="FUTIDX", exch_seg="NFO"):
    return {
        "token": token, "symbol": symbol, "name": name, "expiry": expiry,
        "strike": "0", "lotsize": "65", "instrumenttype": instrumenttype,
        "exch_seg": exch_seg, "tick_size": "10.000000", "freeze_qty": "0",
        "is_cas_enabled": False,
    }


MASTER = [
    rec("48704", "NIFTY27OCT26FUT", "NIFTY", "27OCT2026"),
    rec("48705", "NIFTYNXT5027OCT26FUT", "NIFTYNXT50", "27OCT2026"),
    rec("48699", "BANKNIFTY27OCT26FUT", "BANKNIFTY", "27OCT2026"),
]


@pytest.fixture()
def master_file(tmp_path):
    p = tmp_path / "instruments.json"
    p.write_text(json.dumps(MASTER), encoding="utf-8")
    return p


def args(**kw):
    base = dict(
        instruments=None, underlying=None, master="instruments.json",
        allow_placeholder_tokens=False,
    )
    base.update(kw)
    return argparse.Namespace(**base)


class TestRealTokensRequired:
    def test_missing_master_file_is_refused_with_instructions(self, tmp_path):
        with pytest.raises(SystemExit) as e:
            live_feed.resolve_instruments(
                args(master=str(tmp_path / "nope.json")), mode=None)
        assert "fetch_instruments.py" in str(e.value)

    def test_unknown_token_is_a_hard_failure(self, master_file):
        """A live subscription to an unknown token returns no data and reports
        no error, so it must not be allowed through."""
        with pytest.raises(SystemExit) as e:
            live_feed.resolve_instruments(
                args(instruments="NSE_FO:26001", master=str(master_file)), mode=None)
        assert "not in the instrument master" in str(e.value)

    def test_placeholder_token_26001_is_not_in_a_real_master(self, master_file):
        """26001 is one of the five offline placeholders and does not exist."""
        with pytest.raises(SystemExit):
            live_feed.resolve_instruments(
                args(instruments="NSE_FO:26001", master=str(master_file)), mode=None)

    def test_real_token_is_accepted(self, master_file):
        _, pairs = live_feed.resolve_instruments(
            args(instruments="NSE_FO:48704", master=str(master_file)), mode=None)
        assert [t for _, t in pairs] == [48704]

    def test_placeholder_escape_hatch_still_works(self):
        mapper, pairs = live_feed.resolve_instruments(
            args(instruments="NSE_FO:26000", allow_placeholder_tokens=True), mode=None)
        assert pairs[0][1] == 26000
        assert mapper is None


class TestUnderlyingResolution:
    def test_nifty_resolves_to_the_nifty_future_not_next50(self, master_file):
        _, pairs = live_feed.resolve_instruments(
            args(underlying="NIFTY", master=str(master_file)), mode=None)
        assert [t for _, t in pairs] == [48704]

    def test_multiple_underlyings(self, master_file):
        _, pairs = live_feed.resolve_instruments(
            args(underlying="NIFTY,BANKNIFTY", master=str(master_file)), mode=None)
        assert [t for _, t in pairs] == [48704, 48699]

    def test_unknown_underlying_is_refused(self, master_file):
        with pytest.raises(SystemExit) as e:
            live_feed.resolve_instruments(
                args(underlying="NOSUCHINDEX", master=str(master_file)), mode=None)
        assert "no unexpired future" in str(e.value)

    def test_no_instruments_at_all_is_refused(self, master_file):
        with pytest.raises(SystemExit) as e:
            live_feed.resolve_instruments(args(master=str(master_file)), mode=None)
        assert "no instruments given" in str(e.value)


class TestCredentials:
    def test_missing_env_names_every_variable(self, monkeypatch):
        for k in ("ANGEL_API_KEY", "ANGEL_CLIENT_CODE", "ANGEL_PIN",
                  "ANGEL_TOTP_SECRET"):
            monkeypatch.delenv(k, raising=False)
        with pytest.raises(live_feed.CredentialError) as e:
            live_feed.credentials_from_env()
        msg = str(e.value)
        for k in ("ANGEL_API_KEY", "ANGEL_CLIENT_CODE", "ANGEL_PIN",
                  "ANGEL_TOTP_SECRET"):
            assert k in msg
        assert "chat" in msg.lower()

    def test_credentials_read_from_env_only(self, monkeypatch):
        monkeypatch.setenv("ANGEL_API_KEY", "key")
        monkeypatch.setenv("ANGEL_CLIENT_CODE", "A123")
        monkeypatch.setenv("ANGEL_PIN", "1234")
        monkeypatch.setenv("ANGEL_TOTP_SECRET", "JBSWY3DPEHPK3PXP")
        v = live_feed.credentials_from_env()
        assert v["client_code"] == "A123"
        assert v["feed_token"] == ""


class TestLtpIsRefused:
    def test_ltp_mode_is_rejected(self):
        """LTP carries no volume, so the footprint would be silently empty."""
        from orderflow.feed import SubscriptionMode, require_volume_capable_mode
        from orderflow.feed.tick_reader import SnapshotModeUnsupported

        with pytest.raises(SnapshotModeUnsupported):
            require_volume_capable_mode(SubscriptionMode.LTP)


class FakeStats:
    def __init__(self, frames=0, ticks=0, trades=0, protocol_errors=0,
                 unmapped=0, reconnects=0, heartbeats=0):
        self.frames = frames
        self.ticks = ticks
        self.trades = trades
        self.protocol_errors = protocol_errors
        self.unmapped = unmapped
        self.reconnects = reconnects
        self.heartbeats = heartbeats


class FakeChart:
    def __init__(self, bars):
        self.bars = list(range(bars))


class TestLivenessVerdict:
    """A silent feed must be reported as broken, not as a quiet market."""

    def test_no_frames_fails(self, capsys):
        rc = live_feed.liveness_verdict(FakeStats(frames=0), {"X": FakeChart(0)})
        assert rc == 1
        assert "NO FRAMES RECEIVED" in capsys.readouterr().out

    def test_frames_but_no_ticks_fails(self, capsys):
        rc = live_feed.liveness_verdict(
            FakeStats(frames=50, ticks=0, protocol_errors=50), {"X": FakeChart(0)})
        assert rc == 1
        out = capsys.readouterr().out
        assert "ZERO ticks" in out
        assert "protocol errors=50" in out

    def test_ticks_but_no_bars_fails(self, capsys):
        rc = live_feed.liveness_verdict(
            FakeStats(frames=50, ticks=50, trades=50), {"X": FakeChart(0)})
        assert rc == 1
        assert "no bars" in capsys.readouterr().out

    def test_healthy_run_passes(self, capsys):
        rc = live_feed.liveness_verdict(
            FakeStats(frames=500, ticks=480, trades=300), {"X": FakeChart(12)})
        assert rc == 0
        assert "Feed is live" in capsys.readouterr().out

    def test_flat_volume_is_a_warning_not_a_failure(self, capsys):
        """Zero trades over a live window is suspicious but not proof of a bug."""
        rc = live_feed.liveness_verdict(
            FakeStats(frames=50, ticks=50, trades=0), {"X": FakeChart(3)})
        assert rc == 0
        assert "no trades" in capsys.readouterr().out

    def test_unmapped_tokens_are_surfaced(self, capsys):
        rc = live_feed.liveness_verdict(
            FakeStats(frames=50, ticks=50, trades=10, unmapped=7),
            {"X": FakeChart(3)})
        out = capsys.readouterr().out
        assert "UNMAPPED token" in out
        assert "fetch_instruments.py --download" in out
        assert rc == 0
