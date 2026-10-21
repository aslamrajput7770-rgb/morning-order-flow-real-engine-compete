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
from orderflow.session import IST, Market, MarketCalendar
from orderflow.ticks import TickGrid
from orderflow.session_chart import SessionChart
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


class TestRehearsal:
    """The pre-market dress rehearsal: real path, no socket, no credentials.

    The market-open run should be a formality. Everything below the transport
    is production code, so a rehearsal that records and replays cleanly leaves
    the broker's socket as the only untested piece.
    """

    def test_rehearsal_records_without_credentials(self, no_credentials, tmp_path):
        out = tmp_path / "rec"
        rc = record_ticks.main([
            "--out", str(out), "--rehearse", "3",
            "--instruments", "NSE_FO:48704:0.05",
        ])
        assert rc == 0
        from orderflow.recorder import iter_shards

        shards = list(iter_shards(out))
        assert shards, "the rehearsal recorded nothing"

    def test_rehearsal_frames_land_inside_the_session(self, no_credentials, tmp_path):
        """Off-session frames are discarded by design, so a rehearsal that
        stamped 'now' at 07:00 IST would produce no bars and prove nothing."""
        out = tmp_path / "rec"
        record_ticks.main([
            "--out", str(out), "--rehearse", "3",
            "--instruments", "NSE_FO:48704:0.05",
        ])
        rc = replay_backtest.main([
            "--rec", str(out), "--key", "conn0",
            "--instruments", "NSE_FO:48704:0.05", "--quiet",
        ])
        assert rc == 0

        from orderflow.feed.protocol import decode_packet
        from orderflow.recorder import iter_shards, read_shard

        shard = list(iter_shards(out))[0]
        frames = [r.payload for r in read_shard(shard)]
        # Decode the first frame's timestamp; it must sit inside 09:15-15:40.
        pkt = decode_packet(frames[0])
        # exchange_timestamp is epoch milliseconds, per the wire format.
        ist_ts = dt.datetime.fromtimestamp(
            pkt.exchange_timestamp / 1000, tz=dt.timezone.utc).astimezone(IST)
        assert ist_ts.hour == 9 and ist_ts.minute == 15, ist_ts

    def test_rehearsal_bars_have_delta_and_poc(self, no_credentials, tmp_path):
        """A rehearsal that produces a bar with no delta has proved the plumbing
        and nothing about the analytics."""
        out = tmp_path / "rec"
        record_ticks.main([
            "--out", str(out), "--rehearse", "4",
            "--instruments", "NSE_FO:48704:0.05",
        ])
        from orderflow.feed.connector import SmartStreamConnector, build_plan
        from orderflow.feed.protocol import SubscriptionMode
        from orderflow.feed.transports import ShardTransport

        specs = parse_instruments("NSE_FO:48704:0.05")
        mapper = replay_backtest.build_mapper(specs)
        plan = build_plan([s.subscription() for s in specs],
                          mode=SubscriptionMode.SNAP_QUOTE)
        chart = SessionChart(TickGrid(0.05),
                             MarketCalendar(Market.NSE, segment="NSE_FNO"),
                             bar_seconds=300)
        conn = SmartStreamConnector(
            plan, mapper=mapper,
            transport_factory=lambda i: ShardTransport(out, f"conn{i}"))
        conn.run(on_trade=lambda rt: chart.add(rt.trade),
                 max_seconds=5, reconnect=False)
        assert chart.bars, "no bar was built from the rehearsal"
        bar = chart.bars[0]
        assert bar.total_volume > 0
        assert bar.delta != 0, "delta is zero; the buy/sell split was lost"
        assert bar.poc > 0

    def test_rehearsal_sends_the_subscription(self, no_credentials, tmp_path):
        """A live run that never subscribes streams nothing. The rehearsal
        must exercise the same send."""
        from orderflow.feed.transports import RehearsalTransport

        sent = []
        t = RehearsalTransport([b"x"], on_subscribe=sent.append, idle_polls=1)
        t.connect()
        t.send("subscribe-message")
        assert sent == ["subscribe-message"]
        assert t.recv() == b"x"
        # Live-shaped: the first empty poll is an idle timeout, as on a quiet
        # market, and only then does the stream end.
        assert t.is_live is True
        assert t.recv() is None
        assert t.recv() is None
        assert t.is_live is False


class TestWaitForOpen:
    """A recorder started at 07:00 must wait for the open, not subscribe into a
    closed market. And the login must happen after the wait, because a JWT
    minted at 07:00 has aged more than an hour by 09:00."""

    def _specs(self):
        return parse_instruments("NSE_FO:48704:0.05")

    def test_returns_immediately_when_already_open(self):
        specs = self._specs()
        at = dt.datetime(2026, 10, 21, 10, 0, tzinfo=IST)
        assert record_ticks._wait_for_open(
            specs, at, {"flag": False}, sleep=lambda s: None,
            clock=lambda: at) is True

    def test_waits_until_the_open_then_returns(self):
        specs = self._specs()
        times = [dt.datetime(2026, 10, 21, 7, 0, tzinfo=IST)]

        def clock():
            return times[0]

        def sleep(seconds):
            # Jump the clock to the open rather than sleeping for two hours.
            times[0] = dt.datetime(2026, 10, 21, 9, 15, tzinfo=IST)

        assert record_ticks._wait_for_open(
            specs, times[0], {"flag": False}, sleep=sleep, clock=clock) is True

    def test_cancellation_before_the_open_reports_false(self):
        specs = self._specs()
        at = dt.datetime(2026, 10, 21, 7, 0, tzinfo=IST)
        stopped = {"flag": False}

        def sleep(seconds):
            stopped["flag"] = True  # Ctrl-C arrives during the wait

        assert record_ticks._wait_for_open(
            specs, at, stopped, sleep=sleep, clock=lambda: at) is False

    def test_mcx_opens_before_nse(self, capsys):
        """The desk list spans both markets; the wait must end at the earlier
        open, not the NSE one, or MCX loses the first fifteen minutes."""
        specs = parse_instruments("NSE_FO:48704:0.05,MCX_FO:495213:1.0")
        at = dt.datetime(2026, 10, 21, 7, 0, tzinfo=IST)
        stopped = {"flag": False}

        def sleep(seconds):
            stopped["flag"] = True  # cancel after the target is printed

        record_ticks._wait_for_open(specs, at, stopped,
                                    sleep=sleep, clock=lambda: at)
        out = capsys.readouterr().out
        assert "09:00:00 IST" in out, out
        assert "09:15:00 IST" not in out, out

    def test_nse_only_waits_for_its_own_open(self, capsys):
        specs = parse_instruments("NSE_FO:48704:0.05")
        at = dt.datetime(2026, 10, 21, 7, 0, tzinfo=IST)
        stopped = {"flag": False}

        def sleep(seconds):
            stopped["flag"] = True

        record_ticks._wait_for_open(specs, at, stopped,
                                    sleep=sleep, clock=lambda: at)
        assert "09:15:00 IST" in capsys.readouterr().out


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
