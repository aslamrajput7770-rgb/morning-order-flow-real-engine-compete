"""Dashboard tests: serialization, watchlist resolution, and replay.

These exercise the real engine through the HTTP layer. The replay test drives
`FileTransport` over a capture built with the encoder, so it goes through the
decoder, the router, the tick reader and the session chart exactly as a live
session does. No synthetic trades are injected below the transport.

The tick-size table is checked separately because the MCX contracts share
prefixes: "SILVER" is a prefix of "SILVERMIC" and "GOLD" of "GOLDPETAL", and
the two members of each pair have different ticks. Matching the shorter prefix
first is a silent misconfiguration.
"""

from __future__ import annotations

import datetime as dt
import json
import time
from pathlib import Path

import pytest

from orderflow.feed.protocol import ExchangeType, SubscriptionMode
from orderflow.feed.simulator import TickSpec, encode_tick
from dashboard import default_watchlist
from orderflow.session import Market


# --------------------------------------------------------------- tick table
class TestMcxTickSizes:
    def _inst(self, underlying: str):
        from orderflow.instruments import Exchange, Instrument, Segment, SessionKind  # noqa: F401

        return Instrument(
            symbol=underlying + "FUT", exchange=Exchange.MCX,
            segment=Segment.FUT, tick_size=0.05, underlying=underlying,
            session=SessionKind.MCX_NONAGRI,
        )

    def test_natural_gas_tick(self):
        from dashboard import _tick_for_watch

        assert _tick_for_watch(self._inst("NATURALGAS"), Market.MCX) == 0.10

    def test_crude_oil_tick(self):
        from dashboard import _tick_for_watch

        assert _tick_for_watch(self._inst("CRUDEOIL"), Market.MCX) == 1.0

    def test_gold_and_silver_tick(self):
        from dashboard import _tick_for_watch

        assert _tick_for_watch(self._inst("GOLD"), Market.MCX) == 1.0
        assert _tick_for_watch(self._inst("SILVER"), Market.MCX) == 1.0

    def test_silver_micro_is_not_silver(self):
        """The prefix trap: SILVERMIC must not inherit SILVER's tick."""
        from dashboard import _tick_for_watch

        assert _tick_for_watch(self._inst("SILVERMIC"), Market.MCX) == 0.1
        assert _tick_for_watch(self._inst("SILVERM"), Market.MCX) == 0.1

    def test_gold_petal_is_not_gold(self):
        from dashboard import _tick_for_watch

        assert _tick_for_watch(self._inst("GOLDPETAL"), Market.MCX) == 1.0

    def test_nse_default_tick(self):
        from dashboard import _tick_for_watch

        assert _tick_for_watch(self._inst("NIFTY"), Market.NSE) == 0.05


# ------------------------------------------------------------- watchlist
class TestWatchlist:
    @staticmethod
    @pytest.fixture(scope="class")
    def mapper():
        path = Path("instruments.json")
        if not path.exists():
            pytest.skip("instruments.json not downloaded")
        from orderflow.instrument_master import build_live_token_map

        return build_live_token_map(str(path))[0]



    def test_default_watchlist_has_the_desk_instruments(self, mapper):
        from dashboard import default_watchlist

        symbols = [w.symbol.upper() for w in default_watchlist(mapper)]
        for want in ("NIFTY", "NATURALGAS", "CRUDEOIL", "GOLD", "SILVER"):
            assert any(sym.startswith(want) for sym in symbols), (
                f"{want} missing from default watchlist: {symbols}")

    def test_watchlist_keys_are_path_safe(self, mapper):
        from dashboard import default_watchlist

        for w in default_watchlist(mapper):
            assert ":" not in w.key()
            assert "/" not in w.key()


# ------------------------------------------------------------- serializing
class TestSerialization:
    def _bar(self):
        from orderflow.core import Side, Trade
        from orderflow.footprint import FootprintBar
        from orderflow.ticks import TickGrid

        grid = TickGrid(0.05)
        bar = FootprintBar(start_ts=0.0, end_ts=300.0, grid=grid)
        bar.add(Trade(ts=1.0, price=100.00, size=10, side=Side.SELL))
        bar.add(Trade(ts=2.0, price=100.05, size=25, side=Side.BUY))
        bar.add(Trade(ts=3.0, price=100.05, size=5, side=Side.BUY))
        return bar

    def test_bar_snapshot_has_the_fields_the_page_reads(self):
        from dashboard import bar_snapshot

        snap = bar_snapshot(self._bar())
        for key in ("open", "high", "low", "close", "volume", "buy", "sell",
                    "delta", "poc", "levels", "imbalances", "trades"):
            assert key in snap, key

    def test_delta_is_ask_minus_bid(self):
        from dashboard import bar_snapshot

        snap = bar_snapshot(self._bar())
        assert snap["buy"] == 30
        assert snap["sell"] == 10
        assert snap["delta"] == 20

    def test_snapshot_is_json_serializable(self):
        from dashboard import bar_snapshot

        json.dumps(bar_snapshot(self._bar()))

    def test_level_rows_are_json_serializable(self):
        from dashboard import bar_snapshot

        json.dumps(bar_snapshot(self._bar())["levels"])


# ----------------------------------------------------------------- replay
def _in_session_capture(path: Path, n: int = 40) -> None:
    """A capture inside the NSE_FNO session, built with the encoder."""
    ist = dt.timezone(dt.timedelta(hours=5, minutes=30))
    base = int(dt.datetime(2026, 10, 21, 10, 30, tzinfo=ist).timestamp() * 1000)
    lines, vol, px = [], 1000, 22895.0
    for i in range(n):
        px += 0.05 if i % 3 else -0.05
        vol += 65
        spec = TickSpec(
            mode=SubscriptionMode.SNAP_QUOTE, exchange_type=ExchangeType.NSE_FO,
            token="48704", exchange_timestamp_ms=base + i * 1000,
            last_traded_price=round(px, 2), volume_trade_for_the_day=vol,
            open_interest=16_300_000,
            bids=((round(px - 0.10, 2), 65),), asks=((round(px + 0.05, 2), 65),),
        )
        lines.append("bin:" + encode_tick(spec).hex())
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


class TestRecordingWiring:
    """The dashboard can tee a live session to disk and replay it back."""

    def _recording(self, root, n=30):
        import datetime as dt

        from orderflow.feed.protocol import ExchangeType, SubscriptionMode
        from orderflow.feed.simulator import TickSpec, encode_tick
        from orderflow.recorder import MultiRecorder

        ist = dt.timezone(dt.timedelta(hours=5, minutes=30))
        base = int(dt.datetime(2026, 10, 21, 10, 30, tzinfo=ist).timestamp() * 1000)
        rec = MultiRecorder(root, fsync_every=10)
        vol, px = 1000, 22895.0
        for i in range(n):
            px += 0.05 if i % 3 else -0.05
            vol += 65
            rec.write("conn0", encode_tick(TickSpec(
                mode=SubscriptionMode.SNAP_QUOTE,
                exchange_type=ExchangeType.NSE_FO, token="48704",
                exchange_timestamp_ms=base + i * 1000,
                last_traded_price=round(px, 2),
                volume_trade_for_the_day=vol,
                bids=((round(px - 0.10, 2), 65),),
                asks=((round(px + 0.05, 2), 65),),
            )), ts=base / 1000 + i)
        rec.close_all()

    def test_live_factory_tees_when_a_record_path_is_given(self, tmp_path):
        """A live start with a record path must wrap the socket in the tee."""
        import threading

        from orderflow.feed.transports import RecordingTransport, WebSocketTransport
        from dashboard import DashboardState, FeedRunner
        from orderflow.instrument_master import build_live_token_map

        mapper, _ = build_live_token_map("instruments.json")
        state = DashboardState()
        state.watchlist = default_watchlist(mapper)[:1]
        runner = FeedRunner(state, mapper)

        captured = {}

        def fake_run(make_conn):
            # The connector never opens a socket here: we only want the factory.
            captured["transport"] = make_conn().transport_factory(0)

        runner._run = fake_run
        runner.start_live(
            {"jwtToken": "j", "feedToken": "f"}, "k", "c", "f",
            record_path=str(tmp_path / "rec"),
        )
        for _ in range(50):
            if "transport" in captured:
                break
            threading.Event().wait(0.05)
        assert isinstance(captured["transport"], RecordingTransport)
        assert isinstance(captured["transport"]._inner, WebSocketTransport)

    def test_live_factory_is_bare_without_a_record_path(self, tmp_path):
        import threading

        from orderflow.feed.transports import RecordingTransport, WebSocketTransport
        from dashboard import DashboardState, FeedRunner
        from orderflow.instrument_master import build_live_token_map

        mapper, _ = build_live_token_map("instruments.json")
        state = DashboardState()
        state.watchlist = default_watchlist(mapper)[:1]
        runner = FeedRunner(state, mapper)
        captured = {}
        runner._run = lambda make_conn: captured.update(
            t=make_conn().transport_factory(0))
        runner.start_live({"jwtToken": "j", "feedToken": "f"}, "k", "c", "f")
        for _ in range(50):
            if "t" in captured:
                break
            threading.Event().wait(0.05)
        assert isinstance(captured["t"], WebSocketTransport)
        assert not isinstance(captured["t"], RecordingTransport)

    def test_replay_of_a_shard_directory_produces_bars(self, tmp_path):
        """A recorded directory must replay through the dashboard pipeline."""
        from dashboard import DashboardState, FeedRunner
        from orderflow.instrument_master import build_live_token_map

        self._recording(tmp_path / "rec")
        mapper, _ = build_live_token_map("instruments.json")
        state = DashboardState()
        state.watchlist = default_watchlist(mapper)[:1]
        runner = FeedRunner(state, mapper)
        runner.start_replay(str(tmp_path / "rec"))
        for _ in range(100):
            if not state.running:
                break
            time.sleep(0.05)
        assert state.status in ("stopped", "error"), state.status
        assert state.trades > 0, "no trades routed through the shard replay"
        assert state.charts, "no chart was built"


class TestReplay:
    @pytest.fixture()
    def client(self, tmp_path):
        """A TestClient whose mapper knows token 48704.

        The placeholder map has no such token, so the router would drop every
        tick and the test would pass trivially with zero trades. The real
        master is required to exercise the routing path.
        """
        pytest.importorskip("fastapi")
        from fastapi.testclient import TestClient

        if not Path("instruments.json").exists():
            pytest.skip("instruments.json not downloaded")
        from dashboard import WatchItem, build_app
        from orderflow.feed import TokenNamespace
        from orderflow.instrument_master import build_live_token_map

        mapper, _ = build_live_token_map("instruments.json")
        app = build_app(mapper)
        app.state.watchlist.append(WatchItem(
            label="NIFTY27OCT26FUT", namespace=TokenNamespace.NSE_FO,
            token=48704, symbol="NIFTY27OCT26FUT", market=Market.NSE,
            segment="NSE_FNO", tick=0.05,
        ))
        cap = tmp_path / "cap.txt"
        _in_session_capture(cap)
        return TestClient(app), cap

    def test_index_loads(self, client):
        tc, _ = client
        r = tc.get("/")
        assert r.status_code == 200
        assert "Order Flow Engine" in r.text

    def test_status_reports_the_watchlist(self, client):
        tc, _ = client
        body = tc.get("/api/status").json()
        assert body["watchlist"][0]["symbol"] == "NIFTY27OCT26FUT"
        assert body["watchlist"][0]["tick"] == 0.05

    def test_replay_builds_a_bar_with_delta_and_poc(self, client):
        """The whole point: capture in, footprint bar out."""
        tc, cap = client
        tc.post("/api/feed/start", json={"mode": "replay", "path": str(cap)})
        # The feed runs on a thread; give it a moment to drain the file.
        import time

        for _ in range(50):
            body = tc.get("/api/status").json()
            if body["trades"] >= 40:
                break
            time.sleep(0.1)
        assert body["trades"] == 40, body
        assert body["last_error"] is None

        chart = tc.get("/api/chart/NSE_FO_48704").json()
        assert chart["bars"], "no bars built from an in-session capture"
        bar = chart["bars"][-1]
        # The first print of a session carries the whole cumulative volume for
        # the day, so it counts as its own increment: 1065 + 39 * 65 = 3600.
        assert bar["volume"] == 1065 + 39 * 65
        assert bar["buy"] + bar["sell"] == bar["volume"]
        assert bar["delta"] == bar["buy"] - bar["sell"]
        assert bar["poc"] is not None
        assert chart["cumulative_delta"][-1]["d"] == bar["delta"]
        assert chart["profile"] is not None
        assert chart["profile"]["val"] <= chart["profile"]["poc"] <= chart["profile"]["vah"]

    def test_off_session_capture_is_dropped_not_folded(self, tmp_path):
        """A pre-open frame must not open a session or create a bar."""
        pytest.importorskip("fastapi")
        from fastapi.testclient import TestClient

        from dashboard import WatchItem, build_app
        from orderflow.feed import TokenNamespace
        from orderflow.instrument_master import build_placeholder_map

        # 04:05 IST: the timestamp of the real capture, which is pre-open.
        spec = TickSpec(
            mode=SubscriptionMode.SNAP_QUOTE, exchange_type=ExchangeType.NSE_FO,
            token="48704", exchange_timestamp_ms=1792535738307,
            last_traded_price=22895.2, volume_trade_for_the_day=1000,
            bids=((22893.1, 65),), asks=((22904.8, 65),),
        )
        cap = tmp_path / "pre.txt"
        cap.write_text("bin:" + encode_tick(spec).hex() + "\n", encoding="utf-8")

        from orderflow.instrument_master import build_live_token_map

        mapper, _ = build_live_token_map("instruments.json")
        app = build_app(mapper)
        app.state.watchlist.append(WatchItem(
            label="NIFTY27OCT26FUT", namespace=TokenNamespace.NSE_FO,
            token=48704, symbol="NIFTY27OCT26FUT", market=Market.NSE,
            segment="NSE_FNO", tick=0.05,
        ))
        tc = TestClient(app)
        tc.post("/api/feed/start", json={"mode": "replay", "path": str(cap)})
        import time

        for _ in range(30):
            if tc.get("/api/status").json()["trades"] >= 1:
                break
            time.sleep(0.1)
        chart = tc.get("/api/chart/NSE_FO_48704").json()
        assert chart["bars"] == [], "an off-session print created a bar"
