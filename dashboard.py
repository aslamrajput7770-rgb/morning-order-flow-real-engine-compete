"""Live order-flow dashboard: FastAPI backend.

This serves the same engine `live_feed.py` drives, but over HTTP and a
WebSocket so a browser can watch it. Nothing about the pipeline changes: the
connector, the router, the tick reader and the session chart are the ones the
CLI already uses. The dashboard is a view onto that, not a second
implementation, because two implementations of a footprint disagree eventually.

Design notes worth keeping:

* One background thread owns the feed. The WebSocket handler only reads the
  latest snapshot. A browser that stalls therefore cannot stall the socket that
  Angel One is streaming on, which is the failure that matters most here.
* Snapshots are built on demand from the session chart, not pushed per tick.
  A tick burst of thousands per second would otherwise become thousands of
  JSON documents per second for one browser.
* Subscriptions are capped at Angel One's limits (3 connections, 1000 tokens
  each). Exceeding them is a hard error, not a silent truncation.
"""

from __future__ import annotations

import asyncio
import json
import os
import threading
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import date

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

from orderflow.core import Side
from orderflow.feed import (
    ExchangeType,
    SubscriptionMode,
    TokenNamespace,
    build_plan,
    decode_packet,
)
from orderflow.feed.connector import SmartStreamConnector
from orderflow.feed.transports import FileTransport, WebSocketTransport
from orderflow.instrument_master import (
    TokenMapper,
    build_live_token_map,
    find_instrument,
    load_master,
    upcoming_futures,
)
from orderflow.session import Market, MarketCalendar, UnionCalendar
from orderflow.session_chart import SessionChart
from orderflow.supervisor import AlwaysOnFeed, SupervisorConfig
from orderflow.ticks import TickGrid
from orderflow import session_store

#: How the supervisor's health state maps to the status the page shows. A
#: state not listed here (idle, stopped) keeps whatever the runner set, so a
#: finished replay still reads "stopped" rather than a supervisor word.
_STATUS_FROM_HEALTH = {
    "waiting": "market closed",
    "connecting": "connecting",
    "streaming": "streaming",
    "reconnecting": "reconnecting",
    "error": "error",
}


@dataclass
class WatchItem:
    """One instrument the dashboard is streaming."""

    label: str
    namespace: TokenNamespace
    token: int
    symbol: str
    market: Market
    segment: str
    tick: float
    expiry: str | None = None
    #: Contract size. Required for a correct paper PnL: without it every fill
    #: is one unit and the rupee number is meaningless. Defaults to 1 so a
    #: caller that does not know it still gets a coherent (if unscaled) result.
    lot_size: int = 1

    def key(self) -> str:
        # Path-safe: this string is used as a URL segment.
        return f"{self.namespace.value}_{self.token}"


@dataclass
class DashboardState:
    """Everything the feed thread writes and the browser reads.

    A single lock guards it. Snapshots are built under the lock and handed out
    as plain dicts, so the browser side never touches engine objects.
    """

    watchlist: list[WatchItem] = field(default_factory=list)
    charts: dict[str, SessionChart] = field(default_factory=dict)
    labels: dict[str, str] = field(default_factory=dict)
    lock: threading.Lock = field(default_factory=threading.Lock)
    running: bool = False
    status: str = "idle"
    started_at: float | None = None
    last_tick_at: float | None = None
    frames: int = 0
    ticks: int = 0
    trades: int = 0
    protocol_errors: int = 0
    reconnects: int = 0
    unmapped: int = 0
    last_error: str | None = None
    feed_thread: threading.Thread | None = None
    source: str = "live"  # "live" or a capture path
    #: The supervisor's health view (state, next_open_at, attempts, ...). Empty
    #: until a supervised live run starts. This is how the page can tell "market
    #: closed" from "feed broken" instead of showing a silent zero.
    feed: dict = field(default_factory=dict)
    #: The previous run's snapshots, loaded at boot. Present so the page shows
    #: the last known POC and PnL rather than an empty box.
    last_session: dict | None = None
    #: True when the server holds credentials, so the page can say why there is
    #: no live session instead of leaving it a mystery.
    live_configured: bool = False

    def note_tick(self, key: str) -> None:
        with self.lock:
            self.last_tick_at = time.time()
            self.ticks += 1

    def begin_run(self) -> None:
        """Clear everything a single feed run owns, before the next one starts.

        Charts and counters belong to one run, not to the process. Keeping them
        across runs made a second replay append its bars to the first: six bars
        became twelve, and the volume, delta and POC shown were the sum of two
        unrelated captures. The watchlist is configuration, so it stays, and so
        does `last_session`, which belongs to the *previous* run by definition.
        """
        with self.lock:
            self.charts = {}
            self.labels = {}
            self.frames = 0
            self.ticks = 0
            self.trades = 0
            self.protocol_errors = 0
            self.reconnects = 0
            self.unmapped = 0
            self.last_tick_at = None


class FeedRunner:
    """Owns the feed thread and the connector lifecycle."""

    def __init__(self, state: DashboardState, mapper: TokenMapper,
                 store: "session_store.SessionStore | None" = None) -> None:
        self.state = state
        self.mapper = mapper
        self.store = store or session_store.SessionStore()
        self._stop = threading.Event()
        self._conn: SmartStreamConnector | None = None
        self._supervisor: AlwaysOnFeed | None = None
        self._supervised = False
        self._calendar = MarketCalendar(Market.NSE, segment="NSE_FNO")
        self._recorder = None
        # Paper trading is off unless a caller enables it. When on, every routed
        # trade is fed to the auto-trader as well as to the chart, so the same
        # prints drive both the display and the decisions.
        self.trader = None
        self.paper_config = None
        self.paper_enabled = False
        self._paper_bar_seconds = 300.0

    # ----------------------------------------------------------- paper trading
    def enable_paper(self, config=None, bar_seconds: float = 300.0) -> None:
        """Turn on autonomous paper trading for the current watchlist."""
        from orderflow.autotrade import AutoTrader, AutoTraderConfig
        from orderflow.paper import CostModel, PaperBroker

        cfg = config or AutoTraderConfig()
        cfg.bar_seconds = bar_seconds
        self.paper_enabled = True
        self._paper_bar_seconds = bar_seconds

        def calendar_for(symbol, market, segment=None):
            return MarketCalendar(market, segment=segment or (
                "MCX_FO" if market is Market.MCX else "NSE_FNO"))

        def broker_factory():
            return PaperBroker(cash=cfg.cash if hasattr(cfg, "cash") else 1_000_000.0,
                               cost_model=CostModel())

        self.trader = AutoTrader(
            calendar_for, cfg, broker_factory=broker_factory,
            bar_seconds=bar_seconds,
        )
        self.paper_config = cfg
        self.sync_paper_watchlist()

    def sync_paper_watchlist(self) -> None:
        """Register the current watchlist with the paper trader.

        Called at every run start, not only when paper is enabled: the watchlist
        is seeded after `build_app` and can be edited from the page, so the
        trader must pick up whatever is being streamed at the moment the feed
        actually starts.
        """
        if self.trader is None:
            return
        for w in self.state.watchlist:
            lot = w.lot_size
            if lot <= 1:
                # The watchlist may not carry a lot size. The master does, and
                # the mapper was built from it, so ask there before falling back
                # to one: a wrong lot size scales every rupee of PnL.
                try:
                    inst = self.mapper.by_token(w.namespace, w.token)
                    lot = inst.lot_size
                except Exception:
                    lot = w.lot_size
            self.trader.register(
                w.symbol, w.market, tick_size=w.tick, lot_size=lot,
            )

    def _paper_on_trade(self, rt) -> None:
        if self.trader is None:
            return
        inst = rt.instrument
        symbol = getattr(inst, "symbol", "")
        self.trader.on_trade(symbol, rt.trade)

    def paper_snapshot(self) -> dict | None:
        """The paper account, or None when paper trading is off."""
        if self.trader is None:
            return None
        acct = self.trader.account()
        acct["positions"] = self.trader.all_positions()
        return acct

    # ------------------------------------------------------------------ live
    def calendar_for_watchlist(self):
        """A calendar covering every market in the watchlist.

        A desk watchlist mixes NSE and MCX. A single NSE calendar would end the
        feed at 15:40 and silently drop the MCX evening session, so build the
        union of the markets actually watched. With no watchlist, fall back to
        NSE F&O, which is the common case.
        """
        markets: list[tuple[Market, str]] = []
        for w in self.state.watchlist:
            pair = (w.market, w.segment)
            if pair not in markets:
                markets.append(pair)
        if not markets:
            markets = [(Market.NSE, "NSE_FNO")]
        cals = [MarketCalendar(m, segment=s) for m, s in markets]
        return cals[0] if len(cals) == 1 else UnionCalendar(cals)

    def start_live(self, session: dict, api_key: str, client_code: str,
                   feed_token: str, record_path: str | None = None) -> None:
        if self.state.running:
            raise RuntimeError("feed already running")
        pairs = [(w.namespace, w.token) for w in self.state.watchlist]
        if not pairs:
            raise RuntimeError("watchlist is empty")
        jwt = session.get("jwtToken")
        if not jwt:
            raise RuntimeError("login response had no jwtToken")
        token = session.get("feedToken") or feed_token
        self._calendar = self.calendar_for_watchlist()

        def make_connector():
            # Rebuild the transport on every supervisor (re)build, because the
            # supervisor rebuilds precisely when the old socket or token is
            # suspect. A fresh plan is rebuilt too: the watchlist may have
            # changed since the last run.
            plan = build_plan(
                [(w.namespace, w.token) for w in self.state.watchlist],
                mode=SubscriptionMode.SNAP_QUOTE)

            def factory(index):
                ws = WebSocketTransport(
                    auth_token=jwt,
                    api_key=api_key,
                    client_code=client_code,
                    feed_token=token,
                )
                if record_path:
                    # Tee the live stream to disk so the session can be replayed
                    # later. The tee is transparent: a write failure is swallowed
                    # and the feed keeps running, because a full disk must not
                    # take the desk's data feed down with it.
                    from orderflow.recorder import MultiRecorder
                    from orderflow.feed.transports import RecordingTransport

                    if self._recorder is None:
                        self._recorder = MultiRecorder(record_path)
                    return RecordingTransport(ws, self._recorder, key=f"conn{index}")
                return ws

            return SmartStreamConnector(
                plan, self.mapper, factory, on_error=self._on_error)

        def make_connector_capture():
            conn = make_connector()
            # `_on_trade` mirrors the connector's counters for the header.
            self._conn = conn
            return conn

        self._supervised = True
        self._launch(lambda: self._run(make_connector_capture), source="live")

    # -------------------------------------------------------------- replay
    def start_replay(self, path: str) -> None:
        """Stream a captured file through the same pipeline.

        This is the live-backtest path. The frames are the ones the recorder
        wrote, so a replay exercises the decoder and the aggregation exactly as
        a live session did. It is not a simulation: no synthetic trades are
        invented anywhere.
        """
        if self.state.running:
            raise RuntimeError("feed already running")
        if not os.path.exists(path):
            raise FileNotFoundError(
                f"no capture at {path!r}. Point at a directory of .oft.gz "
                "shards, or at a single .oft.gz file"
            )

        def factory(index):
            # A directory holds .oft.gz shards from record_ticks.py; a single
            # .oft.gz file is one shard; anything else is the older line
            # format. All three feed the same pipeline.
            if os.path.isdir(path) or str(path).endswith(".oft.gz"):
                from orderflow.feed.transports import ShardTransport

                return ShardTransport(
                    path, f"conn{index}",
                    shards=None if os.path.isdir(path) else [path],
                )
            return FileTransport(path)

        pairs = [(w.namespace, w.token) for w in self.state.watchlist]
        if not pairs:
            pairs = [(TokenNamespace.NSE_FO, 48704)]
        plan = build_plan(pairs, mode=SubscriptionMode.SNAP_QUOTE)

        def make_connector():
            conn = SmartStreamConnector(
                plan, self.mapper, factory, on_error=self._on_error)
            # `_on_trade` mirrors the connector's counters, and a replay has a
            # single connector for the whole run.
            self._conn = conn
            return conn

        self._supervised = False
        self._launch(lambda: self._run(make_connector), source=path)

    # --------------------------------------------------------------- runner
    def _launch(self, work, source: str) -> None:
        # A new run must not inherit the previous run's charts or counters.
        self.state.begin_run()
        if self.paper_enabled:
            # Rebuild the paper book for the new run: positions and PnL belong
            # to one session, not to the process. This also re-reads the
            # watchlist, which the page may have edited since the last run.
            self.enable_paper(self.paper_config, self._paper_bar_seconds)
        with self.state.lock:
            self.state.running = True
            self.state.status = "starting"
            self.state.source = source
            self.state.started_at = time.time()
            self.state.last_error = None
        self._stop.clear()
        t = threading.Thread(target=work, name="orderflow-feed", daemon=True)
        self.state.feed_thread = t
        t.start()

    def _run(self, make_conn) -> None:
        """Run the feed. A live session is supervised; a replay is one pass.

        The supervisor is what makes a live feed survive a session boundary, an
        expired token and a run of failures. Driving the connector directly, as
        this used to, meant the feed could not tell a closed market from a dead
        socket: it would sit in `streaming` with zero ticks all night.
        """
        if self._supervised:
            self._run_supervised(make_conn)
        else:
            self._run_once(make_conn)

    def _run_supervised(self, make_conn) -> None:
        try:
            self._supervisor = AlwaysOnFeed(
                self._calendar, make_conn,
                SupervisorConfig(), on_trade=self._on_trade,
            )
            poller = threading.Thread(
                target=self._sync_health, name="orderflow-health", daemon=True)
            poller.start()
            # Returns only when the operator stops it. A supervised feed is
            # meant to outlive every session, so it has no natural end.
            self._supervisor.run()
        except Exception as exc:  # noqa: BLE001 - surfaced to the UI
            with self.state.lock:
                self.state.last_error = f"{type(exc).__name__}: {exc}"
                self.state.status = "error"
        finally:
            self._finish_run()

    def _run_once(self, make_conn) -> None:
        """One pass over the transport. This is the replay path."""
        try:
            self._conn = make_conn()
            with self.state.lock:
                self.state.status = "streaming"
            stats = self._conn.run(
                on_trade=self._on_trade,
                max_seconds=None,
                reconnect=False,
                clock=time.monotonic,
            )
            with self.state.lock:
                self.state.frames = stats.frames
                self.state.ticks = stats.ticks
                self.state.trades = stats.trades
                self.state.protocol_errors = stats.protocol_errors
                self.state.reconnects = stats.reconnects
                self.state.unmapped = stats.unmapped
        except Exception as exc:  # noqa: BLE001 - surfaced to the UI
            with self.state.lock:
                self.state.last_error = f"{type(exc).__name__}: {exc}"
                self.state.status = "error"
        finally:
            self._finish_run()

    def _on_trade(self, rt) -> None:
        """Fold one routed print into the chart, the trader and the counters.

        The chart and the paper trader are fed from here and nowhere else, so
        the two can never disagree about which prints they saw. The trader runs
        after the chart so a trader exception cannot cost a bar.
        """
        chart = self._chart_for(rt)
        if chart is not None:
            chart.add(rt.trade)
        if self.trader is not None:
            try:
                self._paper_on_trade(rt)
            except Exception as exc:  # noqa: BLE001
                with self.state.lock:
                    self.state.last_error = f"paper trader: {exc}"
        # The connector's counters are only returned when `run` ends, and a
        # supervised feed does not end on its own. Mirror them here so the
        # header shows a live count instead of a frozen zero.
        conn_stats = getattr(self._conn, "stats", None)
        with self.state.lock:
            self.state.trades += 1
            if conn_stats is not None:
                self.state.frames = conn_stats.frames
                self.state.ticks = conn_stats.ticks
                self.state.unmapped = conn_stats.unmapped
                self.state.protocol_errors = conn_stats.protocol_errors
            self.state.last_tick_at = time.time()

    def _sync_health(self) -> None:
        """Mirror the supervisor's health into the state the page reads.

        The supervisor knows why it is not streaming (closed market, backoff,
        reconnecting) and the connector does not. Without this the page can only
        show a status string it sets itself, which is how a closed market came
        to look like a stalled feed.
        """
        while not self._stop.is_set():
            sup = self._supervisor
            if sup is not None:
                health = sup.snapshot()
                with self.state.lock:
                    self.state.feed = health
                    mapped = _STATUS_FROM_HEALTH.get(health["state"])
                    if mapped is not None and self.state.running:
                        self.state.status = mapped
            self._stop.wait(0.5)

    def _finish_run(self) -> None:
        """Close the recorder, persist the run, and mark the feed stopped."""
        # Stop the health poller: its loop is bounded by this flag, and a run
        # that has ended has no health left to mirror.
        self._stop.set()
        if self._recorder is not None:
            try:
                self._recorder.flush_all()
                self._recorder.close_all()
            except Exception as exc:  # noqa: BLE001 - surfaced to the UI
                with self.state.lock:
                    self.state.last_error = (
                        f"recorder close failed: {type(exc).__name__}: {exc}")
            self._recorder = None
        self._persist()
        final_feed = self.feed_health()
        with self.state.lock:
            self.state.running = False
            if final_feed:
                # Keep the last health view so the page reads "stopped", not the
                # "streaming" it last saw while the run was live.
                self.state.feed = final_feed
            if self.state.status not in ("error",):
                self.state.status = "stopped"

    def _snapshot_payload(self) -> dict:
        """The last-session snapshot: the same JSON the page renders."""
        with self.state.lock:
            keys = [w.key() for w in self.state.watchlist]
            source = self.state.source
            started_at = self.state.started_at
            ticks = self.state.ticks
            trades = self.state.trades
        return {
            "saved_at": time.time(),
            "started_at": started_at,
            "source": source,
            "ticks": ticks,
            "trades": trades,
            "charts": {k: chart_snapshot(self.state, k) for k in keys},
            "paper": self.paper_snapshot(),
        }

    def _persist(self) -> None:
        """Save the run for the next boot, if it produced anything.

        An empty run must not overwrite a good snapshot with zeros: that would
        turn "the last session had a POC" into "the last session was blank" on
        the strength of a run that never received a tick. The test is whether the
        run *saw* anything, not whether the payload has charts: a populated
        watchlist yields chart stubs even when no tick ever arrived.
        """
        try:
            payload = self._snapshot_payload()
        except Exception:  # noqa: BLE001 - never let persistence break a run
            return
        if not payload["ticks"] and not payload["trades"]:
            return
        if self.store.save(payload):
            with self.state.lock:
                self.state.last_session = payload

    def feed_health(self) -> dict:
        """The supervisor's health, or an empty view before a live run."""
        sup = self._supervisor
        return sup.snapshot() if sup is not None else {}

    def _chart_for(self, rt) -> SessionChart | None:
        inst = rt.instrument
        symbol = getattr(inst, "symbol", "") or ""
        with self.state.lock:
            watch = next(
                (w for w in self.state.watchlist if w.symbol == symbol), None)
            key = watch.key() if watch else symbol
            chart = self.state.charts.get(key)
            if chart is None:
                if watch is not None:
                    tick, mkt, seg = watch.tick, watch.market, watch.segment
                else:
                    tick = getattr(inst, "tick_size", 0.05) or 0.05
                    is_mcx = "MCX" in str(inst.exchange.value)
                    mkt = Market.MCX if is_mcx else Market.NSE
                    seg = "MCX_FO" if is_mcx else "NSE_FNO"
                chart = SessionChart(
                    TickGrid(tick), MarketCalendar(mkt, segment=seg),
                    bar_seconds=300)
                self.state.charts[key] = chart
                self.state.labels[key] = symbol or key
        return chart

    def _on_error(self, exc) -> None:
        with self.state.lock:
            self.state.last_error = f"{type(exc).__name__}: {exc}"
            self.state.protocol_errors += 1

    def stop(self) -> None:
        # A supervised feed is stopped through the supervisor, which also
        # unblocks its wait-for-open sleep. The connector has no stop(); its
        # `should_stop` callback polls the supervisor's flag between frames.
        self._stop.set()
        if self._supervisor is not None:
            self._supervisor.stop()
        self._conn = None
        with self.state.lock:
            self.state.running = False
            self.state.status = "stopped"


# --------------------------------------------------------------- snapshots
def _level_row(level) -> dict:
    return {
        "price": round(level.price, 4),
        "bid": level.bid_vol,
        "ask": level.ask_vol,
        "total": level.total_vol,
        "delta": level.delta,
        "prints": level.prints,
    }


def bar_snapshot(bar) -> dict:
    return {
        "start": bar.start_ts,
        "end": bar.end_ts,
        "open": bar.open,
        "high": bar.high,
        "low": bar.low,
        "close": bar.close,
        "volume": bar.total_volume,
        "buy": bar.buy_volume,
        "sell": bar.sell_volume,
        "delta": bar.delta,
        "poc": bar.poc,
        "trades": bar.trade_count,
        "levels": [_level_row(l) for l in bar.sorted_levels],
        "imbalances": [
            {
                "price": i.price,
                "side": i.side.value if hasattr(i.side, "value") else str(i.side),
                "ratio": round(i.ratio, 2),
                "dominant": i.dominant_vol,
                "weak": i.weak_vol,
                "stacked": i.stacked,
            }
            for i in bar.imbalances()
        ],
    }


def chart_snapshot(state: DashboardState, key: str, bars: int = 60) -> dict:
    with state.lock:
        chart = state.charts.get(key)
        label = state.labels.get(key)
        watch = next((w for w in state.watchlist if w.key() == key), None)
    # The label is a display string, never a token. `NSE_FO_48704` is a URL key,
    # and showing it to a trader looks like a corrupt feed. Prefer the recorded
    # label, then the watch symbol, and fall back to a dash.
    display = label or (watch.symbol if watch else None) or "-"
    symbol = (watch.symbol if watch else None) or label or None
    if chart is None:
        # An empty stub: the page must render "no data", not a token number.
        return {
            "key": key, "label": display, "symbol": symbol,
            "bars": [], "cumulative_delta": [], "profile": None,
        }

    st = chart.state
    bars_out = []
    if st is not None:
        bars_out = [bar_snapshot(b) for b in st.chart.bars[-bars:]]
    cum = []
    running = 0.0
    if st is not None:
        for b in st.chart.bars:
            running += b.delta
            cum.append({"t": b.end_ts, "d": running})

    profile = None
    if st is not None and st.chart.bars:
        from orderflow.profile import ProfileBuilder

        profs = ProfileBuilder(chart.grid).build(st.chart.bars)
        if profs:
            prof = profs[-1]
            va = prof.value_area()
            profile = {
                "poc": va.poc,
                "vah": va.vah,
                "val": va.val,
                "total": va.total_vol,
                "levels": [
                    {"price": round(l.price, 4), "volume": l.total_vol}
                    for l in prof.sorted_levels
                ],
            }

    return {
        "key": key,
        "label": display,
        "symbol": symbol,
        "tick": chart.grid.tick_size,
        "bar_seconds": chart.bar_seconds,
        "bars": bars_out,
        "cumulative_delta": cum[-240:],
        "profile": profile,
        "session": (
            st.session_open.date().isoformat()
            if st is not None and hasattr(st.session_open, "date")
            else None
        ),
    }


def _login(credentials: dict) -> dict:
    """Log in to the broker and return the session tokens.

    Split out so a test can substitute it without a network. The recorder and
    the feed both need the JWT and the feed token from this response.
    """
    from orderflow.feed.transports import login as do_login
    from orderflow.feed import totp

    return do_login(
        credentials["api_key"], credentials["client_code"],
        credentials["pin"], totp(credentials["totp_secret"]),
    )


def build_app(
    mapper: TokenMapper,
    master_path: str = "instruments.json",
    credentials: dict | None = None,
    record_dir: str | None = None,
    session_path: str | None = None,
) -> FastAPI:
    """Build the dashboard app.

    `credentials` and `record_dir` let the page start a live session on its
    own. Without them the page can still search instruments and replay a
    recording, and "Start live" says why it cannot proceed rather than opening
    a socket it has no token for.

    `session_path` is where the last run is remembered, so a boot with no live
    session shows the last known POC and PnL instead of a blank page.
    """
    state = DashboardState()
    store = session_store.SessionStore(
        session_path or session_store.DEFAULT_PATH)
    runner = FeedRunner(state, mapper, store)
    # "Configured" means a login is actually possible. A lone feed token is not
    # enough, and telling the page it is configured would promise a live session
    # that could never start.
    state.live_configured = bool(
        credentials
        and all(credentials.get(k) for k in
                ("api_key", "client_code", "pin", "totp_secret")))
    # Load the previous run before the app serves anything, so the first
    # snapshot the page fetches already carries it.
    state.last_session = store.load()

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        """Bring the live feed up at boot and leave it up.

        Wired to the lifespan rather than import so a test that builds the app
        does not open a socket. With no credentials there is nothing to start
        and the page says so; a replay is still available.
        """
        if credentials and state.watchlist:
            def boot():
                try:
                    session = _login(credentials)
                    runner.start_live(
                        session,
                        credentials["api_key"], credentials["client_code"],
                        session.get("feedToken", ""),
                        record_dir,
                    )
                except Exception as exc:  # noqa: BLE001 - surfaced on the page
                    with state.lock:
                        state.last_error = f"autostart: {type(exc).__name__}: {exc}"
                        state.status = "error"

            threading.Thread(target=boot, name="orderflow-autostart",
                             daemon=True).start()
        yield
        runner.stop()

    app = FastAPI(title="Order Flow Engine", lifespan=lifespan)
    # The endpoints mutate `state.watchlist` directly. `app.state.watchlist` is
    # the same list object so the seeding in `load_dashboard` lands here.
    app.state.watchlist = state.watchlist
    app.state.dashboard = state
    app.state.runner = runner
    # Paper trading is on by default: the desk asked for a feed that analyses
    # and trades on its own, not one that waits to be started. It can be turned
    # off with POST /api/paper {"enabled": false}.
    runner.enable_paper()

    @app.get("/", response_class=HTMLResponse)
    def index() -> str:
        from dashboard_ui import INDEX_HTML

        return INDEX_HTML

    @app.get("/api/status")
    def status() -> dict:
        with state.lock:
            return {
                "running": state.running,
                "status": state.status,
                "source": state.source,
                "frames": state.frames,
                "ticks": state.ticks,
                "trades": state.trades,
                "protocol_errors": state.protocol_errors,
                "reconnects": state.reconnects,
                "unmapped": state.unmapped,
                "last_error": state.last_error,
                "last_tick_at": state.last_tick_at,
                "started_at": state.started_at,
                # The supervisor's view: why the feed is or is not streaming.
                # "market closed" and "waiting for open" are answers; a silent
                # zero is not.
                "feed": dict(state.feed),
                "live_configured": state.live_configured,
                "last_session": state.last_session,
                "paper": runner.paper_snapshot(),
                "watchlist": [
                    {
                        "key": w.key(), "label": w.label, "symbol": w.symbol,
                        "market": w.market.value, "tick": w.tick,
                        "expiry": w.expiry, "token": w.token,
                    }
                    for w in state.watchlist
                ],
            }

    @app.get("/api/instruments")
    def instruments(q: str = "") -> dict:
        """Search the master. Used by the picker."""
        q = q.strip().upper()
        if not q:
            return {"results": []}
        out = []
        for (ns, tok), inst in mapper._by_token.items():
            sym = (inst.symbol or "").upper()
            if q in sym or q in (inst.underlying or "").upper():
                out.append({
                    "symbol": inst.symbol,
                    "token": tok,
                    "namespace": ns.value,
                    "expiry": inst.expiry,
                    "segment": getattr(inst.segment, "name", str(inst.segment)),
                })
                if len(out) >= 40:
                    break
        return {"results": out}

    @app.post("/api/watchlist/add")
    def add(item: dict) -> dict:
        symbol = (item.get("symbol") or "").strip()
        if not symbol:
            raise HTTPException(400, "symbol required")
        inst = find_instrument(mapper, symbol)
        if inst is None:
            raise HTTPException(404, f"unknown symbol {symbol!r}")
        token = None
        namespace = None
        for (ns, tok), i in mapper._by_token.items():
            if i is inst:
                token, namespace = tok, ns
                break
        if token is None:
            raise HTTPException(404, f"no token for {symbol!r}")
        mkt = Market.MCX if namespace is TokenNamespace.MCX_FO else Market.NSE
        tick = _tick_for_watch(inst, mkt)
        seg = "MCX_FO" if mkt is Market.MCX else "NSE_FNO"
        with state.lock:
            if any(w.namespace is namespace and w.token == token
                   for w in state.watchlist):
                return {"ok": True, "note": "already watching"}
            state.watchlist.append(WatchItem(
                label=inst.symbol, namespace=namespace, token=token,
                symbol=inst.symbol, market=mkt, segment=seg, tick=tick,
                expiry=inst.expiry,
            ))
        return {"ok": True}

    @app.post("/api/watchlist/remove")
    def remove(item: dict) -> dict:
        key = item.get("key")
        with state.lock:
            state.watchlist = [w for w in state.watchlist if w.key() != key]
            state.charts.pop(key, None)
        return {"ok": True}

    @app.get("/api/chart/{key}")
    def chart(key: str, bars: int = 60) -> dict:
        return chart_snapshot(state, key, bars=bars)

    @app.post("/api/feed/start")
    def feed_start(body: dict) -> dict:
        mode = body.get("mode", "live")
        try:
            if mode == "replay":
                runner.start_replay(body["path"])
            else:
                # Log in here when the server holds credentials. The page never
                # sees the PIN or the TOTP secret, and the login happens on the
                # server's network rather than in the browser.
                session = body.get("session")
                api_key = body.get("api_key", "")
                client_code = body.get("client_code", "")
                feed_token = body.get("feed_token", "")
                if session is None:
                    if not credentials:
                        raise HTTPException(
                            400,
                            "no credentials on the server: set ANGEL_API_KEY, "
                            "ANGEL_CLIENT_CODE, ANGEL_PIN and ANGEL_TOTP_SECRET, "
                            "or start a replay instead",
                        )
                    session = _login(credentials)
                    api_key = credentials["api_key"]
                    client_code = credentials["client_code"]
                    feed_token = session.get("feedToken", "")
                runner.start_live(
                    session, api_key, client_code, feed_token,
                    body.get("record") or record_dir,
                )
        except HTTPException:
            raise
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(400, str(exc))
        return {"ok": True, "status": state.status}

    @app.post("/api/feed/stop")
    def feed_stop() -> dict:
        runner.stop()
        return {"ok": True}

    @app.get("/api/paper")
    def paper_get() -> dict:
        return {
            "enabled": runner.paper_enabled,
            "account": runner.paper_snapshot(),
        }

    @app.post("/api/paper")
    def paper_set(body: dict) -> dict:
        """Turn autonomous paper trading on or off.

        This is the one switch the desk asked for: on, the engine takes and
        manages its own paper positions from the live tape; off, it only
        analyses. There is no per-trade manual entry, by design.
        """
        enabled = bool(body.get("enabled", True))
        if enabled:
            bar_seconds = float(body.get("bar_seconds", 300.0))
            runner.enable_paper(bar_seconds=bar_seconds)
        else:
            runner.paper_enabled = False
            runner.trader = None
        return {"ok": True, "enabled": runner.paper_enabled}

    @app.websocket("/ws")
    async def ws(sock: WebSocket) -> None:
        await sock.accept()
        try:
            while True:
                with state.lock:
                    keys = [w.key() for w in state.watchlist]
                    running = state.running
                    status_txt = state.status
                    ticks = state.ticks
                    trades = state.trades
                    err = state.last_error
                payload = {
                    "running": running,
                    "status": status_txt,
                    "ticks": ticks,
                    "trades": trades,
                    "error": err,
                    "paper": runner.paper_snapshot(),
                    "charts": {k: chart_snapshot(state, k) for k in keys},
                }
                await sock.send_text(json.dumps(payload, default=str))
                await asyncio.sleep(0.5)
        except WebSocketDisconnect:
            return
        except Exception:
            return

    return app


def _tick_for_watch(inst, market: Market) -> float:
    """Thin alias for the shared rule, kept because tests and callers use it."""
    from orderflow.watchlist import tick_for

    return tick_for(inst.underlying or inst.symbol or "", market)


def default_watchlist(mapper: TokenMapper) -> list[WatchItem]:
    """NSE stock plus the four MCX commodities the desk asked for."""
    from orderflow.watchlist import desk_watchlist

    return [
        WatchItem(
            label=w.label, namespace=w.namespace, token=w.token, symbol=w.symbol,
            market=w.market, segment=w.segment, tick=w.tick, expiry=w.expiry,
        )
        for w in desk_watchlist(mapper)
    ]


def load_dashboard(master_path: str = "instruments.json") -> FastAPI:
    mapper, stats = build_live_token_map(master_path)
    app = build_app(mapper, master_path)
    # Seed the watchlist so the page is useful on first load.
    for item in default_watchlist(mapper):
        app.state.watchlist.append(item)
    # The watchlist is seeded after build_app, so register it with the paper
    # trader now. A later run re-syncs anyway, which also covers edits made
    # from the page.
    app.state.runner.sync_paper_watchlist()
    return app
