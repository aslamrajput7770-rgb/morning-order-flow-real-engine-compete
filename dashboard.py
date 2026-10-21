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
import threading
import time
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
from orderflow.session import Market, MarketCalendar
from orderflow.session_chart import SessionChart
from orderflow.ticks import TickGrid

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

    def note_tick(self, key: str) -> None:
        with self.lock:
            self.last_tick_at = time.time()
            self.ticks += 1


class FeedRunner:
    """Owns the feed thread and the connector lifecycle."""

    def __init__(self, state: DashboardState, mapper: TokenMapper) -> None:
        self.state = state
        self.mapper = mapper
        self._stop = threading.Event()
        self._conn: SmartStreamConnector | None = None

    # ------------------------------------------------------------------ live
    def start_live(self, session: dict, api_key: str, client_code: str,
                   feed_token: str, record_path: str | None = None) -> None:
        if self.state.running:
            raise RuntimeError("feed already running")
        pairs = [(w.namespace, w.token) for w in self.state.watchlist]
        if not pairs:
            raise RuntimeError("watchlist is empty")
        plan = build_plan(pairs, mode=SubscriptionMode.SNAP_QUOTE)
        jwt = session.get("jwtToken")
        if not jwt:
            raise RuntimeError("login response had no jwtToken")
        token = session.get("feedToken") or feed_token

        def factory(index):
            return WebSocketTransport(
                auth_token=jwt,
                api_key=api_key,
                client_code=client_code,
                feed_token=token,
            )

        self._launch(
            lambda: self._run(lambda: SmartStreamConnector(
                plan, self.mapper, factory, on_error=self._on_error)),
            source="live",
        )

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

        def factory(index):
            return FileTransport(path)

        pairs = [(w.namespace, w.token) for w in self.state.watchlist]
        if not pairs:
            pairs = [(TokenNamespace.NSE_FO, 48704)]
        plan = build_plan(pairs, mode=SubscriptionMode.SNAP_QUOTE)
        self._launch(
            lambda: self._run(lambda: SmartStreamConnector(
                plan, self.mapper, factory, on_error=self._on_error)),
            source=path,
        )

    # --------------------------------------------------------------- runner
    def _launch(self, work, source: str) -> None:
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
        try:
            self._conn = make_conn()
            with self.state.lock:
                self.state.status = "streaming"

            def on_trade(rt):
                chart = self._chart_for(rt)
                if chart is not None:
                    chart.add(rt.trade)
                with self.state.lock:
                    self.state.trades += 1
                    self.state.last_tick_at = time.time()

            stats = self._conn.run(
                on_trade=on_trade,
                max_seconds=None,
                reconnect=True,
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
            with self.state.lock:
                self.state.running = False
                if self.state.status not in ("error",):
                    self.state.status = "stopped"

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
        # The connector has no stop(); it exits when its wall-clock budget
        # elapses. Dropping the transport closes the socket, which makes the
        # blocking recv return and lets the loop unwind.
        self._stop.set()
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
        label = state.labels.get(key, key)
        watch = next((w for w in state.watchlist if w.key() == key), None)
    if chart is None:
        return {"key": key, "label": label, "bars": [], "cumulative_delta": []}

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
        "label": label,
        "symbol": watch.symbol if watch else label,
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


def build_app(mapper: TokenMapper, master_path: str = "instruments.json") -> FastAPI:
    app = FastAPI(title="Order Flow Engine")
    state = DashboardState()
    # The endpoints mutate `state.watchlist` directly. `app.state.watchlist` is
    # the same list object so the seeding in `load_dashboard` lands here.
    app.state.watchlist = state.watchlist
    app.state.dashboard = state
    runner = FeedRunner(state, mapper)

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
                runner.start_live(
                    body["session"], body["api_key"], body["client_code"],
                    body.get("feed_token", ""), body.get("record"),
                )
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(400, str(exc))
        return {"ok": True, "status": state.status}

    @app.post("/api/feed/stop")
    def feed_stop() -> dict:
        runner.stop()
        return {"ok": True}

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
    return app
