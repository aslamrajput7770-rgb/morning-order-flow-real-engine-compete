"""Run the always-on paper-trading desk.

This is the entry point that matches how the desk actually works: the feed comes
up and stays up, the engine analyses every completed bar, and paper trades are
taken and managed automatically. Nobody sits and clicks start.

    # Live, from the environment credentials, unattended:
    python3 run_trader.py --instruments "NSE_FO:48704:0.05"

    # Prove the whole path on a recording first (no credentials, no socket):
    python3 run_trader.py --replay recordings --key conn0 \\
        --instruments "NSE_FO:48704:0.05"

    # Watch the paper account as it works:
    python3 run_trader.py ... --report-every 30

The supervisor loop is the same object in both modes; only the transport
differs. That is deliberate: a strategy proven on a replay is exercising the
identical decision code that will run live, so the replay means something.

Nothing here is a promise of profit. It is a disciplined, fully automatic
order-flow trader with costs, slippage and stops modelled honestly, so that
whatever PnL it reports is a number worth believing.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading
import time
from datetime import datetime

from orderflow.autotrade import AutoTrader, AutoTraderConfig
from orderflow.feed import (
    ShardTransport,
    SmartStreamConnector,
    SubscriptionMode,
    build_plan,
)
from orderflow.instruments import Exchange, Instrument, Segment, SessionKind, TokenMapper
from orderflow.paper import CostModel, PaperBroker
from orderflow.session import IST, Market, MarketCalendar
from orderflow.strategy import StrategyConfig
from orderflow.supervisor import AlwaysOnFeed, SupervisorConfig
from orderflow.ticks import TickGrid
from orderflow.watchlist import WatchSpec, parse_instruments


def build_mapper(specs) -> TokenMapper:
    """A mapper covering the subscribed tokens, so the router can name them."""
    mapper = TokenMapper()
    for spec in specs:
        mapper.register(
            spec.namespace,
            spec.token,
            Instrument(
                symbol=spec.symbol,
                exchange=Exchange.NSE if spec.market is Market.NSE else Exchange.MCX,
                segment=Segment.FUT if "FUT" in spec.symbol else Segment.EQ,
                tick_size=spec.tick,
                lot_size=getattr(spec, "lot_size", 1),
                session=SessionKind.MCX_NONAGRI if spec.market is Market.MCX
                else SessionKind.NSE_FNO,
                underlying=getattr(spec, "underlying", None),
            ),
        )
    return mapper


def calendar_for(symbol: str, market: Market, segment: str | None = None) -> MarketCalendar:
    return MarketCalendar(market, segment=segment or (
        "MCX_FO" if market is Market.MCX else "NSE_FNO"))


def build_trader(args, specs) -> AutoTrader:
    cfg = AutoTraderConfig()
    cfg.strategy = StrategyConfig(
        min_strength=args.min_strength,
        imbalance_ratio=args.imbalance_ratio,
        stack_min=args.stack_min,
        min_stacked=args.min_stacked,
        stop_atr_mult=args.stop_atr,
        target_atr_mult=args.target_atr,
        atr_bars=args.atr_bars,
        min_stop_ticks=args.min_stop_ticks,
        max_unknown_frac=args.max_unknown_frac,
    )
    cfg.lots = args.lots
    cfg.cooldown_seconds = args.cooldown
    # Only pin the stop/target if the operator explicitly asked for fixed ticks.
    cfg.stop_ticks = args.stop_ticks
    cfg.target_ticks = args.target_ticks

    def broker_factory():
        return PaperBroker(
            cash=args.cash,
            cost_model=CostModel(),
            slippage_ticks=args.slippage_ticks,
        )

    return AutoTrader(
        calendar_for, cfg, broker_factory=broker_factory,
        bar_seconds=args.bar_seconds,
        on_signal=lambda st, sig, px: print(
            f"[{datetime.now(IST):%H:%M:%S}] ENTRY {sig.detail()} filled {px:g}",
            flush=True),
        on_exit=lambda st, why: print(
            f"[{datetime.now(IST):%H:%M:%S}] EXIT  {st.symbol} ({why})",
            flush=True),
    )


def register_instruments(trader: AutoTrader, specs) -> None:
    for spec in specs:
        trader.register(
            spec.label, spec.market, tick_size=spec.tick,
            lot_size=getattr(spec, "lot_size", 1),
        )


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Always-on order-flow paper trader.")
    ap.add_argument("--instruments", required=True,
                    help="NAMESPACE:TOKEN[:TICK], comma separated")
    ap.add_argument("--replay", default=None,
                    help="replay a recording directory instead of a live socket")
    ap.add_argument("--key", default="conn0", help="shard subdirectory name")
    ap.add_argument("--bar-seconds", type=float, default=300.0)
    ap.add_argument("--lots", type=int, default=1)
    ap.add_argument("--cash", type=float, default=1_000_000.0)
    ap.add_argument("--slippage-ticks", type=int, default=1)
    ap.add_argument("--cooldown", type=float, default=300.0,
                    help="seconds to wait after an exit before re-entering")
    # strategy knobs
    ap.add_argument("--min-strength", type=float, default=0.60)
    ap.add_argument("--imbalance-ratio", type=float, default=3.0)
    ap.add_argument("--stack-min", type=int, default=3)
    ap.add_argument("--min-stacked", type=int, default=2)
    ap.add_argument("--max-unknown-frac", type=float, default=0.40)
    ap.add_argument("--stop-atr", type=float, default=1.5,
                    help="stop distance as a multiple of the average bar range")
    ap.add_argument("--target-atr", type=float, default=3.0,
                    help="target distance as a multiple of the average bar range")
    ap.add_argument("--atr-bars", type=int, default=14)
    ap.add_argument("--min-stop-ticks", type=int, default=8,
                    help="floor for the stop, so it never sits inside the spread")
    ap.add_argument("--stop-ticks", type=int, default=None,
                    help="override: pin the stop to a fixed tick count")
    ap.add_argument("--target-ticks", type=int, default=None,
                    help="override: pin the target to a fixed tick count")
    ap.add_argument("--report-every", type=float, default=60.0,
                    help="seconds between account reports; 0 disables")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args(argv)

    specs = parse_instruments(args.instruments)
    mapper = build_mapper(specs)
    trader = build_trader(args, specs)
    register_instruments(trader, specs)

    print(f"always-on paper trader: {len(specs)} instrument(s), "
          f"bar {args.bar_seconds:.0f}s, min strength {args.min_strength}")
    for spec in specs:
        print(f"  {spec.label:<28} {spec.namespace.value}:{spec.token}  tick {spec.tick}")
    print()

    stop = threading.Event()

    def handle_signal(signum, frame):
        print(f"\n[{datetime.now(IST):%H:%M:%S}] stop requested, winding down...",
              flush=True)
        stop.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    # ------------------------------------------------------------ feed source
    if args.replay:
        from orderflow.recorder import iter_shards

        shards = list(iter_shards(args.replay, args.key))
        if not shards:
            print(f"error: no shards under {args.replay}/{args.key}", file=sys.stderr)
            return 2
        print(f"replaying {len(shards)} shard(s)")
        transport_holder = {"t": None}

        def factory(index):
            if transport_holder["t"] is None:
                transport_holder["t"] = ShardTransport(args.replay, args.key)
            return transport_holder["t"]

        def on_trade(rt):
            sym = getattr(rt.instrument, "symbol", "")
            trader.on_trade(sym, rt.trade)

        plan = build_plan([s.subscription() for s in specs], mode=SubscriptionMode.SNAP_QUOTE)
        connector = SmartStreamConnector(plan, mapper, transport_factory=factory)
        stats = connector.run(on_trade=on_trade, reconnect=False)
        print(f"\nreplay done: frames={stats.frames} trades={stats.trades} "
              f"unmapped={stats.unmapped}")
        _print_report(trader)
        return 0

    # ------------------------------------------------------------------- live
    from orderflow.feed.transports import login, WebSocketTransport

    api_key = os.environ.get("ANGEL_API_KEY")
    client_code = os.environ.get("ANGEL_CLIENT_CODE")
    pin = os.environ.get("ANGEL_PIN")
    totp_secret = os.environ.get("ANGEL_TOTP_SECRET")
    missing = [k for k, v in (
        ("ANGEL_API_KEY", api_key), ("ANGEL_CLIENT_CODE", client_code),
        ("ANGEL_PIN", pin), ("ANGEL_TOTP_SECRET", totp_secret)) if not v]
    if missing:
        print(f"error: missing credentials: {', '.join(missing)}", file=sys.stderr)
        return 2

    from orderflow.feed.totp import totp_now

    session_holder = {"session": None}
    calendar = MarketCalendar(Market.NSE, segment="NSE_FNO")

    def make_connector():
        # Re-login on every rebuild: the supervisor rebuilds precisely when the
        # old token is suspect, so reusing it would defeat the rebuild.
        data = login(api_key, client_code, pin, totp_now(totp_secret))
        session_holder["session"] = data
        jwt = data.get("jwtToken")
        feed_token = data.get("feedToken")
        if not jwt:
            raise RuntimeError("login response had no jwtToken")

        def factory(index):
            return WebSocketTransport(
                auth_token=jwt, api_key=api_key,
                client_code=client_code, feed_token=feed_token,
            )

        plan = build_plan([s.subscription() for s in specs],
                          mode=SubscriptionMode.SNAP_QUOTE)
        conn = SmartStreamConnector(plan, mapper, transport_factory=factory)
        return conn

    def on_trade(rt):
        sym = getattr(rt.instrument, "symbol", "")
        trader.on_trade(sym, rt.trade)

    feed = AlwaysOnFeed(
        calendar, make_connector,
        SupervisorConfig(verbose=args.verbose),
        on_trade=on_trade,
    )

    reporter = None
    if args.report_every > 0:
        def _loop():
            while not stop.wait(args.report_every):
                _print_report(trader, feed)
        reporter = threading.Thread(target=_loop, daemon=True)
        reporter.start()

    try:
        feed.run()
    finally:
        stop.set()
        _print_report(trader, feed)
    return 0


def _print_report(trader: AutoTrader, feed: AlwaysOnFeed | None = None) -> None:
    acct = trader.account()
    print(f"\n=== paper account {datetime.now(IST):%Y-%m-%d %H:%M:%S} IST ===")
    print(f"  equity {acct['equity']:,.2f}  net {acct['net_pnl']:,.2f}  "
          f"realized {acct['realized_pnl']:,.2f}  unrealized {acct['unrealized_pnl']:,.2f}  "
          f"costs {acct['costs']:,.2f}")
    print(f"  entries {acct['entries']}  exits {acct['exits']}  "
          f"round trips {len(acct['round_trips'])}")
    for p in trader.all_positions():
        if p["qty"] != 0:
            print(f"  OPEN {p['symbol']} qty {p['qty']:g} @ {p['avg_price']:g} "
                  f"last {p['last_price']:g} stop {p['stop']} target {p['target']} "
                  f"uPnL {p['unrealized']:,.2f}")
    if feed is not None:
        snap = feed.snapshot()
        print(f"  feed {snap['state']} frames {snap['frames']} "
              f"reconnects {snap['reconnects']} relogins {snap['relogins']} "
              f"{snap['detail']}")
    print(flush=True)


if __name__ == "__main__":
    raise SystemExit(main())
