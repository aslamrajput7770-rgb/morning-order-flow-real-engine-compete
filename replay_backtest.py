"""Replay a recording into footprint bars.

This is the backtest path. It reads `.oft.gz` shards written by
`record_ticks.py` and pushes the frames through the same decoder, router, tick
reader and session chart that a live socket feeds. Nothing is re-derived from a
summary: if a bar comes out wrong here, it is wrong live too, and vice versa.

That property is the whole point. A backtest that uses a second, "simpler"
implementation of the analytics proves nothing about the engine that trades.

Usage:

    # One instrument's whole recording, as bars and a session profile.
    python3 replay_backtest.py --rec recordings --key conn0 \\
        --instruments "NSE_FO:48704:0.05"

    # Write the bars to JSON for a notebook or the dashboard.
    python3 replay_backtest.py --rec recordings --key conn0 \\
        --instruments "NSE_FO:48704:0.05" --out bars.json

    # Cap the run, useful while a recording is still being appended to.
    python3 replay_backtest.py --rec recordings --key conn0 \\
        --instruments "NSE_FO:48704:0.05" --max-frames 50000

A recording is a raw socket stream, so it holds every instrument that was
subscribed on that connection. `--instruments` tells the router how to name the
tokens; `--filter` narrows the output to one of them.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime

from orderflow.feed import (
    ShardTransport,
    SmartStreamConnector,
    SubscriptionMode,
    build_plan,
)
from orderflow.instruments import (
    Exchange,
    Instrument,
    Segment,
    SessionKind,
    TokenMapper,
)
from orderflow.recorder import iter_shards, read_shard, shard_summary
from orderflow.session import IST, Market, MarketCalendar
from orderflow.session_chart import SessionChart
from orderflow.ticks import TickGrid
from orderflow.watchlist import parse_instruments


def build_mapper(specs):
    """A mapper covering the subscribed tokens.

    The router keys on `(namespace, token)`. Without an entry here every frame
    is dropped as unmapped, and the run reports traffic with no trades, which
    looks like a decoder failure rather than a missing registration.
    """
    mapper = TokenMapper()
    for spec in specs:
        is_mcx = spec.market is Market.MCX
        mapper.register(spec.namespace, spec.token, Instrument(
            symbol=spec.symbol,
            exchange=Exchange.MCX if is_mcx else Exchange.NSE,
            segment=Segment.FUT, tick_size=spec.tick,
            session=SessionKind.MCX_NONAGRI if is_mcx else SessionKind.NSE_FNO,
        ))
    return mapper


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Replay a recording into bars.")
    ap.add_argument("--rec", required=True, help="recording root")
    ap.add_argument("--key", default=None,
                    help="connection shard directory, e.g. conn0")
    ap.add_argument("--instruments", required=True,
                    help="NAMESPACE:TOKEN[:TICK], comma separated")
    ap.add_argument("--filter", default=None,
                    help="only report this token's bars")
    ap.add_argument("--bar-seconds", type=int, default=300)
    ap.add_argument("--max-frames", type=int, default=None)
    ap.add_argument("--out", default=None, help="write bars to this JSON file")
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args(argv)

    specs = parse_instruments(args.instruments)
    if not specs:
        print("error: no instruments parsed", file=sys.stderr)
        return 2

    # ------------------------------------------------------------------ shards
    shards = list(iter_shards(args.rec, args.key))
    if not shards:
        print(f"error: no shards under {args.rec}"
              + (f"/{args.key}" if args.key else ""), file=sys.stderr)
        return 2
    if not args.quiet:
        print(f"{len(shards)} shard(s):")
        total = 0
        for shard in shards:
            try:
                summary = shard_summary(shard)
                total += summary["records"]
                print(f"  {shard.name}  {summary['records']} records  "
                      f"{shard.stat().st_size/1024:.1f} KB")
            except Exception as exc:
                print(f"  {shard.name}  UNREADABLE: {exc}")
        print(f"  total {total} records\n")

    # --------------------------------------------------------------- pipeline
    mapper = build_mapper(specs)
    plan = build_plan([s.subscription() for s in specs],
                      mode=SubscriptionMode.SNAP_QUOTE)

    charts: dict[str, SessionChart] = {}
    calendars = {}
    for spec in specs:
        calendars[spec.token] = (spec, MarketCalendar(spec.market, segment=spec.segment))

    def on_trade(rt) -> None:
        token = None
        try:
            token = rt.instrument.symbol
        except Exception:
            pass
        # The symbol is the parse-time label, which is the token string.
        for spec in specs:
            if spec.symbol == token:
                chart = charts.get(spec.symbol)
                if chart is None:
                    chart = SessionChart(
                        TickGrid(spec.tick),
                        MarketCalendar(spec.market, segment=spec.segment),
                        bar_seconds=args.bar_seconds,
                    )
                    charts[spec.symbol] = chart
                chart.add(rt.trade)
                return

    transport = ShardTransport(args.rec, args.key)
    if args.max_frames is not None:
        # Trim before connecting: the transport loads eagerly, and a month of
        # records is more than a smoke test needs.
        transport.connect()
        transport._records = transport._records[: args.max_frames]
        factory = lambda i: transport
    else:
        factory = lambda i: transport

    connector = SmartStreamConnector(
        plan, mapper=mapper, transport_factory=factory,
        on_error=lambda exc: print(f"  error: {type(exc).__name__}: {exc}",
                                   file=sys.stderr),
    )
    stats = connector.run(on_trade=on_trade, reconnect=False)

    print(f"replayed: frames={stats.frames} ticks={stats.ticks} "
          f"heartbeats={stats.heartbeats} errors={stats.protocol_errors} "
          f"reconnects={stats.reconnects}")

    if not charts:
        print("no trades routed. Check --instruments: an unregistered token is "
              "dropped silently.", file=sys.stderr)
        return 1

    # ----------------------------------------------------------------- report
    payload = {}
    for symbol, chart in sorted(charts.items()):
        bars = chart.bars
        profile = chart.session_profile()
        print(f"\n{symbol}: {len(bars)} bar(s), "
              f"{chart.session_volume:.0f} volume, "
              f"cumulative delta {chart.cumulative_delta:.0f}, "
              f"off-session {chart.off_session_trades}")
        for bar in bars:
            when = datetime.fromtimestamp(bar.start_ts, tz=IST).strftime("%H:%M")
            print(f"  {when}  O{bar.open} H{bar.high} L{bar.low} "
                  f"C{bar.close}  vol={bar.total_volume:.0f} "
                  f"delta={bar.delta:+.0f} poc={bar.poc}")
        if profile is not None:
            va = profile.value_area()
            print(f"  profile: vah={va.vah} poc={va.poc} val={va.val}")

        payload[symbol] = {
            "bars": [
                {
                    "start": datetime.fromtimestamp(
                        bar.start_ts, tz=IST).isoformat(),
                    "open": bar.open, "high": bar.high, "low": bar.low,
                    "close": bar.close, "volume": bar.total_volume,
                    "delta": bar.delta, "poc": bar.poc,
                }
                for bar in bars
            ],
            "cumulative_delta": chart.cumulative_delta,
            "session_volume": chart.session_volume,
            "off_session_trades": chart.off_session_trades,
        }
        if profile is not None:
            va = profile.value_area()
            payload[symbol]["profile"] = {
                "vah": va.vah, "poc": va.poc, "val": va.val,
            }

    if args.out:
        with open(args.out, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2)
        print(f"\nwrote {args.out}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
