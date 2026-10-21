"""Record live order flow to disk.

This is the only way to obtain real order-flow data for a backtest. Angel One's
historical API serves candles, and a footprint cannot be rebuilt from OHLCV:
the bid/ask split at each price exists only in the live stream. So the data has
to be captured as it happens, and this is the tool that captures it.

Credentials come from the environment, never from arguments:

    ANGEL_API_KEY       API key from the SmartAPI dashboard
    ANGEL_CLIENT_CODE   client id, e.g. A123456
    ANGEL_PIN           4-digit MPIN
    ANGEL_TOTP_SECRET   base32 secret behind the authenticator app

Usage:

    # The desk list: NIFTY plus the four MCX contracts.
    python3 record_ticks.py --out ./recordings

    # Explicit tokens, with ticks, when the master is unavailable.
    python3 record_ticks.py --out ./recordings \\
        --instruments "NSE_FO:48704:0.05,MCX_FO:495213:1.0"

    # Stop at a wall-clock budget instead of running until the market closes.
    python3 record_ticks.py --out ./recordings --minutes 60

    # Verify credentials and the plan without touching the socket.
    python3 record_ticks.py --out ./recordings --dry-run

The process is meant to be run under a supervisor or `nohup`: it is safe to
kill at any moment. Shards are written with a `.part` suffix and promoted to
their final name only after the data is fsynced, so a `kill -9` loses at most
the last batch. Run with `--finalize` on startup (the default) to recover any
`.part` files a previous crash left behind.
"""

from __future__ import annotations

import argparse
import os
import signal
import sys
import time
from datetime import datetime, timedelta

from orderflow import IST
from orderflow.feed import (
    SubscriptionMode,
    build_plan,
    require_volume_capable_mode,
    totp,
)
from orderflow.feed.protocol import ExchangeType
from orderflow.instruments import UnknownToken
from orderflow.instrument_master import build_live_token_map
from orderflow.recorder import MultiRecorder, finalize_parts
from orderflow.feed.transports import RecordingTransport, WebSocketTransport
from orderflow.session import MarketCalendar
from orderflow.watchlist import WatchSpec, desk_watchlist, parse_instruments


class CredentialError(RuntimeError):
    pass


def credentials_from_env() -> dict:
    keys = ("ANGEL_API_KEY", "ANGEL_CLIENT_CODE", "ANGEL_PIN", "ANGEL_TOTP_SECRET")
    missing = [k for k in keys if not os.environ.get(k)]
    if missing:
        raise CredentialError(
            "missing credentials: " + ", ".join(missing) + "\n"
            "Set them in the environment; they are never read from arguments."
        )
    return {
        "api_key": os.environ["ANGEL_API_KEY"],
        "client_code": os.environ["ANGEL_CLIENT_CODE"],
        "pin": os.environ["ANGEL_PIN"],
        "totp_secret": os.environ["ANGEL_TOTP_SECRET"],
    }


def login(creds: dict) -> dict:
    from orderflow.feed.transports import login as do_login

    code = totp(creds["totp_secret"])
    return do_login(creds["api_key"], creds["client_code"], creds["pin"], code)


def session_state(specs: list[WatchSpec], now: datetime) -> dict:
    """Which of the instruments are inside their session right now.

    Worth reporting before a long run: a subscription outside its session is
    accepted and then streams nothing, which looks exactly like a dead socket.
    """
    out = {}
    for spec in specs:
        cal = MarketCalendar(spec.market, segment=spec.segment)
        win = cal.session_containing(now)
        out[spec.label] = (
            f"{win.open_at.strftime('%H:%M')}-{win.close_at.strftime('%H:%M')}"
            if win else "closed"
        )
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Record live order flow to disk.")
    ap.add_argument("--out", required=True, help="recording root directory")
    ap.add_argument("--instruments", default=None,
                    help="NAMESPACE:TOKEN[:TICK], comma separated")
    ap.add_argument("--minutes", type=float, default=None,
                    help="wall-clock budget; default is to run until stopped")
    ap.add_argument("--mode", default="SNAP_QUOTE",
                    choices=[m.name for m in SubscriptionMode],
                    help="SNAP_QUOTE carries depth and is the footprint mode")
    ap.add_argument("--fsync-every", type=int, default=200,
                    help="fsync after this many frames; lower is safer, slower")
    ap.add_argument("--master", default="instruments.json")
    ap.add_argument("--dry-run", action="store_true",
                    help="check credentials, master and plan; touch nothing")
    ap.add_argument("--rehearse", type=float, default=None, metavar="SECONDS",
                    help="run the whole path on scripted frames, no socket and "
                         "no credentials, for this many seconds")
    ap.add_argument("--no-finalize", action="store_true",
                    help="skip promoting .part files left by a crash")
    ap.add_argument("--max-reconnects", type=int, default=50)
    args = ap.parse_args(argv)

    now = datetime.now(IST)
    mode = SubscriptionMode[args.mode]

    if not args.no_finalize:
        fixed = finalize_parts(args.out)
        if fixed:
            print(f"recovered {len(fixed)} shard(s) left by a previous run:")
            for p in fixed:
                print(f"  {p}")

    # ---------------------------------------------------------- instrument list
    mapper = None
    if args.instruments:
        specs = parse_instruments(args.instruments)
    else:
        if not os.path.exists(args.master):
            print(f"error: {args.master} not found. Run fetch_instruments.py, "
                  f"or pass --instruments explicitly.", file=sys.stderr)
            return 2
        mapper, _ = build_live_token_map(args.master)
        specs = desk_watchlist(mapper)
        if not specs:
            print("error: no instruments resolved from the master", file=sys.stderr)
            return 2

    print(f"recording {len(specs)} instrument(s) at {now.strftime('%H:%M:%S IST')}")
    print()
    sessions = session_state(specs, now)
    print(f"{'INSTRUMENT':<28} {'MARKET':<5} {'TOKEN':>9} {'TICK':>6}  SESSION")
    print("-" * 68)
    for spec in specs:
        print(f"{spec.label:<28} {spec.market.value:<5} {spec.token:>9} "
              f"{spec.tick:>6}  {sessions[spec.label]}")
    print()

    open_now = [s for s in specs if sessions[s.label] != "closed"]
    if not open_now:
        print("warning: every instrument is outside its session right now.")
        print("         The subscription will be accepted and stream nothing.")
        print()

    # ------------------------------------------------------------------- plan
    try:
        require_volume_capable_mode(mode)
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    subs = [spec.subscription() for spec in specs]
    try:
        plan = build_plan(subs, mode=mode)
    except UnknownToken as exc:
        print(f"error: unknown token: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:
        print(f"error building the subscription plan: {exc}", file=sys.stderr)
        return 2

    print(f"plan: {len(plan.sessions)} connection(s), "
          f"mode {mode.name}, {len(subs)} token(s)")
    if args.dry_run:
        print("dry run: nothing was connected and nothing was written.")
        return 0

    # -------------------------------------------------------------- rehearsal
    if args.rehearse is not None:
        return _rehearse(args, specs, mapper, plan, mode, now)

    # ------------------------------------------------------------------ login
    try:
        creds = credentials_from_env()
    except CredentialError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    try:
        auth = login(creds)
    except Exception as exc:
        print(f"error: login failed: {exc}", file=sys.stderr)
        return 3
    print("login ok")

    jwt = auth.get("jwtToken") or auth.get("jwt") or ""
    feed_token = auth.get("feedToken") or auth.get("feed_token") or ""
    if not jwt or not feed_token:
        print(f"error: login response had no tokens: {sorted(auth)}", file=sys.stderr)
        return 3

    # --------------------------------------------------------------- recording
    recorder = MultiRecorder(args.out, fsync_every=args.fsync_every)
    # One key per connection: frames arrive interleaved on a socket and cannot
    # be attributed to an instrument before decoding, so a per-socket shard is
    # the only honest split. Replay routes them exactly as the live path does.
    stopped = {"flag": False}

    def _stop(signum, frame):
        stopped["flag"] = True

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    def factory(index: int):
        ws = WebSocketTransport(
            auth_token=jwt, api_key=creds["api_key"],
            client_code=creds["client_code"], feed_token=feed_token,
        )
        return RecordingTransport(ws, recorder, key=f"conn{index}")

    from orderflow.feed import SmartStreamConnector

    if mapper is None:
        # --instruments path: no master was loaded, so build one that knows the
        # tokens we are subscribing to. Without it the router drops every tick.
        mapper = _mapper_for_tokens(specs)
    connector = SmartStreamConnector(
        plan, mapper=mapper, transport_factory=factory,
        on_error=lambda exc: print(f"  feed error: {type(exc).__name__}: {exc}",
                                   file=sys.stderr),
    )

    started = time.monotonic()
    deadline = None if args.minutes is None else started + args.minutes * 60
    print(f"recording to {os.path.abspath(args.out)}")
    print("press Ctrl-C to stop\n")

    try:
        while not stopped["flag"]:
            remaining = None if deadline is None else max(1.0, deadline - time.monotonic())
            if remaining is not None and remaining <= 1.0:
                break
            try:
                connector.run(
                    on_trade=lambda rt: None,
                    max_seconds=remaining if remaining is not None else 300.0,
                    reconnect=True,
                    max_reconnects=args.max_reconnects,
                )
            except KeyboardInterrupt:
                break
            if deadline is None:
                # No budget: run in bounded chunks and check the stop flag, so
                # Ctrl-C is honoured within one chunk.
                continue
            if time.monotonic() >= deadline:
                break
    except KeyboardInterrupt:
        pass
    finally:
        recorder.flush_all()
        recorder.close_all()

    # ------------------------------------------------------------------ report
    stats = connector.stats
    rec_stats = recorder.stats()
    total_frames = sum(s["frames"] for s in rec_stats.values())
    elapsed = time.monotonic() - started
    print()
    print(f"stopped after {elapsed/60:.1f} min")
    print(f"socket frames: {stats.frames}  ticks: {stats.ticks}  "
          f"heartbeats: {stats.heartbeats}")
    print(f"recorded:      {total_frames} frames")
    print(f"reconnects:    {stats.reconnects}  protocol errors: {stats.protocol_errors}")
    for key, s in sorted(rec_stats.items()):
        print(f"  {key}: {s['frames']} frames, {s['bytes']} payload bytes, "
              f"{s['rotations']} rotation(s)")
    print(f"shards in {os.path.abspath(args.out)}:")
    from orderflow.recorder import iter_shards, shard_summary

    for shard in iter_shards(args.out):
        try:
            summary = shard_summary(shard)
            print(f"  {shard}  {summary['records']} records, "
                  f"{shard.stat().st_size/1024:.1f} KB")
        except Exception as exc:
            print(f"  {shard}  UNREADABLE: {exc}")

    # A silent subscription must not pass as a quiet market.
    return 0 if total_frames > 0 else 1


def _rehearse(args, specs, mapper, plan, mode, now) -> int:
    """Exercise capture end to end with scripted frames and no socket.

    The point is to make the market-open run a formality. Everything below the
    transport is production code, so a rehearsal that records and replays
    cleanly means the only untested piece left is the broker's socket. It needs
    no credentials, so it can be run at any hour.

    Frames are anchored inside the session rather than at the wall clock. A
    capture taken outside its session produces no bars by design, so a rehearsal
    stamped "now" would prove the plumbing and nothing else.
    """
    from orderflow.feed.simulator import TickSpec, encode_tick
    from orderflow.feed.transports import RehearsalTransport

    print("rehearsal: scripted frames, no socket, no credentials\n")

    session_start = _session_start_for(specs[0], now)
    print(f"anchoring frames to {session_start.strftime('%Y-%m-%d %H:%M')} IST "
          f"(inside the {specs[0].market.value} session)\n")

    recorder = MultiRecorder(args.out, fsync_every=args.fsync_every)

    if mapper is None:
        mapper = _mapper_for_tokens(specs)

    # A tick every 250 ms of session time, walking the price both ways so delta,
    # POC and the value area all have something to disagree about.
    base_ms = int(session_start.timestamp() * 1000)
    px = 22895.0
    volume = 1000
    frames = []
    count = max(1, int(args.rehearse / 0.25))
    for i in range(count):
        px += 0.05 if i % 3 else -0.05
        volume += 65
        spec = specs[i % len(specs)]
        frames.append(encode_tick(TickSpec(
            mode=mode,
            exchange_type=ExchangeType.NSE_FO if spec.market.value == "NSE"
            else ExchangeType.MCX_FO,
            token=str(spec.token),
            exchange_timestamp_ms=base_ms + i * 250,
            last_traded_price=round(px, 2),
            volume_trade_for_the_day=volume,
            bids=((round(px - 0.10, 2), 65),),
            asks=((round(px + 0.05, 2), 65),),
        )))

    subscribes = []

    def factory(index: int):
        return RecordingTransport(
            RehearsalTransport(frames, on_subscribe=subscribes.append),
            recorder, key=f"conn{index}")

    from orderflow.feed import SmartStreamConnector

    connector = SmartStreamConnector(
        plan, mapper=mapper, transport_factory=factory,
        on_error=lambda exc: print(f"  feed error: {type(exc).__name__}: {exc}",
                                   file=sys.stderr),
    )
    connector.run(on_trade=lambda rt: None, max_seconds=args.rehearse,
                  reconnect=False)
    recorder.flush_all()
    recorder.close_all()

    stats = connector.stats
    print(f"subscription sent: {len(subscribes)}")
    print(f"frames={stats.frames} ticks={stats.ticks} "
          f"errors={stats.protocol_errors} unmapped={stats.unmapped}")

    from orderflow.recorder import iter_shards, shard_summary

    shards = list(iter_shards(args.out))
    for shard in shards:
        summary = shard_summary(shard)
        print(f"  {shard}  {summary['records']} records, "
              f"{shard.stat().st_size/1024:.1f} KB")

    if not shards or stats.frames == 0:
        print("rehearsal FAILED: nothing was recorded", file=sys.stderr)
        return 1
    if stats.unmapped:
        print(f"rehearsal FAILED: {stats.unmapped} frames were dropped as "
              f"unmapped; the replay would look like a decoder fault",
              file=sys.stderr)
        return 1

    print("\nrehearsal ok: the capture path works. Replay it with:")
    first = specs[0]
    print(f"  python3 replay_backtest.py --rec {args.out} --key conn0 "
          f'--instruments "{first.namespace.value}:{first.token}:{first.tick}"')
    return 0


def _session_start_for(spec: WatchSpec, now: datetime) -> datetime:
    """A timestamp inside `spec`'s session on `now`'s date.

    Used to anchor rehearsal frames. If the session has not opened yet the
    frames are placed just after the open; if it has closed they are placed just
    before the close. A rehearsal must land inside the session or the session
    chart discards every print, which is correct behaviour and useless as a
    test.
    """
    cal = MarketCalendar(spec.market, segment=spec.segment)
    if cal.session_containing(now) is not None:
        return now
    # session_for returns the window even when `now` is outside it, so its
    # bounds are the anchors to clamp to.
    window = cal.session_for(now.date())
    if now < window.open_at:
        return window.open_at
    if now >= window.close_at:
        return window.close_at - timedelta(minutes=5)
    return window.open_at



def _mapper_for_tokens(specs):
    """A mapper covering the subscribed tokens when no master was loaded.

    The placeholder map has fixed ids and would not know a caller's token, so
    the router would drop every tick while the socket reported traffic.
    """
    from orderflow.instruments import (
        Exchange, Instrument, Segment, SessionKind, TokenMapper,
    )

    mapper = TokenMapper()
    for spec in specs:
        inst = Instrument(
            symbol=spec.symbol, exchange=Exchange.MCX if spec.market.value == "MCX"
            else Exchange.NSE, segment=Segment.FUT, tick_size=spec.tick,
            session=SessionKind.MCX_NONAGRI if spec.market.value == "MCX"
            else SessionKind.NSE_FNO,
        )
        mapper.register(spec.namespace, spec.token, inst)
    return mapper


if __name__ == "__main__":
    raise SystemExit(main())
