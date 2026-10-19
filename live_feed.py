"""Live Angel One SmartStream runner.

Reads credentials from the environment, never from arguments or code:

    ANGEL_API_KEY       API key from the SmartAPI dashboard
    ANGEL_CLIENT_CODE   your Angel One client id, e.g. A123456
    ANGEL_PIN           your 4-digit MPIN
    ANGEL_TOTP_SECRET   the base32 secret behind your authenticator app
    ANGEL_FEED_TOKEN    optional; from a previous login response

The instrument master is required. Live subscriptions need real token numbers,
and the placeholder ids in `build_indian_token_map()` are not real: a live
subscription to them is accepted, streams nothing, and reports no error.

    python3 fetch_instruments.py             # once, ~36 MB, refreshed daily

Usage:

    # 1. Check credentials, session and plan; touch nothing.
    python3 live_feed.py --underlying NIFTY --dry-run

    # 2. Stream for five minutes, record every frame, print a summary.
    python3 live_feed.py --underlying NIFTY --minutes 5 --record capture.txt

    # 3. Subscribe by explicit token.
    python3 live_feed.py --instruments "NSE_FO:48704,MCX_FO:495213" --minutes 5

    # 4. Offline smoke test without the master (returns no live data).
    python3 live_feed.py --instruments "NSE_FO:26000" --allow-placeholder-tokens --dry-run

The runner never prints a credential, and never writes one to disk. The
recording contains market data only. Exit code is non-zero when the feed
produced no data, so a silent subscription cannot pass as a quiet market.
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime

from orderflow import IST, Market, MarketCalendar, SessionChart, build_indian_token_map
from orderflow.feed import (
    SubscriptionMode,
    TokenNamespace,
    build_plan,
    require_volume_capable_mode,
    totp,
)
from orderflow.instruments import UnknownToken
from orderflow.instrument_master import build_live_token_map, upcoming_futures
from orderflow.feed.transports import WebSocketTransport, record_frames


class CredentialError(RuntimeError):
    pass


def credentials_from_env() -> dict:
    """Read credentials from the environment. No defaults, no fallbacks."""
    keys = {
        "api_key": "ANGEL_API_KEY",
        "client_code": "ANGEL_CLIENT_CODE",
        "pin": "ANGEL_PIN",
        "totp_secret": "ANGEL_TOTP_SECRET",
    }
    values = {}
    missing = []
    for field, env in keys.items():
        v = os.environ.get(env)
        if not v:
            missing.append(env)
        values[field] = v
    if missing:
        raise CredentialError(
            "missing environment variables: " + ", ".join(missing) + "\n"
            "Set them in the shell, or as registered secrets. Do not paste "
            "credentials into a chat or commit them."
        )
    values["feed_token"] = os.environ.get("ANGEL_FEED_TOKEN", "")
    return values


def login(values: dict) -> dict:
    """Exchange credentials for a JWT and feed token."""
    from orderflow.feed.transports import login as rest_login

    code = totp(values["totp_secret"])
    return rest_login(
        api_key=values["api_key"],
        client_code=values["client_code"],
        pin=values["pin"],
        totp=code,
    )


def parse_instruments(spec: str) -> list[tuple[TokenNamespace, int]]:
    """Parse `"NSE_FO:26000,MCX_FO:42809"` into (namespace, token) pairs."""
    out = []
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        if ":" not in part:
            raise ValueError(f"bad instrument {part!r}, expected NAMESPACE:TOKEN")
        ns_name, token = part.split(":", 1)
        try:
            ns = TokenNamespace[ns_name.strip().upper()]
        except KeyError:
            valid = ", ".join(n.name for n in TokenNamespace)
            raise ValueError(f"unknown namespace {ns_name!r}; valid: {valid}") from None
        out.append((ns, int(token)))
    return out


def resolve_instruments(args, mode) -> tuple:
    """Turn CLI arguments into (namespace, token) pairs, and the mapper.

    Live tokens must come from the instrument master. The placeholder ids in
    `build_indian_token_map()` are not real: a live subscription to them is
    accepted, returns no data at all, and reports no error. Two of the five do
    not exist in the master. So an unknown token is a hard failure here, not a
    warning.
    """
    import pathlib

    want_master = not args.allow_placeholder_tokens
    mapper = None
    if want_master:
        path = pathlib.Path(args.master)
        if not path.exists():
            raise SystemExit(
                f"instrument master not found at {path}\n"
                "Live subscriptions need real token numbers. Run:\n"
                "    python3 fetch_instruments.py\n"
                "or pass --allow-placeholder-tokens to try anyway (returns no data)."
            )
        mapper, stats = build_live_token_map(path)
        print(f"instrument master: {stats}")

    pairs = []
    if args.underlying:
        if mapper is None:
            raise SystemExit("--underlying needs the instrument master")
        for name in args.underlying.split(","):
            got = upcoming_futures(mapper, name.strip(), limit=1)
            if not got:
                raise SystemExit(f"no unexpired future found for {name.strip()!r}")
            ns, tok, inst = got[0]
            print(f"resolved {name.strip().upper():<12} -> {inst.symbol} "
                  f"token={tok} exp={inst.expiry} tick={inst.tick_size}")
            pairs.append((ns, tok))

    if args.instruments:
        pairs.extend(parse_instruments(args.instruments))

    if not pairs:
        raise SystemExit(
            "no instruments given. Use --underlying NIFTY, or "
            "--instruments NSE_FO:48704. See `python3 fetch_instruments.py --show NIFTY`."
        )

    if mapper is not None:
        for ns, tok in pairs:
            try:
                inst = mapper.by_token(ns, tok)
            except UnknownToken:
                raise SystemExit(
                    f"token {tok} in {ns.value} is not in the instrument master.\n"
                    "Subscribing to an unknown token returns no data and reports no "
                    "error, so this is refused. Find the real token with:\n"
                    f"    python3 fetch_instruments.py --show <UNDERLYING>"
                )
            print(f"verified  {ns.value}:{tok}  {inst.symbol}  "
                  f"exp={inst.expiry}  tick={inst.tick_size}")

    return mapper, pairs


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Live Angel One SmartStream feed")
    ap.add_argument("--instruments", default=None,
                    help="comma-separated NAMESPACE:TOKEN, e.g. NSE_FO:48704")
    ap.add_argument("--underlying", default=None,
                    help="resolve the nearest future by underlying, e.g. NIFTY "
                         "(needs instruments.json)")
    ap.add_argument("--master", default="instruments.json",
                    help="instrument master file; required for live tokens")
    ap.add_argument("--allow-placeholder-tokens", action="store_true",
                    help="permit the offline placeholder tokens (they return NO data "
                         "on a live feed and report no error)")
    ap.add_argument("--mode", default="SNAP_QUOTE",
                    choices=["QUOTE", "SNAP_QUOTE", "DEPTH"])
    ap.add_argument("--minutes", type=float, default=1.0, help="how long to stream")
    ap.add_argument("--record", default=None, help="write raw frames to this file")
    ap.add_argument("--dry-run", action="store_true",
                    help="validate credentials, plan and session; do not stream")
    ap.add_argument("--no-reconnect", action="store_true")
    args = ap.parse_args(argv)

    mode = SubscriptionMode[args.mode]
    # LTP has no volume and would produce a silently empty footprint.
    require_volume_capable_mode(mode)

    mapper, resolved = resolve_instruments(args, mode)
    instruments = resolved
    plan = build_plan(instruments, mode=mode)
    print(f"plan: {plan.total} subscriptions across {len(plan.sessions)} connection(s)")
    for i, sess in enumerate(plan.sessions):
        by_ns: dict[str, int] = {}
        for s in sess:
            by_ns[s.namespace.value] = by_ns.get(s.namespace.value, 0) + 1
        print(f"  connection {i}: {len(sess)} tokens {by_ns}")

    now = datetime.now(IST)
    print(f"now: {now:%Y-%m-%d %H:%M:%S} IST ({now:%A})")
    cal = MarketCalendar(Market.NSE, segment="NSE_FNO")
    win = cal.session_for(now.date())
    print(f"NSE F&O session today: {win.open_at:%H:%M}-{win.close_at:%H:%M} IST  "
          f"open={cal.is_open(now)}")

    try:
        values = credentials_from_env()
    except CredentialError as exc:
        print(f"\n{exc}", file=sys.stderr)
        return 2

    print(f"credentials: client_code={values['client_code'][:2]}***  "
          f"api_key=***{values['api_key'][-4:]}  totp now={totp(values['totp_secret'])}")

    if args.dry_run:
        print("\ndry run: credentials present, plan valid, no socket opened.")
        return 0

    print("\nlogging in...")
    try:
        session = login(values)
    except Exception as exc:
        print(f"login failed: {exc}", file=sys.stderr)
        return 1
    jwt = session.get("jwtToken")
    feed_token = session.get("feedToken") or values["feed_token"]
    if not jwt:
        print(f"login response had no jwtToken: {list(session)}", file=sys.stderr)
        return 1
    print("login ok, jwt received")

    # A chart per instrument, so cumulative delta resets per instrument per day.
    charts: dict = {}

    def chart_for(instrument):
        c = charts.get(instrument.symbol)
        if c is None:
            seg = "NSE_FNO" if instrument.exchange.value == "NSE" else "default"
            mkt = Market.NSE if instrument.exchange.value == "NSE" else Market.MCX
            c = charts[instrument.symbol] = SessionChart(
                instrument.grid(),
                MarketCalendar(mkt, segment=seg),
                bar_seconds=300,
            )
        return c

    frames_seen: list = []

    def on_trade(rt):
        chart_for(rt.instrument).add(rt.trade)

    from orderflow.feed import SmartStreamConnector

    def factory(index):
        t = WebSocketTransport(
            auth_token=jwt,
            api_key=values["api_key"],
            client_code=values["client_code"],
            feed_token=feed_token,
        )
        if args.record:
            # Wrap recv to tee every frame to the recorder.
            inner = t.recv

            def recv(timeout=None, _inner=inner):
                f = _inner(timeout)
                if f is not None:
                    frames_seen.append(f)
                return f

            t.recv = recv  # type: ignore[method-assign]
        return t

    conn = SmartStreamConnector(
        plan, mapper, factory,
        on_error=lambda e: print(f"  [feed error] {type(e).__name__}: {e}", file=sys.stderr),
    )

    print(f"\nstreaming for {args.minutes} minute(s)... (Ctrl-C to stop)")
    try:
        stats = conn.run(
            on_trade=on_trade,
            max_seconds=args.minutes * 60,
            reconnect=not args.no_reconnect,
        )
    except KeyboardInterrupt:
        print("\ninterrupted")
        stats = conn.stats

    print("\n--- summary ---")
    print(f"  frames={stats.frames}  ticks={stats.ticks}  trades={stats.trades}")
    print(f"  heartbeats skipped={stats.heartbeats}  protocol errors={stats.protocol_errors}")
    print(f"  unmapped tokens={stats.unmapped}  reconnects={stats.reconnects}")
    for symbol, c in charts.items():
        print(f"  {symbol}: bars={len(c.bars)} volume={c.session_volume:.0f} "
              f"cum_delta={c.cumulative_delta:+.0f} "
              f"off_session={c.off_session_trades}")

    reader_stats = conn.router.stats()
    print(f"  per-token readers: {reader_stats}")

    if args.record and frames_seen:
        path = record_frames(frames_seen, args.record)
        print(f"\nrecorded {len(frames_seen)} frames to {path}")

        # Settle the one question the docs cannot answer: is the best-five
        # bid/ask flag the right way round? Run the check on the capture just
        # taken, so an inverted book is caught now rather than in production.
        from orderflow.feed import VERDICT_INVERTED, report, scan_capture

        diag = scan_capture(path)
        print()
        print(report(diag))
        if diag.verdict() == VERDICT_INVERTED:
            print(
                "\nACTION REQUIRED: the book is crossed, so the best-five flag mapping\n"
                "is inverted. Delta built on it is upside down. Swap the bid/ask branch\n"
                "in orderflow/feed/protocol.py::_parse_best_five and re-run."
            )

    print("\nReminder: SmartStream has no aggressor flag. Sides are inferred "
          "(quote rule, then tick rule). Watch the UNKNOWN share before trusting delta.")

    return liveness_verdict(stats, charts)


def liveness_verdict(stats, charts) -> int:
    """Say plainly whether the run actually produced market data.

    A silent feed is the failure this whole runner is built to surface. The
    socket connects, the login succeeds, the subscription is accepted, and no
    data arrives. Every count is zero and nothing looks wrong. If this function
    does not say so, the run reads as a quiet market instead of a broken setup.

    Returns a process exit code: 0 when data arrived, 1 when it did not.
    """
    print("\n--- liveness ---")
    if stats.frames == 0:
        print("  NO FRAMES RECEIVED. The socket connected but the feed sent nothing.")
        print("  Most likely: the session is closed (NSE 09:15-15:40, MCX 09:00-23:30")
        print("  IST), or the instrument master is stale and the token is not live.")
        return 1
    if stats.ticks == 0:
        print(f"  {stats.frames} frames arrived but decoded to ZERO ticks.")
        print(f"  protocol errors={stats.protocol_errors}. A layout mismatch is the")
        print("  likely cause; re-check the offsets in orderflow/feed/protocol.py.")
        return 1
    if stats.trades == 0:
        print(f"  {stats.ticks} ticks decoded but produced no trades. Volume was flat,")
        print("  which is normal in a thin market but suspicious over minutes.")
    if stats.unmapped:
        print(f"  {stats.unmapped} packets carried an UNMAPPED token and were dropped.")
        print("  Refresh the instrument master: python3 fetch_instruments.py --download")
    if not any(len(c.bars) for c in charts.values()):
        print("  ticks arrived but no bars were built. Check the session window and")
        print("  the instrument's tick grid against the feed's price scale.")
        return 1
    total_bars = sum(len(c.bars) for c in charts.values())
    print(f"  OK: {stats.ticks} ticks, {stats.trades} trades, {total_bars} bars. "
          f"Feed is live.")
    if stats.protocol_errors:
        print(f"  note: {stats.protocol_errors} protocol errors were skipped.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
