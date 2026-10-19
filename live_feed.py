"""Live Angel One SmartStream runner.

Reads credentials from the environment, never from arguments or code:

    ANGEL_API_KEY       API key from the SmartAPI dashboard
    ANGEL_CLIENT_CODE   your Angel One client id, e.g. A123456
    ANGEL_PIN           your 4-digit MPIN
    ANGEL_TOTP_SECRET   the base32 secret behind your authenticator app
    ANGEL_FEED_TOKEN    optional; from a previous login response

Usage:

    # 1. Check the session is open and the plan is sane, touch nothing.
    python3 live_feed.py --dry-run

    # 2. Stream for 60 seconds, record every frame to disk, print a summary.
    python3 live_feed.py --minutes 1 --record capture.txt

    # 3. Subscribe to specific instruments.
    python3 live_feed.py --instruments "NSE_FO:26000,MCX_FO:42809" --minutes 5

The runner never prints a token, and never writes one to disk. The recording
contains market data only.
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


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Live Angel One SmartStream feed")
    ap.add_argument("--instruments", default="NSE_FO:26000,MCX_FO:42809",
                    help="comma-separated NAMESPACE:TOKEN")
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

    instruments = parse_instruments(args.instruments)
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

    mapper = build_indian_token_map()

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
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
