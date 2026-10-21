"""Run the order-flow dashboard.

    python3 run_dashboard.py --port 12000

Serves the dashboard on 0.0.0.0 so it is reachable from outside the sandbox.
The feed is started from the page, not at boot: a socket opened at import time
would burn the login and start streaming before anyone was watching.

Live mode needs the same four environment variables as `live_feed.py`:

    ANGEL_API_KEY  ANGEL_CLIENT_CODE  ANGEL_PIN  ANGEL_TOTP_SECRET

Without them the page still loads, the instrument search works, and the replay
button works. Only "Start live" needs credentials.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from orderflow.instrument_master import build_live_token_map
from dashboard import build_app, default_watchlist


def credentials_from_env() -> dict:
    keys = {
        "api_key": "ANGEL_API_KEY",
        "client_code": "ANGEL_CLIENT_CODE",
        "pin": "ANGEL_PIN",
        "totp_secret": "ANGEL_TOTP_SECRET",
        "feed_token": "ANGEL_FEED_TOKEN",
    }
    out = {}
    for field, env in keys.items():
        val = os.environ.get(env, "")
        if val:
            out[field] = val
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Order-flow dashboard")
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=12000)
    ap.add_argument("--master", default="instruments.json")
    ap.add_argument("--no-default-watchlist", action="store_true")
    ap.add_argument("--record-dir", default="recordings",
                    help="where a live session is recorded; empty to disable")
    args = ap.parse_args(argv)

    if not Path(args.master).exists():
        print(f"instrument master missing at {args.master}; "
              f"run: python3 fetch_instruments.py", file=sys.stderr)
        return 1

    mapper, stats = build_live_token_map(args.master)
    print(f"master: {stats.loaded}/{stats.total_records} instruments")

    creds = credentials_from_env()
    app = build_app(
        mapper, args.master,
        credentials=creds or None,
        record_dir=args.record_dir or None,
    )
    if not args.no_default_watchlist:
        items = default_watchlist(mapper)
        app.state.watchlist.extend(items)
        for w in items:
            print(f"  watch {w.label:28} {w.market.value:5} "
                  f"token={w.token} tick={w.tick} exp={w.expiry}")
        if not items:
            print("  (no default instruments resolved from the master)")

    have = sorted(creds)
    print(f"credentials present: {have if have else 'NONE (replay still works)'}")

    import uvicorn

    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
