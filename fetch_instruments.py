"""Download the Angel One instrument master and inspect it.

    python3 fetch_instruments.py                  # download to instruments.json
    python3 fetch_instruments.py --show NIFTY     # nearest NIFTY future
    python3 fetch_instruments.py --show BANKNIFTY --limit 3
    python3 fetch_instruments.py --show GOLD

Live subscriptions need real token numbers. The placeholders in
`build_indian_token_map()` are not real: subscribing to them returns no data and
reports no error. This script is how you get the real ones.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from orderflow.instrument_master import (
    DEFAULT_MASTER_URL,
    fetch_master,
    find_instrument,
    load_master,
    upcoming_futures,
)

DEFAULT_PATH = "instruments.json"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Angel One instrument master")
    ap.add_argument("--path", default=DEFAULT_PATH, help="master file location")
    ap.add_argument("--download", action="store_true", help="force a fresh download")
    ap.add_argument("--show", metavar="SYMBOL", help="look up a symbol or underlying")
    ap.add_argument("--limit", type=int, default=1, help="futures to list with --show")
    args = ap.parse_args(argv)

    path = Path(args.path)

    if args.download or not path.exists():
        print(f"downloading {DEFAULT_MASTER_URL}")
        try:
            fetch_master(path)
        except Exception as exc:
            print(f"download failed: {type(exc).__name__}: {exc}", file=sys.stderr)
            return 1
        print(f"wrote {path} ({path.stat().st_size / 1e6:.1f} MB)")

    mapper, stats = load_master(path)
    print(f"loaded: {stats}")
    print(f"mapper: {len(mapper)} (namespace, token) pairs")

    if not args.show:
        print("\nuse --show NIFTY to list the nearest future")
        return 0

    want = args.show.upper()
    print(f"\n=== {want} ===")
    futs = upcoming_futures(mapper, want, limit=args.limit)
    if futs:
        for ns, tok, inst in futs:
            print(f"  FUT  ns={ns.value:<7} token={tok:<8} {inst.symbol:<24} "
                  f"exp={inst.expiry}  tick={inst.tick_size}  lot={inst.lot_size}")
    else:
        print("  no unexpired future found for that underlying")

    inst = find_instrument(mapper, want)
    if inst is not None:
        print(f"  exact symbol match: {inst.full_name}  tick={inst.tick_size}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
