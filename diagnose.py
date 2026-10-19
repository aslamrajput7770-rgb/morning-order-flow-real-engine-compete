"""Diagnose a recorded SmartStream capture.

    python3 diagnose.py capture.txt

Reads a file written by `live_feed.py --record` and answers the one question
that cannot be answered from documentation: is the best-five bid/ask flag
mapping the right way round?

Exit codes: 0 = capture is usable, 1 = problem found (inverted book, etc).
"""

from __future__ import annotations

import sys

from orderflow.feed import VERDICT_INVERTED, VERDICT_OK, report, scan_capture


def main(argv=None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv or argv[0] in ("-h", "--help"):
        print(__doc__)
        return 0

    path = argv[0]
    try:
        diag = scan_capture(path)
    except FileNotFoundError:
        print(f"no such capture: {path}", file=sys.stderr)
        return 2

    print(report(diag))
    if diag.verdict() == VERDICT_INVERTED:
        print(
            "\nNext step: in orderflow/feed/protocol.py, _parse_best_five, swap the\n"
            "bid/ask branch, then re-run this tool and the test suite."
        )
        return 1
    return 0 if diag.verdict() == VERDICT_OK else 1


if __name__ == "__main__":
    raise SystemExit(main())
