"""Capture diagnostics: decide whether a recording is trustworthy.

Why this exists
---------------
The one thing that cannot be settled from documentation is the bid/ask flag
mapping in the best-five block. The official `smartapi-python` SDK assigns its
parsed buy list to a key named `best_5_sell_data` and its sell list to
`best_5_buy_data`, which is either an upstream naming bug or a real signal that
the flag is the other way round. Guessing wrong inverts every delta, and the
footprint still looks perfectly reasonable. That is the worst kind of bug.

So do not guess. A real book always satisfies `best_bid < best_ask`. If the flag
mapping is inverted, bids and asks swap and the book reads as crossed on almost
every packet. That single invariant settles it from a five-minute capture, with
no reference data and no eyeballing hex.

    python3 diagnose.py capture.txt

The report ends in a verdict:
  * `OK`        the book is sane; bid/ask are the right way round
  * `INVERTED`  the book is crossed almost always; swap the flag mapping
  * `NO_BOOK`   the capture has no best-five data (LTP or QUOTE mode)
  * `INSUFFICIENT` too few packets with a book to decide
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

from .protocol import (
    ExchangeType,
    PriceScaleError,
    RawTick,
    SubscriptionMode,
    decode_packet,
    price_scale,
)
from .tick_reader import SideSource, TickReader

#: Below this fraction of crossed books the mapping is treated as correct.
#: A real book crosses briefly under fast markets, but nowhere near half the time.
CROSSED_OK_FRACTION = 0.05
#: Above this fraction the mapping is treated as inverted.
CROSSED_INVERTED_FRACTION = 0.50
#: Fewest book-bearing packets needed before a verdict is issued.
MIN_BOOK_TICKS = 20

#: Generous ceiling on a scaled price. NIFTY is ~24k, SENSEX ~80k, MCX gold
#: ~70k, the priciest equities a few lakh. 10^6 clears all of them, so a price
#: above it is the signature of a missing scale division: 24000.05 arrives as
#: 2400005 and lands above the ceiling. Note this cannot be done with a modulo:
#: the raw value need not divide evenly by the scale, only by the tick size.
PLAUSIBLE_MAX_PRICE = 1_000_000.0

VERDICT_OK = "OK"
VERDICT_INVERTED = "INVERTED"
VERDICT_NO_BOOK = "NO_BOOK"
VERDICT_INSUFFICIENT = "INSUFFICIENT"


@dataclass
class Diagnostics:
    """Counters gathered while scanning a capture."""

    packets: int = 0
    decode_errors: int = 0
    tokens: Counter = field(default_factory=Counter)
    modes: Counter = field(default_factory=Counter)
    exchanges: Counter = field(default_factory=Counter)

    # book sanity
    ticks_with_book: int = 0
    crossed_books: int = 0
    locked_books: int = 0
    bid_unsorted: int = 0
    ask_unsorted: int = 0
    empty_book: int = 0

    # prices
    price_above_ceiling: int = 0
    price_implausible: int = 0
    min_price_raw: int | None = None
    max_price_raw: int | None = None

    # volume
    volume_regressions: int = 0
    volume_unchanged: int = 0

    # side inference
    sides: Counter = field(default_factory=Counter)
    trades: int = 0

    error_samples: list = field(default_factory=list)

    # ------------------------------------------------------------------ verdict
    @property
    def crossed_fraction(self) -> float:
        if not self.ticks_with_book:
            return 0.0
        return self.crossed_books / self.ticks_with_book

    @property
    def unknown_side_fraction(self) -> float:
        if not self.trades:
            return 0.0
        return self.sides.get(SideSource.UNKNOWN, 0) / self.trades

    def verdict(self) -> str:
        if not self.ticks_with_book:
            return VERDICT_NO_BOOK if self.packets else VERDICT_INSUFFICIENT
        if self.ticks_with_book < MIN_BOOK_TICKS:
            return VERDICT_INSUFFICIENT
        frac = self.crossed_fraction
        if frac >= CROSSED_INVERTED_FRACTION:
            return VERDICT_INVERTED
        if frac <= CROSSED_OK_FRACTION:
            return VERDICT_OK
        return VERDICT_INSUFFICIENT

    def lines(self) -> list[str]:
        out = [
            f"packets decoded      : {self.packets}",
            f"decode errors        : {self.decode_errors}",
        ]
        for tok, n in self.tokens.most_common(10):
            out.append(f"  token {tok:<12s} {n} packets")
        for m, n in self.modes.most_common():
            out.append(f"  mode {m.name:<12s} {n}")
        for e, n in self.exchanges.most_common():
            out.append(f"  exch {e.name:<12s} {n}")

        out.append("")
        out.append(f"packets with a book  : {self.ticks_with_book}")
        out.append(f"crossed books        : {self.crossed_books} "
                   f"({self.crossed_fraction:.1%})")
        out.append(f"locked books         : {self.locked_books}")
        out.append(f"empty books          : {self.empty_book}")
        out.append(f"bids out of order    : {self.bid_unsorted}")
        out.append(f"asks out of order    : {self.ask_unsorted}")

        out.append("")
        out.append(f"price above ceiling  : {self.price_above_ceiling}")
        out.append(f"price implausible    : {self.price_implausible}")
        if self.min_price_raw is not None:
            out.append(f"price range          : {self.min_price_raw} .. {self.max_price_raw} "
                       f"(raw, scaled)")

        out.append("")
        out.append(f"volume regressions   : {self.volume_regressions}")
        out.append(f"volume unchanged     : {self.volume_unchanged}")

        out.append("")
        out.append(f"prints emitted       : {self.trades}")
        for s, n in self.sides.most_common():
            frac = n / self.trades if self.trades else 0.0
            out.append(f"  side {s.value:<12s} {n} ({frac:.1%})")
        return out


def scan_ticks(ticks, scale_by_token=None) -> Diagnostics:
    """Run every check over decoded `RawTick`s."""
    diag = Diagnostics()
    readers: dict[str, TickReader] = {}
    last_volume: dict[str, int] = {}
    prev_price_raw: dict[str, int] = {}

    for raw in ticks:
        if raw is None:
            diag.decode_errors += 1
            continue
        diag.packets += 1
        diag.tokens[raw.token] += 1
        diag.modes[raw.mode] += 1
        diag.exchanges[raw.exchange_type] += 1

        scale = scale_by_token.get(raw.token) if scale_by_token else None
        if scale is None:
            try:
                scale = price_scale(raw.exchange_type)
            except PriceScaleError:
                scale = 100

        # ---- book sanity
        has_book = bool(raw.bids or raw.asks)
        if has_book:
            diag.ticks_with_book += 1
            if not raw.bids or not raw.asks:
                diag.empty_book += 1
            else:
                bb = raw.bids[0].price_raw
                ba = raw.asks[0].price_raw
                if bb > ba:
                    diag.crossed_books += 1
                elif bb == ba:
                    diag.locked_books += 1
            diag.bid_unsorted += int(not _descending(raw.bids))
            diag.ask_unsorted += int(not _ascending(raw.asks))

        # ---- price sanity
        if raw.last_traded_price is not None:
            p = raw.last_traded_price
            diag.min_price_raw = p if diag.min_price_raw is None else min(diag.min_price_raw, p)
            diag.max_price_raw = p if diag.max_price_raw is None else max(diag.max_price_raw, p)
            price = p / scale if scale else p
            # A missing scale division multiplies every price by 100, so it shows
            # up as a price above anything a real instrument trades at. Do NOT
            # test `p % scale`: 24000.05 * 100 = 2400005, and 2400005 % 100 == 5.
            # The raw value only has to divide evenly by the tick size.
            if price > PLAUSIBLE_MAX_PRICE:
                diag.price_above_ceiling += 1
            if not (0.01 <= price <= PLAUSIBLE_MAX_PRICE):
                diag.price_implausible += 1

        # ---- volume monotonicity
        if raw.volume_trade_for_the_day is not None:
            prev = last_volume.get(raw.token)
            if prev is not None:
                if raw.volume_trade_for_the_day < prev:
                    diag.volume_regressions += 1
                elif raw.volume_trade_for_the_day == prev:
                    diag.volume_unchanged += 1
            last_volume[raw.token] = raw.volume_trade_for_the_day

        # ---- side inference, through the real reader
        reader = readers.get(raw.token)
        if reader is None:
            reader = readers[raw.token] = TickReader(raw.token, scale=scale or 100)
        try:
            trade = reader.read(raw)
        except PriceScaleError:
            diag.price_implausible += 1
            continue
        if trade is not None:
            diag.trades += 1
            diag.sides[trade.source] += 1
        prev_price_raw[raw.token] = raw.last_traded_price

    return diag


def scan_frames(frames) -> Diagnostics:
    """Decode raw frames and diagnose. Non-binary frames are skipped."""
    ticks = []
    errors = []
    for f in frames:
        if isinstance(f, str):
            continue  # heartbeat
        try:
            ticks.append(decode_packet(f))
        except Exception as exc:
            errors.append(exc)
            ticks.append(None)
    diag = scan_ticks(ticks)
    diag.decode_errors += len(errors)
    diag.error_samples.extend(errors[:5])
    return diag


def scan_capture(path: str | Path) -> Diagnostics:
    """Read a `--record` file and diagnose it."""
    frames: list[bytes | str] = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("bin:"):
            frames.append(bytes.fromhex(line[4:]))
        elif line.startswith("txt:"):
            frames.append(line[4:])
    return scan_frames(frames)


def _descending(levels) -> bool:
    prices = [l.price_raw for l in levels]
    return all(a >= b for a, b in zip(prices, prices[1:]))


def _ascending(levels) -> bool:
    prices = [l.price_raw for l in levels]
    return all(a <= b for a, b in zip(prices, prices[1:]))


def report(diag: Diagnostics) -> str:
    verdict = diag.verdict()
    head = {
        VERDICT_OK: "OK - book is sane, bid/ask mapping is correct",
        VERDICT_INVERTED: "INVERTED - book is crossed; swap the best-five flag mapping",
        VERDICT_NO_BOOK: "NO_BOOK - capture carries no best-five data",
        VERDICT_INSUFFICIENT: "INSUFFICIENT - too few packets with a book to decide",
    }[verdict]
    body = "\n".join(diag.lines())
    tail = ""
    if verdict == VERDICT_OK and diag.unknown_side_fraction > 0.4:
        tail = (
            f"\n\nWARNING: {diag.unknown_side_fraction:.0%} of prints have an unknown side.\n"
            "Delta built on that share is not trustworthy. Check that the book is\n"
            "present on most packets, or fall back to a trade-level feed."
        )
    if diag.error_samples:
        tail += "\n\nfirst decode errors:\n" + "\n".join(
            f"  {type(e).__name__}: {e}" for e in diag.error_samples
        )
    return f"--- capture diagnostics ---\n{body}\n\nVERDICT: {head}{tail}"


def main(argv=None) -> int:
    import argparse

    ap = argparse.ArgumentParser(description="Diagnose a recorded SmartStream capture")
    ap.add_argument("path", help="file written by live_feed.py --record")
    args = ap.parse_args(argv)

    diag = scan_capture(args.path)
    print(report(diag))
    return 0 if diag.verdict() in (VERDICT_OK, VERDICT_NO_BOOK) else 1


if __name__ == "__main__":
    raise SystemExit(main())
