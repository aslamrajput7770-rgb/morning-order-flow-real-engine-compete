"""Indian market demo: NSE Nifty futures and MCX gold, two sessions each.

Shows the three things that are easy to get wrong:
  1. tick size 0.05 for NSE, 1.00 for MCX, with no float drift
  2. NSE F&O session running to 15:40 (not the 15:30 cash close)
  3. cumulative delta and the volume profile resetting at each session open

Run: python3 demo_indian.py
"""

from __future__ import annotations

import random
from datetime import date, datetime, timedelta

from orderflow import (
    IST,
    MCX_GOLD_FUT,
    NIFTY_FUT,
    BookSnapshot,
    Market,
    MarketCalendar,
    SessionChart,
    Side,
    TickGrid,
    TokenNamespace,
    Trade,
    build_indian_token_map,
    detect_absorption,
    us_daylight_saving,
)


def at(d: date, hh: int, mm: int) -> datetime:
    return datetime(d.year, d.month, d.day, hh, mm, tzinfo=IST)


def synth_session(
    start: datetime,
    end: datetime,
    base_price: float,
    tick: float,
    seed: int,
    drift: float,
) -> list[Trade]:
    """Generate trades between two IST datetimes on an exact tick grid.

    `drift` is the per-trade price step. It is intentionally small: a realistic
    intraday drift is a fraction of a percent over the whole session, not
    hundreds of points. An unrealistic drift produces a value area thousands of
    points wide and makes the demo look broken.
    """
    rng = random.Random(seed)
    trades: list[Trade] = []
    free = base_price  # free-running float; only a copy is snapped
    t = start
    step = timedelta(seconds=1)
    while t < end:
        t += step * rng.randint(1, 3)
        if t >= end:
            break
        free += drift + rng.gauss(0, tick * 2.0)
        price = round(free / tick) * tick
        price = round(price, 2)
        size = rng.choice([1, 2, 2, 5, 10, 15, 25, 50, 75, 150])
        side = Side.BUY if rng.random() < 0.54 else Side.SELL
        trades.append(Trade(ts=t.timestamp(), price=price, size=size, side=side))
    return trades


def run(name: str, inst, market: Market, days: list[date], base: float, drift: float) -> None:
    grid = inst.grid()
    cal = MarketCalendar(market, segment="NSE_FNO" if market is Market.NSE else "default")
    chart = SessionChart(grid, cal, bar_seconds=300, on_session_open=lambda s: print(
        f"    >> SESSION OPEN {s.session_open:%Y-%m-%d %H:%M} IST  (state reset to zero)"
    ))

    print(f"\n{'=' * 72}")
    print(f"{name}   tick={inst.tick_size:g}   lot={inst.lot_size}")
    print(f"{'=' * 72}")

    for i, d in enumerate(days):
        win = cal.session_for(d)
        if win is None:
            print(f"  {d}: market closed")
            continue
        print(f"  {d}: {win}   duration={win.duration_seconds() / 3600:.2f}h")
        trades = synth_session(win.open_at, win.close_at, base + i * drift * 200, inst.tick_size,
                               seed=100 + i, drift=drift)
        for tr in trades:
            chart.add(tr)

        prof = chart.session_profile()
        va = prof.value_area() if prof else None
        print(f"    trades={len(trades):5d}  vol={chart.session_volume:10.0f}  "
              f"cum_delta={chart.cumulative_delta:+9.0f}")
        if va:
            print(f"    POC={va.poc:g}  VAH={va.vah:g}  VAL={va.val:g}  "
                  f"width={va.width:g}")
        if chart.bars:
            first = datetime.fromtimestamp(chart.bars[0].start_ts, tz=IST)
            last = datetime.fromtimestamp(chart.bars[-1].start_ts, tz=IST)
            print(f"    bars={len(chart.bars)}  first={first:%H:%M}  last={last:%H:%M} IST")

    print(f"  off-session prints dropped: {chart.off_session_trades}")
    print(f"  archived sessions: {len(chart.closed_sessions)}")
    for s in chart.closed_sessions:
        print(f"    {s.session_open:%Y-%m-%d %H:%M}  cum_delta={s.cumulative_delta:+.0f}  "
              f"vol={s.total_volume:.0f}")

    print(f"  grid health: off_grid_count={grid.off_grid_count} (0 is clean)")


def main() -> None:
    wed = date(2026, 10, 14)
    thu = date(2026, 10, 15)
    nov = date(2026, 11, 9)  # US DST off
    jul = date(2026, 7, 15)  # US DST on

    print("US daylight saving check (this drives the MCX close time):")
    print(f"  {jul}  -> DST {us_daylight_saving(jul)}")
    print(f"  {nov}  -> DST {us_daylight_saving(nov)}")

    run("NSE NIFTY FUT (2 sessions)", NIFTY_FUT, Market.NSE, [wed, thu], 24000.0, 0.002)

    print("\nMCX session close is seasonal:")
    for d in (jul, nov):
        win = MarketCalendar(Market.MCX).session_for(d)
        print(f"  {d}: close {win.close_at:%H:%M} IST")

    run("MCX GOLD FUT (2 sessions)", MCX_GOLD_FUT, Market.MCX, [jul, nov], 71000.0, 0.02)

    print(f"\n{'=' * 72}")
    print("TOKEN MAPPER (keyed on namespace + token, never token alone)")
    print(f"{'=' * 72}")
    mapper = build_indian_token_map()
    for ns, tok in [(TokenNamespace.NSE_FO, 26000), (TokenNamespace.NSE_EQ, 2885),
                    (TokenNamespace.MCX_FO, 42809)]:
        inst = mapper.by_token(ns, tok)
        print(f"  {ns.value:8} token {tok:6d} -> {inst.full_name:22} tick={inst.tick_size:g}")

    print("\nBOOK LOOKUP ON THE TICK GRID (float equality would return 0):")
    # A price that arrives from a float calculation, as real feeds often do.
    feed_price = 100.05000000000001
    snap = BookSnapshot(ts=0.0, bids=((feed_price, 75),), asks=((100.10, 150),))
    g = TickGrid(0.05)
    print(f"  feed price  = {feed_price!r}")
    print(f"  == 100.05 ? {feed_price == 100.05}   <-- the trap")
    print(f"  bid_size(100.05) without grid = {snap.bid_size(100.05)}  <-- reads as no liquidity")
    print(f"  bid_size(100.05) with grid    = {snap.bid_size(100.05, grid=g)}")

    print("\nABSORPTION ON THE 0.05 GRID (price range + time bounded):")
    rng = random.Random(3)
    trades = []
    for i in range(400):
        trades.append(Trade(float(i), 24000.05, rng.choice([10, 25, 75]), Side.SELL))
    for a in detect_absorption(trades, g, min_volume=5000.0, max_range_ticks=2.0, max_seconds=600.0):
        print(f"  {a.detail}")


if __name__ == "__main__":
    main()
