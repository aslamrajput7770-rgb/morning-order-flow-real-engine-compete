"""Deterministic demo of the order-flow engine on synthetic tape.

No external data feed required. Prints a footprint, imbalances, profile value
area, delta divergence, absorption, VWAP bands and a tape summary.
"""

from __future__ import annotations

import random

from orderflow import (
    BookSnapshot,
    FootprintChart,
    ProfileBuilder,
    ProfileMode,
    Side,
    Tape,
    TickGrid,
    Trade,
    ansi_footprint,
    big_trade_summary,
    cumulative_delta_series,
    detect_absorption,
    detect_divergence,
    market_analyzer,
    naked_pocs,
    order_flow_vwap,
    write_html,
)


def synth_session(seed: int = 7, bars: int = 40, bar_seconds: float = 60.0):
    """Downtrend into absorption, then a squeeze back up. Seeded => repeatable."""
    rng = random.Random(seed)
    trades: list[Trade] = []
    # Keep a free-running float price and only snap a *copy* onto the tick grid.
    # Snapping the running price itself would trap the walk on one grid point.
    price = 100.00
    t = 0.0
    for b in range(bars):
        drift = -0.02 if b < bars // 2 else 0.015
        for _ in range(rng.randint(30, 60)):
            t += bar_seconds / 45.0
            price += drift + rng.gauss(0, 0.03)
            px = round(price / 0.25) * 0.25
            size = rng.choice([1, 1, 2, 2, 3, 5, 8, 12, 25, 60])
            side = Side.BUY if rng.random() < (0.62 if b >= bars // 2 else 0.42) else Side.SELL
            trades.append(Trade(ts=t, price=px, size=size, side=side))
    return trades


def main() -> None:
    grid = TickGrid(0.25)
    trades = synth_session()

    chart = FootprintChart(grid, bar_seconds=60.0)
    chart.add_many(trades)
    bars = chart.bars
    print(f"=== {len(trades)} trades -> {len(bars)} bars ===")

    mid = bars[len(bars) // 2]
    print("\n--- Footprint (bid x ask), mid session bar ---")
    print(mid.render("bidask"))
    print("\n--- same bar, delta mode ---")
    print(mid.render("delta"))

    print("\n--- Imbalances (3:1 diagonal) on last bar ---")
    last = bars[-1]
    for imb in last.imbalances(ratio=3.0, min_volume=5.0, stack_min=2):
        flag = "STACKED" if imb.stacked else "       "
        print(f"  {flag} {imb.side.value:3} @ {imb.price:8.4f}  ratio={imb.ratio:5.2f}")

    print("\n--- Session volume profile ---")
    builder = ProfileBuilder(grid, ProfileMode.SESSION, value_area_pct=70.0, session_seconds=1e9)
    prof = builder.build(bars)[0]
    va = prof.value_area()
    print(f"  POC={va.poc:g}  VAH={va.vah:g}  VAL={va.val:g}  width={va.width:g}")
    print(f"  total_vol={va.total_vol:g}  poc_vol={va.poc_vol:g}")

    print("\n--- Naked / virgin POCs ---")
    for np_ in naked_pocs(builder.build(bars), bars):
        print(f"  {np_.price:g}  vol={np_.vol:g}  virgin={np_.is_virgin}")

    print("\n--- Cumulative delta (last 6 candles) ---")
    for c in cumulative_delta_series(bars)[-6:]:
        print(f"  ts={c.ts:8.0f}  O{c.open:9.1f} H{c.high:9.1f} L{c.low:9.1f} C{c.close:9.1f}")

    print("\n--- Delta divergence ---")
    divs = detect_divergence(bars, left=3, right=3)
    print("  none" if not divs else "")
    for d in divs:
        print("  " + d.detail)

    print("\n--- Absorption (>= 400 lots that failed to move price) ---")
    for a in detect_absorption(trades, grid, min_volume=400.0, max_range_ticks=2.0):
        print("  " + a.detail)

    print("\n--- Order-flow VWAP (last 3, 1sd/2sd bands) ---")
    for v in order_flow_vwap(bars, bands=(1.0, 2.0))[-3:]:
        bands = "  ".join(f"{b.mult:g}sd[{b.lower:9.4f},{b.upper:9.4f}]" for b in v.bands)
        print(f"  ts={v.ts:8.0f}  vwap={v.vwap:9.4f}  sd={v.sd:6.3f}  {bands}")

    print("\n--- Tape summary ---")
    tape = Tape(min_size=5.0)
    tape.add_many(trades)
    s = big_trade_summary(tape.prints, min_size=25.0)
    print(f"  prints shown={len(tape.prints)}  ticks={tape.tick_count}  vol={tape.total_volume:g}")
    print(f"  delta={tape.delta:g}  buy_pressure={tape.buy_pressure:.1f}%")
    print(f"  big trades={s['count']}  net={s['net']:g}  buy={s['buy_volume']:g}  sell={s['sell_volume']:g}")

    print("\n--- Book snapshot sanity ---")
    snap = BookSnapshot(
        ts=bars[-1].end_ts,
        bids=((99.75, 320), (99.50, 180), (99.25, 90)),
        asks=((100.00, 210), (100.25, 140), (100.50, 75)),
    )
    print(f"  best_bid={snap.best_bid} best_ask={snap.best_ask} spread={snap.spread} mid={snap.mid}")

    print("\n--- Heatmap footprint (delta scale, ANSI) ---")
    print(ansi_footprint(bars[len(bars) // 2], mode="delta"))

    print("\n--- Writing HTML heatmap dashboard -> footprint.html ---")
    path = write_html(bars, "footprint.html", mode="delta")
    print(f"  wrote {path} ({len(open(path).read())} bytes, {len(bars)} bars)")

    print("\n--- Market analyzer scan ---")
    inst = {"ES": bars, "NQ": bars[: len(bars) // 2]}
    for r in market_analyzer(inst):
        print(
            f"  {r.symbol:4} cum_delta={r.cumulative_delta:9.1f} "
            f"imb={r.imbalance_count} stacked={r.stacked_count} bias={r.bias}"
        )


if __name__ == "__main__":
    main()
