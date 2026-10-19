"""End-to-end ingestion demo: simulated SmartStream bytes -> footprint.

No broker, no credentials, no network. This builds real SmartStream V2 binary
packets for an NSE session and an MCX session, pushes them through the actual
ingestion path, and prints what came out:

    encode_tick -> ReplayTransport -> decode_packet -> TickReader -> TickRouter
                -> SessionChart -> FootprintBar / delta / profile

Run: python3 demo_feed.py
"""

from __future__ import annotations

import random
from datetime import date, datetime, timedelta

from orderflow import (
    IST,
    MCX_GOLD_FUT,
    NIFTY_FUT,
    Market,
    MarketCalendar,
    Side,
    SessionChart,
    build_indian_token_map,
)
from orderflow.feed import (
    ExchangeType,
    ReplayTransport,
    SmartStreamConnector,
    SubscriptionMode,
    TickSpec,
    TokenNamespace,
    build_plan,
    decode_packet,
    encode_tick,
)

TICK = 0.05


def nse_frames(day: date, token: str = "26000", seed: int = 11):
    """Simulate one NSE F&O session as SmartStream packets.

    Volume is cumulative, exactly as the real feed sends it, so the reader has
    to difference it. The book is set so the print lands on the intended side.
    """
    rng = random.Random(seed)
    cal = MarketCalendar(Market.NSE, segment="NSE_FNO")
    win = cal.session_for(day)
    t = win.open_at
    free = 24000.0
    cum = 0
    frames = []
    while t < win.close_at:
        t += timedelta(seconds=rng.randint(1, 4))
        if t >= win.close_at:
            break
        free += rng.gauss(0, 0.35)
        price = round(round(free / TICK) * TICK, 2)
        size = rng.choice([1, 2, 5, 10, 25, 75, 150])
        cum += size
        side = Side.BUY if rng.random() < 0.55 else Side.SELL
        if side is Side.BUY:
            bid, ask = round(price - TICK, 2), price
        else:
            bid, ask = price, round(price + TICK, 2)
        frames.append(
            encode_tick(
                TickSpec(
                    mode=SubscriptionMode.SNAP_QUOTE,
                    exchange_type=ExchangeType.NSE_FO,
                    token=token,
                    exchange_timestamp_ms=int(t.timestamp() * 1000),
                    last_traded_price=price,
                    volume_trade_for_the_day=cum,
                    open_interest=12_000_000 + cum,
                    bids=((bid, rng.randint(50, 500)),),
                    asks=((ask, rng.randint(50, 500)),),
                )
            )
        )
    return frames


def mcx_frames(day: date, token: str = "42809", seed: int = 23):
    rng = random.Random(seed)
    cal = MarketCalendar(Market.MCX)
    win = cal.session_for(day)
    t = win.open_at
    free = 71000.0
    cum = 0
    frames = []
    while t < win.close_at:
        t += timedelta(seconds=rng.randint(2, 8))
        if t >= win.close_at:
            break
        free += rng.gauss(0, 3.0)
        price = round(free)
        size = rng.choice([1, 5, 10, 100])
        cum += size
        side = Side.BUY if rng.random() < 0.5 else Side.SELL
        bid, ask = (price - 1, price) if side is Side.BUY else (price, price + 1)
        frames.append(
            encode_tick(
                TickSpec(
                    mode=SubscriptionMode.SNAP_QUOTE,
                    exchange_type=ExchangeType.MCX_FO,
                    token=token,
                    exchange_timestamp_ms=int(t.timestamp() * 1000),
                    last_traded_price=float(price),
                    volume_trade_for_the_day=cum,
                    open_interest=500_000 + cum,
                    bids=((float(bid), 5),),
                    asks=((float(ask), 5),),
                )
            )
        )
    return frames


def run_market(name, instrument, market, namespace, token, days, frames_fn):
    mapper = build_indian_token_map()
    plan = build_plan([(namespace, int(token))], mode=SubscriptionMode.SNAP_QUOTE)
    cal = MarketCalendar(market, segment="NSE_FNO" if market is Market.NSE else "default")
    chart = SessionChart(instrument.grid(), cal, bar_seconds=300)

    print(f"\n{'=' * 74}")
    print(f"{name}   tick={instrument.tick_size:g}   mode=SNAP_QUOTE   token={token}")
    print(f"{'=' * 74}")

    total_frames = 0
    for day in days:
        frames = frames_fn(day, seed=day.toordinal() % 100000)
        total_frames += len(frames)
        # A heartbeat before the data, to prove it is skipped rather than parsed.
        conn = SmartStreamConnector(
            plan, mapper, lambda i, f=frames: ReplayTransport(["ping", *f])
        )
        stats = conn.run(on_trade=lambda rt: chart.add(rt.trade))
        prof = chart.session_profile()
        va = prof.value_area() if prof else None
        win = cal.session_for(day)
        print(f"  {day}  session {win.open_at:%H:%M}-{win.close_at:%H:%M} IST")
        print(f"    frames={len(frames):6d}  trades={stats.trades:6d}  "
              f"heartbeats skipped={stats.heartbeats}  protocol errors={stats.protocol_errors}")
        print(f"    session volume={chart.session_volume:12.0f}   "
              f"cumulative delta={chart.cumulative_delta:+10.0f}")
        if va:
            print(f"    POC={va.poc:g}  VAH={va.vah:g}  VAL={va.val:g}")
        if chart.bars:
            b = chart.bars[len(chart.bars) // 2]
            imbs = b.imbalances(ratio=3.0)
            stacked = [i for i in imbs if i.stacked]
            print(f"    bars={len(chart.bars)}   mid-session bar: "
                  f"vol={b.total_volume:.0f} delta={b.delta:+.0f} "
                  f"buy={b.buy_volume:.0f} sell={b.sell_volume:.0f}")
            print(f"    bar POC={b.poc:g}  imbalances={len(imbs)}  "
                  f"stacked={len(stacked)}")

    print(f"  off-session prints dropped: {chart.off_session_trades}")
    print(f"  archived sessions: {len(chart.closed_sessions)}")
    for s in chart.closed_sessions:
        print(f"    {s.session_open:%Y-%m-%d %H:%M}  cum_delta={s.cumulative_delta:+.0f}")
    print(f"  total frames processed: {total_frames}")


def main() -> None:
    print("SMARTSTREAM INGESTION DEMO (offline, no broker, no network)")

    print("\n--- wire format proof: 24000.05 survives the round trip ---")
    raw = encode_tick(
        TickSpec(
            mode=SubscriptionMode.SNAP_QUOTE,
            exchange_type=ExchangeType.NSE_FO,
            token="26000",
            exchange_timestamp_ms=1784000000000,
            last_traded_price=24000.05,
            volume_trade_for_the_day=1234,
            bids=((24000.00, 150),),
            asks=((24000.05, 75),),
        )
    )
    t = decode_packet(raw)
    print(f"  packet        : {len(raw)} bytes")
    print(f"  raw ltp field : {t.last_traded_price}  (scaled integer)")
    print(f"  scale         : {t.scale}")
    print(f"  decoded price : {t.last_traded_price / t.scale}")
    print(f"  best bid/ask  : {t.bids[0].price_raw / t.scale} / {t.asks[0].price_raw / t.scale}")

    run_market(
        "NSE NIFTY FUT  (2 sessions, cumulative delta resets each day)",
        NIFTY_FUT, Market.NSE, TokenNamespace.NSE_FO, "26000",
        [date(2026, 10, 14), date(2026, 10, 15)], nse_frames,
    )

    run_market(
        "MCX GOLD FUT  (US-DST-off session runs to 23:55 IST)",
        MCX_GOLD_FUT, Market.MCX, TokenNamespace.MCX_FO, "42809",
        [date(2026, 11, 9), date(2026, 11, 10)], mcx_frames,
    )

    print(f"\n{'=' * 74}")
    print("WHAT THIS FEED CANNOT GIVE YOU")
    print(f"{'=' * 74}")
    print("  SmartStream is not order-by-order. It has no aggressor flag and no")
    print("  trade id. The side of each print is inferred: quote rule first (print")
    print("  vs best bid/ask), tick rule as fallback, UNKNOWN when neither applies.")
    print("  A footprint built from it is a good approximation, not a reconstruction")
    print("  of the order book. Genuine order-by-order needs a different source.")


if __name__ == "__main__":
    main()
