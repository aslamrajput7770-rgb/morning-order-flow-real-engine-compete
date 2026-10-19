# Order Flow Analytics Engine

A pure-Python engine for order-flow / footprint analytics: the calculation layer
behind a Sierra Chart / Jigsaw / Bookmap style tool. It takes a stream of trades
and produces footprints, delta, volume profiles, imbalances, absorption and the
supporting indicators.

No data feed, no UI framework, no dependencies beyond the standard library
(`pytest` only for tests).

## Why this shape

The hard and valuable part of an order-flow tool is the **analytics**, not the
data feed or the chart widgets. This package is that analytics layer:

| Layer | Where it lives | Notes |
|---|---|---|
| Data feed | not included | plug in CME/NSE/broker tick + L2 yourself |
| **Analytics** | `orderflow/` | this package |
| Presentation | `orderflow/render.py` | ANSI terminal + self-contained HTML |

## Quick start

```python
from orderflow import TickGrid, Trade, Side, FootprintChart

grid = TickGrid(0.25)
chart = FootprintChart(grid, bar_seconds=60)
chart.add(Trade(ts=0.0, price=100.0, size=5, side=Side.BUY))
chart.add(Trade(ts=1.0, price=99.75, size=20, side=Side.SELL))

print(chart.current.render("bidask"))
print(chart.current.delta, chart.current.poc)
```

Run the full tour on synthetic tape (writes `footprint.html`):

```bash
python3 demo.py
```

Run the tests:

```bash
python3 -m pytest tests -q
```

## Coverage

### 1. Footprint (core)
- Bid x Ask volume at every price level, per bar
- Render modes: `bidask`, `volume`, `delta`, `deltapct`
- Bar POC, high/low delta, total / buy / sell volume
- Heatmap colouring (sequential for volume, diverging for delta)

### 2. Imbalance
- Diagonal imbalance (the 3:1 rule), measured against the *diagonally adjacent*
  level, not the same level
- Stacked-imbalance detection over contiguous runs
- Delta flip per bar

### 3. Delta
- Bar delta and cumulative-delta candles (OHLC)
- Delta profile and delta value area
- Pivot-based price/delta divergence (bullish and bearish)

### 4. Volume profile
- Session / fixed / rolling profiles
- POC, VAH, VAL, value area (single-level expansion, the volume-profile rule)
- Naked / virgin POC detection

### 5. Tape
- Time & sales with a size filter, buy/sell colouring by side
- Big-trade detection and buy/sell summary
- Tick and volume counters, bar timer

### 6. DOM / depth
- Book snapshots with spread and mid
- Liquidity map and largest bid/ask walls
- Absorption detection (bounded by price range *and* time)
- SuperDOM ladder state and fill handling

### 7. Indicators
- Order-flow VWAP with symmetric volume-weighted standard-deviation bands,
  optionally anchored at a timestamp
- Buy/sell pressure gauge (windowed oscillator)
- Market analyzer scan across instruments

## Correctness notes

These are the decisions that separate a working engine from a plausible one:

- **Prices live on an integer tick grid.** Floats are never used as dictionary
  keys, so `0.1 + 0.2` cannot fragment a price level.
- **POC ties are broken deterministically** (closest to bar mid, then lower
  price). Without the final price tie-break the POC depends on trade arrival
  order, and two identical bars can disagree.
- **Stacked imbalance compares price indices**, not list positions. Comparing a
  list position against a price index silently disables stacking entirely.
- **Absorption is bounded by time as well as price.** Price alone collapses an
  hour of two-tick chop into one fake absorption event.
- **Pressure window is over the last N prints**, not N per side; per-side windows
  let stale flow pollute the current reading.
- **Delta heatmap uses a diverging scale**, because the sign is the signal.
- **Unknown-side prints split 50/50** rather than being guessed onto one side.

## Layout

```
orderflow/
  core.py        Trade, Side, TickGrid, PriceLevel, ValueArea, Imbalance, Divergence
  footprint.py   FootprintBar, FootprintChart  (bid/ask, delta, POC, imbalances)
  profile.py     Profile, ProfileBuilder, naked_pocs
  delta.py       cumulative delta, delta profile, divergence
  tape.py        Tape, big trades, counters
  dom.py         BookSnapshot, LiquidityMap, absorption, SuperDOM
  indicators.py  order-flow VWAP, pressure, market analyzer
  render.py      heatmap colouring, ANSI and HTML output
tests/
  test_engine.py 67 tests, real code paths, no mocks
demo.py          end-to-end tour on seeded synthetic tape
```

## Not included

- Live data adapters (broker/exchange specific)
- Order routing (SuperDOM models ladder state and fills; sending orders is the
  adapter's job)
- Historical data storage
