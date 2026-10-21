# Order Flow Analytics Engine

A pure-Python engine for order-flow / footprint analytics: the calculation layer
behind a Sierra Chart / Jigsaw / Bookmap style tool. It takes a stream of trades
and produces footprints, delta, volume profiles, imbalances, absorption and the
supporting indicators.

The calculation core and the Angel One SmartStream ingestion layer are pure
standard library. `pytest` is needed for tests; `websocket-client` is optional
and only for live streaming. Everything else runs offline with no network.

## Install

```bash
pip install pytest              # tests
pip install websocket-client    # optional, live streaming only
```

```bash
python3 -m pytest tests -q      # 198 tests, no mocks, no network
python3 demo_feed.py            # full ingestion path on simulated packets
```

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
  ticks.py       exact integer tick arithmetic (1e-8 base unit, HALF_UP)
  footprint.py   FootprintBar, FootprintChart  (bid/ask, delta, POC, imbalances)
  profile.py     Profile, ProfileBuilder, naked_pocs
  delta.py       cumulative delta, delta profile, divergence
  tape.py        Tape, big trades, counters
  dom.py         BookSnapshot, LiquidityMap, absorption, SuperDOM
  indicators.py  order-flow VWAP, pressure, market analyzer
  render.py      heatmap colouring, ANSI and HTML output
  instruments.py Instrument, Exchange, TokenMapper  (Indian contracts)
  session.py     IST calendars: NSE cash/F&O, MCX (seasonal close)
  session_chart.py  SessionChart: daily reset of delta and profile
tests/
  test_engine.py        69 tests, engine behaviour
  test_indian_market.py 58 tests, tick sizes, sessions, tokens, reset
  test_ingestion.py     57 tests, SmartStream protocol, reader, connector, e2e
demo.py          end-to-end tour on seeded synthetic tape
demo_indian.py   NSE + MCX sessions, reset, token map, tick-grid book lookup
```

## Indian market

The engine is instrument-agnostic; the Indian market specifics live in three
modules.

### Tick sizes are exact, not approximate

`orderflow/ticks.py` represents prices as integer counts of a 1e-8 base unit and
does the grid arithmetic with exact integers and explicit HALF_UP rounding.

| Instrument | Tick | Grid |
|---|---|---|
| NSE equity / index futures (NIFTY, BANKNIFTY) | 0.05 | `TickGrid(0.05)` |
| MCX gold / silver futures | 1.00 | `TickGrid(1.00)` |

Why the obvious implementation is wrong:

```python
round(100.025 * 20) / 20   # -> 100.0    banker's rounding, not HALF_UP
round(100.075 * 20) / 20   # -> 100.10   the snap is not even monotonic
```

A non-monotonic or drifting snap puts volume on the wrong price level, and the
diagonal imbalance comparison (`Bid[P]` vs `Ask[P + one tick]`) then compares the
wrong two levels. Every downstream signal inherits the error.

Off-grid prints are handled as a trade-off, not a toggle:

- a handful of strays (feed noise) are clamped and counted in `off_grid_count`
- a flood means the tick size is wrong for the instrument, so it raises
  `OffGridTick` rather than silently producing wrong numbers forever

### Sessions are the exchange's, not midnight's

`orderflow/session.py` defines the calendars in IST:

| Market | Open | Close |
|---|---|---|
| NSE cash (equity) | 09:15 | 15:30 |
| NSE F&O (futures) | 09:15 | **15:40** |
| MCX non-agri (bullion, metals, energy) | 09:00 | **23:30** US DST on / **23:55** off |

Two traps worth stating plainly:

1. F&O futures close at **15:40**, not the 15:30 cash close. Using one close
   time for both drops the last ten minutes of the futures session, which is the
   settlement window.
2. The MCX close is **seasonal**, driven by US daylight saving, not by Indian
   time. Hardcoding either 23:30 or 23:55 is wrong for half the year. The
   calendar computes US DST from the tz database.

Bars are numbered from the session open, not from midnight. A midnight-based bar
counter misaligns the first NSE bar by 111 bars at a 5-minute interval.

### The daily reset

`orderflow/session_chart.py` (`SessionChart`) resets cumulative delta and the
volume profile at each session open. Reset is keyed to the session boundary, not
to 00:00 IST, because MCX trades until 23:30/23:55 and a midnight reset would
wipe the live evening session's own data. Trades outside any session (pre-open,
post-close, holiday) are counted in `off_session_trades` and dropped rather than
folded into the next session.

### Token mapping

`orderflow/instruments.py` (`TokenMapper`) maps broker token ids to instruments,
keyed on `(namespace, token)`. Indian brokers stream ticks by numeric token, and
the same token number exists in separate namespaces for the NSE cash market, NSE
derivatives and MCX. A bare-token lookup collides across segments and delivers
Nifty futures ticks labelled as some equity, with every number wrong and nothing
raising. The API deliberately offers no bare-token lookup.

### Indian demo

```bash
python3 demo_indian.py
```

Runs two NSE Nifty sessions and two MCX gold sessions, shows the reset firing,
prints the seasonal MCX close, and demonstrates the float-equality trap in book
lookup.

## Angel One SmartStream ingestion

`orderflow/feed/` decodes Angel One SmartStream V2 binary packets into the
engine's `Trade` stream. The socket is injected, so the whole path runs offline.

```
bytes -> protocol.py    decode, no I/O
      -> tick_reader.py cumulative volume -> incremental prints, side inference
      -> connector.py   subscription planning, routing, reconnect, heartbeat
      -> session_chart  footprint, delta, profile, daily reset
```

```python
from orderflow import Market, MarketCalendar, SessionChart, NIFTY_FUT, build_indian_token_map
from orderflow.feed import (ExchangeType, SubscriptionMode, TickSpec, TokenNamespace,
                            build_plan, encode_tick, ReplayTransport,
                            SmartStreamConnector)

mapper = build_indian_token_map()
plan = build_plan([(TokenNamespace.NSE_FO, 26000)], mode=SubscriptionMode.SNAP_QUOTE)
chart = SessionChart(NIFTY_FUT.grid(), MarketCalendar(Market.NSE, segment="NSE_FNO"),
                     bar_seconds=300)

# Offline: real binary packets, no broker.
frames = [encode_tick(TickSpec(mode=SubscriptionMode.SNAP_QUOTE,
                               exchange_type=ExchangeType.NSE_FO, token="26000",
                               exchange_timestamp_ms=..., last_traded_price=24000.05,
                               volume_trade_for_the_day=1000,
                               bids=((24000.00, 150),), asks=((24000.05, 75),)))]
conn = SmartStreamConnector(plan, mapper, lambda i: ReplayTransport(frames))
conn.run(on_trade=lambda rt: chart.add(rt.trade))
```

For live data there is a ready runner. Credentials come from the environment,
never from code:

```bash
export ANGEL_API_KEY=... ANGEL_CLIENT_CODE=... ANGEL_PIN=... ANGEL_TOTP_SECRET=...
python3 live_feed.py --dry-run                                   # no socket opened
python3 live_feed.py --minutes 5 --instruments "NSE_FO:26000" --record capture.txt
```

Before going live, download the instrument master. Live subscriptions need real
token numbers:

```bash
python3 fetch_instruments.py              # downloads ~36 MB, then prints stats
python3 fetch_instruments.py --show NIFTY # nearest Nifty future, with its token
```

```
=== NIFTY ===
  FUT  ns=NSE_FO  token=48704    NIFTY27OCT26FUT   exp=2026-10-27  tick=0.05  lot=65
```

`--record` writes every raw frame to disk, runs a sanity check on the capture,
and reports whether the best-five bid/ask mapping is the right way round.
Re-check any recording offline:

```bash
python3 diagnose.py capture.txt
```

```
packets with a book  : 60
crossed books        : 0 (0.0%)
VERDICT: OK - book is sane, bid/ask mapping is correct
```

The check exists because an inverted book is invisible: the quote rule still
fires on every print and returns a confident, wrong side, so delta comes out
upside down while the footprint looks perfect. A real book always has
best_bid < best_ask; an inverted mapping reads as crossed on nearly every
packet. That settles it with no reference data. The recording also replays
offline through `FileTransport` as a permanent regression fixture.

Or swap the transport yourself (`pip install websocket-client`):

```python
from orderflow.feed import WebSocketTransport
transport = WebSocketTransport(auth_token=jwt, api_key=..., client_code=..., feed_token=...)
```

TOTP is implemented in stdlib and checked against the RFC 6238 vectors. The
runner re-sends subscriptions on reconnect, sends `ping` every 10s, distinguishes
an idle socket from an exhausted replay, and bounds the run by wall clock so a
live session cannot run away.

Four things this layer has to get right, and does:

1. **Prices are scaled integers.** NSE/BSE/MCX divide by 100, CDE by 10^7. The
   tick grid cannot catch a missing division (2400005 is a clean multiple of
   0.05), so `TickReader` raises `PriceScaleError` on an implausible price.
2. **Volume is cumulative.** The reader differences it into prints, treats a
   decrease as a session roll rather than a negative print, and emits nothing
   when the volume is unchanged (a quote update is not a trade).
3. **There is no aggressor flag.** Side is inferred by the quote rule (print vs
   best bid/ask), then the tick rule, and left `Side.UNKNOWN` when neither
   applies. That is the honest limit of this feed.
4. **Heartbeats, limits and reconnects.** Non-binary frames are skipped; 1000
   subscriptions per socket across at most 3 sockets are planned automatically;
   subscriptions are re-sent on every reconnect.

## Recording and replay

A footprint cannot be reconstructed from candles. Angel One's historical API
returns OHLCV, and the bid/ask split at each price only ever exists in the live
stream. So a real backtest needs the stream captured as it happens, and these
two tools do that.

### Capture

```bash
export ANGEL_API_KEY=... ANGEL_CLIENT_CODE=... ANGEL_PIN=... ANGEL_TOTP_SECRET=...

# Check the plan and credentials without opening a socket.
python3 record_ticks.py --out ./recordings --dry-run

# Run it. Safe to kill at any moment.
python3 record_ticks.py --out ./recordings
```

Started before the open, the recorder waits for the first session and logs in
only then. That matters on the desk: a JWT minted at 07:00 has aged past its
hour by the NSE open. MCX opens at 09:00 and NSE at 09:15, so the wait ends at
the earlier of the two and MCX is captured from its first print. Credentials
are checked before the wait, so a missing variable fails in a second rather
than after an hour of waiting. `--no-wait-for-open` subscribes immediately.

The recorder tees the raw socket stream, not per-instrument trades. A frame
does not say which instrument it belongs to until it is decoded, and the
decoder already exists on the replay side, so the shard stores exactly what the
socket delivered. Replay then runs that through the same router, tick reader
and session chart as a live session. Nothing is interpreted twice, and a
decoder fix retroactively applies to every recording ever taken.

Writes are crash-safe. A shard is `<root>/<key>/<YYYYMMDDTHH>.oft.gz`, written
as `.oft.gz.part` and fsynced, then renamed once the hour rolls. A `kill -9`
costs the last batch, not the file; a torn gzip tail still reads back to the
last complete record. Shards rotate hourly, compress about 20x on real tick
payloads, and a month of one instrument fits in tens of megabytes.

The process exits non-zero if it recorded nothing. An accepted subscription
outside its session streams silence, which is otherwise indistinguishable from
a quiet market.

### Before the open: rehearse it

The first live run should not be the first time the capture path has executed.
`--rehearse` drives the whole path on scripted frames, with no socket and no
credentials, at any hour:

```bash
python3 record_ticks.py --out /tmp/rehearse --rehearse 60 \
    --instruments "NSE_FO:48704:0.05"
python3 replay_backtest.py --rec /tmp/rehearse --key conn0 \
    --instruments "NSE_FO:48704:0.05"
```

Everything below the transport is production code, so this exercises the
subscription send, the recorder, the shard format and the replay reader. The
frames are anchored inside the session, not at the wall clock: a capture taken
outside its session produces no bars by design, so a rehearsal stamped "now" at
07:00 IST would prove the plumbing and nothing else. A rehearsal that reports
unmapped frames fails, because on the day that would look like a decoder fault.

What the rehearsal cannot cover is the broker's socket and the login. Those are
the only two things left to go wrong at 09:00.

### Backtest

```bash
python3 replay_backtest.py --rec ./recordings --key conn0 \
    --instruments "NSE_FO:48704:0.05" --out bars.json
```

It prints the footprint bars, delta, POC and session value area, and can write
the bars to JSON. Because it drives the production pipeline, a bar that comes
out wrong here is wrong live too.

`--rec` is the recording root; `--key` picks one connection when several were
recorded.

### Recording from the dashboard

`run_dashboard.py --record-dir ./recordings` records every live session started
from the page. The recorder is closed when the feed stops, so the `.part` shard
is promoted even if the session ends on an error. The replay button then takes
the same directory back, and replays it through the identical pipeline.

The page never holds a credential. Login runs on the server from the
environment, and "Start live" fails with the names of the missing variables
rather than opening a socket it has no token for.

`--instruments` is required and is not a convenience. The router keys on
`(namespace, token)` and drops an unregistered token without complaint, so a
missing registration reads as a decoder failure rather than a configuration
mistake. The tool says so when nothing is routed.


## Not included

- Broker authentication and the instrument master file. `WebSocketTransport`
  takes the tokens from the login response; `login()` in `transports.py` is a
  minimal REST helper. Load real token numbers from the broker's instrument
  master at startup; `build_indian_token_map()` is a working stand-in with
  placeholder ids.
- Order routing (SuperDOM models ladder state and fills; sending orders is the
  adapter's job)
- Historical data storage beyond the tick recorder above. There is no
  database: recordings are files, and `replay_backtest.py` reads them.
- The exchange holiday list. `MarketCalendar(holidays=...)` takes it from the
  caller; a guessed holiday is a silently missing day of profile data.
