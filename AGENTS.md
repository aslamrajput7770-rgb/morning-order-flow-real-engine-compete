# AGENTS.md

## Project

Pure-Python order-flow / footprint analytics engine. Stdlib only at runtime;
`pytest` for tests.

## Commands

```bash
python3 -m pytest tests -q    # 374 tests, no mocks, no network
python3 demo.py               # engine tour; writes footprint.html
python3 demo_indian.py        # NSE + MCX sessions, reset, token map
python3 demo_feed.py          # SmartStream ingestion, offline, simulated bytes

# Always-on autonomous paper trading (the desk's main entry point):
python3 run_trader.py                   # feed + strategy + paper broker, runs forever
python3 run_trader.py --replay recordings/  # same decision loop over a capture
python3 run_dashboard.py --port 12000   # dashboard; feed + paper trading start at boot

# Live (needs credentials in the environment, never in code or chat):
export ANGEL_API_KEY=... ANGEL_CLIENT_CODE=... ANGEL_PIN=... ANGEL_TOTP_SECRET=...
python3 live_feed.py --dry-run          # validate plan + session, no socket
python3 live_feed.py --minutes 1 --record capture.txt
python3 fetch_instruments.py             # download the 36 MB instrument master
python3 fetch_instruments.py --show NIFTY
python3 diagnose.py capture.txt          # verdict on a recording
```

## Wire protocol: three silent-failure bugs, already fixed

Verified against the official client, `SmartApi/smartWebSocketV2.py` in
`angel-one/smartapi-python`. Do not "fix" these back:

- `Authorization` is the BARE JWT. The official SDK sets
  `"Authorization": self.auth_token`; the string "Bearer" appears nowhere in
  its WebSocket module. The REST login does use "Bearer" -- do not copy that
  habit into the socket handshake.
- The subscribe frame is TEXT. The SDK sends `json.dumps(request_data)`, a str.
  websocket-client keys the opcode off the argument type, so passing bytes
  silently produces OPCODE_BINARY, which the server does not read as a
  subscription. `Transport.send(data, binary=False)` defaults to TEXT.
- The heartbeat is a protocol PING frame (`ws.ping()`), not a text "ping". The
  SDK gets it from `run_forever(ping_interval=10)`. A text "ping" draws no
  PONG and leaves a dead socket looking alive. websocket-client answers the
  server's PONG inside `recv_data_frame`, so no manual PONG handling.

- In the best-five block at offset 147, **flag 0 is an ASK and flag 1 is a
  BID**. This is inverted from the naming and it was settled on a live NIFTY
  packet: the flag==0 levels ran 22904.8 -> 22907.0 and the flag==1 levels ran
  22893.1 -> 22892.0, and a bid is always below an ask. The official SDK looks
  like it swaps the two lists, but the swap is deliberate and it ends up
  correct. Reading the flag the intuitive way crosses the book, and then the
  quote rule inverts every inferred trade side.

`tests/test_protocol_conformance.py` and `tests/test_wire_path.py` pin the
first three. `tests/test_live_packet.py` pins the flag convention against a
real captured frame, because the encoder and decoder agreed with each other
while both were wrong, so no round-trip test could catch it. If a live run
connects and delivers nothing, or if delta looks inverted, check these first.

## Going live: real tokens are mandatory

- `build_indian_token_map()` is an OFFLINE placeholder map. Its ids are not
  real. A live subscription to them is accepted, streams nothing, and reports
  no error. `NSE_FO:26001`, `MCX_FO:42809` and `MCX_FO:43210` do not exist in
  the master; `NSE_FO:26000` is the NSE index, not a future.
- Real tokens come from `instruments.json` (36 MB, gitignored, refreshed
  daily). `live_feed.py` refuses an unknown token and exits rather than
  subscribing into silence.
- Match underlyings on the master's `name` field, never on the symbol prefix.
  "NIFTY" is a prefix of "NIFTYNXT50" and "NIFTYFPI", so a prefix search for the
  Nifty future returns a Nifty Next 50 contract. Same trap inside MCX tick
  lookup: "SILVER" is a prefix of "SILVERMIC", so match longest prefix first.
- Ignore the master's `tick_size` field. It is not the traded tick (10.0 for
  RELIANCE, 10.0 for a Nifty future, real tick 0.05). Use exchange rules.
- A run that connects but receives nothing must exit non-zero and say so.
  `liveness_verdict` exists for exactly that; do not soften it into a warning.

## Ingestion layer (orderflow/feed/)

Angel One SmartStream V2, decoded from raw bytes. No socket in the core path:

```
bytes -> protocol.py    decode, no I/O
      -> tick_reader.py cumulative volume -> incremental prints, side inference
      -> connector.py   subscription planning, routing, reconnect, heartbeat
      -> session_chart  footprint, delta, profile, daily reset
```

`ReplayTransport` and `FileTransport` replace the socket, so the whole path is
offline-testable. `WebSocketTransport` is the only place that touches a socket
and it imports `websocket-client` lazily, so importing `orderflow.feed` never
requires it.

## SmartStream invariants (do not "simplify" these)

- Prices are **scaled integers**. NSE/BSE/MCX use 100, CDE uses 10^7. Always
  divide by `raw.scale`. The tick grid does **not** catch a missing division:
  2400005 is an exact multiple of 0.05, so it looks perfectly aligned.
  `TickReader` raises `PriceScaleError` on an implausible price instead.
- `volume_trade_for_the_day` is **cumulative**. The footprint needs the
  increment (`volume - previous_volume`). Feeding the cumulative value puts the
  whole day into every bar.
- A cumulative volume **below** the previous one is a session roll or feed
  restart, not a negative print. Take the new value as the increment.
- Unchanged volume means **no trade**: a quote update. Emit nothing, or the
  footprint fills with zero prints.
- The feed has **no aggressor flag and no trade id**. Side is inferred: quote
  rule (print vs best bid/ask) first, tick rule fallback, `Side.UNKNOWN` when
  neither applies. Never invent a side; `UNKNOWN` splits 50/50 downstream.
- **LTP mode carries no volume** and cannot build a footprint.
  `require_volume_capable_mode` guards this; the failure mode is a silently
  empty chart.
- Limit is **1000 token subscriptions per socket**, 3 sockets per client.
  `build_plan` splits and dedupes. Subscribe in one mode per token: two modes
  count as two subscriptions.
- Re-send subscriptions on every reconnect. A reconnected socket with no
  subscriptions is silent, which looks exactly like a dead market.
- Heartbeats are non-binary (the literal `ping`). Skip them; never parse them.
- The official SDK maps its parsed buy list to a key named `best_5_sell_data`
  and vice versa. `_parse_best_five` maps bid -> bid. Do not settle this by
  reading hex: `diagnose.py` decides it from the capture, because a real book
  always satisfies best_bid < best_ask and an inverted mapping reads as crossed
  on ~every packet. `live_feed.py --record` runs the same check automatically.
  An inverted book does NOT look broken downstream: the quote rule still fires
  on every print and returns a confident, wrong side. It is the quietest way to
  invert delta.
- A price-scale check must use a ceiling, not a modulo. 24000.05 * 100 = 2400005,
  and 2400005 % 100 == 5. The raw value only divides evenly by the TICK SIZE,
  never necessarily by the scale, so `raw % scale != 0` fires on every good
  packet and trains the reader to ignore the diagnostic.
- `Transport.is_live` decides what `recv() is None` means. On a replay it means
  exhausted and ends the loop; on a live socket it means idle and the loop keeps
  waiting. Treating them the same kills a live feed on the first quiet minute.
  Reconnect only applies to live transports; a replay is never retried.
- Credentials come from the environment (`ANGEL_*`). Never in code, never in a
  chat message, never committed. `live_feed.py` prints only masked values.
- TOTP is stdlib (`orderflow/feed/totp.py`), verified against the RFC 6238
  vectors in the tests. No `pyotp` dependency.
- `websocket-client` is optional and imported lazily. Importing
  `orderflow.feed` must never require it.

## Indian market invariants (do not "simplify" these)

- Tick sizes: NSE equity and index futures 0.05, MCX gold/silver 1.00. Prices go
  through `orderflow.ticks.TickGrid`, which is exact integer arithmetic with
  HALF_UP rounding. Never `round(price / tick)`.
- NSE F&O futures close at **15:40**; cash closes at 15:30. These are different
  calendars.
- The MCX non-agri close is **seasonal**: 23:30 IST while US DST is in effect,
  23:55 IST otherwise. Never hardcode it.
- Cumulative delta and the volume profile reset at the **session open**, never at
  midnight IST. MCX trades past 23:00.
- Bars are numbered from the session open, not from midnight.
- Broker tokens are keyed `(namespace, token)`. The same token number exists in
  the NSE cash, NSE derivatives and MCX namespaces.
- Holidays come from the caller (`MarketCalendar(holidays=...)`). Do not guess.

## Conventions

- Prices are snapped onto an integer tick grid (`TickGrid`). Never use raw floats
  as dict keys — that silently merges adjacent price levels.
- All aggregation is deterministic. Any tie-break must be explicit and
  order-independent (see `FootprintBar.poc`, `Profile.value_area`).
- Keep the engine (`core`/`footprint`/`profile`/`delta`/`tape`/`dom`/
  `indicators`) free of presentation and I/O. Rendering lives in `render.py`.
- Tests assert exact numbers wherever the contract is exact (bid/ask split,
  delta, POC, value area, imbalance ratio). Loose tolerances only where the
  definition itself is a convention.

## Gotchas learned the hard way

- Diagonal imbalance must compare against the *diagonally adjacent* level, and
  stacked-run detection must compare price indices, not list positions.
- Absorption needs a time bound as well as a price bound, or a whole session of
  chop collapses into one fake event.
- The pressure window is over the last N prints, not N per side.
- Delta heatmap must use a diverging scale; a sequential scale hides the sign.

## Trading layer (strategy -> paper -> auto-trader)

Four modules sit on top of the analytics. Each is pure and testable without a
socket; only `supervisor.py` touches the clock and the network.

- `orderflow/strategy.py` — `SignalEngine.evaluate(bars, grid, symbol, trades)`
  composes delta, footprint imbalances, absorption, VWAP and divergence into a
  scored entry with stated reasons. It refuses to confirm on delta when the
  unknown-aggressor fraction exceeds `max_unknown_frac` (0.40): an unknown side
  is split 50/50 by the engine, so a delta built mostly from unknowns is a
  guess, not evidence. Stop and target are ATR-sized (`stop_atr_mult=1.5`,
  `target_atr_mult=3.0`), never a fixed number of ticks.
- `orderflow/paper.py` — `PaperBroker` with an Indian cost model (brokerage,
  STT, exchange, GST, stamp) plus slippage. `register(symbol, lot_size,
  tick_size)` per instrument.
- `orderflow/autotrade.py` — `AutoTrader` decides on bar completion, not per
  tick, and manages stop/target. `AutoTraderConfig` is a plain mutable config;
  it must stay mutable so callers can override one knob (a frozen config made
  `--stop-atr-mult` silently do nothing).
- `orderflow/supervisor.py` — `AlwaysOnFeed` survives session ends, token
  expiry and socket drops. Its sleep is injected, so a fake clock can test the
  wait loop without a real `Event.wait` (a real wait hung the tests).

Bugs that each cost a debugging session, recorded so they are not reintroduced:

- **Limit fills must respect the limit.** A limit BUY at 99.0 was filling at
  99.05 because the crossing test compared the wrong way. A limit BUY fills
  only when the market price is `<= order.price`; a limit SELL only when it is
  `>= order.price`.
- **Entry at the current print, not the prior bar close.** Taking `signal.price`
  (the last bar's close) let the entry use a price that had already traded away,
  which is look-ahead. The entry price is the print that triggered the signal.
- **`all_positions()` returns open positions only.** Including flat
  instruments made the dashboard show phantom rows with `stop=None`.
- **Paper state is per run.** The trader is rebuilt on every run start, like the
  charts. A trader reused across runs stacked a second replay's prints onto the
  first run's book.

## Dashboard

`run_dashboard.py` serves the dashboard; `dashboard.py` holds the app and
`dashboard_ui.py` holds the page. The page is one self-contained HTML string
with no build step and no CDN, so it works on a box with no outbound network.

Design rules that are easy to break by accident:

- **The feed is always on.** `run_dashboard.py` starts the live feed at boot via
  the app's lifespan and leaves it up; the only control on the page pauses it.
  There is no "Start live" button, because a feed meant to run unattended does
  not have a start. Autostart is on the lifespan, not at import, so building the
  app in a test never opens a socket.
- **Paper trading is on by default** and trades on its own from the live tape.
  There is no per-trade button by design. `POST /api/paper {"enabled": false}`
  turns it off; the header checkbox drives that. The page only reports what the
  engine did — positions, PnL, closed trades — it never decides.
- **The trader is rebuilt on every run start** (`_launch` -> `enable_paper`),
  and the watchlist is re-registered then too, because the page can edit it and
  the default watchlist is seeded after `build_app`.
- **Live counters are mirrored from the connector inside `on_trade`.** The
  connector's stats are only returned when `run` ends, and an always-on feed
  never ends, so reading them after `run` would leave `frames`/`ticks` frozen
  at zero forever.
- The feed runs on one background thread and owns the socket. The WebSocket
  handler only reads the latest snapshot. If the browser were allowed to drive
  the socket, a stalled tab would stall the exchange connection.
- Snapshots are built on demand from the session chart, not pushed per tick.
  A busy session delivers thousands of ticks a second; that must not become
  thousands of JSON documents a second.
- `WatchItem.key()` is `"<NAMESPACE>_<token>"`. It is used as a URL path
  segment, so it must stay free of `:` and `/`.
- Charts are keyed by that same key. `Instrument` has no `token` field; the
  token lives in the mapper, so `_chart_for` resolves the watch item by symbol.
- MCX tick sizes are matched longest-name-first. `SILVER` is a prefix of
  `SILVERMIC` and `GOLD` of `GOLDPETAL`, and each pair has different ticks, so
  matching the short name first is a silent misconfiguration.
- An off-session capture produces no bars. That is correct, not a bug: the
  session chart drops prints outside the session so they cannot corrupt the
  opening profile. A capture taken before 09:15 IST will therefore replay with
  zero bars while still reporting trades.

Replay is the live-backtest path: `FileTransport` feeds recorded frames through
the same decoder, router, tick reader and session chart as a live socket. No
synthetic trades are injected below the transport, so a replay exercises the
real pipeline.

## Recording (the only source of real order flow)

A footprint cannot be rebuilt from candles: the bid/ask split at each price
exists only in the live stream. Angel One's historical API returns OHLCV, so
the data has to be captured live. `record_ticks.py` does the capture;
`replay_backtest.py` reads it back.

`orderflow/recorder.py` — `ShardRecorder(dir, key, fsync_every)` appends gzipped
binary records to an hourly shard. `MultiRecorder` keeps one shard set per
connection. Module functions: `read_shard`, `iter_shards`, `finalize_parts`,
`shard_summary`; constants `TYPE_BINARY`, `TYPE_TEXT`, `MAGIC`.

Invariants that took a bug each to learn:

- The recorder tees the **raw socket stream**, not per-instrument trades. A
  frame does not name its instrument until decoded, and the decoder already
  exists on the replay side. So a shard stores what the socket delivered, and
  replay routes it through the same code path as a live session. A decoder fix
  then applies retroactively to every recording.
- Writes go to `<hour>.oft.gz.part` and are renamed only after the hour rolls.
  `finalize_parts` promotes a `.part` left by a crash. A `kill -9` costs the
  last batch, not the file.
- A torn gzip tail raises **`EOFError`, not `OSError`**. `read_shard` catches
  both and falls back to a tolerant zlib inflate, so a killed process costs the
  last record rather than the whole shard. Catching only `OSError` was a real
  bug: it made every crash-safety test pass and every real crash lose data.
- `ShardRecorder.flush()` must never be called while the write lock is held.
  `threading.Lock` is not reentrant; nesting it deadlocks the feed thread.
- `RecordingTransport.recv()` swallows recorder write failures. A full disk
  must not kill a live session.
- `ShardTransport` reports unreadable shards in `torn_shards` and keeps going.
  One bad file must not abort a month-long replay.
- The router keys on `(namespace, token)`. `TickRouter.route` returns three
  things and callers must not conflate them: a `RoutedTrade`, `None` for "known
  token, no new trade" (quote-only update or the baseline tick), and the
  `UNMAPPED` sentinel for a token the mapper does not know. `None` is normal;
  only `UNMAPPED` counts toward `stats.unmapped`. Returning `None` for both
  made every baseline and every quote update look like a config fault, which
  `record_ticks.py --rehearse` then reported as a decoder failure.
- `TickReader` treats the **first tick of a stream as a baseline**, not a
  trade: its `volume_trade_for_the_day` is the whole day's volume so far.
  Booking it as one print put 1,061,320 contracts into a single bar on a real
  09:46 attach against 11,505 for every genuine print after it, so the bar's
  delta, POC and imbalance were decided by one synthetic trade. Skipping costs
  at most the first print's size at a true session open; not skipping destroys
  the bar on any mid-session attach. `state.has_baseline` records that the
  baseline was taken.
- **A small backward move in the day's cumulative volume is a stale snapshot,
  not a session reset.** The feed interleaves tokens and the exchange re-sends
  snapshots, so `volume_trade_for_the_day` can drop by a few hundred contracts
  and then recover. The old code treated every backward move as a rollover and
  booked the entire cumulative total as one print: on a real capture that put
  **2,152,670 contracts** into a single NIFTY bar whose median print is 22, and
  those phantom prints then drove bogus SELL entries. Only a drop that loses
  more than half the accumulated volume is a real rollover; anything smaller
  emits nothing and keeps the high-water mark, so the next fresh tick measures
  from the right base. 32 of these fired in a 25-minute capture.
- `record_ticks.py` exits non-zero when it recorded nothing. An accepted
  subscription outside its session streams silence, indistinguishable from a
  quiet market.
- The dashboard replay accepts a directory of shards, a single `.oft.gz` shard,
  or a legacy line-format file, chosen by what the path is. Anything ending
  `.oft.gz` must go to `ShardTransport`: `FileTransport` utf-8 decodes its
  input, so feeding it a gzip shard reports a decode error rather than
  replaying.
- **The connector routes only the tokens in the subscription plan.** A live
  server sends nothing else, but a replay shard holds *every* token that was
  recorded on the connection, so without a filter a one-instrument replay routes
  the whole shard. A NIFTY-only replay of a real capture routed all five desk
  symbols and took a CRUDEOIL trade: the "plan" was meaningless and every
  strategy conclusion drawn from a replay was unsound. `SmartStreamConnector`
  now builds a `(namespace, token)` set from `plan.all()` and drops anything
  outside it before the reader sees it, so a foreign token cannot even seed its
  volume-delta state. The drop is counted as `not_subscribed`, kept distinct
  from `unmapped` (a token absent from the master, which is a real config
  fault): one means "a real instrument we did not ask for", the other means "the
  master is stale".
- Credentials come from the environment only: `ANGEL_API_KEY`,
  `ANGEL_CLIENT_CODE`, `ANGEL_PIN`, `ANGEL_TOTP_SECRET`. Never from arguments,
  never written to disk, never committed.

- `record_ticks.py` waits for the session open by default and logs in only
  after the wait, because a JWT minted before the open has aged past its hour
  by the time the session starts. Credentials are read *before* the wait so a
  missing variable fails in a second instead of two hours later.
- `record_ticks.py --rehearse SECONDS` drives the whole capture path on
  scripted frames, with no socket and no credentials. `RehearsalTransport` is
  `is_live = True` so the connector takes the live control flow, then drops out
  of live mode once the script is spent so the run ends instead of idling to
  its budget. Frames are anchored inside the session; a rehearsal stamped "now"
  outside the session produces no bars and proves nothing.

`orderflow/watchlist.py` owns the desk watchlist and the MCX tick table;
`dashboard.py` delegates to it. There is one tick rule, not two.

Recordings are large and machine-specific: `recordings/` and `*.oft.gz` are
gitignored.
