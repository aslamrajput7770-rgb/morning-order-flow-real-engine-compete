# AGENTS.md

## Project

Pure-Python order-flow / footprint analytics engine. Stdlib only at runtime;
`pytest` for tests.

## Commands

```bash
python3 -m pytest tests -q    # 184 tests, no mocks, no network
python3 demo.py               # engine tour; writes footprint.html
python3 demo_indian.py        # NSE + MCX sessions, reset, token map
python3 demo_feed.py          # SmartStream ingestion, offline, simulated bytes

# Live (needs credentials in the environment, never in code or chat):
export ANGEL_API_KEY=... ANGEL_CLIENT_CODE=... ANGEL_PIN=... ANGEL_TOTP_SECRET=...
python3 live_feed.py --dry-run          # validate plan + session, no socket
python3 live_feed.py --minutes 1 --record capture.txt
```

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
  and vice versa. `_parse_best_five` maps bid -> bid; validate against a live
  packet dump before trusting the direction in production.
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
