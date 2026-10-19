# AGENTS.md

## Project

Pure-Python order-flow / footprint analytics engine. Stdlib only at runtime;
`pytest` for tests.

## Commands

```bash
python3 -m pytest tests -q    # 127 tests, no mocks
python3 demo.py               # engine tour; writes footprint.html
python3 demo_indian.py        # NSE + MCX sessions, reset, token map
```

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
