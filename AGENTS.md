# AGENTS.md

## Project

Pure-Python order-flow / footprint analytics engine. Stdlib only at runtime;
`pytest` for tests.

## Commands

```bash
python3 -m pytest tests -q    # test suite (67 tests, no mocks)
python3 demo.py               # end-to-end tour; writes footprint.html
```

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
