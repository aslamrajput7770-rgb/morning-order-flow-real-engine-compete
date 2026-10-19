"""Heatmap colouring and HTML rendering for footprint bars.

The renderer is deliberately separate from the aggregation engine: the engine
must stay dependency-free and deterministic, while presentation is where you
plug in a theme, a chart library or a web UI.

Two output paths are provided:
  * `heatmap_row` / `ansi_footprint` for terminal debugging
  * `footprint_html` for embedding in a browser dashboard
"""

from __future__ import annotations

from collections.abc import Sequence

from .core import Side
from .footprint import FootprintBar


def heatmap_color(
    value: float,
    vmin: float,
    vmax: float,
    mode: str = "volume",
) -> str:
    """Map a value to a CSS hex colour.

    `mode="volume"` is a blue scale (low -> high liquidity). `mode="delta"` is a
    red/green diverging scale centred on zero, which is the only correct choice
    for delta: a sequential scale would hide the sign, and sign is the signal.
    """
    if vmax == vmin:
        t = 0.5
    else:
        t = (value - vmin) / (vmax - vmin)
    t = min(1.0, max(0.0, t))

    if mode == "delta":
        # Diverging: -1 red, 0 dark, +1 green.
        if t >= 0.5:
            k = (t - 0.5) * 2
            return _mix((40, 40, 48), (46, 204, 113), k)
        k = (0.5 - t) * 2
        return _mix((40, 40, 48), (231, 76, 60), k)
    if mode == "volume":
        return _mix((24, 32, 48), (52, 152, 219), t)
    raise ValueError(f"unknown heatmap mode: {mode}")


def _mix(a: tuple[int, int, int], b: tuple[int, int, int], t: float) -> str:
    r = round(a[0] + (b[0] - a[0]) * t)
    g = round(a[1] + (b[1] - a[1]) * t)
    bl = round(a[2] + (b[2] - a[2]) * t)
    return f"#{r:02x}{g:02x}{bl:02x}"


def ansi_footprint(bar: FootprintBar, mode: str = "delta") -> str:
    """Terminal footprint with a 24-bit colour heatmap."""
    levels = bar.sorted_levels
    if not levels:
        return "(empty bar)"
    values = [l.total_vol if mode == "volume" else l.delta for l in levels]
    vmin, vmax = min(values), max(values)
    poc = bar.poc
    lines = []
    for lvl in reversed(levels):
        v = lvl.total_vol if mode == "volume" else lvl.delta
        hexcol = heatmap_color(v, vmin, vmax, mode)
        r, g, b = int(hexcol[1:3], 16), int(hexcol[3:5], 16), int(hexcol[5:7], 16)
        marker = "*" if lvl.price == poc else " "
        text = f"{lvl.price:>9.4f}{marker}{int(lvl.bid_vol):>7} x {int(lvl.ask_vol):<7}"
        lines.append(f"\033[48;2;{r};{g};{b}m{text}\033[0m")
    return "\n".join(lines)


def footprint_html(
    bars: Sequence[FootprintBar],
    mode: str = "delta",
    title: str = "Order Flow Footprint",
) -> str:
    """Self-contained HTML page: one heatmapped table per bar.

    No external assets, so it can be written to a file and opened directly.
    """
    if mode not in ("volume", "delta"):
        raise ValueError(f"unknown heatmap mode: {mode}")

    blocks = []
    for bar in bars:
        levels = bar.sorted_levels
        if not levels:
            continue
        values = [l.total_vol if mode == "volume" else l.delta for l in levels]
        vmin, vmax = min(values), max(values)
        poc = bar.poc
        rows = []
        for lvl in reversed(levels):
            v = lvl.total_vol if mode == "volume" else lvl.delta
            bg = heatmap_color(v, vmin, vmax, mode)
            poc_style = "font-weight:700;border-left:3px solid #f1c40f;" if lvl.price == poc else ""
            rows.append(
                f"<tr style='background:{bg};{poc_style}'>"
                f"<td>{lvl.price:.4f}</td>"
                f"<td class='num'>{lvl.bid_vol:g}</td>"
                f"<td class='num'>{lvl.ask_vol:g}</td>"
                f"<td class='num'>{lvl.delta:+g}</td>"
                f"<td class='num'>{lvl.delta_pct:+.1f}%</td></tr>"
            )
        blocks.append(
            f"<section><h3>bar {bar.start_ts:g}–{bar.end_ts:g} "
            f"&nbsp; vol {bar.total_volume:g} &nbsp; delta {bar.delta:+g}</h3>"
            "<table><thead><tr><th>price</th><th>bid</th><th>ask</th>"
            "<th>delta</th><th>delta%</th></tr></thead><tbody>"
            + "".join(rows)
            + "</tbody></table></section>"
        )

    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<title>{title}</title>
<style>
 body{{background:#0e1117;color:#e6e6e6;font:13px/1.4 ui-monospace,Menlo,Consolas,monospace;margin:24px}}
 h1{{font-size:16px;letter-spacing:.08em;text-transform:uppercase;color:#8fa6c4}}
 h3{{font-size:12px;color:#9fb3c8;font-weight:600;margin:18px 0 6px}}
 table{{border-collapse:collapse;min-width:420px}}
 th,td{{padding:3px 10px;border:1px solid #1d2430}}
 th{{background:#141a24;color:#9fb3c8;text-align:left;font-weight:600}}
 .num{{text-align:right}}
 section{{display:inline-block;vertical-align:top;margin-right:28px}}
</style></head><body>
<h1>{title}</h1>
<p>colour scale: {mode}. Yellow left border marks the bar POC.</p>
{"".join(blocks)}
</body></html>"""


def write_html(
    bars: Sequence[FootprintBar],
    path: str = "footprint.html",
    mode: str = "delta",
) -> str:
    html = footprint_html(bars, mode=mode)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(html)
    return path
