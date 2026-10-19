"""Volume profile: POC, VAH/VAL, value area, naked (virgin) POC.

Profile is a *volume-at-price* histogram, independent of time. It is built from
the same footprint levels so the two views can never disagree — a classic bug in
tools that aggregate twice with different rounding.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from enum import Enum

from .core import PriceLevel, TickGrid, ValueArea
from .footprint import FootprintBar


class ProfileMode(str, Enum):
    SESSION = "session"  # one profile per trading session
    FIXED = "fixed"  # one profile per fixed number of bars
    ROLLING = "rolling"  # one profile over a sliding window of bars


@dataclass(slots=True)
class Profile:
    """A volume-at-price histogram with its value area."""

    start_ts: float
    end_ts: float
    grid: TickGrid
    levels: dict[int, PriceLevel] = field(default_factory=dict)
    value_area_pct: float = 70.0

    def add(self, level: PriceLevel) -> None:
        idx = self.grid.to_index(level.price)
        existing = self.levels.get(idx)
        if existing is None:
            self.levels[idx] = PriceLevel(
                price=level.price,
                bid_vol=level.bid_vol,
                ask_vol=level.ask_vol,
                prints=level.prints,
            )
        else:
            existing.bid_vol += level.bid_vol
            existing.ask_vol += level.ask_vol
            existing.prints += level.prints

    @property
    def sorted_levels(self) -> list[PriceLevel]:
        return [self.levels[i] for i in sorted(self.levels)]

    @property
    def total_volume(self) -> float:
        return sum(l.total_vol for l in self.levels.values())

    def value_area(self, pct: float | None = None) -> ValueArea:
        """Grow the value area outward from the POC, one level at a time.

        This is the standard volume-profile rule (Sierra Chart / ToS): starting
        at the POC, repeatedly take whichever single adjacent level has the
        larger volume, stopping once the target percentage of total volume is
        covered. Note this differs from the classic TPO value area, which
        compares *pairs* of levels; the single-level rule is the volume-profile
        convention and the two produce visibly different widths.
        """
        pct = self.value_area_pct if pct is None else pct
        if not 0 < pct <= 100:
            raise ValueError("value area pct must be in (0, 100]")
        if not self.levels:
            raise ValueError("cannot compute value area of an empty profile")

        idxs = sorted(self.levels)
        vols = {i: self.levels[i].total_vol for i in idxs}
        total = sum(vols.values())

        # Deterministic tie-break toward the lower price: iterating a dict
        # yields insertion order, which would otherwise make the POC depend on
        # the order trades arrived.
        poc_idx = min(idxs, key=lambda i: (-vols[i], self.levels[i].price))
        lo = hi = idxs.index(poc_idx)
        target = total * pct / 100.0
        covered = vols[poc_idx]

        while covered < target and (lo > 0 or hi < len(idxs) - 1):
            below = vols[idxs[lo - 1]] if lo > 0 else -1.0
            above = vols[idxs[hi + 1]] if hi < len(idxs) - 1 else -1.0
            if above >= below:
                hi += 1
                covered += above
            else:
                lo -= 1
                covered += below

        return ValueArea(
            poc=self.levels[poc_idx].price,
            vah=self.levels[idxs[hi]].price,
            val=self.levels[idxs[lo]].price,
            total_vol=total,
            poc_vol=vols[poc_idx],
            levels=tuple(self.levels[i] for i in idxs[lo : hi + 1]),
        )


class ProfileBuilder:
    """Builds profiles from bars, in session / fixed / rolling mode."""

    def __init__(
        self,
        grid: TickGrid,
        mode: ProfileMode = ProfileMode.SESSION,
        value_area_pct: float = 70.0,
        session_seconds: float = 24 * 3600.0,
        fixed_bars: int = 30,
        rolling_bars: int = 60,
    ) -> None:
        if mode is ProfileMode.FIXED and fixed_bars <= 0:
            raise ValueError("fixed_bars must be > 0")
        if mode is ProfileMode.ROLLING and rolling_bars <= 0:
            raise ValueError("rolling_bars must be > 0")
        self.grid = grid
        self.mode = mode
        self.value_area_pct = value_area_pct
        self.session_seconds = session_seconds
        self.fixed_bars = fixed_bars
        self.rolling_bars = rolling_bars

    def build(self, bars: Iterable[FootprintBar]) -> list[Profile]:
        bars = list(bars)
        if self.mode is ProfileMode.SESSION:
            return self._build_grouped(bars, lambda b: int(b.start_ts // self.session_seconds))
        if self.mode is ProfileMode.FIXED:
            return self._build_grouped(bars, lambda b: None, chunk=self.fixed_bars)
        return self._build_rolling(bars)

    # ------------------------------------------------------------------ impl
    def _new_profile(self, start: float, end: float) -> Profile:
        return Profile(
            start_ts=start,
            end_ts=end,
            grid=self.grid,
            value_area_pct=self.value_area_pct,
        )

    def _build_grouped(self, bars, key_fn, chunk: int | None = None) -> list[Profile]:
        out: list[Profile] = []
        current: Profile | None = None
        last_key = None
        count = 0
        for bar in bars:
            if chunk is None:
                key = key_fn(bar)
                start = bar.start_ts
            else:
                key = count // chunk
                start = bar.start_ts
            if current is None or key != last_key:
                current = self._new_profile(start, bar.end_ts)
                out.append(current)
                last_key = key
            current.end_ts = bar.end_ts
            for lvl in bar.levels.values():
                current.add(lvl)
            count += 1
        return out

    def _build_rolling(self, bars: list[FootprintBar]) -> list[Profile]:
        out: list[Profile] = []
        for i in range(len(bars)):
            window = bars[max(0, i - self.rolling_bars + 1) : i + 1]
            prof = self._new_profile(window[0].start_ts, window[-1].end_ts)
            for bar in window:
                for lvl in bar.levels.values():
                    prof.add(lvl)
            out.append(prof)
        return out


@dataclass(frozen=True, slots=True)
class NakedPoc:
    """A session POC that later price action never traded back through."""

    session_start: float
    session_end: float
    price: float
    vol: float
    first_touch_ts: float | None

    @property
    def is_virgin(self) -> bool:
        """Virgin = still untouched, i.e. an untested magnet above/below."""
        return self.first_touch_ts is None


def naked_pocs(profiles: list[Profile], bars: list[FootprintBar]) -> list[NakedPoc]:
    """Find session POCs that were not revisited by any later bar.

    These are the "naked"/"virgin" POCs traders use as price magnets. The check
    is done on the tick grid, not with float equality.
    """
    if not profiles:
        return []

    out: list[NakedPoc] = []
    for i, prof in enumerate(profiles):
        if not prof.levels:
            continue
        va = prof.value_area()
        poc_idx = prof.grid.to_index(va.poc)
        later = [b for b in bars if b.start_ts >= prof.end_ts]

        touch: float | None = None
        for bar in later:
            if poc_idx in bar.levels:
                touch = bar.start_ts
                break

        out.append(
            NakedPoc(
                session_start=prof.start_ts,
                session_end=prof.end_ts,
                price=va.poc,
                vol=va.poc_vol,
                first_touch_ts=touch,
            )
        )
    return out
