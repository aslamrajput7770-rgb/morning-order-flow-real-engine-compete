"""Indian market sessions: NSE and MCX trading calendars and boundaries.

Why this exists
---------------
A 24-hour market does not care about midnight. The Indian market does. Three
things go wrong if you treat time as a plain UTC number:

1. NSE equity derivatives trade 09:15-15:40 IST, but the cash market closes at
   15:30. Using one close time for both drops the last ten minutes of the F&O
   session, which is the settlement window and often the most active part of the
   day. The two segments must be separate calendars.

2. MCX non-agri commodities close at 23:30 IST while US daylight saving is in
   effect and 23:55 IST otherwise. The close is *seasonal*, and it is driven by
   US DST, not Indian time. Hardcoding one close time is wrong for half the year.

3. The MCX evening session crosses local midnight relative to a naive
   "day = date" model only if you build it carelessly; more importantly the
   daily reset must happen at the session open, not at 00:00 IST. Resetting
   cumulative delta at midnight would wipe the MCX evening session's own data.

So: sessions are defined in IST, anchored to the calendar date of the session
open, and the daily reset is tied to the session boundary.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from enum import Enum
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")
US_EASTERN = ZoneInfo("America/New_York")


class Market(str, Enum):
    NSE = "NSE"
    MCX = "MCX"
    NSE_COMMODITY = "NSECOMM"


@dataclass(frozen=True, slots=True)
class SessionWindow:
    """A trading window on one IST calendar date."""

    market: str
    trading_date: date
    open_at: datetime
    close_at: datetime
    label: str = "regular"

    def contains(self, ts: datetime) -> bool:
        return self.open_at <= ts < self.close_at

    def duration_seconds(self) -> float:
        return (self.close_at - self.open_at).total_seconds()

    def __str__(self) -> str:
        return (
            f"{self.market} {self.trading_date} "
            f"{self.open_at:%H:%M}-{self.close_at:%H:%M} IST ({self.label})"
        )


def _at(d: date, hh: int, mm: int) -> datetime:
    return datetime.combine(d, time(hh, mm), tzinfo=IST)


def us_daylight_saving(d: date) -> bool:
    """True while US DST is in effect on `d`.

    US DST runs from the second Sunday of March to the first Sunday of November.
    This is what moves the MCX non-agri close between 23:30 and 23:55 IST, so it
    is computed from the tz database rather than hardcoded.
    """
    probe = datetime(d.year, d.month, d.day, 12, 0, tzinfo=US_EASTERN)
    return probe.dst() != timedelta(0)


class MarketCalendar:
    """Builds session windows and answers time questions for a market.

    `holidays` is a set of IST dates with no trading. It must be supplied by the
    caller from the exchange holiday list; this class does not guess, because a
    guessed holiday is a silently missing day of profile data.
    """

    def __init__(
        self,
        market: Market,
        holidays: set[date] | None = None,
        segment: str = "NSE_FNO",
    ) -> None:
        self.market = market
        self.holidays = holidays or set()
        self.segment = segment

    # -------------------------------------------------------------- sessions
    def session_for(self, d: date) -> SessionWindow | None:
        """The primary trading window whose *open* falls on date `d`."""
        if d.weekday() >= 5 or d in self.holidays:
            return None

        if self.market is Market.NSE:
            if self.segment == "NSE_EQUITY":
                return SessionWindow("NSE", d, _at(d, 9, 15), _at(d, 15, 30), "cash")
            return SessionWindow("NSE", d, _at(d, 9, 15), _at(d, 15, 40), "fno")

        if self.market is Market.MCX:
            close_h, close_m = (23, 30) if us_daylight_saving(d) else (23, 55)
            return SessionWindow("MCX", d, _at(d, 9, 0), _at(d, close_h, close_m), "nonagri")

        if self.market is Market.NSE_COMMODITY:
            close_h, close_m = (23, 30) if us_daylight_saving(d) else (23, 55)
            return SessionWindow("NSECOMM", d, _at(d, 9, 0), _at(d, close_h, close_m), "nonagri")

        raise ValueError(f"unknown market: {self.market}")

    def session_containing(self, ts: datetime) -> SessionWindow | None:
        """The session that contains `ts`, checking today and the day before.

        The previous day is checked because an MCX session opened yesterday can
        still be running after local midnight.
        """
        ts = ts.astimezone(IST)
        for offset in (0, -1):
            win = self.session_for(ts.date() + timedelta(days=offset))
            if win is not None and win.contains(ts):
                return win
        return None

    def is_open(self, ts: datetime) -> bool:
        return self.session_containing(ts) is not None

    # ------------------------------------------------------- session-relative
    def session_key(self, ts: datetime) -> int | None:
        """Stable integer key identifying the session a timestamp belongs to.

        This is the anchor for the daily reset: cumulative delta and the volume
        profile key off `session_key`, so they roll over exactly at the session
        open and never at midnight.
        """
        win = self.session_containing(ts)
        return None if win is None else win.trading_date.toordinal()

    def session_start(self, ts: datetime) -> datetime | None:
        win = self.session_containing(ts)
        return None if win is None else win.open_at

    def session_elapsed(self, ts: datetime) -> float:
        """Seconds since the current session opened; 0 when closed."""
        start = self.session_start(ts)
        return 0.0 if start is None else (ts.astimezone(IST) - start).total_seconds()

    def next_open(self, ts: datetime) -> datetime:
        """The next session open at or after `ts` (skips weekends and holidays)."""
        ts = ts.astimezone(IST)
        d = ts.date()
        for _ in range(400):
            win = self.session_for(d)
            if win is not None and win.open_at > ts:
                return win.open_at
            d += timedelta(days=1)
        raise RuntimeError("no session open found within 400 days")

    # ------------------------------------------------------------- bar grid
    def session_bars(
        self,
        ts: datetime,
        bar_seconds: float,
    ) -> int:
        """Which bar of the session `ts` falls in, counted from the session open.

        Bar numbering restarts at every session open. Counting from midnight
        instead would misalign bars against the opening auction and against every
        other day's profile.
        """
        start = self.session_start(ts)
        if start is None:
            raise ValueError(f"{ts} is outside any trading session")
        return int((ts.astimezone(IST) - start).total_seconds() // bar_seconds)

    def bar_bucket_ts(self, ts: datetime, bar_seconds: float) -> datetime:
        """The session-aligned start of the bar containing `ts`."""
        start = self.session_start(ts)
        if start is None:
            raise ValueError(f"{ts} is outside any trading session")
        offset = int((ts.astimezone(IST) - start).total_seconds() // bar_seconds)
        return start + timedelta(seconds=offset * bar_seconds)


@dataclass(slots=True)
class SessionTracker:
    """Tracks session rollovers and reports when a reset is due."""

    calendar: MarketCalendar
    _current_key: int | None = field(default=None, init=False)

    @property
    def current_key(self) -> int | None:
        return self._current_key

    def observe(self, ts: datetime) -> bool:
        """Feed a timestamp; True when a new session just started.

        A `False` return on the very first observation is deliberate: the first
        session establishes a baseline and has nothing to reset from.
        """
        key = self.calendar.session_key(ts)
        if key is None:
            return False
        if self._current_key is None:
            self._current_key = key
            return False
        if key != self._current_key:
            self._current_key = key
            return True
        return False

    def reset(self) -> None:
        self._current_key = None
