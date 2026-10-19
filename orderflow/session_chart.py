"""Session-scoped aggregators: the daily reset, done properly.

The bug this module prevents: cumulative delta and the volume profile are
running totals. If they are never reset, yesterday's 40,000 lots of delta are
still in today's number at 09:15, and the "value area" spans two days of prices.
Every footprint platform that gets this wrong shows a cumulative delta line that
never returns to zero and a profile with a POC from a week ago.

The reset fires on the *session* boundary, not at midnight IST. MCX trades until
23:30/23:55 IST; resetting at midnight would wipe the live evening session's own
accumulated data halfway through it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from .core import Trade
from .footprint import FootprintBar, FootprintChart
from .profile import Profile, ProfileBuilder, ProfileMode
from .session import IST, MarketCalendar, SessionTracker


@dataclass(slots=True)
class SessionState:
    """Per-session running state that must reset at each session open."""

    session_key: int
    session_open: datetime
    chart: FootprintChart
    trades: int = 0

    @property
    def bars(self) -> list[FootprintBar]:
        return self.chart.bars

    @property
    def cumulative_delta(self) -> float:
        """Session-to-date cumulative delta. Starts at zero every session."""
        return sum(b.delta for b in self.chart.bars)

    @property
    def total_volume(self) -> float:
        return sum(b.total_volume for b in self.chart.bars)

    def profile(self, pct: float = 70.0) -> Profile:
        """Volume profile for this session only."""
        builder = ProfileBuilder(
            self.chart.grid,
            ProfileMode.SESSION,
            value_area_pct=pct,
            session_seconds=1e18,
        )
        profs = builder.build(self.chart.bars)
        return profs[0] if profs else Profile(0.0, 0.0, self.chart.grid, value_area_pct=pct)


class SessionChart:
    """A footprint chart whose running state resets every trading session.

        cal = MarketCalendar(Market.NSE, segment="NSE_FNO")
        chart = SessionChart(grid, cal, bar_seconds=300)
        for trade in feed:
            chart.add(trade)
        chart.cumulative_delta   # today only, zero at 09:15

    `on_session_open` fires once per new session; that is the hook a live system
    uses to persist yesterday's profile and clear the UI.
    """

    def __init__(
        self,
        grid,
        calendar: MarketCalendar,
        bar_seconds: float = 300.0,
        value_area_pct: float = 70.0,
        on_session_open=None,
    ) -> None:
        if bar_seconds <= 0:
            raise ValueError("bar_seconds must be > 0")
        self.grid = grid
        self.calendar = calendar
        self.bar_seconds = bar_seconds
        self.value_area_pct = value_area_pct
        self.on_session_open = on_session_open
        self.tracker = SessionTracker(calendar)
        self.state: SessionState | None = None
        self.closed_sessions: list[SessionState] = []
        self._off_session_trades = 0

    # ------------------------------------------------------------------ feed
    def add(self, trade: Trade, ts: datetime | None = None) -> bool:
        """Feed one trade. Returns True when it opened a new session.

        `trade.ts` is epoch seconds. `ts` may be passed when the caller already
        holds a timezone-aware datetime.
        """
        when = datetime.fromtimestamp(trade.ts, tz=IST) if ts is None else ts.astimezone(IST)

        if self.calendar.session_key(when) is None:
            # Pre-open, post-close or a holiday. Counted and dropped: folding
            # these into the next session would corrupt the opening profile.
            self._off_session_trades += 1
            return False

        rolled = self.tracker.observe(when)
        if rolled:
            self._roll(when)
        elif self.state is None:
            self._open_session(when)

        assert self.state is not None
        self.state.chart.add(trade)
        self.state.trades += 1
        return rolled

    def add_many(self, trades, ts_of=None) -> None:
        for t in trades:
            self.add(t, None if ts_of is None else ts_of(t))

    # --------------------------------------------------------------- internals
    def _open_session(self, when: datetime) -> None:
        key = self.calendar.session_key(when)
        start = self.calendar.session_start(when)
        assert key is not None and start is not None
        self.state = SessionState(
            session_key=key,
            session_open=start,
            chart=FootprintChart(self.grid, bar_seconds=self.bar_seconds),
        )

    def _roll(self, when: datetime) -> None:
        if self.state is not None:
            self.closed_sessions.append(self.state)
        self.state = None
        self._open_session(when)
        if self.on_session_open is not None:
            self.on_session_open(self.state)

    # ---------------------------------------------------------------- access
    @property
    def bars(self) -> list[FootprintBar]:
        return self.state.bars if self.state else []

    @property
    def current(self) -> FootprintBar | None:
        return self.state.chart.current if self.state else None

    @property
    def cumulative_delta(self) -> float:
        """Session-to-date cumulative delta; zero at every session open."""
        return self.state.cumulative_delta if self.state else 0.0

    @property
    def session_volume(self) -> float:
        return self.state.total_volume if self.state else 0.0

    def session_profile(self) -> Profile | None:
        return self.state.profile(self.value_area_pct) if self.state and self.state.bars else None

    @property
    def off_session_trades(self) -> int:
        return self._off_session_trades
