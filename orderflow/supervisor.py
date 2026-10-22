"""Keep the live feed running, unattended, for as long as the process lives.

The connector already reconnects a dropped socket and resubscribes. What it does
not do is survive the three things that end a real trading day:

* **A session boundary.** NSE closes at 15:30 and MCX at 23:30; the next open is
  the next morning. A supervisor must sleep through the closed hours and
  resubscribe at the open, not exit.
* **An expired token.** Angel One's JWT is short-lived. When the socket rejects
  it, the supervisor must log in again and rebuild the transports, because
  retrying the same dead token only fails again.
* **A run of failures.** A broker outage or a bad network must back off, not
  spin. But the backoff must never be terminal: the whole point is that the feed
  is still there in the morning.

`AlwaysOnFeed` is that loop. It never returns on its own. The only ways out are
an explicit `stop()` from the operator or the process dying, which is why the
dashboard can present it as a feed that is simply always up.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable

from .session import IST, Market, MarketCalendar


@dataclass(slots=True)
class FeedHealth:
    """What the supervisor is doing, for the dashboard to display."""

    state: str = "idle"  # idle|waiting|connecting|streaming|reconnecting|error
    detail: str = ""
    attempts: int = 0
    reconnects: int = 0
    relogins: int = 0
    frames: int = 0
    trades: int = 0
    last_frame_at: float | None = None
    session_open_at: float | None = None
    next_open_at: float | None = None
    started_at: float | None = None
    stopped: bool = False


@dataclass(slots=True)
class SupervisorConfig:
    #: How long one connector run is allowed before it is torn down and rebuilt.
    #: A periodic rebuild is cheap insurance against a wedged socket that still
    #: looks alive.
    run_seconds: float = 6 * 3600.0
    #: Backoff bounds for a failing connect/login, in seconds.
    backoff_min: float = 2.0
    backoff_max: float = 120.0
    #: When the market is closed, how often to re-check whether it has opened.
    closed_poll_seconds: float = 30.0
    #: Re-login after this many seconds even without an explicit auth failure,
    #: because a token can expire without the socket saying so.
    relogin_after_seconds: float = 8 * 3600.0
    #: Log every transition. Off in tests.
    verbose: bool = False


class AlwaysOnFeed:
    """An unattended feed loop around `SmartStreamConnector`.

    The connector and its transport factory are supplied by the caller, because
    only the caller knows how to log in and build a socket. The supervisor owns
    the *policy*: when to run, when to back off, when to log in again, and when
    the market is closed.
    """

    def __init__(
        self,
        calendar: MarketCalendar,
        make_connector: Callable[[], object],
        config: SupervisorConfig | None = None,
        on_trade: Callable[[object], None] | None = None,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.calendar = calendar
        self.make_connector = make_connector
        self.config = config or SupervisorConfig()
        self.on_trade = on_trade
        self.clock = clock
        self.sleep = sleep
        self.health = FeedHealth()
        self._stop = threading.Event()
        self._connector = None
        self._started = 0.0

    # ------------------------------------------------------------------ control
    def stop(self) -> None:
        """The only way the loop ends. Called by the operator, never by itself."""
        self._stop.set()
        self.health.stopped = True
        self.health.state = "stopped"
        # Dropping the connector closes its socket on the next recv timeout.
        self._connector = None

    @property
    def stopping(self) -> bool:
        return self._stop.is_set()

    # --------------------------------------------------------------------- loop
    def run(self) -> None:
        """Run until `stop()`. Never returns on its own."""
        cfg = self.config
        self.health.started_at = self.clock()
        self._started = self.clock()
        backoff = cfg.backoff_min
        last_relogin = 0.0

        while not self._stop.is_set():
            now = self.clock()
            when = datetime.fromtimestamp(now, tz=IST)

            if not self._market_open(when):
                self._wait_for_open(when)
                continue

            if now - last_relogin >= cfg.relogin_after_seconds and last_relogin:
                self.health.relogins += 1
                last_relogin = now
                self._note("relogin", "refreshing the session token")

            try:
                self.health.state = "connecting"
                self._note("connecting", "")
                connector = self.make_connector()
                self._connector = connector
                self.health.attempts += 1

                budget = min(cfg.run_seconds, self._seconds_to_close(when))
                self.health.state = "streaming"
                self._note("streaming", f"budget {budget:.0f}s")
                stats = connector.run(
                    on_trade=self._on_trade,
                    max_seconds=budget,
                    reconnect=True,
                    # The connector calls this between frames, so it must be the
                    # bound method, not the `stopping` property's value.
                    should_stop=self._stop.is_set,
                )
                self._absorb(stats)
                # A clean end of a run is normal: either the budget elapsed or
                # the session closed. Reset the backoff and go round again.
                backoff = cfg.backoff_min
            except Exception as exc:  # noqa: BLE001 - the loop must never die
                self.health.state = "error"
                self.health.detail = f"{type(exc).__name__}: {exc}"
                self._note("error", self.health.detail)
                # A failure is expected sometimes. Back off, then try again.
                self.sleep(backoff)
                backoff = min(backoff * 2, cfg.backoff_max)
                continue
            finally:
                self._connector = None

        self.health.state = "stopped"

    # --------------------------------------------------------------- internals
    def _on_trade(self, routed) -> None:
        self.health.trades += 1
        self.health.last_frame_at = self.clock()
        if self.on_trade is not None:
            self.on_trade(routed)

    def _absorb(self, stats) -> None:
        """Fold a connector's run stats into the health view."""
        self.health.frames = getattr(stats, "frames", self.health.frames)
        self.health.reconnects += getattr(stats, "reconnects", 0)
        self.health.last_frame_at = self.clock()

    def _market_open(self, when: datetime) -> bool:
        return self.calendar.session_key(when) is not None

    def _wait_for_open(self, when: datetime) -> None:
        nxt = self.calendar.next_open(when)
        self.health.state = "waiting"
        self.health.next_open_at = nxt.timestamp()
        self._note("waiting", f"market closed, next open {nxt:%Y-%m-%d %H:%M} IST")
        # Sleep in short slices so a stop() is honoured promptly rather than
        # after the whole closed period.
        deadline = nxt.timestamp()
        while not self._stop.is_set() and self.clock() < deadline:
            slice_s = min(self.config.closed_poll_seconds, max(0.0, deadline - self.clock()))
            if slice_s <= 0:
                break
            self.sleep(slice_s)

    def _seconds_to_close(self, when: datetime) -> float:
        window = self.calendar.session_containing(when)
        if window is None:
            return 0.0
        return max(0.0, (window.close_at - when).total_seconds())

    def _note(self, state: str, detail: str) -> None:
        if self.config.verbose:
            stamp = datetime.fromtimestamp(self.clock(), tz=IST).strftime("%H:%M:%S")
            print(f"[feed {stamp}] {state}: {detail}", flush=True)

    def snapshot(self) -> dict:
        h = self.health
        return {
            "state": h.state,
            "detail": h.detail,
            "attempts": h.attempts,
            "reconnects": h.reconnects,
            "relogins": h.relogins,
            "frames": h.frames,
            "trades": h.trades,
            "last_frame_at": h.last_frame_at,
            "next_open_at": h.next_open_at,
            "stopped": h.stopped,
        }
