"""Persist the last feed run so the page is never a blank box.

The dashboard's state is in memory. A restart, or simply a market that is
closed, leaves every panel reading zero, and a page of zeros looks like a dead
engine rather than an idle one. The desk reads a zero POC as "the feed is
broken", which is the one thing it must never have to guess.

This saves the last run's *snapshots* — the same JSON the page already renders —
to a single file and loads them at boot. So the last known POC, cumulative delta
and PnL stay visible with the time they were taken, clearly marked as the last
session rather than live.

Only snapshots are stored, never engine objects. Two reasons:

* A stored object would pin the engine's internal schema to the file, so any
  refactor becomes a migration. Snapshots are the page's contract and change
  only when the page changes.
* A file written by an older build must still load. An unknown shape is ignored,
  not fatal: a dashboard that refuses to start because of its own cache is worse
  than one that starts blank.

Every write is atomic (write a temp file, then `os.replace`), so a crash mid-save
cannot leave a half-written file for the next boot to trip over. Reads never
raise: a corrupt or absent file simply yields `None`.
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

#: Bumped when the payload shape changes in a way an older reader must reject.
SAVE_VERSION = 1

DEFAULT_PATH = "last_session.json"


class SessionStore:
    """Atomic JSON storage for one last-session snapshot."""

    def __init__(self, path: str | Path | None = None) -> None:
        # Resolve the default at call time, not at class-definition time, so a
        # caller (or a test) that rebinds DEFAULT_PATH is honoured.
        self.path = Path(path) if path is not None else Path(DEFAULT_PATH)

    def save(self, payload: dict) -> bool:
        """Write `payload` atomically. Returns False on any IO failure.

        A failed save must not raise into the feed thread: the desk's data feed
        matters more than its cache.
        """
        body = dict(payload)
        body["version"] = SAVE_VERSION
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            fd, tmp = tempfile.mkstemp(
                dir=str(self.path.parent), prefix=self.path.name, suffix=".tmp")
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as fh:
                    json.dump(body, fh, default=str)
                    fh.flush()
                    os.fsync(fh.fileno())
                os.replace(tmp, self.path)
            except BaseException:
                # Never leave the temp file behind on a failed write.
                try:
                    os.unlink(tmp)
                except OSError:
                    pass
                raise
        except Exception:
            return False
        return True

    def load(self) -> dict | None:
        """The stored payload, or None when absent, unreadable or a bad version."""
        try:
            with open(self.path, encoding="utf-8") as fh:
                body = json.load(fh)
        except (OSError, ValueError):
            return None
        if not isinstance(body, dict):
            return None
        if body.get("version") != SAVE_VERSION:
            return None
        return body
