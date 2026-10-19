"""Strict tick-size arithmetic for Indian market instruments.

Why this module exists
----------------------
Indian instruments have small, decimal tick sizes: NSE equities and index
futures tick at 0.05 (5 paise), MCX gold futures tick at 1.00 (1 rupee). The
naive implementation `index = round(price / tick_size)` breaks on both counts:

1. `round()` is banker's rounding (round-half-to-even). For a 0.05 grid the
   half-way points are exactly the prices ending in .025/.075, and `round()`
   sends 100.025 to 100.00 but 100.075 to 100.10. The snap is not a clean grid
   snap, and worse, it is not monotonic at the half-way points.

2. Dividing by a float tick accumulates error. `1/0.03` is not exact, so a grid
   built from it drifts, and two ticks that should map to adjacent indices can
   collide.

Both failures put volume on the wrong price level, which corrupts the diagonal
imbalance comparison (Bid[P] vs Ask[P + one tick]) and therefore every
downstream signal.

The fix: represent prices as integer counts of a fixed base unit (1e-8), keep
the tick as an integer count of the same base unit, and do the division with
exact integer arithmetic and explicit HALF_UP rounding. No float ever decides
which level a trade belongs to.
"""

from __future__ import annotations

from decimal import Decimal

# 1e-8 base unit. Indian tick sizes are multiples of 0.01, so every real tick
# size divides this exactly, and every price the exchanges publish (max 2
# decimals) is representable without loss.
BASE_EXP = 8
BASE_UNITS = 10**BASE_EXP


class OffGridTick(ValueError):
    """Raised when a price is not on the instrument's tick grid."""


class TickGrid:
    """Exact price <-> integer index mapping at a fixed tick size.

    `tick_size` must be an exact decimal multiple of 1e-8 (all exchange tick
    sizes are). Anything else raises immediately rather than silently drifting.
    """

    __slots__ = (
        "tick_size",
        "tick_units",
        "_strict",
        "_off_grid",
        "_tolerance_units",
        "_max_off_grid",
    )

    def __init__(
        self,
        tick_size: float | str | Decimal,
        *,
        strict: bool = True,
        tolerance_units: int = 0,
        max_off_grid: int = 100,
    ) -> None:
        tick = Decimal(str(tick_size))
        if tick <= 0:
            raise ValueError("tick_size must be > 0")
        units = tick * BASE_UNITS
        if units != units.to_integral_value():
            raise ValueError(
                f"tick_size {tick_size!r} is not an exact multiple of 1e-{BASE_EXP}"
            )
        self.tick_size = float(tick)
        self.tick_units = int(units)
        self._strict = strict
        self._off_grid = 0
        self._tolerance_units = tolerance_units
        self._max_off_grid = max_off_grid

    # ------------------------------------------------------------- internals
    @staticmethod
    def _to_units(price: float | str | Decimal) -> int:
        """Exact integer base units for a price, via its decimal text form.

        `str(price)` yields the shortest decimal that round-trips to the same
        float, which is what the feed actually meant. `Decimal(price)` on a
        float would instead capture the binary expansion and reintroduce the
        error we are trying to eliminate.
        """
        return int(Decimal(str(price)) * BASE_UNITS)

    # ---------------------------------------------------------------- public
    @property
    def off_grid_count(self) -> int:
        """How many ticks were rejected or clamped as off-grid."""
        return self._off_grid

    def is_aligned(self, price: float | str | Decimal) -> bool:
        """True when price sits exactly on the grid."""
        return self._to_units(price) % self.tick_units == 0

    def to_index(self, price: float | str | Decimal) -> int:
        """Snap a price onto the grid and return its integer index.

        Uses HALF_UP: a price exactly half a tick away rounds up. This is the
        exchange convention and it is monotonic, unlike banker's rounding.

        Off-grid handling is a trade-off, not a toggle:

        * A handful of off-grid prints is feed noise. Raising on the first one
          would let a single bad tick kill the whole engine mid-session, which
          is worse than the rounding error. Those are clamped and counted.
        * A flood of off-grid prints means the grid is configured wrong (wrong
          tick size for the instrument). That must fail loudly, because every
          number produced afterwards is silently wrong.

        `off_grid_count` is the signal to watch: log it, alert on it.
        """
        units = self._to_units(price)
        rem = units % self.tick_units
        if rem != 0 and rem > self._tolerance_units and self.tick_units - rem > self._tolerance_units:
            self._off_grid += 1
            if self._strict and self._off_grid > self._max_off_grid:
                raise OffGridTick(
                    f"price {price!r} is off the {self.tick_size:g} tick grid, and "
                    f"{self._off_grid} such prints have been seen; the tick size "
                    "is probably wrong for this instrument"
                )
        q, r = divmod(units, self.tick_units)
        if 2 * r >= self.tick_units:
            q += 1
        return q

    def to_price(self, index: int) -> float:
        """Grid index back to a price."""
        return index * self.tick_units / BASE_UNITS

    def to_decimal(self, index: int) -> Decimal:
        """Grid index back to an exact Decimal price (for display/reporting)."""
        return Decimal(index * self.tick_units) / BASE_UNITS

    def snap(self, price: float | str | Decimal) -> float:
        """Round a price onto the grid."""
        return self.to_price(self.to_index(price))

    def ticks_between(self, a: float | str | Decimal, b: float | str | Decimal) -> int:
        """Signed distance in ticks from a to b."""
        return self.to_index(b) - self.to_index(a)

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"TickGrid(tick_size={self.tick_size:g}, strict={self._strict})"
