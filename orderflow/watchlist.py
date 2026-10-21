"""The desk's instrument list and the tick-size rules that go with it.

Kept free of any web dependency so the recorder, the CLI and the dashboard can
all share one source of truth. Two copies of a tick table is one copy too many:
a wrong tick silently misplaces every price in the footprint, and the symptom
looks like a decoder bug rather than a configuration one.
"""

from __future__ import annotations

from dataclasses import dataclass

from .feed import TokenNamespace
from .instrument_master import TokenMapper, upcoming_futures
from .session import Market

# MCX contract sizes differ by a factor of ten within a family, so each name
# carries its own tick. The master's `tick_size` field is not usable for this:
# it is set to 10.0 for index records and is not the trading tick.
MCX_TICK_BY_UNDERLYING = {
    "NATURALGAS": 0.10,
    "CRUDEOIL": 1.0,
    "GOLD": 1.0,
    "GOLDM": 1.0,
    "GOLDPETAL": 1.0,
    "SILVER": 1.0,
    "SILVERM": 0.1,
    "SILVERMIC": 0.1,
    "COPPER": 0.05,
    "ZINC": 0.05,
    "ALUMINIUM": 0.05,
    "LEAD": 0.05,
    "NICKEL": 0.10,
}

#: The contracts the desk asked to watch: NIFTY plus the four liquid MCX
#: energy and metals futures.
DESK_UNDERLYINGS: tuple[tuple[str, Market, str], ...] = (
    ("NIFTY", Market.NSE, "NSE_FNO"),
    ("NATURALGAS", Market.MCX, "MCX_FO"),
    ("CRUDEOIL", Market.MCX, "MCX_FO"),
    ("GOLD", Market.MCX, "MCX_FO"),
    ("SILVER", Market.MCX, "MCX_FO"),
)


@dataclass
class WatchSpec:
    """One instrument to subscribe to."""

    label: str
    namespace: TokenNamespace
    token: int
    symbol: str
    market: Market
    segment: str
    tick: float
    expiry: str | None = None

    def key(self) -> str:
        """Path-safe identity. Used as a URL segment and a dict key."""
        return f"{self.namespace.value}_{self.token}"

    def subscription(self) -> tuple[TokenNamespace, int]:
        return (self.namespace, self.token)


def tick_for(underlying: str, market: Market) -> float:
    """The trading tick for an underlying.

    Longest name first: "SILVER" is a prefix of "SILVERMIC" and "GOLD" of
    "GOLDPETAL", and each pair trades on a different tick. Matching the short
    name first is a silent misconfiguration, not a crash.
    """
    if market is not Market.MCX:
        return 0.05
    name = (underlying or "").upper()
    for key in sorted(MCX_TICK_BY_UNDERLYING, key=len, reverse=True):
        if name.startswith(key):
            return MCX_TICK_BY_UNDERLYING[key]
    return 1.0


def resolve(mapper: TokenMapper, underlying: str, market: Market,
            segment: str, limit: int = 1) -> list[WatchSpec]:
    """Nearest futures for an underlying, as watch specs.

    Uses the master's `underlying` field, not a symbol prefix. "NIFTY" is a
    prefix of "NIFTYNXT50", so a prefix search returns a different instrument
    and the feed streams it without complaint.
    """
    try:
        found = upcoming_futures(mapper, underlying, limit=limit)
    except Exception:
        return []
    out = []
    for ns, tok, inst in found:
        out.append(WatchSpec(
            label=inst.symbol, namespace=ns, token=tok, symbol=inst.symbol,
            market=market, segment=segment,
            tick=tick_for(inst.underlying or underlying, market),
            expiry=inst.expiry,
        ))
    return out


def desk_watchlist(mapper: TokenMapper) -> list[WatchSpec]:
    """The desk list, resolved against the master."""
    out: list[WatchSpec] = []
    for underlying, market, segment in DESK_UNDERLYINGS:
        out.extend(resolve(mapper, underlying, market, segment))
    return out


def parse_instruments(spec: str) -> list[WatchSpec]:
    """Parse `NAMESPACE:TOKEN[:TICK]` pairs, comma separated.

    Ticks are per-contract and cannot be guessed for an arbitrary token, so a
    caller subscribing by raw token must supply one. Without it the default of
    0.05 applies, which is right for NSE and wrong for most of MCX.
    """
    out: list[WatchSpec] = []
    for chunk in spec.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        parts = chunk.split(":")
        if len(parts) < 2:
            raise ValueError(f"bad instrument {chunk!r}, want NAMESPACE:TOKEN[:TICK]")
        ns = TokenNamespace(parts[0].strip().upper())
        token = int(parts[1])
        tick = float(parts[2]) if len(parts) > 2 else 0.05
        market = Market.MCX if ns is TokenNamespace.MCX_FO else Market.NSE
        segment = "MCX_FO" if market is Market.MCX else "NSE_FNO"
        out.append(WatchSpec(
            label=f"{ns.value}:{token}", namespace=ns, token=token,
            symbol=f"{ns.value}:{token}", market=market, segment=segment,
            tick=tick,
        ))
    return out
