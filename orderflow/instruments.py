"""Instruments and broker token mapping for the Indian market.

Two things live here:

* `Instrument` — the contract spec (exchange, segment, tick size, lot size,
  currency, expiry) plus the session calendar it belongs to.
* `TokenMapper` — maps a broker/exchange token id onto an instrument.

The token mapper exists because Indian brokers stream ticks keyed by numeric
tokens, not symbols. Angel One, for example, uses several *namespaces* of token
ids: one series for the NSE cash market, a separate series for NSE derivatives,
and another for MCX. Token `12345` in the cash namespace and token `12345` in
the derivatives namespace are different instruments. A mapper keyed on the bare
token number will silently deliver Nifty futures ticks labelled as some equity,
and every downstream number will be wrong while looking perfectly plausible.

So the lookup key is `(namespace, token)`, never the token alone.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from .ticks import TickGrid


class Exchange(str, Enum):
    NSE = "NSE"  # National Stock Exchange (equity + equity derivatives)
    BSE = "BSE"  # BSE (equity + derivatives)
    MCX = "MCX"  # Multi Commodity Exchange
    NCDEX = "NCDEX"  # agri commodities
    NSE_COMMODITY = "NSECOMM"  # NSE commodity derivatives segment


class Segment(str, Enum):
    EQ = "EQ"  # cash / spot
    FUT = "FUT"  # futures
    OPT = "OPT"  # options
    IDX = "IDX"  # index


class SessionKind(str, Enum):
    NSE_EQUITY = "NSE_EQUITY"  # 09:15 - 15:30 cash
    NSE_FNO = "NSE_FNO"  # 09:15 - 15:40 equity derivatives
    MCX_NONAGRI = "MCX_NONAGRI"  # 09:00 - 23:30 / 23:55
    MCX_AGRI = "MCX_AGRI"  # 09:00 - 21:00
    MCX_AGRI_OTHER = "MCX_AGRI_OTHER"  # 09:00 - 17:00


@dataclass(frozen=True, slots=True)
class Instrument:
    """A tradable contract with its exact tick arithmetic and session."""

    symbol: str
    exchange: Exchange
    segment: Segment
    tick_size: float
    lot_size: int = 1
    session: SessionKind = SessionKind.NSE_FNO
    expiry: str | None = None  # ISO date, for futures/options
    currency: str = "INR"

    def __post_init__(self) -> None:
        if self.tick_size <= 0:
            raise ValueError("tick_size must be > 0")
        if self.lot_size <= 0:
            raise ValueError("lot_size must be > 0")

    def grid(self, **kwargs) -> TickGrid:
        """A strict tick grid for this instrument."""
        return TickGrid(self.tick_size, **kwargs)

    @property
    def full_name(self) -> str:
        exp = f" {self.expiry}" if self.expiry else ""
        return f"{self.exchange.value}:{self.symbol}:{self.segment.value}{exp}"


# --------------------------------------------------------------------------
# Well-known Indian contracts. Tick sizes here are the exchange-published ones:
#   NSE equity and index derivatives: 0.05
#   MCX bullion (gold, silver) and base metals: 1.00 for gold, 0.05-1.00 varies
# Lot sizes change with contract revisions; they are defaults, not gospel, and
# should be refreshed from the broker's instrument master at startup.
# --------------------------------------------------------------------------
NIFTY_FUT = Instrument(
    symbol="NIFTY",
    exchange=Exchange.NSE,
    segment=Segment.FUT,
    tick_size=0.05,
    lot_size=75,
    session=SessionKind.NSE_FNO,
)
BANKNIFTY_FUT = Instrument(
    symbol="BANKNIFTY",
    exchange=Exchange.NSE,
    segment=Segment.FUT,
    tick_size=0.05,
    lot_size=35,
    session=SessionKind.NSE_FNO,
)
RELIANCE_EQ = Instrument(
    symbol="RELIANCE",
    exchange=Exchange.NSE,
    segment=Segment.EQ,
    tick_size=0.05,
    lot_size=1,
    session=SessionKind.NSE_EQUITY,
)
MCX_GOLD_FUT = Instrument(
    symbol="GOLD",
    exchange=Exchange.MCX,
    segment=Segment.FUT,
    tick_size=1.00,
    lot_size=100,
    session=SessionKind.MCX_NONAGRI,
)
MCX_SILVER_FUT = Instrument(
    symbol="SILVER",
    exchange=Exchange.MCX,
    segment=Segment.FUT,
    tick_size=1.00,
    lot_size=30,
    session=SessionKind.MCX_NONAGRI,
)


class TokenNamespace(str, Enum):
    """Broker token id series. Tokens are only unique inside a namespace."""

    NSE_EQ = "NSE_EQ"
    NSE_FO = "NSE_FO"
    BSE_EQ = "BSE_EQ"
    BSE_FO = "BSE_FO"
    MCX_FO = "MCX_FO"
    NCDEX_FO = "NCDEX_FO"


class UnknownToken(KeyError):
    """Raised when a (namespace, token) pair has no registered instrument."""


@dataclass(slots=True)
class TokenMapper:
    """Maps `(namespace, token)` to `Instrument`, and back.

    Deliberately keyed on the pair. A bare-token lookup is a latent
    cross-segment collision bug, so the API does not offer one.
    """

    _by_token: dict[tuple[TokenNamespace, int], Instrument] = field(default_factory=dict)
    _by_symbol: dict[str, Instrument] = field(default_factory=dict)

    def register(
        self,
        namespace: TokenNamespace,
        token: int,
        instrument: Instrument,
    ) -> None:
        key = (namespace, int(token))
        existing = self._by_token.get(key)
        if existing is not None and existing is not instrument:
            raise ValueError(
                f"token {token} in {namespace.value} already maps to "
                f"{existing.full_name}; refusing to overwrite with "
                f"{instrument.full_name}"
            )
        self._by_token[key] = instrument
        self._by_symbol[instrument.symbol] = instrument

    def register_many(
        self,
        namespace: TokenNamespace,
        mapping: dict[int, Instrument],
    ) -> None:
        for token, inst in mapping.items():
            self.register(namespace, token, inst)

    def by_token(self, namespace: TokenNamespace, token: int) -> Instrument:
        try:
            return self._by_token[(namespace, int(token))]
        except KeyError:
            raise UnknownToken(
                f"no instrument registered for token {token} in {namespace.value}"
            ) from None

    def by_symbol(self, symbol: str) -> Instrument:
        try:
            return self._by_symbol[symbol]
        except KeyError:
            raise UnknownToken(f"no instrument registered for symbol {symbol!r}") from None

    def tokens_for(self, symbol: str) -> list[tuple[TokenNamespace, int]]:
        return [
            (ns, tok) for (ns, tok), inst in self._by_token.items() if inst.symbol == symbol
        ]

    def __len__(self) -> int:
        return len(self._by_token)


def build_indian_token_map() -> TokenMapper:
    """A ready-made mapper with the well-known contracts registered.

    Real deployments replace the token numbers from the broker's instrument
    master file at startup; the namespaces and structure stay the same.
    """
    m = TokenMapper()
    m.register(TokenNamespace.NSE_FO, 26000, NIFTY_FUT)
    m.register(TokenNamespace.NSE_FO, 26001, BANKNIFTY_FUT)
    m.register(TokenNamespace.NSE_EQ, 2885, RELIANCE_EQ)
    m.register(TokenNamespace.MCX_FO, 42809, MCX_GOLD_FUT)
    m.register(TokenNamespace.MCX_FO, 43210, MCX_SILVER_FUT)
    return m
